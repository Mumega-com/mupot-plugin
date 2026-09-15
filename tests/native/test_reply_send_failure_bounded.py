"""Round 5 class fix (2026-09-15, PR #9, head c9b79aa -> this round):
**custody is the only thing that may hold `pending`; every other path is
bounded; every reader of `pending`/`reply_outbox` is pinned.**

kasra-review P0 (head c9b79aa, `adapter.py:3524`): `staged.get("status") !=
"complete"` held `pending` for a `"prepared"` reply -- staged by
`_prepare_final_reply`, NEVER transmitted, zero custody -- exactly as it did
for a genuinely `"custodied"` one. A successful handler whose reply `send`
itself failed (peer down) therefore held `pending` forever, and
`_replay_reply_outbox` (statement #2 of every poll tick, BEFORE
`inbox_lease`) retried the same doomed `send` every tick: one lease ever,
`mupot_gateway_status` reporting fully healthy (`connected: true,
turn_failure_dlq: []`).

Fix: `_resolve_turn_failure` holds `pending` ONLY via
`_reply_outbox_has_custody` (status `"custodied"`/`"complete"` WITH a
receipt); a `"prepared"` reply that cannot be sent is Failed-bounded like
any other no-custody turn failure, and `_replay_reply_outbox`'s own new
`_bound_reply_send_failure` bounds ITS retries of that same `"prepared"`
record turn-tick over turn-tick (the message's own `delivery_attempts`
cannot bound this -- the server never re-leases a message this process is
still holding) with a local counter capped at the same
`max_delivery_attempts` semantics, then DLQs it (`reply_send_failed`) and
drops the orphaned record so the poll loop resumes leasing.

Athena MED 3a: `_resolve_turn_failure`'s at-cap branch used to DLQ
unconditionally, even when the staged reply WAS custodied (Delivered, not
failed) -- `turn_failure_dlq` named a message that then showed up in
`processed`. Fixed: DLQ only when custody was never achieved;
`turn_failure_dlq_summary()` also now excludes any id already in
`processed`, defensively, on the read side.

Athena MED 3b/3c: this plugin trusted the pot's server-side reaper entirely
-- against a non-conforming server that keeps re-leasing a message past
`max_delivery_attempts`, nothing bounded LOCAL re-execution (measured: 258
turns). Fixed: `_process_leased_message` skips (no turn, no ack) any
message id already carrying one of this class's own `dlq` rows.

MED-1 (both lenses, "pin the reader"): `_replay_reply_outbox`'s
pending-mismatch escalation to `reconciliation_required` still functions for
the case it exists for -- a genuinely `"custodied"` (or later) record whose
`pending` does not match it -- distinct from a `"prepared"` record, which
this round exempts from that check (see its own comment in
`_replay_reply_outbox`).

LOW-1: the `adapter.py:414-416` claim that the custody guard is "a no-op"
for `no_custody`/`turn_timeout` was FALSE under the pre-fix, status-based
predicate (it held `pending` for exactly this test's `"prepared"` shape);
under the round-5 custody-based predicate it is genuinely true, pinned here.
"""
from __future__ import annotations

import asyncio
import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pytest

from gateway.config import PlatformConfig
from plugin.mupot_gateway.adapter import (
    MupotAdapter,
    _final_request_id,
    _reply_source_fingerprint,
    _TurnFailureDeferred,
    MupotProtocolError,
)
from plugin.mupot_gateway.lease_ownership import legacy_ack_ownership

# The native test runner (`scripts/test-native.sh`) can execute each file in
# its own subprocess/worker (see `run_tests_parallel.py`), so cross-test-file
# imports are not reliable here -- the small amount of shared fixture
# infrastructure from `test_lease_expiry_deferred.py` (client fakes, the
# `_await_until` poller, `make_adapter`) is duplicated below rather than
# imported, matching that file's own self-contained style.

FAKE_SCOPE = {"tenant": "tenant-a", "agent_id": "agent-consumer", "effective_inbox_seat": None}
EXPIRED_LEASE = "2000-01-01T00:00:00.000Z"
FRESH_LEASE = "2099-01-01T00:00:00.000Z"

PEER_MSG = {
    "id": "m-peer", "seq": 7, "from_agent": "hadi-codex", "from_member": "member-code",
    "body": "full Mupot answer", "project_id": "project-1", "request_id": "req-7",
    "in_reply_to": None, "kind": "message", "created_at": "2026-09-13T00:00:00.000Z",
    "delivery_attempts": 1, "lease_expires_at": EXPIRED_LEASE,
}


def attempt_result(attempt_id, state, messages=None):
    return {
        **FAKE_SCOPE,
        "attempt_id": attempt_id,
        "state": state,
        "lease_expires_at": FRESH_LEASE if state == "leased" else None,
        "messages": messages or [],
        "consumed": False,
    }


def _iso_in(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat().replace(
        "+00:00", "Z"
    )


class ExpiryDriverClient:
    """One-message fake mupot server (see `test_lease_expiry_deferred.py`'s
    identically-named class for the full rationale -- duplicated here since
    cross-test-file imports are not reliable under the native runner)."""

    def __init__(self, message):
        self.message = copy.deepcopy(message)
        self.calls: list[tuple[str, dict]] = []
        self.lease_calls = 0
        self.acked = False
        self.redeliver = False
        self.sent: list[dict] = []

    async def connect(self):
        return None

    async def close(self):
        return None

    async def call(self, tool, arguments):
        self.calls.append((tool, copy.deepcopy(arguments)))
        if tool == "inbox_consumer_status":
            return {"strict_scope": True, **FAKE_SCOPE, "mode": "bearer_only",
                    "generation": 0, "key_matches": True}
        if tool == "inbox_lease":
            self.lease_calls += 1
            aid = arguments.get("attempt_id")
            if self.acked:
                return attempt_result(aid, "empty")
            msg = dict(self.message)
            if self.redeliver:
                msg["lease_expires_at"] = FRESH_LEASE
            return {
                **FAKE_SCOPE,
                "attempt_id": aid,
                "state": "leased",
                "lease_expires_at": msg["lease_expires_at"],
                "messages": [msg],
                "consumed": False,
            }
        if tool == "inbox_lease_ack":
            self.acked = True
            return {**FAKE_SCOPE, "attempt_id": arguments["attempt_id"],
                    "state": "acked", "consumed": True}
        if tool == "inbox_ack":
            self.acked = True
            return {"acked": [self.message["id"]], "already_read": [], "refused": []}
        if tool == "send":
            self.sent.append(arguments)
            return {"id": "m-out", "seq": 8, "duplicate": False, "to": arguments["to"],
                    "project_id": arguments.get("project_id"), "target_seat": None}
        raise AssertionError(f"unexpected tool {tool} {arguments}")

    def ack_calls(self):
        return [t for t, _ in self.calls if t in {"inbox_ack", "inbox_lease_ack"}]


class RollingLiveLeaseClient(ExpiryDriverClient):
    """Fake mupot server that always grants a FRESH, long-lived lease for the
    same message, incrementing `delivery_attempts` each time. When
    `dead_letter_after` is left `None`, this server NEVER reaps -- it models
    a non-conforming server that keeps re-leasing a message forever, past
    `max_delivery_attempts`, to prove LOCAL re-execution is still bounded
    (Athena MED 3b/3c)."""

    def __init__(self, message):
        super().__init__(message)
        self.dead_letter_after: Optional[int] = None
        self.dead_lettered = False

    async def call(self, tool, arguments):
        if tool == "inbox_lease":
            if self.dead_lettered:
                aid = arguments.get("attempt_id")
                return attempt_result(aid, "empty")
            if (
                not self.acked
                and self.dead_letter_after is not None
                and self.lease_calls >= self.dead_letter_after
            ):
                self.dead_lettered = True
                aid = arguments.get("attempt_id")
                return attempt_result(aid, "empty")
            self.lease_calls += 1
            aid = arguments.get("attempt_id")
            nxt = _iso_in(30)
            msg = dict(self.message)
            msg["lease_expires_at"] = nxt
            msg["delivery_attempts"] = self.lease_calls
            return {
                **FAKE_SCOPE,
                "attempt_id": aid,
                "state": "leased",
                "lease_expires_at": nxt,
                "messages": [msg],
                "consumed": False,
            }
        return await super().call(tool, arguments)


def make_adapter(tmp_path: Path, client: ExpiryDriverClient, *, extra=None) -> MupotAdapter:
    ex = {
        "allowed_agents": "hadi-codex",
        "poll_interval": 0.01,
        "state_path": str(tmp_path / "state.json"),
    }
    ex.update(extra or {})
    return MupotAdapter(
        PlatformConfig(enabled=True, typing_indicator=False, extra=ex),
        client_factory=lambda *_: client,
    )


async def _await_until(pred, n=300, dt=0.01) -> bool:
    for _ in range(n):
        if pred():
            return True
        await asyncio.sleep(dt)
    return False


def _state(state_path: Path) -> dict:
    return json.loads(state_path.read_text()) if state_path.exists() else {}


def _prepared_record(message: dict, *, status: str = "prepared", receipt=None, send_attempts=0):
    source_id = message["id"]
    return {
        "version": 2,
        "source_id": source_id,
        "source": dict(message),
        "source_fingerprint": _reply_source_fingerprint(message),
        "ack_ownership": legacy_ack_ownership(),
        "arguments": {
            "to": "hadi-codex",
            "body": "reply body",
            "kind": "ack",
            "request_id": _final_request_id(source_id),
            "in_reply_to": source_id,
        },
        "status": status,
        "receipt": receipt,
        "send_attempts": send_attempts,
    }


class AlwaysFailingSendClient(ExpiryDriverClient):
    """Every peer `send` call raises -- models a persistently unreachable
    peer, distinct from `ExpiryDriverClient`'s ordinary succeeding `send`."""

    async def call(self, tool, arguments):
        if tool == "send":
            self.calls.append((tool, dict(arguments)))
            self.sent.append(arguments)
            raise RuntimeError("peer send failed (mupot 503)")
        return await super().call(tool, arguments)


# ---------------------------------------------------------------------------
# The custody predicate itself
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,receipt,expect_custody",
    [
        ("prepared", None, False),
        ("sent", {"id": "out-1"}, False),
        ("custodied", None, False),
        ("custodied", {"id": "out-1"}, True),
        ("complete", {"id": "out-1"}, True),
        ("reconciliation_required", {"id": "out-1"}, False),
    ],
)
async def test_reply_outbox_has_custody_predicate(
    tmp_path: Path, status: str, receipt, expect_custody: bool
) -> None:
    """`_reply_outbox_has_custody` -- the guard `_resolve_turn_failure` uses
    to decide whether `pending` may stay held -- fires ONLY for
    `"custodied"`/`"complete"` WITH a receipt. Neither `"prepared"` (zero
    custody, never transmitted) nor `"sent"` (peer confirmed, local
    `enqueue()` not yet run) qualifies."""
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    adapter._state["reply_outbox"] = {
        message["id"]: _prepared_record(message, status=status, receipt=receipt)
    }
    assert adapter._reply_outbox_has_custody(message["id"]) is expect_custody
    await adapter.cancel_background_tasks()


# ---------------------------------------------------------------------------
# P0: the pending guard itself (kills M7 -- reverting `_reply_outbox_has_
# custody` back to `staged.get("status") != "complete"` must turn this red)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["no_custody", "handler_error", "turn_timeout"])
async def test_resolve_turn_failure_never_holds_pending_for_a_prepared_reply(
    tmp_path: Path, kind: str
) -> None:
    """P0 fix, direct call: a `"prepared"` (zero-custody, never-transmitted)
    staged reply must NOT hold `pending` for ANY turn-failure `kind` -- the
    pre-fix predicate (`status != "complete"`) held it for this shape
    exactly as it did for a genuinely `"custodied"` one, which is what let
    `_replay_reply_outbox` retry a doomed `send` forever before ever
    reaching `inbox_lease` again."""
    state_path = tmp_path / "state.json"
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    adapter._state["pending"] = {"message": message}
    adapter._state["reply_outbox"] = {message["id"]: _prepared_record(message)}
    adapter.store.save(adapter._state)

    with pytest.raises(_TurnFailureDeferred):
        await adapter._resolve_turn_failure(message, None, kind)

    st = _state(state_path)
    assert st.get("pending") is None, (
        f"kind={kind}: a zero-custody `prepared` reply must never hold "
        "`pending` -- LOW-1's docstring claim that the guard is a no-op "
        "for no_custody/turn_timeout is only true under this predicate"
    )
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_resolve_turn_failure_still_holds_pending_for_custodied_reply(
    tmp_path: Path,
) -> None:
    """Countercase: the round-4 guard this fix narrows is not dead -- a
    genuinely `"custodied"` staged reply (WITH a receipt) still holds
    `pending`, below the cap, same as before."""
    state_path = tmp_path / "state.json"
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    adapter._state["pending"] = {"message": message}
    adapter._state["reply_outbox"] = {
        message["id"]: _prepared_record(message, status="custodied", receipt={"id": "out-1"})
    }
    adapter.store.save(adapter._state)

    with pytest.raises(_TurnFailureDeferred):
        await adapter._resolve_turn_failure(message, None, "handler_error")

    st = _state(state_path)
    assert st.get("pending") is not None
    await adapter.cancel_background_tasks()


# ---------------------------------------------------------------------------
# Athena MED 3a: at the cap, a custodied reply is Delivered, not DLQ'd
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_turn_failure_at_cap_with_custody_never_dlqs(tmp_path: Path) -> None:
    """`turn_failure_dlq` must never name a message that is actually
    Delivered -- at the cap, a custodied reply gets NO `dlq` row (the
    pre-fix code appended one unconditionally)."""
    state_path = tmp_path / "state.json"
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    message_at_cap = dict(message, delivery_attempts=adapter.max_delivery_attempts)
    adapter._state["pending"] = {"message": message_at_cap}
    adapter._state["reply_outbox"] = {
        message_at_cap["id"]: _prepared_record(
            message_at_cap, status="custodied", receipt={"id": "out-1"}
        )
    }
    adapter.store.save(adapter._state)

    with pytest.raises(_TurnFailureDeferred) as excinfo:
        await adapter._resolve_turn_failure(message_at_cap, None, "handler_error")
    assert excinfo.value.terminal is True

    st = _state(state_path)
    assert st.get("dlq") in (None, []), (
        "a custodied reply at the cap is Delivered -- it must never appear "
        "in turn_failure_dlq"
    )
    assert st.get("pending") is not None
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_turn_failure_dlq_summary_excludes_processed_ids(tmp_path: Path) -> None:
    """Defense-in-depth on the read side: even if a stale `dlq` row somehow
    survives for a message that IS in `processed`, the operator-facing
    summary must never claim a delivered message failed."""
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    adapter._state["dlq"] = [{"message": dict(message), "reason": "no_custody"}]
    adapter._state["processed"] = [message["id"]]
    assert adapter.turn_failure_dlq_summary() == []

    adapter._state["processed"] = []
    assert adapter.turn_failure_dlq_summary() == [
        {"source_id": message["id"], "reason": "no_custody"}
    ]
    await adapter.cancel_background_tasks()


# ---------------------------------------------------------------------------
# P0 continued: `_replay_reply_outbox`'s own bound on a `"prepared"` reply
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replay_reply_outbox_bounds_prepared_send_failures_then_dlqs(
    tmp_path: Path,
) -> None:
    """The doomed `send` for a `"prepared"` reply must not retry forever --
    below the cap it persists a growing local `send_attempts` counter and
    leaves the record `"prepared"` for the next tick; at the cap it DLQs
    (`reply_send_failed`), drops the orphaned record, and clears `pending`
    -- all WITHOUT raising, so the caller (`_poll_loop`) proceeds straight
    to `inbox_lease` on the very next statement instead of looping here."""
    state_path = tmp_path / "state.json"
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = AlwaysFailingSendClient(message)
    adapter = make_adapter(tmp_path, client)
    adapter._state["reply_outbox"] = {message["id"]: _prepared_record(message)}
    adapter._state["pending"] = None
    adapter.store.save(adapter._state)

    for attempt in range(1, adapter.max_delivery_attempts):
        await adapter._replay_reply_outbox()  # must never raise below the cap
        st = _state(state_path)
        record = st.get("reply_outbox", {}).get(message["id"])
        assert record is not None, f"attempt {attempt}: record dropped before the cap"
        assert record["status"] == "prepared"
        assert record["send_attempts"] == attempt
        assert st.get("dlq") in (None, []), f"attempt {attempt}: DLQ'd before the cap"

    # The cap-th attempt: DLQ, drop, clear -- and still no raise.
    await adapter._replay_reply_outbox()
    st = _state(state_path)
    assert message["id"] not in st.get("reply_outbox", {}), (
        "an exhausted, zero-custody reply must be dropped -- leaving it "
        "`prepared` with `pending` cleared is exactly the orphan shape the "
        "mismatch check would otherwise escalate to reconciliation_required"
    )
    dlq = st.get("dlq") or []
    assert len(dlq) == 1 and dlq[0]["reason"] == "reply_send_failed"
    assert dlq[0]["message"]["id"] == message["id"]
    assert st.get("pending") is None
    assert len(client.sent) == adapter.max_delivery_attempts

    # And the poll loop really does resume leasing: one more tick leases
    # fresh work with nothing left to block it.
    await adapter._replay_reply_outbox()  # empty outbox now -- a true no-op
    await adapter.cancel_background_tasks()


class RollingLiveLeaseFailingSendClient(RollingLiveLeaseClient):
    """`RollingLiveLeaseClient`'s realistic one-outstanding-lease-at-a-time,
    incrementing-`delivery_attempts` redelivery model, but the peer `send`
    always fails -- so the staged reply can never gain custody, and BOTH
    bounding mechanisms (the message's own `delivery_attempts`, via
    `_resolve_turn_failure`'s cap; and `_bound_reply_send_failure`'s own
    local `send_attempts` counter) are exercised on the SAME record."""

    async def call(self, tool, arguments):
        if tool == "send":
            self.calls.append((tool, dict(arguments)))
            self.sent.append(arguments)
            raise RuntimeError("peer send failed (mupot 503)")
        return await super().call(tool, arguments)


@pytest.mark.asyncio
async def test_full_poll_loop_stays_alive_and_converges_when_reply_send_always_fails(
    tmp_path: Path,
) -> None:
    """End-to-end (real `_poll_loop`, real redelivery, real Hermes send-retry
    wrapper), not just direct calls: a successful handler whose reply can
    never be sent must not wedge the poll loop -- the pre-fix P0 symptom was
    exactly this shape (one lease ever, `mupot_gateway_status` reporting
    healthy). Whichever bounding mechanism gets there first (the message's
    own `delivery_attempts` cap, or `_bound_reply_send_failure`'s local
    counter), the system must reach a terminal, DLQ'd, non-quarantined
    disposition and the poll loop must still be alive and leasing
    afterwards -- never fatal, never a durable `lease_reconciliation`."""
    state_path = tmp_path / "state.json"
    client = RollingLiveLeaseFailingSendClient(
        dict(PEER_MSG, lease_expires_at=_iso_in(30))
    )
    # A low cap keeps this real, real-time-backoff-driven scenario (Hermes's
    # own `_send_with_retry` adds several real seconds per redelivery cycle
    # before this adapter ever sees the failure) to the minimum number of
    # cycles that still proves the property.
    adapter = make_adapter(tmp_path, client, extra={"max_delivery_attempts": 2})
    client.dead_letter_after = adapter.max_delivery_attempts + 2

    async def handler(_event):
        return "reply content"

    adapter.set_message_handler(handler)
    assert await adapter.connect()
    try:
        assert await _await_until(
            lambda: bool(_state(state_path).get("dlq")),
            n=3000,
        ), "the doomed reply never reached a terminal DLQ disposition"
        await asyncio.sleep(0.2)
        st = _state(state_path)
        assert st.get("lease_reconciliation") is None, (
            "a bounded, DLQ'd turn failure must never durably quarantine"
        )
        assert adapter._lease_quarantined is False
        assert adapter.has_fatal_error is False
        assert not adapter._poll_task.done(), "poll loop died on a bounded reply failure"
        dlq = st.get("dlq") or []
        assert len(dlq) == 1
        assert dlq[0]["reason"] in {"handler_error", "reply_send_failed"}
        assert st.get("processed") in (None, []), "never delivered -- must not be processed"
    finally:
        await adapter.disconnect()


# ---------------------------------------------------------------------------
# MED-1 ("pin the reader", M3): the mismatch escalation still functions for
# a genuinely custodied (non-"prepared") orphaned record.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replay_reply_outbox_still_escalates_mismatched_custodied_record(
    tmp_path: Path,
) -> None:
    """A `"custodied"` (non-`"prepared"`) record whose `pending` does not
    name it is the genuine ambiguous-crash shape -- `_replay_reply_outbox`
    must still flip it to `reconciliation_required` and raise. This is the
    property `_replay_reply_outbox`'s round-5 `if record["status"] ==
    "prepared":` branch deliberately does NOT exempt: only a zero-custody
    reply gets the new bounded-retry treatment."""
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    other_message = dict(PEER_MSG, id="m-other", lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    adapter._state["pending"] = {"message": other_message}  # names a DIFFERENT source
    # `_replay_reply_outbox` validates the FULL record schema (unlike
    # `_reply_outbox_has_custody`, which only checks status+receipt shape),
    # so this needs a receipt that actually satisfies `validate_send_
    # receipt` for the record's own `arguments`.
    full_receipt = {
        "id": "out-1", "seq": 100, "duplicate": False,
        "to": "hadi-codex", "project_id": None, "target_seat": None,
    }
    adapter._state["reply_outbox"] = {
        message["id"]: _prepared_record(message, status="custodied", receipt=full_receipt)
    }
    adapter.store.save(adapter._state)

    with pytest.raises(MupotProtocolError):
        await adapter._replay_reply_outbox()

    st = _state(adapter.store.path)
    assert (
        st.get("reply_outbox", {}).get(message["id"], {}).get("status")
        == "reconciliation_required"
    )
    assert adapter._reply_reconciliation_required is True
    await adapter.cancel_background_tasks()


# ---------------------------------------------------------------------------
# Athena MED 3b/3c: bound LOCAL re-execution even against a server that
# never reaps.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_leased_message_skips_a_locally_dead_lettered_id(
    tmp_path: Path,
) -> None:
    """Direct call: once a message id carries one of this class's own `dlq`
    rows, `_process_leased_message` must skip it -- no handler invocation,
    no ack -- rather than run another turn."""
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    adapter._state["dlq"] = [{"message": dict(message), "reason": "no_custody"}]
    adapter.store.save(adapter._state)

    handled: list[str] = []

    async def handler(event):
        handled.append(event.message_id)
        return "should never run"

    adapter.set_message_handler(handler)

    with pytest.raises(_TurnFailureDeferred) as excinfo:
        await adapter._process_leased_message(message, attempt_id=None)
    assert excinfo.value.terminal is True
    assert handled == [], "a locally dead-lettered message must not get another turn"
    assert client.calls == [], "must not ack a locally dead-lettered message either"
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_local_turns_bounded_against_a_server_that_never_reaps(
    tmp_path: Path,
) -> None:
    """Full poll loop, real bound: a server that keeps re-leasing the SAME
    message past `max_delivery_attempts` forever (never reaps it) must not
    get unbounded local re-execution -- exactly ONE more turn happens after
    the cap is first reached (the at-cap turn itself), never a second."""
    state_path = tmp_path / "state.json"
    client = RollingLiveLeaseClient(dict(PEER_MSG, lease_expires_at=_iso_in(30)))
    # dead_letter_after left None: this server NEVER reaps -- it keeps
    # granting fresh leases with a climbing delivery_attempts forever.
    adapter = make_adapter(tmp_path, client)
    handled: list[str] = []

    async def empty_handler(event):
        handled.append(event.message_id)
        return ""

    adapter.set_message_handler(empty_handler)
    assert await adapter.connect()
    try:
        assert await _await_until(
            lambda: len(handled) >= adapter.max_delivery_attempts, n=1000
        )
        # Give the non-reaping server plenty more chances to redeliver.
        await asyncio.sleep(0.3)
        assert len(handled) == adapter.max_delivery_attempts, (
            "local re-execution is unbounded against a non-conforming "
            "server that never reaps -- measured 258 turns pre-fix"
        )
        st = _state(state_path)
        dlq = st.get("dlq") or []
        assert len(dlq) == 1 and dlq[0]["reason"] == "no_custody"
        assert not adapter._poll_task.done()
        assert adapter.has_fatal_error is False
    finally:
        await adapter.disconnect()

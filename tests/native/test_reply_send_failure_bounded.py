"""Round 6 class fix (2026-09-15, PR #9, head e0556d61 -> this round): STOP
adding write paths. A stuck outbound reply must never BLOCK inbound leasing
and must never be repaired by REWRITING custody records.

Round 5 (head c9b79aa -> e0556d61) fixed the P0 that `pending` was held for
a `"prepared"` (zero-custody, never-transmitted) reply exactly as it was for
a genuinely `"custodied"` one, and added `_bound_reply_send_failure` to
bound `_replay_reply_outbox`'s own retries of a doomed `send` with a local
`send_attempts` counter -- but at the cap it DLQ'd the message and DROPPED
the `reply_outbox` record, and on ANY exception (including one raised well
AFTER a successful `send`, e.g. `notifications.enqueue` failing post-
confirmation) it blind-wrote `dict(record)` -- the CALLER's loop-top
snapshot -- back over the durable row, silently regressing an already-
delivered `"sent"`/receipt-bearing reply back to `"prepared"`.

Round 6 fixes, all pinned below:

**NEW-1 (P0):** `_bound_reply_send_failure` re-reads the DURABLE record
before doing anything. If it already advanced past `"prepared"` (the
transmit landed; something else raised afterward), it does nothing. Below
the cap it increments `send_attempts` via `_persist_reply_record` (load_
checked + readback), record stays `"prepared"`. At the cap, status
transitions to `"send_failed"` IN PLACE -- receipt untouched (always
`None`), record KEPT (never popped, never DLQ'd) -- visible via
`mupot_gateway_status`'s new `stuck_replies` field for operator
reconciliation.

**Item 2 / rule:** `_replay_reply_outbox` never auto-retries a
`"send_failed"` record (skipped outright) and never raises/stalls the poll
tick for a `"prepared"`/`"send_failed"` record -- at most one send attempt
per tick, then the poll loop always proceeds to `_flush_notifications`/
`inbox_lease` the same tick.

**NEW-3:** the unconditional `_preflight_persisted_ownership` before ANY
`"prepared"`-record transmit (round-4 commit 6547fd8 semantics) is
restored -- round 5 had gated it on `pending_matches`, letting a reply
whose ownership no longer checks out transmit to the peer before ownership
validation whenever `pending` had already been cleared (the COMMON shape
for a `"prepared"` retry).

**NEW-4:** `_process_leased_message`'s local-turn-failure-dlq skip now runs
BELOW the `processed` re-ack short-circuit (an id in both could never be
re-acked otherwise), and its own evidence lives in a dedicated,
independently-capped `turn_failure_ids` set (`_mark_local_turn_failure`/
`_has_local_turn_failure_dlq`) instead of the tail of the shared
`dlq[-100:]` window, so 100+ later, unrelated DLQ rows can never evict it.

**Athena gate addendum (folded into this round, head e0556d61):** ONE
custody definition -- `_reply_outbox_has_custody` now accepts `"sent"` too
(a validated peer `send` receipt on file), not just `"custodied"`/
`"complete"` -- merged with the removed `_reply_has_human_custody`, which
additionally required local notification durability. That extra
requirement was itself a live P0: a handler that succeeded, whose reply the
peer's own `send` CONFIRMED, whose only failure was the local
`notifications.enqueue` call afterward, reported "no custody" for a reply
the peer already had -- `pending` got cleared, and the next tick's
mismatch escalation turned that into a fatal, `connect()`-refusing state.
`_transmit_final_reply`'s own `"sent"` branch now bounds a failing
`enqueue()` internally (logged, never raised) instead of propagating, and
`_replay_reply_outbox`'s else-branch (reached only for `"sent"`/
`"custodied"`, both of which `_validated_reply_record` already requires a
receipt for) no longer escalates a mismatched `pending` at all -- it always
already has custody by construction.
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pytest

from gateway.config import PlatformConfig

import plugin.mupot_gateway.adapter as adapter_module
from plugin.mupot_gateway.adapter import (
    MupotAdapter,
    MupotProtocolError,
    _final_request_id,
    _reply_source_fingerprint,
    _TurnFailureDeferred,
)
from plugin.mupot_gateway.lease_ownership import attempt_ack_ownership, legacy_ack_ownership

# The native test runner (`scripts/test-native.sh`) can execute each file in
# its own subprocess/worker (see `run_tests_parallel.py`), so cross-test-file
# imports are not reliable here -- the small amount of shared fixture
# infrastructure from `test_lease_expiry_deferred.py`/`test_reply_protocol.py`
# (client fakes, the `_await_until` poller, `make_adapter`, `ScopeOwner`) is
# duplicated below rather than imported, matching those files' own
# self-contained style.

FAKE_SCOPE = {"tenant": "tenant-a", "agent_id": "agent-consumer", "effective_inbox_seat": None}
EXPIRED_LEASE = "2000-01-01T00:00:00.000Z"
FRESH_LEASE = "2099-01-01T00:00:00.000Z"
OWNER_FINGERPRINT = "a" * 64
ATTEMPT_ID = "attempt-aaaaaaaaaaaaaaaa"

PEER_MSG = {
    "id": "m-peer", "seq": 7, "from_agent": "hadi-codex", "from_member": "member-code",
    "body": "full Mupot answer", "project_id": "project-1", "request_id": "req-7",
    "in_reply_to": None, "kind": "message", "created_at": "2026-09-13T00:00:00.000Z",
    "delivery_attempts": 1, "lease_expires_at": EXPIRED_LEASE,
}


class ScopeOwner:
    """Deterministic `secret_owner` -- see `test_reply_protocol.py`'s
    identically-named class for the full rationale (duplicated here since
    cross-test-file imports are not reliable under the native runner)."""

    def __init__(self, fingerprint: str) -> None:
        self.fingerprint = fingerprint

    def validated_fingerprint(self) -> str:
        return self.fingerprint

    @contextmanager
    def activate(self):
        yield


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


def _attempt_ownership(attempt_id: str = ATTEMPT_ID) -> dict:
    """A valid `"attempt"`-kind ack_ownership matching `FAKE_SCOPE` and
    `OWNER_FINGERPRINT` -- passes `_preflight_persisted_ownership` against
    any fake client below whose `inbox_consumer_status` echoes `FAKE_SCOPE`
    (all of them), when the adapter was built with `owner=ScopeOwner(
    OWNER_FINGERPRINT)` (see `make_adapter`)."""
    return attempt_ack_ownership({
        "attempt_id": attempt_id,
        "tenant": FAKE_SCOPE["tenant"],
        "agent_id": FAKE_SCOPE["agent_id"],
        "effective_inbox_seat": FAKE_SCOPE["effective_inbox_seat"],
        "mode": "bearer_only",
        "generation": 0,
        "profile_owner_fingerprint": OWNER_FINGERPRINT,
    })


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


def make_adapter(
    tmp_path: Path, client: ExpiryDriverClient, *, extra=None, owner=None
) -> MupotAdapter:
    ex = {
        "allowed_agents": "hadi-codex",
        "poll_interval": 0.01,
        "state_path": str(tmp_path / "state.json"),
    }
    ex.update(extra or {})
    return MupotAdapter(
        PlatformConfig(enabled=True, typing_indicator=False, extra=ex),
        client_factory=lambda *_: client,
        secret_owner=owner,
    )


async def _await_until(pred, n=300, dt=0.01) -> bool:
    for _ in range(n):
        if pred():
            return True
        await asyncio.sleep(dt)
    return False


def _state(state_path: Path) -> dict:
    return json.loads(state_path.read_text()) if state_path.exists() else {}


def _prepared_record(
    message: dict,
    *,
    status: str = "prepared",
    receipt=None,
    send_attempts=0,
    ack_ownership=None,
):
    source_id = message["id"]
    return {
        "version": 2,
        "source_id": source_id,
        "source": dict(message),
        "source_fingerprint": _reply_source_fingerprint(message),
        "ack_ownership": ack_ownership if ack_ownership is not None else legacy_ack_ownership(),
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
# The custody predicate itself (Athena gate addendum: ONE definition)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,receipt,expect_custody",
    [
        ("prepared", None, False),
        ("sent", {"id": "out-1"}, True),
        ("custodied", None, False),
        ("custodied", {"id": "out-1"}, True),
        ("complete", {"id": "out-1"}, True),
        ("reconciliation_required", {"id": "out-1"}, False),
        ("send_failed", None, False),
    ],
)
async def test_reply_outbox_has_custody_predicate(
    tmp_path: Path, status: str, receipt, expect_custody: bool
) -> None:
    """`_reply_outbox_has_custody` -- the ONE merged custody definition
    (Athena gate addendum, head e0556d61) -- fires for `"sent"`/
    `"custodied"`/`"complete"` WITH a receipt. Neither `"prepared"` (zero
    custody, never transmitted) nor `"send_failed"` (permanently unsendable,
    round 6) ever carries a receipt, so neither ever qualifies. Mutate
    `"sent"` back out of the accepted status set -> this case turns red,
    pinning the merge (round-5's predicate reported `False` for `"sent"`,
    which was itself the P0 the addendum closed)."""
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    adapter._state["reply_outbox"] = {
        message["id"]: _prepared_record(message, status=status, receipt=receipt)
    }
    assert adapter._reply_outbox_has_custody(message["id"]) is expect_custody
    await adapter.cancel_background_tasks()


# ---------------------------------------------------------------------------
# M7 pin (item 6): `_resolve_turn_failure`'s custody predicate, isolated
# from `_bound_reply_send_failure` -- the two are DIFFERENT methods reached
# from DIFFERENT call sites, but the isolation is made explicit here (stub
# `_bound_reply_send_failure` to fail loudly if ever called) so a future
# change that couples them cannot silently break this pin's guarantee.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["no_custody", "handler_error", "turn_timeout"])
async def test_resolve_turn_failure_never_holds_pending_for_a_prepared_reply(
    tmp_path: Path, kind: str
) -> None:
    """P0 fix, direct call, ISOLATED from `_bound_reply_send_failure` (item
    6, M7 pin): a `"prepared"` (zero-custody, never-transmitted) staged
    reply must NOT hold `pending` for ANY turn-failure `kind` -- the pre-fix
    predicate (`status != "complete"`) held it for this shape exactly as it
    did for a genuinely `"custodied"` one, which is what let
    `_replay_reply_outbox` retry a doomed `send` forever before ever
    reaching `inbox_lease` again. Mutate the predicate back to `!=
    "complete"` -> this turns red."""
    state_path = tmp_path / "state.json"
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    adapter._state["pending"] = {"message": message}
    adapter._state["reply_outbox"] = {message["id"]: _prepared_record(message)}
    adapter.store.save(adapter._state)

    async def _never_called(*_a, **_k):
        raise AssertionError(
            "_resolve_turn_failure's custody predicate must be isolated from "
            "_bound_reply_send_failure -- this is a DIFFERENT call site "
            "(_replay_reply_outbox's own try/except), never reached from here"
        )

    adapter._bound_reply_send_failure = _never_called  # type: ignore[method-assign]

    with pytest.raises(_TurnFailureDeferred):
        await adapter._resolve_turn_failure(message, None, kind)

    st = _state(state_path)
    assert st.get("pending") is None, (
        f"kind={kind}: a zero-custody `prepared` reply must never hold "
        "`pending` -- the guard is a no-op for no_custody/turn_timeout only "
        "under this predicate"
    )
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["sent", "custodied"])
async def test_resolve_turn_failure_still_holds_pending_for_a_receipted_reply(
    tmp_path: Path, status: str,
) -> None:
    """Countercase: the round-4 guard this fix narrows is not dead -- a
    staged reply that already holds a validated receipt (`"sent"` OR
    `"custodied"` -- Athena gate addendum: BOTH count as custody now, not
    just `"custodied"`) still holds `pending`, below the cap, same as
    before."""
    state_path = tmp_path / "state.json"
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    adapter._state["pending"] = {"message": message}
    adapter._state["reply_outbox"] = {
        message["id"]: _prepared_record(message, status=status, receipt={"id": "out-1"})
    }
    adapter.store.save(adapter._state)

    with pytest.raises(_TurnFailureDeferred):
        await adapter._resolve_turn_failure(message, None, "handler_error")

    st = _state(state_path)
    assert st.get("pending") is not None
    await adapter.cancel_background_tasks()


# ---------------------------------------------------------------------------
# Athena MED 3a: at the cap, a receipted reply is Delivered, not DLQ'd
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["sent", "custodied"])
async def test_resolve_turn_failure_at_cap_with_custody_never_dlqs(
    tmp_path: Path, status: str,
) -> None:
    """`turn_failure_dlq` must never name a message that is actually
    Delivered -- at the cap, a receipted reply (`"sent"` or `"custodied"`)
    gets NO `dlq` row (the pre-fix code appended one unconditionally)."""
    state_path = tmp_path / "state.json"
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    message_at_cap = dict(message, delivery_attempts=adapter.max_delivery_attempts)
    adapter._state["pending"] = {"message": message_at_cap}
    adapter._state["reply_outbox"] = {
        message_at_cap["id"]: _prepared_record(
            message_at_cap, status=status, receipt={"id": "out-1"}
        )
    }
    adapter.store.save(adapter._state)

    with pytest.raises(_TurnFailureDeferred) as excinfo:
        await adapter._resolve_turn_failure(message_at_cap, None, "handler_error")
    assert excinfo.value.terminal is True

    st = _state(state_path)
    assert st.get("dlq") in (None, []), (
        "a receipted reply at the cap is Delivered -- it must never appear "
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
# NEW-1 (P0): `_bound_reply_send_failure` never overwrites a landed send
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bound_reply_send_failure_does_not_overwrite_a_landed_send(
    tmp_path: Path,
) -> None:
    """The round-5 P0: `_bound_reply_send_failure` used to blind-write
    `dict(record)` -- the CALLER's loop-top snapshot -- over the durable
    row on ANY exception. Simulate the exact race: the durable record has
    ALREADY advanced to `"sent"` with a validated receipt (the peer
    `send` succeeded), but the caller still holds a STALE `"prepared"`
    snapshot from before the transmit (e.g. `_replay_reply_outbox`'s
    loop-top `record`, now stale because `_transmit_final_reply` updated
    the DURABLE state out from under it before some later step raised).
    `_bound_reply_send_failure` must re-read the durable record, see it
    already advanced, and do NOTHING -- never regress `"sent"`+receipt
    back to `"prepared"`+`None`. Mutate the re-read away (use the stale
    `record` argument directly instead) -> this turns red."""
    state_path = tmp_path / "state.json"
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    source_id = message["id"]
    landed_receipt = {
        "id": "out-1", "seq": 100, "duplicate": False,
        "to": "hadi-codex", "project_id": None, "target_seat": None,
    }
    durable_record = _prepared_record(message, status="sent", receipt=landed_receipt)
    adapter._state["reply_outbox"] = {source_id: durable_record}
    adapter.store.save(adapter._state)

    stale_prepared_snapshot = _prepared_record(message, status="prepared", receipt=None)
    await adapter._bound_reply_send_failure(
        source_id, stale_prepared_snapshot, RuntimeError("disk full during notification enqueue")
    )

    st = _state(state_path)
    record = st.get("reply_outbox", {}).get(source_id)
    assert record is not None, "the landed record must never be dropped"
    assert record["status"] == "sent", "must never regress a landed send back to prepared"
    assert record["receipt"] == landed_receipt, "the receipt must survive untouched"
    await adapter.cancel_background_tasks()


# ---------------------------------------------------------------------------
# NEW-1 continued / item 2: `_replay_reply_outbox`'s own bound on a
# `"prepared"` reply -- below the cap: local counter. At the cap:
# `"send_failed"` IN PLACE, never DLQ'd, never dropped, never auto-retried.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replay_reply_outbox_bounds_prepared_send_failures_then_marks_send_failed(
    tmp_path: Path,
) -> None:
    """The doomed `send` for a `"prepared"` reply must not retry forever --
    below the cap it persists a growing local `send_attempts` counter and
    leaves the record `"prepared"` for the next tick; at the cap it
    transitions the record to `"send_failed"` IN PLACE (receipt untouched,
    record KEPT -- never DLQ'd, never dropped) -- all WITHOUT raising, so
    the caller (`_poll_loop`) proceeds straight to `inbox_lease` on the very
    next statement instead of looping here. A FURTHER replay tick must not
    attempt another send at all (never auto-retried)."""
    state_path = tmp_path / "state.json"
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = AlwaysFailingSendClient(message)
    adapter = make_adapter(tmp_path, client, owner=ScopeOwner(OWNER_FINGERPRINT))
    adapter._state["reply_outbox"] = {
        message["id"]: _prepared_record(message, ack_ownership=_attempt_ownership())
    }
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

    # The cap-th attempt: status -> send_failed IN PLACE. Still no raise.
    await adapter._replay_reply_outbox()
    st = _state(state_path)
    record = st.get("reply_outbox", {}).get(message["id"])
    assert record is not None, (
        "an exhausted reply must be KEPT, not dropped -- round 6 replaces "
        "the round-5 DLQ+drop with an in-place status transition so an "
        "operator can see and reconcile it"
    )
    assert record["status"] == "send_failed"
    assert record["receipt"] is None, "it never reached the peer -- receipt stays None"
    assert record["send_attempts"] == adapter.max_delivery_attempts
    assert st.get("dlq") in (None, []), "round 6: send_failed is never DLQ'd"
    assert st.get("pending") is None
    assert len(client.sent) == adapter.max_delivery_attempts
    assert adapter.stuck_replies() == [
        {"source_id": message["id"], "send_attempts": adapter.max_delivery_attempts}
    ]

    # Never auto-retried: one more tick must not attempt another send.
    await adapter._replay_reply_outbox()
    assert len(client.sent) == adapter.max_delivery_attempts, (
        "a send_failed record must never be retried automatically"
    )
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
    healthy). Whichever bounding mechanism gets there first -- the message's
    own `delivery_attempts` cap (-> a `dlq` row) or `_bound_reply_send_
    failure`'s local counter (-> `"send_failed"` in place, round 6: no `dlq`
    row for this path anymore) -- the system must reach a terminal,
    non-quarantined disposition and the poll loop must still be alive and
    leasing afterwards -- never fatal, never a durable `lease_
    reconciliation`."""
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
        def _terminal() -> bool:
            st = _state(state_path)
            if st.get("dlq"):
                return True
            return any(
                isinstance(r, dict) and r.get("status") == "send_failed"
                for r in st.get("reply_outbox", {}).values()
            )

        assert await _await_until(_terminal, n=3000), (
            "the doomed reply never reached a terminal disposition"
        )
        await asyncio.sleep(0.2)
        st = _state(state_path)
        assert st.get("lease_reconciliation") is None, (
            "a bounded turn failure must never durably quarantine"
        )
        assert adapter._lease_quarantined is False
        assert adapter.has_fatal_error is False
        assert not adapter._poll_task.done(), "poll loop died on a bounded reply failure"
        dlq = st.get("dlq") or []
        if dlq:
            assert len(dlq) == 1
            assert dlq[0]["reason"] in {"handler_error", "no_custody"}, (
                "round 6: reply_send_failed is a status transition "
                "(send_failed), never a dlq reason anymore"
            )
        assert st.get("processed") in (None, []), "never delivered -- must not be processed"
    finally:
        await adapter.disconnect()


# ---------------------------------------------------------------------------
# Athena gate addendum: the else-branch tolerates a receipted record whose
# `pending` doesn't match it -- it already has custody by construction, so
# it completes normally instead of escalating to reconciliation_required.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replay_reply_outbox_tolerates_mismatched_receipted_record_and_completes(
    tmp_path: Path,
) -> None:
    """Round 6 (Athena gate addendum, head e0556d61): a `"custodied"` (or
    `"sent"`) record whose `pending` does not name it used to be escalated
    to `reconciliation_required` (round 5) -- but `_validated_reply_record`
    already REQUIRES a validated receipt for both statuses, so this shape
    means the peer already has the reply regardless of what `pending` says.
    The live P0 this closed: a confirmed `"sent"` reply, `pending` cleared
    by an unrelated no-custody classification elsewhere, escalated on the
    very next tick to a FATAL, `connect()`-refusing state. `_replay_reply_
    outbox` must instead complete it via the ordinary path: ack, commit,
    mark complete -- never raise. Mutate the tolerance back out (escalate
    whenever `pending` doesn't match) -> this turns red."""
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    other_message = dict(PEER_MSG, id="m-other", lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client, owner=ScopeOwner(OWNER_FINGERPRINT))
    adapter._state["pending"] = {"message": other_message}  # names a DIFFERENT source
    full_receipt = {
        "id": "out-1", "seq": 100, "duplicate": False,
        "to": "hadi-codex", "project_id": None, "target_seat": None,
    }
    adapter._state["reply_outbox"] = {
        message["id"]: _prepared_record(
            message, status="custodied", receipt=full_receipt,
            ack_ownership=_attempt_ownership(),
        )
    }
    adapter.store.save(adapter._state)

    await adapter._replay_reply_outbox()  # must NOT raise

    st = _state(adapter.store.path)
    assert st.get("reply_outbox", {}).get(message["id"], {}).get("status") == "complete"
    assert message["id"] in (st.get("processed") or [])
    assert adapter._reply_reconciliation_required is False
    assert client.ack_calls(), "the source must actually be acked, not just left custodied"
    await adapter.cancel_background_tasks()


# ---------------------------------------------------------------------------
# NEW-3: the ownership preflight runs before EVERY prepared-record transmit,
# unconditionally (round-4 commit 6547fd8 semantics) -- not gated on
# `pending_matches`, which is the COMMON shape for a bounded retry.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replay_reply_outbox_preflights_ownership_before_every_prepared_send(
    tmp_path: Path,
) -> None:
    """A `"prepared"` record with `ack_ownership` that no longer checks out
    (here: `"attempt"` kind at all, which `require_attempt=True` demands,
    against a record that only carries legacy ownership) must be refused
    BEFORE any peer `send` is attempted -- regardless of whether `pending`
    matches this source (it does not, here: `pending` is `None`, the
    common shape for a `"prepared"` retry). Mutate the preflight call back
    to `if pending_matches: ...` (round 5) -> this turns red (the send
    would go through since `pending` never matches a `"prepared"` retry)."""
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    adapter._state["pending"] = None
    adapter._state["reply_outbox"] = {
        message["id"]: _prepared_record(message)  # legacy_ack_ownership() by default
    }
    adapter.store.save(adapter._state)

    with pytest.raises(MupotProtocolError):
        await adapter._replay_reply_outbox()

    assert client.sent == [], "ownership must be validated before ANY peer send"
    await adapter.cancel_background_tasks()


# ---------------------------------------------------------------------------
# NEW-4: the local-turn-failure-dlq skip runs BELOW the `processed` re-ack
# short-circuit, and its evidence survives well past `dlq[-100:]`'s own
# eviction window.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_leased_message_skips_a_locally_dead_lettered_id(
    tmp_path: Path,
) -> None:
    """Direct call: once a message id carries local turn-failure evidence,
    `_process_leased_message` must skip it -- no handler invocation, no ack
    -- rather than run another turn."""
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    adapter._state["dlq"] = [{"message": dict(message), "reason": "no_custody"}]
    adapter._mark_local_turn_failure(message["id"])
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
async def test_process_leased_message_reacks_when_both_processed_and_locally_dead_lettered(
    tmp_path: Path,
) -> None:
    """NEW-4 P3 pin: an id in BOTH `processed` (it later completed through
    the ordinary reply-replay flow) AND carrying local turn-failure evidence
    from an EARLIER attempt (`dlq` rows are not retroactively removed on
    later success) must still be RE-ACKED on redelivery -- with the skip
    checked BEFORE the `processed` short-circuit (round 5's order), such an
    id could NEVER be re-acked: every redelivery hit the skip branch, which
    raises without acking, forever. Mutate the order back (skip before
    processed) -> this turns red."""
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    adapter._state["processed"] = [message["id"]]
    adapter._state["dlq"] = [{"message": dict(message), "reason": "turn_timeout"}]
    adapter._mark_local_turn_failure(message["id"])
    adapter.store.save(adapter._state)

    handled: list[str] = []

    async def handler(event):
        handled.append(event.message_id)
        return "should never run"

    adapter.set_message_handler(handler)

    # Must NOT raise -- the processed short-circuit re-acks normally.
    await adapter._process_leased_message(message, attempt_id=None)
    assert handled == [], "an already-processed message must not get another turn either"
    assert client.ack_calls(), "an id in both processed and dlq must still be re-acked"
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_has_local_turn_failure_dlq_survives_100_plus_later_dlq_rows(
    tmp_path: Path,
) -> None:
    """NEW-4: the skip's own evidence must survive 100+ LATER, unrelated
    `dlq` rows -- the round-5 predicate scanned `dlq[-100:]` directly, so
    150 later rows for other messages evicted the original row from that
    window and silently reopened unbounded local re-execution. Mutate
    `_has_local_turn_failure_dlq` back to a pure `dlq` scan (drop the
    `turn_failure_ids` check) -> this turns red."""
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    adapter._mark_local_turn_failure(message["id"])
    # 150 unrelated later DLQ rows -- more than enough to evict `message`'s
    # own row from a bare `dlq[-100:]` scan (it's now row 0 of 151).
    dlq = [{"message": dict(message), "reason": "no_custody"}]
    for i in range(150):
        other = dict(message, id=f"m-other-{i}")
        dlq.append({"message": other, "reason": "no_custody"})
    adapter._state["dlq"] = dlq[-100:]  # what every real write site would leave on disk
    adapter.store.save(adapter._state)

    assert adapter._has_local_turn_failure_dlq(message["id"]) is True, (
        "150+ later dlq rows must not evict this id's local-turn-failure "
        "evidence -- it lives in the independently-capped turn_failure_ids "
        "set, not the dlq[-100:] window"
    )
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


# ---------------------------------------------------------------------------
# NEW-5: the per-tick WARN is rate-limited, not re-logged every tick.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_warn_rate_limited_suppresses_repeats_within_the_window(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    """NEW-5 (LOW from the round-5 gate: ~1,226 WARNING lines in 6 seconds
    against a non-reaping fake peer that re-leases the same terminal
    message every poll tick). `_warn_rate_limited` must log at most once per
    window per key, then log again once the window elapses. Mutate it to
    always log (drop the rate limit) -> this turns red."""
    message = dict(PEER_MSG, lease_expires_at=_iso_in(30))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    caplog.set_level(logging.WARNING, logger=adapter_module.logger.name)

    for _ in range(20):
        adapter._warn_rate_limited("test-key", "repeated warning %s", "x", window=60.0)
    assert len(caplog.records) == 1, (
        "20 calls within the window must produce exactly one log line"
    )

    caplog.clear()
    adapter._warn_rate_limited("test-key", "repeated warning %s", "x", window=0.0)
    assert len(caplog.records) == 1, "a zero window must always log again"
    await adapter.cancel_background_tasks()

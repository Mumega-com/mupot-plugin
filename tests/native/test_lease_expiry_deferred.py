"""A turn ending without custody is a deferral, not a violation.

Live incident (4 bricks, 2026-09-15, master 6c86c2b0): a turn outlives the
lease -> `_deliver` returns silently at either expiry checkpoint ->
`_poll_loop` reads "message not in processed" as a protocol violation ->
`_quarantine_inbox_polling()` -> a durable `lease_reconciliation` marker
refuses `connect()` across every subsequent restart, because nothing ever
called `reconcile_inbox_polling()` for it.

Round 2 (Athena BLOCK, 2026-09-15): round 1's fix covered only the message's
OWN lease expiring. Athena executed the class against this PR's OWN shipped
17:06 incident fixture and found a SECOND, un-fixed instance of it:
`turn_timeout` firing first while that same lease was still live is "turn
ended without custody" exactly the same way -- round 1 explicitly declared it
out of scope ("keeps base behaviour"), but master and round 1 are IDENTICAL
on that branch, and it is what the 17:06 fixture actually shows. Both
deferral reasons (`lease_expired`, `turn_timeout` -- see `_DeliveryDeferred`)
are covered here now.

Covers items 1-6 of the minimal fix (plus the turn_timeout branch above)
through the REAL `_poll_loop`/`connect()`/`reconcile_inbox_polling()`
machinery wherever practical -- direct method calls only where a full
poll-loop drive would duplicate existing coverage (see
tests/native/test_lease_attempt_reconciliation.py, unchanged) or depend on
Hermes internals outside this module's scope (handler_error bounding --
explicitly NOT this PR's class; local delivery-attempt bounding -- issue #10,
also explicitly not this PR's class, the pot's own reaper bounds instead).
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from gateway.config import PlatformConfig

from plugin.mupot_gateway.adapter import (
    MupotAdapter,
    MupotProtocolError,
    StateStore,
    _DeliveryDeferred,
    _LeaseExpiredDeferred,
    _configured_mcp_tool_timeout,
    _final_request_id,
    _reply_source_fingerprint,
)
from plugin.mupot_gateway.lease_ownership import legacy_ack_ownership


def reply_record(source_id: str, status: str, receipt: dict | None = None) -> dict[str, Any]:
    source = {"id": source_id}
    return {
        "version": 2,
        "source_id": source_id,
        "source": source,
        "source_fingerprint": _reply_source_fingerprint(source),
        "ack_ownership": legacy_ack_ownership(),
        "arguments": {
            "to": "hadi-codex", "body": "x", "kind": "ack",
            "request_id": _final_request_id(source_id), "in_reply_to": source_id,
        },
        "status": status,
        "receipt": receipt,
    }


SCOPE = {
    "tenant": "tenant-a",
    "agent_id": "agent-consumer",
    "effective_inbox_seat": "seat-a",
}
STATUS = {"strict_scope": True, **SCOPE, "mode": "bearer_only", "generation": 0, "key_matches": True}


def far_future() -> str:
    return "2099-01-01T00:00:00.000Z"


def message_at(
    message_id: str,
    delivery_attempts: int,
    lease_expires_at: str,
    *,
    body: str = "run this turn",
) -> dict[str, Any]:
    return {
        "id": message_id,
        "seq": 1,
        "delivery_attempts": delivery_attempts,
        "lease_expires_at": lease_expires_at,
        "from_agent": "hadi-codex",
        "from_member": "member-code",
        "kind": "message",
        "body": body,
        "created_at": "2026-09-15T00:00:00.000Z",
    }


def attempt_result(attempt_id: str, state: str, messages: list | None = None,
                    lease_expires_at: str | None = None) -> dict[str, Any]:
    return {
        **SCOPE,
        "attempt_id": attempt_id,
        "state": state,
        "lease_expires_at": lease_expires_at,
        "messages": messages or [],
        "consumed": False,
    }


class RedeliveringLeaseClient:
    """Real redelivery: `lease_plan[n]` governs the (n+1)th `inbox_lease` call.

    Each plan entry is `(lease_expires_at, delivery_attempts)`; the SAME
    message id/body is redelivered with a fresh lease each time, exactly as
    Mupot's own server would after a lease expires unconsumed.
    """

    def __init__(self, lease_plan: list[tuple[str, int]], message_id: str = "msg-1") -> None:
        self.lease_plan = lease_plan
        self.message_id = message_id
        self.lease_calls = 0
        self.acked_attempt_ids: list[str] = []
        self.sent: list[dict[str, Any]] = []

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if tool == "inbox_consumer_status":
            return dict(STATUS)
        if tool == "inbox_lease":
            attempt_id = arguments["attempt_id"]
            if self.lease_calls >= len(self.lease_plan):
                return attempt_result(attempt_id, "empty")
            lease_expires_at, attempts = self.lease_plan[self.lease_calls]
            self.lease_calls += 1
            message = message_at(self.message_id, attempts, lease_expires_at)
            return attempt_result(attempt_id, "leased", [message], lease_expires_at)
        if tool == "inbox_lease_ack":
            self.acked_attempt_ids.append(arguments["attempt_id"])
            return {**SCOPE, "attempt_id": arguments["attempt_id"], "state": "acked", "consumed": True}
        if tool == "send":
            self.sent.append(arguments)
            return {"id": "delivery-1", "seq": 1, "duplicate": False,
                     "to": arguments["to"], "project_id": arguments.get("project_id")}
        raise AssertionError(f"unexpected tool: {tool} {arguments}")


def make_adapter(state_path: Path, client: Any, **extra: Any) -> MupotAdapter:
    return MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "allowed_agents": "hadi-codex",
                "poll_interval": 0.01,
                "rpc_timeout": 5,
                "state_path": str(state_path),
                **extra,
            },
        ),
        client_factory=lambda *_: client,
    )


async def wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition never became true")


# ---------------------------------------------------------------------------
# Item 1: lease expiry at both _deliver sites is a deferral, not a violation.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pre_turn_lease_expiry_defers_then_redelivers_and_completes_once(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    already_expired = "2020-01-01T00:00:00.000Z"
    client = RedeliveringLeaseClient([(already_expired, 1), (far_future(), 2)])
    adapter = make_adapter(state_path, client)
    handled: list[str] = []

    async def handler(event: Any) -> None:
        handled.append(event.message_id)
        await adapter.send(event.source.chat_id, "ok")

    adapter.set_message_handler(handler)
    assert await adapter.connect() is True
    try:
        await wait_until(lambda: client.lease_calls >= 2)
        await wait_until(lambda: handled == ["msg-1"])
        await wait_until(lambda: "msg-1" in StateStore(state_path).load().get("processed", []))
    finally:
        await adapter.disconnect()

    assert handled == ["msg-1"]  # never ran the model on the expired attempt
    assert len(client.acked_attempt_ids) == 1  # exactly one ack, for the successful redelivery
    state = StateStore(state_path).load()
    assert "msg-1" in state["processed"]  # completed exactly once, via redelivery
    assert state.get("lease_reconciliation") is None  # never quarantined


@pytest.mark.asyncio
async def test_mid_poll_lease_expiry_logs_expiry_not_estop(
    tmp_path: Path, caplog: Any,
) -> None:
    """F4 (kasra-review re-gate #1, 2026-09-15): `_poll_loop`'s mid-poll
    `except _EstopDeferred` also absorbs `_LeaseExpiredDeferred` (a
    subclass) -- it must log the true cause, not always name the e-stop."""
    state_path = tmp_path / "state.json"
    already_expired = "2020-01-01T00:00:00.000Z"
    client = RedeliveringLeaseClient([(already_expired, 1), (far_future(), 2)])
    adapter = make_adapter(state_path, client)

    async def handler(event: Any) -> None:
        await adapter.send(event.source.chat_id, "ok")

    adapter.set_message_handler(handler)
    with caplog.at_level(logging.INFO, logger="plugin"):
        assert await adapter.connect() is True
        try:
            await wait_until(lambda: client.lease_calls >= 2)
            await wait_until(
                lambda: "msg-1" in StateStore(state_path).load().get("processed", [])
            )
        finally:
            await adapter.disconnect()

    messages = [r.getMessage() for r in caplog.records]
    mid_poll = [m for m in messages if "deferring leased message=msg-1 mid-poll" in m]
    assert mid_poll, "expected a mid-poll deferral log"
    assert "its own lease expired" in mid_poll[0]
    assert "emergency stop" not in mid_poll[0]


@pytest.mark.asyncio
async def test_mid_poll_turn_timeout_logs_turn_timeout_not_lease_expired(
    tmp_path: Path, caplog: Any,
) -> None:
    """Round 2 companion to the lease-expiry case above: the SAME mid-poll
    catch site, for the turn_timeout-with-a-live-lease reason -- must name
    that reason specifically, not the generic "its own lease expired" text
    (both raise the same `_DeliveryDeferred` class, but the alias-based
    `isinstance` check round 1 used could not tell them apart; `.reason`
    can)."""
    state_path = tmp_path / "state.json"
    client = RedeliveringLeaseClient([(far_future(), 1), (far_future(), 2)])
    adapter = make_adapter(state_path, client)
    adapter.turn_timeout = 0.05  # bypass __init__'s 10.0 floor for a fast test
    stuck = asyncio.Event()
    handled: list[str] = []

    async def handler(event: Any) -> None:
        handled.append(event.message_id)
        if len(handled) == 1:
            await stuck.wait()
            return
        await adapter.send(event.source.chat_id, "ok")

    adapter.set_message_handler(handler)
    with caplog.at_level(logging.INFO, logger="plugin"):
        assert await adapter.connect() is True
        try:
            await wait_until(lambda: client.lease_calls >= 2)
            stuck.set()
            await wait_until(
                lambda: "msg-1" in StateStore(state_path).load().get("processed", [])
            )
        finally:
            stuck.set()
            await adapter.disconnect()

    messages = [r.getMessage() for r in caplog.records]
    mid_poll = [m for m in messages if "deferring leased message=msg-1 mid-poll" in m]
    assert mid_poll, "expected a mid-poll deferral log"
    assert "its own turn_timeout with a still-live lease" in mid_poll[0]
    assert "lease expired" not in mid_poll[0]
    assert "emergency stop" not in mid_poll[0]


@pytest.mark.asyncio
async def test_mid_poll_estop_pause_logs_the_true_cause(
    tmp_path: Path, caplog: Any,
) -> None:
    """F4 companion: the SAME mid-poll catch site, for the OTHER cause (a
    real e-stop pause racing in after a successful lease, no lease expiry
    involved at all) -- must still say "emergency stop", not "lease
    expired"."""
    import hermes_constants
    from agent import estop as real_estop

    # Isolate the ESTOP sentinel to this test's own tmp_path -- the default
    # HERMES_HOME is shared by every test file's subprocess under the
    # parallel runner, and an un-isolated engage()/disengage() here raced
    # with an unrelated, concurrently-running file's own estop assertions
    # in CI.
    home = tmp_path / "hermes-home"
    home.mkdir(exist_ok=True)
    home_token = hermes_constants.set_hermes_home_override(str(home))

    state_path = tmp_path / "state.json"

    class OneShotPausingClient:
        def __init__(self) -> None:
            self.lease_calls = 0

        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
            if tool == "inbox_consumer_status":
                return dict(STATUS)
            if tool == "inbox_lease":
                attempt_id = arguments["attempt_id"]
                self.lease_calls += 1
                if self.lease_calls == 1:
                    real_estop.engage(reason="f4-companion-test")
                    return attempt_result(
                        attempt_id,
                        "leased",
                        [message_at("msg-1", 1, far_future())],
                        far_future(),
                    )
                return attempt_result(attempt_id, "empty")
            raise AssertionError(f"unexpected tool: {tool} {arguments}")

    assert real_estop.is_engaged() is False
    client = OneShotPausingClient()
    adapter = make_adapter(state_path, client)
    try:
        with caplog.at_level(logging.INFO, logger="plugin"):
            assert await adapter.connect() is True
            await wait_until(lambda: client.lease_calls >= 1)
            await asyncio.sleep(0.1)  # let the mid-poll deferral log land
    finally:
        real_estop.disengage()
        await adapter.disconnect()
        hermes_constants.reset_hermes_home_override(home_token)

    messages = [r.getMessage() for r in caplog.records]
    mid_poll = [m for m in messages if "deferring leased message=msg-1 mid-poll" in m]
    assert mid_poll, "expected a mid-poll deferral log"
    assert "Hermes global emergency stop is engaged" in mid_poll[0]
    assert "lease expired" not in mid_poll[0]


@pytest.mark.asyncio
async def test_post_timeout_lease_expiry_defers_then_redelivers_and_completes_once(
    tmp_path: Path, caplog: Any,
) -> None:
    """Also pins three mutation survivors from the adversarial gate (PR #11
    round 3, 2026-09-15) -- all three change what `reason` computes to for a
    GENUINELY lease-driven timeout (as opposed to `turn_timeout`-with-a-
    live-lease, covered by the sibling test above): M2 forces
    `lease_expired` to always read False; M3 forces `reason` to always
    resolve `"turn_timeout"`; M13 forces `_delivery_deadline`'s returned
    lease deadline to always be `None` (which also makes `_lease_has_expired`
    always False). Any of the three flips the mid-poll log from "its own
    lease expired" to "its own turn_timeout with a still-live lease" for
    this exact scenario, where the lease -- not `turn_timeout` (30s here) --
    is what actually fired."""
    state_path = tmp_path / "state.json"
    from datetime import datetime, timedelta, timezone

    soon_expired = (datetime.now(timezone.utc) + timedelta(milliseconds=300)).isoformat()
    client = RedeliveringLeaseClient([(soon_expired, 1), (far_future(), 2)])
    adapter = make_adapter(state_path, client, turn_timeout=30)
    handled: list[str] = []
    stuck = asyncio.Event()

    async def handler(event: Any) -> None:
        handled.append(event.message_id)
        if len(handled) == 1:
            await stuck.wait()  # block past the lease's own short deadline once
            return
        await adapter.send(event.source.chat_id, "ok")

    adapter.set_message_handler(handler)
    with caplog.at_level(logging.INFO, logger="plugin"):
        assert await adapter.connect() is True
        try:
            await wait_until(lambda: client.lease_calls >= 2)
            stuck.set()
            await wait_until(lambda: "msg-1" in StateStore(state_path).load().get("processed", []))
        finally:
            stuck.set()
            await adapter.disconnect()

    state = StateStore(state_path).load()
    assert "msg-1" in state["processed"]
    assert state.get("lease_reconciliation") is None
    assert len(client.acked_attempt_ids) == 1  # deferral itself never acked; redelivery did once

    messages = [r.getMessage() for r in caplog.records]
    mid_poll = [m for m in messages if "deferring leased message=msg-1 mid-poll" in m]
    assert mid_poll, "expected a mid-poll deferral log"
    assert "its own lease expired" in mid_poll[0]
    assert "turn_timeout" not in mid_poll[0]


@pytest.mark.asyncio
async def test_pre_turn_expiry_clears_pending_when_no_reply_staged(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    adapter = make_adapter(state_path, object())
    already_expired = "2020-01-01T00:00:00.000Z"

    async def handler(_event: Any) -> None:
        raise AssertionError("must not run the model on an already-expired lease")

    adapter.set_message_handler(handler)
    with pytest.raises(_LeaseExpiredDeferred):
        await adapter._deliver(message_at("msg-1", 1, already_expired))

    state = StateStore(state_path).load()
    assert state["pending"] is None
    assert state.get("lease_reconciliation") is None
    assert state.get("processed", []) == []


@pytest.mark.asyncio
async def test_pre_turn_expiry_preserves_pending_when_a_reply_is_staged(tmp_path: Path) -> None:
    """A background handler may have already staged a validated receipt for
    a DIFFERENT, earlier delivery attempt of the same source before this
    exact attempt's own lease expired -- pending must not be cleared out
    from under that in-flight custody."""
    state_path = tmp_path / "state.json"
    adapter = make_adapter(state_path, object())
    adapter._state["reply_outbox"] = {
        "msg-1": reply_record(
            "msg-1", "sent",
            receipt={"id": "d-1", "seq": 1, "duplicate": False, "to": "hadi-codex", "project_id": None},
        ),
    }
    adapter.store.save(adapter._state)
    already_expired = "2020-01-01T00:00:00.000Z"

    async def handler(_event: Any) -> None:
        raise AssertionError("must not run the model on an already-expired lease")

    adapter.set_message_handler(handler)
    with pytest.raises(_LeaseExpiredDeferred):
        await adapter._deliver(message_at("msg-1", 1, already_expired))

    state = StateStore(state_path).load()
    assert state["pending"]["message"]["id"] == "msg-1"  # left untouched, not cleared


@pytest.mark.asyncio
async def test_pre_turn_expiry_preserves_pending_for_a_never_transmitted_prepared_reply(
    tmp_path: Path,
) -> None:
    """Athena F1 (PR #11 round 2, 2026-09-15): round 1's deferral sites used
    `_has_validated_reply_receipt` (status in {sent, custodied, complete} +
    a receipt) to decide whether to clear `pending` -- narrower than
    `_staged_reply_blocks_reconcile`'s self-heal check, which blocks on ANY
    non-complete staged record, "prepared" included. A never-transmitted
    "prepared" record (no receipt yet -- a background handler mid-flight
    preparing the final reply) did NOT block round 1's clearing, so a
    deferral could drop `pending` right out from under it. Both call sites
    now share `_reply_staged_incomplete` and agree: "prepared" blocks too.
    """
    state_path = tmp_path / "state.json"
    adapter = make_adapter(state_path, object())
    adapter._state["reply_outbox"] = {"msg-1": reply_record("msg-1", "prepared")}
    adapter.store.save(adapter._state)
    already_expired = "2020-01-01T00:00:00.000Z"

    async def handler(_event: Any) -> None:
        raise AssertionError("must not run the model on an already-expired lease")

    adapter.set_message_handler(handler)
    with pytest.raises(_LeaseExpiredDeferred):
        await adapter._deliver(message_at("msg-1", 1, already_expired))

    state = StateStore(state_path).load()
    assert state["pending"]["message"]["id"] == "msg-1"  # left untouched, not cleared


@pytest.mark.asyncio
async def test_turn_timeout_with_live_lease_defers_not_returns(tmp_path: Path) -> None:
    """Round 2 (Athena BLOCK on PR #11, 2026-09-15): this branch was
    explicitly OUT of round 1's class ("keeps base behaviour" -- returns
    silently, pending intact) -- but the PR's OWN shipped 17:06 incident
    fixture (`tests/fixtures/state.json.bak-quarantine-20260915170649`) is
    exactly this shape: `lease_expires_at` 41s AFTER the quarantine
    snapshot's own mtime, i.e. `turn_timeout` (300s, smaller than
    `lease_seconds`) fired first while the lease was still live. Master and
    round 1 are IDENTICAL there: `_deliver` returns silently ->
    `_poll_loop`'s "message not in processed" reads that as a protocol
    violation -> durable quarantine. Turn ended without custody is turn
    ended without custody regardless of which deadline fired -- this must
    now defer, the same as a lease expiry, not silently return."""
    state_path = tmp_path / "state.json"
    adapter = make_adapter(state_path, object())
    # __init__ floors turn_timeout at 10.0 -- set it directly, post-construction,
    # to get a genuinely short timeout for this test (same technique
    # test_adapter.py's own turn-timeout tests already use).
    adapter.turn_timeout = 0.05

    async def handler(_event: Any) -> None:
        await asyncio.Event().wait()

    adapter.set_message_handler(handler)
    with pytest.raises(_DeliveryDeferred) as exc_info:
        await adapter._deliver(message_at("msg-1", 1, far_future()))
    assert exc_info.value.reason == "turn_timeout"

    state = StateStore(state_path).load()
    assert state["pending"] is None  # no reply staged for it -- safe to clear
    assert state.get("lease_reconciliation") is None  # never quarantined
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_turn_timeout_with_live_lease_defers_then_redelivers_and_completes_once(
    tmp_path: Path,
) -> None:
    """The same class, through the REAL `_poll_loop`/redelivery machinery
    (not just a direct `_deliver` call) -- the end-to-end proof the
    lease-expiry siblings above already have. Mirrors the PR's own shipped
    17:06 fixture's shape: the lease stays live throughout, `turn_timeout`
    is what fires."""
    state_path = tmp_path / "state.json"
    client = RedeliveringLeaseClient([(far_future(), 1), (far_future(), 2)])
    adapter = make_adapter(state_path, client)
    # __init__ floors turn_timeout at 10.0 -- set it directly, post-
    # construction, so the FIRST delivery attempt's turn_timeout genuinely
    # fires (short) well before its handler ever resolves and lets
    # `on_processing_complete` set `completion_event` on its own.
    adapter.turn_timeout = 0.05
    handled: list[str] = []
    stuck = asyncio.Event()

    async def handler(event: Any) -> None:
        handled.append(event.message_id)
        if len(handled) == 1:
            await stuck.wait()  # outlive turn_timeout while the lease stays live
            return
        await adapter.send(event.source.chat_id, "ok")

    adapter.set_message_handler(handler)
    assert await adapter.connect() is True
    try:
        await wait_until(lambda: client.lease_calls >= 2)
        stuck.set()
        await wait_until(lambda: "msg-1" in StateStore(state_path).load().get("processed", []))
    finally:
        stuck.set()
        await adapter.disconnect()

    state = StateStore(state_path).load()
    assert "msg-1" in state["processed"]
    assert state.get("lease_reconciliation") is None  # never quarantined
    assert len(client.acked_attempt_ids) == 1  # deferral itself never acked; redelivery did once


# ---------------------------------------------------------------------------
# Round 4 (adversarial BLOCK-2, PR #11 round 3, 2026-09-15): the remaining
# 3 of 5 `_deliver` exits that end a turn without human custody -- until now
# these returned silently and were read as a protocol violation by
# `_poll_loop`'s "message not in processed" check, the same incident class
# as the two above, unfixed on three branches.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_output_defers_then_redelivers_and_completes_once(
    tmp_path: Path,
) -> None:
    """Exit 1 of 5: `outcome == SUCCESS` but the handler produced no reply at
    all (Hermes scores that as a successful no-op) -- turn ended without
    custody exactly as much as a lease expiry or a turn timeout, not a
    protocol violation."""
    state_path = tmp_path / "state.json"
    client = RedeliveringLeaseClient([(far_future(), 1), (far_future(), 2)])
    adapter = make_adapter(state_path, client)
    handled: list[str] = []

    async def handler(event: Any) -> None:
        handled.append(event.message_id)
        if len(handled) == 1:
            return  # produce no reply at all
        await adapter.send(event.source.chat_id, "ok")

    adapter.set_message_handler(handler)
    assert await adapter.connect() is True
    try:
        await wait_until(lambda: "msg-1" in StateStore(state_path).load().get("processed", []))
    finally:
        await adapter.disconnect()

    assert handled == ["msg-1", "msg-1"]  # redelivered exactly once
    state = StateStore(state_path).load()
    assert "msg-1" in state["processed"]
    assert state.get("lease_reconciliation") is None  # never quarantined
    assert len(client.acked_attempt_ids) == 1  # deferral itself never acked; redelivery did once


@pytest.mark.asyncio
async def test_handler_error_defers_then_redelivers_and_completes_once(
    tmp_path: Path,
) -> None:
    """Exit 2 of 5: the handler raising is the fall-through FAILURE outcome
    -- turn ended without custody, not a protocol violation. Hermes itself
    catches the handler's exception (`handle_message`'s own
    `except BaseException`), so this never propagates out of `_deliver`
    directly; it surfaces only via the FAILURE outcome `_deliver` observes
    on `runtime.outcome`."""
    state_path = tmp_path / "state.json"
    client = RedeliveringLeaseClient([(far_future(), 1), (far_future(), 2)])
    adapter = make_adapter(state_path, client)
    handled: list[str] = []

    async def handler(event: Any) -> None:
        handled.append(event.message_id)
        if len(handled) == 1:
            raise RuntimeError("simulated handler crash")
        await adapter.send(event.source.chat_id, "ok")

    adapter.set_message_handler(handler)
    assert await adapter.connect() is True
    try:
        await wait_until(lambda: "msg-1" in StateStore(state_path).load().get("processed", []))
    finally:
        await adapter.disconnect()

    assert handled == ["msg-1", "msg-1"]  # redelivered exactly once
    state = StateStore(state_path).load()
    assert "msg-1" in state["processed"]
    assert state.get("lease_reconciliation") is None  # never quarantined
    assert len(client.acked_attempt_ids) == 1  # deferral itself never acked; redelivery did once


@pytest.mark.asyncio
async def test_runtime_invalidated_preserves_pending_when_a_reply_is_staged(
    tmp_path: Path,
) -> None:
    """Exit 3 of 5: `runtime.invalidated` with a non-SUCCESS outcome
    (reachable from `disconnect()` mid-turn -- see test_adapter.py's
    `test_disconnect_invalidates_generation_before_surviving_callback` for
    the no-reply-staged, pending-cleared case) shares the SAME
    `_reply_staged_incomplete` gate as the other four exits: a runtime
    invalidated mid-turn must not clear `pending` out from under a reply a
    background handler already staged for a DIFFERENT, earlier attempt of
    the same source.
    """
    state_path = tmp_path / "state.json"
    adapter = make_adapter(state_path, object())
    adapter._state["reply_outbox"] = {
        "msg-1": reply_record(
            "msg-1", "sent",
            receipt={"id": "d-1", "seq": 1, "duplicate": False, "to": "hadi-codex", "project_id": None},
        ),
    }
    adapter.store.save(adapter._state)

    async def handler(_event: Any) -> None:
        await asyncio.Event().wait()

    adapter.set_message_handler(handler)
    delivery = asyncio.create_task(adapter._deliver(message_at("msg-1", 1, far_future())))
    await wait_until(lambda: bool(adapter._live_generations))
    for runtime in list(adapter._live_generations.values()):
        adapter._invalidate_delivery(runtime)  # simulate disconnect()'s own call
    with pytest.raises(_DeliveryDeferred) as exc_info:
        await asyncio.wait_for(delivery, 1)
    assert exc_info.value.reason == "runtime_invalidated"

    state = StateStore(state_path).load()
    assert state["pending"]["message"]["id"] == "msg-1"  # left untouched, not cleared
    await adapter.cancel_background_tasks()


# ---------------------------------------------------------------------------
# Item 2: _replay_reply_outbox tolerates a cleared pending for a validated
# receipt (the sent + pending-cleared-by-a-deferral scenario).
# ---------------------------------------------------------------------------


class SendThenEnqueueFailsClient:
    def __init__(self) -> None:
        self.acked_ids: list[str] = []
        self.sent: list[dict[str, Any]] = []

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if tool == "inbox_ack":
            message_id = arguments["ids"][0]
            self.acked_ids.append(message_id)
            return {"acked": [message_id], "already_read": [], "refused": []}
        if tool == "send":
            self.sent.append(arguments)
            return {"id": "delivery-1", "seq": 1, "duplicate": False,
                     "to": arguments["to"], "project_id": arguments.get("project_id")}
        raise AssertionError(f"unexpected tool: {tool}")


@pytest.mark.asyncio
async def test_sent_reply_with_cleared_pending_completes_via_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from plugin.mupot_gateway import notifications as notifications_module

    state_path = tmp_path / "state.json"
    client = SendThenEnqueueFailsClient()
    adapter = make_adapter(state_path, client, allowed_agents="sender")
    message = {
        "id": "source-1", "seq": 7, "tenant": "tenant-test", "to_agent": "receiver",
        "target_seat": "native", "from_agent": "sender", "from_member": "member-sender",
        "kind": "request", "body": "Please complete the task.", "request_id": "request-1",
        "in_reply_to": None, "created_at": "2026-09-15T00:00:00.000Z", "project_id": "project-1",
        "fenced_delivery_id": "lease-1", "body_length": 25, "checksum_sha256": "a" * 64,
        "is_intact": True, "expects_reply": True, "reply_basis": "request_id_field",
        "delivery_attempts": 1, "lease_expires_at": far_future(),
    }
    event, _runtime = adapter._begin_delivery(message)
    await adapter.on_processing_start(event)

    real_enqueue = notifications_module.enqueue
    calls = {"n": 0}

    def flaky_enqueue(*args: Any, **kwargs: Any) -> Any:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("simulated crash between send() and enqueue()")
        return real_enqueue(*args, **kwargs)

    monkeypatch.setattr(notifications_module, "enqueue", flaky_enqueue)
    result = await adapter.send("sender", "Immutable final.")
    assert result.success is False  # enqueue's exception surfaced, not swallowed

    record = adapter._state["reply_outbox"]["source-1"]
    assert record["status"] == "sent"
    assert isinstance(record["receipt"], dict)

    # Simulate item 1: a lease-expiry deferral for this exact source cleared
    # `pending` in between (the "no validated receipt staged" branch would
    # have run before this send() completed; here it runs after, which is
    # exactly the race item 2 exists for).
    adapter._state["pending"] = None
    adapter.store.save(adapter._state)

    await adapter._replay_reply_outbox()  # must NOT raise _protocol_error

    final = StateStore(state_path).load()
    assert final["reply_outbox"]["source-1"]["status"] == "complete"
    assert "source-1" in final["processed"]
    assert client.acked_ids == ["source-1"]


@pytest.mark.asyncio
async def test_prepared_reply_with_mismatched_pending_still_fences(tmp_path: Path) -> None:
    """The opposite of item 2's tolerance: a never-transmitted "prepared"
    record (no validated receipt) with a mismatched pending is genuine crash
    ambiguity and must still escalate -- unchanged from master."""
    state_path = tmp_path / "state.json"
    adapter = make_adapter(state_path, object())
    adapter._state["reply_outbox"] = {"msg-1": reply_record("msg-1", "prepared")}
    adapter._state["pending"] = None
    adapter.store.save(adapter._state)

    with pytest.raises(MupotProtocolError):
        await adapter._replay_reply_outbox()
    assert adapter._state["reply_outbox"]["msg-1"]["status"] == "reconciliation_required"


@pytest.mark.asyncio
async def test_mark_reply_complete_refuses_a_prepared_record_without_a_receipt(
    tmp_path: Path,
) -> None:
    """Athena gate (PR #9 r6, point 1): a "prepared" record whose source_id
    lands in `processed` some other way must never be forced to "complete"
    -- that write persists cleanly but the NEXT load rejects it (a
    "complete" record requires a receipt), bricking connect() invisibly.

    F6 (kasra-review re-gate #1, 2026-09-15): the round-1 fix just refused
    and left the record "prepared" -- every subsequent tick re-walked it
    through this exact same refusal, logging forever. Park it terminal as
    "invalid_receipt" instead: surfaced (`invalid_reply_receipts()`), never
    forced to "complete", and never re-walked again.
    """
    state_path = tmp_path / "state.json"
    adapter = make_adapter(state_path, object())
    adapter._state["reply_outbox"] = {"msg-1": reply_record("msg-1", "prepared")}
    adapter._state["processed"] = ["msg-1"]
    adapter.store.save(adapter._state)

    await adapter._replay_reply_outbox()  # hits the `processed` shortcut branch

    state = StateStore(state_path).load()
    assert state["reply_outbox"]["msg-1"]["status"] == "invalid_receipt"
    assert adapter.invalid_reply_receipts() == ["msg-1"]

    # A fresh load of this exact state must not be poisoned by the refusal.
    fresh = make_adapter(state_path, object())
    assert fresh._reply_state_invalid is False

    # F6: a second replay must not re-walk this record at all -- spy on the
    # persistence call so an idempotent re-write of the same status (which
    # a status-only assertion cannot distinguish from "never re-entered")
    # still fails this test.
    persist_calls: list[str] = []
    original_persist = adapter._persist_reply_record

    def _spy_persist(source_id: str, record: dict[str, Any]) -> dict[str, Any]:
        persist_calls.append(source_id)
        return original_persist(source_id, record)

    adapter._persist_reply_record = _spy_persist  # type: ignore[method-assign]
    await adapter._replay_reply_outbox()
    assert persist_calls == []  # never re-walked -> never re-persisted
    assert (
        StateStore(state_path).load()["reply_outbox"]["msg-1"]["status"]
        == "invalid_receipt"
    )


# ---------------------------------------------------------------------------
# Item 3 + 4: reconcile_inbox_polling's exact-scope clean tombstone, its
# staged-reply countercase, and connect()'s auto-reconcile-before-refusal.
# ---------------------------------------------------------------------------


async def quarantine_via_transport_failure(tmp_path: Path) -> Path:
    from plugin.mupot_gateway.adapter import MupotTransportError

    class FailingLeaseClient:
        def __init__(self) -> None:
            self.first_lease = asyncio.Event()

        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
            if tool == "inbox_consumer_status":
                return dict(STATUS)
            if tool == "inbox_lease":
                self.first_lease.set()
                raise MupotTransportError("Mupot request failed")
            raise AssertionError(f"unexpected tool: {tool}")

    state_path = tmp_path / "state.json"
    client = FailingLeaseClient()
    adapter = make_adapter(state_path, client, lease_seconds=1)
    assert await adapter.connect()
    try:
        await asyncio.wait_for(client.first_lease.wait(), 1)
        await wait_until(lambda: adapter._running is False)
    finally:
        await adapter.disconnect()
    return state_path


@pytest.mark.asyncio
async def test_reconcile_clears_clean_tombstone_with_no_staged_reply(tmp_path: Path) -> None:
    state_path = await quarantine_via_transport_failure(tmp_path)
    attempt_id = StateStore(state_path).load()["lease_reconciliation"]["attempt_id"]

    class ReconcileClient:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
            if tool == "inbox_consumer_status":
                return dict(STATUS)
            if tool == "inbox_lease_reconcile":
                return attempt_result(arguments["attempt_id"], "expired")
            raise AssertionError(f"unexpected tool: {tool}")

    adapter = make_adapter(state_path, ReconcileClient())
    assert await adapter.reconcile_inbox_polling() is True
    assert StateStore(state_path).load().get("lease_reconciliation") is None
    assert StateStore(state_path).load().get("pending") is None
    assert attempt_id  # sanity: a real attempt_id existed


@pytest.mark.asyncio
async def test_reconcile_refuses_when_reply_still_staged_for_pending_source(
    tmp_path: Path,
) -> None:
    state_path = await quarantine_via_transport_failure(tmp_path)

    class ReconcileClient:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
            if tool == "inbox_consumer_status":
                return dict(STATUS)
            if tool == "inbox_lease_reconcile":
                return attempt_result(arguments["attempt_id"], "empty")
            raise AssertionError(f"unexpected tool: {tool}")

    state = StateStore(state_path).load()
    state["pending"] = {"message": {"id": "msg-staged"}}
    state["reply_outbox"] = {
        "msg-staged": reply_record(
            "msg-staged", "sent",
            receipt={"id": "d-1", "seq": 1, "duplicate": False, "to": "hadi-codex", "project_id": None},
        ),
    }
    StateStore(state_path).save(state)

    adapter = make_adapter(state_path, ReconcileClient())
    assert await adapter.reconcile_inbox_polling() is False
    after = StateStore(state_path).load()
    assert after.get("lease_reconciliation") is not None  # still fenced
    assert after["pending"]["message"]["id"] == "msg-staged"  # untouched


@pytest.mark.asyncio
async def test_connect_self_heals_via_reconcile_before_ambiguous_pending_refusal(
    tmp_path: Path,
) -> None:
    """Item 4: connect() attempts the bounded self-heal even though the
    quarantine marker is present -- proven end to end via connect() itself,
    not just reconcile_inbox_polling()."""
    state_path = await quarantine_via_transport_failure(tmp_path)

    class ReconcileClient:
        def __init__(self) -> None:
            self.tools: list[str] = []

        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
            self.tools.append(tool)
            if tool == "inbox_consumer_status":
                return dict(STATUS)
            if tool == "inbox_lease_reconcile":
                return attempt_result(arguments["attempt_id"], "cancelled")
            if tool == "inbox_lease":
                return attempt_result(arguments["attempt_id"], "empty")
            raise AssertionError(f"unexpected tool: {tool}")

    client = ReconcileClient()
    adapter = make_adapter(state_path, client)
    try:
        assert await adapter.connect() is True
    finally:
        await adapter.disconnect()
    assert client.tools[:2] == ["inbox_consumer_status", "inbox_lease_reconcile"]
    assert StateStore(state_path).load().get("lease_reconciliation") is None


@pytest.mark.asyncio
async def test_genuine_protocol_violation_still_quarantines(tmp_path: Path) -> None:
    """Widening the deferred-exception net (item 1) must not swallow a REAL
    protocol violation unrelated to lease expiry."""
    state_path = tmp_path / "state.json"

    class MalformedAckClient:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
            if tool == "inbox_consumer_status":
                return dict(STATUS)
            if tool == "inbox_lease":
                attempt_id = arguments["attempt_id"]
                message = message_at("msg-1", 1, far_future())
                return attempt_result(attempt_id, "leased", [message], far_future())
            if tool == "inbox_lease_ack":
                return {"state": "not-a-real-state"}  # malformed -> _protocol_error
            if tool == "send":
                return {"id": "delivery-1", "seq": 1, "duplicate": False,
                         "to": arguments["to"], "project_id": arguments.get("project_id")}
            raise AssertionError(f"unexpected tool: {tool}")

    client = MalformedAckClient()
    adapter = make_adapter(state_path, client)

    async def handler(event: Any) -> None:
        await adapter.send(event.source.chat_id, "ok")

    adapter.set_message_handler(handler)
    assert await adapter.connect() is True
    try:
        await wait_until(lambda: adapter._running is False)
    finally:
        await adapter.disconnect()

    state = StateStore(state_path).load()
    assert state.get("lease_reconciliation") is not None
    assert "msg-1" not in state.get("processed", [])


# ---------------------------------------------------------------------------
# Item 5: lease sizing formula, config injected -- never reads the host.
# ---------------------------------------------------------------------------


def test_lease_seconds_defaults_to_turn_timeout_plus_mcp_tool_timeout_plus_60(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "plugin.mupot_gateway.adapter._configured_mcp_tool_timeout",
        lambda server_name: 180.0,
    )
    adapter = make_adapter(tmp_path / "state.json", object(), turn_timeout=300)
    assert adapter.lease_seconds == 300 + 180 + 60


def test_lease_seconds_clamps_to_server_ceiling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "plugin.mupot_gateway.adapter._configured_mcp_tool_timeout",
        lambda server_name: 100000.0,
    )
    adapter = make_adapter(tmp_path / "state.json", object(), turn_timeout=300)
    assert adapter.lease_seconds == 3600


def test_lease_seconds_explicit_config_bypasses_the_formula(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "plugin.mupot_gateway.adapter._configured_mcp_tool_timeout",
        lambda server_name: (_ for _ in ()).throw(AssertionError("must not be called")),
    )
    adapter = make_adapter(tmp_path / "state.json", object(), lease_seconds=42)
    assert adapter.lease_seconds == 42


def test_configured_mcp_tool_timeout_never_reads_host_falls_back_on_any_error() -> None:
    # No mupot MCP server configured under this test's isolated environment --
    # must fall back to the documented default rather than raise or hang.
    assert _configured_mcp_tool_timeout("mupot-server-not-configured") == 300.0


@pytest.mark.parametrize(
    "bad_timeout", [float("nan"), float("inf"), float("-inf"), 0.0, -5.0]
)
def test_configured_mcp_tool_timeout_rejects_non_finite_or_non_positive(
    bad_timeout: float, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F5 (kasra-review re-gate #1, 2026-09-15): a NaN/inf/non-positive
    configured timeout must fall back to the default instead of reaching
    `int(requested_lease)` in `__init__` and crashing it."""
    import hermes_cli.config as hermes_config
    import hermes_cli.mcp_config as hermes_mcp_config

    monkeypatch.setattr(
        hermes_config, "load_config",
        lambda: {"mcp_servers": {"srv": {"timeout": "irrelevant"}}},
    )
    monkeypatch.setattr(
        hermes_mcp_config, "_resolve_mcp_server_config",
        lambda raw_cfg: {"timeout": bad_timeout},
    )
    assert _configured_mcp_tool_timeout("srv") == 300.0


# ---------------------------------------------------------------------------
# Item 6: mupot_gateway_status reports lease_reconciliation + connected.
# ---------------------------------------------------------------------------


def test_lease_reconciliation_status_reports_marker_and_attempt_id(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    adapter = make_adapter(state_path, object())
    assert adapter.lease_reconciliation_status() == {"required": False, "attempt_id": None}

    adapter._state["lease_reconciliation"] = {
        "version": 3, "required": True, **SCOPE, "mode": "bearer_only", "generation": 0,
        "profile_owner_fingerprint": "a" * 64, "attempt_id": "attempt-id-1234567890ab",
    }
    assert adapter.lease_reconciliation_status() == {
        "required": True,
        "attempt_id": "attempt-id-1234567890ab",
    }


@pytest.mark.asyncio
async def test_reply_reconciliation_required_reports_true_through_replay(
    tmp_path: Path,
) -> None:
    """M10 (adversarial gate, PR #11 round 3, 2026-09-15): the mutation
    survivor forces `reply_reconciliation_required()` to always return
    False. The existing `mupot_gateway_status` regression test only ever
    exercises the False case (a fresh adapter with no reconciliation-needed
    record), so that mutation survived undetected -- this drives the SAME
    public method to True through the real `_replay_reply_outbox` path that
    sets the underlying flag.
    """
    state_path = tmp_path / "state.json"
    adapter = make_adapter(state_path, object())
    adapter._state["reply_outbox"] = {
        "msg-1": reply_record("msg-1", "reconciliation_required"),
    }
    adapter.store.save(adapter._state)

    assert adapter.reply_reconciliation_required() is False  # not yet replayed
    with pytest.raises(MupotProtocolError):
        await adapter._replay_reply_outbox()
    assert adapter.reply_reconciliation_required() is True


def test_delivery_deferred_reason_is_validated(tmp_path: Path) -> None:
    """M15 (adversarial gate, PR #11 round 3, 2026-09-15): the reason
    validation itself is a mutation target -- prove all five real reasons
    construct, and an unknown one is still rejected, now that round 4 added
    three more valid values to the set."""
    for reason in (
        "lease_expired", "turn_timeout", "empty_output",
        "handler_error", "runtime_invalidated",
    ):
        assert _DeliveryDeferred("msg-1", reason=reason).reason == reason
    with pytest.raises(ValueError):
        _DeliveryDeferred("msg-1", reason="not_a_real_reason")


@pytest.mark.asyncio
async def test_reconcile_self_heals_a_clean_tombstone_with_an_invalid_receipt_reply(
    tmp_path: Path,
) -> None:
    """Athena F-B (PR #11 round 3, 2026-09-15): `invalid_receipt` is ALSO a
    terminal state (`_mark_reply_complete`'s own "no validated receipt"
    branch) -- before this fix, `_reply_staged_incomplete` treated it as
    still-staged, so a clean tombstone (server-side attempt over, nothing
    left to consume) plus a matching `invalid_receipt` record could never
    clear `pending` across a restart; the only escape was a manual
    state.json edit.
    """
    state_path = await quarantine_via_transport_failure(tmp_path)

    class ReconcileClient:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
            if tool == "inbox_consumer_status":
                return dict(STATUS)
            if tool == "inbox_lease_reconcile":
                return attempt_result(arguments["attempt_id"], "expired")
            raise AssertionError(f"unexpected tool: {tool}")

    state = StateStore(state_path).load()
    state["pending"] = {"message": {"id": "msg-orphaned"}}
    state["reply_outbox"] = {"msg-orphaned": reply_record("msg-orphaned", "invalid_receipt")}
    StateStore(state_path).save(state)

    adapter = make_adapter(state_path, ReconcileClient())
    assert await adapter.reconcile_inbox_polling() is True
    after = StateStore(state_path).load()
    assert after.get("lease_reconciliation") is None
    assert after.get("pending") is None  # self-heals, not held forever

    # A fresh connect() on the now-healed state needs no further intervention.
    fresh = make_adapter(state_path, ReconcileClient())
    try:
        assert await fresh.connect() is True
    finally:
        await fresh.disconnect()


@pytest.mark.asyncio
async def test_mid_poll_reconcile_deferral_logs_true_cause_not_estop(
    tmp_path: Path, caplog: Any,
) -> None:
    """F-A (Athena gate) / P2-4 (adversarial gate), PR #11 round 3,
    2026-09-15: `reconcile_inbox_polling`'s own `except _EstopDeferred:`
    clause also absorbs `_DeliveryDeferred` (a subclass) unless it has its
    own clause checked first -- it must log the true cause, not always name
    the e-stop, exactly the same class of bug `_poll_loop`'s own mid-poll
    log already had to fix (F4)."""
    state_path = await quarantine_via_transport_failure(tmp_path)
    attempt_id = StateStore(state_path).load()["lease_reconciliation"]["attempt_id"]

    class StillLeasedClient:
        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
            if tool == "inbox_consumer_status":
                return dict(STATUS)
            if tool == "inbox_lease_reconcile":
                message = message_at("msg-1", 1, far_future())
                return attempt_result(attempt_id, "leased", [message], far_future())
            raise AssertionError(f"unexpected tool: {tool}")

    adapter = make_adapter(state_path, StillLeasedClient())

    async def handler(_event: Any) -> None:
        raise RuntimeError("simulated handler crash")  # -> handler_error

    adapter.set_message_handler(handler)
    with caplog.at_level(logging.INFO, logger="plugin"):
        assert await adapter.reconcile_inbox_polling() is False

    after = StateStore(state_path).load()
    assert after.get("lease_reconciliation") is not None  # marker left exactly as it was

    messages = [r.getMessage() for r in caplog.records]
    reconcile_logs = [m for m in messages if "inbox reconciliation deferred" in m]
    assert reconcile_logs, "expected a reconciliation deferral log"
    assert "the turn handler raising before producing a reply" in reconcile_logs[0]
    assert "emergency stop" not in reconcile_logs[0]


# ---------------------------------------------------------------------------
# Install simulation: a real production incident snapshot.
# ---------------------------------------------------------------------------


# F2 (kasra-review re-gate #1, 2026-09-15): these were an out-of-repo
# fixture (`parents[3]/fixtures/...`), skipif-gated -- CI silently skipped
# the PR's headline evidence. Sanitized copies (bodies/Telegram chat ids
# stripped, structural keys + reply_outbox schema preserved -- see
# `scripts/sanitize_fixtures.py` used to generate them) now live in-repo.
FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"
REAL_INCIDENT_FIXTURE = FIXTURES_DIR / "state.json.bak-quarantine-20260915170649"


@contextmanager
def _real_fingerprint_owner(fingerprint: str):
    class Owner:
        def __init__(self) -> None:
            self.fingerprint = fingerprint

        def validated_fingerprint(self) -> str:
            return self.fingerprint

        @contextmanager
        def activate(self):
            yield

    yield Owner()


class InstallSimClient:
    """Mocked Mupot RPC surface for install-sim tests: echoes the fixture's
    own marker scope so `inbox_consumer_status`/`inbox_lease_reconcile`
    read as belonging to that snapshot's exact quarantined attempt."""

    def __init__(self, marker: dict[str, Any], reconcile_state: str = "expired") -> None:
        self.marker = marker
        self.reconcile_state = reconcile_state
        self.tools: list[str] = []

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.tools.append(tool)
        marker = self.marker
        if tool == "inbox_consumer_status":
            return {
                "strict_scope": True,
                "tenant": marker["tenant"],
                "agent_id": marker["agent_id"],
                "effective_inbox_seat": marker["effective_inbox_seat"],
                "mode": marker["mode"],
                "generation": marker["generation"],
                "key_matches": True,
            }
        if tool == "inbox_lease_reconcile":
            return {
                "tenant": marker["tenant"],
                "agent_id": marker["agent_id"],
                "effective_inbox_seat": marker["effective_inbox_seat"],
                "attempt_id": arguments["attempt_id"],
                "state": self.reconcile_state,
                "lease_expires_at": None,
                "messages": [],
                "consumed": False,
            }
        if tool == "inbox_lease":
            return {
                "tenant": marker["tenant"],
                "agent_id": marker["agent_id"],
                "effective_inbox_seat": marker["effective_inbox_seat"],
                "attempt_id": arguments["attempt_id"],
                "state": "empty",
                "lease_expires_at": None,
                "messages": [],
                "consumed": False,
            }
        raise AssertionError(f"unexpected tool: {tool}")


@pytest.mark.asyncio
async def test_install_simulation_on_real_incident_state_is_safe_and_idempotent(
    tmp_path: Path,
) -> None:
    """A copy of the real production snapshot that produced this incident.
    Never touches the original file. Proves: loading it and calling
    connect() against it never crashes, never makes an unauthorised network
    call, and is idempotent across a restart.

    This exact historical snapshot's `pending` also fails the PRE-EXISTING,
    unrelated `_legacy_pending_ambiguous` gate (computed once at __init__,
    from BEFORE this process's own self-heal runs) -- its referenced message
    was abandoned with NO reply ever staged for it, itself evidence of the
    bug this fix closes, captured before the fix existed. So the FIRST
    connect() still correctly refuses -- on that pre-existing gate, not the
    lease one, which it self-heals in the same call (clearing both the
    marker and the now-provably-stale `pending`, item 3's clean-tombstone
    case). A SECOND connect() -- a fresh restart, as an operator would do --
    loads the now-healed state and connects clean with no further
    intervention: full recovery from this exact incident needs one restart,
    not a manual state.json edit.
    """
    real = json.loads(REAL_INCIDENT_FIXTURE.read_text())
    marker = real["lease_reconciliation"]
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps(real), encoding="utf-8")

    with _real_fingerprint_owner(marker["profile_owner_fingerprint"]) as owner:
        client = InstallSimClient(marker)
        adapter = make_adapter(state_path, client)
        adapter._secret_owner = owner
        adapter._profile_owner_fingerprint = marker["profile_owner_fingerprint"]

        # First connect(): the lease marker self-heals (clean tombstone, no
        # reply staged for its `pending` source -- item 3), which ALSO drops
        # that stale `pending` as a side effect. But THIS process's own
        # `_legacy_pending_ambiguous` was already computed at __init__, from
        # the pre-heal snapshot -- so this exact connect() still correctly
        # refuses, on the pre-existing, unrelated gate (not the lease one).
        first = await adapter.connect()
        after_first = StateStore(state_path).load()
        assert after_first.get("lease_reconciliation") is None  # lease marker self-healed
        assert after_first.get("pending") is None  # stale pending dropped too
        assert first is False

        # Second connect() (a fresh restart, e.g. by the operator): loads the
        # ALREADY-healed state from disk -- no marker, no ambiguous pending
        # -- and connects clean, with zero reconcile network calls needed.
        second_client = InstallSimClient(marker)
        adapter2 = make_adapter(state_path, second_client)
        adapter2._secret_owner = owner
        adapter2._profile_owner_fingerprint = marker["profile_owner_fingerprint"]
        try:
            second = await adapter2.connect()
        finally:
            await adapter2.disconnect()

        assert second is True
        assert "inbox_lease_reconcile" not in second_client.tools  # nothing left to heal


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fixture_name",
    [
        "state.json.bak-quarantine-20260915021900",
        "state.json.bak-quarantine-20260915155821",
        "state.json.bak-quarantine-20260915170106",
    ],
)
async def test_install_simulation_clean_marker_self_heals_on_first_connect(
    tmp_path: Path,
    fixture_name: str,
) -> None:
    """Three more real snapshots from the same 2026-09-15 crash loop (F2):
    `pending` is already None here (nothing left to lose) but the durable
    v3 marker is still present -- proof the process kept re-quarantining on
    every restart with no work outstanding. No `_legacy_pending_ambiguous`
    gate applies (pending is None), so the FIRST connect() self-heals the
    marker and connects clean -- unlike the ambiguous-pending shape below,
    this one needs no restart at all.
    """
    real = json.loads((FIXTURES_DIR / fixture_name).read_text())
    marker = real["lease_reconciliation"]
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps(real), encoding="utf-8")

    with _real_fingerprint_owner(marker["profile_owner_fingerprint"]) as owner:
        client = InstallSimClient(marker)
        adapter = make_adapter(state_path, client)
        adapter._secret_owner = owner
        adapter._profile_owner_fingerprint = marker["profile_owner_fingerprint"]
        try:
            connected = await adapter.connect()
        finally:
            await adapter.disconnect()

        assert connected is True
        after = StateStore(state_path).load()
        assert after.get("lease_reconciliation") is None
        assert after.get("pending") is None


@pytest.mark.asyncio
async def test_install_simulation_unstaged_pending_needs_one_restart(
    tmp_path: Path,
) -> None:
    """A fourth real snapshot (F2), same shape as the 17:06 fixture above:
    `pending` set with no reply ever staged for it. The PRE-EXISTING
    `_legacy_pending_ambiguous` gate refuses the first connect() even
    though the lease marker self-heals in that same call; a second
    connect() (a fresh restart) loads the healed state and connects clean.
    """
    real = json.loads(
        (FIXTURES_DIR / "state.json.bak-quarantine-20260915144726").read_text()
    )
    marker = real["lease_reconciliation"]
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps(real), encoding="utf-8")

    with _real_fingerprint_owner(marker["profile_owner_fingerprint"]) as owner:
        first_client = InstallSimClient(marker)
        adapter = make_adapter(state_path, first_client)
        adapter._secret_owner = owner
        adapter._profile_owner_fingerprint = marker["profile_owner_fingerprint"]
        first = await adapter.connect()
        after_first = StateStore(state_path).load()
        assert after_first.get("lease_reconciliation") is None
        assert after_first.get("pending") is None
        assert first is False

        second_client = InstallSimClient(marker)
        adapter2 = make_adapter(state_path, second_client)
        adapter2._secret_owner = owner
        adapter2._profile_owner_fingerprint = marker["profile_owner_fingerprint"]
        try:
            second = await adapter2.connect()
        finally:
            await adapter2.disconnect()

        assert second is True
        assert "inbox_lease_reconcile" not in second_client.tools


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fixture_name",
    [
        "state.json.bak-quarantine-20260915170649",
        "state.json.bak-quarantine-20260915155821",
        "state.json.bak-quarantine-20260915144726",
        "state.json.bak-quarantine-20260915021900",
        "state.json.bak-quarantine-20260915170106",
    ],
)
async def test_install_simulation_leased_attempt_refuses_without_a_turn(
    tmp_path: Path,
    fixture_name: str,
) -> None:
    """P2-6 (adversarial gate, PR #11 round 3, 2026-09-15): every install-sim
    fixture above used `reconcile_state="expired"` -- a clean tombstone. A
    still-`leased` attempt is the OTHER outcome `inbox_lease_reconcile` can
    report, and it is exactly the FENCED case `execute_leased=False` exists
    to refuse: `connect()`'s automatic self-heal must not re-execute the
    turn, must leave the marker exactly as it was, and must make zero calls
    beyond the read-only preflight (`inbox_consumer_status` +
    `inbox_lease_reconcile` itself) -- no `inbox_lease`, no turn, no ack.
    """
    real = json.loads((FIXTURES_DIR / fixture_name).read_text())
    marker = real["lease_reconciliation"]
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps(real), encoding="utf-8")

    with _real_fingerprint_owner(marker["profile_owner_fingerprint"]) as owner:
        client = InstallSimClient(marker, reconcile_state="leased")
        adapter = make_adapter(state_path, client)
        adapter._secret_owner = owner
        adapter._profile_owner_fingerprint = marker["profile_owner_fingerprint"]
        try:
            connected = await adapter.connect()
        finally:
            await adapter.disconnect()

        assert connected is False  # still fenced -- correct refusal
        after = StateStore(state_path).load()
        assert after.get("lease_reconciliation") is not None  # marker left exactly as it was
        assert "inbox_lease" not in client.tools  # zero turns
        assert client.tools == ["inbox_consumer_status", "inbox_lease_reconcile"]

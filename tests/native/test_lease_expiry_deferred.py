"""Lease-expiry-is-not-a-violation class fix (2026-09-15).

Live incident (kayhermes gateway, plugin 6c86c2b0, 2026-09-15): an MCP tool
call blocked 180s inside a turn, the message's own visibility lease expired
before the turn could ACK, and `_deliver` returned silently -- so
`_poll_loop`'s "message not in `processed`" check folded a routine, expected
expiry into `_protocol_error()` -> `_quarantine_inbox_polling()`, a state
that survived every subsequent operator restart. SAME class as the e-stop
bug (`_EstopDeferred`, see `tests/native/test_estop_lease_gate.py`): a
temporal condition must never become a durable state transition.

Drives the REAL `_poll_loop` (not `_deliver` in isolation) end to end:
  - `test_expired_lease_defers_not_quarantines`: a leased message whose own
    `lease_expires_at` has already passed is never acked, never marked
    processed, and never quarantines the poll loop -- and the exact same
    message, redelivered by the server on a later lease with a fresh
    (unexpired) window, completes and acks exactly once.
  - `test_genuine_protocol_violation_still_quarantines`: the countercase
    that proves `_quarantine_inbox_polling()` is not dead code -- a real
    protocol violation unrelated to lease expiry still quarantines.

Round 2 (kasra-review re-gate, head 5046ea79, 2026-09-15) BLOCK-1/BLOCK-2/
M9/M10 fixes -- see `_TurnFailureDeferred`'s and each raise site's own
docstrings in `mupot_gateway/adapter.py` for the full class rationale:
  - `test_restart_after_expiry_deferral_reconnects`: BLOCK-1 -- a lease
    expiry deferral must clear `pending`, or a restart reads it as ambiguous
    legacy work and `connect()` refuses forever, moving the brick from
    `lease_reconciliation` to `pending`.
  - `test_post_timeout_lease_expiry_also_clears_pending`: same BLOCK-1 fix
    at the OTHER `_LeaseExpiredDeferred` raise site (post-turn-timeout, lease
    was the binding constraint).
  - `test_turn_timeout_with_live_lease_bounds_retries_then_dlq`,
    `test_no_custody_bounds_retries_then_dlq`,
    `test_handler_error_bounds_retries_then_dlq`: BLOCK-2/WARN -- a turn
    that hangs past `turn_timeout` with a LIVE lease, a handler that returns
    no custody, and a handler that raises are turn failures, not lease
    expiries -- bounded by `delivery_attempts`, never retried forever and
    never silently quarantined.
  - `test_reconcile_defers_on_lease_expiry_not_reconciliation_failed`: M9 --
    a `_LeaseExpiredDeferred` mid-`reconcile_inbox_polling()` must be read
    as "deferred, retry later", not folded into the generic "reconciliation
    failed" path (both return `False`; the OBSERVABLE difference this test
    pins is the log line, per the standing "verify the PROPERTY, not just
    the return value" rule -- state alone survives the M9 mutation).
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from gateway.config import PlatformConfig
from plugin.mupot_gateway.adapter import (
    MupotAdapter,
    StateStore,
    _LeaseExpiredDeferred,
    _TurnFailureDeferred,
)

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


class ExpiryDriverClient:
    """One-message fake mupot server: first lease carries an already-expired
    lease window; a later lease (once `redeliver` is armed) carries a fresh
    one, simulating the server redelivering the exact same message once
    nothing acked it in time."""

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
            # The envelope's own `lease_expires_at` must echo the leased
            # message's (validate_lease_attempt_result requires an exact
            # match), so build it directly rather than through the
            # "leased"-implies-FRESH_LEASE helper default.
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


def _iso_in(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat().replace(
        "+00:00", "Z"
    )


class RollingLiveLeaseClient(ExpiryDriverClient):
    """Fake mupot server that always grants a FRESH, long-lived lease for the
    same message, incrementing `delivery_attempts` each time -- simulates a
    turn that keeps failing (hanging past `turn_timeout`, returning no
    custody, or raising) while its own lease never comes close to expiring.
    Round 2 (kasra-review re-gate BLOCK-2, 2026-09-15): proves
    `_TurnFailureDeferred` is bounded by `delivery_attempts`, distinct from
    `_LeaseExpiredDeferred`'s unbounded-by-design redelivery."""

    async def call(self, tool, arguments):
        if tool == "inbox_lease":
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


@pytest.mark.asyncio
async def test_expired_lease_defers_not_quarantines(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    client = ExpiryDriverClient(PEER_MSG)
    adapter = make_adapter(tmp_path, client)
    handled: list[str] = []

    async def handler(event):
        handled.append(event.message_id)
        return "{ack_for:req-7} accepted"

    adapter.set_message_handler(handler)
    assert await adapter.connect()
    try:
        # Give the poll loop several ticks against the already-expired lease.
        await _await_until(lambda: client.lease_calls >= 3)

        st = _state(state_path)
        assert handled == [], "handler ran on an already-expired lease"
        assert client.ack_calls() == [], "acked an already-expired lease"
        assert st.get("processed", []) == [], "marked an expired lease processed"
        assert st.get("lease_reconciliation") is None, (
            "expired lease quarantined inbox polling")
        assert adapter._lease_quarantined is False
        assert adapter.has_fatal_error is False
        assert not adapter._poll_task.done(), "poll loop died on an expired lease"

        # The server redelivers the exact same message with a fresh lease --
        # this time it completes and acks exactly once.
        client.redeliver = True
        assert await _await_until(lambda: client.acked), "redelivered message never acked"
        await asyncio.sleep(0.05)  # let the poll loop settle past the ack tick

        st2 = _state(state_path)
        assert handled == ["m-peer"]
        assert client.ack_calls() == ["inbox_lease_ack"], (
            "redelivered message acked more than once"
        )
        assert st2.get("processed", []) == ["m-peer"]
        assert st2.get("lease_reconciliation") is None
        assert adapter._lease_quarantined is False
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_genuine_protocol_violation_still_quarantines(tmp_path: Path) -> None:
    """Countercase: `_quarantine_inbox_polling()` is not dead code. A real
    protocol violation -- here, a successful delivery whose commit is
    sabotaged so it never lands in `processed` -- must still quarantine,
    proving the class fix narrows the exception, it does not remove it."""
    state_path = tmp_path / "state.json"
    message = dict(PEER_MSG, lease_expires_at=FRESH_LEASE)
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)

    async def handler(event):
        return "{ack_for:req-7} accepted"

    adapter.set_message_handler(handler)
    # Sabotage _commit so a genuinely successful, acked delivery never marks
    # the source processed -- the exact condition _poll_loop's own
    # "message not in processed" check exists to catch.
    adapter._commit = lambda message_id: None

    assert await adapter.connect()
    try:
        assert await _await_until(lambda: adapter._lease_quarantined is True), (
            "a real protocol violation (never marked processed) failed to quarantine"
        )
        st = _state(state_path)
        assert isinstance(st.get("lease_reconciliation"), dict)
        assert adapter.has_fatal_error is True

        # M10 (kasra-review re-gate round 2, 2026-09-15): every existing test
        # exercising `lease_reconciliation_status()` only ever saw the CLEAR
        # case (marker absent) -- mutating it to always return `None`
        # (regardless of state) left every one of them green. Positive case:
        # a genuinely live marker must be reported, with real fields, not
        # just its absence.
        status = adapter.lease_reconciliation_status()
        assert status is not None, "a live lease_reconciliation marker must be reported"
        assert status["required"] is True
        assert status["version"] == 3
        assert isinstance(status["attempt_id"], str) and status["attempt_id"]
        assert status["connect_will_attempt_auto_reconcile"] is True
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_restart_after_expiry_deferral_reconnects(tmp_path: Path) -> None:
    """BLOCK-1 (kasra-review re-gate round 2, 2026-09-15, head 5046ea79):
    `_deliver` wrote `pending = {"message": message}` BEFORE either
    `_LeaseExpiredDeferred` raise site and left it set on every deferral. On
    restart, the constructor's `_legacy_pending_ambiguous` check read that
    survivor as ambiguous crash state (its source id was never staged in
    `reply_outbox`) and `connect()` refused -- BEFORE the
    `_lease_quarantined` auto-reconcile branch even runs, since the pending-
    ambiguity check comes first in `_connect_with_active_scope`. The brick
    moved from `lease_reconciliation` to `pending`, silently: 476 green
    tests and green CI were blind to it because none of them restarted a
    fresh `MupotAdapter` against state a real deferral had produced.

    Ported from kasra-review's own probe (`scratchpad/kasra-probes-pr9/
    test_kasra_probe_a.py`, read-only, never shipped) with the outcome
    updated to prove the FIX rather than reproduce the bug."""
    state_path = tmp_path / "state.json"
    client = ExpiryDriverClient(PEER_MSG)  # PEER_MSG's lease is already expired
    adapter = make_adapter(tmp_path, client)

    async def handler(event):
        return "{ack_for:req-7} accepted"

    adapter.set_message_handler(handler)
    assert await adapter.connect()
    try:
        await _await_until(lambda: client.lease_calls >= 2)
        st = _state(state_path)
        assert st.get("lease_reconciliation") is None
        assert st.get("pending") is None, (
            "BLOCK-1: a clean lease-expiry deferral must clear pending -- "
            "nothing here was ever a candidate for reply_outbox custody"
        )
        assert st.get("reply_outbox") == {}
    finally:
        await adapter.disconnect()

    restarted = MupotAdapter(
        PlatformConfig(enabled=True, typing_indicator=False, extra={
            "allowed_agents": "hadi-codex", "poll_interval": 0.01,
            "state_path": str(state_path)}),
        client_factory=lambda *_: ExpiryDriverClient(PEER_MSG),
    )
    restarted.set_message_handler(handler)
    assert restarted._legacy_pending_ambiguous is False, (
        "BLOCK-1: pending survived the deferral and bricked the restart"
    )
    assert restarted._lease_quarantined is False
    assert await restarted.connect() is True, "restart after a lease-expiry deferral must reconnect"
    await restarted.disconnect()


@pytest.mark.asyncio
async def test_post_timeout_lease_expiry_also_clears_pending(tmp_path: Path) -> None:
    """BLOCK-1's SECOND raise site (kasra-review re-gate round 2, 2026-09-15):
    the pre-turn `_expire_if_needed` check is not the only place
    `_LeaseExpiredDeferred` is raised -- the post-`asyncio.wait_for` timeout
    branch, when the lease deadline (not `turn_timeout`) is what bound the
    wait, must clear `pending` too, or a restart bricks on a mid-turn lease
    expiry exactly the same way."""
    state_path = tmp_path / "state.json"
    message = dict(PEER_MSG, lease_expires_at=_iso_in(0.2))
    client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, client)
    adapter.turn_timeout = 5.0  # far longer than the lease -- the LEASE binds

    async def hung(event):
        await asyncio.Event().wait()

    adapter.set_message_handler(hung)
    with pytest.raises(_LeaseExpiredDeferred):
        await adapter._deliver(message)
    st = _state(state_path)
    assert st.get("pending") is None
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_turn_timeout_with_live_lease_bounds_retries_then_dlq(tmp_path: Path) -> None:
    """BLOCK-2 (kasra-review re-gate round 2, 2026-09-15): pre-fix, a turn
    that hangs past `turn_timeout` with its message's lease STILL LIVE
    unconditionally raised `_LeaseExpiredDeferred` -- `_poll_loop` releases
    the lease and the server immediately redelivers, so a turn that will
    very likely hang again re-executes without limit (A/B measured 1 turn
    before this fix vs 11+ and climbing, with no operator signal). Below
    `max_delivery_attempts`, this now defers (`_TurnFailureDeferred`,
    kind="turn_timeout") the same way; AT the cap, `_deliver` acks the
    source, DLQs it, and commits -- terminal, never a durable quarantine and
    never unbounded re-execution.

    Adapted from kasra-review's own probe D (`scratchpad/kasra-probes-pr9/
    test_kasra_probe_d.py`, read-only, never shipped)."""
    state_path = tmp_path / "state.json"
    client = RollingLiveLeaseClient(dict(PEER_MSG, lease_expires_at=_iso_in(30)))
    adapter = make_adapter(tmp_path, client)
    adapter.turn_timeout = 0.05
    handled: list[str] = []

    async def hung(event):
        handled.append(event.message_id)
        await asyncio.sleep(30)

    adapter.set_message_handler(hung)
    assert await adapter.connect()
    try:
        assert await _await_until(
            lambda: len(handled) >= adapter.max_delivery_attempts, n=1000
        ), "turn failure never reached the cap"
        await asyncio.sleep(0.2)  # let the terminal ack/DLQ/commit tick settle

        assert len(handled) == adapter.max_delivery_attempts, (
            "turn re-executed past max_delivery_attempts -- unbounded again"
        )
        st = _state(state_path)
        assert st.get("lease_reconciliation") is None, (
            "a bounded turn failure must never quarantine inbox polling"
        )
        assert adapter._lease_quarantined is False
        assert adapter.has_fatal_error is False
        assert not adapter._poll_task.done(), "poll loop died on a bounded turn failure"
        assert st.get("processed") == [PEER_MSG["id"]]
        assert st.get("pending") is None

        dlq = st.get("dlq") or []
        assert len(dlq) == 1
        assert dlq[0]["reason"] == "turn_timeout"
        assert dlq[0]["message"]["id"] == PEER_MSG["id"]

        # mupot_gateway_status must surface this (item 2), not just state.json.
        summary = adapter.turn_failure_dlq_summary()
        assert summary == [{"source_id": PEER_MSG["id"], "reason": "turn_timeout"}]

        # Item 2/6 (kasra-review re-gate round 2, 2026-09-15): assert through
        # the ACTUAL registered tool and its own `adapter_factory`-built
        # instance, not a bare `MupotAdapter()` poked into a private
        # closure variable -- a hardcoded `"turn_failure_dlq": []` in the
        # tool's own dict literal would pass every OTHER status test (none
        # of them have a live DLQ entry to notice) while still lying to an
        # operator. `_live_adapter` is a closure-local inside `register()`;
        # the only supported way to populate it is `ctx.adapter_factory(...)`.
        from plugin.mupot_gateway.adapter import register as register_native_gateway

        tools: dict[str, object] = {}

        class Ctx:
            def inject_message(self, *_a, **_kw):
                return True

            def register_platform(self, **kwargs):
                self.adapter_factory = kwargs["adapter_factory"]

            def register_tool(self, **kwargs):
                tools[kwargs["name"]] = kwargs["handler"]

        ctx = Ctx()
        register_native_gateway(ctx)
        status_instance = ctx.adapter_factory(
            PlatformConfig(enabled=True, extra={"state_path": str(state_path)})
        )
        try:
            reported = json.loads(tools["mupot_gateway_status"]({}))
        finally:
            await status_instance.disconnect()
        assert reported["turn_failure_dlq"] == [
            {"source_id": PEER_MSG["id"], "reason": "turn_timeout"}
        ]
        assert reported["reconciling"] is False

        # No further executions after the cap: the message is processed, so
        # redelivery just re-acks without ever calling the handler again.
        attempts_at_cap = client.lease_calls
        await asyncio.sleep(0.2)
        assert len(handled) == adapter.max_delivery_attempts
        assert client.lease_calls > attempts_at_cap, "poll loop must still be alive/leasing"
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_no_custody_bounds_retries_then_dlq(tmp_path: Path) -> None:
    """WARN (kasra-review re-gate round 2, 2026-09-15): a handler that
    completes but produces nothing with human custody used to `return`
    bare -- unacked, unprocessed, no raise -- which `_poll_loop` folded into
    `_protocol_error()` -> `_quarantine_inbox_polling()`. Now bounded the
    same way as `turn_timeout` (kind="no_custody")."""
    state_path = tmp_path / "state.json"
    client = RollingLiveLeaseClient(dict(PEER_MSG, lease_expires_at=_iso_in(30)))
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
        await asyncio.sleep(0.2)
        st = _state(state_path)
        assert len(handled) == adapter.max_delivery_attempts
        assert st.get("lease_reconciliation") is None
        assert adapter._lease_quarantined is False
        assert not adapter._poll_task.done()
        dlq = st.get("dlq") or []
        assert len(dlq) == 1 and dlq[0]["reason"] == "no_custody"
        assert st.get("processed") == [PEER_MSG["id"]]
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_handler_error_bounds_retries_then_dlq(tmp_path: Path) -> None:
    """WARN (kasra-review re-gate round 2, 2026-09-15): a handler that raises
    (surfaced by `BasePlatformAdapter` as `ProcessingOutcome.FAILURE`, see
    `on_processing_complete`) used to `return` bare -- same silent-quarantine
    shape as `no_custody`. Now bounded (kind="handler_error").

    `BasePlatformAdapter`'s own crash handling calls `_notify_turn_error`
    (unrelated to this fix, pre-existing on every handler exception), which
    tries to tell the human via this adapter's `send()` -- an UNCONFOUNDED
    test of the bounded-retry property needs that side channel neutered, or
    a successful error notification legitimately stages a "complete"
    reply_outbox record that `_replay_reply_outbox` commits on its own,
    finishing the message via a different, equally legitimate path before
    `max_delivery_attempts` is ever reached. Stub `send()` to fail cleanly
    (no reply_outbox write) so this test isolates `_resolve_turn_failure`'s
    own bounded-retry-then-DLQ behavior specifically."""
    state_path = tmp_path / "state.json"
    client = RollingLiveLeaseClient(dict(PEER_MSG, lease_expires_at=_iso_in(30)))
    adapter = make_adapter(tmp_path, client)
    handled: list[str] = []

    async def raising_handler(event):
        handled.append(event.message_id)
        raise RuntimeError("handler exploded")

    async def stub_send(*_args, **_kwargs):
        return None

    adapter.set_message_handler(raising_handler)
    adapter.send = stub_send
    assert await adapter.connect()
    try:
        assert await _await_until(
            lambda: len(handled) >= adapter.max_delivery_attempts, n=1000
        )
        await asyncio.sleep(0.2)
        st = _state(state_path)
        assert len(handled) == adapter.max_delivery_attempts
        assert st.get("lease_reconciliation") is None
        assert adapter._lease_quarantined is False
        assert not adapter._poll_task.done()
        dlq = st.get("dlq") or []
        assert len(dlq) == 1 and dlq[0]["reason"] == "handler_error"
        assert st.get("processed") == [PEER_MSG["id"]]
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_gateway_status_reports_reconciling_during_auto_reconcile(
    tmp_path: Path,
) -> None:
    """Item 6 (kasra-review re-gate round 2, 2026-09-15): `connected: false`
    alone cannot distinguish "not yet attempted" from "connect()'s bounded
    auto-reconcile turn is actively running right now". Force a genuine
    quarantine, restart, and observe `mupot_gateway_status.reconciling` flip
    `True` while `connect()`'s auto-reconcile is in flight and back to
    `False` once it resolves -- through the ACTUAL registered tool, not just
    the adapter attribute, so a hardcoded status literal cannot pass this."""
    state_path = tmp_path / "state.json"
    message = dict(PEER_MSG, lease_expires_at=FRESH_LEASE)
    setup_client = ExpiryDriverClient(message)
    setup_adapter = make_adapter(tmp_path, setup_client)

    async def handler(event):
        return "{ack_for:req-7} accepted"

    setup_adapter.set_message_handler(handler)
    setup_adapter._commit = lambda message_id: None  # force a genuine quarantine
    assert await setup_adapter.connect()
    try:
        assert await _await_until(lambda: setup_adapter._lease_quarantined is True)
    finally:
        await setup_adapter.disconnect()

    from plugin.mupot_gateway.adapter import register as register_native_gateway

    tools: dict[str, object] = {}

    class Ctx:
        def inject_message(self, *_a, **_kw):
            return True

        def register_platform(self, **kwargs):
            self.adapter_factory = kwargs["adapter_factory"]

        def register_tool(self, **kwargs):
            tools[kwargs["name"]] = kwargs["handler"]

    ctx = Ctx()
    register_native_gateway(ctx)
    restarted = ctx.adapter_factory(
        PlatformConfig(enabled=True, typing_indicator=False, extra={
            "allowed_agents": "hadi-codex", "poll_interval": 0.01,
            "state_path": str(state_path)})
    )
    restarted._client = ExpiryDriverClient(message)
    restarted._send_client = restarted._client
    restarted.set_message_handler(handler)

    # The fake client for `restarted` has no memory of the ORIGINAL attempt
    # (a fresh `ExpiryDriverClient` doesn't implement `inbox_lease_reconcile`
    # at all) -- this test only needs to observe the `reconciling` flag's
    # lifecycle around the call, not a successful reconciliation (that
    # property is already covered by `test_restart_after_expiry_deferral_
    # reconnects`'s clean-tombstone case), so the stub returns `False`
    # directly rather than delegating to the real implementation.
    entered = asyncio.Event()
    release = asyncio.Event()

    async def paused_reconcile():
        entered.set()
        await release.wait()
        return False

    restarted._reconcile_inbox_polling_with_active_scope = paused_reconcile

    try:
        connect_task = asyncio.create_task(restarted.connect())
        await asyncio.wait_for(entered.wait(), 1)
        mid_flight = json.loads(tools["mupot_gateway_status"]({}))
        assert mid_flight["connected"] is False
        assert mid_flight["reconciling"] is True, (
            "connect()'s in-flight auto-reconcile must be visible, not "
            "indistinguishable from 'not yet attempted'"
        )

        release.set()
        assert await asyncio.wait_for(connect_task, 2) is False

        after = json.loads(tools["mupot_gateway_status"]({}))
        assert after["reconciling"] is False, (
            "reconciling must clear once connect()'s attempt resolves, "
            "success or failure alike"
        )
    finally:
        await restarted.disconnect()


class ReconcileDeferralClient:
    """Fake mupot server driving `reconcile_inbox_polling()` through a
    `_LeaseExpiredDeferred` mid-reconciliation -- M9 (kasra-review re-gate
    round 2, 2026-09-15): the reconcile catch must recognise every
    `_DeliveryDeferred` subclass, not just `_EstopDeferred`, without folding
    it into the generic "reconciliation failed" path."""

    def __init__(self, message):
        self.message = copy.deepcopy(message)
        self.reconcile_calls = 0

    async def connect(self):
        return None

    async def close(self):
        return None

    async def call(self, tool, arguments):
        if tool == "inbox_consumer_status":
            return {"strict_scope": True, **FAKE_SCOPE, "mode": "bearer_only",
                    "generation": 0, "key_matches": True}
        if tool == "inbox_lease":
            aid = arguments.get("attempt_id")
            msg = dict(self.message, lease_expires_at=FRESH_LEASE)
            return {**FAKE_SCOPE, "attempt_id": aid, "state": "leased",
                    "lease_expires_at": FRESH_LEASE, "messages": [msg], "consumed": False}
        if tool == "inbox_lease_reconcile":
            self.reconcile_calls += 1
            aid = arguments["attempt_id"]
            # The reconcile attempt re-leases the SAME message, but this
            # time ITS OWN lease has already expired -- a genuine
            # _LeaseExpiredDeferred site, distinct from _EstopDeferred.
            msg = dict(self.message, lease_expires_at=EXPIRED_LEASE)
            return {**FAKE_SCOPE, "attempt_id": aid, "state": "leased",
                    "lease_expires_at": EXPIRED_LEASE, "messages": [msg], "consumed": False}
        raise AssertionError(f"unexpected tool {tool} {arguments}")


@pytest.mark.asyncio
async def test_reconcile_defers_on_lease_expiry_not_reconciliation_failed(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """M9: force a genuine quarantine, then feed `reconcile_inbox_polling()`
    a `_LeaseExpiredDeferred` mid-attempt. Both the pre-fix (mutated) and
    fixed code return `False` here -- state alone cannot distinguish them
    (the standing "verify the PROPERTY, not just the return value" rule) --
    so this asserts the actual observable difference: which log line fires,
    and that the marker survives untouched for a later retry rather than
    being discarded as a failure."""
    state_path = tmp_path / "state.json"
    message = dict(PEER_MSG, lease_expires_at=FRESH_LEASE)
    setup_client = ExpiryDriverClient(message)
    adapter = make_adapter(tmp_path, setup_client)

    async def handler(event):
        return "{ack_for:req-7} accepted"

    adapter.set_message_handler(handler)
    adapter._commit = lambda message_id: None  # force a genuine quarantine

    assert await adapter.connect()
    try:
        assert await _await_until(lambda: adapter._lease_quarantined is True)
    finally:
        await adapter.disconnect()

    st_before = _state(state_path)
    assert isinstance(st_before.get("lease_reconciliation"), dict)

    reconcile_client = ReconcileDeferralClient(message)
    adapter._client = reconcile_client
    adapter._send_client = reconcile_client

    with caplog.at_level(logging.INFO):
        result = await adapter.reconcile_inbox_polling()

    assert result is False, "a deferred condition must not report reconciliation as cleared"
    assert reconcile_client.reconcile_calls == 1
    assert adapter._lease_quarantined is True, (
        "the marker must survive untouched so a later retry can succeed "
        "once the deferred condition clears"
    )
    st_after = _state(state_path)
    assert st_after.get("lease_reconciliation") == st_before.get("lease_reconciliation")

    messages = [record.getMessage() for record in caplog.records]
    assert any("inbox reconciliation deferred" in m for m in messages), messages
    assert not any("inbox attempt reconciliation failed" in m for m in messages), messages

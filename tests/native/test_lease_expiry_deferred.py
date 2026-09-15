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
"""
from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path

import pytest

from gateway.config import PlatformConfig
from plugin.mupot_gateway.adapter import MupotAdapter, StateStore

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
    finally:
        await adapter.disconnect()

"""Round-4 whole-iteration e-stop lease gate (kasra-review re-gate #3, 2026-09-14).

Ported from kasra-review's own independent execution driver
(tests/native/test_k3drv.py in the reviewer's isolated sandbox -- not shipped
with the PR) into a real, permanent regression test. Drives the REAL
_poll_loop, with the REAL agent/estop.py sentinel, across all 5 message
classes the re-gate named: peer deliver, routine event, ack envelope,
sender-policy DLQ, and routine-events-disabled quarantine. Proves:
  - PRE-lease pause: nothing is ever leased/acked, the poll task stays alive,
    no lease_reconciliation marker is written, nothing is marked processed.
  - MID-lease pause (e-stop engages between lease and processing): the poll
    task survives and the message is deferred rather than fatally quarantined.
See adapter.py's _EstopDeferred docstring for the full fix shape.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from pathlib import Path

import pytest

from gateway.config import PlatformConfig
from plugin.mupot_gateway.adapter import MupotAdapter, StateStore, _EstopDeferred

FAKE_LEASE_EXPIRY = "2099-01-01T00:00:00.000Z"
FAKE_SCOPE = {"tenant": "tenant-a", "agent_id": "agent-consumer", "effective_inbox_seat": None}


def attempt_result(attempt_id, state, messages=None):
    return {
        **FAKE_SCOPE,
        "attempt_id": attempt_id,
        "state": state,
        "lease_expires_at": FAKE_LEASE_EXPIRY if state == "leased" else None,
        "messages": messages or [],
        "consumed": False,
    }


def _u16(v):
    return len(v.encode("utf-16-le")) // 2


def routine_body(**up):
    value = {
        "version": "routine.human-wait/v1",
        "type": "routine_human_wait",
        "project_id": "project-1",
        "run_id": "run-1",
        "action_key": "question-1",
        "reason": "answer",
        "decision": {"type": "answer", "question": "Which receipt is authoritative?",
                     "choices": ["Booked", "Paid"]},
    }
    value.update(up)
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


PEER_MSG = {
    "id": "m-peer", "seq": 7, "from_agent": "hadi-codex", "from_member": "member-code",
    "body": "full Mupot answer", "project_id": "project-1", "request_id": "req-7",
    "in_reply_to": None, "kind": "message", "created_at": "2026-09-13T00:00:00.000Z",
    "delivery_attempts": 1, "lease_expires_at": FAKE_LEASE_EXPIRY,
}
ACK_MSG = {
    "id": "m-ack", "seq": 9, "from_agent": "hadi-codex", "from_member": "member-code",
    "body": "{ack_for:request-1} received", "request_id": "ack:request-1",
    "in_reply_to": "source-1", "kind": "ack", "expects_reply": False,
    "created_at": "2026-09-13T00:00:00.000Z", "delivery_attempts": 1,
    "lease_expires_at": FAKE_LEASE_EXPIRY,
}
_RB = routine_body()
ROUTINE_MSG = {
    "seq": 41, "id": "routine-message-1", "from_agent": "mupot-routines",
    "from_member": "system:routines", "kind": "ack", "body": _RB,
    "request_id": "routine-human:run-1:question-1", "in_reply_to": None,
    "created_at": "2026-09-13T10:00:00.000Z", "project_id": "project-1",
    "target_seat": None, "body_length": _u16(_RB),
    "checksum_sha256": hashlib.sha256(_RB.encode()).hexdigest(), "is_intact": True,
    "expects_reply": False, "reply_basis": "ack_is_terminal", "delivery_attempts": 1,
    "lease_expires_at": FAKE_LEASE_EXPIRY,
}
EVIL_MSG = dict(PEER_MSG, id="m-evil", from_agent="attacker-not-allowed")


class DriverClient:
    """One-message fake mupot server. Optionally engages e-stop on first lease."""

    def __init__(self, message, *, on_first_lease=None):
        self.message = copy.deepcopy(message)
        self.on_first_lease = on_first_lease
        self._fired = False
        self.calls = []
        self.lease_calls = 0
        self.acked = False
        self.sent = []

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
            leased = not self.acked
            result = attempt_result(aid, "leased" if leased else "empty",
                                    [self.message] if leased else [])
            if leased and not self._fired:
                self._fired = True
                if self.on_first_lease is not None:
                    self.on_first_lease()
            return result
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


def make_adapter(tmp_path, client, *, routine_events_enabled=True, extra=None):
    ex = {
        "allowed_agents": "hadi-codex,mupot-routines",
        "poll_interval": 0.01,
        "state_path": str(tmp_path / "state.json"),
        "routine_events_enabled": routine_events_enabled,
    }
    ex.update(extra or {})
    a = MupotAdapter(PlatformConfig(enabled=True, typing_indicator=False, extra=ex),
                     client_factory=lambda *_: client)
    a.injections = []
    a.message_injector = lambda content, **kw: (a.injections.append((content, kw)) or True)
    return a


def digest(p: Path):
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else "<absent>"


async def _ticks(n=25, dt=0.01):
    for _ in range(n):
        await asyncio.sleep(dt)


async def _await_until(pred, n=300, dt=0.01):
    for _ in range(n):
        if pred():
            return True
        await asyncio.sleep(dt)
    return False


def _report(name, payload):
    print(f"\n@@K3 {name} " + json.dumps(payload, sort_keys=True, default=str))


# ----------------------------------------------------------------- scenarios

async def _run_case(tmp_path, message, *, mid_message, routine_events_enabled=True,
                    activations=None):
    """Drive the REAL _poll_loop with the REAL agent/estop.py sentinel."""
    import hermes_constants
    from agent import estop as real_estop

    home = tmp_path / "hermes-home"
    home.mkdir(exist_ok=True)
    token = hermes_constants.set_hermes_home_override(str(home))
    state_path = tmp_path / "state.json"
    try:
        assert real_estop.is_engaged() is False
        hook = None
        if mid_message:
            hook = lambda: real_estop.engage(reason="k3-mid")  # noqa: E731
        else:
            real_estop.engage(reason="k3-pre")
            assert real_estop.is_engaged() is True

        client = DriverClient(message, on_first_lease=hook)
        adapter = make_adapter(tmp_path, client,
                               routine_events_enabled=routine_events_enabled)
        handled = []
        injected = []

        async def handler(event):
            handled.append(event.text)
            return "{ack_for:req-7} accepted"

        adapter.set_message_handler(handler)

        assert await adapter.connect()
        out = {}
        try:
            if mid_message:
                await _await_until(lambda: client.lease_calls > 0)
            await _ticks(30)

            paused_state = state_path.read_text() if state_path.exists() else None
            out["engaged_during_pause"] = real_estop.is_engaged()
            out["lease_calls_paused"] = client.lease_calls
            out["ack_calls_paused"] = client.ack_calls()
            out["handled_paused"] = list(handled)
            out["sent_paused"] = len(client.sent)
            out["injections_paused"] = len(adapter.injections)
            out["poll_task_alive"] = not adapter._poll_task.done()
            out["quarantined_paused"] = adapter._lease_quarantined
            out["fatal_code_paused"] = adapter._fatal_error_code
            st = json.loads(paused_state) if paused_state else {}
            out["lease_reconciliation_paused"] = st.get("lease_reconciliation")
            out["processed_paused"] = st.get("processed", [])
            out["dlq_paused"] = len(st.get("dlq") or [])
            # NOTE (adaptation from the original driver): the real durable
            # state key is "routine_event_quarantine" (routine_events.py's
            # quarantine_routine_event) -- the driver's own "routine_quarantine"
            # never matched any real key, so this always silently read 0
            # regardless of what the code did. Fixed here so this is an actual
            # measurement, not a vacuous one.
            out["routine_quarantine_paused"] = len(st.get("routine_event_quarantine") or [])
            out["pending_paused"] = st.get("pending")
            # P3 (kasra-review re-gate #4, 2026-09-14): the real durable
            # outbox keys are "notification_outbox" and "reply_outbox" (both
            # dicts keyed by source_id -- see notifications.py/adapter.py's
            # own state initialization). This used to read "notifications"/
            # "outbox", neither a real key, so this was always a silent 0
            # regardless of what the code did -- fixed here so it is an
            # actual measurement.
            out["notification_outbox_paused"] = len(st.get("notification_outbox") or {})
            out["reply_outbox_paused"] = len(st.get("reply_outbox") or {})
            out["state_digest_paused"] = digest(state_path)

            real_estop.disengage()
            assert real_estop.is_engaged() is False
            await _await_until(lambda: client.acked)
            await _ticks(20)

            st2 = json.loads(state_path.read_text()) if state_path.exists() else {}
            out["acked_after_resume"] = client.acked
            out["ack_calls_after"] = client.ack_calls()
            out["handled_after"] = list(handled)
            out["processed_after"] = st2.get("processed", [])
            out["lease_calls_after"] = client.lease_calls
            out["quarantined_after"] = adapter._lease_quarantined
            out["lease_reconciliation_after"] = st2.get("lease_reconciliation")
            out["poll_task_alive_after"] = not adapter._poll_task.done()
            out["injections_after"] = len(adapter.injections)
        finally:
            await adapter.disconnect()
        return out
    finally:
        real_estop.disengage()
        hermes_constants.reset_hermes_home_override(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("label,msg,ren", [
    ("peer_deliver", PEER_MSG, True),
    ("routine_event", ROUTINE_MSG, True),
    ("ack_envelope", ACK_MSG, True),
    ("sender_policy_dlq", EVIL_MSG, True),
    ("routine_disabled_quarantine", ROUTINE_MSG, False),
])
async def test_pre_lease_pause(tmp_path, label, msg, ren):
    out = await _run_case(tmp_path, msg, mid_message=False, routine_events_enabled=ren)
    _report(f"PRE/{label}", out)
    assert out["lease_calls_paused"] == 0, "leased while paused"
    assert out["ack_calls_paused"] == []
    assert out["poll_task_alive"] is True
    assert out["quarantined_paused"] is False
    assert out["lease_reconciliation_paused"] is None
    assert out["processed_paused"] == []
    assert out["dlq_paused"] == 0, "wrote to the DLQ while paused"
    assert out["routine_quarantine_paused"] == 0, "wrote a routine quarantine record while paused"
    assert out["sent_paused"] == 0, "transmitted a peer send while paused"
    assert out["injections_paused"] == 0, "injected into the human session while paused"
    assert out["pending_paused"] is None, "pause drained or grew the pending queue"
    assert out["notification_outbox_paused"] == 0, "enqueued a human notification while paused"
    assert out["reply_outbox_paused"] == 0, "wrote a reply-outbox record while paused"


@pytest.mark.asyncio
@pytest.mark.parametrize("label,msg,ren", [
    ("peer_deliver", PEER_MSG, True),
    ("routine_event", ROUTINE_MSG, True),
    ("ack_envelope", ACK_MSG, True),
    ("sender_policy_dlq", EVIL_MSG, True),
    ("routine_disabled_quarantine", ROUTINE_MSG, False),
])
async def test_mid_message_pause(tmp_path, label, msg, ren):
    out = await _run_case(tmp_path, msg, mid_message=True, routine_events_enabled=ren)
    _report(f"MID/{label}", out)
    assert out["poll_task_alive"] is True
    # This is the specific race _process_leased_message's own top-of-function
    # gate closes: the e-stop engages DURING the inbox_lease call (the
    # DriverClient's on_first_lease hook), so the pre-lease check above this
    # function already passed -- without a gate at the top of
    # _process_leased_message itself, three of its branches
    # (routine-events-disabled quarantine, sender-policy DLQ, and the
    # already-processed re-ack) would write durable state (a DLQ entry or a
    # routine_event_quarantine record) BEFORE ever reaching the ack choke
    # point inside _ack_expected -- proving the gate is not merely redundant
    # with the ack choke, it prevents the write that would otherwise precede
    # it.
    assert out["dlq_paused"] == 0, "wrote to the DLQ mid-lease while paused"
    assert out["routine_quarantine_paused"] == 0, (
        "wrote a routine quarantine record mid-lease while paused")
    assert out["processed_paused"] == [], "marked a source processed mid-lease while paused"
    assert out["ack_calls_paused"] == [], "acked the leased message mid-lease while paused"
    assert out["sent_paused"] == 0, "transmitted a peer send mid-lease while paused"
    assert out["injections_paused"] == 0, (
        "injected into the human session mid-lease while paused")
    assert out["pending_paused"] is None, (
        "pause drained or grew the pending queue mid-lease")

"""Lease expiry is a deferral, not a violation (2026-09-15 minimal fix).

Live incident (4 bricks, 2026-09-15, master 6c86c2b0): a turn outlives the
lease -> `_deliver` returns silently at either expiry checkpoint ->
`_poll_loop` reads "message not in processed" as a protocol violation ->
`_quarantine_inbox_polling()` -> a durable `lease_reconciliation` marker
refuses `connect()` across every subsequent restart, because nothing ever
called `reconcile_inbox_polling()` for it.

Covers items 1-6 of the minimal fix through the REAL `_poll_loop`/`connect()`/
`reconcile_inbox_polling()` machinery wherever practical -- direct method
calls only where a full poll-loop drive would duplicate existing coverage
(see tests/native/test_lease_attempt_reconciliation.py, unchanged) or depend
on Hermes internals outside this module's scope (turn_timeout/handler_error
bounding -- explicitly NOT this PR's class).
"""

from __future__ import annotations

import asyncio
import copy
import json
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
async def test_post_timeout_lease_expiry_defers_then_redelivers_and_completes_once(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    from datetime import datetime, timedelta, timezone

    soon_expired = (datetime.now(timezone.utc) + timedelta(milliseconds=80)).isoformat()
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
async def test_turn_timeout_with_live_lease_keeps_base_behaviour(tmp_path: Path) -> None:
    """Out of this class by design (see `_LeaseExpiredDeferred`'s docstring):
    a plain turn timeout while the message's own lease is still live must
    NOT raise the deferral -- it keeps returning, pending intact, exactly as
    on master today."""
    state_path = tmp_path / "state.json"
    adapter = make_adapter(state_path, object(), turn_timeout=0.05)

    async def handler(_event: Any) -> None:
        await asyncio.Event().wait()

    adapter.set_message_handler(handler)
    await adapter._deliver(message_at("msg-1", 1, far_future()))  # no raise

    state = StateStore(state_path).load()
    assert state["pending"]["message"]["id"] == "msg-1"
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


# ---------------------------------------------------------------------------
# Install simulation: a real production incident snapshot.
# ---------------------------------------------------------------------------


REAL_INCIDENT_FIXTURE = (
    Path(__file__).resolve().parents[3]
    / "fixtures"
    / "state.json.bak-quarantine-20260915170649"
)


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


@pytest.mark.asyncio
@pytest.mark.skipif(not REAL_INCIDENT_FIXTURE.exists(), reason="real incident fixture not present")
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

    class InstallSimClient:
        def __init__(self) -> None:
            self.tools: list[str] = []

        async def connect(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
            self.tools.append(tool)
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
                    "state": "expired",
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

    with _real_fingerprint_owner(marker["profile_owner_fingerprint"]) as owner:
        client = InstallSimClient()
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
        second_client = InstallSimClient()
        adapter2 = make_adapter(state_path, second_client)
        adapter2._secret_owner = owner
        adapter2._profile_owner_fingerprint = marker["profile_owner_fingerprint"]
        try:
            second = await adapter2.connect()
        finally:
            await adapter2.disconnect()

        assert second is True
        assert "inbox_lease_reconcile" not in second_client.tools  # nothing left to heal

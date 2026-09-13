from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

import pytest

from gateway.config import PlatformConfig

from plugin.mupot_gateway.adapter import (  # noqa: E402
    MupotProtocolError,
    MupotTransportError,
    MupotAdapter,
    StateStore,
    build_mupot_event,
    is_ack_envelope,
    is_terminal_ack,
)


class FakeMupotClient:
    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.acked = False
        self.connect_calls = 0
        self.lease_calls = 0
        self.message = {
            "id": "m-1",
            "seq": 7,
            "from_agent": "hadi-codex",
            "body": "full Mupot answer",
            "project_id": "project-1",
            "request_id": "req-7",
            "in_reply_to": None,
            "kind": "message",
        }

    async def connect(self) -> None:
        self.connect_calls += 1
        return None

    async def close(self) -> None:
        return None

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool == "inbox_lease":
            self.lease_calls += 1
            return {
                "messages": [] if self.acked else [self.message],
                "remaining": 0,
                "complete": True,
                "dead_lettered": 0,
                "lease_seconds": arguments["lease_seconds"],
            }
        if tool == "inbox_ack":
            assert arguments == {"ids": ["m-1"]}
            self.acked = True
            return {"acked": ["m-1"], "already_read": [], "refused": []}
        if tool == "send":
            self.sent.append(arguments)
            return {
                "id": "m-2",
                "seq": 8,
                "duplicate": False,
                "to": arguments["to"],
                "project_id": arguments.get("project_id"),
                "target_seat": None,
            }
        if tool == "inbox_consumer_status":
            return {
                "agent_id": "agent-consumer",
                "mode": "bearer_only",
                "generation": 0,
                "key_matches": True,
            }
        raise AssertionError(f"unexpected tool: {tool} {arguments}")


class AckMupotClient(FakeMupotClient):
    def __init__(
        self,
        *,
        fail_ack_once: bool = False,
        safe_ack_once: bool = False,
    ) -> None:
        super().__init__()
        self.message = {
            "id": "ack-1",
            "seq": 9,
            "from_agent": "hadi-codex",
            "body": "{ack_for:request-1} received",
            "request_id": "ack:request-1",
            "in_reply_to": "source-1",
            "kind": "ack",
            "expects_reply": False,
        }
        self.fail_ack_once = fail_ack_once
        self.safe_ack_once = safe_ack_once
        self.ack_calls = 0

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool == "inbox_ack":
            self.ack_calls += 1
            if self.safe_ack_once and self.ack_calls == 1:
                from plugin.mupot_gateway import adapter as adapter_module

                safe_error = getattr(adapter_module, "MupotSafeRetryError", RuntimeError)
                raise safe_error("Mupot request failed")
            if self.fail_ack_once and self.ack_calls == 1:
                return {"acked": [], "already_read": [], "refused": ["ack-1"]}
            self.acked = True
            return {"acked": ["ack-1"], "already_read": [], "refused": []}
        return await super().call(tool, arguments)


class LeaseFailureClient(FakeMupotClient):
    def __init__(self, failure: Exception) -> None:
        super().__init__()
        self.failure = failure
        self.lease_calls = 0
        self.first_lease = asyncio.Event()

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool == "inbox_lease":
            self.lease_calls += 1
            self.first_lease.set()
            raise self.failure
        return await super().call(tool, arguments)


class LeasePayloadClient(FakeMupotClient):
    def __init__(self, payload: object) -> None:
        super().__init__()
        self.payload = payload
        self.first_lease = asyncio.Event()

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool == "inbox_lease":
            self.lease_calls += 1
            self.first_lease.set()
            return self.payload  # type: ignore[return-value]
        return await super().call(tool, arguments)


class ReconciliationClient(FakeMupotClient):
    def __init__(self, status: object) -> None:
        super().__init__()
        self.status = status
        self.tools: list[str] = []

    async def call(self, tool: str, arguments: dict) -> dict:
        self.tools.append(tool)
        if tool == "inbox_consumer_status":
            return self.status  # type: ignore[return-value]
        raise AssertionError(f"unexpected reconciliation tool: {tool} {arguments}")


async def persist_ambiguous_lease_quarantine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    monkeypatch.setattr(time, "time", lambda: 100.0)
    state_path = tmp_path / "state.json"
    client = LeaseFailureClient(MupotTransportError("Mupot request failed"))
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "lease_seconds": 1,
                "poll_interval": 0.01,
                "state_path": str(state_path),
            },
        ),
        client_factory=lambda *_: client,
    )
    assert await adapter.connect()
    try:
        await asyncio.wait_for(client.first_lease.wait(), 1)
        for _ in range(100):
            if not adapter._running:
                break
            await asyncio.sleep(0.01)
        assert adapter._running is False
    finally:
        await adapter.disconnect()
    return state_path


def test_build_mupot_event_preserves_project_and_correlation() -> None:
    event = build_mupot_event(
        {
            "id": "m-1",
            "seq": 7,
            "from_agent": "hadi-codex",
            "body": "answer",
            "project_id": "project-1",
            "request_id": "req-7",
            "in_reply_to": "m-0",
        }
    )

    assert event.text == "answer"
    assert event.message_id == "m-1"
    assert event.internal is True
    assert event.source.chat_id == "hadi-codex"
    assert event.source.thread_id == "project-1"
    assert event.metadata["request_id"] == "req-7"
    assert event.metadata["in_reply_to"] == "m-0"


def test_unscoped_message_gets_isolated_session() -> None:
    event = build_mupot_event(
        {
            "id": "m-unscoped",
            "seq": 8,
            "from_agent": "hadi-codex",
            "body": "control command",
            "request_id": "req-8",
        }
    )

    assert event.internal is True
    assert event.source.thread_id == "m-unscoped"


def test_terminal_ack_requires_no_explicit_reply_expectation() -> None:
    assert is_terminal_ack({"id": "ack-1", "kind": "ack", "expects_reply": False})
    assert not is_terminal_ack({"id": "ack-1", "kind": "ack"})
    assert not is_terminal_ack({"id": "ack-1", "kind": "ack", "expects_reply": True})
    assert not is_terminal_ack({"kind": "ack", "expects_reply": False})
    assert not is_terminal_ack({"kind": "message", "expects_reply": False})
    assert is_ack_envelope({"kind": "ack", "body": "spoof"})
    assert not is_ack_envelope({"kind": "message", "expects_reply": False, "body": "{ack_for:x}"})


@pytest.mark.asyncio
async def test_adapter_leases_then_acks_only_after_success(tmp_path: Path) -> None:
    client = FakeMupotClient()
    config = PlatformConfig(
        enabled=True,
        typing_indicator=False,
        extra={
            "allowed_agents": "hadi-codex,hadi-codex-cli",
            "poll_interval": 0.01,
            "state_path": str(tmp_path / "state.json"),
        },
    )
    adapter = MupotAdapter(config, client_factory=lambda *_: client)

    async def handler(event):
        assert not client.acked
        return "{ack_for:req-7} accepted"

    adapter.set_message_handler(handler)
    assert await adapter.connect()
    try:
        for _ in range(100):
            if client.acked:
                break
            await asyncio.sleep(0.01)
        assert client.acked
        assert client.sent == [
            {
                "to": "hadi-codex",
                "body": "{ack_for:req-7} accepted",
                "project_id": "project-1",
                "request_id": "resp-m-1",
                "in_reply_to": "m-1",
            }
        ]
        state = StateStore(tmp_path / "state.json").load()
        assert state["pending"] is None
        assert "m-1" in state["processed"]
        assert os.stat(tmp_path / "state.json").st_mode & 0o777 == 0o600
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_terminal_ack_is_consumed_without_outbound_response(tmp_path: Path) -> None:
    client = AckMupotClient()
    handled: list[str] = []
    config = PlatformConfig(
        enabled=True,
        typing_indicator=False,
        extra={"poll_interval": 0.01, "state_path": str(tmp_path / "state.json")},
    )
    adapter = MupotAdapter(config, client_factory=lambda *_: client)
    async def handler(event):
        handled.append(event.text)

    adapter.set_message_handler(handler)
    assert await adapter.connect()
    try:
        for _ in range(100):
            if client.acked:
                break
            await asyncio.sleep(0.01)
        assert client.acked
        assert handled == []
        assert client.sent == []
        state = StateStore(tmp_path / "state.json").load()
        assert state["pending"] is None
        assert "ack-1" in state["processed"]
        assert state["terminal_receipts"] == [client.message]
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_terminal_ack_domain_failure_quarantines_without_retry(
    tmp_path: Path,
) -> None:
    client = AckMupotClient(fail_ack_once=True)
    handled: list[str] = []
    config = PlatformConfig(
        enabled=True,
        typing_indicator=False,
        extra={"poll_interval": 0.01, "state_path": str(tmp_path / "state.json")},
    )
    adapter = MupotAdapter(config, client_factory=lambda *_: client)
    async def handler(event):
        handled.append(event.text)

    adapter.set_message_handler(handler)
    assert await adapter.connect()
    try:
        for _ in range(100):
            if client.ack_calls >= 1:
                break
            await asyncio.sleep(0.01)
        assert client.ack_calls == 1
        assert "ack-1" not in StateStore(tmp_path / "state.json").load().get("processed", [])
        await asyncio.sleep(0.05)
        assert client.ack_calls == 1
        assert client.acked is False
        assert getattr(adapter, "_lease_quarantined", False) is True
        assert adapter._running is False
        assert handled == []
        state = StateStore(tmp_path / "state.json").load()
        assert "ack-1" not in state["processed"]
        assert len(state["terminal_receipts"]) == 1
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_terminal_ack_retries_one_safe_before_send_failure_without_releasing(
    tmp_path: Path,
) -> None:
    client = AckMupotClient(safe_ack_once=True)
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={"poll_interval": 0.01, "state_path": str(tmp_path / "state.json")},
        ),
        client_factory=lambda *_: client,
    )

    assert await adapter.connect()
    try:
        for _ in range(100):
            if client.acked:
                break
            await asyncio.sleep(0.01)
        assert client.acked is True
        assert client.ack_calls == 2
        assert client.lease_calls == 1
        assert getattr(adapter, "_lease_quarantined", False) is False
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        MupotTransportError("Mupot request failed"),
        MupotProtocolError("Mupot MCP request failed"),
    ],
)
async def test_poll_loop_quarantines_ambiguous_or_protocol_lease_without_releasing(
    tmp_path: Path,
    failure: Exception,
) -> None:
    client = LeaseFailureClient(failure)
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "poll_interval": 0.01,
                "state_path": str(tmp_path / "state.json"),
            },
        ),
        client_factory=lambda *_: client,
    )

    assert await adapter.connect()
    try:
        await asyncio.wait_for(client.first_lease.wait(), 1)
        await asyncio.sleep(0.05)
        assert client.lease_calls == 1
        assert getattr(adapter, "_lease_quarantined", False) is True
        assert adapter._running is False
        assert await adapter.connect(is_reconnect=True) is False
        assert client.lease_calls == 1
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [{}, {"messages": "not-a-list"}, {"messages": [{}, {}]}])
async def test_poll_loop_quarantines_malformed_lease_result_without_releasing(
    tmp_path: Path,
    payload: object,
) -> None:
    client = LeasePayloadClient(payload)
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={"poll_interval": 0.01, "state_path": str(tmp_path / "state.json")},
        ),
        client_factory=lambda *_: client,
    )

    assert await adapter.connect()
    try:
        await asyncio.wait_for(client.first_lease.wait(), 1)
        await asyncio.sleep(0.05)
        assert client.lease_calls == 1
        assert getattr(adapter, "_lease_quarantined", False) is True
        assert adapter._running is False
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_poll_loop_retries_only_classified_safe_before_send_failure(
    tmp_path: Path,
) -> None:
    from plugin.mupot_gateway import adapter as adapter_module

    safe_error = getattr(adapter_module, "MupotSafeRetryError", None)
    assert safe_error is not None
    client = LeaseFailureClient(safe_error("Mupot request failed"))
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "poll_interval": 0.01,
                "state_path": str(tmp_path / "state.json"),
            },
        ),
        client_factory=lambda *_: client,
    )

    assert await adapter.connect()
    try:
        for _ in range(100):
            if client.lease_calls >= 2:
                break
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)
        assert client.lease_calls == 2
        assert getattr(adapter, "_lease_quarantined", False) is True
        assert adapter._running is False
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_reconstructed_adapter_stays_fenced_without_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = await persist_ambiguous_lease_quarantine(tmp_path, monkeypatch)
    marker = StateStore(state_path).load().get("lease_reconciliation")
    assert isinstance(marker, dict)
    assert marker["required"] is True
    monkeypatch.setattr(time, "time", lambda: 200.0)

    client = ReconciliationClient(
        {
            "agent_id": "agent-consumer",
            "mode": "bearer_only",
            "generation": 0,
            "key_matches": True,
        }
    )
    reconstructed = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: client,
    )

    assert await reconstructed.connect() is False
    assert client.connect_calls == 0
    assert client.tools == []
    assert client.lease_calls == 0


@pytest.mark.asyncio
async def test_corrupt_existing_state_fails_closed_without_network(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text("{not-valid-json", encoding="utf-8")
    client = ReconciliationClient(
        {
            "agent_id": "agent-consumer",
            "mode": "bearer_only",
            "generation": 0,
            "key_matches": True,
        }
    )
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: client,
    )

    assert await adapter.connect() is False
    assert client.connect_calls == 0
    assert client.tools == []


@pytest.mark.asyncio
async def test_prelease_fence_write_failure_makes_zero_lease_calls(tmp_path: Path) -> None:
    client = LeasePayloadClient({"messages": []})
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={"poll_interval": 0.01, "state_path": str(tmp_path / "state.json")},
        ),
        client_factory=lambda *_: client,
    )

    def fail_save(_state: dict) -> None:
        raise OSError("state unavailable")

    adapter.store.save = fail_save
    assert await adapter.connect()
    try:
        await asyncio.sleep(0.05)
        assert client.lease_calls == 0
        assert adapter._running is False
        assert adapter.fatal_error_retryable is False
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_postlease_save_failure_leaves_prelease_fence_for_restart(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.json"
    client = LeasePayloadClient({"messages": []})
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={"poll_interval": 0.01, "state_path": str(state_path)},
        ),
        client_factory=lambda *_: client,
    )
    real_save = adapter.store.save
    save_calls = 0

    def fail_after_prelease(state: dict) -> None:
        nonlocal save_calls
        save_calls += 1
        if save_calls > 1:
            raise OSError("post-lease state unavailable")
        real_save(state)

    adapter.store.save = fail_after_prelease
    assert await adapter.connect()
    try:
        await asyncio.sleep(0.05)
    finally:
        await adapter.disconnect()

    assert save_calls >= 2
    assert isinstance(StateStore(state_path).load().get("lease_reconciliation"), dict)
    reconstructed_client = ReconciliationClient({})
    reconstructed = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: reconstructed_client,
    )
    assert await reconstructed.connect() is False
    assert reconstructed_client.connect_calls == 0
    assert reconstructed_client.tools == []
    assert reconstructed_client.lease_calls == 0


@pytest.mark.asyncio
async def test_explicit_reconciliation_before_lease_deadline_does_no_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = await persist_ambiguous_lease_quarantine(tmp_path, monkeypatch)
    monkeypatch.setattr(time, "time", lambda: 100.5)
    client = ReconciliationClient({})
    reconstructed = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: client,
    )

    reconcile = getattr(reconstructed, "reconcile_inbox_polling", None)
    assert callable(reconcile)
    assert await reconcile() is False
    assert client.connect_calls == 0
    assert client.tools == []
    assert isinstance(StateStore(state_path).load().get("lease_reconciliation"), dict)


@pytest.mark.asyncio
async def test_explicit_reconciliation_clears_only_after_exact_readback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = await persist_ambiguous_lease_quarantine(tmp_path, monkeypatch)
    monkeypatch.setattr(time, "time", lambda: 101.0)
    client = ReconciliationClient(
        {
            "agent_id": "agent-consumer",
            "mode": "bearer_only",
            "generation": 0,
            "key_matches": True,
        }
    )
    reconstructed = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: client,
    )

    reconcile = getattr(reconstructed, "reconcile_inbox_polling", None)
    assert callable(reconcile)
    assert await reconcile() is True
    assert client.connect_calls == 1
    assert client.tools == ["inbox_consumer_status"]
    assert StateStore(state_path).load().get("lease_reconciliation") is None
    assert reconstructed._running is False
    assert reconstructed.has_fatal_error is False

    fresh = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: ReconciliationClient({}),
    )
    assert getattr(fresh, "_lease_quarantined", True) is False


@pytest.mark.asyncio
async def test_failed_reconciliation_readback_remains_durably_fenced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = await persist_ambiguous_lease_quarantine(tmp_path, monkeypatch)
    monkeypatch.setattr(time, "time", lambda: 101.0)
    client = ReconciliationClient(
        {
            "agent_id": "agent-consumer",
            "mode": "bearer_only",
            "generation": 1,
            "key_matches": True,
        }
    )
    reconstructed = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda *_: client,
    )

    reconcile = getattr(reconstructed, "reconcile_inbox_polling", None)
    assert callable(reconcile)
    assert await reconcile() is False
    assert isinstance(StateStore(state_path).load().get("lease_reconciliation"), dict)
    assert getattr(reconstructed, "_lease_quarantined", False) is True
    assert await reconstructed.connect() is False
    assert client.tools == ["inbox_consumer_status"]


@pytest.mark.asyncio
async def test_terminal_ack_receipt_persistence_failure_prevents_ack(tmp_path: Path) -> None:
    client = AckMupotClient()
    config = PlatformConfig(
        enabled=True,
        typing_indicator=False,
        extra={"state_path": str(tmp_path / "state.json")},
    )
    adapter = MupotAdapter(config, client_factory=lambda *_: client)

    def fail_save(_state):
        raise OSError("receipt disk unavailable")

    adapter.store.save = fail_save
    with pytest.raises(OSError, match="receipt disk unavailable"):
        await adapter._handle_ack_envelope(client.message)
    assert client.ack_calls == 0
    assert "ack-1" not in adapter._state["processed"]


@pytest.mark.asyncio
@pytest.mark.parametrize("tenant,agent,accepted", [("right-tenant", "right-agent", True),
    ("other-tenant", "right-agent", False), ("right-tenant", "other-agent", False)])
async def test_gateway_verifies_operator_identity_before_reading_mail(tmp_path, tenant, agent, accepted):
    calls = []
    class BoundClient(FakeMupotClient):
        async def call(self, tool, arguments):
            calls.append(tool)
            if tool == "boot_context":
                return {"tenant": tenant, "bound_agent_id": agent, "channel": "workspace",
                        "role": "member", "capabilities": []}
            if tool == "inbox_consumer_status":
                return {"agent_id": agent, "mode": "bearer_only", "generation": 0,
                        "key_matches": True}
            return await super().call(tool, arguments)
    client = BoundClient()
    adapter = MupotAdapter(PlatformConfig(enabled=True, extra={
        "state_path": str(tmp_path / "state.json"), "expected_agent_id": "right-agent",
        "expected_tenant": "right-tenant"}), client_factory=lambda _: client)
    try:
        assert await adapter.connect() is accepted
        assert calls[0] == "boot_context"
        if not accepted:
            assert calls == ["boot_context"]
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize("role,caps", [("owner", []), ("admin", []), (None, []),
    ("member", None), ("member", [{"capability": "owner"}]), ("member", [{"capability": "admin"}])])
async def test_gateway_rejects_privileged_or_unverifiable_operator_before_mail(tmp_path, role, caps):
    calls = []
    class BoundClient(FakeMupotClient):
        async def call(self, tool, args):
            calls.append(tool)
            if tool == "boot_context":
                return {"tenant": "tenant-test", "bound_agent_id": "agent-test",
                        "channel": "workspace", "role": role, "capabilities": caps}
            return await super().call(tool, args)
    client = BoundClient()
    adapter = MupotAdapter(PlatformConfig(enabled=True, extra={
        "state_path": str(tmp_path / "state.json"), "expected_agent_id": "agent-test",
        "expected_tenant": "tenant-test"}), client_factory=lambda _: client)
    try:
        assert await adapter.connect() is False
        assert calls == ["boot_context"]
    finally:
        await adapter.disconnect()

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest

from gateway.config import PlatformConfig

from plugin.mupot_gateway.adapter import (  # noqa: E402
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
        return None

    async def close(self) -> None:
        return None

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool == "inbox_lease":
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
            return {"id": "m-2", "seq": 8}
        if tool == "inbox_consumer_status":
            return {"mode": "bearer_only", "generation": 0, "key_matches": True}
        raise AssertionError(f"unexpected tool: {tool} {arguments}")


class AckMupotClient(FakeMupotClient):
    def __init__(self, *, fail_ack_once: bool = False) -> None:
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
        self.ack_calls = 0

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool == "inbox_ack":
            self.ack_calls += 1
            if self.fail_ack_once and self.ack_calls == 1:
                return {"acked": [], "already_read": [], "refused": ["ack-1"]}
            self.acked = True
            return {"acked": ["ack-1"], "already_read": [], "refused": []}
        return await super().call(tool, arguments)


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
async def test_terminal_ack_failure_is_retried_before_commit(tmp_path: Path) -> None:
    client = AckMupotClient(fail_ack_once=True)
    handled: list[str] = []
    config = PlatformConfig(
        enabled=True,
        typing_indicator=False,
        extra={"poll_interval": 0.2, "state_path": str(tmp_path / "state.json")},
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
        for _ in range(200):
            if client.ack_calls >= 2 and client.acked:
                break
            await asyncio.sleep(0.01)
        assert client.ack_calls >= 2
        assert client.acked
        assert handled == []
        state = StateStore(tmp_path / "state.json").load()
        assert "ack-1" in state["processed"]
        assert len(state["terminal_receipts"]) == 1
    finally:
        await adapter.disconnect()


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

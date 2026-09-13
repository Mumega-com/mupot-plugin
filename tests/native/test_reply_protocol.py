"""Hostile terminal-reply durability and replay contract tests."""

from __future__ import annotations

import asyncio
import copy
from pathlib import Path
from typing import Any

import pytest

from gateway.config import PlatformConfig

from plugin.mupot_gateway.adapter import (
    MupotAdapter,
    MupotProtocolError,
    MupotTransportError,
    StateStore,
)


class SimulatedCrash(BaseException):
    """Stop execution at a crash boundary that ordinary recovery must not swallow."""


class ProtocolClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.messages_by_key: dict[str, dict[str, Any]] = {}
        self.acked_ids: list[str] = []
        self.fail_after_store_once = False
        self.crash_before_store_once = False
        self.malformed_receipt = False
        self.permanent_send_failure = False

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((tool, copy.deepcopy(arguments)))
        if tool == "inbox_consumer_status":
            return {
                "agent_id": "receiver",
                "mode": "bearer_only",
                "generation": 0,
                "key_matches": True,
            }
        if tool == "inbox_lease":
            return {
                "messages": [],
                "remaining": 0,
                "complete": True,
                "dead_lettered": 0,
                "lease_seconds": arguments["lease_seconds"],
            }
        if tool == "inbox_ack":
            message_id = arguments["ids"][0]
            self.acked_ids.append(message_id)
            return {"acked": [message_id], "already_read": [], "refused": []}
        if tool != "send":
            raise AssertionError(f"unexpected tool: {tool}")

        if self.crash_before_store_once:
            self.crash_before_store_once = False
            raise SimulatedCrash("process stopped before transport completion")
        if self.permanent_send_failure:
            raise MupotProtocolError("Mupot MCP request failed")

        key = arguments["request_id"]
        existing = self.messages_by_key.get(key)
        duplicate = existing is not None
        if existing is not None and existing != arguments:
            raise MupotProtocolError("Mupot MCP request failed")
        self.messages_by_key.setdefault(key, copy.deepcopy(arguments))
        if self.fail_after_store_once:
            self.fail_after_store_once = False
            raise MupotTransportError("Mupot request failed")
        if self.malformed_receipt:
            return {"id": ""}
        return {
            "id": f"delivery-{len(self.messages_by_key)}",
            "seq": len(self.messages_by_key),
            "duplicate": duplicate,
            "to": arguments["to"],
            "project_id": arguments.get("project_id"),
            "target_seat": None,
        }


def source_message(source_id: str = "source-1") -> dict[str, Any]:
    return {
        "id": source_id,
        "seq": 7,
        "tenant": "tenant-test",
        "to_agent": "receiver",
        "target_seat": "native",
        "from_agent": "sender",
        "from_member": "member-sender",
        "kind": "request",
        "body": "Please complete the task.",
        "request_id": "request-1",
        "in_reply_to": None,
        "created_at": "2026-09-13T10:00:00.000Z",
        "project_id": "project-1",
        "fenced_delivery_id": "lease-1",
        "body_length": 25,
        "checksum_sha256": "a" * 64,
        "is_intact": True,
        "expects_reply": True,
        "reply_basis": "request_id_field",
        "delivery_attempts": 1,
        "lease_expires_at": "2099-09-13T10:05:00.000Z",
    }


def adapter_at(tmp_path: Path, client: ProtocolClient) -> MupotAdapter:
    return MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "allowed_agents": "sender,receiver",
                "poll_interval": 0.01,
                "state_path": str(tmp_path / "state.json"),
                "notification_recipients": {"telegram": "owner"},
            },
        ),
        client_factory=lambda _server: client,
    )


async def bind_delivery(
    adapter: MupotAdapter,
    message: dict[str, Any] | None = None,
) -> None:
    event, _runtime = adapter._begin_delivery(message or source_message())
    await adapter.on_processing_start(event)


@pytest.mark.asyncio
async def test_progress_and_final_use_distinct_terminal_ack_keys_and_one_notice(
    tmp_path: Path,
) -> None:
    """Reusing the final key for progress makes the real final conflict and disappear."""
    client = ProtocolClient()
    adapter = adapter_at(tmp_path, client)
    await bind_delivery(adapter)

    first = await adapter.send(
        "sender", "still working", metadata={"_interim_send": True}
    )
    replay = await adapter.send(
        "sender", "still working", metadata={"_interim_send": True}
    )
    final = await adapter.send("sender", "Final answer.")

    assert first.success and replay.success and final.success
    progress_key = (
        "prog-f76248e305c7c3ff97ee68020794226ba"
        "3417eb42e0c0e35c7cd8c255e8c272d"
    )
    assert list(client.messages_by_key) == [progress_key, "resp-source-1"]
    assert client.messages_by_key[progress_key]["kind"] == "ack"
    assert client.messages_by_key["resp-source-1"] == {
        "to": "sender",
        "body": "Final answer.",
        "kind": "ack",
        "project_id": "project-1",
        "request_id": "resp-source-1",
        "in_reply_to": "source-1",
    }
    state = StateStore(tmp_path / "state.json").load()
    assert list(state["notification_outbox"]) == ["source-1"]
    assert state["reply_outbox"]["source-1"]["arguments"]["body"] == "Final answer."


@pytest.mark.asyncio
async def test_lost_final_receipt_retry_reuses_immutable_envelope_and_stores_once(
    tmp_path: Path,
) -> None:
    """An ambiguous response plus formatting fallback must not mint a second reply."""
    client = ProtocolClient()
    client.fail_after_store_once = True
    adapter = adapter_at(tmp_path, client)
    await bind_delivery(adapter)

    first = await adapter.send("sender", "Original exact final.")
    retry = await adapter.send("sender", "Fallback reformatted final.")

    assert first.success is False
    assert retry.success is True
    assert list(client.messages_by_key) == ["resp-source-1"]
    send_calls = [args for tool, args in client.calls if tool == "send"]
    assert len(send_calls) == 2
    assert send_calls[0] == send_calls[1]
    assert send_calls[1]["body"] == "Original exact final."
    persisted = StateStore(tmp_path / "state.json").load()
    assert persisted["reply_outbox"]["source-1"]["arguments"] == send_calls[0]


@pytest.mark.asyncio
async def test_restart_replays_prepared_final_without_another_model_turn(
    tmp_path: Path,
) -> None:
    """A crash after preparation must replay bytes from disk before leasing new work."""
    first_client = ProtocolClient()
    first_client.crash_before_store_once = True
    first = adapter_at(tmp_path, first_client)
    message = source_message()
    first._state["pending"] = {"message": copy.deepcopy(message)}
    first.store.save(first._state)
    await bind_delivery(first, message)

    with pytest.raises(SimulatedCrash):
        await first.send("sender", "Exact prepared final.")
    prepared = copy.deepcopy(StateStore(tmp_path / "state.json").load())

    replay_client = ProtocolClient()
    restarted = adapter_at(tmp_path, replay_client)
    model_turns: list[str] = []

    async def model(event: Any) -> str:
        model_turns.append(event.message_id)
        return "regenerated output"

    restarted.set_message_handler(model)
    assert await restarted.connect()
    try:
        for _ in range(100):
            if replay_client.acked_ids:
                break
            await asyncio.sleep(0.01)
        assert replay_client.acked_ids == ["source-1"]
    finally:
        await restarted.disconnect()

    assert model_turns == []
    sent = [args for tool, args in replay_client.calls if tool == "send"]
    assert sent == [prepared["reply_outbox"]["source-1"]["arguments"]]
    assert sent[0]["body"] == "Exact prepared final."


@pytest.mark.asyncio
async def test_two_native_receivers_exchange_one_final_ack_then_stop(
    tmp_path: Path,
) -> None:
    """A structured terminal ACK is a receipt, not another model request."""
    sender_client = ProtocolClient()
    receiver_a = adapter_at(tmp_path / "a", sender_client)
    receiver_a.set_message_handler(lambda _event: _async_value("Completed once."))
    await receiver_a._deliver(source_message("request-a"))

    assert sender_client.acked_ids == ["request-a"]
    assert len(sender_client.messages_by_key) == 1
    outgoing = next(iter(sender_client.messages_by_key.values()))
    incoming_ack = {
        "id": "ack-at-b",
        "seq": 8,
        "from_agent": "receiver",
        "from_member": "member-receiver",
        "kind": outgoing["kind"],
        "body": outgoing["body"],
        "request_id": outgoing["request_id"],
        "in_reply_to": outgoing["in_reply_to"],
        "project_id": outgoing["project_id"],
        "expects_reply": False,
    }

    receiver_client = ProtocolClient()
    receiver_b = adapter_at(tmp_path / "b", receiver_client)
    model_turns: list[str] = []
    receiver_b.set_message_handler(lambda event: _record_model(model_turns, event.message_id))
    await receiver_b._handle_ack_envelope(incoming_ack)

    assert model_turns == []
    assert receiver_client.acked_ids == ["ack-at-b"]
    assert receiver_client.messages_by_key == {}
    state = StateStore(tmp_path / "b" / "state.json").load()
    assert state["terminal_receipts"] == [incoming_ack]


async def _async_value(value: str) -> str:
    return value


async def _record_model(seen: list[str], message_id: str) -> str:
    seen.append(message_id)
    return "must not run"


@pytest.mark.asyncio
async def test_missing_final_receipt_and_empty_output_never_authorize_source_consume(
    tmp_path: Path,
) -> None:
    """Hermes success alone is insufficient without a concrete final send receipt."""
    malformed = ProtocolClient()
    malformed.malformed_receipt = True
    missing_receipt = adapter_at(tmp_path / "missing", malformed)
    missing_receipt.set_message_handler(lambda _event: _async_value("Final answer."))
    await missing_receipt._deliver(source_message("missing-receipt"))
    assert malformed.acked_ids == []

    empty_client = ProtocolClient()
    empty = adapter_at(tmp_path / "empty", empty_client)
    empty.set_message_handler(lambda _event: _async_value(""))
    await empty._deliver(source_message("empty-output"))
    assert empty_client.acked_ids == []
    assert empty_client.messages_by_key == {}


@pytest.mark.asyncio
async def test_legacy_pending_ambiguity_is_preserved_and_fences_connect(
    tmp_path: Path,
) -> None:
    """Old pending work has no immutable reply bytes and must never be regenerated."""
    state_path = tmp_path / "state.json"
    legacy = {
        "processed": ["done-1"],
        "terminal_receipts": [{"id": "ack-1"}],
        "notification_outbox": {"notice-1": {"status": "transport_unknown"}},
        "pending": {"message": source_message("legacy-pending")},
        "legacy_extension": {"version": 7},
    }
    StateStore(state_path).save(copy.deepcopy(legacy))
    client = ProtocolClient()
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(state_path)}),
        client_factory=lambda _server: client,
    )

    assert await adapter.connect() is False
    assert client.calls == []
    persisted = StateStore(state_path).load()
    assert persisted["pending"] == legacy["pending"]
    assert persisted["processed"] == legacy["processed"]
    assert persisted["terminal_receipts"] == legacy["terminal_receipts"]
    assert persisted["notification_outbox"] == legacy["notification_outbox"]
    assert persisted["legacy_extension"] == legacy["legacy_extension"]


@pytest.mark.asyncio
async def test_long_source_id_uses_bounded_stable_response_key(tmp_path: Path) -> None:
    """Raw source IDs that cannot fit the server RID grammar need a stable hash fallback."""
    client = ProtocolClient()
    adapter = adapter_at(tmp_path, client)
    await bind_delivery(adapter, source_message("x" * 200))

    result = await adapter.send("sender", "Final answer.")

    assert result.success is True
    assert list(client.messages_by_key) == [
        "resp-aa20c23e3201834050679e1d88941b9a6fed0557c9a705cb2c315e2e63fd486d"
    ]


@pytest.mark.asyncio
async def test_permanent_final_conflict_requires_restart_reconciliation(
    tmp_path: Path,
) -> None:
    """A possible request-id conflict must fence automatic replay and leasing."""
    client = ProtocolClient()
    client.permanent_send_failure = True
    adapter = adapter_at(tmp_path, client)
    message = source_message()
    adapter._state["pending"] = {"message": copy.deepcopy(message)}
    adapter.store.save(adapter._state)
    await bind_delivery(adapter, message)

    result = await adapter.send("sender", "Immutable final.")
    assert result.success is False
    prepared = StateStore(tmp_path / "state.json").load()["reply_outbox"]["source-1"]
    assert prepared["arguments"]["body"] == "Immutable final."
    assert prepared["status"] == "reconciliation_required"

    restarted_client = ProtocolClient()
    restarted = adapter_at(tmp_path, restarted_client)
    assert await restarted.connect() is False
    assert restarted_client.calls == []


@pytest.mark.asyncio
async def test_corrupt_persisted_envelope_returns_generic_failure_without_transport(
    tmp_path: Path,
) -> None:
    """Invalid durable state must fail closed without an unbound error-path variable."""
    client = ProtocolClient()
    adapter = adapter_at(tmp_path, client)
    message = source_message()
    adapter._state["reply_outbox"] = {
        "source-1": {"version": 1, "status": "prepared"}
    }
    adapter.store.save(adapter._state)
    await bind_delivery(adapter, message)

    result = await adapter.send("sender", "Final answer.")

    assert result.success is False
    assert result.error == "Mupot MCP request failed"
    assert [call for call in client.calls if call[0] == "send"] == []

from __future__ import annotations

import asyncio
import os
import threading
import time
from contextvars import Context
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from gateway.config import PlatformConfig
from gateway.platforms.base import ProcessingOutcome

from plugin.mupot_gateway.adapter import (  # noqa: E402
    MupotProtocolError,
    MupotTransportError,
    MupotAdapter,
    StateStore,
    build_mupot_event,
    is_ack_envelope,
    is_terminal_ack,
)


FAKE_LEASE_EXPIRY = "2099-01-01T00:00:00.000Z"
FAKE_SCOPE = {
    "tenant": "tenant-a",
    "agent_id": "agent-consumer",
    "effective_inbox_seat": None,
}


def fake_attempt_result(
    attempt_id: str,
    state: str,
    messages: list[dict] | None = None,
) -> dict:
    return {
        **FAKE_SCOPE,
        "attempt_id": attempt_id,
        "state": state,
        "lease_expires_at": FAKE_LEASE_EXPIRY if state == "leased" else None,
        "messages": messages or [],
        "consumed": False,
    }


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
            "from_member": "member-code",
            "body": "full Mupot answer",
            "project_id": "project-1",
            "request_id": "req-7",
            "in_reply_to": None,
            "kind": "message",
            "created_at": "2026-09-13T00:00:00.000Z",
            "delivery_attempts": 1,
            "lease_expires_at": FAKE_LEASE_EXPIRY,
        }

    async def connect(self) -> None:
        self.connect_calls += 1
        return None

    async def close(self) -> None:
        return None

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool == "inbox_lease":
            self.lease_calls += 1
            attempt_id = arguments.get("attempt_id")
            if isinstance(attempt_id, str):
                messages = [] if self.acked else [self.message]
                return fake_attempt_result(
                    attempt_id,
                    "empty" if self.acked else "leased",
                    messages,
                )
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
        if tool == "inbox_lease_ack":
            self.acked = True
            return {
                **FAKE_SCOPE,
                "attempt_id": arguments["attempt_id"],
                "state": "acked",
                "consumed": True,
            }
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
                "strict_scope": True,
                **FAKE_SCOPE,
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
            "from_member": "member-code",
            "body": "{ack_for:request-1} received",
            "request_id": "ack:request-1",
            "in_reply_to": "source-1",
            "kind": "ack",
            "expects_reply": False,
            "created_at": "2026-09-13T00:00:00.000Z",
            "delivery_attempts": 1,
            "lease_expires_at": FAKE_LEASE_EXPIRY,
        }
        self.fail_ack_once = fail_ack_once
        self.safe_ack_once = safe_ack_once
        self.ack_calls = 0

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool in {"inbox_ack", "inbox_lease_ack"}:
            self.ack_calls += 1
            if self.safe_ack_once and self.ack_calls == 1:
                from plugin.mupot_gateway import adapter as adapter_module

                safe_error = getattr(adapter_module, "MupotSafeRetryError", RuntimeError)
                raise safe_error("Mupot request failed")
            if self.fail_ack_once and self.ack_calls == 1:
                if tool == "inbox_lease_ack":
                    return {
                        **FAKE_SCOPE,
                        "attempt_id": arguments["attempt_id"],
                        "state": "expired",
                        "consumed": False,
                    }
                return {"acked": [], "already_read": [], "refused": ["ack-1"]}
            self.acked = True
            if tool == "inbox_lease_ack":
                return {
                    **FAKE_SCOPE,
                    "attempt_id": arguments["attempt_id"],
                    "state": "acked",
                    "consumed": True,
                }
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
            if callable(self.payload):
                return self.payload(arguments)  # type: ignore[no-any-return]
            return self.payload  # type: ignore[return-value]
        return await super().call(tool, arguments)


class TimedLeaseFailureClient(FakeMupotClient):
    def __init__(
        self,
        clock: list[float],
        outcomes: list[tuple[float, Exception]],
    ) -> None:
        super().__init__()
        self.clock = clock
        self.outcomes = outcomes
        self.first_lease = asyncio.Event()

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool == "inbox_lease":
            self.lease_calls += 1
            self.first_lease.set()
            issued_at, failure = self.outcomes.pop(0)
            self.clock[0] = issued_at
            raise failure
        return await super().call(tool, arguments)


class ReconciliationClient(FakeMupotClient):
    def __init__(self, status: object, reconcile_outcome: object = None) -> None:
        super().__init__()
        self.status = (
            {"strict_scope": True, **FAKE_SCOPE, **status}
            if isinstance(status, dict)
            else status
        )
        self.reconcile_outcome = reconcile_outcome
        self.tools: list[str] = []

    async def call(self, tool: str, arguments: dict) -> dict:
        self.tools.append(tool)
        if tool == "inbox_consumer_status":
            return self.status  # type: ignore[return-value]
        if tool == "inbox_lease_reconcile":
            if self.reconcile_outcome is not None:
                return self.reconcile_outcome  # type: ignore[return-value]
            return fake_attempt_result(arguments["attempt_id"], "cancelled")
        raise AssertionError(f"unexpected reconciliation tool: {tool} {arguments}")


class GenerationClient(FakeMupotClient):
    def __init__(self) -> None:
        super().__init__()
        self.acked_ids: list[str] = []

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool == "inbox_ack":
            message_id = arguments["ids"][0]
            self.acked_ids.append(message_id)
            return {"acked": [message_id], "already_read": [], "refused": []}
        return await super().call(tool, arguments)


def delivery_message(
    message_id: str,
    *,
    sender: str = "hadi-codex",
    project: str = "project-1",
    body: str = "run this turn",
    lease_expires_at: str | None = None,
) -> dict:
    message = {
        "id": message_id,
        "seq": 7,
        "from_agent": sender,
        "body": body,
        "project_id": project,
        "request_id": f"request-{message_id}",
        "in_reply_to": None,
        "kind": "message",
    }
    if lease_expires_at is not None:
        message["lease_expires_at"] = lease_expires_at
    return message


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
                "rpc_timeout": 5,
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
async def test_send_without_delivery_context_fails_closed(tmp_path: Path) -> None:
    client = GenerationClient()
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda *_: client,
    )
    adapter._begin_delivery(delivery_message("live-but-unbound"))

    result = await Context().run(
        lambda: asyncio.create_task(adapter.send("hadi-codex", "must not escape"))
    )

    assert result.success is False
    assert result.retryable is False
    assert client.sent == []


@pytest.mark.asyncio
async def test_wrong_recipient_and_inconsistent_callback_fail_closed(tmp_path: Path) -> None:
    client = GenerationClient()
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda *_: client,
    )
    event, runtime = adapter._begin_delivery(delivery_message("source-context"))
    await adapter.on_processing_start(event)

    result = await adapter.send("other-agent", "misdirected")
    event.raw_message = {**event.raw_message, "request_id": "other-request"}
    await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)

    assert result.success is False
    assert client.sent == []
    assert runtime.completion_event.is_set() is False


@pytest.mark.asyncio
async def test_callback_without_context_cannot_complete_live_generation(
    tmp_path: Path,
) -> None:
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda *_: GenerationClient(),
    )
    event, runtime = adapter._begin_delivery(delivery_message("source-no-context"))

    task = Context().run(
        lambda: asyncio.create_task(
            adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)
        )
    )
    await task

    assert runtime.completion_event.is_set() is False
    assert runtime.outcome is None


@pytest.mark.asyncio
async def test_expired_leased_event_never_starts_model_work(tmp_path: Path) -> None:
    client = GenerationClient()
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda *_: client,
    )
    handled: list[str] = []

    async def handler(event):
        handled.append(event.message_id)
        return "unexpected"

    adapter.set_message_handler(handler)
    expired = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()

    await adapter._deliver(delivery_message("expired", lease_expires_at=expired))

    assert handled == []
    assert client.sent == []
    assert client.acked_ids == []


@pytest.mark.asyncio
async def test_queued_event_expiry_cancels_session_before_model_start(tmp_path: Path) -> None:
    client = GenerationClient()
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda *_: client,
    )
    adapter.turn_timeout = 0.05
    handled: list[str] = []

    async def handler(event):
        handled.append(event.message_id)
        return "unexpected"

    adapter.set_message_handler(handler)
    message = delivery_message("queued-expiry")
    preview = build_mupot_event(message, adapter.platform)
    session_key = adapter._event_session_key(preview)
    blocker = asyncio.create_task(asyncio.Event().wait())
    adapter._active_sessions[session_key] = asyncio.Event()
    adapter._session_tasks[session_key] = blocker
    adapter._background_tasks.add(blocker)
    try:
        await adapter._deliver(message)
        assert handled == []
        assert session_key not in adapter._pending_messages
        assert blocker.cancelled()
        assert client.sent == []
    finally:
        blocker.cancel()
        await asyncio.gather(blocker, return_exceptions=True)


@pytest.mark.asyncio
async def test_timeout_invalidates_generation_before_bounded_cancellation(
    tmp_path: Path,
) -> None:
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda *_: GenerationClient(),
    )
    adapter.turn_timeout = 0.02
    adapter.cancel_timeout = 0.02
    cancellation_started = asyncio.Event()
    registry_snapshots = []

    async def handler(_event):
        await asyncio.Event().wait()

    async def stuck_cancel(session_key):
        registry_snapshots.append((session_key, dict(adapter._live_generations)))
        cancellation_started.set()
        await asyncio.Event().wait()

    adapter.set_message_handler(handler)
    adapter.cancel_session_processing = stuck_cancel
    started = time.monotonic()
    await adapter._deliver(delivery_message("bounded-timeout"))
    elapsed = time.monotonic() - started

    assert cancellation_started.is_set()
    assert registry_snapshots[0][1] == {}
    assert elapsed < 0.2
    pending = StateStore(tmp_path / "state.json").load()["pending"]
    assert pending["message"]["id"] == "bounded-timeout"
    await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_disconnect_invalidates_generation_before_surviving_callback(
    tmp_path: Path,
) -> None:
    client = GenerationClient()
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda *_: client,
    )
    adapter.turn_timeout = 1.0
    started = asyncio.Event()
    late_release = asyncio.Event()
    late_done = asyncio.Event()
    late_results = []
    cancellation_snapshots = []
    original_cancel = adapter.cancel_session_processing

    async def checking_cancel(session_key):
        cancellation_snapshots.append((session_key, dict(adapter._live_generations)))
        await original_cancel(session_key)

    adapter.cancel_session_processing = checking_cancel

    async def handler(event):
        async def finish_after_disconnect() -> None:
            await late_release.wait()
            late_results.append(await adapter.send(event.source.chat_id, "late output"))
            late_done.set()

        asyncio.create_task(finish_after_disconnect())
        started.set()
        await asyncio.Event().wait()

    adapter.set_message_handler(handler)
    delivery = asyncio.create_task(adapter._deliver(delivery_message("disconnect-source")))
    try:
        await asyncio.wait_for(started.wait(), 1)
        await adapter.disconnect()
        await asyncio.wait_for(delivery, 1)
        assert adapter._live_generations == {}
        assert cancellation_snapshots[0][1] == {}

        late_release.set()
        await asyncio.wait_for(late_done.wait(), 1)
        assert len(late_results) == 1
        assert late_results[0].success is False
        assert client.sent == []
        pending = StateStore(tmp_path / "state.json").load()["pending"]
        assert pending["message"]["id"] == "disconnect-source"
    finally:
        late_release.set()
        await asyncio.gather(delivery, return_exceptions=True)


@pytest.mark.asyncio
async def test_timed_out_thread_callback_cannot_send_with_next_delivery_context(
    tmp_path: Path,
) -> None:
    client = GenerationClient()
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "state_path": str(tmp_path / "state.json"),
                "notification_recipients": {"telegram": "owner"},
            },
        ),
        client_factory=lambda *_: client,
    )
    adapter.turn_timeout = 0.05
    thread_ready = threading.Event()
    release_thread = threading.Event()
    thread_done = threading.Event()
    late_results = []
    b_started = asyncio.Event()
    release_b = asyncio.Event()
    late_event = None

    async def handler(event):
        nonlocal late_event
        if event.message_id == "source-a":
            late_event = event
            loop = asyncio.get_running_loop()

            def late_thread_callback() -> None:
                thread_ready.set()
                release_thread.wait(2)
                future = asyncio.run_coroutine_threadsafe(
                    adapter.send(event.source.chat_id, "late A final"), loop
                )
                late_results.append(future.result(2))
                completion = asyncio.run_coroutine_threadsafe(
                    adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS), loop
                )
                completion.result(2)
                thread_done.set()

            await asyncio.to_thread(late_thread_callback)
            return "late A handler return"
        b_started.set()
        await release_b.wait()
        return "B final"

    adapter.set_message_handler(handler)
    a = delivery_message("source-a", sender="hadi-codex", project="project-a")
    b = delivery_message("source-b", sender="kasra", project="project-b")
    try:
        await adapter._deliver(a)
        assert thread_ready.wait(1)
        adapter.turn_timeout = 1.0
        b_delivery = asyncio.create_task(adapter._deliver(b))
        await asyncio.wait_for(b_started.wait(), 1)
        release_thread.set()
        await asyncio.wait_for(asyncio.to_thread(thread_done.wait, 2), 3)

        assert len(late_results) == 1
        assert late_results[0].success is False
        assert b_delivery.done() is False
        assert client.sent == []
        assert StateStore(tmp_path / "state.json").load().get("notification_outbox") == {}

        release_b.set()
        await asyncio.wait_for(b_delivery, 1)
        assert client.sent == [
            {
                "to": "kasra",
                "body": "B final",
                "kind": "ack",
                "project_id": "project-b",
                "request_id": "resp-source-b",
                "in_reply_to": "source-b",
            }
        ]
        notices = StateStore(tmp_path / "state.json").load()["notification_outbox"]
        assert list(notices) == ["source-b"]
        assert client.acked_ids == ["source-b"]
    finally:
        release_thread.set()
        release_b.set()
        await adapter.cancel_background_tasks()


@pytest.mark.asyncio
async def test_late_same_source_redelivery_cannot_complete_new_generation(
    tmp_path: Path,
) -> None:
    client = GenerationClient()
    adapter = MupotAdapter(
        PlatformConfig(enabled=True, extra={"state_path": str(tmp_path / "state.json")}),
        client_factory=lambda *_: client,
    )
    adapter.turn_timeout = 0.05
    late_release = asyncio.Event()
    late_done = asyncio.Event()
    b_started = asyncio.Event()
    release_b = asyncio.Event()
    handler_calls = 0

    async def handler(event):
        nonlocal handler_calls
        handler_calls += 1
        if handler_calls == 1:
            async def finish_late() -> None:
                await late_release.wait()
                await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)
                late_done.set()

            asyncio.create_task(finish_late())
            await asyncio.Event().wait()
        b_started.set()
        await release_b.wait()
        return "redelivery final"

    adapter.set_message_handler(handler)
    message = delivery_message(
        "same-source",
        sender="hadi-codex",
        project="same-project",
        body="same body",
    )
    try:
        await adapter._deliver(message)
        adapter.turn_timeout = 1.0
        b_delivery = asyncio.create_task(adapter._deliver(dict(message)))
        await asyncio.wait_for(b_started.wait(), 1)
        late_release.set()
        await asyncio.wait_for(late_done.wait(), 1)
        await asyncio.sleep(0)
        assert b_delivery.done() is False
        assert client.acked_ids == []

        release_b.set()
        await asyncio.wait_for(b_delivery, 1)
        assert client.acked_ids == ["same-source"]
    finally:
        late_release.set()
        release_b.set()
        await adapter.cancel_background_tasks()


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
                "kind": "ack",
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
    assert marker["version"] == 3
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
    client = LeasePayloadClient(
        lambda args: fake_attempt_result(args["attempt_id"], "empty")
    )
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
    client = LeasePayloadClient(
        lambda args: fake_attempt_result(args["attempt_id"], "empty")
    )
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
    state_path = tmp_path / "state.json"
    StateStore(state_path).save({
        "lease_reconciliation": {
            "version": 1,
            "required": True,
            "agent_id": "agent-consumer",
            "mode": "bearer_only",
            "generation": 0,
            "reconcile_after": 106.0,
        }
    })
    monkeypatch.setattr(time, "time", lambda: 105.999)
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
    monkeypatch.setattr(time, "time", lambda: 106.0)
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
    assert client.tools == ["inbox_consumer_status", "inbox_lease_reconcile"]
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
    monkeypatch.setattr(time, "time", lambda: 106.0)
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
                return {"strict_scope": True, "tenant": tenant,
                        "agent_id": agent, "effective_inbox_seat": None,
                        "mode": "bearer_only", "generation": 0,
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

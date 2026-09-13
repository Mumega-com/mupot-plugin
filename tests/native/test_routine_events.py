from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from pathlib import Path

import pytest
import yaml
from gateway.config import PlatformConfig

from plugin.mupot_gateway.adapter import MupotAdapter, StateStore
from plugin.mupot_gateway.routine_events import (
    RoutineEventValidationError,
    is_routine_event_candidate,
    validate_routine_event,
)


def _utf16_length(value: str) -> int:
    return len(value.encode("utf-16-le")) // 2


def routine_body(**updates: object) -> str:
    value: dict[str, object] = {
        "version": "routine.human-wait/v1",
        "type": "routine_human_wait",
        "project_id": "project-1",
        "run_id": "run-1",
        "action_key": "question-1",
        "reason": "answer",
        "decision": {
            "type": "answer",
            "question": "Which receipt is authoritative?",
            "choices": ["Booked", "Paid"],
        },
    }
    value.update(updates)
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def routine_message(**updates: object) -> dict[str, object]:
    body = str(updates.pop("body", routine_body()))
    value: dict[str, object] = {
        "seq": 41,
        "id": "routine-message-1",
        "from_agent": "mupot-routines",
        "from_member": "system:routines",
        "kind": "ack",
        "body": body,
        "request_id": "routine-human:run-1:question-1",
        "in_reply_to": None,
        "created_at": "2026-09-13T10:00:00.000Z",
        "project_id": "project-1",
        "target_seat": None,
        "body_length": _utf16_length(body),
        "checksum_sha256": hashlib.sha256(body.encode()).hexdigest(),
        "is_intact": True,
        "expects_reply": False,
        "reply_basis": "ack_is_terminal",
        "delivery_attempts": 1,
        "lease_expires_at": "2026-09-13T10:05:00.000Z",
    }
    value.update(updates)
    return value


class RoutineClient:
    def __init__(self, *, already_read: bool = False, before_ack=None) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.already_read = already_read
        self.connect_calls = 0
        self.before_ack = before_ack

    async def connect(self) -> None:
        self.connect_calls += 1

    async def call(self, tool: str, arguments: dict) -> dict:
        self.calls.append((tool, copy.deepcopy(arguments)))
        if tool != "inbox_ack":
            raise AssertionError(f"routine receive must not call {tool}")
        if self.before_ack is not None:
            self.before_ack()
        message_id = arguments["ids"][0]
        if self.already_read:
            return {"acked": [], "already_read": [message_id], "refused": []}
        self.already_read = True
        return {"acked": [message_id], "already_read": [], "refused": []}


class PollRoutineClient(RoutineClient):
    def __init__(self, message: dict[str, object]) -> None:
        super().__init__()
        self.message = message
        self.leased = False

    async def close(self) -> None:
        return None

    async def call(self, tool: str, arguments: dict) -> dict:
        if tool == "inbox_consumer_status":
            self.calls.append((tool, copy.deepcopy(arguments)))
            return {
                "agent_id": "agent-consumer",
                "mode": "bearer_only",
                "generation": 0,
                "key_matches": True,
            }
        if tool == "inbox_lease":
            self.calls.append((tool, copy.deepcopy(arguments)))
            if self.leased:
                messages = []
            else:
                self.leased = True
                messages = [copy.deepcopy(self.message)]
            return {
                "messages": messages,
                "remaining": 0,
                "complete": True,
                "dead_lettered": 0,
                "lease_seconds": arguments["lease_seconds"],
            }
        return await super().call(tool, arguments)


def adapter_at(
    tmp_path: Path,
    client: RoutineClient,
    *,
    enabled: bool = True,
    injector=None,
) -> MupotAdapter:
    return MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={
                "allowed_agents": "kasra",
                "routine_events_enabled": enabled,
                "state_path": str(tmp_path / "state.json"),
                "notification_recipients": {"telegram": "owner"},
            },
        ),
        client_factory=lambda *_: client,
        message_injector=injector,
    )


def test_validates_exact_server_human_wait_contract_and_stable_hashed_key() -> None:
    event = validate_routine_event(routine_message())
    assert event.source_id == "routine-message-1"
    assert event.project_id == "project-1"
    assert event.run_id == "run-1"
    assert event.action_key == "question-1"
    assert event.reason == "answer"
    assert "fetch the current allowed actions from mupot" in event.notice.lower()
    assert "not executable consent" in event.notice

    run_id = "r" * 120
    action_key = "a" * 120
    body = routine_body(run_id=run_id, action_key=action_key)
    expected = (
        "routine-human:" + hashlib.sha256(f"{run_id}:{action_key}".encode()).hexdigest()
    )
    hashed = validate_routine_event(
        routine_message(body=body, project_id="project-1", request_id=expected)
    )
    assert hashed.request_id == expected


@pytest.mark.parametrize(
    ("updates", "body_updates"),
    [
        ({"from_agent": "agent:mupot-routines"}, {}),
        ({"from_member": "member:routines"}, {}),
        ({"kind": "request"}, {}),
        ({"expects_reply": True}, {}),
        ({"reply_basis": "request_id_field"}, {}),
        ({"is_intact": None}, {}),
        ({"body_length": 1}, {}),
        ({"checksum_sha256": "0" * 64}, {}),
        ({"delivery_attempts": 0}, {}),
        ({"id": "bad source"}, {}),
        ({"body": "x" * 8001}, {}),
        ({"lease_expires_at": "not-an-instant"}, {}),
        ({"request_id": "routine-human:run-1:other"}, {}),
        ({"project_id": "project-other"}, {}),
        ({"in_reply_to": "legacy-parent"}, {}),
        ({}, {"version": "routine.human-wait/v0"}),
        ({}, {"type": "routine_run"}),
        ({}, {"run_id": "run id"}),
        ({}, {"action_key": "bad/action"}),
        ({}, {"reason": "review"}),
        ({}, {"decision": {"type": "review", "task_id": "task-1"}}),
        ({}, {"unexpected": True}),
    ],
)
def test_refuses_each_spoof_or_legacy_dimension(
    updates: dict[str, object], body_updates: dict[str, object]
) -> None:
    if body_updates:
        updates = {**updates, "body": routine_body(**body_updates)}
    with pytest.raises(RoutineEventValidationError):
        validate_routine_event(routine_message(**updates))


def test_rejects_duplicate_json_keys_and_truncated_summary_is_never_consent() -> None:
    duplicate = routine_body().replace(
        '"run_id":"run-1"', '"run_id":"spoof","run_id":"run-1"'
    )
    with pytest.raises(RoutineEventValidationError):
        validate_routine_event(routine_message(body=duplicate))

    body = routine_body(
        decision={
            "type": "answer",
            "question": "Decision summary omitted to fit the message limit.",
            "choices": [],
            "truncated": True,
        }
    )
    event = validate_routine_event(routine_message(body=body))
    assert "truncated" in event.notice.lower()
    assert "fetch the current allowed actions from mupot" in event.notice.lower()
    assert "not executable consent" in event.notice


def test_candidate_detection_catches_every_synthetic_spoof_route() -> None:
    assert is_routine_event_candidate(routine_message())
    assert is_routine_event_candidate(routine_message(from_agent="kasra"))
    assert is_routine_event_candidate(
        routine_message(from_agent="kasra", from_member="member-kasra")
    )
    assert is_routine_event_candidate(
        routine_message(
            from_agent="kasra",
            from_member="member-kasra",
            request_id="request-other",
        )
    )
    malformed = routine_body().replace(
        '"run_id":"run-1"', '"run_id":"spoof","run_id":"run-1"'
    )
    assert is_routine_event_candidate(
        routine_message(
            from_agent="kasra",
            from_member="member-kasra",
            request_id="request-other",
            body=malformed,
        )
    )


@pytest.mark.asyncio
async def test_valid_event_custody_then_exact_ack_then_processed_then_one_activation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from plugin.mupot_gateway import notifications

    activations: list[tuple[str, dict]] = []
    state_path = tmp_path / "state.json"
    ordering: list[str] = []

    def before_ack() -> None:
        state = StateStore(state_path).load()
        assert (
            state["routine_event_receipts"]["routine-message-1"]["status"] == "custody"
        )
        assert (
            state["notification_outbox"]["routine-message-1"]["custody_status"]
            == "durable"
        )
        assert "routine-message-1" not in state.get("processed", [])
        ordering.append("ack")

    client = RoutineClient(before_ack=before_ack)

    def activate(content, **kwargs):
        state = StateStore(state_path).load()
        assert "routine-message-1" in state["processed"]
        ordering.append("activate")
        activations.append((content, kwargs))
        return True

    adapter = adapter_at(
        tmp_path,
        client,
        injector=activate,
    )
    model_turns: list[str] = []
    adapter.set_message_handler(lambda event: model_turns.append(event.text))  # type: ignore[arg-type]
    monkeypatch.setattr(
        notifications,
        "active_sessions",
        lambda: [
            {
                "id": "human",
                "session_key": "agent:main:telegram:dm:123",
                "source": "telegram",
                "user_id": "owner",
                "chat_id": "123",
                "chat_type": "dm",
                "last_active": 1,
            }
        ],
    )

    await adapter._process_leased_message(routine_message())
    state = StateStore(tmp_path / "state.json").load()
    assert client.calls == [("inbox_ack", {"ids": ["routine-message-1"]})]
    assert model_turns == []
    assert "mupot-routines" not in adapter.allowed_agents
    assert state["routine_event_receipts"]["routine-message-1"]["status"] == "processed"
    assert state["processed"] == ["routine-message-1"]
    assert (
        state["notification_outbox"]["routine-message-1"]["activation_status"]
        == "not_started"
    )
    assert activations == []

    await adapter._flush_notifications()
    assert len(activations) == 1
    assert ordering == ["ack", "activate"]
    assert activations[0][1] == {"session_key": "agent:main:telegram:dm:123"}
    assert "not executable consent" in activations[0][0]
    notice = StateStore(tmp_path / "state.json").load()["notification_outbox"][
        "routine-message-1"
    ]
    assert notice["activation_status"] == "queued"
    assert notice["delivery_status"] == "pending"
    await adapter._flush_notifications()
    assert len(activations) == 1


@pytest.mark.asyncio
async def test_poll_loop_routes_routine_to_private_activation_without_peer_turn_or_send(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from plugin.mupot_gateway import adapter as adapter_module
    from plugin.mupot_gateway import notifications

    client = PollRoutineClient(routine_message())
    activations: list[str] = []
    adapter = adapter_at(
        tmp_path,
        client,
        injector=lambda content, **_kwargs: activations.append(content) or True,
    )
    adapter.poll_interval = 0.01
    peer_turns: list[str] = []

    async def peer_handler(event) -> None:
        peer_turns.append(event.text)

    adapter.set_message_handler(peer_handler)
    monkeypatch.setattr(
        adapter_module, "require_supported_profile_runtime", lambda _config: None
    )
    monkeypatch.setattr(
        notifications,
        "active_sessions",
        lambda: [
            {
                "id": "human",
                "session_key": "agent:main:telegram:dm:123",
                "source": "telegram",
                "user_id": "owner",
                "chat_id": "123",
                "chat_type": "dm",
                "last_active": 1,
            }
        ],
    )

    assert await adapter.connect() is True
    try:
        for _ in range(100):
            if activations:
                break
            await asyncio.sleep(0.01)
        assert len(activations) == 1
        assert peer_turns == []
        assert not any(tool == "send" for tool, _arguments in client.calls)
        assert ("inbox_ack", {"ids": ["routine-message-1"]}) in client.calls
    finally:
        await adapter.disconnect()


@pytest.mark.asyncio
async def test_custody_failure_makes_zero_source_ack_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = RoutineClient()
    adapter = adapter_at(tmp_path, client)
    monkeypatch.setattr(
        adapter.store, "save", lambda _value: (_ for _ in ()).throw(OSError("disk"))
    )
    with pytest.raises(OSError, match="disk"):
        await adapter._handle_routine_event(routine_message())
    assert client.calls == []


@pytest.mark.asyncio
async def test_crash_after_ack_before_processed_replays_exact_ack_and_then_activates_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from plugin.mupot_gateway import notifications

    client = RoutineClient()
    premature_activations: list[str] = []
    first = adapter_at(
        tmp_path,
        client,
        injector=lambda content, **_kwargs: (
            premature_activations.append(content) or True
        ),
    )
    monkeypatch.setattr(
        notifications,
        "active_sessions",
        lambda: [
            {
                "id": "human",
                "session_key": "agent:main:telegram:dm:123",
                "source": "telegram",
                "user_id": "owner",
                "chat_id": "123",
                "chat_type": "dm",
                "last_active": 1,
            }
        ],
    )
    real_save = first.store.save

    def crash_before_processed(value: dict) -> None:
        if "routine-message-1" in value.get("processed", []):
            raise OSError("crash before processed")
        real_save(value)

    monkeypatch.setattr(first.store, "save", crash_before_processed)
    with pytest.raises(OSError, match="crash before processed"):
        await first._handle_routine_event(routine_message())
    durable = StateStore(tmp_path / "state.json").load()
    assert durable["routine_event_receipts"]["routine-message-1"]["status"] == "custody"
    assert durable.get("processed", []) == []
    assert client.calls == [("inbox_ack", {"ids": ["routine-message-1"]})]
    await first._flush_notifications()
    assert premature_activations == []

    activations: list[str] = []
    restarted_client = RoutineClient(already_read=True)
    restarted = adapter_at(
        tmp_path,
        restarted_client,
        injector=lambda content, **_kwargs: activations.append(content) or True,
    )
    await restarted._replay_routine_events()
    assert restarted_client.calls == [("inbox_ack", {"ids": ["routine-message-1"]})]
    assert StateStore(tmp_path / "state.json").load()["processed"] == [
        "routine-message-1"
    ]
    await restarted._flush_notifications()
    await restarted._flush_notifications()
    assert len(activations) == 1


@pytest.mark.asyncio
async def test_crash_between_receipt_and_notice_restarts_with_one_notice_and_one_ack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from plugin.mupot_gateway import notifications

    first_client = RoutineClient()
    first = adapter_at(tmp_path, first_client)
    real_save = first.store.save
    failed = False

    def crash_on_notice(value: dict) -> None:
        nonlocal failed
        if value.get("notification_outbox") and not failed:
            failed = True
            raise OSError("crash before notice custody")
        real_save(value)

    monkeypatch.setattr(first.store, "save", crash_on_notice)
    with pytest.raises(OSError, match="crash before notice custody"):
        await first._handle_routine_event(routine_message())
    durable = StateStore(tmp_path / "state.json").load()
    assert durable["routine_event_receipts"]["routine-message-1"]["status"] == "custody"
    assert durable.get("notification_outbox", {}) == {}
    assert first_client.calls == []

    activations: list[str] = []
    restarted_client = RoutineClient()
    restarted = adapter_at(
        tmp_path,
        restarted_client,
        injector=lambda content, **_kwargs: activations.append(content) or True,
    )
    monkeypatch.setattr(
        notifications,
        "active_sessions",
        lambda: [
            {
                "id": "human",
                "session_key": "agent:main:telegram:dm:123",
                "source": "telegram",
                "user_id": "owner",
                "chat_id": "123",
                "chat_type": "dm",
                "last_active": 1,
            }
        ],
    )
    await restarted._replay_routine_events()
    await restarted._flush_notifications()
    await restarted._flush_notifications()
    state = StateStore(tmp_path / "state.json").load()
    assert restarted_client.calls == [("inbox_ack", {"ids": ["routine-message-1"]})]
    assert list(state["notification_outbox"]) == ["routine-message-1"]
    assert len(activations) == 1


@pytest.mark.asyncio
async def test_invalid_routine_spoof_is_durably_quarantined_without_model_or_activation(
    tmp_path: Path,
) -> None:
    client = RoutineClient()
    activations: list[str] = []
    adapter = adapter_at(
        tmp_path,
        client,
        injector=lambda content, **_kwargs: activations.append(content) or True,
    )
    model_turns: list[str] = []
    adapter.set_message_handler(lambda event: model_turns.append(event.text))  # type: ignore[arg-type]
    spoof = routine_message(from_member="attacker")

    await adapter._process_leased_message(spoof)
    state = StateStore(tmp_path / "state.json").load()
    assert (
        state["routine_event_quarantine"]["routine-message-1"]["reason"]
        == "invalid_routine_event"
    )
    assert state["processed"] == ["routine-message-1"]
    assert "routine-message-1" not in state.get("notification_outbox", {})
    assert model_turns == []
    assert activations == []
    assert client.calls == [("inbox_ack", {"ids": ["routine-message-1"]})]


@pytest.mark.asyncio
async def test_processed_source_id_cannot_bypass_routine_spoof_validation(
    tmp_path: Path,
) -> None:
    first_client = RoutineClient()
    first = adapter_at(tmp_path, first_client)
    await first._process_leased_message(routine_message())
    original = copy.deepcopy(StateStore(tmp_path / "state.json").load())

    restarted_client = RoutineClient()
    restarted = adapter_at(tmp_path, restarted_client)
    with pytest.raises(RuntimeError, match="conflict"):
        await restarted._process_leased_message(
            routine_message(from_member="attacker", delivery_attempts=2)
        )
    assert restarted_client.calls == []
    assert StateStore(tmp_path / "state.json").load() == original


@pytest.mark.asyncio
async def test_disabled_config_keeps_routine_path_absent_and_never_expands_peer_allowlist(
    tmp_path: Path,
) -> None:
    client = RoutineClient()
    adapter = adapter_at(tmp_path, client, enabled=False)
    model_turns: list[str] = []
    adapter.set_message_handler(lambda event: model_turns.append(event.text))  # type: ignore[arg-type]

    await adapter._process_leased_message(routine_message())
    state = StateStore(tmp_path / "state.json").load()
    assert adapter.routine_events_enabled is False
    assert "mupot-routines" not in adapter.allowed_agents
    assert (
        state["routine_event_quarantine"]["routine-message-1"]["reason"]
        == "routine_events_disabled"
    )
    assert state["processed"] == ["routine-message-1"]
    assert model_turns == []


def test_config_requires_real_boolean_and_example_explicitly_enables_routines(
    tmp_path: Path,
) -> None:
    client = RoutineClient()
    with pytest.raises(ValueError, match="routine_events_enabled"):
        MupotAdapter(
            PlatformConfig(
                enabled=True,
                extra={
                    "routine_events_enabled": "true",
                    "state_path": str(tmp_path / "state.json"),
                },
            ),
            client_factory=lambda *_: client,
        )

    example = yaml.safe_load(
        (
            Path(__file__).resolve().parents[2] / "examples/native-gateway-config.yaml"
        ).read_text()
    )
    assert example["mupot"]["routine_events_enabled"] is True
    assert "mupot-routines" not in example["mupot"]["allowed_agents"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ledger",
    [[], {"routine-message-1": {"status": "processed"}}],
)
async def test_corrupt_routine_ledger_fails_closed_before_network(
    tmp_path: Path,
    ledger: object,
) -> None:
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps({"routine_event_receipts": ledger}) + "\n", encoding="utf-8"
    )
    client = RoutineClient()
    adapter = MupotAdapter(
        PlatformConfig(
            enabled=True,
            extra={"routine_events_enabled": True, "state_path": str(path)},
        ),
        client_factory=lambda *_: client,
        message_injector=lambda *_args, **_kwargs: True,
    )
    assert await adapter.connect() is False
    assert client.calls == []
    assert client.connect_calls == 0

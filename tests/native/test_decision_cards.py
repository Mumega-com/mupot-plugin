from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from gateway.config import PlatformConfig

from plugin.mupot_gateway.adapter import MupotAdapter, StateStore

TASK = "abcdef12-0000-4000-8000-000000000001"
SHORT = "abcdef12"


def card_body(**updates: object) -> str:
    value: dict[str, object] = {
        "type": "decision_card",
        "task_id": TASK,
        "short_id": SHORT,
        "title": "Decide the thing",
        "priority": "P1",
        "project": "Telegram pilot",
        "asked_by": "kasra",
        "result_excerpt": "All tests pass.",
        "approve": f"/muapprove {SHORT}",
        "reject": f"/mureject {SHORT}",
        "dashboard_url": "https://mupot.mumega.com/approvals",
    }
    value.update(updates)
    return json.dumps(value)


def message(**updates: object) -> dict[str, object]:
    value: dict[str, object] = {
        "seq": 7,
        "id": "msg-card-1",
        "from_agent": "mupot-decision-card",
        "kind": "request",
        "body": card_body(),
        "request_id": f"decision-card:{TASK}:2026-10-17 10:00:00:agent-1",
    }
    value.update(updates)
    return value


class Client:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def call(self, tool: str, arguments: dict) -> dict:
        self.calls.append((tool, copy.deepcopy(arguments)))
        if tool != "inbox_ack":
            raise AssertionError(f"decision card receive must not call {tool}")
        return {"acked": list(arguments["ids"]), "already_read": [], "refused": []}


def adapter_at(
    tmp_path: Path,
    client: Client,
    *,
    enabled: bool = True,
    recipients: bool = True,
    injector=None,
    **more: object,
) -> MupotAdapter:
    extra: dict[str, object] = {**more,
        "allowed_agents": "kasra",
        "decision_cards_enabled": enabled,
        "state_path": str(tmp_path / "state.json"),
    }
    if recipients:
        extra["notification_recipients"] = {"telegram": "123456789"}
    return MupotAdapter(
        PlatformConfig(enabled=True, extra=extra),
        client_factory=lambda *_: client,
        message_injector=injector,
    )


def state(tmp_path: Path) -> dict:
    return StateStore(tmp_path / "state.json").load()


@pytest.mark.asyncio
async def test_valid_card_is_enqueued_durably_and_acked_exactly_once(
    tmp_path: Path,
) -> None:
    client = Client()
    adapter = adapter_at(tmp_path, client)
    turns: list[str] = []
    adapter.set_message_handler(lambda event: turns.append(event.text))  # type: ignore[arg-type]
    assert "mupot-decision-card" not in adapter.allowed_agents

    await adapter._process_leased_message(message())

    assert client.calls == [("inbox_ack", {"ids": ["msg-card-1"]})]
    assert turns == []
    saved = state(tmp_path)
    notice = saved["notification_outbox"]["msg-card-1"]
    assert notice["plain_card"] is True
    assert notice["custody_status"] == "durable"
    assert notice["text"].startswith("Decision needed: Decide the thing\n")
    assert f"Reply: /muapprove {SHORT}   or   /mureject {SHORT}" in notice["text"]
    assert "Mupot update" not in notice["text"]
    assert "activation_required" not in notice
    assert saved["processed"] == ["msg-card-1"]
    assert saved["dlq"] == []

    # A redelivery of the same row re-acks but never enqueues a second notice.
    await adapter._process_leased_message(message())
    assert len(state(tmp_path)["notification_outbox"]) == 1
    assert [c[0] for c in client.calls] == ["inbox_ack", "inbox_ack"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad",
    [
        message(body=card_body(type="decision_card", short_id="zzzzzzzz")),
        message(body=card_body(approve="/muapprove 12345678")),
        message(body=card_body(reject="/mureject abcdef12 now")),
        message(body=card_body(result_excerpt="x" * 9000)),
        message(body='{"type":"decision_card"'),
        message(request_id="decision-card:someone-elses-task:x:agent"),
    ],
)
async def test_malformed_or_forged_card_goes_to_dlq_not_the_outbox(
    tmp_path: Path, bad: dict[str, object]
) -> None:
    client = Client()
    adapter = adapter_at(tmp_path, client)
    await adapter._process_leased_message(bad)
    saved = state(tmp_path)
    assert saved.get("notification_outbox", {}) == {}
    assert [entry["reason"] for entry in saved["dlq"]] == ["invalid_decision_card"]
    assert client.calls == [("inbox_ack", {"ids": ["msg-card-1"]})]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "msg",
    [
        message(from_agent="mupot-review-gate", body=json.dumps({"type": "task_review", "task_id": TASK})),
        # review-gate is NOT the card sender: even a well-formed card from it is untouched.
        message(from_agent="mupot-review-gate"),
    ],
)
async def test_review_gate_messages_keep_sender_policy_behaviour(
    tmp_path: Path, msg: dict[str, object]
) -> None:
    client = Client()
    adapter = adapter_at(tmp_path, client)
    await adapter._process_leased_message(msg)
    saved = state(tmp_path)
    assert [entry["reason"] for entry in saved["dlq"]] == ["sender_policy"]
    assert saved.get("notification_outbox", {}) == {}


@pytest.mark.asyncio
async def test_card_from_any_other_sender_is_never_treated_as_a_card(
    tmp_path: Path,
) -> None:
    client = Client()
    adapter = adapter_at(tmp_path, client)
    await adapter._process_leased_message(message(from_agent="evil-peer"))
    saved = state(tmp_path)
    assert [entry["reason"] for entry in saved["dlq"]] == ["sender_policy"]
    assert saved.get("notification_outbox", {}) == {}


@pytest.mark.asyncio
async def test_flag_off_cards_keep_todays_sender_policy_dlq(tmp_path: Path) -> None:
    client = Client()
    adapter = adapter_at(tmp_path, client, enabled=False)
    await adapter._process_leased_message(message())
    saved = state(tmp_path)
    assert [entry["reason"] for entry in saved["dlq"]] == ["sender_policy"]
    assert saved.get("notification_outbox", {}) == {}


@pytest.mark.asyncio
async def test_card_without_notification_recipients_is_quarantined_and_acked(
    tmp_path: Path,
) -> None:
    client = Client()
    adapter = adapter_at(tmp_path, client, recipients=False)
    await adapter._process_leased_message(message())
    saved = state(tmp_path)
    assert [entry["reason"] for entry in saved["dlq"]] == ["decision_card_no_recipients"]
    assert saved.get("notification_outbox", {}) == {}
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_card_is_delivered_as_plain_card_text_not_fenced_and_mirrored_fenced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from plugin.mupot_gateway import notifications

    sent: list[str] = []
    mirrored: list[str] = []

    async def deliver_text(target, text):
        sent.append(text)
        return {"message_id": "tg-1", "platform": "telegram", "chat_id": "123"}

    monkeypatch.setattr(notifications, "deliver_text", deliver_text)
    monkeypatch.setattr(notifications, "mirror_text", lambda target, text: mirrored.append(text))
    monkeypatch.setattr(
        notifications,
        "active_sessions",
        lambda: [
            {
                "id": "human",
                "session_key": "agent:main:telegram:dm:123",
                "source": "telegram",
                "user_id": "123456789",
                "chat_id": "123",
                "chat_type": "dm",
                "last_active": 1,
            }
        ],
    )
    activations: list[str] = []
    adapter = adapter_at(
        tmp_path, Client(), injector=lambda content, **kw: activations.append(content) or True
    )
    await adapter._process_leased_message(message())
    await adapter._flush_notifications()

    assert activations == []  # never a model turn
    assert len(sent) == 1
    assert sent[0].startswith("Decision needed: Decide the thing")
    assert "```" not in sent[0]
    assert "```" in mirrored[0] and "Decision needed" in mirrored[0]
    assert state(tmp_path)["notification_outbox"]["msg-card-1"]["status"] == "delivered"


def test_decision_cards_enabled_must_be_boolean(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="decision_cards_enabled"):
        MupotAdapter(
            PlatformConfig(
                enabled=True,
                extra={"decision_cards_enabled": "yes", "state_path": str(tmp_path / "s.json")},
            ),
            client_factory=lambda *_: Client(),
        )


def _sessions(monkeypatch, notifications, rows) -> None:
    monkeypatch.setattr(notifications, "active_sessions", lambda: rows)


DM = {
    "id": "human", "session_key": "agent:main:telegram:dm:123", "source": "telegram",
    "user_id": "123456789", "chat_id": "123", "chat_type": "dm", "last_active": 1,
}


@pytest.mark.asyncio
async def test_card_is_sent_never_activated_even_with_notification_activate_and_routine_events(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from plugin.mupot_gateway import notifications

    sent: list[str] = []
    activations: list[str] = []

    async def deliver_text(target, text):
        sent.append(text)
        return {"message_id": "tg-1", "platform": "telegram", "chat_id": "123"}

    monkeypatch.setattr(notifications, "deliver_text", deliver_text)
    monkeypatch.setattr(notifications, "mirror_text", lambda t, x: None)
    _sessions(monkeypatch, notifications, [DM])
    adapter = adapter_at(
        tmp_path, Client(),
        injector=lambda content, **kw: activations.append(content) or True,
        notification_activate=True, routine_events_enabled=True,
    )
    await adapter._process_leased_message(message())
    await adapter._flush_notifications()
    assert len(sent) == 1 and sent[0].startswith("Decision needed:")
    assert activations == []
    assert state(tmp_path)["notification_outbox"]["msg-card-1"]["status"] == "delivered"


@pytest.mark.asyncio
async def test_no_active_dm_session_falls_back_to_direct_send_to_configured_chat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from plugin.mupot_gateway import notifications

    sent: list[tuple[dict, str]] = []
    mirrored: list[str] = []

    async def deliver_text(target, text):
        sent.append((target, text))
        return {"message_id": "tg-1", "platform": "telegram", "chat_id": target["chat_id"]}

    monkeypatch.setattr(notifications, "deliver_text", deliver_text)
    monkeypatch.setattr(notifications, "mirror_text", lambda t, x: mirrored.append(x))
    _sessions(monkeypatch, notifications, [])
    adapter = adapter_at(tmp_path, Client())
    await adapter._process_leased_message(message())
    with caplog.at_level("WARNING"):
        await adapter._flush_notifications()
    assert len(sent) == 1
    assert sent[0][0]["chat_id"] == "123456789" and sent[0][0]["platform"] == "telegram"
    assert sent[0][1].startswith("Decision needed:")
    assert mirrored == []  # no session to mirror into
    assert any("no active conversation" in r.message for r in caplog.records)
    assert state(tmp_path)["notification_outbox"]["msg-card-1"]["status"] == "delivered"


@pytest.mark.asyncio
async def test_non_card_notice_still_requires_an_active_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from plugin.mupot_gateway import notifications

    sent: list[str] = []

    async def deliver_text(target, text):
        sent.append(text)
        return {"message_id": "x", "platform": "telegram", "chat_id": "1"}

    monkeypatch.setattr(notifications, "deliver_text", deliver_text)
    _sessions(monkeypatch, notifications, [])
    adapter = adapter_at(tmp_path, Client())
    notifications.enqueue(adapter._state, adapter.store, {"id": "peer-1", "from_agent": "kasra"}, "hi")
    await adapter._flush_notifications()
    assert sent == []
    assert state(tmp_path)["notification_outbox"]["peer-1"]["status"] == "pending"


@pytest.mark.asyncio
async def test_conflicting_redelivery_is_dead_lettered_and_acked_not_raised(
    tmp_path: Path,
) -> None:
    client = Client()
    adapter = adapter_at(tmp_path, client)
    await adapter._process_leased_message(message())
    # Same source id, different facts, and not yet marked processed.
    adapter._state["processed"] = []
    adapter.store.save(adapter._state)
    await adapter._process_leased_message(
        message(body=card_body(title="A different title"))
    )
    assert [e["reason"] for e in state(tmp_path)["dlq"]] == ["decision_card_conflict"]
    assert len(client.calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["title", "result_excerpt"])
async def test_lone_surrogate_card_is_dead_lettered_and_acked_not_raised(
    tmp_path: Path, field: str
) -> None:
    client = Client()
    adapter = adapter_at(tmp_path, client)
    await adapter._process_leased_message(message(body=card_body(**{field: "x \ud83d"})))
    saved = state(tmp_path)
    assert [e["reason"] for e in saved["dlq"]] == ["invalid_decision_card"]
    assert saved.get("notification_outbox", {}) == {}
    assert client.calls == [("inbox_ack", {"ids": ["msg-card-1"]})]


@pytest.mark.asyncio
@pytest.mark.parametrize("recipient", ["-1001234567890", "@somechannel", "0", "owner", "12ab", "\u0661\u0662"])
async def test_direct_fallback_refuses_a_non_user_id_recipient(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, recipient: str
) -> None:
    from plugin.mupot_gateway import notifications

    sent: list[str] = []

    async def deliver_text(target, text):
        sent.append(text)
        return {"message_id": "x", "platform": "telegram", "chat_id": "1"}

    monkeypatch.setattr(notifications, "deliver_text", deliver_text)
    _sessions(monkeypatch, notifications, [])
    adapter = adapter_at(tmp_path, Client(), recipients=False, notification_recipients={"telegram": recipient})
    await adapter._process_leased_message(message())
    await adapter._flush_notifications()
    assert sent == []
    assert state(tmp_path)["notification_outbox"]["msg-card-1"]["status"] != "delivered"


@pytest.mark.asyncio
async def test_raw_lone_surrogate_envelope_is_dead_lettered_and_acked_not_raised(
    tmp_path: Path,
) -> None:
    client = Client()
    adapter = adapter_at(tmp_path, client)
    body = card_body().replace('"title"', '"ti\ud83dtle"', 1)  # raw, unescaped surrogate
    await adapter._process_leased_message(message(body=body, **{"from_agent": "mupot-decision-card"}))
    saved = state(tmp_path)
    assert [e["reason"] for e in saved["dlq"]] == ["invalid_decision_card"]
    assert client.calls == [("inbox_ack", {"ids": ["msg-card-1"]})]


@pytest.mark.asyncio
async def test_int_recipient_from_yaml_is_normalised_and_delivered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from plugin.mupot_gateway import notifications

    sent: list[str] = []

    async def deliver_text(target, text):
        sent.append(text)
        return {"message_id": "x", "platform": "telegram", "chat_id": target["chat_id"]}

    monkeypatch.setattr(notifications, "deliver_text", deliver_text)
    _sessions(monkeypatch, notifications, [])
    adapter = adapter_at(tmp_path, Client(), recipients=False, notification_recipients={"telegram": 123456789})
    await adapter._process_leased_message(message())
    await adapter._flush_notifications()
    assert len(sent) == 1
    assert state(tmp_path)["notification_outbox"]["msg-card-1"]["status"] == "delivered"


@pytest.mark.asyncio
async def test_refused_recipient_warning_is_throttled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from plugin.mupot_gateway import notifications

    monkeypatch.setattr(notifications, "_REFUSED_WARNED", set())
    with caplog.at_level("WARNING"):
        for _ in range(3):
            assert notifications.direct_target({"telegram": "@chan"}) is None
    assert len([r for r in caplog.records if "refusing direct send" in r.message]) == 1

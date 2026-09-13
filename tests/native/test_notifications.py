from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from plugin.mupot_gateway.adapter import MupotAdapter, StateStore
from gateway.config import PlatformConfig


class Client:
    async def call(self, tool, arguments):
        assert tool == "send"
        return {"id": "reply-1", "seq": 8, "duplicate": False}


def adapter_at(tmp_path):
    return MupotAdapter(PlatformConfig(enabled=True, extra={
        "allowed_agents": "kasra",
        "state_path": str(tmp_path / "inbox.json"),
        "notification_recipients": {"telegram": "owner"},
    }), client_factory=lambda _: Client())


@pytest.mark.asyncio
async def test_native_reply_enqueues_one_notification_and_skips_interim(tmp_path):
    """A successful Mupot response must persist a human notice before source completion."""
    adapter = adapter_at(tmp_path)
    adapter._current_message = {"id": "source-1", "from_agent": "kasra", "request_id": "req-1"}
    assert (await adapter.send("kasra", "still working", metadata={"_interim_send": True})).success
    assert not StateStore(tmp_path / "inbox.json").load().get("notification_outbox")
    assert (await adapter.send("kasra", "Please review the deployment plan.")).success
    state = StateStore(tmp_path / "inbox.json").load()
    notices = state.get("notification_outbox", {})
    assert list(notices) == ["source-1"]
    assert "Please review the deployment plan." in notices["source-1"]["text"]
    assert "source-1" in notices["source-1"]["text"]
    # Redelivery or repeated final send cannot create a second notification.
    assert (await adapter.send("kasra", "Please review the deployment plan.")).success
    assert len(StateStore(tmp_path / "inbox.json").load()["notification_outbox"]) == 1


@pytest.mark.asyncio
async def test_persisted_notice_retries_independently_and_cannot_follow_other_users(tmp_path, monkeypatch):
    """Notification retry must survive restart without rerunning Mupot work or leaking to a group."""
    from plugin.mupot_gateway import notifications
    sessions = [
        {"id": "owner-dm", "source": "telegram", "user_id": "owner", "chat_id": "123",
         "chat_type": "dm", "last_active": 10, "ended_at": None},
        {"id": "stranger-dm", "source": "telegram", "user_id": "stranger", "chat_id": "456",
         "chat_type": "dm", "last_active": 30, "ended_at": None},
        {"id": "group", "source": "telegram", "user_id": "owner", "chat_id": "-999",
         "chat_type": "group", "last_active": 40, "ended_at": None},
    ]
    monkeypatch.setattr(notifications, "active_sessions", lambda: sessions)
    wire = []
    mirror = []
    async def deliver(target, text):
        assert target["session_id"] == "owner-dm"
        wire.append(target["chat_id"])
        if len(wire) == 1:
            raise RuntimeError("offline")
        return {"message_id": "telegram-7"}
    monkeypatch.setattr(notifications, "deliver_text", deliver)
    monkeypatch.setattr(notifications, "mirror_text", lambda target, text: mirror.append((target["session_id"], text)))
    adapter = adapter_at(tmp_path)
    adapter._current_message = {"id": "source-1", "from_agent": "kasra", "request_id": "req-1"}
    assert (await adapter.send("kasra", "Ready for your review.")).success
    await adapter._flush_notifications()
    assert adapter._state["notification_outbox"]["source-1"]["status"] == "pending"
    restarted = adapter_at(tmp_path)
    restarted._state["notification_outbox"]["source-1"]["retry_at"] = 0
    await restarted._flush_notifications()
    assert wire == ["123", "123"]
    assert len(mirror) == 1
    assert "Ready for your review." in mirror[0][1]
    assert restarted._state["notification_outbox"]["source-1"]["status"] == "delivered"
    again = adapter_at(tmp_path)
    await again._flush_notifications()
    assert len(wire) == 2


def test_target_selection_follows_only_configured_human_sessions():
    from plugin.mupot_gateway.notifications import select_target
    sessions = [
        {"id": "a", "source": "telegram", "user_id": "owner", "chat_id": "1", "chat_type": "dm", "last_active": 10},
        {"id": "b", "source": "discord", "user_id": "owner-discord", "chat_id": "2", "chat_type": "dm", "last_active": 20},
        {"id": "c", "source": "mupot", "user_id": "owner", "chat_id": "3", "chat_type": "dm", "last_active": 50},
    ]
    assert select_target(sessions, {"telegram": "owner"})["session_id"] == "a"
    assert select_target(sessions, {"telegram": "owner", "discord": "owner-discord"})["session_id"] == "b"
    assert select_target(sessions, {"telegram": "someone-else"}) is None


@pytest.mark.asyncio
async def test_native_delivery_is_visible_in_real_human_conversation(tmp_path, monkeypatch):
    """Exercise real session lookup, adapter transport and SQLite conversation mirroring."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from hermes_state import SessionDB
    from gateway.config import Platform
    from gateway.platforms.base import SendResult
    from tools import send_message_senders
    db = SessionDB(tmp_path / "state.db")
    try:
        db.create_session("human", "telegram", user_id="owner", chat_id="123", chat_type="dm",
                          session_key="agent:main:telegram:dm:123")
        db.create_session("other", "telegram", user_id="stranger", chat_id="456", chat_type="dm",
                          session_key="agent:main:telegram:dm:456")
        db.append_message("human", "user", "Keep me informed here.")
        db.append_message("other", "user", "An unrelated newer conversation.")
        delivered = []
        class Transport:
            async def send(self, *, chat_id, content, metadata):
                delivered.append((chat_id, content))
                return SendResult(success=True, message_id="tg-17")
        def connected(platform):
            assert platform == Platform.TELEGRAM
            return None, Transport()
        monkeypatch.setattr(send_message_senders, "_live_adapter", connected)
        adapter = adapter_at(tmp_path)
        adapter._current_message = {"id": "source-native", "from_agent": "kasra"}
        await adapter.send("kasra", "The requested work is ready to review.")
        await adapter._flush_notifications()
        assert len(delivered) == 1
        assert delivered[0][0] == "123"
        assert "The requested work is ready to review." in delivered[0][1]
        assert db.get_messages("human")[-1]["content"] == delivered[0][1]
        assert len(db.get_messages("other")) == 1
        notice = StateStore(tmp_path / "inbox.json").load()["notification_outbox"]["source-native"]
        assert notice["status"] == "delivered"
        assert notice["delivery_receipt"]["message_id"] == "tg-17"
        # Existing Hermes mirroring is best-effort: success without a DB write
        # must not be promoted into a completed notification receipt.
        from plugin.mupot_gateway.notifications import mirror_text
        import gateway.mirror
        monkeypatch.setattr(gateway.mirror, "mirror_to_session", lambda *a, **kw: True)
        with pytest.raises(RuntimeError, match="readback"):
            mirror_text(notice["target"], "A different notification that was not stored.")
    finally:
        db.close()


@pytest.mark.asyncio
async def test_mirror_retry_does_not_resend_to_human(tmp_path, monkeypatch):
    from plugin.mupot_gateway import notifications
    sessions = [{"id": "human", "source": "telegram", "user_id": "owner", "chat_id": "123",
                 "chat_type": "dm", "last_active": 1}]
    monkeypatch.setattr(notifications, "active_sessions", lambda: sessions)
    sends = []
    async def deliver(target, text):
        sends.append(text)
        return {"message_id": "tg-18"}
    monkeypatch.setattr(notifications, "deliver_text", deliver)
    def unavailable(*_):
        raise OSError("mirror unavailable")
    monkeypatch.setattr(notifications, "mirror_text", unavailable)
    adapter = adapter_at(tmp_path)
    adapter._current_message = {"id": "source-mirror", "from_agent": "kasra"}
    await adapter.send("kasra", "Progress update.")
    await adapter._flush_notifications()
    restarted = adapter_at(tmp_path)
    restarted._state["notification_outbox"]["source-mirror"]["retry_at"] = 0
    monkeypatch.setattr(notifications, "mirror_text", lambda *_: None)
    await restarted._flush_notifications()
    assert len(sends) == 1
    assert restarted._state["notification_outbox"]["source-mirror"]["status"] == "delivered"


@pytest.mark.parametrize("failure", ["timeout", "crash"])
@pytest.mark.asyncio
async def test_uncertain_delivery_never_resends_after_restart(tmp_path, monkeypatch, failure):
    from plugin.mupot_gateway import notifications
    from gateway.platforms.base import SendResult
    from tools import send_message_senders
    monkeypatch.setattr(notifications, "active_sessions", lambda: [
        {"id": "human", "source": "telegram", "user_id": "owner", "chat_id": "123",
         "chat_type": "dm", "last_active": 1}])
    sends = []
    class Transport:
        async def send(self, **kwargs):
            sends.append(kwargs)
            if failure == "crash":
                raise asyncio.CancelledError()
            return SendResult(success=False, retryable=False, error="timed out")
    monkeypatch.setattr(send_message_senders, "_live_adapter", lambda _: (None, Transport()))
    adapter = adapter_at(tmp_path)
    adapter._current_message = {"id": "uncertain-source", "from_agent": "kasra"}
    await adapter.send("kasra", "One notification.")
    if failure == "crash":
        with pytest.raises(asyncio.CancelledError):
            await adapter._flush_notifications()
    else:
        await adapter._flush_notifications()
    restarted = adapter_at(tmp_path)
    restarted._state["notification_outbox"]["uncertain-source"]["retry_at"] = 0
    await restarted._flush_notifications()
    assert len(sends) == 1
    assert restarted._state["notification_outbox"]["uncertain-source"]["status"] == "transport_unknown"


@pytest.mark.asyncio
async def test_terminal_receipt_reaches_human_without_ack_loop(tmp_path):
    """Incoming terminal gate/status receipts are visible to the human without another agent reply."""
    calls = []
    class AckClient:
        async def call(self, tool, args):
            calls.append((tool, args))
            return {"acked": ["terminal-1"], "already_read": [], "refused": []}
    adapter = adapter_at(tmp_path)
    adapter._client = AckClient()
    message = {"id": "terminal-1", "from_agent": "kasra", "kind": "ack", "expects_reply": False,
               "body": "Review completed. Your decision is needed.", "in_reply_to": "request-1"}
    await adapter._handle_ack_envelope(message)
    state = StateStore(tmp_path / "inbox.json").load()
    assert calls == [("inbox_ack", {"ids": ["terminal-1"]})]
    assert "Review completed. Your decision is needed." in state["notification_outbox"]["terminal-1"]["text"]


@pytest.mark.asyncio
async def test_activation_queues_existing_human_conversation_instead_of_passive_send(tmp_path, monkeypatch):
    from plugin.mupot_gateway import notifications
    monkeypatch.setattr(notifications, "active_sessions", lambda: [
        {"id": "human", "session_key": "agent:main:telegram:dm:123", "source": "telegram",
         "user_id": "owner", "chat_id": "123", "chat_type": "dm", "last_active": 1}])
    calls = []
    async def no_passive_send(*_):
        raise AssertionError("Activation must run the agent, not send a passive notification")
    monkeypatch.setattr(notifications, "deliver_text", no_passive_send)
    adapter = adapter_at(tmp_path)
    adapter.notification_activate = True
    adapter.message_injector = lambda content, **kw: calls.append((content, kw)) or True
    adapter._current_message = {"id": "activate-1", "from_agent": "kasra"}
    await adapter.send("kasra", "The project needs your direction.")
    await adapter._flush_notifications()
    assert len(calls) == 1
    assert calls[0][1] == {"session_key": "agent:main:telegram:dm:123"}
    assert "activate-1" in calls[0][0]
    notice = StateStore(tmp_path / "inbox.json").load()["notification_outbox"]["activate-1"]
    assert notice["status"] == "activation_queued"
    await adapter._flush_notifications()
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_registered_plugin_activates_native_gateway_and_preserves_control_fence(tmp_path, monkeypatch):
    import yaml
    from gateway.config import GatewayConfig, Platform
    from gateway.platforms.base import BasePlatformAdapter, SendResult
    from gateway.platform_registry import platform_registry
    from gateway.run import GatewayRunner
    from gateway.session import SessionStore, SessionSource
    from hermes_cli import plugins
    from plugin.mupot_gateway import notifications
    home = tmp_path / "hermes-home"
    plugin_dir = home / "plugins" / "mupot"
    plugin_dir.parent.mkdir(parents=True)
    plugin_dir.symlink_to(Path(__file__).resolve().parents[2], target_is_directory=True)
    config_file = home / "config.yaml"
    config_file.write_text(yaml.safe_dump({"plugins": {
        "enabled": ["mupot"],
        "entries": {"mupot": {
            "allow_gateway_injection": True,
            "settings": {"mode": "operator", "operator": {
                "base_url": "https://pot.example.invalid",
                "expected_tenant": "tenant-test",
                "squad_id": "squad-test",
                "agent_id": "agent-test",
                "approval_owner": "human-test",
                "native_gateway_enabled": True,
            }},
        }},
    }}))
    empty_bundled = tmp_path / "empty-bundled"
    empty_bundled.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_BUNDLED_PLUGINS", str(empty_bundled))
    monkeypatch.setenv("MUPOT_AGENT_TOKEN", "test-agent-token")
    store = SessionStore(sessions_dir=tmp_path / "sessions", config=GatewayConfig())
    entry = store.get_or_create_session(SessionSource(platform=Platform.TELEGRAM,
        chat_id="123", user_id="owner", chat_type="dm"))
    seen = []
    completed = asyncio.Event()
    class TargetAdapter(BasePlatformAdapter):
        def __init__(self):
            super().__init__(PlatformConfig(enabled=True), Platform.TELEGRAM)
        async def connect(self, **kw):
            return True
        async def disconnect(self):
            pass
        async def get_chat_info(self, chat_id):
            return {"id": chat_id, "type": "dm"}
        async def send(self, chat_id, content, **kw):
            completed.set()
            return SendResult(success=True, message_id="native-response")
    target_adapter = TargetAdapter()
    async def model_turn(event):
        seen.append(event)
        return "Automatic conversation response."
    target_adapter.set_message_handler(model_turn)
    runner = object.__new__(GatewayRunner)
    runner.session_store = store
    runner.adapters = {Platform.TELEGRAM: target_adapter}
    runner._profile_adapters = {}
    runner._gateway_loop = asyncio.get_running_loop()
    runner._running = True
    runner._draining = False
    runner._background_tasks = set()
    runner._is_user_authorized = lambda *a, **kw: True
    manager = plugins.PluginManager(scope_key=str(home.resolve()))
    monkeypatch.setattr(manager, "_scan_entry_points", lambda: [])
    monkeypatch.setattr(plugins, "get_plugin_manager", lambda: manager)
    runner._install_plugin_message_injector()
    manager.discover_and_load()
    assert manager._plugins["mupot"].enabled is True
    adapter = platform_registry.get("mupot").adapter_factory(PlatformConfig(enabled=True, extra={
        "state_path": str(tmp_path / "outbox.json"), "notification_activate": True,
        "notification_recipients": {"telegram": "owner"}}))
    adapter._send_client = Client()
    monkeypatch.setattr(notifications, "active_sessions", lambda: [{
        "id": entry.session_id, "session_key": entry.session_key, "source": "telegram",
        "user_id": "owner", "chat_id": "123", "chat_type": "dm", "last_active": 1}])
    adapter._current_message = {"id": "native-activation", "from_agent": "kasra"}
    try:
        await adapter.send("kasra", "Status ready.")
        await adapter._flush_notifications()
        await asyncio.wait_for(completed.wait(), 5)
        assert len(seen) == 1
        event = seen[0]
        assert event.metadata["gateway_session_id"] == entry.session_id
        assert event.metadata["hermes_plugin_id"] == "mupot"
        assert event.allow_gateway_control is False
        assert event.internal is True
        assert event.source.chat_id == "123"
        await adapter._flush_notifications()
        await asyncio.sleep(0)
        assert len(seen) == 1
    finally:
        runner._clear_plugin_message_injector()
        manager.unload("mupot")
        await asyncio.gather(*list(runner._background_tasks), return_exceptions=True)
        await asyncio.gather(*list(target_adapter._session_tasks.values()), return_exceptions=True)

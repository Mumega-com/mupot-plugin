from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml


PLUGIN_ROOT = Path(__file__).resolve().parents[2]


def _discover_plugin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from hermes_cli import plugins

    home = tmp_path / "hermes-home"
    plugin_dir = home / "plugins" / "mupot"
    plugin_dir.parent.mkdir(parents=True)
    plugin_dir.symlink_to(PLUGIN_ROOT, target_is_directory=True)
    (home / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "plugins": {
                    "enabled": ["mupot"],
                    "entries": {
                        "mupot": {
                            "settings": {
                                "mode": "operator",
                                "operator": {
                                    "base_url": "https://pot.example.invalid",
                                    "expected_tenant": "tenant-test",
                                    "squad_id": "squad-test",
                                    "agent_id": "agent-test",
                                    "approval_owner": "human-test",
                                    "telegram_control_enabled": True,
                                    "telegram_control_webhook_secret_env": (
                                        "TEST_IM_WEBHOOK_SECRET"
                                    ),
                                },
                            }
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    empty_bundled = tmp_path / "empty-bundled"
    empty_bundled.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_BUNDLED_PLUGINS", str(empty_bundled))
    monkeypatch.setenv("MUPOT_AGENT_TOKEN", "test-agent-token")
    monkeypatch.setenv("TEST_IM_WEBHOOK_SECRET", "test-webhook-secret")

    manager = plugins.PluginManager(scope_key=str(home.resolve()))
    monkeypatch.setattr(manager, "_scan_entry_points", lambda: [])
    manager.discover_and_load()
    loaded = manager._plugins["mupot"]
    assert loaded.enabled is True
    assert loaded.error is None
    return manager, loaded


def _telegram_update(application, text: str, update_id: int):
    from telegram import Update

    first_token = text.split(maxsplit=1)[0]
    entities = []
    if first_token.startswith("/"):
        entities.append(
            {
                "type": "bot_command",
                "offset": 0,
                "length": len(first_token),
            }
        )
    return Update.de_json(
        {
            "update_id": update_id,
            "message": {
                "message_id": update_id,
                "date": 0,
                "chat": {"id": 123, "type": "private", "first_name": "Ada"},
                "from": {
                    "id": 123,
                    "is_bot": False,
                    "first_name": "Ada",
                },
                "text": text,
                "entities": entities,
            },
        },
        application.bot,
    )


@pytest.mark.asyncio
async def test_discovered_handlers_isolate_commands_and_are_owned_on_unload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plugin commands bypass core once; ordinary text and unload remain Hermes-owned."""
    assert "telegram.ext" not in sys.modules
    manager, loaded = _discover_plugin(tmp_path, monkeypatch)
    assert "telegram.ext" not in sys.modules

    from gateway.config import PlatformConfig
    from hermes_cli import plugins
    from plugins.platforms.telegram.adapter import TelegramAdapter
    from telegram import Message, User
    from telegram.ext import Application, MessageHandler

    control_module = sys.modules[f"{loaded.module.__name__}.telegram_control"]
    relayed: list[str] = []

    def relay(_settings, update) -> str:
        relayed.append(update.effective_message.text)
        return f"Mupot handled {update.effective_message.text.split()[0]}"

    monkeypatch.setattr(control_module, "relay_telegram_update", relay)
    replies: list[str] = []

    async def reply_text(_message, text: str, **_kwargs) -> None:
        replies.append(text)

    monkeypatch.setattr(Message, "reply_text", reply_text)

    application = Application.builder().token("123:test-token").build()
    application.bot._bot_user = User(
        id=999, is_bot=True, first_name="Mupot Test", username="mupot_test_bot"
    )
    adapter = TelegramAdapter(
        PlatformConfig(enabled=True, token="123:test-token", extra={})
    )
    core_turns: list[str] = []

    async def core_handler(update, _context) -> None:
        core_turns.append(update.effective_message.text)

    adapter._handle_text_message = core_handler
    adapter._handle_command = core_handler
    monkeypatch.setattr(plugins, "get_plugin_manager", lambda: manager)
    adapter._wire_plugin_handlers(application)
    adapter._wire_plugin_handlers(application)
    adapter._register_handlers(application)

    plugin_handlers = [
        handler
        for handler in application.handlers[-1]
        if isinstance(handler, MessageHandler)
    ]
    assert len(plugin_handlers) == 1  # one handler, wired once despite double wiring
    assert application.handlers[0]  # Hermes core handlers remain registered.

    application._initialized = True
    await application.process_update(_telegram_update(application, "/start invite-1", 1))
    await application.process_update(
        _telegram_update(application, "/muanswer decision-1 accept", 2)
    )
    await application.process_update(_telegram_update(application, "ordinary work", 3))

    assert relayed == ["/start invite-1", "/muanswer decision-1 accept"]
    assert replies == ["Mupot handled /start", "Mupot handled /muanswer"]
    assert core_turns == ["ordinary work"]

    assert manager.unload("mupot") is True
    assert manager.get_telegram_handler_factories() == []
    assert not application.handlers.get(-1)


def test_targeted_unload_preserves_another_owner_of_the_same_factory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from hermes_cli.plugins import PluginContext, PluginManifest

    manager, _loaded = _discover_plugin(tmp_path, monkeypatch)
    factory, owner = manager.get_telegram_handler_factories()[0]
    assert owner == "mupot"
    competing_context = PluginContext(
        PluginManifest(name="unrelated-plugin", source="user"), manager
    )
    competing_context.register_telegram_handler(factory)
    assert manager.get_telegram_handler_factories() == [
        (factory, "mupot"),
        (factory, "unrelated-plugin"),
    ]

    try:
        assert manager.unload("mupot") is True
        assert manager.get_telegram_handler_factories() == [
            (factory, "unrelated-plugin")
        ]
    finally:
        manager.unload()


async def _wired_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, lstrip_entity: bool = True):
    """Real PTB Application: plugin wired first, then a core-like catch-all."""
    manager, loaded = _discover_plugin(tmp_path, monkeypatch)
    from hermes_cli import plugins
    from plugins.platforms.telegram.adapter import TelegramAdapter
    from gateway.config import PlatformConfig
    from telegram import Message, User
    from telegram.ext import Application, MessageHandler, filters

    control_module = sys.modules[f"{loaded.module.__name__}.telegram_control"]
    relayed: list[str] = []

    def relay(_settings, update) -> str:
        relayed.append(update.effective_message.text)
        return "ok"

    monkeypatch.setattr(control_module, "relay_telegram_update", relay)
    replies: list[str] = []

    async def reply_text(_message, text: str, **_kwargs) -> None:
        replies.append(text)

    monkeypatch.setattr(Message, "reply_text", reply_text)
    application = Application.builder().token("123:test-token").build()
    application.bot._bot_user = User(
        id=999, is_bot=True, first_name="Mupot Test", username="mupot_test_bot"
    )
    adapter = TelegramAdapter(
        PlatformConfig(enabled=True, token="123:test-token", extra={})
    )
    monkeypatch.setattr(plugins, "get_plugin_manager", lambda: manager)
    adapter._wire_plugin_handlers(application)
    core: list[str] = []

    async def catch_all(update, _context) -> None:
        core.append(update.effective_message.text)

    # Core-like text path, added AFTER the plugin, in group 0.
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, catch_all))
    application._initialized = True
    return application, relayed, replies, core, control_module


def _entityless(application, text: str, update_id: int, *, entities=None, edited=False):
    from telegram import Update

    key = "edited_message" if edited else "message"
    return Update.de_json(
        {
            "update_id": update_id,
            key: {
                "message_id": update_id,
                "date": 0,
                "chat": {"id": 123, "type": "private", "first_name": "Ada"},
                "from": {"id": 123, "is_bot": False, "first_name": "Ada"},
                "text": text,
                **({"entities": entities} if entities else {}),
            },
        },
        application.bot,
    )


@pytest.mark.asyncio
async def test_entityless_and_whitespace_commands_reach_plugin_not_core(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    application, relayed, replies, core, _ = await _wired_app(tmp_path, monkeypatch)
    await application.process_update(_entityless(application, "/muapprove abcdef12", 1))
    await application.process_update(_entityless(application, "  /muapprove abcdef12", 2))
    await application.process_update(_entityless(application, "/MUAPPROVE abcdef12", 3))
    await application.process_update(
        _entityless(
            application,
            "/muapprove abcdef12",
            4,
            entities=[{"type": "bot_command", "offset": 0, "length": 10}],
        )
    )
    await application.process_update(
        _entityless(application, "/muapprove@MUPOT_test_bot abcdef12", 5)
    )
    assert len(relayed) == 5
    assert replies == ["ok"] * 5
    assert core == []  # core never sees a plugin command


@pytest.mark.asyncio
async def test_other_bot_suffix_edited_and_plain_text_are_not_swallowed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    application, relayed, replies, core, _ = await _wired_app(tmp_path, monkeypatch)
    await application.process_update(
        _entityless(application, "/muapprove@otherbot abcdef12", 1)
    )
    await application.process_update(
        _entityless(application, "/muapprove abcdef12", 2, edited=True)
    )
    await application.process_update(_entityless(application, "/muapproveX abc", 3))
    await application.process_update(_entityless(application, "hello /muapprove", 4))
    assert relayed == [] and replies == []
    # Everything the plugin does not own still reaches the core-like catch-all
    # (not swallowed). The plugin itself ignores the edited update.
    assert core == [
        "/muapprove@otherbot abcdef12",
        "/muapprove abcdef12",  # the edited message, answered by no plugin
        "/muapproveX abc",
        "hello /muapprove",
    ]


def test_first_person_intake_filter_ignores_entityless_slash_text() -> None:
    """Real PTB filters: pasted/whitespace-led slash text is never intake text."""
    from unittest.mock import MagicMock

    from telegram.ext import Application

    plugin_module = sys.modules["plugin"]
    from plugin.first_person import FirstPersonSettings, register_first_person

    factories: list = []
    ctx = MagicMock()
    ctx.register_telegram_handler = factories.append
    ctx._manager = None
    register_first_person(
        ctx,
        FirstPersonSettings(
            enabled=True,
            base_url="https://pot.example.invalid/base",
            webhook_secret_env="TEST_IM_WEBHOOK_SECRET",
            timeout=7.0,
        ),
        client=MagicMock(),
    )
    application = Application.builder().token("123:test-token").build()
    factories[0](application, None)
    (handler,) = application.handlers[-5]
    assert plugin_module is not None

    def matches(text: str) -> bool:
        return bool(
            handler.check_update(_entityless(application, text, 1))
        )

    assert matches("hello there")
    assert not matches("/muapprove abcdef12")
    assert not matches("  /muapprove abcdef12")
    assert not matches("\n/start x")

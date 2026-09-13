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
    from telegram.ext import Application, CommandHandler

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

    group_zero = application.handlers[0]
    plugin_handlers = [
        handler for handler in group_zero if isinstance(handler, CommandHandler)
    ]
    assert len(plugin_handlers) == 5
    assert [next(iter(handler.commands)) for handler in plugin_handlers] == [
        "start",
        "needs",
        "answer",
        "approve",
        "reject",
    ]
    assert group_zero[5:]  # Hermes core handlers remain registered after the plugin.

    application._initialized = True
    await application.process_update(_telegram_update(application, "/start invite-1", 1))
    await application.process_update(
        _telegram_update(application, "/answer decision-1 accept", 2)
    )
    await application.process_update(_telegram_update(application, "ordinary work", 3))

    assert relayed == ["/start invite-1", "/answer decision-1 accept"]
    assert replies == ["Mupot handled /start", "Mupot handled /answer"]
    assert core_turns == ["ordinary work"]

    assert manager.unload("mupot") is True
    assert manager.get_telegram_handler_factories() == []
    assert all(handler not in application.handlers[0] for handler in plugin_handlers)

"""Real-PTB dispatch test for the Telegram inline-approval callback handler.

Mirrors tests/native/test_telegram_control_native.py's harness (real
PluginManager discovery + a real python-telegram-bot Application) instead of
duck-typed fakes, so the CallbackQueryHandler's ``pattern="^mv:"`` scoping and
PTB's own first-match-per-group dispatch are exercised for real -- the exact
concern the brief called out ("core has a catch-all CallbackQueryHandler --
scope or you swallow core flows").
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

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
                                    "telegram_inline_approval_enabled": True,
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

    manager = plugins.PluginManager(scope_key=str(home.resolve()))
    monkeypatch.setattr(manager, "_scan_entry_points", lambda: [])
    manager.discover_and_load()
    loaded = manager._plugins["mupot"]
    assert loaded.enabled is True
    assert loaded.error is None
    return manager, loaded


def _callback_update(*, bot: Any, update_id: int, data: str, chat_id: int, user_id: int, message_id: int):
    from telegram import Update

    return Update.de_json(
        {
            "update_id": update_id,
            "callback_query": {
                "id": f"cbq-{update_id}",
                "from": {"id": user_id, "is_bot": False, "first_name": "Ada"},
                "chat_instance": "instance-1",
                "data": data,
                "message": {
                    "message_id": message_id,
                    "date": 0,
                    "chat": {"id": chat_id, "type": "private", "first_name": "Ada"},
                    "text": "Task pending-task-1 needs your decision",
                },
            },
        },
        bot,
    )


@pytest.mark.asyncio
async def test_real_callback_dispatch_submits_verdict_once_and_refuses_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager, loaded = _discover_plugin(tmp_path, monkeypatch)

    from gateway.config import PlatformConfig
    from hermes_cli import plugins
    from plugins.platforms.telegram.adapter import TelegramAdapter
    from telegram import User
    from telegram.ext import Application, CallbackQueryHandler

    inline_module = sys.modules[f"{loaded.module.__name__}.telegram_inline_approval"]
    operator_module = sys.modules[f"{loaded.module.__name__}.mupot_operator"]

    verdict_calls: list[tuple[str, dict[str, Any]]] = []

    def fake_call(self: Any, action: str, args: Any) -> Any:
        verdict_calls.append((action, dict(args)))
        return {"ok": True, "result": {"human_origin": {"applied": True, "reason": "ok"}, "id": "v1"}}

    monkeypatch.setattr(operator_module.MupotOperatorClient, "call", fake_call)

    answered: list[tuple[Any, bool]] = []

    async def fake_answer_callback_query(self: Any, callback_query_id: str, text: Any = None, show_alert: bool = False, **_kwargs: Any) -> bool:
        answered.append((text, show_alert))
        return True

    edited: list[Any] = []

    async def fake_edit_message_reply_markup(self: Any, *_args: Any, **_kwargs: Any) -> Any:
        edited.append(_kwargs.get("reply_markup"))
        return True

    application = Application.builder().token("123:test-token").build()
    application.bot._bot_user = User(
        id=999, is_bot=True, first_name="Mupot Test", username="mupot_test_bot"
    )
    monkeypatch.setattr(type(application.bot), "answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr(
        type(application.bot), "edit_message_reply_markup", fake_edit_message_reply_markup
    )

    adapter = TelegramAdapter(PlatformConfig(enabled=True, token="123:test-token", extra={}))
    core_callback_updates: list[Any] = []

    async def core_callback_handler(update: Any, _context: Any) -> None:
        core_callback_updates.append(update)

    # Hermes core's OWN real catch-all CallbackQueryHandler is
    # TelegramAdapter._handle_callback_query, wired by _register_handlers
    # below -- stand in for it (same convention test_telegram_control_native
    # uses for _handle_text_message/_handle_command) so a fall-through is
    # observable instead of silently invoking real Hermes callback routing.
    adapter._handle_callback_query = core_callback_handler
    monkeypatch.setattr(plugins, "get_plugin_manager", lambda: manager)
    adapter._wire_plugin_handlers(application)
    adapter._register_handlers(application)

    group_zero = application.handlers[0]
    callback_handlers = [h for h in group_zero if isinstance(h, CallbackQueryHandler)]
    # The plugin's scoped handler AND Hermes core's own catch-all both land in
    # group 0. PTB dispatches to the first handler in a group whose
    # check_update() matches; the plugin's handler must be registered FIRST
    # (see _wire_plugin_handlers docs) and its pattern must actually scope it
    # to "mv:" data -- otherwise it would swallow every core callback flow.
    assert len(callback_handlers) == 2
    assert callback_handlers[0].pattern.pattern == "^mv:"
    assert callback_handlers[1].callback is core_callback_handler

    keyboard, on_sent = inline_module.build_approval_keyboard(
        task_id="pending-task-1", presser=inline_module.VerifiedPresser(id=123)
    )
    on_sent(999)
    approve_nonce = keyboard.inline_keyboard[0][0].callback_data[len(inline_module.CALLBACK_PREFIX):]

    application._initialized = True
    await application.process_update(
        _callback_update(
            bot=application.bot,
            update_id=1,
            data=inline_module.CALLBACK_PREFIX + approve_nonce,
            chat_id=123,
            user_id=123,
            message_id=999,
        )
    )

    assert verdict_calls == [
        (
            "task_verdict",
            {
                "task_id": "pending-task-1",
                "verdict": "approve",
                "human_origin": {
                    "channel": "telegram",
                    "user_id": "123",
                    "chat_id": "123",
                    "message_id": "999",
                    "message_at": verdict_calls[0][1]["human_origin"]["message_at"],
                    "text": "approve pending-task-1",
                },
            },
        )
    ]
    assert answered == [("Recorded.", False)]
    assert edited == [None]
    assert core_callback_updates == []  # never fell through to the core catch-all

    # Replay: the SAME callback_data pressed again must not resubmit.
    await application.process_update(
        _callback_update(
            bot=application.bot,
            update_id=2,
            data=inline_module.CALLBACK_PREFIX + approve_nonce,
            chat_id=123,
            user_id=123,
            message_id=999,
        )
    )
    assert len(verdict_calls) == 1
    assert answered[-1][1] is True  # show_alert on the replay refusal

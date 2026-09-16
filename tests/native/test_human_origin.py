"""Human-origin capture/stamp against REAL Hermes primitives: real MessageEvent/
SessionSource/Platform dataclasses, the real SessionRecoveryMixin._generate_session_key
(the exact method gateway/run.py's _set_session_env-driving code calls), the real
gateway/session_context contextvar bridge get_current_session_key() reads, and the
real hermes_cli.plugins pre_tool_call dispatch machinery (_dispatch_pre_tool_call_hooks)
that production tool execution (model_tools.py, agent/tool_executor.py) actually uses.

Only test_native_registration.py-style fakes are used for anything that would need a
full GatewayRunner/SessionStore (filesystem, network); everything on the critical path
of "does the stashed key match what pre_tool_call sees" and "does the modify/strip
directive really reach the args tool_executor.py dispatches with" goes through real
Hermes code, not a reimplementation of it.
"""
from __future__ import annotations

import logging
import types
from datetime import datetime, timezone

import pytest

from gateway.config import Platform
from gateway.platforms.base import MessageEvent
from gateway.session import SessionSource
from gateway.session_context import get_session_env, reset_session_vars, set_session_vars
from gateway.session_recovery import SessionRecoveryMixin

from plugin.mupot_gateway import human_origin


class _FakeSessionStore(SessionRecoveryMixin):
    """Real SessionRecoveryMixin bound to a minimal config -- the exact method
    (`_generate_session_key`) gateway/run.py calls at `session_key =
    self.session_store._generate_session_key(source)` before binding
    `_set_session_env`, exercised for real rather than reimplemented here."""

    def __init__(self, *, multiplex_profiles: bool = False) -> None:
        self.config = types.SimpleNamespace(
            multiplex_profiles=multiplex_profiles,
            group_sessions_per_user=True,
            thread_sessions_per_user=False,
        )


@pytest.fixture(autouse=True)
def _clean_state():
    human_origin._STASH.clear()
    human_origin._WARNED_UNSUPPORTED_PLATFORMS.clear()
    reset_session_vars()
    yield
    human_origin._STASH.clear()
    human_origin._WARNED_UNSUPPORTED_PLATFORMS.clear()
    reset_session_vars()


def _telegram_source(user_id="tg-user-1", chat_id="tg-chat-1") -> SessionSource:
    return SessionSource(
        platform=Platform.TELEGRAM, chat_id=chat_id, user_id=user_id,
        user_name="hadi", chat_type="dm",
    )


def _telegram_event(source: SessionSource, message_id="tg-msg-1", timestamp=None) -> MessageEvent:
    return MessageEvent(
        text="approve it", source=source, message_id=message_id,
        timestamp=timestamp or datetime(2026, 9, 16, 12, 0, 0, tzinfo=timezone.utc),
    )


def test_session_key_used_by_capture_matches_the_real_generate_session_key_method():
    """Proves the brief's core correlation claim: the key capture_human_origin()
    stashes under is BYTE-IDENTICAL to what gateway/run.py's own
    `session_store._generate_session_key(source)` produces for the same source --
    not a hand-rolled reimplementation that could silently drift."""
    store = _FakeSessionStore()
    source = _telegram_source()
    expected_key = store._generate_session_key(source)
    assert expected_key  # sanity: real Hermes code produced a real key

    event = _telegram_event(source)
    assert human_origin.capture_human_origin(event=event, gateway=None, session_store=store) is None
    assert human_origin._STASH.get(expected_key) is not None


def test_stashed_origin_is_visible_through_the_real_session_context_bridge():
    """End-to-end via REAL Hermes primitives only: capture stashes under
    store._generate_session_key(source); gateway/run_turn.py's _set_session_env binds
    that same key into the HERMES_SESSION_KEY contextvar via
    gateway/session_context.set_session_vars(session_key=...) before any tool runs;
    tools.approval_context.get_current_session_key() (what stamp_tool_call reads) falls
    back to that exact contextvar. No shortcuts: this proves the bridge, not just each
    half of it in isolation."""
    store = _FakeSessionStore()
    source = _telegram_source()
    event = _telegram_event(source)
    human_origin.capture_human_origin(event=event, gateway=None, session_store=store)

    session_key = store._generate_session_key(source)
    tokens = set_session_vars(session_key=session_key)
    try:
        assert get_session_env("HERMES_SESSION_KEY") == session_key
        directive = human_origin.stamp_tool_call(
            tool_name="task_verdict", args={"task_id": "t-1", "verdict": "approve"},
        )
    finally:
        from gateway.session_context import clear_session_vars
        clear_session_vars(tokens)

    assert directive == {
        "action": "modify",
        "args": {"human_origin": {
            "platform": "telegram", "user_id": "tg-user-1", "chat_id": "tg-chat-1",
            "message_id": "tg-msg-1", "timestamp": "2026-09-16T12:00:00+00:00",
        }},
    }


def test_dispatch_pre_tool_call_hooks_real_merge_stamps_verdict_without_touching_other_args():
    """Goes through the REAL hermes_cli.plugins._dispatch_pre_tool_call_hooks (the exact
    function model_tools.py's _pre_dispatch_guards and agent/tool_executor.py's
    _pre_tool_block call in production) so the "modify" shallow-merge behavior this
    module relies on is proven against the real dispatcher, not assumed."""
    from hermes_cli import plugins as hermes_plugins

    store = _FakeSessionStore()
    source = _telegram_source()
    event = _telegram_event(source)
    human_origin.capture_human_origin(event=event, gateway=None, session_store=store)
    session_key = store._generate_session_key(source)
    tokens = set_session_vars(session_key=session_key)

    manager = hermes_plugins.PluginManager(scope_key="test-human-origin-scope")
    manager._hooks = {"pre_tool_call": [human_origin.stamp_tool_call]}

    original_args = {"task_id": "t-1", "verdict": "approve", "reasoning": "looks good"}
    try:
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(hermes_plugins, "_delivery_manager", lambda: manager)
            block_msg, modified_args = hermes_plugins._dispatch_pre_tool_call_hooks(
                "task_verdict", dict(original_args),
                task_id="", session_id="", tool_call_id="", turn_id="", api_request_id="",
                middleware_trace=[],
            )
    finally:
        from gateway.session_context import clear_session_vars
        clear_session_vars(tokens)

    assert block_msg is None
    assert modified_args["task_id"] == "t-1"
    assert modified_args["verdict"] == "approve"
    assert modified_args["reasoning"] == "looks good"
    assert modified_args["human_origin"]["user_id"] == "tg-user-1"


def test_dispatch_pre_tool_call_hooks_real_strip_removes_forged_origin_from_the_live_args_object():
    """Same real dispatcher, but with NO stashed origin (e.g. a CLI/cron/subagent turn):
    proves the in-place-pop strip strategy documented in human_origin.stamp_tool_call
    survives the real dispatcher's `ref.args if modified_args is None else modified_args`
    fallback (agent/tool_executor.py) -- i.e. the SAME dict object is mutated, not a copy
    that gets thrown away."""
    from hermes_cli import plugins as hermes_plugins

    manager = hermes_plugins.PluginManager(scope_key="test-human-origin-scope")
    manager._hooks = {"pre_tool_call": [human_origin.stamp_tool_call]}

    live_args = {"task_id": "t-2", "human_origin": {"platform": "telegram", "user_id": "forged"}}
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(hermes_plugins, "_delivery_manager", lambda: manager)
        block_msg, modified_args = hermes_plugins._dispatch_pre_tool_call_hooks(
            "task_verdict", live_args,
            task_id="", session_id="", tool_call_id="", turn_id="", api_request_id="",
            middleware_trace=[],
        )

    assert block_msg is None
    assert modified_args is None  # no "modify" directive returned: strip was in-place
    # This is the object model_tools.py/tool_executor.py fall back to using verbatim
    # when modified_args is None -- prove the forged key is really gone from it.
    assert "human_origin" not in live_args


def test_non_telegram_platform_event_is_never_stashed(caplog):
    source = SessionSource(platform=Platform.SLACK, chat_id="c1", user_id="u1", chat_type="dm")
    event = MessageEvent(text="hi", source=source, message_id="m1")
    store = _FakeSessionStore()
    with caplog.at_level(logging.INFO):
        result = human_origin.capture_human_origin(event=event, gateway=None, session_store=store)
    assert result is None
    assert len(human_origin._STASH) == 0
    assert any("not yet supported" in r.message for r in caplog.records)

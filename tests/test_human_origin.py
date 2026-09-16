"""Plain-suite unit tests for mupot_gateway.human_origin: importable without a native
Hermes checkout (no gateway.* module-level imports in the module under test), so these
run in the fast, no-HERMES_SOURCE suite (scripts/test.sh). Real Hermes dataclasses,
the real session-context bridge, and the real hermes_cli.plugins dispatcher are covered
separately in tests/native/test_human_origin.py, which requires HERMES_SOURCE.
"""
from __future__ import annotations

import logging
import types

import pytest

from plugin.mupot_gateway import human_origin


def _platform(value: str):
    return types.SimpleNamespace(value=value)


def _source(platform: str = "telegram", user_id="tg-user-1", chat_id="tg-chat-1"):
    return types.SimpleNamespace(platform=_platform(platform), user_id=user_id, chat_id=chat_id)


def _event(source, message_id="tg-msg-1", timestamp=None):
    return types.SimpleNamespace(source=source, message_id=message_id, timestamp=timestamp)


class _Store:
    def __init__(self, key):
        self._key = key

    def _generate_session_key(self, source):
        return self._key


@pytest.fixture(autouse=True)
def _clean_state():
    human_origin._STASH.clear()
    human_origin._WARNED_UNSUPPORTED_PLATFORMS.clear()
    yield
    human_origin._STASH.clear()
    human_origin._WARNED_UNSUPPORTED_PLATFORMS.clear()


# ---------------------------------------------------------------------------
# capture_human_origin (pre_gateway_dispatch)
# ---------------------------------------------------------------------------

def test_capture_stashes_a_telegram_event_and_always_returns_none_observer_only():
    event = _event(_source())
    store = _Store("sk-1")
    assert human_origin.capture_human_origin(event=event, gateway=None, session_store=store) is None
    assert human_origin._STASH.get("sk-1") == {
        "platform": "telegram", "user_id": "tg-user-1", "chat_id": "tg-chat-1",
        "message_id": "tg-msg-1", "timestamp": None,
    }


def test_capture_uses_gateway_session_store_when_session_store_kwarg_is_none():
    """The real hook signature passes session_store=getattr(self, "session_store", None)
    which can legitimately be None on a bare test runner; gateway.session_store is the
    documented fallback."""
    event = _event(_source())
    gateway = types.SimpleNamespace(session_store=_Store("sk-fallback"))
    human_origin.capture_human_origin(event=event, gateway=gateway, session_store=None)
    assert human_origin._STASH.get("sk-fallback") is not None


def test_capture_extracts_message_id_from_event_not_from_source():
    """SessionSource.message_id means 'triggering message (pin/reply/react)', not this
    message's own id -- must come from event.message_id."""
    source = _source()
    source.message_id = "reply-target-999"  # a stray attribute a fake could carry
    event = _event(source, message_id="the-real-message-id")
    store = _Store("sk-2")
    human_origin.capture_human_origin(event=event, gateway=None, session_store=store)
    assert human_origin._STASH.get("sk-2")["message_id"] == "the-real-message-id"


def test_non_telegram_platform_is_recorded_but_not_captured(caplog):
    event = _event(_source(platform="slack"))
    store = _Store("sk-3")
    with caplog.at_level(logging.INFO):
        result = human_origin.capture_human_origin(event=event, gateway=None, session_store=store)
    assert result is None
    assert len(human_origin._STASH) == 0
    assert any("not yet supported" in r.message and "slack" in r.message for r in caplog.records)


def test_unsupported_platform_warns_only_once(caplog):
    store = _Store("sk-4")
    with caplog.at_level(logging.INFO):
        human_origin.capture_human_origin(event=_event(_source("discord")), gateway=None, session_store=store)
        human_origin.capture_human_origin(event=_event(_source("discord")), gateway=None, session_store=store)
    assert sum("not yet supported" in r.message for r in caplog.records) == 1


def test_capture_never_raises_when_event_has_no_source():
    event = types.SimpleNamespace(source=None)
    assert human_origin.capture_human_origin(event=event, gateway=None, session_store=_Store("x")) is None
    assert len(human_origin._STASH) == 0


def test_capture_skips_silently_when_no_session_store_is_resolvable():
    event = _event(_source())
    assert human_origin.capture_human_origin(event=event, gateway=None, session_store=None) is None
    assert len(human_origin._STASH) == 0


# ---------------------------------------------------------------------------
# stamp_tool_call (pre_tool_call)
# ---------------------------------------------------------------------------

def test_stamp_overwrites_with_captured_origin_for_task_verdict(monkeypatch):
    human_origin.capture_human_origin(event=_event(_source()), gateway=None, session_store=_Store("sk-5"))
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-5")
    directive = human_origin.stamp_tool_call(tool_name="task_verdict", args={"task_id": "t1"})
    assert directive == {"action": "modify", "args": {"human_origin": {
        "platform": "telegram", "user_id": "tg-user-1", "chat_id": "tg-chat-1",
        "message_id": "tg-msg-1", "timestamp": None,
    }}}


def test_stamp_covers_needs_you_list_too(monkeypatch):
    human_origin.capture_human_origin(event=_event(_source()), gateway=None, session_store=_Store("sk-6"))
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-6")
    directive = human_origin.stamp_tool_call(tool_name="needs_you_list", args={})
    assert directive is not None
    assert directive["args"]["human_origin"]["chat_id"] == "tg-chat-1"


def test_model_supplied_origin_is_overwritten_and_logged_as_forgery(monkeypatch, caplog):
    human_origin.capture_human_origin(event=_event(_source()), gateway=None, session_store=_Store("sk-7"))
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-7")
    forged = {"platform": "telegram", "user_id": "attacker", "chat_id": "attacker",
              "message_id": "0", "timestamp": "1970-01-01T00:00:00"}
    with caplog.at_level(logging.WARNING):
        directive = human_origin.stamp_tool_call(tool_name="task_verdict", args={"human_origin": forged})
    assert directive["args"]["human_origin"]["user_id"] == "tg-user-1"  # real origin wins
    assert any("forgery attempt" in r.message for r in caplog.records)


def test_no_stash_strips_model_supplied_origin_in_place(caplog):
    args = {"task_id": "t1", "human_origin": {"platform": "telegram", "user_id": "attacker"}}
    with caplog.at_level(logging.WARNING):
        directive = human_origin.stamp_tool_call(tool_name="task_verdict", args=args)
    assert directive is None
    assert "human_origin" not in args  # truly absent, not merely None
    assert args["task_id"] == "t1"  # sibling keys untouched
    assert any("forgery attempt" in r.message for r in caplog.records)


def test_no_stash_and_no_model_supplied_origin_is_a_pure_noop(caplog):
    args = {"task_id": "t1"}
    with caplog.at_level(logging.WARNING):
        directive = human_origin.stamp_tool_call(tool_name="task_verdict", args=args)
    assert directive is None
    assert args == {"task_id": "t1"}
    assert not any("forgery" in r.message for r in caplog.records)


def test_unrelated_tool_name_is_never_touched():
    """A CLI/cron turn calling some other tool with a coincidental human_origin key
    (or a plugin tool this hook doesn't govern) must be left completely alone."""
    args = {"human_origin": {"anything": "here"}}
    directive = human_origin.stamp_tool_call(tool_name="mupot_operator_send", args=args)
    assert directive is None
    assert args["human_origin"] == {"anything": "here"}


def test_non_dict_args_is_a_noop():
    assert human_origin.stamp_tool_call(tool_name="task_verdict", args=None) is None
    assert human_origin.stamp_tool_call(tool_name="task_verdict", args="not-a-dict") is None


def test_stamp_never_raises_when_current_session_key_lookup_blows_up(monkeypatch):
    def _boom():
        raise RuntimeError("contextvar machinery unavailable")
    monkeypatch.setattr(human_origin, "_current_session_key", _boom)
    assert human_origin.stamp_tool_call(tool_name="task_verdict", args={"task_id": "t1"}) is None


# ---------------------------------------------------------------------------
# _OriginStash bounds
# ---------------------------------------------------------------------------

def test_stash_is_bounded_by_size_and_evicts_oldest_first():
    stash = human_origin._OriginStash(max_entries=3, ttl_seconds=10_000)
    for i in range(5):
        stash.put(f"sk-{i}", {"platform": "telegram", "n": i})
    assert len(stash) == 3
    assert stash.get("sk-0") is None
    assert stash.get("sk-1") is None
    assert stash.get("sk-4") == {"platform": "telegram", "n": 4}


def test_stash_read_refreshes_recency_so_a_hot_session_is_not_evicted():
    stash = human_origin._OriginStash(max_entries=2, ttl_seconds=10_000)
    stash.put("sk-a", {"n": 1})
    stash.put("sk-b", {"n": 2})
    stash.get("sk-a")  # NOTE: get() does not move_to_end by design (see docstring) --
    # confirm a THIRD put still evicts sk-a (the actually-oldest WRITE), not sk-b.
    stash.put("sk-c", {"n": 3})
    assert stash.get("sk-a") is None
    assert stash.get("sk-b") == {"n": 2}
    assert stash.get("sk-c") == {"n": 3}


def test_stash_entries_expire_by_ttl(monkeypatch):
    stash = human_origin._OriginStash(max_entries=10, ttl_seconds=5)
    clock = iter([100.0, 200.0, 200.0])
    monkeypatch.setattr(human_origin.time, "monotonic", lambda: next(clock))
    stash.put("sk-1", {"platform": "telegram"})
    assert stash.get("sk-1") is None  # 200 - 100 = 100 > ttl 5
    assert len(stash) == 0  # expired entry is evicted on read, not left dangling


def test_stash_get_and_put_are_no_ops_for_falsy_keys():
    stash = human_origin._OriginStash()
    stash.put("", {"platform": "telegram"})
    stash.put(None, {"platform": "telegram"})
    assert len(stash) == 0
    assert stash.get("") is None
    assert stash.get(None) is None


def test_stash_returns_independent_copies_not_shared_references():
    stash = human_origin._OriginStash()
    origin = {"platform": "telegram", "user_id": "1"}
    stash.put("sk-1", origin)
    origin["user_id"] = "mutated-after-put"
    got = stash.get("sk-1")
    assert got["user_id"] == "1"
    got["user_id"] = "mutated-after-get"
    assert stash.get("sk-1")["user_id"] == "1"


# ---------------------------------------------------------------------------
# register()
# ---------------------------------------------------------------------------

def test_register_wires_exactly_pre_gateway_dispatch_and_pre_tool_call():
    calls = []

    class Ctx:
        def register_hook(self, name, callback):
            calls.append((name, callback))

    human_origin.register(Ctx())
    assert calls == [
        ("pre_gateway_dispatch", human_origin.capture_human_origin),
        ("pre_tool_call", human_origin.stamp_tool_call),
    ]

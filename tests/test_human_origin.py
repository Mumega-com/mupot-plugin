"""Plain-suite unit tests for mupot_gateway.human_origin: importable without a native
Hermes checkout (no gateway.* module-level imports in the module under test), so these
run in the fast, no-HERMES_SOURCE suite (scripts/test.sh). Real Hermes dataclasses, the
real session-context bridge, the real hermes_cli.plugins dispatcher, the real
agent.delegation_context marker, and the real tools.mcp_tool_schema sanitizer are
covered separately in tests/native/test_human_origin.py, which requires HERMES_SOURCE.

Round 3 rewrite: kasra-review + Athena's round-2 BLOCK on PR#13 found the FIFO/
first-claim design was STILL first-claimant-wins (an internal/injected/cron turn on
the same session could spend a pending, unclaimed credit). This module now uses a
positive per-turn custody token (pre_llm_call content+sender match, see
human_origin.py's module docstring) instead of claiming; every test below either
proves that design or is a direct regression test for a named finding.
"""
from __future__ import annotations

import logging
import types

import pytest

from plugin.mupot_gateway import human_origin


def _platform(value: str):
    return types.SimpleNamespace(value=value)


def _raw_message(forwarded: bool = False):
    ns = types.SimpleNamespace()
    if forwarded:
        ns.forward_date = "2026-01-01T00:00:00"
    return ns


def _source(platform: str = "telegram", user_id="tg-user-1", chat_id="tg-user-1",
            chat_type: str = "dm", thread_id=None):
    return types.SimpleNamespace(platform=_platform(platform), user_id=user_id,
                                 chat_id=chat_id, chat_type=chat_type, thread_id=thread_id)


def _event(source, message_id="tg-msg-1", timestamp=None, raw_message=None, text="hello"):
    return types.SimpleNamespace(
        source=source, message_id=message_id, timestamp=timestamp, text=text,
        raw_message=raw_message if raw_message is not None else _raw_message(),
    )


class _Store:
    def __init__(self, key):
        self._key = key

    def _generate_session_key(self, source):
        return self._key


@pytest.fixture(autouse=True)
def _clean_state():
    human_origin._STASH.clear()
    human_origin._WARNED_UNSUPPORTED_PLATFORMS.clear()
    human_origin._SESSION_ID_TO_KEY.clear()
    human_origin.set_mcp_server_name(None)
    yield
    human_origin._STASH.clear()
    human_origin._WARNED_UNSUPPORTED_PLATFORMS.clear()
    human_origin._SESSION_ID_TO_KEY.clear()
    human_origin.set_mcp_server_name(None)


def _capture(session_key, text="hello", **source_kwargs):
    store = _Store(session_key)
    human_origin.capture_human_origin(
        event=_event(_source(**source_kwargs), text=text), gateway=None, session_store=store,
    )
    return session_key


def _bind(session_key, turn_id, *, text="hello", sender_id="tg-user-1", platform="telegram", monkeypatch):
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: session_key)
    return human_origin.bind_turn_custody(
        turn_id=turn_id, user_message=text, sender_id=sender_id, platform=platform,
    )


# ---------------------------------------------------------------------------
# capture_human_origin (pre_gateway_dispatch) — trust fence (unchanged from r2)
# ---------------------------------------------------------------------------

def test_capture_stashes_a_private_dm_and_always_returns_none_observer_only():
    event = _event(_source())
    store = _Store("sk-1")
    assert human_origin.capture_human_origin(event=event, gateway=None, session_store=store) is None
    assert len(human_origin._STASH) == 1


def test_capture_records_chat_type_thread_id_forwarded_and_text_hash():
    event = _event(_source(thread_id="7"), text="approve it")
    store = _Store("sk-1b")
    human_origin.capture_human_origin(event=event, gateway=None, session_store=store)
    # peek via a matching bind
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(human_origin, "_current_session_key", lambda: "sk-1b")
        assert human_origin.bind_turn_custody(
            turn_id="T1", user_message="approve it", sender_id="tg-user-1", platform="telegram",
        ) is None
    origin = human_origin._STASH.read("sk-1b", "T1")
    assert origin["chat_type"] == "dm"
    assert origin["thread_id"] == "7"
    assert origin["forwarded"] is False
    assert "text_sha256" in origin  # internal correlation field is present in the raw record


def test_capture_refuses_a_group_chat():
    event = _event(_source(chat_type="group", chat_id="-1001234"))
    store = _Store("sk-group")
    human_origin.capture_human_origin(event=event, gateway=None, session_store=store)
    assert len(human_origin._STASH) == 0


def test_capture_refuses_when_chat_id_differs_from_user_id():
    event = _event(_source(chat_type="dm", user_id="u1", chat_id="different"))
    store = _Store("sk-mismatch")
    human_origin.capture_human_origin(event=event, gateway=None, session_store=store)
    assert len(human_origin._STASH) == 0


def test_capture_refuses_a_forwarded_message():
    event = _event(_source(), raw_message=_raw_message(forwarded=True))
    store = _Store("sk-fwd")
    human_origin.capture_human_origin(event=event, gateway=None, session_store=store)
    assert len(human_origin._STASH) == 0


def test_capture_refuses_when_raw_message_is_missing_fail_closed():
    event = types.SimpleNamespace(source=_source(), message_id="tg-msg-1", timestamp=None,
                                  raw_message=None, text="hi")
    store = _Store("sk-noraw")
    human_origin.capture_human_origin(event=event, gateway=None, session_store=store)
    assert len(human_origin._STASH) == 0


def test_non_telegram_platform_is_recorded_but_not_captured(caplog):
    event = _event(_source(platform="slack", chat_id="tg-user-1"))
    store = _Store("sk-3")
    with caplog.at_level(logging.INFO):
        result = human_origin.capture_human_origin(event=event, gateway=None, session_store=store)
    assert result is None
    assert len(human_origin._STASH) == 0
    assert any("not yet supported" in r.message and "slack" in r.message for r in caplog.records)


def test_capture_never_raises_when_event_has_no_source():
    event = types.SimpleNamespace(source=None)
    assert human_origin.capture_human_origin(event=event, gateway=None, session_store=_Store("x")) is None


def test_capture_skips_silently_when_no_session_store_is_resolvable():
    event = _event(_source())
    assert human_origin.capture_human_origin(event=event, gateway=None, session_store=None) is None
    assert len(human_origin._STASH) == 0


# ---------------------------------------------------------------------------
# bind_turn_custody (pre_llm_call) — the positive custody token
# ---------------------------------------------------------------------------

def test_bind_matches_on_exact_text_and_sender(monkeypatch):
    _capture("sk-bind-1", text="approve f9408956")
    result = _bind("sk-bind-1", "T1", text="approve f9408956", monkeypatch=monkeypatch)
    assert result is None  # observer hook, no return value expected by callers
    bound = human_origin._STASH.read("sk-bind-1", "T1")
    assert bound is not None
    assert bound["user_id"] == "tg-user-1"


def test_bind_refuses_non_telegram_platform(monkeypatch):
    _capture("sk-bind-2", text="approve it")
    _bind("sk-bind-2", "T1", text="approve it", platform="slack", monkeypatch=monkeypatch)
    assert human_origin._STASH.read("sk-bind-2", "T1") is None


def test_bind_refuses_without_turn_id(monkeypatch):
    _capture("sk-bind-3", text="approve it")
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-bind-3")
    human_origin.bind_turn_custody(turn_id="", user_message="approve it",
                                   sender_id="tg-user-1", platform="telegram")
    assert human_origin._STASH.read("sk-bind-3", "") is None


def test_bind_refuses_non_string_user_message(monkeypatch):
    _capture("sk-bind-4", text="approve it")
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-bind-4")
    human_origin.bind_turn_custody(turn_id="T1", user_message=None,
                                   sender_id="tg-user-1", platform="telegram")
    assert human_origin._STASH.read("sk-bind-4", "T1") is None


def test_bind_refuses_when_sender_id_does_not_match(monkeypatch):
    _capture("sk-bind-5", text="approve it")
    _bind("sk-bind-5", "T1", text="approve it", sender_id="a-different-user", monkeypatch=monkeypatch)
    assert human_origin._STASH.read("sk-bind-5", "T1") is None


def test_bind_refuses_when_text_does_not_match(monkeypatch):
    _capture("sk-bind-6", text="approve it")
    _bind("sk-bind-6", "T1", text="something else entirely", monkeypatch=monkeypatch)
    assert human_origin._STASH.read("sk-bind-6", "T1") is None


def test_bind_never_raises_when_current_session_key_lookup_blows_up(monkeypatch):
    def _boom():
        raise RuntimeError("contextvar machinery unavailable")
    monkeypatch.setattr(human_origin, "_current_session_key", _boom)
    human_origin.bind_turn_custody(turn_id="T1", user_message="x", sender_id="u",
                                   platform="telegram")  # must not raise


def test_bind_refused_for_delegated_child_context(monkeypatch):
    _capture("sk-bind-7", text="approve it")
    monkeypatch.setattr(human_origin, "_in_delegated_child_context", lambda: True)
    _bind("sk-bind-7", "T-child", text="approve it", monkeypatch=monkeypatch)
    assert human_origin._STASH.read("sk-bind-7", "T-child") is None
    monkeypatch.setattr(human_origin, "_in_delegated_child_context", lambda: False)
    _bind("sk-bind-7", "T-parent", text="approve it", monkeypatch=monkeypatch)
    assert human_origin._STASH.read("sk-bind-7", "T-parent") is not None


def test_injected_turn_with_unclaimed_pending_record_never_binds(monkeypatch):
    """The exact class kasra-review/Athena's round-2 BLOCK named: the human's own
    turn made NO governed call (ordinary chatter), so the record is still pending
    when an internal/injected turn asks. Its user_message is the injected prompt
    text, never the human's, so bind() cannot match it regardless of session/turn
    ordering."""
    _capture("sk-injected", text="hey, how's the build going?")
    # human's own turn: makes no governed tool call at all -- record stays pending.
    # An injected turn on the SAME session, fresh turn_id, injected prompt text:
    _bind("sk-injected", "T-injected-notification-turn",
          text="[Automated Mupot event] please review the pending decision",
          monkeypatch=monkeypatch)
    assert human_origin._STASH.read("sk-injected", "T-injected-notification-turn") is None
    directive = human_origin.stamp_tool_call(
        tool_name="task_verdict", args={"task_id": "t-attacker"}, turn_id="T-injected-notification-turn",
    )
    assert directive is None


def test_chatter_then_approve_stamps_the_approve_messages_id(monkeypatch):
    """Two DISTINCT pending records (different text); the approve turn's bind must
    match its OWN message, not the chatter's."""
    store = _Store("sk-order")
    human_origin.capture_human_origin(
        event=_event(_source(), message_id="M-chatter", text="hey what's up"),
        gateway=None, session_store=store,
    )
    human_origin.capture_human_origin(
        event=_event(_source(), message_id="M-approve", text="approve f9408956"),
        gateway=None, session_store=store,
    )
    # chatter's own turn binds its own (unhelpful) record; never read.
    _bind("sk-order", "T-chatter", text="hey what's up", monkeypatch=monkeypatch)
    # approve turn binds ITS OWN message.
    _bind("sk-order", "T-approve", text="approve f9408956", monkeypatch=monkeypatch)

    directive = human_origin.stamp_tool_call(
        tool_name="task_verdict", args={"task_id": "t-real"}, turn_id="T-approve",
    )
    assert directive["args"]["human_origin"]["message_id"] == "M-approve"


def test_same_text_twice_binds_oldest_match_first_one_credit_each(monkeypatch):
    store = _Store("sk-dup")
    human_origin.capture_human_origin(
        event=_event(_source(), message_id="M1", text="approve X"), gateway=None, session_store=store,
    )
    human_origin.capture_human_origin(
        event=_event(_source(), message_id="M2", text="approve X"), gateway=None, session_store=store,
    )
    _bind("sk-dup", "T1", text="approve X", monkeypatch=monkeypatch)
    _bind("sk-dup", "T2", text="approve X", monkeypatch=monkeypatch)
    d1 = human_origin.stamp_tool_call(tool_name="task_verdict", args={}, turn_id="T1")
    d2 = human_origin.stamp_tool_call(tool_name="task_verdict", args={}, turn_id="T2")
    assert d1["args"]["human_origin"]["message_id"] == "M1"
    assert d2["args"]["human_origin"]["message_id"] == "M2"
    # a THIRD turn with the same text finds nothing left to bind:
    _bind("sk-dup", "T3", text="approve X", monkeypatch=monkeypatch)
    d3 = human_origin.stamp_tool_call(tool_name="task_verdict", args={}, turn_id="T3")
    assert d3 is None


# ---------------------------------------------------------------------------
# stamp_tool_call (pre_tool_call) — read-only, strip-first regardless of server
# ---------------------------------------------------------------------------

def test_stamp_reads_a_bound_record_for_bare_name(monkeypatch):
    _capture("sk-stamp-1", text="approve")
    _bind("sk-stamp-1", "T1", text="approve", monkeypatch=monkeypatch)
    directive = human_origin.stamp_tool_call(tool_name="task_verdict", args={"task_id": "t1"}, turn_id="T1")
    assert directive["args"]["human_origin"]["user_id"] == "tg-user-1"


def test_stamp_reads_a_bound_record_via_the_configured_wire_name(monkeypatch):
    _capture("sk-stamp-2", text="approve")
    _bind("sk-stamp-2", "T1", text="approve", monkeypatch=monkeypatch)
    human_origin.set_mcp_server_name("mupot")
    directive = human_origin.stamp_tool_call(
        tool_name="mcp__mupot__task_verdict", args={"task_id": "t1"}, turn_id="T1",
    )
    assert directive is not None


def test_stamp_never_reads_across_turns_even_with_a_bound_record_elsewhere(monkeypatch):
    _capture("sk-stamp-3", text="approve")
    _bind("sk-stamp-3", "T-bound", text="approve", monkeypatch=monkeypatch)
    directive = human_origin.stamp_tool_call(
        tool_name="task_verdict", args={"task_id": "t1"}, turn_id="T-different",
    )
    assert directive is None


def test_stamp_strips_forged_origin_for_looks_governed_suffix_even_without_a_bind(caplog):
    args = {"task_id": "t1", "human_origin": {"platform": "telegram", "user_id": "attacker"}}
    with caplog.at_level(logging.WARNING):
        directive = human_origin.stamp_tool_call(tool_name="task_verdict", args=args, turn_id="T-unbound")
    assert directive is None
    assert "human_origin" not in args
    assert any("forgery attempt" in r.message for r in caplog.records)


def test_stamp_strips_regardless_of_server_name_mismatch_p1_1(caplog):
    """kasra-review round-2 P1-1: a misconfigured/wrong server name must never let
    a forged human_origin pass through unstripped."""
    human_origin.set_mcp_server_name("some-other-server")
    args = {"human_origin": {"user_id": "attacker"}}
    with caplog.at_level(logging.WARNING):
        directive = human_origin.stamp_tool_call(
            tool_name="mcp__mupot__task_verdict", args=args, turn_id="T1",
        )
    assert directive is None
    assert "human_origin" not in args


def test_stamp_never_stamps_when_server_name_is_unset_p1_2(monkeypatch, caplog):
    """kasra-review round-2 P1-2: before adapter_factory ever calls
    set_mcp_server_name (module default is unset), a wire-named call must strip,
    never stamp -- even though a bound record legitimately exists."""
    _capture("sk-unset", text="approve")
    _bind("sk-unset", "T1", text="approve", monkeypatch=monkeypatch)
    assert human_origin._mcp_server_name is None  # sanity: still unset
    args = {"human_origin": {"user_id": "attacker"}}
    with caplog.at_level(logging.WARNING):
        directive = human_origin.stamp_tool_call(
            tool_name="mcp__mupot__task_verdict", args=args, turn_id="T1",
        )
    assert directive is None
    assert "human_origin" not in args


def test_stamp_unrelated_tool_name_is_never_touched():
    args = {"human_origin": {"anything": "here"}}
    directive = human_origin.stamp_tool_call(tool_name="mupot_operator_send", args=args, turn_id="T1")
    assert directive is None
    assert args["human_origin"] == {"anything": "here"}


def test_stamp_non_dict_args_is_a_noop():
    assert human_origin.stamp_tool_call(tool_name="task_verdict", args=None, turn_id="T1") is None
    assert human_origin.stamp_tool_call(tool_name="task_verdict", args="not-a-dict", turn_id="T1") is None


def test_stamp_no_bind_and_no_model_supplied_is_a_pure_noop(caplog):
    args = {"task_id": "t1"}
    with caplog.at_level(logging.WARNING):
        directive = human_origin.stamp_tool_call(tool_name="task_verdict", args=args, turn_id="T1")
    assert directive is None
    assert args == {"task_id": "t1"}
    assert not any("forgery" in r.message for r in caplog.records)


def test_stamp_never_raises_when_current_session_key_lookup_blows_up(monkeypatch):
    def _boom():
        raise RuntimeError("contextvar machinery unavailable")
    monkeypatch.setattr(human_origin, "_current_session_key", _boom)
    assert human_origin.stamp_tool_call(tool_name="task_verdict", args={"task_id": "t1"}, turn_id="T1") is None


# ---------------------------------------------------------------------------
# _resolve_governed_tool_name / _looks_like_governed_suffix / sanitizer
# ---------------------------------------------------------------------------

def test_resolve_governed_tool_name_bare_always_works_even_unset():
    assert human_origin._resolve_governed_tool_name("task_verdict") == "task_verdict"
    assert human_origin._resolve_governed_tool_name("needs_you_list") == "needs_you_list"


def test_resolve_governed_tool_name_prefixed_requires_server_set():
    assert human_origin._resolve_governed_tool_name("mcp__mupot__task_verdict") is None
    human_origin.set_mcp_server_name("mupot")
    assert human_origin._resolve_governed_tool_name("mcp__mupot__task_verdict") == "task_verdict"


def test_resolve_governed_tool_name_sanitizes_hyphenated_server_name():
    human_origin.set_mcp_server_name("mupot-prod")
    assert human_origin._resolve_governed_tool_name("mcp__mupot_prod__task_verdict") == "task_verdict"
    # the UNsanitized literal must NOT match:
    assert human_origin._resolve_governed_tool_name("mcp__mupot-prod__task_verdict") is None


def test_resolve_governed_tool_name_none_for_unrelated():
    assert human_origin._resolve_governed_tool_name("mupot_operator_send") is None
    assert human_origin._resolve_governed_tool_name(123) is None
    assert human_origin._resolve_governed_tool_name(None) is None


@pytest.mark.parametrize("name", [
    "mupot__task_verdict", "mupot.task_verdict", "Task_Verdict", "task_verdict ",
])
def test_looks_like_governed_suffix_rejects_lookalikes(name):
    assert human_origin._looks_like_governed_suffix(name) is None


def test_looks_like_governed_suffix_matches_any_server():
    assert human_origin._looks_like_governed_suffix("mcp__literally_anything__task_verdict") == "task_verdict"
    assert human_origin._looks_like_governed_suffix("task_verdict") == "task_verdict"


def test_set_mcp_server_name_falsy_means_unset_not_default():
    human_origin.set_mcp_server_name("mupot")
    assert human_origin._resolve_governed_tool_name("mcp__mupot__task_verdict") == "task_verdict"
    human_origin.set_mcp_server_name(None)
    assert human_origin._resolve_governed_tool_name("mcp__mupot__task_verdict") is None
    human_origin.set_mcp_server_name("")
    assert human_origin._resolve_governed_tool_name("mcp__mupot__task_verdict") is None


# ---------------------------------------------------------------------------
# _OriginStash
# ---------------------------------------------------------------------------

def test_stash_bind_pops_from_pending_and_read_finds_it():
    stash = human_origin._OriginStash()
    stash.capture("sk", {"user_id": "u1", "text_sha256": "h1"})
    assert stash.bind("sk", "T1", sender_id="u1", text_sha256="h1") is True
    assert stash.read("sk", "T1") is not None
    assert stash.read("sk", "T1")["user_id"] == "u1"


def test_stash_bind_is_idempotent_for_the_same_turn():
    stash = human_origin._OriginStash()
    stash.capture("sk", {"user_id": "u1", "text_sha256": "h1"})
    assert stash.bind("sk", "T1", sender_id="u1", text_sha256="h1") is True
    stash.capture("sk", {"user_id": "u1", "text_sha256": "h1"})
    # re-binding the SAME turn again does not consume a second record
    assert stash.bind("sk", "T1", sender_id="u1", text_sha256="h1") is True
    assert len(stash) == 2  # 1 still-bound, 1 still-pending


def test_stash_bind_no_match_leaves_pending_untouched():
    stash = human_origin._OriginStash()
    stash.capture("sk", {"user_id": "u1", "text_sha256": "h1"})
    assert stash.bind("sk", "T1", sender_id="u1", text_sha256="WRONG") is False
    assert stash.bind("sk", "T2", sender_id="WRONG", text_sha256="h1") is False
    assert stash.bind("sk", "T3", sender_id="u1", text_sha256="h1") is True


def test_stash_bind_consumes_oldest_matching_record_first():
    stash = human_origin._OriginStash()
    stash.capture("sk", {"user_id": "u1", "text_sha256": "h1", "message_id": "M1"})
    stash.capture("sk", {"user_id": "u1", "text_sha256": "h1", "message_id": "M2"})
    assert stash.bind("sk", "T1", sender_id="u1", text_sha256="h1") is True
    assert stash.read("sk", "T1")["message_id"] == "M1"
    assert stash.bind("sk", "T2", sender_id="u1", text_sha256="h1") is True
    assert stash.read("sk", "T2")["message_id"] == "M2"
    assert stash.bind("sk", "T3", sender_id="u1", text_sha256="h1") is False


def test_stash_read_never_pops_or_consumes():
    stash = human_origin._OriginStash()
    stash.capture("sk", {"user_id": "u1", "text_sha256": "h1"})
    stash.bind("sk", "T1", sender_id="u1", text_sha256="h1")
    stash.read("sk", "T1")
    stash.read("sk", "T1")
    assert stash.read("sk", "T1") is not None


def test_stash_pending_is_bounded_per_session():
    stash = human_origin._OriginStash(max_pending_per_session=2, max_pending_total=100, max_bound=100, ttl_seconds=10_000)
    for i in range(5):
        stash.capture("sk", {"user_id": "u", "text_sha256": f"h{i}"})
    assert stash.bind("sk", "t1", sender_id="u", text_sha256="h3") is True
    assert stash.bind("sk", "t2", sender_id="u", text_sha256="h4") is True
    assert stash.bind("sk", "t3", sender_id="u", text_sha256="h0") is False  # already evicted


def test_stash_pending_is_bounded_globally_across_sessions():
    stash = human_origin._OriginStash(max_pending_per_session=10, max_pending_total=2, max_bound=100, ttl_seconds=10_000)
    stash.capture("sk-a", {"user_id": "u", "text_sha256": "a"})
    stash.capture("sk-b", {"user_id": "u", "text_sha256": "b"})
    stash.capture("sk-c", {"user_id": "u", "text_sha256": "c"})  # evicts sk-a's entry
    assert stash.bind("sk-a", "t1", sender_id="u", text_sha256="a") is False
    assert stash.bind("sk-b", "t2", sender_id="u", text_sha256="b") is True
    assert stash.bind("sk-c", "t3", sender_id="u", text_sha256="c") is True


def test_stash_ttl_expires_pending_records(monkeypatch):
    stash = human_origin._OriginStash(ttl_seconds=5)
    clock = iter([100.0, 200.0])
    monkeypatch.setattr(human_origin.time, "monotonic", lambda: next(clock))
    stash.capture("sk", {"user_id": "u", "text_sha256": "h"})
    assert stash.bind("sk", "T1", sender_id="u", text_sha256="h") is False  # 100s > ttl 5


def test_stash_ttl_expires_bound_records_too(monkeypatch):
    stash = human_origin._OriginStash(ttl_seconds=5)
    clock = iter([100.0, 100.0, 200.0])
    monkeypatch.setattr(human_origin.time, "monotonic", lambda: next(clock))
    stash.capture("sk", {"user_id": "u", "text_sha256": "h"})
    assert stash.bind("sk", "T1", sender_id="u", text_sha256="h") is True
    assert stash.read("sk", "T1") is None  # now expired


def test_stash_bound_refreshes_lru_on_read():
    stash = human_origin._OriginStash(max_bound=2, max_pending_total=100, max_pending_per_session=100, ttl_seconds=10_000)
    stash.capture("sk-a", {"user_id": "u", "text_sha256": "a"})
    stash.capture("sk-b", {"user_id": "u", "text_sha256": "b"})
    stash.bind("sk-a", "ta", sender_id="u", text_sha256="a")
    stash.bind("sk-b", "tb", sender_id="u", text_sha256="b")
    stash.read("sk-a", "ta")  # refresh
    stash.capture("sk-c", {"user_id": "u", "text_sha256": "c"})
    stash.bind("sk-c", "tc", sender_id="u", text_sha256="c")  # forces eviction
    assert stash.read("sk-a", "ta") is not None
    assert stash.read("sk-b", "tb") is None


def test_stash_drop_session_clears_pending_and_bound():
    stash = human_origin._OriginStash()
    stash.capture("sk", {"user_id": "u", "text_sha256": "a"})
    stash.capture("sk", {"user_id": "u", "text_sha256": "b"})
    stash.bind("sk", "T1", sender_id="u", text_sha256="a")
    stash.drop_session("sk")
    assert len(stash) == 0
    assert stash.read("sk", "T1") is None
    assert stash.bind("sk", "T2", sender_id="u", text_sha256="b") is False


def test_stash_capture_bind_are_no_ops_for_falsy_keys():
    stash = human_origin._OriginStash()
    stash.capture("", {"user_id": "u"})
    stash.capture(None, {"user_id": "u"})
    assert len(stash) == 0
    assert stash.bind("", "t", sender_id="u", text_sha256="h") is False
    assert stash.bind("sk", "", sender_id="u", text_sha256="h") is False


def test_stash_returns_independent_copies_not_shared_references():
    stash = human_origin._OriginStash()
    record = {"user_id": "u", "text_sha256": "h"}
    stash.capture("sk", record)
    record["user_id"] = "mutated"
    stash.bind("sk", "T1", sender_id="u", text_sha256="h")
    got = stash.read("sk", "T1")
    assert got["user_id"] == "u"
    got["user_id"] = "mutated-after-read"
    assert stash.read("sk", "T1")["user_id"] == "u"


# ---------------------------------------------------------------------------
# on_session_boundary
# ---------------------------------------------------------------------------

def test_session_boundary_drops_records_for_that_session(monkeypatch):
    _capture("sk-boundary", text="approve")
    _bind("sk-boundary", "T1", text="approve", monkeypatch=monkeypatch)
    human_origin._remember_session_id("db-session-1", "sk-boundary")
    assert human_origin._STASH.read("sk-boundary", "T1") is not None
    human_origin._on_session_boundary(session_id="db-session-1")
    assert human_origin._STASH.read("sk-boundary", "T1") is None


def test_session_boundary_with_unknown_session_id_is_a_noop():
    human_origin._on_session_boundary(session_id="never-seen")  # must not raise


def test_session_boundary_never_raises():
    human_origin._on_session_boundary(session_id="")  # must not raise


# ---------------------------------------------------------------------------
# register()
# ---------------------------------------------------------------------------

def test_register_wires_all_five_hooks():
    calls = []

    class Ctx:
        def register_hook(self, name, callback):
            calls.append(name)

    human_origin.register(Ctx())
    assert calls == [
        "pre_gateway_dispatch", "pre_llm_call", "pre_tool_call",
        "on_session_reset", "on_session_end",
    ]


def test_register_fails_closed_when_ctx_has_no_register_hook(caplog):
    class OldCtx:
        pass

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError):
            human_origin.register(OldCtx())
    assert any("register_hook" in r.message for r in caplog.records)


def test_register_fails_closed_and_reraises_when_register_hook_itself_raises(caplog):
    class BoomCtx:
        def register_hook(self, *_a, **_kw):
            raise ValueError("simulated Hermes-side registration failure")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(ValueError):
            human_origin.register(BoomCtx())
    assert any("registration failed" in r.message for r in caplog.records)

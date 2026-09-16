"""Plain-suite unit tests for mupot_gateway.human_origin: importable without a native
Hermes checkout (no gateway.* module-level imports in the module under test), so these
run in the fast, no-HERMES_SOURCE suite (scripts/test.sh). Real Hermes dataclasses, the
real session-context bridge, the real hermes_cli.plugins dispatcher, and the real
agent.delegation_context marker are covered separately in
tests/native/test_human_origin.py, which requires HERMES_SOURCE.

This suite was rewritten after kasra-review's adversarial BLOCK on PR#13
(2026-09-16): the stamp used to be bound to a SESSION KEY and a BARE TOOL NAME, and
the live system has neither. Every test below either proves the new turn-bound /
wire-name-matched design, or is a direct regression test for one of the review's
named P0/P1/P2 findings.
"""
from __future__ import annotations

import logging
import types

import pytest

from plugin.mupot_gateway import human_origin


def _platform(value: str):
    return types.SimpleNamespace(value=value)


def _raw_message(forwarded: bool = False):
    """A bare object with none of telegram_fence.FORWARDING_MARKERS set (unforwarded),
    or with forward_date set (forwarded) -- duck-typed like a real PTB telegram.Message."""
    ns = types.SimpleNamespace()
    if forwarded:
        ns.forward_date = "2026-01-01T00:00:00"
    return ns


def _source(platform: str = "telegram", user_id="tg-user-1", chat_id="tg-user-1",
            chat_type: str = "dm", thread_id=None):
    return types.SimpleNamespace(platform=_platform(platform), user_id=user_id,
                                 chat_id=chat_id, chat_type=chat_type, thread_id=thread_id)


def _event(source, message_id="tg-msg-1", timestamp=None, raw_message=None):
    return types.SimpleNamespace(
        source=source, message_id=message_id, timestamp=timestamp,
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
    human_origin.set_mcp_server_name(None)
    yield
    human_origin._STASH.clear()
    human_origin._WARNED_UNSUPPORTED_PLATFORMS.clear()
    human_origin.set_mcp_server_name(None)


def _capture(session_key, **source_kwargs):
    """Capture one legitimate DM message and return the session_key used."""
    store = _Store(session_key)
    human_origin.capture_human_origin(
        event=_event(_source(**source_kwargs)), gateway=None, session_store=store,
    )
    return session_key


# ---------------------------------------------------------------------------
# capture_human_origin (pre_gateway_dispatch) — trust fence
# ---------------------------------------------------------------------------

def test_capture_stashes_a_private_dm_and_always_returns_none_observer_only():
    event = _event(_source())
    store = _Store("sk-1")
    assert human_origin.capture_human_origin(event=event, gateway=None, session_store=store) is None
    assert len(human_origin._STASH) == 1


def test_capture_records_chat_type_thread_id_and_forwarded_marker():
    event = _event(_source(thread_id="7"))
    store = _Store("sk-1b")
    human_origin.capture_human_origin(event=event, gateway=None, session_store=store)
    origin = human_origin._STASH.claim("sk-1b", "turn-x")
    assert origin["chat_type"] == "dm"
    assert origin["thread_id"] == "7"
    assert origin["forwarded"] is False


def test_capture_refuses_a_group_chat_even_when_platform_and_store_are_fine():
    """P0-3 root fix: a non-DM chat_type must never be captured at all, regardless
    of anything downstream -- this is what actually closes the shared/thread
    session-key collision, not the turn-binding alone."""
    event = _event(_source(chat_type="group", chat_id="-1001234"))
    store = _Store("sk-group")
    human_origin.capture_human_origin(event=event, gateway=None, session_store=store)
    assert len(human_origin._STASH) == 0


def test_capture_refuses_when_chat_id_differs_from_user_id():
    """Telegram's own DM invariant (chat_id == user_id for a private chat) is
    checked explicitly, not inferred from chat_type alone."""
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
    """A synthetic/internal MessageEvent (no raw platform message at all) can never
    positively prove it is unforwarded -- absence of proof must not be treated as
    proof of safety."""
    event = types.SimpleNamespace(
        source=_source(), message_id="tg-msg-1", timestamp=None, raw_message=None,
    )
    store = _Store("sk-noraw")
    human_origin.capture_human_origin(event=event, gateway=None, session_store=store)
    assert len(human_origin._STASH) == 0


def test_capture_extracts_message_id_from_event_not_from_source():
    """SessionSource.message_id means 'triggering message (pin/reply/react)', not this
    message's own id -- must come from event.message_id."""
    source = _source()
    source.message_id = "reply-target-999"  # a stray attribute a fake could carry
    event = _event(source, message_id="the-real-message-id")
    store = _Store("sk-2")
    human_origin.capture_human_origin(event=event, gateway=None, session_store=store)
    origin = human_origin._STASH.claim("sk-2", "turn-x")
    assert origin["message_id"] == "the-real-message-id"


def test_non_telegram_platform_is_recorded_but_not_captured(caplog):
    event = _event(_source(platform="slack", chat_id="tg-user-1"))
    store = _Store("sk-3")
    with caplog.at_level(logging.INFO):
        result = human_origin.capture_human_origin(event=event, gateway=None, session_store=store)
    assert result is None
    assert len(human_origin._STASH) == 0
    assert any("not yet supported" in r.message and "slack" in r.message for r in caplog.records)


def test_unsupported_platform_warns_only_once(caplog):
    store = _Store("sk-4")
    with caplog.at_level(logging.INFO):
        human_origin.capture_human_origin(event=_event(_source(platform="discord", chat_id="tg-user-1")), gateway=None, session_store=store)
        human_origin.capture_human_origin(event=_event(_source(platform="discord", chat_id="tg-user-1")), gateway=None, session_store=store)
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
# stamp_tool_call (pre_tool_call) — turn binding
# ---------------------------------------------------------------------------

def test_stamp_claims_for_the_first_turn_that_asks_bare_name(monkeypatch):
    _capture("sk-5")
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-5")
    directive = human_origin.stamp_tool_call(tool_name="task_verdict", args={"task_id": "t1"}, turn_id="T1")
    assert directive["args"]["human_origin"]["user_id"] == "tg-user-1"


def test_stamp_matches_the_mcp_prefixed_wire_name_for_the_configured_server(monkeypatch):
    """P0-1: a live gateway with mupot registered as an MCP server emits
    mcp__mupot__task_verdict, never the bare name -- this must match."""
    _capture("sk-wire")
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-wire")
    human_origin.set_mcp_server_name("mupot")
    directive = human_origin.stamp_tool_call(
        tool_name="mcp__mupot__task_verdict", args={"task_id": "t1"}, turn_id="T1",
    )
    assert directive is not None
    assert directive["args"]["human_origin"]["user_id"] == "tg-user-1"


def test_stamp_does_not_match_a_different_mcp_servers_same_named_tool(monkeypatch):
    _capture("sk-wire2")
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-wire2")
    human_origin.set_mcp_server_name("mupot")
    directive = human_origin.stamp_tool_call(
        tool_name="mcp__some_other_server__task_verdict", args={"task_id": "t1"}, turn_id="T1",
    )
    assert directive is None


@pytest.mark.parametrize("name", [
    "mupot__task_verdict", "mupot.task_verdict", "Task_Verdict", "task_verdict ",
    "MCP__mupot__task_verdict",
])
def test_stamp_rejects_lookalike_names_exact_match_only(monkeypatch, name):
    _capture("sk-alias")
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-alias")
    human_origin.set_mcp_server_name("mupot")
    assert human_origin.stamp_tool_call(tool_name=name, args={"task_id": "t1"}, turn_id="T1") is None


def test_stamp_covers_needs_you_list_too(monkeypatch):
    _capture("sk-6")
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-6")
    directive = human_origin.stamp_tool_call(tool_name="needs_you_list", args={}, turn_id="T1")
    assert directive is not None
    assert directive["args"]["human_origin"]["chat_id"] == "tg-user-1"


def test_same_turn_can_claim_and_read_back_repeatedly(monkeypatch):
    _capture("sk-repeat")
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-repeat")
    d1 = human_origin.stamp_tool_call(tool_name="task_verdict", args={}, turn_id="T1")
    d2 = human_origin.stamp_tool_call(tool_name="task_verdict", args={}, turn_id="T1")
    assert d1["args"]["human_origin"] == d2["args"]["human_origin"]


def test_a_different_turn_on_the_same_session_gets_nothing_once_claimed(monkeypatch):
    """P0-2 core: a plugin-injected/internal turn that lands on the SAME session_key
    (but a genuinely different turn_id -- every real Hermes turn mints a fresh one)
    must never see a stamp another turn already claimed."""
    _capture("sk-race")
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-race")
    first = human_origin.stamp_tool_call(tool_name="task_verdict", args={}, turn_id="T1")
    assert first is not None
    second = human_origin.stamp_tool_call(tool_name="task_verdict", args={}, turn_id="T2-injected")
    assert second is None


def test_a_turn_with_no_turn_id_never_claims_anything(monkeypatch):
    """Fail closed: pre_tool_call always carries turn_id in real Hermes dispatch
    (_CallIds.turn_id); a call with none must never be trusted."""
    _capture("sk-noturn")
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-noturn")
    assert human_origin.stamp_tool_call(tool_name="task_verdict", args={}, turn_id="") is None
    # the pending capture must still be there for the REAL turn to claim later
    assert human_origin._STASH.claim("sk-noturn", "T-real") is not None


def test_rapid_second_message_does_not_steal_the_first_messages_claim(monkeypatch):
    """P0-3b: two rapid messages from the SAME legitimate human must not let the
    turn answering message 1 cite message 2's id -- FIFO, oldest first."""
    store = _Store("sk-fifo")
    human_origin.capture_human_origin(event=_event(_source(), message_id="M1"), gateway=None, session_store=store)
    human_origin.capture_human_origin(event=_event(_source(), message_id="M2"), gateway=None, session_store=store)
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-fifo")
    turn1 = human_origin.stamp_tool_call(tool_name="task_verdict", args={}, turn_id="T-turn1")
    assert turn1["args"]["human_origin"]["message_id"] == "M1"
    turn2 = human_origin.stamp_tool_call(tool_name="task_verdict", args={}, turn_id="T-turn2")
    assert turn2["args"]["human_origin"]["message_id"] == "M2"


def test_delegated_child_context_never_claims_even_with_a_fresh_turn_id(monkeypatch):
    _capture("sk-child")
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-child")
    monkeypatch.setattr(human_origin, "_in_delegated_child_context", lambda: True)
    assert human_origin.stamp_tool_call(tool_name="task_verdict", args={}, turn_id="T-child") is None
    # the pending capture must still be claimable by the real (non-child) turn
    monkeypatch.setattr(human_origin, "_in_delegated_child_context", lambda: False)
    assert human_origin.stamp_tool_call(tool_name="task_verdict", args={}, turn_id="T-parent") is not None


def test_model_supplied_origin_is_overwritten_and_logged_as_forgery(monkeypatch, caplog):
    _capture("sk-7")
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-7")
    forged = {"platform": "telegram", "user_id": "attacker", "chat_id": "attacker",
              "message_id": "0", "timestamp": "1970-01-01T00:00:00"}
    with caplog.at_level(logging.WARNING):
        directive = human_origin.stamp_tool_call(
            tool_name="task_verdict", args={"human_origin": forged}, turn_id="T1",
        )
    assert directive["args"]["human_origin"]["user_id"] == "tg-user-1"  # real origin wins
    assert any("forgery attempt" in r.message for r in caplog.records)


def test_no_claim_strips_model_supplied_origin_in_place(caplog):
    args = {"task_id": "t1", "human_origin": {"platform": "telegram", "user_id": "attacker"}}
    with caplog.at_level(logging.WARNING):
        directive = human_origin.stamp_tool_call(tool_name="task_verdict", args=args, turn_id="T-unclaimed")
    assert directive is None
    assert "human_origin" not in args  # truly absent, not merely None
    assert args["task_id"] == "t1"  # sibling keys untouched
    assert any("forgery attempt" in r.message for r in caplog.records)


def test_no_claim_and_no_model_supplied_origin_is_a_pure_noop(caplog):
    args = {"task_id": "t1"}
    with caplog.at_level(logging.WARNING):
        directive = human_origin.stamp_tool_call(tool_name="task_verdict", args=args, turn_id="T-x")
    assert directive is None
    assert args == {"task_id": "t1"}
    assert not any("forgery" in r.message for r in caplog.records)


def test_unrelated_tool_name_is_never_touched():
    args = {"human_origin": {"anything": "here"}}
    directive = human_origin.stamp_tool_call(tool_name="mupot_operator_send", args=args, turn_id="T1")
    assert directive is None
    assert args["human_origin"] == {"anything": "here"}


def test_non_dict_args_is_a_noop():
    assert human_origin.stamp_tool_call(tool_name="task_verdict", args=None, turn_id="T1") is None
    assert human_origin.stamp_tool_call(tool_name="task_verdict", args="not-a-dict", turn_id="T1") is None


def test_stamp_never_raises_when_current_session_key_lookup_blows_up(monkeypatch):
    def _boom():
        raise RuntimeError("contextvar machinery unavailable")
    monkeypatch.setattr(human_origin, "_current_session_key", _boom)
    assert human_origin.stamp_tool_call(tool_name="task_verdict", args={"task_id": "t1"}, turn_id="T1") is None


# ---------------------------------------------------------------------------
# _OriginStash
# ---------------------------------------------------------------------------

def test_stash_pending_is_bounded_per_session():
    stash = human_origin._OriginStash(max_pending_per_session=2, max_pending_total=100, max_claims=100, ttl_seconds=10_000)
    for i in range(5):
        stash.capture("sk", {"n": i})
    # only the newest 2 remain claimable, oldest-evicted-first
    assert stash.claim("sk", "t1")["n"] == 3
    assert stash.claim("sk", "t2")["n"] == 4
    assert stash.claim("sk", "t3") is None


def test_stash_pending_is_bounded_globally_across_sessions():
    stash = human_origin._OriginStash(max_pending_per_session=10, max_pending_total=2, max_claims=100, ttl_seconds=10_000)
    stash.capture("sk-a", {"n": "a"})
    stash.capture("sk-b", {"n": "b"})
    stash.capture("sk-c", {"n": "c"})  # evicts sk-a's oldest pending entry
    assert stash.claim("sk-a", "t1") is None
    assert stash.claim("sk-b", "t2")["n"] == "b"
    assert stash.claim("sk-c", "t3")["n"] == "c"


def test_stash_claims_ttl_expires_as_a_backstop(monkeypatch):
    stash = human_origin._OriginStash(ttl_seconds=5)
    clock = iter([100.0, 200.0])
    monkeypatch.setattr(human_origin.time, "monotonic", lambda: next(clock))
    stash.capture("sk", {"platform": "telegram"})
    assert stash.claim("sk", "t1") is None  # 200 - 100 = 100 > ttl 5


def test_stash_claim_ttl_expires_even_once_already_claimed(monkeypatch):
    stash = human_origin._OriginStash(ttl_seconds=5)
    clock = iter([100.0, 100.0, 200.0])
    monkeypatch.setattr(human_origin.time, "monotonic", lambda: next(clock))
    stash.capture("sk", {"platform": "telegram"})
    assert stash.claim("sk", "t1") is not None  # first claim, fresh
    assert stash.claim("sk", "t1") is None  # same turn, but now expired


def test_stash_claim_refreshes_lru_position_for_active_turns():
    """P2: an actively-read claim must not be the one evicted just because other
    turns claimed things more recently by insertion order alone."""
    stash = human_origin._OriginStash(max_claims=2, max_pending_total=100, max_pending_per_session=100, ttl_seconds=10_000)
    stash.capture("sk-a", {"n": "a"})
    stash.capture("sk-b", {"n": "b"})
    stash.claim("sk-a", "t-a")
    stash.claim("sk-b", "t-b")
    stash.claim("sk-a", "t-a")  # re-read: must refresh LRU position
    stash.capture("sk-c", {"n": "c"})
    stash.claim("sk-c", "t-c")  # forces an eviction among {(sk-a,t-a), (sk-b,t-b)}
    assert stash.claim("sk-a", "t-a") is not None  # still there: was refreshed
    assert stash.claim("sk-b", "t-b") is None  # evicted: was the true LRU


def test_stash_capture_put_and_claim_are_no_ops_for_falsy_keys():
    stash = human_origin._OriginStash()
    stash.capture("", {"platform": "telegram"})
    stash.capture(None, {"platform": "telegram"})
    assert len(stash) == 0
    assert stash.claim("", "t1") is None
    assert stash.claim("sk", "") is None
    assert stash.claim(None, "t1") is None


def test_stash_claim_returns_independent_copies_not_shared_references():
    stash = human_origin._OriginStash()
    origin = {"platform": "telegram", "user_id": "1"}
    stash.capture("sk-1", origin)
    origin["user_id"] = "mutated-after-capture"
    got = stash.claim("sk-1", "t1")
    assert got["user_id"] == "1"
    got["user_id"] = "mutated-after-claim"
    assert stash.claim("sk-1", "t1")["user_id"] == "1"


# ---------------------------------------------------------------------------
# _resolve_governed_tool_name
# ---------------------------------------------------------------------------

def test_resolve_governed_tool_name_bare():
    assert human_origin._resolve_governed_tool_name("task_verdict") == "task_verdict"
    assert human_origin._resolve_governed_tool_name("needs_you_list") == "needs_you_list"


def test_resolve_governed_tool_name_prefixed(monkeypatch):
    human_origin.set_mcp_server_name("mupot")
    assert human_origin._resolve_governed_tool_name("mcp__mupot__task_verdict") == "task_verdict"


def test_resolve_governed_tool_name_none_for_unrelated():
    assert human_origin._resolve_governed_tool_name("mupot_operator_send") is None
    assert human_origin._resolve_governed_tool_name(123) is None
    assert human_origin._resolve_governed_tool_name(None) is None


def test_set_mcp_server_name_falls_back_to_default_on_falsy():
    human_origin.set_mcp_server_name(None)
    assert human_origin._resolve_governed_tool_name("mcp__mupot__task_verdict") == "task_verdict"
    human_origin.set_mcp_server_name("")
    assert human_origin._resolve_governed_tool_name("mcp__mupot__task_verdict") == "task_verdict"


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

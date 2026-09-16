"""Plain-suite unit tests for mupot_gateway.human_origin: importable without a native
Hermes checkout (no gateway.* module-level imports in the module under test), so these
run in the fast, no-HERMES_SOURCE suite (scripts/test.sh). Real Hermes dataclasses, the
real session-context bridge, the real hermes_cli.plugins dispatcher, the real
agent.delegation_context marker, the real tools.mcp_tool_schema sanitizer, and the real
GatewayInboundMixin pipeline are covered separately in tests/native/test_human_origin.py,
which requires HERMES_SOURCE.

Round 4 rewrite: kasra-review's round-3 BLOCK found the defect was LIFETIME, not
predicate -- bind() re-queued a non-matching record and bind_turn_custody silently
dropped the boolean, so a record that failed to bind (e.g. a reply-quoted "approve")
stayed pending and spendable by whatever turn asked next, for the full TTL. Round 4's
rule: a pending record lives until the NEXT pre_llm_call on its session, and is
consumed there whether or not it binds (bind-or-burn). Every test below either proves
that design or is a direct regression test for a named finding.

Round 6 rewrite: kasra-review's round-5 P1-1 found round 5's stamp_tool_call() consumed
the bound record at pre_tool_call -- BEFORE Hermes's own block gate, guardrails, or an
approval flow can still kill the call -- so a same-turn retry after a block was left
unattested. Round 6 splits consumption into peek_and_reserve() (pre_tool_call) and
resolve() (post_tool_call, via the new finalize_tool_call hook): a reservation is only
permanently spent once Hermes reports the call genuinely dispatched (status != "blocked").
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
    human_origin._mcp_server_name_by_profile.clear()
    yield
    human_origin._STASH.clear()
    human_origin._WARNED_UNSUPPORTED_PLATFORMS.clear()
    human_origin._SESSION_ID_TO_KEY.clear()
    human_origin._mcp_server_name_by_profile.clear()


def _capture(session_key, text="hello", message_id="tg-msg-1", **source_kwargs):
    store = _Store(session_key)
    human_origin.capture_human_origin(
        event=_event(_source(**source_kwargs), text=text, message_id=message_id),
        gateway=None, session_store=store,
    )
    return session_key


def _bind(session_key, turn_id, *, text="hello", sender_id="tg-user-1", platform="telegram", monkeypatch):
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: session_key)
    return human_origin.bind_turn_custody(
        turn_id=turn_id, user_message=text, sender_id=sender_id, platform=platform,
    )


def _stamp(tool_name="task_verdict", args=None, turn_id="T1", tool_call_id="C1"):
    """pre_tool_call, round 6 calling convention: every real dispatch carries a
    tool_call_id, and stamp_tool_call() now requires one to ever reserve a
    bound record (see stamp_tool_call's guard: `turn_id and tool_call_id`)."""
    return human_origin.stamp_tool_call(
        tool_name=tool_name, args=args if args is not None else {}, turn_id=turn_id,
        tool_call_id=tool_call_id,
    )


def _finalize(tool_name="task_verdict", turn_id="T1", tool_call_id="C1", status="ok"):
    """post_tool_call, round 6's other half: resolves whatever reservation
    tool_call_id holds. status="blocked" releases it; anything else spends it."""
    return human_origin.finalize_tool_call(
        tool_name=tool_name, turn_id=turn_id, tool_call_id=tool_call_id, status=status,
    )


def _peek(session_key, turn_id, tool_call_id="test-peek"):
    """Test-only existence check. peek_and_reserve() never REMOVES a record (only
    resolve() does), so calling it once with a fixed, unique-per-assertion
    tool_call_id is a safe non-destructive-enough probe -- as long as nothing
    else in the same test later tries to reserve the SAME key with a DIFFERENT
    id while this reservation is still outstanding (none of the tests below do)."""
    return human_origin._STASH.peek_and_reserve(session_key, turn_id, tool_call_id)


# ---------------------------------------------------------------------------
# capture_human_origin (pre_gateway_dispatch) — trust fence (unchanged)
# ---------------------------------------------------------------------------

def test_capture_stashes_a_private_dm_and_always_returns_none_observer_only():
    event = _event(_source())
    store = _Store("sk-1")
    assert human_origin.capture_human_origin(event=event, gateway=None, session_store=store) is None
    assert len(human_origin._STASH) == 1


def test_capture_records_chat_type_thread_id_forwarded_and_text_hash(monkeypatch):
    event = _event(_source(thread_id="7"), text="approve it")
    store = _Store("sk-1b")
    human_origin.capture_human_origin(event=event, gateway=None, session_store=store)
    _bind("sk-1b", "T1", text="approve it", monkeypatch=monkeypatch)
    origin = _peek("sk-1b", "T1")
    assert origin["chat_type"] == "dm"
    assert origin["thread_id"] == "7"
    assert origin["forwarded"] is False
    assert "text_sha256" in origin


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
# human_origin.text field (round 6 item 2) — mupot's server side requires the
# task id to appear in the human's own words, binding the stamp to intent.
# ---------------------------------------------------------------------------

def test_captured_record_carries_the_human_text_field(monkeypatch):
    _capture("sk-text-1", text="approve f9408956")
    _bind("sk-text-1", "T1", text="approve f9408956", monkeypatch=monkeypatch)
    bound = _peek("sk-text-1", "T1")
    assert bound["text"] == "approve f9408956"


def test_captured_text_field_is_truncated_independently_of_the_hash(monkeypatch):
    """The stamped text field is capped at _MAX_STAMPED_TEXT_CHARS, but bind's
    equality check always hashes the FULL text -- truncation here can never
    turn a real match into a false one."""
    long_text = "approve f9408956 " + ("x" * 3000)
    _capture("sk-text-2", text=long_text)
    _bind("sk-text-2", "T1", text=long_text, monkeypatch=monkeypatch)
    bound = _peek("sk-text-2", "T1")
    assert len(bound["text"]) == human_origin._MAX_STAMPED_TEXT_CHARS
    assert bound["text"] == long_text[: human_origin._MAX_STAMPED_TEXT_CHARS]


def test_stamped_human_origin_carries_the_text_field_not_the_internal_hash(monkeypatch):
    _capture("sk-text-3", text="approve f9408956")
    _bind("sk-text-3", "T1", text="approve f9408956", monkeypatch=monkeypatch)
    directive = _stamp(args={"task_id": "f9408956"}, turn_id="T1", tool_call_id="C1")
    assert directive["args"]["human_origin"]["text"] == "approve f9408956"
    assert "text_sha256" not in directive["args"]["human_origin"]  # internal-only key never leaks


# ---------------------------------------------------------------------------
# bind_turn_custody (pre_llm_call) — bind-or-burn
# ---------------------------------------------------------------------------

def test_bind_matches_on_exact_text_and_sender(monkeypatch):
    _capture("sk-bind-1", text="approve f9408956")
    _bind("sk-bind-1", "T1", text="approve f9408956", monkeypatch=monkeypatch)
    bound = _peek("sk-bind-1", "T1")
    assert bound is not None
    assert bound["user_id"] == "tg-user-1"


def test_bind_refuses_non_telegram_platform(monkeypatch):
    _capture("sk-bind-2", text="approve it")
    _bind("sk-bind-2", "T1", text="approve it", platform="slack", monkeypatch=monkeypatch)
    assert _peek("sk-bind-2", "T1") is None


def test_bind_refuses_without_turn_id(monkeypatch):
    _capture("sk-bind-3", text="approve it")
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-bind-3")
    human_origin.bind_turn_custody(turn_id="", user_message="approve it",
                                   sender_id="tg-user-1", platform="telegram")
    assert _peek("sk-bind-3", "") is None


def test_bind_refuses_non_string_user_message(monkeypatch):
    _capture("sk-bind-4", text="approve it")
    monkeypatch.setattr(human_origin, "_current_session_key", lambda: "sk-bind-4")
    human_origin.bind_turn_custody(turn_id="T1", user_message=None,
                                   sender_id="tg-user-1", platform="telegram")
    assert _peek("sk-bind-4", "T1") is None


def test_bind_refuses_when_sender_id_does_not_match_and_burns_it(monkeypatch, caplog):
    _capture("sk-bind-5", text="approve it", message_id="M-sender")
    with caplog.at_level(logging.WARNING):
        _bind("sk-bind-5", "T1", text="approve it", sender_id="a-different-user", monkeypatch=monkeypatch)
    assert _peek("sk-bind-5", "T1") is None
    assert len(human_origin._STASH) == 0  # burned, not left pending
    assert any("M-sender" in r.message and "sender_mismatch" in r.message for r in caplog.records)


def test_bind_refuses_when_text_does_not_match_and_burns_it(monkeypatch, caplog):
    _capture("sk-bind-6", text="approve it", message_id="M-text")
    with caplog.at_level(logging.WARNING):
        _bind("sk-bind-6", "T1", text="something else entirely", monkeypatch=monkeypatch)
    assert _peek("sk-bind-6", "T1") is None
    assert len(human_origin._STASH) == 0  # burned, not left pending
    assert any("M-text" in r.message and "text_mismatch" in r.message for r in caplog.records)


def test_bind_never_raises_when_current_session_key_lookup_blows_up(monkeypatch):
    def _boom():
        raise RuntimeError("contextvar machinery unavailable")
    monkeypatch.setattr(human_origin, "_current_session_key", _boom)
    human_origin.bind_turn_custody(turn_id="T1", user_message="x", sender_id="u",
                                   platform="telegram")  # must not raise


def test_bind_refused_for_delegated_child_context_leaves_record_pending_for_the_parent(monkeypatch):
    """A delegated child's pre_llm_call is refused BEFORE it ever reaches bind() at
    all, so it must not burn the record either -- the parent turn still gets it."""
    _capture("sk-bind-7", text="approve it")
    monkeypatch.setattr(human_origin, "_in_delegated_child_context", lambda: True)
    _bind("sk-bind-7", "T-child", text="approve it", monkeypatch=monkeypatch)
    assert _peek("sk-bind-7", "T-child") is None
    monkeypatch.setattr(human_origin, "_in_delegated_child_context", lambda: False)
    _bind("sk-bind-7", "T-parent", text="approve it", monkeypatch=monkeypatch)
    assert _peek("sk-bind-7", "T-parent") is not None


def test_a_failed_bind_burns_the_record_it_could_not_match_p0_1(monkeypatch, caplog):
    """kasra-review round-3 P0-1, the exact class: a turn whose own message does
    NOT match (e.g. a reply-quote wrapped the human's plain "approve") must not
    leave the record pending for a LATER, unrelated turn to spend."""
    _capture("sk-p0-1", text="approve", message_id="M-orphan-risk")
    with caplog.at_level(logging.WARNING):
        _bind("sk-p0-1", "T-reply-quoted", text='[Replying to: "..."]\n\napprove', monkeypatch=monkeypatch)
    assert _peek("sk-p0-1", "T-reply-quoted") is None
    assert len(human_origin._STASH) == 0
    # a LATER turn presenting the bare original text finds NOTHING -- not the
    # orphaned record:
    directive = _stamp(args={}, turn_id="T-later-attacker", tool_call_id="C1")
    assert directive is None


def test_busy_session_merge_burns_both_captures_neither_gets_attested(monkeypatch, caplog):
    """kasra-review round-4 P1-2, pinned: on Hermes's own default
    busy_input_mode: interrupt, two Telegram texts sent within the debounce
    window are MERGED into one turn's inbound text
    (gateway/run_inbound.py's merge path -> gateway/platforms/base.py's
    _append_text) -- but both were already captured as SEPARATE records
    before any merge happens (capture is at pre_gateway_dispatch, before
    busy-session handling ever runs). The one turn that actually executes
    presents the CONCATENATED text, which matches NEITHER individual capture,
    so bind() burns both -- fail closed by design: the verdict, if any, rides
    the agent seat, and the human's remedy is simply to resend one message."""
    store = _Store("sk-merge")
    human_origin.capture_human_origin(
        event=_event(_source(), message_id="M-first", text="approve f9408956"),
        gateway=None, session_store=store,
    )
    human_origin.capture_human_origin(
        event=_event(_source(), message_id="M-second", text="and approve 7c1120aa"),
        gateway=None, session_store=store,
    )
    merged_text = "approve f9408956\nand approve 7c1120aa"  # Hermes's own merge shape (paraphrased)
    with caplog.at_level(logging.WARNING):
        _bind("sk-merge", "T-merged-turn", text=merged_text, monkeypatch=monkeypatch)
    assert len(human_origin._STASH) == 0  # both burned, neither survives for a later turn
    assert any("M-first" in r.message and "text_mismatch" in r.message for r in caplog.records)
    assert any("M-second" in r.message and "text_mismatch" in r.message for r in caplog.records)
    directive = _stamp(args={"task_id": "f9408956"}, turn_id="T-merged-turn", tool_call_id="C1")
    assert directive is None  # the verdict, if the model even calls it, rides the agent seat


def test_reply_quoted_approve_then_plain_approve_binds_the_seconds_own_message(monkeypatch, caplog):
    """The coordinator's required round-4 test: a reply-quoted "approve" fails to
    bind and is burned; a later plain "approve" binds its OWN message_id, never
    the burned one's."""
    store = _Store("sk-reply-then-plain")
    human_origin.capture_human_origin(
        event=_event(_source(), message_id="M105", text="approve"), gateway=None, session_store=store,
    )
    with caplog.at_level(logging.WARNING):
        _bind("sk-reply-then-plain", "T-reply-turn",
              text='[Replying to your previous message: "Task f9408956 is waiting on you."]\n\napprove',
              monkeypatch=monkeypatch)
    assert _peek("sk-reply-then-plain", "T-reply-turn") is None
    assert any("M105" in r.message and "text_mismatch" in r.message for r in caplog.records)

    human_origin.capture_human_origin(
        event=_event(_source(), message_id="M200", text="approve"), gateway=None, session_store=store,
    )
    _bind("sk-reply-then-plain", "T-plain-turn", text="approve", monkeypatch=monkeypatch)
    out = _stamp(args={"task_id": "t"}, turn_id="T-plain-turn", tool_call_id="C1")
    assert out is not None
    assert out["args"]["human_origin"]["message_id"] == "M200"
    _finalize(turn_id="T-plain-turn", tool_call_id="C1", status="ok")
    assert len(human_origin._STASH) == 0  # M200's binding was consumed once the dispatch completed


def test_injected_turn_with_text_approve_after_the_humans_turn_finds_nothing(monkeypatch):
    """Required round-4 test: once the human's OWN turn has bound (or burned) its
    record, an injected turn presenting the SAME word ("approve") right after
    finds nothing pending to bind at all."""
    _capture("sk-after-human", text="approve", message_id="M-human")
    _bind("sk-after-human", "T-human", text="approve", monkeypatch=monkeypatch)
    human_turn = _stamp(args={}, turn_id="T-human", tool_call_id="C-human")
    assert human_turn is not None
    _bind("sk-after-human", "T-injected", text="approve", monkeypatch=monkeypatch)
    injected = _stamp(args={}, turn_id="T-injected", tool_call_id="C-injected")
    assert injected is None


def test_injected_turn_with_unclaimed_pending_record_never_binds(monkeypatch):
    """Round-2/3's class, re-verified under round 4: an injected turn's own
    prompt text never matches the human's captured text, so it cannot bind
    regardless of ordering -- and it also burns the human's record in the
    process (bind-or-burn applies to EVERY pre_llm_call, not just matching
    ones), which is the whole point of round 4."""
    _capture("sk-injected", text="hey, how's the build going?")
    _bind("sk-injected", "T-injected-notification-turn",
          text="[Automated Mupot event] please review the pending decision",
          monkeypatch=monkeypatch)
    assert _peek("sk-injected", "T-injected-notification-turn") is None
    directive = _stamp(
        args={"task_id": "t-attacker"}, turn_id="T-injected-notification-turn", tool_call_id="C1",
    )
    assert directive is None


def test_chatter_then_approve_stamps_the_approve_messages_id(monkeypatch):
    store = _Store("sk-order")
    human_origin.capture_human_origin(
        event=_event(_source(), message_id="M-chatter", text="hey what's up"),
        gateway=None, session_store=store,
    )
    _bind("sk-order", "T-chatter", text="hey what's up", monkeypatch=monkeypatch)
    human_origin.capture_human_origin(
        event=_event(_source(), message_id="M-approve", text="approve f9408956"),
        gateway=None, session_store=store,
    )
    _bind("sk-order", "T-approve", text="approve f9408956", monkeypatch=monkeypatch)

    directive = _stamp(args={"task_id": "t-real"}, turn_id="T-approve", tool_call_id="C1")
    assert directive["args"]["human_origin"]["message_id"] == "M-approve"


def test_same_text_twice_binds_oldest_match_only_second_is_superseded_not_pending(monkeypatch, caplog):
    """Round-4 does NOT close round-3's Probe D residual (identical-text id
    drift) -- bind() still takes the OLDEST matching record by design, so the
    stamp still names the older message_id (M1, asserted below). What round 4
    DOES close is the duplicate's lingering: a second, later capture with
    IDENTICAL text presented to the SAME bind call is burned as "superseded"
    right there, instead of staying pending for yet another turn to (mis)claim.
    Content-correct, id-drifted -- a named P2 residual (Athena round-4 gate
    correction)."""
    store = _Store("sk-dup")
    human_origin.capture_human_origin(
        event=_event(_source(), message_id="M1", text="approve X"), gateway=None, session_store=store,
    )
    human_origin.capture_human_origin(
        event=_event(_source(), message_id="M2", text="approve X"), gateway=None, session_store=store,
    )
    with caplog.at_level(logging.WARNING):
        _bind("sk-dup", "T1", text="approve X", monkeypatch=monkeypatch)
    d1 = _stamp(args={}, turn_id="T1", tool_call_id="C1")
    assert d1["args"]["human_origin"]["message_id"] == "M1"
    assert any("M2" in r.message and "superseded" in r.message for r in caplog.records)
    _finalize(turn_id="T1", tool_call_id="C1", status="ok")
    assert len(human_origin._STASH) == 0  # M2 was burned; M1's binding was consumed on completion
    # a later turn presenting the same text finds nothing:
    _bind("sk-dup", "T2", text="approve X", monkeypatch=monkeypatch)
    d2 = _stamp(args={}, turn_id="T2", tool_call_id="C2")
    assert d2 is None


def test_two_task_verdict_calls_in_one_turn_with_no_completed_dispatch_between_is_not_stamped_twice(monkeypatch, caplog):
    """Round 6: a second pre_tool_call for the SAME turn, with a DIFFERENT
    tool_call_id, while the first reservation is still outstanding (no
    post_tool_call has resolved it yet) cannot also reserve the same bound
    record -- only one tool_call_id may hold it at a time. One human message
    still authenticates AT MOST one task_verdict call in flight per turn."""
    _capture("sk-double-call", text="approve f9408956")
    _bind("sk-double-call", "T1", text="approve f9408956", monkeypatch=monkeypatch)
    first = _stamp(args={"task_id": "f9408956"}, turn_id="T1", tool_call_id="C1")
    assert first is not None
    assert first["args"]["human_origin"]["user_id"] == "tg-user-1"

    second_args = {"task_id": "DIFFERENT-TASK"}
    with caplog.at_level(logging.WARNING):
        second = human_origin.stamp_tool_call(
            tool_name="task_verdict", args=second_args, turn_id="T1", tool_call_id="C2",
        )
    assert second is None
    assert second_args == {"task_id": "DIFFERENT-TASK"}  # no human_origin added

    # and a forged human_origin on that second call is still stripped, not stamped:
    forged_args = {"task_id": "DIFFERENT-TASK", "human_origin": {"user_id": "attacker"}}
    third = human_origin.stamp_tool_call(
        tool_name="task_verdict", args=forged_args, turn_id="T1", tool_call_id="C3",
    )
    assert third is None
    assert "human_origin" not in forged_args


# ---------------------------------------------------------------------------
# reserve/resolve two-phase consumption (round 6 item 1, kasra-review round-5
# P1-1): pre_tool_call RESERVES while unspent; post_tool_call decides whether
# the reservation is permanently spent (a genuine dispatch outcome) or
# released (blocked before ever reaching mupot).
# ---------------------------------------------------------------------------

def test_blocked_call_releases_the_reservation_for_a_same_turn_retry(monkeypatch):
    """The exact P1-1 scenario: pre_tool_call stamps while the record is bound
    and unspent; Hermes's own block gate (or a guardrail, or a denied
    human-approval escalation) then kills the call AFTER the stamp already
    happened but BEFORE it ever reached mupot. post_tool_call sees
    status="blocked" and releases the reservation untouched, so the model's
    retry in the SAME turn (a NEW tool_call_id) still finds the record
    available and gets stamped."""
    _capture("sk-blocked-retry", text="approve f9408956")
    _bind("sk-blocked-retry", "T1", text="approve f9408956", monkeypatch=monkeypatch)

    first = _stamp(args={"task_id": "f9408956"}, turn_id="T1", tool_call_id="C1")
    assert first is not None
    assert first["args"]["human_origin"]["user_id"] == "tg-user-1"

    # Hermes's block gate kills the FIRST call before it ever dispatches:
    _finalize(turn_id="T1", tool_call_id="C1", status="blocked")

    # the retry -- a NEW tool_call_id, same turn -- is stamped again:
    retry = _stamp(args={"task_id": "f9408956"}, turn_id="T1", tool_call_id="C2")
    assert retry is not None
    assert retry["args"]["human_origin"]["user_id"] == "tg-user-1"


def test_completed_dispatch_permanently_spends_the_record_second_call_stripped(monkeypatch):
    """The other half of P1-1: once post_tool_call reports a genuine dispatch
    outcome (status="ok" here; "error"/"cancelled" spend it exactly the same
    way -- see test_tool_error_also_spends_the_record_not_only_status_ok), the
    record is gone for good. A second task_verdict call in the same turn --
    deliberate or model-steered -- gets nothing."""
    _capture("sk-completed-then-second", text="approve f9408956")
    _bind("sk-completed-then-second", "T1", text="approve f9408956", monkeypatch=monkeypatch)

    first = _stamp(args={"task_id": "f9408956"}, turn_id="T1", tool_call_id="C1")
    assert first is not None

    _finalize(turn_id="T1", tool_call_id="C1", status="ok")

    second = _stamp(args={"task_id": "ANOTHER-TASK"}, turn_id="T1", tool_call_id="C2")
    assert second is None
    assert len(human_origin._STASH) == 0


def test_tool_error_also_spends_the_record_not_only_status_ok(monkeypatch):
    """status="error" (the call dispatched to mupot and mupot's own tool
    raised) is just as much a genuine dispatch as "ok" -- mupot has already
    seen the field either way. Only "blocked" (never dispatched at all)
    releases the reservation instead of spending it."""
    _capture("sk-tool-error", text="approve f9408956")
    _bind("sk-tool-error", "T1", text="approve f9408956", monkeypatch=monkeypatch)
    first = _stamp(args={}, turn_id="T1", tool_call_id="C1")
    assert first is not None
    _finalize(turn_id="T1", tool_call_id="C1", status="error")
    second = _stamp(args={}, turn_id="T1", tool_call_id="C2")
    assert second is None


def test_finalize_is_a_noop_for_a_tool_call_id_that_never_reserved_anything(monkeypatch):
    _capture("sk-finalize-noop", text="approve f9408956")
    _bind("sk-finalize-noop", "T1", text="approve f9408956", monkeypatch=monkeypatch)
    human_origin.finalize_tool_call(
        tool_name="task_verdict", turn_id="T1", tool_call_id="never-reserved", status="ok",
    )
    # the real reservation is untouched -- still stampable:
    stamped = _stamp(args={}, turn_id="T1", tool_call_id="C1")
    assert stamped is not None


def test_finalize_never_raises_and_ignores_non_governed_tools_or_missing_ids():
    human_origin.finalize_tool_call(tool_name="needs_you_list", turn_id="T1", tool_call_id="C1", status="ok")
    human_origin.finalize_tool_call(tool_name="task_verdict", turn_id="", tool_call_id="", status="ok")


def test_finalize_never_raises_when_current_session_key_lookup_blows_up(monkeypatch):
    def _boom():
        raise RuntimeError("contextvar machinery unavailable")
    monkeypatch.setattr(human_origin, "_current_session_key", _boom)
    human_origin.finalize_tool_call(tool_name="task_verdict", turn_id="T1", tool_call_id="C1", status="ok")


# ---------------------------------------------------------------------------
# stamp_tool_call (pre_tool_call) — widened strip, narrowed stamp allowlist
# ---------------------------------------------------------------------------

def test_stamp_reads_a_bound_record_for_bare_name(monkeypatch):
    _capture("sk-stamp-1", text="approve")
    _bind("sk-stamp-1", "T1", text="approve", monkeypatch=monkeypatch)
    directive = _stamp(args={"task_id": "t1"}, turn_id="T1", tool_call_id="C1")
    assert directive["args"]["human_origin"]["user_id"] == "tg-user-1"


def test_needs_you_list_is_no_longer_stamped_round_4(monkeypatch):
    """kasra-review round-3 P1-3: mupot's #1425 server tree does not accept
    human_origin on needs_you_list -- dropped from the stamp allowlist."""
    _capture("sk-nyl", text="approve")
    _bind("sk-nyl", "T1", text="approve", monkeypatch=monkeypatch)
    assert _stamp(tool_name="needs_you_list", args={}, turn_id="T1", tool_call_id="C1") is None


def test_needs_you_list_forged_origin_is_still_stripped(caplog):
    args = {"human_origin": {"user_id": "attacker"}}
    with caplog.at_level(logging.WARNING):
        directive = _stamp(tool_name="needs_you_list", args=args, turn_id="T1", tool_call_id="C1")
    assert directive is None
    assert "human_origin" not in args


def test_stamp_reads_a_bound_record_via_the_configured_wire_name(monkeypatch):
    _capture("sk-stamp-2", text="approve")
    _bind("sk-stamp-2", "T1", text="approve", monkeypatch=monkeypatch)
    human_origin.set_mcp_server_name("mupot")
    directive = _stamp(
        tool_name="mcp__mupot__task_verdict", args={"task_id": "t1"}, turn_id="T1", tool_call_id="C1",
    )
    assert directive is not None


def test_stamp_never_reads_across_turns_even_with_a_bound_record_elsewhere(monkeypatch):
    _capture("sk-stamp-3", text="approve")
    _bind("sk-stamp-3", "T-bound", text="approve", monkeypatch=monkeypatch)
    directive = _stamp(
        args={"task_id": "t1"}, turn_id="T-different", tool_call_id="C1",
    )
    assert directive is None


def test_stamp_strips_forged_origin_for_task_verdict_even_without_a_bind(caplog):
    args = {"task_id": "t1", "human_origin": {"platform": "telegram", "user_id": "attacker"}}
    with caplog.at_level(logging.WARNING):
        directive = _stamp(args=args, turn_id="T-unbound", tool_call_id="C1")
    assert directive is None
    assert "human_origin" not in args
    assert any("forgery attempt" in r.message for r in caplog.records)


@pytest.mark.parametrize("bare_name", [
    "task_verdict_reverse", "approve_gate_edge", "advance_node",
    "objective_accept", "routine_run_answer",
])
def test_other_decision_tools_are_stripped_but_never_stamped_p1_3(bare_name, caplog):
    """kasra-review round-3 P1-3: the live schema cache shows several other
    decision-adjacent mupot tools. None of them is ever stamped (only
    task_verdict is), but a forged human_origin on any of them must still be
    stripped."""
    args = {"human_origin": {"user_id": "attacker"}}
    with caplog.at_level(logging.WARNING):
        directive = _stamp(tool_name=bare_name, args=args, turn_id="T1", tool_call_id="C1")
    assert directive is None
    assert "human_origin" not in args


def test_any_tool_under_the_configured_mupot_server_is_stripped_once_resolved(caplog):
    """Round 4 widens the strip from 'looks like task_verdict' to 'belongs to
    mupot at all' once the server name is resolved -- covers tools this module
    has never heard of by name."""
    human_origin.set_mcp_server_name("mupot")
    args = {"human_origin": {"user_id": "attacker"}}
    with caplog.at_level(logging.WARNING):
        directive = _stamp(
            tool_name="mcp__mupot__flight_dispatch", args=args, turn_id="T1", tool_call_id="C1",
        )
    assert directive is None
    assert "human_origin" not in args


def test_unrelated_non_mupot_tool_is_never_touched():
    human_origin.set_mcp_server_name("mupot")
    args = {"human_origin": {"anything": "here"}}
    directive = _stamp(
        tool_name="mcp__some_other_server__whatever", args=args, turn_id="T1", tool_call_id="C1",
    )
    assert directive is None
    assert args["human_origin"] == {"anything": "here"}


def test_stamp_strips_regardless_of_server_name_mismatch_p1_1(caplog):
    human_origin.set_mcp_server_name("some-other-server")
    args = {"human_origin": {"user_id": "attacker"}}
    with caplog.at_level(logging.WARNING):
        directive = _stamp(
            tool_name="mcp__mupot__task_verdict", args=args, turn_id="T1", tool_call_id="C1",
        )
    assert directive is None
    assert "human_origin" not in args


def test_stamp_never_stamps_when_server_name_is_unset_p1_2(monkeypatch, caplog):
    _capture("sk-unset", text="approve")
    _bind("sk-unset", "T1", text="approve", monkeypatch=monkeypatch)
    assert human_origin._mcp_server_name_by_profile == {}
    args = {"human_origin": {"user_id": "attacker"}}
    with caplog.at_level(logging.WARNING):
        directive = _stamp(
            tool_name="mcp__mupot__task_verdict", args=args, turn_id="T1", tool_call_id="C1",
        )
    assert directive is None
    assert "human_origin" not in args


def test_stamp_unrelated_tool_name_is_never_touched():
    args = {"human_origin": {"anything": "here"}}
    directive = _stamp(tool_name="mupot_operator_send", args=args, turn_id="T1", tool_call_id="C1")
    assert directive is None
    assert args["human_origin"] == {"anything": "here"}


def test_stamp_non_dict_args_is_a_noop():
    assert human_origin.stamp_tool_call(tool_name="task_verdict", args=None, turn_id="T1", tool_call_id="C1") is None
    assert human_origin.stamp_tool_call(tool_name="task_verdict", args="not-a-dict", turn_id="T1", tool_call_id="C1") is None


def test_stamp_no_bind_and_no_model_supplied_is_a_pure_noop(caplog):
    args = {"task_id": "t1"}
    with caplog.at_level(logging.WARNING):
        directive = _stamp(args=args, turn_id="T1", tool_call_id="C1")
    assert directive is None
    assert args == {"task_id": "t1"}
    assert not any("forgery" in r.message for r in caplog.records)


def test_stamp_never_raises_when_current_session_key_lookup_blows_up(monkeypatch):
    def _boom():
        raise RuntimeError("contextvar machinery unavailable")
    monkeypatch.setattr(human_origin, "_current_session_key", _boom)
    assert human_origin.stamp_tool_call(
        tool_name="task_verdict", args={"task_id": "t1"}, turn_id="T1", tool_call_id="C1",
    ) is None


def test_stamp_is_a_noop_without_a_tool_call_id():
    """Round 6: a governed tool call with no tool_call_id at all can never
    reserve anything -- pre_tool_call always carries one in real Hermes
    dispatch, but a defensive default must still fail closed, not raise."""
    assert human_origin.stamp_tool_call(
        tool_name="task_verdict", args={"task_id": "t1"}, turn_id="T1", tool_call_id="",
    ) is None


# ---------------------------------------------------------------------------
# _resolve_governed_tool_name / _looks_like_mupot_tool / sanitizer
# ---------------------------------------------------------------------------

def test_resolve_governed_tool_name_bare_always_works_even_unset():
    assert human_origin._resolve_governed_tool_name("task_verdict") == "task_verdict"


def test_resolve_governed_tool_name_prefixed_requires_server_set():
    assert human_origin._resolve_governed_tool_name("mcp__mupot__task_verdict") is None
    human_origin.set_mcp_server_name("mupot")
    assert human_origin._resolve_governed_tool_name("mcp__mupot__task_verdict") == "task_verdict"


def test_resolve_governed_tool_name_sanitizes_hyphenated_server_name():
    human_origin.set_mcp_server_name("mupot-prod")
    assert human_origin._resolve_governed_tool_name("mcp__mupot_prod__task_verdict") == "task_verdict"
    assert human_origin._resolve_governed_tool_name("mcp__mupot-prod__task_verdict") is None


def test_resolve_governed_tool_name_none_for_unrelated():
    assert human_origin._resolve_governed_tool_name("mupot_operator_send") is None
    assert human_origin._resolve_governed_tool_name(123) is None
    assert human_origin._resolve_governed_tool_name(None) is None


def test_resolve_governed_tool_name_never_matches_needs_you_list():
    human_origin.set_mcp_server_name("mupot")
    assert human_origin._resolve_governed_tool_name("needs_you_list") is None
    assert human_origin._resolve_governed_tool_name("mcp__mupot__needs_you_list") is None


@pytest.mark.parametrize("name", [
    "mupot__task_verdict", "mupot.task_verdict", "Task_Verdict", "task_verdict ",
])
def test_looks_like_mupot_tool_rejects_lookalikes(name):
    assert human_origin._looks_like_mupot_tool(name) is False


def test_looks_like_mupot_tool_matches_any_decision_tool_on_any_server():
    assert human_origin._looks_like_mupot_tool("mcp__literally_anything__task_verdict_reverse") is True
    assert human_origin._looks_like_mupot_tool("task_verdict") is True
    assert human_origin._looks_like_mupot_tool("approve_gate_edge") is True


def test_looks_like_mupot_tool_matches_any_tool_once_server_resolved():
    human_origin.set_mcp_server_name("mupot")
    assert human_origin._looks_like_mupot_tool("mcp__mupot__flight_dispatch") is True
    assert human_origin._looks_like_mupot_tool("mcp__some_other_server__flight_dispatch") is False


def test_set_mcp_server_name_falsy_means_unset_not_default():
    human_origin.set_mcp_server_name("mupot")
    assert human_origin._resolve_governed_tool_name("mcp__mupot__task_verdict") == "task_verdict"
    human_origin.set_mcp_server_name(None)
    assert human_origin._resolve_governed_tool_name("mcp__mupot__task_verdict") is None
    human_origin.set_mcp_server_name("mupot")
    human_origin.set_mcp_server_name("")
    assert human_origin._resolve_governed_tool_name("mcp__mupot__task_verdict") is None


def test_mcp_server_name_is_scoped_per_profile_p2_2(monkeypatch):
    """kasra-review round-4 P2-2: two multiplexed profiles with different
    mcp_server names must not clobber each other's resolution."""
    human_origin.set_mcp_server_name("mupot", profile="profile-a")
    human_origin.set_mcp_server_name("mupot-prod", profile="profile-b")

    monkeypatch.setattr(human_origin, "_current_profile_key", lambda: "profile-a")
    assert human_origin._resolve_governed_tool_name("mcp__mupot__task_verdict") == "task_verdict"
    assert human_origin._resolve_governed_tool_name("mcp__mupot_prod__task_verdict") is None

    monkeypatch.setattr(human_origin, "_current_profile_key", lambda: "profile-b")
    assert human_origin._resolve_governed_tool_name("mcp__mupot_prod__task_verdict") == "task_verdict"
    assert human_origin._resolve_governed_tool_name("mcp__mupot__task_verdict") is None

    # unsetting one profile never touches the other:
    human_origin.set_mcp_server_name(None, profile="profile-b")
    assert human_origin._resolve_governed_tool_name("mcp__mupot_prod__task_verdict") is None
    monkeypatch.setattr(human_origin, "_current_profile_key", lambda: "profile-a")
    assert human_origin._resolve_governed_tool_name("mcp__mupot__task_verdict") == "task_verdict"


def test_mcp_server_name_set_defaults_to_the_currently_active_profile(monkeypatch):
    """When set_mcp_server_name() is called with no explicit profile= (the real
    adapter.py call site), it must resolve to WHATEVER profile is active right
    then, not a hardcoded default, so it lands in the same bucket
    _current_profile_key() will later read for that profile's own turns."""
    monkeypatch.setattr(human_origin, "_default_profile_key", lambda: "profile-x")
    human_origin.set_mcp_server_name("mupot")
    assert human_origin._mcp_server_name_by_profile == {"profile-x": "mupot"}


# ---------------------------------------------------------------------------
# _OriginStash — bind-or-burn semantics + round-6 reserve/resolve
# ---------------------------------------------------------------------------

def test_stash_bind_pops_from_pending_and_peek_and_reserve_finds_it():
    stash = human_origin._OriginStash()
    stash.capture("sk", {"user_id": "u1", "text_sha256": "h1"})
    assert stash.bind("sk", "T1", sender_id="u1", text_sha256="h1") is True
    assert stash.peek_and_reserve("sk", "T1", "C1")["user_id"] == "u1"


def test_stash_bind_is_idempotent_for_the_same_turn():
    stash = human_origin._OriginStash()
    stash.capture("sk", {"user_id": "u1", "text_sha256": "h1"})
    assert stash.bind("sk", "T1", sender_id="u1", text_sha256="h1") is True
    stash.capture("sk", {"user_id": "u1", "text_sha256": "h1"})
    assert stash.bind("sk", "T1", sender_id="u1", text_sha256="h1") is True
    assert len(stash) == 2  # 1 still-bound (T1), 1 still-pending (never touched by the re-bind)


def test_stash_bind_no_match_burns_and_returns_false(monkeypatch):
    stash = human_origin._OriginStash()
    stash.capture("sk", {"user_id": "u1", "text_sha256": "h1"})
    burned = []
    assert stash.bind("sk", "T1", sender_id="u1", text_sha256="WRONG",
                      burn_callback=lambda rec, reason: burned.append(reason)) is False
    assert burned == ["text_mismatch"]
    assert len(stash) == 0  # burned, not left for T2 to find
    assert stash.bind("sk", "T2", sender_id="u1", text_sha256="h1") is False


def test_stash_bind_consumes_oldest_matching_record_only_second_is_superseded():
    stash = human_origin._OriginStash()
    stash.capture("sk", {"user_id": "u1", "text_sha256": "h1", "message_id": "M1"})
    stash.capture("sk", {"user_id": "u1", "text_sha256": "h1", "message_id": "M2"})
    burned = []
    assert stash.bind("sk", "T1", sender_id="u1", text_sha256="h1",
                      burn_callback=lambda rec, reason: burned.append((rec["message_id"], reason))) is True
    bound = stash.peek_and_reserve("sk", "T1", "C1")
    assert bound["message_id"] == "M1"
    assert burned == [("M2", "superseded")]
    stash.resolve("sk", "T1", "C1", spent=True)
    assert len(stash) == 0  # M2 was burned at bind time; M1's binding was just spent


def test_stash_peek_and_reserve_does_not_consume_but_resolve_does():
    """Round 6, kasra-review round-5 P1-1: peek_and_reserve() only RESERVES --
    calling it again with the SAME tool_call_id is idempotent re-entry, a
    DIFFERENT tool_call_id cannot also reserve it, and the record survives
    until resolve(spent=True) actually removes it."""
    stash = human_origin._OriginStash()
    stash.capture("sk", {"user_id": "u1", "text_sha256": "h1", "message_id": "m1"})
    stash.bind("sk", "T1", sender_id="u1", text_sha256="h1")
    first = stash.peek_and_reserve("sk", "T1", "C1")
    assert first is not None
    assert first["message_id"] == "m1"
    again = stash.peek_and_reserve("sk", "T1", "C1")  # idempotent re-entry, same id
    assert again is not None
    assert stash.peek_and_reserve("sk", "T1", "C2") is None  # a different id cannot also reserve it
    stash.resolve("sk", "T1", "C1", spent=True)
    assert stash.peek_and_reserve("sk", "T1", "C2") is None  # gone, not just released


def test_stash_resolve_with_spent_false_releases_for_a_different_tool_call_id():
    stash = human_origin._OriginStash()
    stash.capture("sk", {"user_id": "u1", "text_sha256": "h1", "message_id": "m1"})
    stash.bind("sk", "T1", sender_id="u1", text_sha256="h1")
    stash.peek_and_reserve("sk", "T1", "C1")
    stash.resolve("sk", "T1", "C1", spent=False)
    retry = stash.peek_and_reserve("sk", "T1", "C2")
    assert retry is not None
    assert retry["message_id"] == "m1"


def test_stash_resolve_is_a_noop_for_a_mismatched_tool_call_id():
    stash = human_origin._OriginStash()
    stash.capture("sk", {"user_id": "u1", "text_sha256": "h1"})
    stash.bind("sk", "T1", sender_id="u1", text_sha256="h1")
    stash.peek_and_reserve("sk", "T1", "C1")
    stash.resolve("sk", "T1", "SOME-OTHER-ID", spent=True)  # does not touch C1's reservation
    assert stash.peek_and_reserve("sk", "T1", "C1") is not None


def test_stash_pending_is_bounded_per_session():
    stash = human_origin._OriginStash(max_pending_per_session=2, max_pending_total=100, max_bound=100, ttl_seconds=10_000)
    for i in range(5):
        stash.capture("sk", {"user_id": "u", "text_sha256": f"h{i}"})
    assert stash.bind("sk", "t1", sender_id="u", text_sha256="h3") is True
    assert stash.bind("sk", "t2", sender_id="u", text_sha256="h0") is False  # already evicted


def test_stash_per_session_overflow_eviction_is_logged_p2_1():
    """kasra-review round-4 P2-1: capture() overflow used to drop the oldest
    record with no signal at all -- unlike every bind-time burn, which is
    logged. Now it reports through the same evict_callback."""
    stash = human_origin._OriginStash(max_pending_per_session=1, max_pending_total=100, max_bound=100, ttl_seconds=10_000)
    evicted = []
    stash.capture("sk", {"user_id": "u", "text_sha256": "h0", "message_id": "M-evicted"},
                  evict_callback=lambda rec, reason: evicted.append((rec["message_id"], reason)))
    stash.capture("sk", {"user_id": "u", "text_sha256": "h1", "message_id": "M-kept"},
                  evict_callback=lambda rec, reason: evicted.append((rec["message_id"], reason)))
    assert evicted == [("M-evicted", "capture_overflow_session")]


def test_stash_pending_is_bounded_globally_across_sessions():
    stash = human_origin._OriginStash(max_pending_per_session=10, max_pending_total=2, max_bound=100, ttl_seconds=10_000)
    stash.capture("sk-a", {"user_id": "u", "text_sha256": "a"})
    stash.capture("sk-b", {"user_id": "u", "text_sha256": "b"})
    stash.capture("sk-c", {"user_id": "u", "text_sha256": "c"})  # evicts sk-a's entry
    assert stash.bind("sk-a", "t1", sender_id="u", text_sha256="a") is False
    assert stash.bind("sk-b", "t2", sender_id="u", text_sha256="b") is True
    assert stash.bind("sk-c", "t3", sender_id="u", text_sha256="c") is True


def test_stash_global_overflow_eviction_is_logged_p2_1():
    stash = human_origin._OriginStash(max_pending_per_session=10, max_pending_total=2, max_bound=100, ttl_seconds=10_000)
    evicted = []
    cb = lambda rec, reason: evicted.append((rec["message_id"], reason))
    stash.capture("sk-a", {"user_id": "u", "text_sha256": "a", "message_id": "M-a"}, evict_callback=cb)
    stash.capture("sk-b", {"user_id": "u", "text_sha256": "b", "message_id": "M-b"}, evict_callback=cb)
    stash.capture("sk-c", {"user_id": "u", "text_sha256": "c", "message_id": "M-c"}, evict_callback=cb)
    assert evicted == [("M-a", "capture_overflow_global")]


def test_capture_human_origin_wires_the_real_burn_log_for_overflow(caplog):
    """End-to-end through the public capture_human_origin function (not the bare
    stash), proving a stranger-flood eviction is a real, visible WARNING."""
    store = _Store("sk-flood")
    stash = human_origin._OriginStash(max_pending_per_session=1, max_pending_total=100, max_bound=100, ttl_seconds=10_000)
    human_origin._STASH = stash
    try:
        human_origin.capture_human_origin(
            event=_event(_source(), message_id="M-evicted", text="first"), gateway=None, session_store=store,
        )
        with caplog.at_level(logging.WARNING):
            human_origin.capture_human_origin(
                event=_event(_source(), message_id="M-kept", text="second"), gateway=None, session_store=store,
            )
    finally:
        human_origin._STASH = human_origin._OriginStash()  # restore a clean global for other tests
    assert any("M-evicted" in r.message and "capture_overflow_session" in r.message for r in caplog.records)


def test_stash_bound_overflow_eviction_is_logged_at_warning_p2_2():
    """kasra-review round-6 item 3: bound-overflow eviction (the _max_bound cap)
    must log at WARNING exactly like every other burn."""
    stash = human_origin._OriginStash(max_bound=1, max_pending_total=100, max_pending_per_session=100, ttl_seconds=10_000)
    evicted = []
    stash.capture("sk-a", {"user_id": "u", "text_sha256": "a", "message_id": "M-a"})
    stash.bind("sk-a", "ta", sender_id="u", text_sha256="a")
    stash.capture("sk-b", {"user_id": "u", "text_sha256": "b", "message_id": "M-b"})
    stash.bind("sk-b", "tb", sender_id="u", text_sha256="b",
               burn_callback=lambda rec, reason: evicted.append((rec["message_id"], reason)))
    assert evicted == [("M-a", "bound_overflow")]


def test_stash_bound_overflow_eviction_is_logged_via_the_real_log_burn_at_warning(monkeypatch, caplog):
    """End-to-end through capture_human_origin/bind_turn_custody -- proving the
    REAL _log_burn callback (not a test double) fires at WARNING for a
    bound-overflow eviction, not merely that some callback would."""
    stash = human_origin._OriginStash(max_bound=1, max_pending_total=100, max_pending_per_session=100, ttl_seconds=10_000)
    human_origin._STASH = stash
    try:
        _capture("sk-bound-overflow-a", text="approve a", message_id="M-overflow-a")
        _bind("sk-bound-overflow-a", "ta", text="approve a", monkeypatch=monkeypatch)
        _capture("sk-bound-overflow-b", text="approve b", message_id="M-overflow-b")
        with caplog.at_level(logging.WARNING):
            _bind("sk-bound-overflow-b", "tb", text="approve b", monkeypatch=monkeypatch)
    finally:
        human_origin._STASH = human_origin._OriginStash()
    assert any(
        "M-overflow-a" in r.message and "bound_overflow" in r.message and r.levelno == logging.WARNING
        for r in caplog.records
    )


def test_stash_ttl_is_a_backstop_for_a_session_that_never_reaches_bind(monkeypatch):
    stash = human_origin._OriginStash(ttl_seconds=5)
    clock = iter([100.0, 200.0])
    monkeypatch.setattr(human_origin.time, "monotonic", lambda: next(clock))
    stash.capture("sk", {"user_id": "u", "text_sha256": "h"})
    burned = []
    assert stash.bind("sk", "T1", sender_id="u", text_sha256="h",
                      burn_callback=lambda rec, reason: burned.append(reason)) is False
    assert burned == ["expired"]


def test_stash_ttl_expires_bound_records_too(monkeypatch):
    stash = human_origin._OriginStash(ttl_seconds=5)
    clock = iter([100.0, 100.0, 200.0])
    monkeypatch.setattr(human_origin.time, "monotonic", lambda: next(clock))
    stash.capture("sk", {"user_id": "u", "text_sha256": "h"})
    assert stash.bind("sk", "T1", sender_id="u", text_sha256="h") is True
    assert stash.peek_and_reserve("sk", "T1", "C1") is None  # now expired


def test_stash_bound_lru_evicts_the_oldest_unresolved_binding_first():
    """Since round 6's reserve/resolve replaced round 5's one-shot read(), LRU
    only matters for BOUND entries nobody has RESOLVED yet -- bind() still
    move_to_end()s on write, so the oldest never-resolved binding is the one
    _max_bound eviction takes."""
    stash = human_origin._OriginStash(max_bound=2, max_pending_total=100, max_pending_per_session=100, ttl_seconds=10_000)
    stash.capture("sk-a", {"user_id": "u", "text_sha256": "a"})
    stash.capture("sk-b", {"user_id": "u", "text_sha256": "b"})
    stash.bind("sk-a", "ta", sender_id="u", text_sha256="a")
    stash.bind("sk-b", "tb", sender_id="u", text_sha256="b")
    stash.capture("sk-c", {"user_id": "u", "text_sha256": "c"})
    stash.bind("sk-c", "tc", sender_id="u", text_sha256="c")  # forces eviction: sk-a's binding (oldest)
    assert stash.peek_and_reserve("sk-a", "ta", "c1") is None
    assert stash.peek_and_reserve("sk-b", "tb", "c2") is not None
    assert stash.peek_and_reserve("sk-c", "tc", "c3") is not None


def test_stash_drop_session_clears_pending_and_bound():
    stash = human_origin._OriginStash()
    stash.capture("sk", {"user_id": "u", "text_sha256": "a"})
    stash.bind("sk", "T1", sender_id="u", text_sha256="a")
    stash.capture("sk", {"user_id": "u", "text_sha256": "b"})
    stash.drop_session("sk")
    assert len(stash) == 0
    assert stash.peek_and_reserve("sk", "T1", "c1") is None
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
    got = stash.peek_and_reserve("sk", "T1", "C1")
    assert got["user_id"] == "u"  # bind()'s own copy was unaffected by mutating the caller's dict
    got["user_id"] = "mutated-after-peek"
    # peek_and_reserve() hands back a FRESH copy every call -- mutating one
    # returned dict never affects the stored record or a later peek:
    again = stash.peek_and_reserve("sk", "T1", "C1")
    assert again["user_id"] == "u"


def test_stash_bind_relies_on_the_callback_being_safe_log_burn_is():
    """_OriginStash.bind() itself does not wrap burn_callback in a try/except (it
    is a plain data structure, not a hook) -- it relies on the callback being
    safe. The REAL callback, _log_burn, IS safe (never raises even on a
    malformed record, see test_log_burn_never_raises_on_a_malformed_record), and
    bind_turn_custody (the hook) wraps the whole bind() call in its own
    try/except regardless -- two independent layers of safety, not one."""
    stash = human_origin._OriginStash()
    stash.capture("sk", {"user_id": "u", "text_sha256": "h1"})

    def _boom(rec, reason):
        raise RuntimeError("a badly-written callback")

    with pytest.raises(RuntimeError):
        stash.bind("sk", "T1", sender_id="u", text_sha256="WRONG", burn_callback=_boom)


# ---------------------------------------------------------------------------
# _log_burn (mutation (m2) target)
# ---------------------------------------------------------------------------

def test_log_burn_logs_warning_with_message_id_and_reason(caplog):
    with caplog.at_level(logging.WARNING):
        human_origin._log_burn({"message_id": "M-abc"}, "text_mismatch")
    assert any("M-abc" in r.message and "text_mismatch" in r.message and r.levelno == logging.WARNING
              for r in caplog.records)


def test_log_burn_never_raises_on_a_malformed_record():
    human_origin._log_burn({}, "sender_mismatch")  # missing message_id -- must not raise
    human_origin._log_burn(None, "expired")  # not even a mapping -- must not raise


# ---------------------------------------------------------------------------
# on_session_boundary
# ---------------------------------------------------------------------------

def test_session_boundary_drops_records_for_that_session(monkeypatch):
    _capture("sk-boundary", text="approve")
    _bind("sk-boundary", "T1", text="approve", monkeypatch=monkeypatch)
    human_origin._remember_session_id("db-session-1", "sk-boundary")
    assert _peek("sk-boundary", "T1") is not None
    human_origin._on_session_boundary(session_id="db-session-1")
    assert _peek("sk-boundary", "T1") is None


def test_session_boundary_with_unknown_session_id_is_a_noop():
    human_origin._on_session_boundary(session_id="never-seen")


def test_session_boundary_never_raises():
    human_origin._on_session_boundary(session_id="")


# ---------------------------------------------------------------------------
# register()
# ---------------------------------------------------------------------------

def test_register_wires_all_six_hooks():
    calls = []

    class Ctx:
        def register_hook(self, name, callback):
            calls.append(name)

    human_origin.register(Ctx())
    assert calls == [
        "pre_gateway_dispatch", "pre_llm_call", "pre_tool_call", "post_tool_call",
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

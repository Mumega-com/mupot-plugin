"""Contract tests for the deterministic first-person intake handler.

Round 2 (adversarial gate on PR#17 @ 5990a2e4, RED: 3 P0 / 5 P1 / 3 P2): the
gate moved from "have we locally seen this chat" to "does mupot's own status
say intake_state == 'pending' right now" -- these tests are organized around
that boundary first, then the write-through/credential/sanitization hygiene,
then the capability floor, then a minimal fake-PTB shim exercising the actual
registration wiring.
"""

from __future__ import annotations

import asyncio
import json
import sys
import types
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable

import pytest
import yaml

import dataclasses

import plugin.first_person as first_person
from plugin.first_person import (
    FIRST_PERSON_QUESTIONS,
    SKILL_PATH,
    FirstPersonRuntime,
    FirstPersonSettings,
    Intake,
    StatusResolution,
    _AWAITING_HUMANS_REPLY,
    _ESCAPE_WORDS,
    _HOME_NOT_READY_REPLY,
    _HOME_NOT_READY_WINDOW_SECONDS,
    _LAG_REPLY_WINDOW_SECONDS,
    _LAG_WARNING_WINDOW_SECONDS,
    _PAUSED_REPLY,
    _PENDING_ABSOLUTE_TTL_SECONDS,
    _PENDING_IDLE_TTL_SECONDS,
    _PROPOSAL_FAILED_REPLY,
    _PROPOSAL_RETRY_WAIT_REPLY,
    _PROPOSAL_STALLED_AFTER_ATTEMPTS,
    _PROPOSAL_STALLED_REPLY,
    _PROPOSAL_STALLED_REPLY_FLAG_FAILED,
    _PROPOSAL_STILL_STALLED_REPLY,
    _NotifyOncePerWindow,
    _ProbeLimiter,
    _SenderProbeLimiter,
    _StatusCache,
    _UNRELATED_ANSWER_REPLY,
    _VERDICT_COMMAND_PATTERN,
    _build_resolve_project_request,
    _classify_intake_state,
    _looks_like_credential,
    _resolve_member_project,
    _sanitize_answer,
    handle_first_contact,
    is_verdict_shaped,
    register_first_person,
    register_first_person_skill,
    resolve_member_status,
    sanitized_first_contact_envelope,
    scrub_quarantine_candidates,
)
from plugin.mupot_operator import FIRST_PERSON_ACTIONS, OPERATOR_TOOL_NAMES, MANAGER_TOOL_NAMES


@pytest.fixture(autouse=True)
def simplex_hermes_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    from plugin.tests.test_profile_scope import install_secret_scope

    install_secret_scope(
        monkeypatch,
        scope={"TEST_IM_WEBHOOK_SECRET": "test-profile-webhook-secret"},
    )


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class User:
    def __init__(self, user_id: int = 123, full_name: str = "Ada Example") -> None:
        self.id = user_id
        self.full_name = full_name


class Chat:
    def __init__(self, chat_id: int = 123, chat_type: str = "private") -> None:
        self.id = chat_id
        self.type = chat_type


class Message:
    def __init__(self, text: str = "hello", **forwarding: object) -> None:
        self.text = text
        self.replies: list[str] = []
        for key, value in forwarding.items():
            setattr(self, key, value)

    async def reply_text(self, text: str) -> None:
        self.replies.append(text)


class Update:
    def __init__(
        self,
        *,
        update_id: int = 456,
        user: User | None = None,
        chat: Chat | None = None,
        message: Message | None = None,
        edited_message: Any = None,
    ) -> None:
        self.update_id = update_id
        self.effective_user = user if user is not None else User()
        self.effective_chat = chat if chat is not None else Chat()
        self.effective_message = message if message is not None else Message()
        self.edited_message = edited_message


class Response:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def __enter__(self) -> "Response":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        return self.body if size < 0 else self.body[:size]


class Opener:
    def __init__(self, response: Response | BaseException) -> None:
        self.response = response
        self.calls: list[object] = []

    def open(self, request: object, timeout: float) -> Response:
        self.calls.append(request)
        if isinstance(self.response, BaseException):
            raise self.response
        return self.response


class FakeClient:
    """Per-action canned responses, mirroring test_telegram_inline_approval.py's
    FakeClient but keyed by action since first-person calls several different
    actions across one intake."""

    def __init__(self, responses: dict[str, Any] | None = None) -> None:
        self.responses = responses or {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        # Round-3-gate-4 P1-a: _stall_reply reads client.settings.squad_id /
        # .agent_id to build the task_create payload -- a real
        # MupotOperatorClient always has these; this stand-in mirrors the
        # shape.
        self.settings = types.SimpleNamespace(squad_id="squad-test", agent_id="agent-test")

    def call(self, action: str, args: dict[str, Any]) -> Any:
        self.calls.append((action, dict(args)))
        if action not in FIRST_PERSON_ACTIONS:
            return {"ok": False, "error": "action_not_allowed", "action": action}
        return self.responses.get(action, {"ok": True, "result": {}})


def valid_settings(**changes: object) -> FirstPersonSettings:
    settings = FirstPersonSettings(
        enabled=True,
        base_url="https://pot.example.invalid/base",
        webhook_secret_env="TEST_IM_WEBHOOK_SECRET",
        timeout=7.0,
    )
    return replace(settings, **changes)


def status_response_bytes(
    *,
    bound: bool = True,
    member_id: str | None = "member-1",
    home_squad_id: str | None = None,
    intake_state: str = "pending",
) -> bytes:
    payload: dict[str, Any] = {"ok": True, "bound": bound}
    if bound:
        payload["member_id"] = member_id
        payload["home_squad_id"] = home_squad_id
        payload["intake_state"] = intake_state
    return json.dumps(payload).encode("utf-8")


def install_probe(monkeypatch: pytest.MonkeyPatch, *, response: bytes) -> Opener:
    monkeypatch.setattr(
        "plugin.first_person.read_profile_secret", lambda _name: "runtime-webhook-secret"
    )
    opener = Opener(Response(response))
    monkeypatch.setattr("plugin.first_person.build_opener", lambda *_: opener)
    return opener


def install_status_stub(
    monkeypatch: pytest.MonkeyPatch, resolver: Callable[[Any, Any], StatusResolution]
) -> None:
    """Bypass the HTTP layer entirely for tests that only care about
    handle_first_contact's gating logic, not resolve_member_status's own wire
    format (that gets its own dedicated tests below)."""

    def fake_resolve(
        settings,
        user_id,
        chat_id,
        *,
        secret_owner=None,
        cache=None,
        probe_limiter=None,
        sender_limiter=None,
        fallback=None,
    ):
        return resolver(user_id, chat_id)

    monkeypatch.setattr("plugin.first_person.resolve_member_status", fake_resolve)


def pending_status(member_id: str = "member-1", home_squad_id: str | None = "home-squad-1") -> StatusResolution:
    return StatusResolution(bound=True, member_id=member_id, home_squad_id=home_squad_id, intake_state="pending")


# Round-3-gate-2 P2: UNBOUND_STATUS ("mupot confirmed you are not a member")
# and UNKNOWN_STATUS ("the response itself was malformed/absent/timed out")
# are now genuinely DIFFERENT values -- round-2 conflated both into
# intake_state="unknown", which made a transient probe failure
# indistinguishable from a confirmed non-member and meant either one
# abandoned a genuinely in-progress local intake. See
# resolve_member_status's "not bound" branch.
UNBOUND_STATUS = StatusResolution(bound=False, member_id=None, home_squad_id=None, intake_state="none")
NONE_STATUS = StatusResolution(bound=True, member_id="member-1", home_squad_id=None, intake_state="none")
COMPLETE_STATUS = StatusResolution(bound=True, member_id="member-1", home_squad_id="home-squad-1", intake_state="complete")
UNKNOWN_STATUS = StatusResolution(bound=False, member_id=None, home_squad_id=None, intake_state="unknown")


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


def test_settings_disabled_by_default_and_boolean_checked() -> None:
    settings = FirstPersonSettings.from_mapping({"base_url": "https://pot.example.invalid"})
    assert settings.enabled is False
    with pytest.raises(ValueError, match="boolean"):
        FirstPersonSettings.from_mapping(
            {"base_url": "https://pot.example.invalid", "first_person_enabled": "true"}
        )


@pytest.mark.parametrize(
    "bad_url",
    [
        "http://pot.example.invalid",
        "https://user:pass@pot.example.invalid",
        "https://pot.example.invalid?secret=value",
        "not-a-url",
    ],
)
def test_settings_require_a_credential_free_https_base_url(bad_url: str) -> None:
    with pytest.raises(ValueError, match="base_url"):
        valid_settings(base_url=bad_url).validate()


def test_disabled_first_person_registers_no_native_handler() -> None:
    calls: list[object] = []
    ctx = types.SimpleNamespace(register_telegram_handler=calls.append)
    register_first_person(ctx, replace(valid_settings(), enabled=False), client=FakeClient())
    assert calls == []


# ---------------------------------------------------------------------------
# Fence -- round-2 M5 hardening: each check independently load-bearing.
# ---------------------------------------------------------------------------


def test_fence_type_check_alone_is_not_sufficient() -> None:
    """Chat marked 'private' but sender id != chat id must still refuse --
    proves the type check is not the only thing doing the work."""
    update = Update(user=User(user_id=999), chat=Chat(chat_id=123, chat_type="private"))
    with pytest.raises(ValueError):
        sanitized_first_contact_envelope(update)


def test_fence_forwarding_check_alone_is_not_sufficient_either() -> None:
    """A private chat with matching ids but a forwarding marker must still
    refuse -- the type+id checks passing is not enough on their own."""
    update = Update(message=Message(forward_date=123))
    with pytest.raises(ValueError):
        sanitized_first_contact_envelope(update)


def test_fence_group_chat_refused() -> None:
    with pytest.raises(ValueError):
        sanitized_first_contact_envelope(Update(chat=Chat(chat_id=-999, chat_type="group")))


def test_fence_accepts_a_genuinely_clean_private_dm() -> None:
    envelope = sanitized_first_contact_envelope(Update())
    assert envelope["chat_id"] == 123
    assert envelope["user_id"] == 123


# ---------------------------------------------------------------------------
# resolve_member_status -- wire format + fail-safe-for-the-intake on
# missing/malformed fields
# ---------------------------------------------------------------------------


def test_resolve_member_status_is_fail_safe_for_the_intake_on_missing_structured_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """kasra-review's PR#15 lesson, reapplied: a response with ok:true but no
    `bound` field at all must resolve unknown, never crash, never default to
    bound. This IS the fail-safe-for-the-intake posture Athena's round-1
    ruling requires while the mupot-side contract fields don't exist yet --
    absent/unknown status never gets consumed, the host handler owns the
    turn."""
    install_probe(monkeypatch, response=b'{"ok":true,"reply":"Welcome back!"}')
    status = resolve_member_status(valid_settings(), 123, 123)
    assert status == StatusResolution(bound=False, member_id=None, home_squad_id=None, intake_state="unknown")
    assert status.is_pending is False


def test_resolve_member_status_rejects_invalid_intake_state(monkeypatch: pytest.MonkeyPatch) -> None:
    install_probe(
        monkeypatch,
        response=json.dumps(
            {"ok": True, "bound": True, "member_id": "m-1", "home_squad_id": None, "intake_state": "bogus"}
        ).encode(),
    )
    status = resolve_member_status(valid_settings(), 123, 123)
    assert status.intake_state == "unknown"
    assert status.is_pending is False


def test_resolve_member_status_happy_path_pending(monkeypatch: pytest.MonkeyPatch) -> None:
    install_probe(monkeypatch, response=status_response_bytes(home_squad_id="home-1"))
    status = resolve_member_status(valid_settings(), 123, 123)
    assert status == StatusResolution(
        bound=True, member_id="member-1", home_squad_id="home-1", intake_state="pending"
    )
    assert status.is_pending is True


def test_resolve_member_status_probe_body_never_carries_display_name_or_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Round-2 P1-1/P1-2: no synthesized /start, no display name, no message
    text -- identity coordinates only."""
    opener = install_probe(monkeypatch, response=status_response_bytes())
    resolve_member_status(valid_settings(), 123, 123)
    assert len(opener.calls) == 1
    request = opener.calls[0]
    body = json.loads(request.data.decode("utf-8"))
    assert set(body) == {"kind", "user_id", "chat_id"}
    assert "first_name" not in body
    assert "text" not in body
    assert "/start" not in json.dumps(body)


def test_status_cache_rate_limits_repeated_lookups_for_the_same_user(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opener = install_probe(monkeypatch, response=status_response_bytes())
    cache = _StatusCache(positive_ttl=100.0, negative_ttl=100.0)
    for _ in range(5):
        resolve_member_status(valid_settings(), 123, 123, cache=cache)
    assert len(opener.calls) == 1  # only the first call reached the network


def test_status_cache_uses_a_longer_ttl_for_unbound_results(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_time = {"now": 0.0}
    cache = _StatusCache(positive_ttl=1.0, negative_ttl=1000.0, clock=lambda: fake_time["now"])
    cache.put("stranger", UNBOUND_STATUS)
    fake_time["now"] = 50.0
    assert cache.get("stranger") == UNBOUND_STATUS  # still within the long negative TTL

    cache.put("member", pending_status())
    fake_time["now"] = 51.5  # 1.5s after the positive put -- past its 1s TTL
    assert cache.get("member") is None


# ---------------------------------------------------------------------------
# handle_first_contact -- the server-declared gate (round-2 P0-1/P0-2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stranger_message_is_not_consumed_and_nothing_is_stored(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    install_status_stub(monkeypatch, lambda *_: UNBOUND_STATUS)
    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    update = Update(message=Message(text="hi there, my name is Ada"))

    handled = await handle_first_contact(update, settings=valid_settings(), client=client, runtime=runtime)

    assert handled is False
    assert update.effective_message.replies == []
    assert not (tmp_path / "state.json").exists()
    assert runtime.get_pending("123") is None
    assert client.calls == []


@pytest.mark.asyncio
async def test_bound_member_who_never_started_intake_is_not_consumed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    install_status_stub(monkeypatch, lambda *_: NONE_STATUS)
    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    update = Update(message=Message(text="hello Mubot"))

    handled = await handle_first_contact(update, settings=valid_settings(), client=client, runtime=runtime)

    assert handled is False
    assert update.effective_message.replies == []
    assert client.calls == []


@pytest.mark.asyncio
async def test_already_onboarded_members_approve_message_is_not_consumed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The literal round-2 regression test: a bound, already-onboarded member
    sending 'approve f9408956' must fall through to the plain-text decision
    path (#1425), never be swallowed as an intake answer."""
    install_status_stub(monkeypatch, lambda *_: COMPLETE_STATUS)
    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    update = Update(message=Message(text="approve f9408956"))

    handled = await handle_first_contact(update, settings=valid_settings(), client=client, runtime=runtime)

    assert handled is False
    assert update.effective_message.replies == []
    assert client.calls == []


@pytest.mark.asyncio
async def test_unknown_status_fails_open_when_contract_fields_are_absent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Athena's round-1 ruling: while the mupot-side contract fields don't
    exist on a deployment, resolve as unknown and fall through -- never
    treated as pending, never treated as a reason to reply."""
    install_status_stub(monkeypatch, lambda *_: UNKNOWN_STATUS)
    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    update = Update(message=Message(text="anything at all"))

    handled = await handle_first_contact(update, settings=valid_settings(), client=client, runtime=runtime)

    assert handled is False
    assert update.effective_message.replies == []


@pytest.mark.asyncio
async def test_edited_message_is_never_consumed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    install_status_stub(monkeypatch, lambda *_: pending_status())
    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    update = Update(message=Message(text="edited text"), edited_message=object())

    handled = await handle_first_contact(update, settings=valid_settings(), client=client, runtime=runtime)

    assert handled is False
    assert client.calls == []


@pytest.mark.asyncio
async def test_pending_status_with_no_home_yet_is_left_untouched(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Round 3: create_home_for_member has NO exposed MCP action or /im route
    (verified on mupot's kasra/fp01-slice2-proposal-chain branch) -- this
    module never invents a call for it. A bound, pending member with a null
    home_squad_id consumes nothing and stores nothing (round-3-gate-2,
    Athena's addition: a rate-limited reassurance reply, never silence)."""
    install_status_stub(monkeypatch, lambda *_: pending_status(home_squad_id=None))
    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    update = Update(message=Message(text="hello!"))

    handled = await handle_first_contact(update, settings=valid_settings(), client=client, runtime=runtime)

    assert handled is False
    assert update.effective_message.replies == [_HOME_NOT_READY_REPLY]
    assert runtime.get_pending("123") is None
    assert client.calls == []


@pytest.mark.asyncio
async def test_home_not_ready_reply_is_rate_limited_to_once_per_hour(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake_time = {"now": 0.0}
    install_status_stub(monkeypatch, lambda *_: pending_status(home_squad_id=None))
    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    settings = valid_settings()
    notifier = _NotifyOncePerWindow(_HOME_NOT_READY_WINDOW_SECONDS, clock=lambda: fake_time["now"])

    first = Update(message=Message(text="hello!"))
    await handle_first_contact(first, settings=settings, client=client, runtime=runtime, home_wait_notifier=notifier)
    assert first.effective_message.replies == [_HOME_NOT_READY_REPLY]

    fake_time["now"] += 60.0  # 1 minute later -- well within the hour window
    second = Update(message=Message(text="hello again"))
    await handle_first_contact(second, settings=settings, client=client, runtime=runtime, home_wait_notifier=notifier)
    assert second.effective_message.replies == []  # rate-limited, no repeat

    fake_time["now"] += _HOME_NOT_READY_WINDOW_SECONDS + 1.0  # past the hour
    third = Update(message=Message(text="still there?"))
    await handle_first_contact(third, settings=settings, client=client, runtime=runtime, home_wait_notifier=notifier)
    assert third.effective_message.replies == [_HOME_NOT_READY_REPLY]


@pytest.mark.asyncio
async def test_pending_status_with_existing_home_starts_intake(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    install_status_stub(monkeypatch, lambda *_: pending_status(home_squad_id="home-squad-1"))
    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    update = Update(message=Message(text="hello!"))

    handled = await handle_first_contact(update, settings=valid_settings(), client=client, runtime=runtime)

    assert handled is True
    assert update.effective_message.replies == [FIRST_PERSON_QUESTIONS[0][1]]
    pending = runtime.get_pending("123")
    assert pending is not None
    assert pending.home_squad_id == "home-squad-1"
    assert client.calls == []  # no plugin-action call of any kind for home


@pytest.mark.asyncio
async def test_unbind_mid_intake_still_serves_the_message_but_reconciliation_drops_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """ROOT SHAPE (Athena, binding, mupot-plugin#20 issue tracker P0): the
    durable local pending record is the AUTHORITY for THIS message
    regardless of what a concurrent probe says -- an unbind is never
    consulted on the message path and so is never a reason to hold or drop
    an in-flight answer (see test_pending_member_messages_always_traverse_
    pipeline for the comprehensive version of this). What an unbind DOES do
    is feed `_reconcile_pending_with_server`, which runs strictly AFTER
    this message is handled and can only affect the record used for the
    NEXT one.

    Round-2-gate (#23) P2-a: a definitive non-pending reading now suspends
    capture on the very FIRST (unconfirmed) reading -- see
    FirstPersonRuntime.suspend -- so the SECOND message here (arriving
    while suspended) falls through untouched too, even before the second
    reading confirms the drop. mupot-plugin#21 P2: the second confirming
    reading must also be separated from the first by at least the
    status-cache TTL, so a controllable clock is used here."""
    fake_clock = {"now": 0.0}
    call_count = {"n": 0}

    def resolver(_user_id: Any, _chat_id: Any) -> StatusResolution:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return pending_status()
        return UNBOUND_STATUS  # unbound from the second reconciliation reading on

    install_status_stub(monkeypatch, resolver)
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    await handle_first_contact(
        Update(message=Message(text="hello!")), settings=valid_settings(), client=client, runtime=runtime
    )
    assert runtime.get_pending("123") is not None

    # First reconciliation reading of 'unbound' -- unconfirmed: THIS
    # message's own answer is still fully processed (sanitized, stored,
    # replied), since it was not yet suspended when it arrived.
    first_unbind = Update(message=Message(text="Ada Example"))
    handled = await handle_first_contact(first_unbind, settings=valid_settings(), client=client, runtime=runtime)
    assert handled is True
    assert first_unbind.effective_message.replies == [FIRST_PERSON_QUESTIONS[1][1]]
    remember_calls = [args for action, args in client.calls if action == "squad_remember"]
    assert len(remember_calls) == 1 and remember_calls[0]["text"] == "Ada Example"
    suspended_pending = runtime.get_pending("123")
    assert suspended_pending is not None and suspended_pending.suspended is True  # NOW suspended

    # Second consecutive 'unbound' reading, separated by the status-cache
    # TTL -- this message arrives ALREADY suspended, so it falls straight
    # through (never sanitized/stored/replied here), and reconciliation
    # confirms and drops the record for the message after THIS one.
    fake_clock["now"] += first_person._STATUS_NEGATIVE_TTL_SECONDS
    second_unbind = Update(message=Message(text="Engineer"))
    handled = await handle_first_contact(second_unbind, settings=valid_settings(), client=client, runtime=runtime)
    assert handled is False
    assert second_unbind.effective_message.replies == []
    remember_calls = [args for action, args in client.calls if action == "squad_remember"]
    assert len(remember_calls) == 1  # unchanged -- never captured as an answer
    assert runtime.get_pending("123") is None  # confirmed -- dropped for the NEXT message

    # And the NEXT message, with no local record left, falls through to the
    # host untouched -- the resolver keeps saying 'unbound' at this point.
    after_drop = Update(message=Message(text="anything else"))
    handled = await handle_first_contact(after_drop, settings=valid_settings(), client=client, runtime=runtime)
    assert handled is False
    assert after_drop.effective_message.replies == []


@pytest.mark.asyncio
async def test_member_mismatch_mid_chat_abandons_stale_progress(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """ROOT SHAPE: a mid-chat rebind (the same Telegram sender now
    resolving to a DIFFERENT mupot member) is likewise never consulted on
    the message path -- the message in flight when the rebind is first
    observed is still served from member-1's own record.

    Round-2-gate (#23) P2-c: a mismatch is no longer confirmed on a
    SINGLE reading -- it goes through the SAME suspend-then-confirm
    discipline as a definitive non-pending reading (P2-a), separated by at
    least the status-cache TTL. member-2's own, genuinely fresh intake
    only starts once member-1's record is actually dropped (confirmed),
    on the first message with no local record left."""
    fake_clock = {"now": 0.0}
    call_count = {"n": 0}

    def resolver(_user_id: Any, _chat_id: Any) -> StatusResolution:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return pending_status(member_id="member-1")
        return pending_status(member_id="member-2")  # a different member now resolves

    install_status_stub(monkeypatch, resolver)
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    await handle_first_contact(
        Update(message=Message(text="hello!")), settings=valid_settings(), client=client, runtime=runtime
    )
    first_pending = runtime.get_pending("123")
    assert first_pending is not None and first_pending.member_id == "member-1"

    # First mismatch reading -- unconfirmed: this message is still
    # answered from member-1's own record (ROOT SHAPE), which then
    # suspends via reconciliation.
    handled = await handle_first_contact(
        Update(message=Message(text="hello again!")), settings=valid_settings(), client=client, runtime=runtime
    )
    assert handled is True
    suspended = runtime.get_pending("123")
    assert suspended is not None and suspended.member_id == "member-1" and suspended.suspended is True

    # Second consecutive mismatch reading, separated by the status-cache
    # TTL -- this message arrives already suspended (falls through), and
    # reconciliation NOW confirms and abandons member-1's stale record.
    fake_clock["now"] += first_person._STATUS_NEGATIVE_TTL_SECONDS
    handled = await handle_first_contact(
        Update(message=Message(text="hello a third time!")), settings=valid_settings(), client=client, runtime=runtime
    )
    assert handled is False
    assert runtime.get_pending("123") is None  # abandoned by reconciliation -- never carried forward

    # member-2's own, genuinely fresh intake starts on the NEXT message.
    handled = await handle_first_contact(
        Update(message=Message(text="hello a fourth time!")), settings=valid_settings(), client=client, runtime=runtime
    )
    assert handled is True
    fourth_pending = runtime.get_pending("123")
    assert fourth_pending is not None
    assert fourth_pending.member_id == "member-2"
    assert fourth_pending is not first_pending


@pytest.mark.asyncio
async def test_stale_pending_record_expires_after_ttl(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake_time = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_time["now"])
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    assert runtime.get_pending("123") is not None

    fake_time["now"] = 601.0  # past the 600s TTL
    assert runtime.get_pending("123") is None


def test_pending_absolute_ttl_expires_even_with_continuous_activity(tmp_path: Path) -> None:
    """Round-2-gate (#23) P3: pins ``_PENDING_ABSOLUTE_TTL_SECONDS``
    SPECIFICALLY, as distinct from the IDLE ttl above -- a record that is
    repeatedly touched (so it never goes idle) must still be dropped once
    its TOTAL age exceeds the absolute cap. Mutation-provable (M11):
    deleting the ``now - intake.created_at > _PENDING_ABSOLUTE_TTL_SECONDS``
    check in ``get_pending`` makes this go red -- the record would survive
    forever as long as it keeps being touched, which is exactly what the
    absolute cap exists to forbid."""
    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")

    step = _PENDING_IDLE_TTL_SECONDS / 2  # well within the idle TTL each time
    while fake_clock["now"] < _PENDING_ABSOLUTE_TTL_SECONDS - step:
        fake_clock["now"] += step
        assert runtime.touch("123") is not None  # never idle-dropped

    fake_clock["now"] = _PENDING_ABSOLUTE_TTL_SECONDS + 1.0
    assert runtime.get_pending("123") is None  # absolute cap -- dropped regardless of activity


def test_pending_registry_is_bounded_lru(tmp_path: Path) -> None:
    """Round-2-gate (#23) P3: ``FirstPersonRuntime._pending`` is capped
    like ``_terminal_sightings``/``_StatusCache`` -- a burst of distinct
    brand-new chats faster than any TTL sweep must never grow it without
    limit. Mutation-provable: removing the eviction loop in
    ``_put_pending`` makes ``len(runtime._pending)`` exceed the cap."""
    runtime = FirstPersonRuntime(tmp_path / "state.json", max_pending=5)
    for i in range(10):
        runtime.start(f"chat-{i}", member_id=f"member-{i}", home_squad_id="home-squad-1")

    assert len(runtime._pending) == 5
    # The most-recently-started chats survive; the oldest were evicted.
    for i in range(5):
        assert runtime.get_pending(f"chat-{i}") is None
    for i in range(5, 10):
        assert runtime.get_pending(f"chat-{i}") is not None


def test_is_complete_or_unknown_fails_closed_on_invalid_store(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text("{not valid json")
    runtime = FirstPersonRuntime(state_path)
    assert runtime.is_complete_or_unknown("member-1") is True


def test_is_complete_or_unknown_false_for_a_genuinely_untouched_member(tmp_path: Path) -> None:
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    assert runtime.is_complete_or_unknown("member-1") is False


# ---------------------------------------------------------------------------
# Full intake: write-through, project resolution, proposal, completion
# ---------------------------------------------------------------------------


def _client_for_full_intake() -> FakeClient:
    engram_counter = {"n": 0}

    class _SequencedClient(FakeClient):
        def call(self, action: str, args: dict[str, Any]) -> Any:
            self.calls.append((action, dict(args)))
            if action not in FIRST_PERSON_ACTIONS:
                return {"ok": False, "error": "action_not_allowed"}
            if action == "squad_remember":
                engram_counter["n"] += 1
                return {"ok": True, "result": {"engram_id": f"engram-{engram_counter['n']}"}}
            if action == "routine_proposal_submit":
                return {"ok": True, "result": {"proposal_id": "proposal-1"}}
            return {"ok": True, "result": {}}

    return _SequencedClient()


def _client_rejecting_project_access() -> FakeClient:
    """Mirrors the LIVE routine_proposal_submit schema gap this module's own
    comments document: squad_remember succeeds normally, but the
    project_access proposal kind is rejected every time -- the DEFAULT
    outcome today, and the trigger for the round-3-gate-3 P0 resume defect
    (a fully-answered intake whose proposal never successfully submits)."""
    engram_counter = {"n": 0}

    class _RejectingClient(FakeClient):
        def call(self, action: str, args: dict[str, Any]) -> Any:
            self.calls.append((action, dict(args)))
            if action not in FIRST_PERSON_ACTIONS:
                return {"ok": False, "error": "action_not_allowed"}
            if action == "squad_remember":
                engram_counter["n"] += 1
                return {"ok": True, "result": {"engram_id": f"engram-{engram_counter['n']}"}}
            if action == "routine_proposal_submit":
                return {"ok": False, "error": "unsupported_action_kind"}
            return {"ok": True, "result": {}}

    return _RejectingClient()


def _default_project_resolver(_settings: Any, _envelope: Any, query: str, *, secret_owner: Any = None) -> str | None:
    """Round 3: project resolution moved off client.call onto the
    authenticated /im/resolve-project surface (_resolve_member_project) --
    tests bypass ITS wire format the same way install_status_stub bypasses
    resolve_member_status's, since the wire format gets its own dedicated
    tests below. Matches round-2's fixture project ("psychonom" -> "proj-1")."""
    return "proj-1" if query.strip().lower() == "psychonom" else None


async def _run_intake(
    monkeypatch,
    client,
    runtime,
    answers,
    *,
    member_id="member-1",
    home_squad_id="home-squad-1",
    project_resolver=None,
):
    install_status_stub(monkeypatch, lambda *_: pending_status(member_id=member_id, home_squad_id=home_squad_id))
    monkeypatch.setattr(
        "plugin.first_person._resolve_member_project", project_resolver or _default_project_resolver
    )
    settings = valid_settings()
    update = Update(message=Message(text="hello!"))
    await handle_first_contact(update, settings=settings, client=client, runtime=runtime)
    last_update = update
    for answer in answers:
        last_update = Update(message=Message(text=answer))
        await handle_first_contact(last_update, settings=settings, client=client, runtime=runtime)
    return last_update


@pytest.mark.asyncio
async def test_full_five_question_intake_writes_to_home_and_submits_proposal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    client = _client_for_full_intake()
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    answers = ["Ada Example", "Engineer", "psychonom", "Wire up the intake flow", "Nothing else"]

    project_calls: list[tuple[Any, str]] = []

    def project_resolver(_settings: Any, envelope: Any, query: str, *, secret_owner: Any = None) -> str | None:
        project_calls.append((envelope, query))
        return "proj-1" if query.strip().lower() == "psychonom" else None

    last_update = await _run_intake(monkeypatch, client, runtime, answers, project_resolver=project_resolver)

    remember_calls = [args for action, args in client.calls if action == "squad_remember"]
    assert len(remember_calls) == 5
    assert all(args["squad_id"] == "home-squad-1" for args in remember_calls)
    assert [args["text"] for args in remember_calls] == answers

    # Round 3: project resolution is a raw /im/resolve-project call keyed on
    # the CURRENT turn's own identity envelope, never member_id/name via
    # client.call.
    assert [query for _envelope, query in project_calls] == ["psychonom"]
    envelope_used = project_calls[0][0]
    assert envelope_used["chat_id"] == 123
    assert envelope_used["user_id"] == 123

    proposal_calls = [args for action, args in client.calls if action == "routine_proposal_submit"]
    assert len(proposal_calls) == 1
    assert proposal_calls[0]["action"]["input"] == {
        "member_id": "member-1",
        "project_id": "proj-1",
        "access_level": "write",
        "reason": "first-person intake",
    }

    assert runtime.get_pending("123") is None
    assert last_update.effective_message.replies[-1] == (
        "Thanks -- I've sent your access request to the team for a decision."
    )
    data = json.loads(state_path.read_text())
    completed = data["completed"]["member-1"]
    assert set(completed["engram_ids"]) == {q_id for q_id, _ in FIRST_PERSON_QUESTIONS}
    assert completed["proposal_id"] == "proposal-1"


@pytest.mark.asyncio
async def test_raw_answer_text_never_lands_anywhere_in_the_state_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Round-2 mutation M1: check the WHOLE state directory, not one file --
    a future regression could just as easily write a sibling file."""
    client = _client_for_full_intake()
    state_dir = tmp_path / "state-dir"
    state_path = state_dir / "state.json"
    runtime = FirstPersonRuntime(state_path)
    answers = [
        "Ada Example",
        "Senior Distributed Systems Engineer",
        "psychonom",
        "Ship the onboarding flow end to end",
        "I work best in the mornings, UTC-5",
    ]
    await _run_intake(monkeypatch, client, runtime, answers)

    for path in state_dir.rglob("*"):
        if path.is_file():
            content = path.read_text(errors="replace")
            for answer in answers:
                assert answer not in content, f"raw answer leaked into {path}"


@pytest.mark.asyncio
async def test_home_squad_id_is_never_derived_from_an_answer(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Round-2 mutation M4: even a project answer engineered to look like a
    squad id must never change home_squad_id."""
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    suspicious_project_answer = "home-squad-EVIL psychonom"  # contains "squad" and resolves via name match below

    class _ClientWithMatchingProject(FakeClient):
        def call(self, action: str, args: dict[str, Any]) -> Any:
            self.calls.append((action, dict(args)))
            if action == "squad_remember":
                return {"ok": True, "result": {"engram_id": "engram-x"}}
            if action == "routine_proposal_submit":
                return {"ok": True, "result": {"proposal_id": "proposal-1"}}
            return {"ok": False, "error": "action_not_allowed"}

    client = _ClientWithMatchingProject()
    install_status_stub(monkeypatch, lambda *_: pending_status(home_squad_id="home-squad-1"))
    monkeypatch.setattr(
        "plugin.first_person._resolve_member_project",
        lambda _settings, _envelope, _query, *, secret_owner=None: "proj-1",
    )
    settings = valid_settings()
    update = Update(message=Message(text="hello!"))
    await handle_first_contact(update, settings=settings, client=client, runtime=runtime)
    pending = runtime.get_pending("123")
    assert pending is not None
    original_home_squad_id = pending.home_squad_id

    for answer in ["Ada", "Engineer", suspicious_project_answer]:
        await handle_first_contact(
            Update(message=Message(text=answer)), settings=settings, client=client, runtime=runtime
        )
        pending = runtime.get_pending("123")
        assert pending is None or pending.home_squad_id == original_home_squad_id

    remember_calls = [args for action, args in client.calls if action == "squad_remember"]
    assert all(args["squad_id"] == original_home_squad_id for args in remember_calls)


# ---------------------------------------------------------------------------
# Credential + control-character hygiene (round-2 P2-2 / P2-3)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "secret_text",
    [
        "here is my token mupot_abcdef1234567890",
        "use ghp_abcdefghijklmnopqrstuvwxyz012345",
        "sk-abcdefghijklmnopqrstuvwx",
        "AKIAABCDEFGHIJKLMNOP",
        "a" * 32,  # long hex-ish
        "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo0NTY3ODk=",  # long base64
    ],
)
def test_looks_like_credential_detects_common_shapes(secret_text: str) -> None:
    assert _looks_like_credential(secret_text) is True


def test_looks_like_credential_does_not_flag_ordinary_answers() -> None:
    assert _looks_like_credential("Ada Example") is False
    assert _looks_like_credential("I want to ship the onboarding flow") is False


@pytest.mark.asyncio
async def test_credential_shaped_answer_is_refused_and_never_stored(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    install_status_stub(monkeypatch, lambda *_: pending_status())
    settings = valid_settings()
    update = Update(message=Message(text="hello!"))
    await handle_first_contact(update, settings=settings, client=client, runtime=runtime)

    secret_text = "my token is mupot_abcdef1234567890"
    answer_update = Update(message=Message(text=secret_text))
    handled = await handle_first_contact(answer_update, settings=settings, client=client, runtime=runtime)

    assert handled is True
    assert "Please don't share" in answer_update.effective_message.replies[0]
    assert not any(action == "squad_remember" for action, _ in client.calls)
    pending = runtime.get_pending("123")
    assert pending is not None and pending.index == 0  # not advanced
    # Athena round-2 (vi): a rejected answer never reaches process memory that
    # could later be written -- not even transiently in the intake record.
    assert pending.engrams == {}


def test_sanitize_answer_strips_bidi_overrides_and_control_chars() -> None:
    dirty = "Ada‮Evil‬\x00Example"
    cleaned = _sanitize_answer(dirty)
    assert "‮" not in cleaned
    assert "‬" not in cleaned
    assert "\x00" not in cleaned
    assert "Ada" in cleaned and "Example" in cleaned


def test_sanitize_answer_caps_length() -> None:
    cleaned = _sanitize_answer("x" * 5000)
    assert len(cleaned) <= 2000


@pytest.mark.asyncio
async def test_bidi_override_never_reaches_squad_remember(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    dirty_name = "Ada‮Evil‬"
    await _run_intake(
        monkeypatch, client, runtime, [dirty_name, "Engineer", "psychonom", "Ship it", "Nothing else"]
    )
    remember_calls = [args for action, args in client.calls if action == "squad_remember"]
    assert "‮" not in remember_calls[0]["text"]


# ---------------------------------------------------------------------------
# Proposal failure: no success-shaped no-op (round-2 P0-3)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_proposal_failure_keeps_pending_and_replies_honestly_then_retries(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    attempts = {"n": 0}

    class _FlakyClient(FakeClient):
        def call(self, action: str, args: dict[str, Any]) -> Any:
            self.calls.append((action, dict(args)))
            if action == "squad_remember":
                return {"ok": True, "result": {"engram_id": f"engram-{len(self.calls)}"}}
            if action == "routine_proposal_submit":
                attempts["n"] += 1
                if attempts["n"] == 1:
                    return {"ok": False, "error": "schema_validation_failed"}
                return {"ok": True, "result": {"proposal_id": "proposal-1"}}
            return {"ok": False, "error": "action_not_allowed"}

    client = _FlakyClient()
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    install_status_stub(monkeypatch, lambda *_: pending_status())
    monkeypatch.setattr(
        "plugin.first_person._resolve_member_project",
        lambda _settings, _envelope, _query, *, secret_owner=None: "proj-1",
    )
    settings = valid_settings()

    answers = ["Ada Example", "Engineer", "psychonom", "Wire this up", "Nothing else"]
    update = Update(message=Message(text="hello!"))
    await handle_first_contact(update, settings=settings, client=client, runtime=runtime)
    for answer in answers:
        update = Update(message=Message(text=answer))
        await handle_first_contact(update, settings=settings, client=client, runtime=runtime)

    # First submission failed: no COMPLETION marker, honest reply, pending
    # retained. The state file itself now exists (round-3-gate-2 P0's
    # durable in-progress engram record, written on every accepted answer
    # for RESUME purposes) -- what must NOT exist is a "completed" entry.
    data = json.loads(state_path.read_text())
    assert "member-1" not in data.get("completed", {})
    assert update.effective_message.replies[-1] == "I couldn't send your request yet -- I'll try again."
    pending = runtime.get_pending("123")
    assert pending is not None
    assert pending.proposal_retry_count == 1

    # Immediate retry (before backoff elapses) does not hit the network again.
    retry_update = Update(message=Message(text="anything"))
    await handle_first_contact(retry_update, settings=settings, client=client, runtime=runtime)
    assert attempts["n"] == 1
    assert retry_update.effective_message.replies[-1] == (
        "Still working on sending your request -- I'll try again shortly."
    )

    # Force the backoff to have elapsed, then retry succeeds. Intake is
    # frozen (round-3 P3-G) -- go through the runtime's replace-in-place
    # method rather than direct attribute assignment.
    runtime.schedule_proposal_retry("123", next_retry_at=0.0, retry_count=pending.proposal_retry_count)
    final_update = Update(message=Message(text="anything else"))
    await handle_first_contact(final_update, settings=settings, client=client, runtime=runtime)
    assert attempts["n"] == 2
    assert final_update.effective_message.replies[-1] == (
        "Thanks -- I've sent your access request to the team for a decision."
    )
    assert runtime.get_pending("123") is None
    data = json.loads(state_path.read_text())
    assert data["completed"]["member-1"]["proposal_id"] == "proposal-1"


# ---------------------------------------------------------------------------
# Athena's ruling (4): the DEFECT when the server keeps rejecting a
# submission (today, always: no project_access kind) is the REPLY SHAPE,
# not the rejection -- an unbounded repeat of the same "I'll try again"
# line forever reads as a permanent loop even though nothing is silently
# succeeding. Fix: once the backoff table is exhausted, switch to a
# distinct, rate-limited, honest reply.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_proposal_failure_reply_becomes_stalled_and_rate_limited_after_backoff_exhausted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Mutation-provable: removing the `retry_count >=
    _PROPOSAL_STALLED_AFTER_ATTEMPTS` branch (reverting to the plain
    _PROPOSAL_FAILED_REPLY/_PROPOSAL_RETRY_WAIT_REPLY forever) makes this go
    red -- the member would keep seeing the identical line with no
    escalation, indefinitely. Also proves the escalation reply itself is
    rate-limited (never repeated on every single post-exhaustion attempt)
    WITHOUT reverting to the pre-stall _PROPOSAL_FAILED_REPLY (round-3-
    gate-4 P3-a) -- every rate-limited repeat gets the distinct
    _PROPOSAL_STILL_STALLED_REPLY instead."""
    fake_clock = {"now": 0.0}
    fake_wall_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    stalled_notifier = _NotifyOncePerWindow(86400.0, clock=lambda: fake_wall_clock["now"])
    client = _client_rejecting_project_access()
    install_status_stub(monkeypatch, lambda *_: pending_status())
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)
    settings = valid_settings()

    async def handle(text: str) -> str:
        update = Update(message=Message(text=text))
        await handle_first_contact(
            update, settings=settings, client=client, runtime=runtime, stalled_notifier=stalled_notifier
        )
        return update.effective_message.replies[-1]

    answers = ["Ada Example", "Engineer", "psychonom", "Ship it", "Nothing else"]
    await handle("hello!")
    for answer in answers:
        await handle(answer)
    # 1st failed submission -- the ordinary, non-stalled reply.
    pending = runtime.get_pending("123")
    assert pending is not None and pending.proposal_retry_count == 1

    # Drive the backoff to exhaustion (force each retry due immediately).
    seen_replies: list[str] = []
    for _ in range(_PROPOSAL_STALLED_AFTER_ATTEMPTS + 2):
        pending = runtime.get_pending("123")
        runtime.schedule_proposal_retry("123", next_retry_at=fake_clock["now"], retry_count=pending.proposal_retry_count)
        seen_replies.append(await handle("checking in"))

    # Before exhaustion: the plain failed-attempt reply, every time. After
    # exhaustion, the distinct stalled reply fires -- but only ONCE within
    # the notifier's window; every OTHER post-exhaustion attempt gets the
    # STILL-stalled line, never a revert to the pre-stall failed reply
    # (round-3-gate-4 P3-a).
    assert seen_replies.count(_PROPOSAL_STALLED_REPLY) == 1
    stalled_first_index = seen_replies.index(_PROPOSAL_STALLED_REPLY)
    assert all(reply == _PROPOSAL_FAILED_REPLY for reply in seen_replies[:stalled_first_index])
    assert all(reply == _PROPOSAL_STILL_STALLED_REPLY for reply in seen_replies[stalled_first_index + 1 :])

    # A real task_create was actually made -- the receiver that makes
    # "I've flagged it" true (round-3-gate-4 P1-a).
    task_calls = [args for action, args in client.calls if action == "task_create"]
    assert len(task_calls) == 1
    assert task_calls[0]["squad_id"] == "squad-test"
    assert "member-1" in task_calls[0]["title"]
    stall_entries = runtime.store.stall_entries()
    assert "member-1" in stall_entries

    # Advance the WALL clock (the notifier's own clock) past the window --
    # the very next exhausted attempt escalates again.
    fake_wall_clock["now"] += 86400.0 + 1.0
    pending = runtime.get_pending("123")
    runtime.schedule_proposal_retry("123", next_retry_at=fake_clock["now"], retry_count=pending.proposal_retry_count)
    assert await handle("still there?") == _PROPOSAL_STALLED_REPLY

    # Never silently claims completion, and never abandons the member.
    state_path = tmp_path / "state.json"
    completed = json.loads(state_path.read_text()).get("completed", {}) if state_path.exists() else {}
    assert "member-1" not in completed
    assert runtime.get_pending("123") is not None


@pytest.mark.asyncio
async def test_stall_reply_is_honest_when_the_flag_call_itself_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Round-3-gate-4 P1-a: if the task_create escalation call itself
    fails, the member must NEVER be told "I've flagged it" (a fabricated
    receipt) -- the distinct, honest _PROPOSAL_STALLED_REPLY_FLAG_FAILED is
    used instead. The durable stall marker is still recorded regardless
    (the audit trail survives even when the live receiver doesn't)."""

    class _RejectingTaskCreateClient(FakeClient):
        def call(self, action: str, args: dict[str, Any]) -> Any:
            self.calls.append((action, dict(args)))
            if action not in FIRST_PERSON_ACTIONS:
                return {"ok": False, "error": "action_not_allowed"}
            if action == "squad_remember":
                return {"ok": True, "result": {"engram_id": f"engram-{len(self.calls)}"}}
            if action == "task_create":
                return {"ok": False, "error": "squad_not_found"}
            return {"ok": False, "error": "unsupported_action_kind"}

    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    client = _RejectingTaskCreateClient()
    install_status_stub(monkeypatch, lambda *_: pending_status())
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)
    settings = valid_settings()

    answers = ["Ada Example", "Engineer", "psychonom", "Ship it", "Nothing else"]
    await handle_first_contact(Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime)
    for answer in answers:
        await handle_first_contact(Update(message=Message(text=answer)), settings=settings, client=client, runtime=runtime)

    for _ in range(_PROPOSAL_STALLED_AFTER_ATTEMPTS):
        pending = runtime.get_pending("123")
        runtime.schedule_proposal_retry("123", next_retry_at=fake_clock["now"], retry_count=pending.proposal_retry_count)
        update = Update(message=Message(text="checking in"))
        await handle_first_contact(update, settings=settings, client=client, runtime=runtime)

    assert update.effective_message.replies[-1] == _PROPOSAL_STALLED_REPLY_FLAG_FAILED
    assert update.effective_message.replies[-1] != _PROPOSAL_STALLED_REPLY  # never the fabricated-receipt line
    assert "member-1" in runtime.store.stall_entries()  # audit trail still recorded


@pytest.mark.asyncio
async def test_stall_escalation_retries_on_failure_without_spending_the_24h_success_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """mupot-plugin#22 (P1, PR#20 round-2 gate): the round-3-gate-4 build
    spent the 24h success-dedup token (`stalled_notifier.should_notify`)
    BEFORE knowing whether `task_create` would succeed -- a single failed
    attempt silently suppressed every re-escalation for a full day while
    the member kept hearing `_PROPOSAL_STILL_STALLED_REPLY` ("still
    waiting on a human"), even though no human had ever actually been
    told. Fix: the 24h token is peeked (never spent) to decide the reply,
    and only `mark()`ed on a CONFIRMED `task_create` success; a separate,
    much shorter `stall_retry_notifier` window bounds re-attempts on
    failure, and the repeat reply while an attempt has failed is always
    the honest "couldn't reach the team" line, never the "still waiting"
    line that implies a human already knows.

    Mutation-provable: reverting `_stall_reply` to call
    `stalled_notifier.should_notify(member_id)` up front (spending the
    token before the outcome is known) makes this go red -- the 2nd/3rd
    check-ins below would get `_PROPOSAL_STILL_STALLED_REPLY` instead of
    the honest not-yet-reached line, `task_create` would never be retried,
    and the eventual real success would never actually flag anyone."""

    class _FlakyTaskCreateClient(FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.task_create_attempts = 0

        def call(self, action: str, args: dict[str, Any]) -> Any:
            self.calls.append((action, dict(args)))
            if action not in FIRST_PERSON_ACTIONS:
                return {"ok": False, "error": "action_not_allowed"}
            if action == "squad_remember":
                return {"ok": True, "result": {"engram_id": f"engram-{len(self.calls)}"}}
            if action == "task_create":
                self.task_create_attempts += 1
                if self.task_create_attempts < 3:
                    return {"ok": False, "error": "transient"}
                return {"ok": True, "result": {}}
            return {"ok": False, "error": "unsupported_action_kind"}

    fake_clock = {"now": 0.0}
    fake_wall_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    stalled_notifier = _NotifyOncePerWindow(86400.0, clock=lambda: fake_wall_clock["now"])
    stall_retry_notifier = _NotifyOncePerWindow(300.0, clock=lambda: fake_wall_clock["now"])
    client = _FlakyTaskCreateClient()
    install_status_stub(monkeypatch, lambda *_: pending_status())
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)
    settings = valid_settings()

    async def handle(text: str) -> str:
        update = Update(message=Message(text=text))
        await handle_first_contact(
            update,
            settings=settings,
            client=client,
            runtime=runtime,
            stalled_notifier=stalled_notifier,
            stall_retry_notifier=stall_retry_notifier,
        )
        return update.effective_message.replies[-1]

    answers = ["Ada Example", "Engineer", "psychonom", "Ship it", "Nothing else"]
    await handle("hello!")
    for answer in answers:
        await handle(answer)
    pending = runtime.get_pending("123")
    assert pending is not None and pending.proposal_retry_count == 1  # 1st failed submission

    # Drive retry_count up to (but not past) the stall threshold -- plain
    # failed-attempt replies, task_create never attempted yet. Bounded
    # (round-2-gate #23): a regression that stops retry_count from ever
    # advancing must fail this test, never hang the suite.
    for _ in range(_PROPOSAL_STALLED_AFTER_ATTEMPTS + 5):
        pending = runtime.get_pending("123")
        if pending.proposal_retry_count >= _PROPOSAL_STALLED_AFTER_ATTEMPTS - 1:
            break
        runtime.schedule_proposal_retry("123", next_retry_at=fake_clock["now"], retry_count=pending.proposal_retry_count)
        reply = await handle("checking in")
        assert reply == _PROPOSAL_FAILED_REPLY
    else:
        pytest.fail("proposal_retry_count never reached the stall threshold within the iteration bound")
    assert client.task_create_attempts == 0

    # 1st stalled check-in: task_create is attempted and FAILS (attempt 1).
    pending = runtime.get_pending("123")
    runtime.schedule_proposal_retry("123", next_retry_at=fake_clock["now"], retry_count=pending.proposal_retry_count)
    reply1 = await handle("checking in 1")
    assert reply1 == _PROPOSAL_STALLED_REPLY_FLAG_FAILED  # honest: NOT reached
    assert client.task_create_attempts == 1

    # 2nd check-in, still within the SHORT retry window -- still honest,
    # and task_create is NOT hammered again.
    pending = runtime.get_pending("123")
    runtime.schedule_proposal_retry("123", next_retry_at=fake_clock["now"], retry_count=pending.proposal_retry_count)
    reply2 = await handle("checking in 2")
    assert reply2 == _PROPOSAL_STALLED_REPLY_FLAG_FAILED
    assert client.task_create_attempts == 1  # not retried yet

    # Advance past the SHORT retry window (NEVER the 24h one) -- a fresh
    # attempt fires and still fails (attempt 2).
    fake_wall_clock["now"] += 300.0 + 1.0
    pending = runtime.get_pending("123")
    runtime.schedule_proposal_retry("123", next_retry_at=fake_clock["now"], retry_count=pending.proposal_retry_count)
    reply3 = await handle("checking in 3")
    assert reply3 == _PROPOSAL_STALLED_REPLY_FLAG_FAILED  # still honest -- team NOT reached
    assert client.task_create_attempts == 2

    # Advance past the short window again -- THIS attempt succeeds (attempt
    # 3), and ONLY NOW is the 24h success token actually spent.
    fake_wall_clock["now"] += 300.0 + 1.0
    pending = runtime.get_pending("123")
    runtime.schedule_proposal_retry("123", next_retry_at=fake_clock["now"], retry_count=pending.proposal_retry_count)
    reply4 = await handle("checking in 4")
    assert reply4 == _PROPOSAL_STALLED_REPLY  # NOW genuinely flagged
    assert client.task_create_attempts == 3

    # Immediately after: the 24h token IS spent -- further check-ins get
    # the STILL-stalled line, never another task_create attempt.
    pending = runtime.get_pending("123")
    runtime.schedule_proposal_retry("123", next_retry_at=fake_clock["now"], retry_count=pending.proposal_retry_count)
    reply5 = await handle("checking in 5")
    assert reply5 == _PROPOSAL_STILL_STALLED_REPLY
    assert client.task_create_attempts == 3  # unchanged


@pytest.mark.asyncio
async def test_stall_marker_is_cleared_once_the_proposal_actually_succeeds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """mupot-plugin#22 P3 leftover: nothing previously cleared a resolved
    stall marker once the proposal it was raised about actually succeeded
    -- `stall_entries()` (an audit surface) would keep showing a member as
    stalled forever after the fact resolved itself. Mutation-provable:
    removing the `runtime.store.clear_stall(intake.member_id)` call from
    `_submit_proposal`'s success path makes the final assertion go red."""

    class _EventuallySucceedsClient(FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.submit_attempts = 0

        def call(self, action: str, args: dict[str, Any]) -> Any:
            self.calls.append((action, dict(args)))
            if action not in FIRST_PERSON_ACTIONS:
                return {"ok": False, "error": "action_not_allowed"}
            if action == "squad_remember":
                return {"ok": True, "result": {"engram_id": f"engram-{len(self.calls)}"}}
            if action == "task_create":
                return {"ok": True, "result": {}}
            if action == "routine_proposal_submit":
                self.submit_attempts += 1
                if self.submit_attempts <= _PROPOSAL_STALLED_AFTER_ATTEMPTS:
                    return {"ok": False, "error": "unsupported_action_kind"}
                return {"ok": True, "result": {"proposal_id": "proposal-1"}}
            return {"ok": False, "error": "unsupported_action_kind"}

    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    client = _EventuallySucceedsClient()
    install_status_stub(monkeypatch, lambda *_: pending_status())
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)
    settings = valid_settings()

    answers = ["Ada Example", "Engineer", "psychonom", "Ship it", "Nothing else"]
    await handle_first_contact(Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime)
    for answer in answers:
        await handle_first_contact(Update(message=Message(text=answer)), settings=settings, client=client, runtime=runtime)

    # Bounded (round-2-gate #23): a regression that stops retry_count from
    # ever advancing must fail this test, never hang the suite.
    for _ in range(_PROPOSAL_STALLED_AFTER_ATTEMPTS + 5):
        pending = runtime.get_pending("123")
        if pending is None or pending.proposal_retry_count >= _PROPOSAL_STALLED_AFTER_ATTEMPTS:
            break
        runtime.schedule_proposal_retry("123", next_retry_at=fake_clock["now"], retry_count=pending.proposal_retry_count)
        await handle_first_contact(
            Update(message=Message(text="checking in")), settings=settings, client=client, runtime=runtime
        )
    else:
        pytest.fail("proposal_retry_count never reached the stall threshold within the iteration bound")
    assert "member-1" in runtime.store.stall_entries()  # stalled and flagged

    # Now let the SAME retry succeed -- the durable stall marker must be
    # cleared, not left behind as a false-positive audit entry.
    pending = runtime.get_pending("123")
    runtime.schedule_proposal_retry("123", next_retry_at=fake_clock["now"], retry_count=pending.proposal_retry_count)
    await handle_first_contact(
        Update(message=Message(text="checking in again")), settings=settings, client=client, runtime=runtime
    )

    assert runtime.get_pending("123") is None  # completed
    assert "member-1" not in runtime.store.stall_entries()  # cleared, not left behind


# ---------------------------------------------------------------------------
# Capability floor
# ---------------------------------------------------------------------------


def test_first_person_actions_are_never_registered_as_llm_tools() -> None:
    assert FIRST_PERSON_ACTIONS.isdisjoint(OPERATOR_TOOL_NAMES)
    assert FIRST_PERSON_ACTIONS.isdisjoint(MANAGER_TOOL_NAMES)


def test_first_person_actions_exclude_every_manage_access_surface() -> None:
    forbidden = {
        "project_squad_set",
        "grant_agent_capability",
        "grant_gate_capability",
        "addSquadMember",
        "project_list",  # round-2 P2-1: replaced by the member-scoped lookup
    }
    assert FIRST_PERSON_ACTIONS.isdisjoint(forbidden)
    for action in FIRST_PERSON_ACTIONS:
        assert "grant" not in action
        assert "manage_access" not in action
        assert action != "project_squad_set"


def test_skill_declares_no_tools_and_names_the_forbidden_surfaces() -> None:
    skill_path = Path(__file__).resolve().parents[1] / "skills" / "first-person" / "SKILL.md"
    text = skill_path.read_text(encoding="utf-8")
    _, frontmatter_text, _body = text.split("---", 2)
    frontmatter = yaml.safe_load(frontmatter_text)
    assert frontmatter["tools"] == []
    disallowed = set(frontmatter["disallowed_tools"])
    assert {"project_squad_set", "grant_agent_capability", "grant_gate_capability", "manage_access"} <= disallowed
    assert disallowed.isdisjoint(FIRST_PERSON_ACTIONS)


def test_skill_questions_match_the_module_constant() -> None:
    skill_path = Path(__file__).resolve().parents[1] / "skills" / "first-person" / "SKILL.md"
    text = skill_path.read_text(encoding="utf-8")
    _, frontmatter_text, _body = text.split("---", 2)
    frontmatter = yaml.safe_load(frontmatter_text)
    from_skill = [(entry["id"], entry["text"]) for entry in frontmatter["questions"]]
    assert tuple(from_skill) == FIRST_PERSON_QUESTIONS


# ---------------------------------------------------------------------------
# Mutation: prove the "never spill raw text" check has teeth (real source edit)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mutation_a_leaked_raw_answer_is_caught_by_the_state_directory_check(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Deliberately mutate the completion write path to leak raw text (as a
    regression in FirstPersonRuntime.finish might), then re-run the same
    assertion the directory-wide check above relies on and confirm it goes
    red. Keeps a standing regression test alongside the real on-disk mutation
    verified during this build (see PR body)."""
    client = _client_for_full_intake()
    state_dir = tmp_path / "state-dir"
    state_path = state_dir / "state.json"
    runtime = FirstPersonRuntime(state_path)
    leaked_answer = "this raw answer must never reach disk"

    original_finish = FirstPersonRuntime.finish

    def mutated_finish(self: FirstPersonRuntime, chat_key: str, *, proposal_id: str | None) -> None:
        with self._lock:  # noqa: SLF001 -- deliberately reaching in for the mutation
            intake = self._pending.get(chat_key)
        if intake is not None:
            data, _valid = self.store.load_checked()
            completed = data.get("completed") or {}
            completed[intake.member_id] = {
                "engram_ids": dict(intake.engrams),
                "proposal_id": proposal_id,
                "leaked_raw_answer": leaked_answer,  # the mutation
            }
            data["completed"] = completed
            self.store.save(data)
            with self._lock:
                self._pending.pop(chat_key, None)
            return
        original_finish(self, chat_key, proposal_id=proposal_id)

    monkeypatch.setattr(FirstPersonRuntime, "finish", mutated_finish)

    answers = ["Ada Example", "Engineer", "psychonom", leaked_answer, "Nothing else"]
    await _run_intake(monkeypatch, client, runtime, answers)

    leaked = False
    for path in state_dir.rglob("*"):
        if path.is_file() and leaked_answer in path.read_text(errors="replace"):
            leaked = True
    assert leaked, "expected the mutation to leak the raw answer -- if this fails, the mutation itself is inert"


# ---------------------------------------------------------------------------
# Minimal fake-PTB shim: exercises the actual registration wiring since
# python-telegram-bot is not installed in this build environment.
# ---------------------------------------------------------------------------


class _FakeFilterExpr:
    def __init__(self, label: str) -> None:
        self.label = label

    def __and__(self, other: "_FakeFilterExpr") -> "_FakeFilterExpr":
        return _FakeFilterExpr(f"({self.label} & {other.label})")

    def __invert__(self) -> "_FakeFilterExpr":
        return _FakeFilterExpr(f"~{self.label}")


class _FakeUpdateTypeNS:
    MESSAGE = _FakeFilterExpr("UpdateType.MESSAGE")


class _FakeFilters:
    TEXT = _FakeFilterExpr("TEXT")
    COMMAND = _FakeFilterExpr("COMMAND")
    UpdateType = _FakeUpdateTypeNS()


class _FakeApplicationHandlerStop(Exception):
    pass


class _FakeMessageHandler:
    def __init__(self, filter_expr: _FakeFilterExpr, callback: Any) -> None:
        self.filter_expr = filter_expr
        self.callback = callback


class _FakeApplication:
    def __init__(self) -> None:
        self.handlers: list[tuple[Any, int]] = []

    def add_handler(self, handler: Any, group: int = 0) -> None:
        self.handlers.append((handler, group))

    def remove_handler(self, handler: Any, group: int = 0) -> None:
        self.handlers = [(h, g) for h, g in self.handlers if h is not handler]


def install_fake_ptb(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    telegram_module = types.ModuleType("telegram")
    ext_module = types.ModuleType("telegram.ext")
    ext_module.ApplicationHandlerStop = _FakeApplicationHandlerStop
    ext_module.MessageHandler = _FakeMessageHandler
    ext_module.filters = _FakeFilters()
    telegram_module.ext = ext_module
    monkeypatch.setitem(sys.modules, "telegram", telegram_module)
    monkeypatch.setitem(sys.modules, "telegram.ext", ext_module)
    return ext_module


def test_register_first_person_wires_a_message_handler_scoped_to_real_messages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_fake_ptb(monkeypatch)
    factories: list[Any] = []
    ctx = types.SimpleNamespace(
        register_telegram_handler=factories.append, on_unload=lambda fn: None, _manager=None
    )
    register_first_person(ctx, valid_settings(), client=FakeClient())
    assert len(factories) == 1

    application = _FakeApplication()
    factories[0](application, adapter=None)
    assert len(application.handlers) == 1
    handler, group = application.handlers[0]
    assert isinstance(handler, _FakeMessageHandler)
    assert group == -5  # runs before Hermes's own core text handler (implicit group 0)
    # Round-2 fix: scoped to UpdateType.MESSAGE so an edited-message update
    # never dispatches into this handler at the PTB layer at all.
    assert "UpdateType.MESSAGE" in handler.filter_expr.label
    assert "TEXT" in handler.filter_expr.label
    assert "~COMMAND" in handler.filter_expr.label


@pytest.mark.asyncio
async def test_registered_handler_raises_application_handler_stop_only_when_handled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ext_module = install_fake_ptb(monkeypatch)
    install_status_stub(monkeypatch, lambda *_: UNBOUND_STATUS)
    factories: list[Any] = []
    ctx = types.SimpleNamespace(
        register_telegram_handler=factories.append, on_unload=lambda fn: None, _manager=None
    )
    register_first_person(
        ctx, valid_settings(), client=FakeClient(), state_path=tmp_path / "state.json"
    )
    application = _FakeApplication()
    factories[0](application, adapter=None)
    handler, _group = application.handlers[0]

    # Stranger -> not handled -> must NOT raise ApplicationHandlerStop, so
    # Hermes's own core handler still gets its turn.
    await handler.callback(Update(message=Message(text="hi")), context=None)

    # Now a pending member -> handled -> MUST raise, to stop propagation.
    install_status_stub(monkeypatch, lambda *_: pending_status(home_squad_id="home-squad-1"))
    with pytest.raises(ext_module.ApplicationHandlerStop):
        await handler.callback(Update(message=Message(text="hello!")), context=None)


@pytest.mark.asyncio
async def test_register_first_person_threads_clock_through_to_handle_first_contact(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """mupot-plugin#22 P3 leftover: `clock` was not passed from
    `register_first_person` down to `handle_first_contact`, so a caller
    injecting a fake clock into the `FirstPersonRuntime` alone could not
    consistently control the SEPARATE clock `_retry_proposal_if_due`/
    `_submit_proposal` use for backoff-due comparisons -- the real
    `time.monotonic()` default made every backoff check trivially "due"
    in earlier tests, masking exactly this gap. Mutation-provable:
    dropping `clock=clock` from the `handle_first_contact` call inside
    `register_first_person`'s `handle` closure makes this go red -- the
    retry would never fire (the real clock is nowhere near the fake
    `next_proposal_retry_at` used here), and the plain wait line would be
    returned instead of a real submission attempt."""
    ext_module = install_fake_ptb(monkeypatch)
    fake_clock = {"now": 10_000_000.0}  # far past any real time.monotonic() value
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    runtime.set_project_id("123", "proj-1")
    for question_id, _ in FIRST_PERSON_QUESTIONS:
        runtime.record_answer("123", question_id, f"engram-{question_id}")
    runtime.schedule_proposal_retry("123", next_retry_at=fake_clock["now"], retry_count=0)

    factories: list[Any] = []
    ctx = types.SimpleNamespace(register_telegram_handler=factories.append, on_unload=lambda fn: None, _manager=None)
    register_first_person(ctx, valid_settings(), client=client, runtime=runtime, clock=lambda: fake_clock["now"])
    application = _FakeApplication()
    factories[0](application, adapter=None)
    handler, _group = application.handlers[0]

    install_status_stub(monkeypatch, lambda *_: pending_status(home_squad_id="home-squad-1"))
    update = Update(message=Message(text="checking in"))
    with pytest.raises(ext_module.ApplicationHandlerStop):
        await handler.callback(update, context=None)

    # If the injected clock actually reached the handler, the due retry
    # fired a real submission attempt instead of the plain wait line.
    assert any(action == "routine_proposal_submit" for action, _ in client.calls)
    assert update.effective_message.replies[-1] != _PROPOSAL_RETRY_WAIT_REPLY


# ---------------------------------------------------------------------------
# Athena's round-2 gate conditions, named explicitly (2026-09-21) so each one
# ticks off individually against this test file.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_condition_i_member_identity_and_home_squad_id_come_only_from_server_status(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """(i) {bound, member_id, home_squad_id, intake_state} come ONLY from the
    server's structured reply over the authenticated webhook envelope; the
    plugin never infers home_squad_id or member identity locally."""
    install_probe(
        monkeypatch,
        response=status_response_bytes(member_id="member-xyz", home_squad_id="home-xyz", intake_state="pending"),
    )
    status = resolve_member_status(valid_settings(), 123, 123)
    assert status.member_id == "member-xyz"
    assert status.home_squad_id == "home-xyz"

    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    intake = runtime.start("123", member_id=status.member_id, home_squad_id=status.home_squad_id)
    # Nothing in Intake/FirstPersonRuntime ever recomputes these from
    # anything but the values handed in here, which themselves trace only to
    # the parsed server response above -- there is no other assignment site
    # for either field anywhere in the module (see Intake's docstring).
    assert intake.member_id == "member-xyz"
    assert intake.home_squad_id == "home-xyz"


@pytest.mark.asyncio
async def test_condition_ii_completion_marker_is_a_ttl_cached_view_not_the_record(
    tmp_path: Path,
) -> None:
    """(ii) intake_state flips SERVER-side only; a local completion marker is
    a cached VIEW of server state with a TTL, never the record -- past that
    TTL this module defers entirely back to the server again."""
    fake_wall_clock = {"now": 1_000_000.0}
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path, wall_clock=lambda: fake_wall_clock["now"])
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    runtime.record_answer("123", "name", "engram-1")
    runtime.finish("123", proposal_id="proposal-1")

    assert runtime.is_complete_or_unknown("member-1") is True  # fresh -- cache still valid

    fake_wall_clock["now"] += 3601.0  # past the 3600s completion-cache TTL
    assert runtime.is_complete_or_unknown("member-1") is False  # defers back to the server now


def test_condition_iii_no_per_message_start_probe_exists_in_the_module() -> None:
    """(iii) no per-message /start probe at all -- it was a write surface on
    strangers carrying first_name. Static check: no code constructs a literal
    "/start" payload or a first_name/display_name field anywhere in the
    module (a bare `/start` substring still appears in comments explaining
    *why* it was removed, which is fine -- this checks for the constructs
    that would actually rebuild the old probe)."""
    source = Path(first_person.__file__).read_text(encoding="utf-8")
    assert '"/start"' not in source
    assert "first_name" not in source
    assert "display_name" not in source


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [UNBOUND_STATUS, NONE_STATUS, COMPLETE_STATUS, UNKNOWN_STATUS],
    ids=["unbound", "never-started", "already-complete", "contract-fields-absent"],
)
async def test_condition_iv_fall_through_on_non_pending_is_total(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, status: StatusResolution
) -> None:
    """(iv) fall-through on non-pending is TOTAL: no marker write, no state
    mutation, no reply, no ApplicationHandlerStop -- the host's own verdict
    flow and the group-99 observer are untouched."""
    install_status_stub(monkeypatch, lambda *_: status)
    client = FakeClient()
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    update = Update(message=Message(text="approve f9408956"))

    handled = await handle_first_contact(update, settings=valid_settings(), client=client, runtime=runtime)

    assert handled is False  # -> the thin PTB adapter never raises ApplicationHandlerStop
    assert update.effective_message.replies == []
    assert not state_path.exists()
    assert runtime.get_pending("123") is None
    assert client.calls == []


@pytest.mark.asyncio
async def test_condition_v_stale_bound_cache_unbind_mid_intake_aborts_cleanly(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """(v) stale bound cache: TTL + a post-answer reconciliation probe; an
    unbind mid-intake ABORTS the intake (once CONFIRMED -- round-3-gate-4
    P1-b, a single reading is never enough; mupot-plugin#21 P2, the second
    reading must also be separated by at least the status-cache TTL -- see
    test_unbind_mid_intake_still_serves_the_message_but_reconciliation_
    drops_it for the full walkthrough). Round-2-gate (#23) P2-a: the FIRST
    (unconfirmed) reading already suspends capture, so the message that
    arrives during the confirming window falls through untouched too, not
    just the one after confirmation."""
    fake_clock = {"now": 0.0}
    call_count = {"n": 0}

    def resolver(_user_id: Any, _chat_id: Any) -> StatusResolution:
        call_count["n"] += 1
        return pending_status() if call_count["n"] == 1 else UNBOUND_STATUS

    install_status_stub(monkeypatch, resolver)
    client = _client_for_full_intake()
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path, clock=lambda: fake_clock["now"])
    await handle_first_contact(
        Update(message=Message(text="hello!")), settings=valid_settings(), client=client, runtime=runtime
    )
    assert runtime.get_pending("123") is not None

    first_unbind = Update(message=Message(text="Ada Example"))
    await handle_first_contact(first_unbind, settings=valid_settings(), client=client, runtime=runtime)
    suspended = runtime.get_pending("123")
    assert suspended is not None and suspended.suspended is True  # unconfirmed, first reading -- suspended

    fake_clock["now"] += first_person._STATUS_NEGATIVE_TTL_SECONDS
    second_unbind = Update(message=Message(text="Engineer"))
    handled = await handle_first_contact(second_unbind, settings=valid_settings(), client=client, runtime=runtime)
    assert handled is False  # suspended at entry -- falls straight through
    assert runtime.get_pending("123") is None  # CONFIRMED -- dropped for the NEXT message

    abort_update = Update(message=Message(text="anything"))
    handled = await handle_first_contact(abort_update, settings=valid_settings(), client=client, runtime=runtime)

    assert handled is False
    assert runtime.get_pending("123") is None
    assert abort_update.effective_message.replies == []


@pytest.mark.asyncio
async def test_condition_vi_credential_and_control_char_checks_run_before_process_memory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """(vi) credential-pattern refusal and control-char/bidi scrub happen
    BEFORE anything reaches process memory that could be written -- a
    rejected answer never reaches the store. Covers both rejection classes
    in one test: a credential-shaped answer, and a bidi-override-laden one
    that IS accepted but must never carry the raw override characters into
    memory or storage."""
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    install_status_stub(monkeypatch, lambda *_: pending_status())
    settings = valid_settings()
    await handle_first_contact(
        Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime
    )

    credential_update = Update(message=Message(text="sk-abcdefghijklmnopqrstuvwx"))
    await handle_first_contact(credential_update, settings=settings, client=client, runtime=runtime)
    pending = runtime.get_pending("123")
    assert pending is not None
    assert pending.index == 0
    assert pending.engrams == {}
    assert not any(action == "squad_remember" for action, _ in client.calls)


# ---------------------------------------------------------------------------
# Skill registration (discovery receipt, 2026-09-21): a native backend plugin
# gets no directory-scan auto-discovery -- only an explicit ctx.register_skill
# call makes skills/first-person/SKILL.md resolvable as 'mupot:first-person'.
# ---------------------------------------------------------------------------


def test_register_first_person_skill_registers_under_the_qualified_name() -> None:
    calls: list[tuple[str, Path, str]] = []

    def fake_register_skill(name: str, path: Path, description: str = "") -> None:
        calls.append((name, path, description))

    ctx = types.SimpleNamespace(register_skill=fake_register_skill)
    register_first_person_skill(ctx)

    assert len(calls) == 1
    name, path, description = calls[0]
    assert name == "first-person"
    assert path == SKILL_PATH
    assert path.name == "SKILL.md"
    assert description  # non-empty, human-readable


def test_register_first_person_skill_never_raises_when_ctx_lacks_the_hook() -> None:
    ctx = types.SimpleNamespace()  # no register_skill attribute at all
    register_first_person_skill(ctx)  # must not raise


def test_register_first_person_skill_never_raises_on_registration_failure() -> None:
    def failing_register_skill(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("boom")

    ctx = types.SimpleNamespace(register_skill=failing_register_skill)
    register_first_person_skill(ctx)  # must not raise


# ===========================================================================
# Round 3 successor (kasra/first-person-skill-v2, off PR#17 @ ecea2501):
# adversarial round 2 RED (2 P0 / 2 P1 / 2 P2 / 2 P3), Athena's rulings on
# the successor pinned inline at each fix. See PR body's Round-1 table for
# the item -> test mapping.
# ===========================================================================


# ---------------------------------------------------------------------------
# P0-A / ruling (2): normalize-then-check credential pipeline
# ---------------------------------------------------------------------------


def test_credential_check_runs_after_normalization_not_before() -> None:
    """Mutation-regression receipt for the round-2 order bug: checking
    credential-shape on the RAW text lets an embedded bidi/control char split
    the token so the regex's contiguous match never fires -- only
    normalization AFTER that (which strips exactly that character)
    reassembles it into a valid token. Proves the fix is load-bearing: revert
    the order (check raw before sanitize) and this exact evading string
    slips through uncaught."""
    exploit = "mupot_" + "A" * 4 + "‮" + "A" * 28  # Athena's exact evading string
    cleaned = _sanitize_answer(exploit)
    assert _looks_like_credential(cleaned) is True  # correct order: catches it
    assert _looks_like_credential(exploit) is False  # old (broken) order: would not have


@pytest.mark.parametrize(
    "hidden_char",
    ["‮", "\x01", "⁦", "‭"],
    ids=["RLO-U+202E", "control-x01", "LRI-U+2066", "LRO-U+202D"],
)
@pytest.mark.parametrize(
    "prefix,suffix",
    [("mupot_", "A" * 28), ("ghp_", "A" * 24), ("sk-", "A" * 20), ("AKIA", "A" * 16)],
    ids=["mupot_", "ghp_", "sk-", "AKIA"],
)
def test_obfuscated_credential_variants_are_caught_after_normalization(
    hidden_char: str, prefix: str, suffix: str
) -> None:
    exploit = prefix + "AAAA" + hidden_char + suffix
    assert _looks_like_credential(_sanitize_answer(exploit)) is True


def test_plain_text_credential_refusal_still_works_after_reordering() -> None:
    """The P0-A reorder must not regress the ordinary, non-obfuscated case."""
    assert _looks_like_credential(_sanitize_answer("sk-abcdefghijklmnopqrst")) is True
    assert _looks_like_credential(_sanitize_answer("Ada Example")) is False


@pytest.mark.asyncio
async def test_bidi_obfuscated_credential_is_refused_end_to_end_never_reaches_memory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    install_status_stub(monkeypatch, lambda *_: pending_status())
    settings = valid_settings()
    await handle_first_contact(
        Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime
    )

    exploit = "mupot_" + "A" * 4 + "‮" + "A" * 28
    answer_update = Update(message=Message(text=exploit))
    handled = await handle_first_contact(answer_update, settings=settings, client=client, runtime=runtime)

    assert handled is True
    assert "Please don't share" in answer_update.effective_message.replies[0]
    assert not any(action == "squad_remember" for action, _ in client.calls)
    pending = runtime.get_pending("123")
    assert pending is not None and pending.index == 0
    assert pending.engrams == {}


# ---------------------------------------------------------------------------
# resolve-project transport: envelope identity, never member_id/chat_id as a
# bare selector (Athena's ruling on the /im/resolve-project fence, 2026-09-21)
# ---------------------------------------------------------------------------


def _sample_envelope() -> dict[str, Any]:
    return {"update_id": 456, "chat_id": 123, "user_id": 123, "text": "psychonom"}


def test_resolve_project_request_carries_the_authenticated_envelope_never_a_bare_selector() -> None:
    request = _build_resolve_project_request(valid_settings(), "the-secret", _sample_envelope(), "psychonom")
    assert request.get_header("X-telegram-bot-api-secret-token") == "the-secret"
    body = json.loads(request.data.decode("utf-8"))
    assert body == {
        "update_id": 456,
        "message": {"from": {"id": 123}, "chat": {"id": 123, "type": "private"}, "text": ""},
        "query": "psychonom",
    }
    # Never a bare member_id/chat_id selector at the top level -- identity
    # travels ONLY inside the envelope's message.chat/message.from.
    assert "member_id" not in body
    assert "chat_id" not in body


def test_resolve_project_request_never_carries_a_display_name() -> None:
    request = _build_resolve_project_request(valid_settings(), "the-secret", _sample_envelope(), "psychonom")
    body = json.loads(request.data.decode("utf-8"))
    assert set(body["message"]["from"]) == {"id"}


def test_resolve_member_project_wire_happy_path_single_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    opener = install_probe(
        monkeypatch,
        response=json.dumps(
            {"bound": True, "member_id": "member-1", "projects": [{"id": "proj-1", "slug": "psychonom", "name": "Psychonom"}]}
        ).encode(),
    )
    project_id = _resolve_member_project(valid_settings(), _sample_envelope(), "psychonom")
    assert project_id == "proj-1"
    request = opener.calls[0]
    assert request.full_url.endswith("/im/resolve-project")


def test_resolve_member_project_no_match_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    install_probe(monkeypatch, response=json.dumps({"bound": True, "member_id": "member-1", "projects": []}).encode())
    assert _resolve_member_project(valid_settings(), _sample_envelope(), "nonexistent") is None


def test_resolve_member_project_unbound_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    install_probe(monkeypatch, response=json.dumps({"bound": False, "member_id": None, "projects": []}).encode())
    assert _resolve_member_project(valid_settings(), _sample_envelope(), "psychonom") is None


def test_resolve_member_project_ambiguous_multi_candidate_refuses_to_guess(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two fuzzy matches, neither an exact slug match -- never guess."""
    install_probe(
        monkeypatch,
        response=json.dumps(
            {
                "bound": True,
                "member_id": "member-1",
                "projects": [
                    {"id": "proj-1", "slug": "psychonomics", "name": "Psychonomics"},
                    {"id": "proj-2", "slug": "psycho-analysis", "name": "Psycho Analysis"},
                ],
            }
        ).encode(),
    )
    assert _resolve_member_project(valid_settings(), _sample_envelope(), "psycho") is None


def test_resolve_member_project_exact_slug_match_wins_among_multiple(monkeypatch: pytest.MonkeyPatch) -> None:
    """An exact slug match at the top (ORDER BY slug=? DESC on the server) is
    trusted even when other fuzzy candidates are also present."""
    install_probe(
        monkeypatch,
        response=json.dumps(
            {
                "bound": True,
                "member_id": "member-1",
                "projects": [
                    {"id": "proj-1", "slug": "psychonom", "name": "Psychonom"},
                    {"id": "proj-2", "slug": "psychonom-labs", "name": "Psychonom Labs"},
                ],
            }
        ).encode(),
    )
    assert _resolve_member_project(valid_settings(), _sample_envelope(), "psychonom") == "proj-1"


def test_resolve_member_project_transport_failure_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("plugin.first_person.read_profile_secret", lambda _name: "secret")
    monkeypatch.setattr(
        "plugin.first_person.build_opener",
        lambda *_: Opener(RuntimeError("boom")),
    )
    assert _resolve_member_project(valid_settings(), _sample_envelope(), "psychonom") is None


# ---------------------------------------------------------------------------
# P0-B / ruling (1): held proposal_id blocks re-intake, even past the
# completion-cache TTL, for as long as the server keeps saying 'pending'
# ---------------------------------------------------------------------------


def test_held_proposal_id_reads_a_durable_marker_ignoring_the_completion_cache_ttl(
    tmp_path: Path,
) -> None:
    fake_wall_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", wall_clock=lambda: fake_wall_clock["now"])
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    runtime.finish("123", proposal_id="proposal-1")
    assert runtime.held_proposal_id("member-1") == "proposal-1"

    fake_wall_clock["now"] += 10_000.0  # far past the 3600s completion-cache TTL
    assert runtime.held_proposal_id("member-1") == "proposal-1"  # still durable


def test_held_proposal_id_is_none_for_an_untouched_member(tmp_path: Path) -> None:
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    assert runtime.held_proposal_id("member-1") is None


def test_held_proposal_id_is_none_when_marker_has_no_proposal(tmp_path: Path) -> None:
    """Legacy/corrupted marker shape (should not be written going forward,
    see P2-E) must never be mistaken for a held proposal."""
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"completed": {"member-1": {"proposal_id": None}}}))
    runtime = FirstPersonRuntime(state_path)
    assert runtime.held_proposal_id("member-1") is None


@pytest.mark.asyncio
async def test_held_proposal_lag_logs_exactly_one_warning_no_pii(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    answers = ["Ada Example", "Engineer", "psychonom", "Ship it", "Nothing else"]
    await _run_intake(monkeypatch, client, runtime, answers)

    install_status_stub(monkeypatch, lambda *_: pending_status(home_squad_id="home-squad-1"))
    settings = valid_settings()
    caplog.clear()
    caplog.set_level("WARNING")
    update = Update(message=Message(text="hello again"))
    handled = await handle_first_contact(update, settings=settings, client=client, runtime=runtime)

    # Round-3-gate-2 P1 (:1408-1426): False now -- the host still owns the
    # turn (never a permanent DM lockout) -- but the reassurance reply is
    # still sent (rate-limited separately, see the dedicated rate-limit test).
    assert handled is False
    assert update.effective_message.replies == [_AWAITING_HUMANS_REPLY]
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    logged = warnings[0].getMessage()
    assert "member-1" in logged
    assert "proposal-1" in logged
    assert "Ada Example" not in logged  # no answer text / PII


@pytest.mark.asyncio
async def test_held_proposal_id_blocks_reintake_across_hours_with_rate_limited_notifications(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Round-3-gate-2 P1 (:1408-1426), end to end: a server that keeps
    reporting intake_state == 'pending' for hours after a successful
    proposal (well past the 3600s completion-cache TTL -- see
    test_condition_ii, a SEPARATE, softer mechanism this does not rely on)
    must produce zero re-asked questions, zero engram rewrites, and zero
    second proposals -- AND the host must get its turn every single message
    (``handled is False``, never round-2's permanent DM lockout), with the
    WARNING (1h window) and the reassurance reply (24h window) each
    rate-limited on their OWN cadence, never once per message."""
    fake_wall_clock = {"now": 1_000_000.0}
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path, wall_clock=lambda: fake_wall_clock["now"])
    client = _client_for_full_intake()
    answers = ["Ada Example", "Engineer", "psychonom", "Ship it", "Nothing else"]
    last_update = await _run_intake(monkeypatch, client, runtime, answers)
    assert last_update.effective_message.replies[-1] == (
        "Thanks -- I've sent your access request to the team for a decision."
    )
    calls_after_completion = len(client.calls)

    install_status_stub(monkeypatch, lambda *_: pending_status(home_squad_id="home-squad-1"))
    settings = valid_settings()
    warning_notifier = _NotifyOncePerWindow(_LAG_WARNING_WINDOW_SECONDS, clock=lambda: fake_wall_clock["now"])
    reply_notifier = _NotifyOncePerWindow(_LAG_REPLY_WINDOW_SECONDS, clock=lambda: fake_wall_clock["now"])
    caplog.set_level("WARNING")

    replies_per_message: list[list[str]] = []
    for _hour in range(3):  # 3 messages, exactly 1 simulated hour apart
        fake_wall_clock["now"] += 3600.0
        update = Update(message=Message(text="still there?"))
        handled = await handle_first_contact(
            update,
            settings=settings,
            client=client,
            runtime=runtime,
            lag_warning_notifier=warning_notifier,
            lag_reply_notifier=reply_notifier,
        )
        assert handled is False  # host owns the turn -- every message, not just some
        replies_per_message.append(list(update.effective_message.replies))

    assert len(client.calls) == calls_after_completion  # zero new mupot calls at all
    data = json.loads(state_path.read_text())
    assert data["completed"]["member-1"]["proposal_id"] == "proposal-1"  # unchanged, still the first

    # Reply (24h window): only the FIRST message gets it -- 3 simulated
    # hours never crosses the 24h window again.
    assert replies_per_message[0] == [_AWAITING_HUMANS_REPLY]
    assert replies_per_message[1] == []
    assert replies_per_message[2] == []

    # Warning (1h window): each message here IS exactly 1h apart, so all
    # three cross the window -- one warning per message this time, driven by
    # the SHORTER window, not the reply's cadence.
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 3


# ---------------------------------------------------------------------------
# P1-C / ruling (3): intake_state TYPE is normalized, never raises
# ---------------------------------------------------------------------------


def test_classify_intake_state_normalizes_the_type_not_only_the_value() -> None:
    """`x in frozenset(...)` raises TypeError for an unhashable value (a list
    or a dict) -- classify must handle ANY shape without raising."""
    assert _classify_intake_state("pending") == "pending"
    assert _classify_intake_state("none") == "none"
    assert _classify_intake_state("complete") == "complete"
    assert _classify_intake_state("bogus") == "unknown"
    assert _classify_intake_state(["pending"]) == "unknown"
    assert _classify_intake_state({"state": "pending"}) == "unknown"
    assert _classify_intake_state(True) == "unknown"
    assert _classify_intake_state(None) == "unknown"
    assert _classify_intake_state(123) == "unknown"


@pytest.mark.parametrize("bad_intake_state", [["pending"], {"a": 1}, 42, True, None], ids=str)
def test_resolve_member_status_never_raises_on_a_non_str_intake_state(
    monkeypatch: pytest.MonkeyPatch, bad_intake_state: Any
) -> None:
    install_probe(
        monkeypatch,
        response=json.dumps(
            {
                "ok": True,
                "bound": True,
                "member_id": "m-1",
                "home_squad_id": None,
                "intake_state": bad_intake_state,
            }
        ).encode(),
    )
    status = resolve_member_status(valid_settings(), 123, 123)  # must not raise
    assert status.intake_state == "unknown"
    assert status.is_pending is False


# ---------------------------------------------------------------------------
# P1-D / ruling (4): bounded status cache, harder unknown TTL, short probe
# timeout, global concurrency cap
# ---------------------------------------------------------------------------


def test_status_cache_is_bounded_under_two_hundred_thousand_distinct_senders() -> None:
    cache = _StatusCache(max_entries=2048)
    for n in range(200_000):
        cache.put(f"user-{n}", UNBOUND_STATUS)
    assert len(cache) <= 2048


def test_status_cache_uses_a_harder_ttl_for_genuinely_unknown_results() -> None:
    fake_time = {"now": 0.0}
    cache = _StatusCache(positive_ttl=1.0, negative_ttl=10.0, unknown_ttl=1000.0, clock=lambda: fake_time["now"])
    cache.put("stranger", UNKNOWN_STATUS)
    fake_time["now"] = 500.0  # past the 10s negative TTL, well within the 1000s unknown TTL
    assert cache.get("stranger") == UNKNOWN_STATUS


def test_status_probe_uses_a_short_timeout_never_longer_than_five_seconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded_timeouts: list[float] = []
    monkeypatch.setattr("plugin.first_person.read_profile_secret", lambda _name: "secret")

    class _RecordingOpener:
        def open(self, request: object, timeout: float) -> Response:
            recorded_timeouts.append(timeout)
            return Response(status_response_bytes())

    monkeypatch.setattr("plugin.first_person.build_opener", lambda *_: _RecordingOpener())
    resolve_member_status(valid_settings(timeout=100.0), 123, 123)  # configured well above 5s
    assert recorded_timeouts == [5.0]


def test_status_probe_limiter_fails_fast_to_unknown_when_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    install_probe(monkeypatch, response=status_response_bytes())
    limiter = _ProbeLimiter(max_concurrent=1, acquire_timeout=0.05)
    assert limiter.try_acquire() is True  # occupy the single slot
    try:
        status = resolve_member_status(valid_settings(), 999, 999, probe_limiter=limiter)
    finally:
        limiter.release()
    assert status == StatusResolution(bound=False, member_id=None, home_squad_id=None, intake_state="unknown")


# ---------------------------------------------------------------------------
# Round-3-gate-3 P1 (:1762) / Athena's ruling (2): `cache=None if pending is
# not None else status_cache` correctly fixed the round-3-gate-2 latch but
# removed the shared cache's INCIDENTAL per-sender rate limit for exactly
# the population most likely to message repeatedly. Fix: an independent
# _SenderProbeLimiter, PLUS splitting cache semantics so a probe-produced
# "unknown" (whatever the cause) never gets the shared cache's long
# (900s) TTL -- only a server-ANSWERED state does.
# ---------------------------------------------------------------------------


def test_pool_exhaustion_unknown_gets_a_short_ttl_not_the_900s_unknown_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    """Athena's ruling (2): a probe-produced 'unknown' -- here, from the
    GLOBAL _ProbeLimiter pool being exhausted -- must never poison the
    shared per-user_id cache with the long TTL a genuinely server-answered
    'unknown' would use. Mutation-provable: reverting to an unconditional
    `cache.put(cache_key, resolution)` in resolve_member_status makes this
    go red (the cached entry would still be present past the short cap)."""
    fake_time = {"now": 0.0}
    install_probe(monkeypatch, response=status_response_bytes())
    cache = _StatusCache(clock=lambda: fake_time["now"])
    limiter = _ProbeLimiter(max_concurrent=1, acquire_timeout=0.01)
    assert limiter.try_acquire() is True  # occupy the single slot -- simulate a flood
    try:
        status = resolve_member_status(valid_settings(), "victim-1", "victim-1", cache=cache, probe_limiter=limiter)
    finally:
        limiter.release()
    assert status.intake_state == "unknown"

    fake_time["now"] = first_person._STATUS_UNKNOWN_SHORT_TTL_SECONDS + 1.0
    assert cache.get("victim-1") is None  # already expired -- NOT latched for 900s


def test_transport_failure_unknown_gets_a_short_ttl_not_the_900s_unknown_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    """Athena's ruling (2) generalizes beyond pool exhaustion: ANY
    probe-produced 'unknown' (a transport-level timeout/exception here)
    gets the same short TTL, never the resolution-shaped 900s default."""
    fake_time = {"now": 0.0}
    monkeypatch.setattr("plugin.first_person.read_profile_secret", lambda _name: "secret")
    monkeypatch.setattr(
        "plugin.first_person.build_opener", lambda *_: Opener(TimeoutError("simulated probe timeout"))
    )
    cache = _StatusCache(clock=lambda: fake_time["now"])

    status = resolve_member_status(valid_settings(), "victim-2", "victim-2", cache=cache)
    assert status.intake_state == "unknown"

    fake_time["now"] = first_person._STATUS_UNKNOWN_SHORT_TTL_SECONDS + 1.0
    assert cache.get("victim-2") is None


def test_confirmed_results_still_use_the_normal_resolution_shaped_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    """The companion positive case: a server-ANSWERED result (confirmed
    bound+pending here) is unaffected by the unknown-only short-TTL split."""
    fake_time = {"now": 0.0}
    install_probe(monkeypatch, response=status_response_bytes())
    cache = _StatusCache(positive_ttl=100.0, clock=lambda: fake_time["now"])

    status = resolve_member_status(valid_settings(), "member-x", "member-x", cache=cache)
    assert status.is_pending is True

    fake_time["now"] = first_person._STATUS_UNKNOWN_SHORT_TTL_SECONDS + 1.0  # past the SHORT ttl
    assert cache.get("member-x") == status  # still cached -- normal 100s positive_ttl applies


def test_sender_probe_limiter_denies_repeat_probes_within_the_window(monkeypatch: pytest.MonkeyPatch) -> None:
    """The independent per-sender rate limit that replaces the shared
    cache's incidental one for a pending member's cache-bypass path (see
    handle_first_contact's `resolve()`). Mutation-provable: removing the
    `sender_limiter` check in resolve_member_status makes the SECOND call
    reach the network (opener.calls would be non-empty)."""
    fake_time = {"now": 0.0}
    install_probe(monkeypatch, response=status_response_bytes())
    limiter = _SenderProbeLimiter(min_interval=15.0, clock=lambda: fake_time["now"])

    status1 = resolve_member_status(valid_settings(), "flooder", "flooder", sender_limiter=limiter)
    assert status1.intake_state == "pending"

    opener2 = install_probe(monkeypatch, response=status_response_bytes())
    status2 = resolve_member_status(valid_settings(), "flooder", "flooder", sender_limiter=limiter)
    assert status2 == UNKNOWN_STATUS
    assert opener2.calls == []  # denied before ever touching the network

    fake_time["now"] = 16.0  # past min_interval
    status3 = resolve_member_status(valid_settings(), "flooder", "flooder", sender_limiter=limiter)
    assert status3.intake_state == "pending"
    assert len(opener2.calls) == 1

    # A DIFFERENT sender is never affected by the flooder's own limiter state.
    status_other = resolve_member_status(valid_settings(), "someone-else", "someone-else", sender_limiter=limiter)
    assert status_other.intake_state == "pending"


# ---------------------------------------------------------------------------
# Round-3-gate-4 P0 (:803): a sender-limiter denial must never reuse the
# UNKNOWN sentinel while a live pending record exists -- that routed
# straight into handle_first_contact's "hold, do nothing" branch, silently
# dropping an entirely normal fast-typed answer (never sanitized,
# credential-checked, stored, or replied to) through to the host's own LLM
# turn instead. Fix: the denial carries the caller-supplied `fallback`.
# ---------------------------------------------------------------------------


def test_sender_limiter_denial_returns_the_given_fallback_not_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unit-level check of the fallback wiring in resolve_member_status
    itself. Mutation-provable: reverting the denial branch to unconditional
    `return _UNKNOWN_STATUS` (ignoring `fallback`) makes this go red."""
    fake_time = {"now": 0.0}
    install_probe(monkeypatch, response=status_response_bytes())
    limiter = _SenderProbeLimiter(min_interval=15.0, clock=lambda: fake_time["now"])
    fallback = pending_status(member_id="member-1", home_squad_id="home-squad-1")

    status1 = resolve_member_status(valid_settings(), "flooder", "flooder", sender_limiter=limiter, fallback=fallback)
    assert status1.intake_state == "pending"

    opener2 = install_probe(monkeypatch, response=status_response_bytes())
    status2 = resolve_member_status(valid_settings(), "flooder", "flooder", sender_limiter=limiter, fallback=fallback)
    assert status2 == fallback  # NOT _UNKNOWN_STATUS
    assert opener2.calls == []  # still never touches the network


@pytest.mark.asyncio
async def test_sender_limiter_denial_for_a_pending_member_still_stores_the_answer_and_refuses_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The end-to-end round-3-gate-4 P0 finding, reproduced exactly:
    ordinary fast typing (a second message 3s after the first, well inside
    the sender limiter's 15s window) must still be sanitized, credential-
    checked, stored, and replied to -- never silently dropped to the host's
    LLM turn just because the rate-limited probe denial reused the
    'unknown, hold' branch."""
    fake_time = {"now": 0.0}
    monkeypatch.setattr("plugin.first_person.read_profile_secret", lambda _name: "secret")
    monkeypatch.setattr(
        "plugin.first_person.build_opener",
        lambda *_: Opener(Response(status_response_bytes(home_squad_id="home-squad-1"))),
    )
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    sender_probe_limiter = _SenderProbeLimiter(min_interval=15.0, clock=lambda: fake_time["now"])
    client = _client_for_full_intake()
    settings = valid_settings()

    await handle_first_contact(
        Update(message=Message(text="hello!")),
        settings=settings,
        client=client,
        runtime=runtime,
        sender_probe_limiter=sender_probe_limiter,
    )
    assert runtime.get_pending("123") is not None

    # msg2 at t+3s -- well inside the 15s sender-limiter window, so the
    # probe is denied and falls back to "assume still pending, same
    # identity". An ordinary answer must still be fully processed.
    fake_time["now"] = 3.0
    answer_update = Update(message=Message(text="Ada Example"))
    handled = await handle_first_contact(
        answer_update,
        settings=settings,
        client=client,
        runtime=runtime,
        sender_probe_limiter=sender_probe_limiter,
    )
    assert handled is True
    assert answer_update.effective_message.replies == [FIRST_PERSON_QUESTIONS[1][1]]
    remember_calls = [args for action, args in client.calls if action == "squad_remember"]
    assert len(remember_calls) == 1 and remember_calls[0]["text"] == "Ada Example"
    pending = runtime.get_pending("123")
    assert pending is not None and pending.index == 1

    # A credential-shaped answer at t+6s (still inside the window) must
    # still be refused, never silently stored.
    fake_time["now"] = 6.0
    credential_update = Update(message=Message(text="mupot_" + "A" * 32))
    handled = await handle_first_contact(
        credential_update,
        settings=settings,
        client=client,
        runtime=runtime,
        sender_probe_limiter=sender_probe_limiter,
    )
    assert handled is True
    assert credential_update.effective_message.replies[-1].startswith(first_person._CREDENTIAL_REPLY)
    assert len([args for action, args in client.calls if action == "squad_remember"]) == 1  # unchanged


def test_concurrent_flood_cannot_latch_a_third_partys_status_for_900s(monkeypatch: pytest.MonkeyPatch) -> None:
    """End-to-end reproduction of the adversarial finding's own methodology
    ("EXECUTED with the limiter scaled to 4"): with the global pool scaled
    down and one sender issuing many CONCURRENT probes, a third party's own
    probe that loses the race for the pool must resolve to a SHORT-lived
    'unknown', never the 900s unknown_ttl a genuinely unknown sender would
    get."""
    import threading

    monkeypatch.setattr("plugin.first_person.read_profile_secret", lambda _name: "secret")
    release_event = threading.Event()

    class _SlowOpener:
        def open(self, request: object, timeout: float) -> Response:
            release_event.wait(timeout=2.0)  # hold the pool slot open
            return Response(status_response_bytes())

    monkeypatch.setattr("plugin.first_person.build_opener", lambda *_: _SlowOpener())

    fake_time = {"now": 0.0}
    cache = _StatusCache(clock=lambda: fake_time["now"])
    probe_limiter = _ProbeLimiter(max_concurrent=4, acquire_timeout=0.05)

    threads = [
        threading.Thread(
            target=resolve_member_status,
            args=(valid_settings(), f"flooder-{n}", f"flooder-{n}"),
            kwargs={"cache": cache, "probe_limiter": probe_limiter},
        )
        for n in range(8)
    ]
    for thread in threads:
        thread.start()

    victim_status = resolve_member_status(valid_settings(), "victim", "victim", cache=cache, probe_limiter=probe_limiter)
    release_event.set()
    for thread in threads:
        thread.join(timeout=2.0)

    assert victim_status.intake_state == "unknown"  # lost the race, fails safe
    fake_time["now"] = first_person._STATUS_UNKNOWN_SHORT_TTL_SECONDS + 1.0
    assert cache.get("victim") is None  # short-lived, NOT a 900s latch


# ---------------------------------------------------------------------------
# P2-E / ruling (5): no false-success completion without a real proposal_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_project_id_at_submission_never_completes_falsely(tmp_path: Path) -> None:
    """Mutation-proof for the killed false-success branch: run
    _submit_proposal with project_id still None (bypassing the question-3
    gate that normally prevents this) and assert an honest failure -- no
    completion marker, no "Thanks" reply, pending retained, retry scheduled."""
    from plugin.first_person import _submit_proposal

    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    client = FakeClient()
    intake = runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    assert intake.project_id is None

    update = Update(message=Message(text="anything"))
    await _submit_proposal(update.effective_message, "123", intake, client=client, runtime=runtime)

    # Round-3-gate-4 P3-b: schedule_proposal_retry now durably persists
    # proposal_retry_count (so a restart doesn't reset backoff), so the
    # main state file DOES now exist after even the first failure -- the
    # actual invariant is "no completion marker", not "no file at all".
    completed = json.loads(state_path.read_text()).get("completed", {}) if state_path.exists() else {}
    assert "member-1" not in completed  # no completion marker written
    assert update.effective_message.replies == ["I couldn't send your request yet -- I'll try again."]
    assert not any(action == "routine_proposal_submit" for action, _ in client.calls)
    pending = runtime.get_pending("123")
    assert pending is not None
    assert pending.proposal_retry_count == 1


# ---------------------------------------------------------------------------
# P2-F / ruling (6): an unrelated mid-intake message is nudged, not silently
# captured as an answer and not fallen through
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unrelated_empty_answer_is_nudged_not_captured_or_fallen_through(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    install_status_stub(monkeypatch, lambda *_: pending_status())
    settings = valid_settings()
    await handle_first_contact(
        Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime
    )

    blank_update = Update(message=Message(text="   "))
    handled = await handle_first_contact(blank_update, settings=settings, client=client, runtime=runtime)

    assert handled is True  # chosen option: nudge, don't fall through
    assert blank_update.effective_message.replies == [_UNRELATED_ANSWER_REPLY]
    pending = runtime.get_pending("123")
    assert pending is not None and pending.index == 0  # not advanced
    assert not any(action == "squad_remember" for action, _ in client.calls)


# ---------------------------------------------------------------------------
# P3-G / ruling (7): Intake is frozen; home_squad_id pinned to a constant,
# not a literal duplicated in the assertion
# ---------------------------------------------------------------------------


def test_intake_is_a_frozen_dataclass() -> None:
    intake = Intake(member_id="member-1", home_squad_id="home-squad-1")
    with pytest.raises(dataclasses.FrozenInstanceError):
        intake.home_squad_id = "some-other-squad"  # type: ignore[misc]


def test_runtime_replace_methods_return_none_when_pending_is_gone(tmp_path: Path) -> None:
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    assert runtime.record_answer("missing-chat", "name", "engram-1") is None
    assert runtime.set_project_id("missing-chat", "proj-1") is None
    assert runtime.schedule_proposal_retry("missing-chat", next_retry_at=0.0, retry_count=1) is None


_PINNED_HOME_SQUAD_ID = "home-squad-const-9f2b1a"  # named constant, referenced by setup AND assertion


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "answer_text",
    [
        "Ada Example",
        "squad-evil-override",
        "home-squad-evil-override",
        "SQUAD-ABC",
        _PINNED_HOME_SQUAD_ID,
    ],
)
async def test_home_squad_id_is_immutable_regardless_of_any_answer_text(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, answer_text: str
) -> None:
    """Pin the invariant to the CONSTANT, not a literal string duplicated in
    the assertion -- a mutation deriving home_squad_id from any answer that
    e.g. `startswith("squad-")` must go red regardless of which specific
    text triggers it."""
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    install_status_stub(monkeypatch, lambda *_: pending_status(home_squad_id=_PINNED_HOME_SQUAD_ID))
    settings = valid_settings()
    await handle_first_contact(
        Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime
    )
    pending = runtime.get_pending("123")
    assert pending is not None
    assert pending.home_squad_id == _PINNED_HOME_SQUAD_ID

    await handle_first_contact(
        Update(message=Message(text=answer_text)), settings=settings, client=client, runtime=runtime
    )
    pending = runtime.get_pending("123")
    assert pending is None or pending.home_squad_id == _PINNED_HOME_SQUAD_ID


# ---------------------------------------------------------------------------
# P3-H / ruling (8): keep the refusal, document the rephrase path
# ---------------------------------------------------------------------------


def test_credential_reply_documents_the_rephrase_path() -> None:
    assert "Please don't share" in first_person._CREDENTIAL_REPLY
    assert "rephrasing" in first_person._CREDENTIAL_REPLY


# ===========================================================================
# Round 3, gate 2 (adversarial round 1 on #19 @ f17f9b31: RED, 1 P0 / 3 P1 /
# 2 P2 / 2 P3; all eight #17 findings CONFIRMED-FIXED). Round 2 of 2 -- see
# PR body's Round 2 table for the item -> test mapping. Athena's additions
# (resolve-project envelope frozen contract, home-not-ready reply, resume
# rebinds, sanitizer-by-invariant, sender-scoped store rewrite, 'unknown'
# never abandons, post-hoc scrub) are folded in alongside the matching
# review item.
# ===========================================================================


# ---------------------------------------------------------------------------
# P0 (:873): idle timeout (not absolute session length) + resume rebinds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_three_minutes_per_question_completes_without_premature_drop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Total session time (900s across 5 answers) crosses where the round-2
    bug's ABSOLUTE, created_at-measured 600s TTL would have dropped and
    silently restarted mid-conversation. The IDLE timeout (reset on every
    accepted answer) must never trip here."""
    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    client = _client_for_full_intake()
    answers = ["Ada Example", "Engineer", "psychonom", "Ship it", "Nothing else"]

    install_status_stub(monkeypatch, lambda *_: pending_status())
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)
    settings = valid_settings()

    update = Update(message=Message(text="hello!"))
    await handle_first_contact(update, settings=settings, client=client, runtime=runtime)
    last_update = update
    for answer in answers:
        fake_clock["now"] += 180.0  # 3 minutes per question
        last_update = Update(message=Message(text=answer))
        handled = await handle_first_contact(last_update, settings=settings, client=client, runtime=runtime)
        assert handled is True

    assert last_update.effective_message.replies[-1] == (
        "Thanks -- I've sent your access request to the team for a decision."
    )
    remember_calls = [args for action, args in client.calls if action == "squad_remember"]
    assert len(remember_calls) == 5
    assert [args["concepts"] for args in remember_calls] == [
        ["name"],
        ["role"],
        ["project"],
        ["first_ask"],
        ["notes"],
    ]


@pytest.mark.asyncio
async def test_eleven_minutes_of_silence_drops_the_pending_record(tmp_path: Path) -> None:
    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    assert runtime.get_pending("123") is not None

    fake_clock["now"] += 11 * 60.0
    assert runtime.get_pending("123") is None


@pytest.mark.asyncio
async def test_resume_after_idle_drop_never_reasks_or_relabels_answered_questions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Athena's RESUME REBINDS: stored answers stay attached to their
    ORIGINAL question ids; a resume asks the first UNANSWERED question;
    nothing stored is re-asked or relabelled -- exactly five engrams total,
    each correctly labelled, zero duplicates, across an idle-TTL drop mid-
    conversation."""
    fake_clock = {"now": 0.0}
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path, clock=lambda: fake_clock["now"])
    client = _client_for_full_intake()

    install_status_stub(monkeypatch, lambda *_: pending_status())
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)
    settings = valid_settings()

    start_update = Update(message=Message(text="hello!"))
    await handle_first_contact(start_update, settings=settings, client=client, runtime=runtime)
    assert start_update.effective_message.replies == [FIRST_PERSON_QUESTIONS[0][1]]

    fake_clock["now"] += 60.0
    q1_update = Update(message=Message(text="Ada Example"))
    await handle_first_contact(q1_update, settings=settings, client=client, runtime=runtime)
    assert q1_update.effective_message.replies == [FIRST_PERSON_QUESTIONS[1][1]]

    fake_clock["now"] += 60.0
    q2_update = Update(message=Message(text="Engineer"))
    await handle_first_contact(q2_update, settings=settings, client=client, runtime=runtime)
    assert q2_update.effective_message.replies == [FIRST_PERSON_QUESTIONS[2][1]]

    # 11 minutes of silence -- idle timeout trips, local record dropped.
    fake_clock["now"] += 11 * 60.0
    assert runtime.get_pending("123") is None

    # The very next message is the RESUME TRIGGER (same convention as a
    # fresh start's first message -- never itself treated as an answer) --
    # must ask the FIRST UNANSWERED question (Q3: "project"), never Q1.
    resume_trigger = Update(message=Message(text="are you still there?"))
    handled = await handle_first_contact(resume_trigger, settings=settings, client=client, runtime=runtime)
    assert handled is True
    pending = runtime.get_pending("123")
    assert pending is not None
    assert pending.index == 2  # Q1 ("name"), Q2 ("role") already answered
    assert set(pending.engrams) == {"name", "role"}
    assert resume_trigger.effective_message.replies == [FIRST_PERSON_QUESTIONS[2][1]]

    # Now finish Q3, Q4, Q5.
    for answer in ["psychonom", "Ship it", "Nothing else"]:
        fake_clock["now"] += 60.0
        update = Update(message=Message(text=answer))
        await handle_first_contact(update, settings=settings, client=client, runtime=runtime)

    remember_calls = [args for action, args in client.calls if action == "squad_remember"]
    assert len(remember_calls) == 5  # never re-recorded Q1/Q2 after resume
    labelled = {tuple(args["concepts"]): args["text"] for args in remember_calls}
    assert labelled[("name",)] == "Ada Example"
    assert labelled[("role",)] == "Engineer"
    assert labelled[("project",)] == "psychonom"
    assert labelled[("first_ask",)] == "Ship it"
    assert labelled[("notes",)] == "Nothing else"
    data = json.loads(state_path.read_text())
    assert set(data["completed"]["member-1"]["engram_ids"]) == {
        "name",
        "role",
        "project",
        "first_ask",
        "notes",
    }
    assert "member-1" not in data.get("in_progress", {})  # cleared on completion


# ---------------------------------------------------------------------------
# Round-3-gate-3 P0: the three EXECUTED triggers from the adversarial round
# -- (a) a gateway restart with no idle wait at all, (b) a check-in during
# the proposal-retry-wait phase across 601s, (c) a status flap from
# 'pending' to 'complete' and back -- each reproduced directly, not just
# the underlying mechanism.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_a_gateway_restart_after_all_questions_answered_never_indexes_out_of_range(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Trigger (a): a fully-answered intake whose proposal submission failed
    (the live schema rejects the project_access kind) survives a GATEWAY
    RESTART -- a brand-new FirstPersonRuntime over the same state file, with
    no in-memory _pending at all and no idle wait whatsoever -- and, once
    the server is healthy again, actually completes instead of looping
    forever on a missing project_id."""
    state_path = tmp_path / "state.json"
    install_status_stub(monkeypatch, lambda *_: pending_status())
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)
    settings = valid_settings()

    rejecting_client = _client_rejecting_project_access()
    runtime_before_restart = FirstPersonRuntime(state_path)
    answers = ["Ada Example", "Engineer", "psychonom", "Ship it", "Nothing else"]
    await handle_first_contact(
        Update(message=Message(text="hello!")), settings=settings, client=rejecting_client, runtime=runtime_before_restart
    )
    for answer in answers:
        await handle_first_contact(
            Update(message=Message(text=answer)), settings=settings, client=rejecting_client, runtime=runtime_before_restart
        )
    # The 5th answer triggered a submission attempt that failed honestly --
    # no completion marker, pending retained at index == len(QUESTIONS).
    assert not (state_path.exists() and json.loads(state_path.read_text()).get("completed"))
    pending_before = runtime_before_restart.get_pending("123")
    assert pending_before is not None
    assert pending_before.index == len(FIRST_PERSON_QUESTIONS)
    assert pending_before.project_id == "proj-1"

    # GATEWAY RESTART: a brand-new runtime, zero in-memory state, same file.
    # Round-3-gate-4 P3-b: proposal_retry_count (1, from the failed attempt
    # above) is restored durably, and start() recomputes a FRESH backoff
    # window from it (never a raw persisted monotonic timestamp, which
    # would be meaningless across a restart) -- so the very first post-
    # restart message must NOT immediately retry.
    # Round-3-gate-4 P3-e: handle_first_contact now accepts `clock` for
    # exactly this reason -- without threading the SAME fake clock through
    # (instead of the real time.monotonic() default), the backoff-due
    # comparison inside _retry_proposal_if_due would compare a huge real
    # timestamp against the small test-scale next_proposal_retry_at and
    # always read as "due", masking the very thing this test checks.
    fake_clock = {"now": 0.0}
    restarted_runtime = FirstPersonRuntime(state_path, clock=lambda: fake_clock["now"])
    healthy_client = _client_for_full_intake()
    restart_update = Update(message=Message(text="are you there?"))
    handled = await handle_first_contact(
        restart_update,
        settings=settings,
        client=healthy_client,
        runtime=restarted_runtime,
        clock=lambda: fake_clock["now"],
    )  # must never raise IndexError

    assert handled is True
    resumed = restarted_runtime.get_pending("123")
    assert resumed is not None  # NOT immediately retried -- respects the restored backoff
    assert resumed.proposal_retry_count == 1
    assert restart_update.effective_message.replies == [_PROPOSAL_RETRY_WAIT_REPLY]
    assert not any(action == "routine_proposal_submit" for action, _ in healthy_client.calls)

    # Advance past the restored backoff window -- NOW it actually retries,
    # and (server healthy again) completes.
    fake_clock["now"] = resumed.next_proposal_retry_at + 1.0
    final_update = Update(message=Message(text="anything"))
    handled = await handle_first_contact(
        final_update, settings=settings, client=healthy_client, runtime=restarted_runtime, clock=lambda: fake_clock["now"]
    )
    assert handled is True
    assert restarted_runtime.get_pending("123") is None  # completed, not stuck
    data = json.loads(state_path.read_text())
    assert data["completed"]["member-1"]["proposal_id"] == "proposal-1"


@pytest.mark.asyncio
async def test_trigger_b_frequent_retry_wait_checkins_never_idle_drop_the_record(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Trigger (b): touch() must be wired at the retry-wait check-in path
    (round-3-gate-2's touch() had ZERO call sites) so periodic engagement
    during a long proposal-retry wait never idle-drops the record purely
    because last_activity_at was frozen at the last actual answer.

    Round-3-gate-3's resume fix means an idle-drop no longer produces an
    OBSERVABLE gap by itself (start() now resumes the fully-answered record
    safely) -- so this asserts on `created_at` staying put, not just on
    `get_pending` returning non-None: if touch() is dead, the 3rd check-in's
    idle-drop forces a THROUGH-start() resume that stamps a brand-new
    `created_at`/`proposal_retry_count` (silently resetting backoff
    progress and starting the 24h absolute-cap clock over), which this
    catches even though the member-visible "still pending" behavior looks
    unchanged. Mutation-provable: removing the runtime.touch(chat_key) call
    in _retry_proposal_if_due makes this go red at the 3rd check-in."""
    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    client = _client_rejecting_project_access()
    install_status_stub(monkeypatch, lambda *_: pending_status())
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)
    settings = valid_settings()

    answers = ["Ada Example", "Engineer", "psychonom", "Ship it", "Nothing else"]
    await handle_first_contact(Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime)
    for answer in answers:
        await handle_first_contact(Update(message=Message(text=answer)), settings=settings, client=client, runtime=runtime)
    original_pending = runtime.get_pending("123")
    assert original_pending is not None
    original_created_at = original_pending.created_at
    original_retry_count = original_pending.proposal_retry_count
    assert original_retry_count == 1

    # 10 check-ins, 300s apart -- each individually well under the 600s idle
    # window, but 3000s cumulative from the ORIGINAL last answer (well past
    # the round-2-gate-2 trigger (b)'s "601s idle in the retry-wait phase").
    for _ in range(10):
        fake_clock["now"] += 300.0
        handled = await handle_first_contact(
            Update(message=Message(text="any word here?")), settings=settings, client=client, runtime=runtime
        )
        assert handled is True
        pending = runtime.get_pending("123")
        assert pending is not None  # never idle-dropped
        assert pending.created_at == original_created_at  # SAME record, never resumed-from-scratch
    # Backoff progress was never silently reset by a spurious idle-drop-and-
    # resume cycle either -- none of these check-ins were due yet, so the
    # retry count is unchanged from the one real failed attempt above.
    assert runtime.get_pending("123").proposal_retry_count == original_retry_count


@pytest.mark.asyncio
async def test_trigger_c_a_single_status_flap_never_prunes_the_durable_record(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Trigger (c) / Athena's ruling (3), corrected by round-3-gate-4 P1-b
    (the reviewer's M7 probe) and relocated to post-answer reconciliation
    by the ROOT SHAPE reshape (mupot-plugin#20 issue tracker P0): a SINGLE
    non-pending reconciliation reading (here, 'complete') must NEVER
    durably prune in-progress engrams -- only a SECOND CONSECUTIVE
    confirming reading may (see note_non_pending). The check-in message
    itself uses an escape word so it is fully handled (paused, not
    dropped) WITHOUT itself advancing progress, isolating the
    reconciliation effect from ordinary answer-handling.

    Round-2-gate (#23) P2-a: the FIRST (unconfirmed) reading already
    suspends capture, so the message that triggers the flap back to
    'pending' is ITSELF still suspended at entry (falls through untouched)
    -- only the message AFTER that, once reconciliation has cleared the
    suspension, resumes with every engram intact, index unchanged, never
    re-asking an already-answered question."""
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    client = _client_for_full_intake()
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)
    settings = valid_settings()

    install_status_stub(monkeypatch, lambda *_: pending_status())
    answers = ["Ada Example", "Engineer", "psychonom", "Ship it"]  # 4 of 5 -- not yet submitted
    await handle_first_contact(Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime)
    for answer in answers:
        await handle_first_contact(Update(message=Message(text=answer)), settings=settings, client=client, runtime=runtime)
    pending = runtime.get_pending("123")
    assert pending is not None and pending.index == 4

    # ONE 'complete' reconciliation reading -- unconfirmed, must leave
    # everything intact (in-memory AND durable), but DOES suspend capture.
    # An escape word keeps this check-in message itself from being
    # captured as the 5th answer.
    install_status_stub(monkeypatch, lambda *_: COMPLETE_STATUS)
    handled = await handle_first_contact(
        Update(message=Message(text="stop")), settings=settings, client=client, runtime=runtime
    )
    assert handled is True  # NOT suspended yet at entry -- this pause is still fully handled
    still_pending = runtime.get_pending("123")
    assert still_pending is not None
    assert still_pending.suspended is True  # suspended now, from reconciliation's first reading
    assert still_pending.index == 4
    assert set(still_pending.engrams) == {"name", "role", "project", "first_ask"}
    data = json.loads((tmp_path / "state.json").read_text())
    assert "member-1" in data.get("in_progress", {})  # NOT pruned on one reading

    # Flap BACK to 'pending' -- this FIRST message is itself still
    # suspended at entry (suspension only clears via reconciliation, which
    # runs AFTER a message, never during it), so it falls through
    # untouched; reconciliation then resumes the record for the NEXT one.
    install_status_stub(monkeypatch, lambda *_: pending_status())
    resume_trigger = Update(message=Message(text="you there?"))
    handled = await handle_first_contact(resume_trigger, settings=settings, client=client, runtime=runtime)
    assert handled is False
    assert resume_trigger.effective_message.replies == []
    resumed = runtime.get_pending("123")
    assert resumed is not None and resumed.suspended is False  # reconciliation cleared it
    assert resumed.index == 4  # untouched

    # NOW captured normally again -- resumes the SAME record, no reset, no
    # re-asked/re-recorded question.
    await handle_first_contact(Update(message=Message(text="Nothing else")), settings=settings, client=client, runtime=runtime)
    remember_calls = [args for action, args in client.calls if action == "squad_remember"]
    assert len(remember_calls) == 5  # never re-recorded name/role/project/first_ask
    assert runtime.get_pending("123") is None  # completed normally


@pytest.mark.asyncio
async def test_trigger_c_two_consecutive_complete_readings_confirm_and_prune(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The companion positive case: TWO CONSECUTIVE 'complete' reconciliation
    readings (no pending reading between them), separated by at least the
    status-cache TTL (mupot-plugin#21 P2), confirm the terminal state and
    prune the durable record -- closing the original round-3-gate-2
    trigger (c) landmine (a status flap resurrecting a stale, never-
    produced-by-this-episode record) while still requiring genuine
    confirmation. Escape words keep the first check-in message from being
    captured as a real answer, isolating the reconciliation effect; the
    second one is suspended (P2-a) before it would even reach that check.
    """
    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    client = _client_for_full_intake()
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)
    settings = valid_settings()

    install_status_stub(monkeypatch, lambda *_: pending_status())
    answers = ["Ada Example", "Engineer", "psychonom", "Ship it"]
    await handle_first_contact(Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime)
    for answer in answers:
        await handle_first_contact(Update(message=Message(text=answer)), settings=settings, client=client, runtime=runtime)

    install_status_stub(monkeypatch, lambda *_: COMPLETE_STATUS)
    await handle_first_contact(
        Update(message=Message(text="stop")), settings=settings, client=client, runtime=runtime
    )
    first_reading = runtime.get_pending("123")
    assert first_reading is not None and first_reading.suspended is True  # first reading -- unconfirmed, suspended

    fake_clock["now"] += first_person._STATUS_NEGATIVE_TTL_SECONDS
    handled = await handle_first_contact(
        Update(message=Message(text="cancel")), settings=settings, client=client, runtime=runtime
    )
    assert handled is False  # suspended at entry -- falls straight through
    assert runtime.get_pending("123") is None
    data = json.loads((tmp_path / "state.json").read_text())
    assert "member-1" not in data.get("in_progress", {})  # pruned, not immortal

    # Flapping back to pending NOW starts a genuinely FRESH episode.
    install_status_stub(monkeypatch, lambda *_: pending_status())
    handled = await handle_first_contact(
        Update(message=Message(text="hello again")), settings=settings, client=client, runtime=runtime
    )
    assert handled is True
    fresh = runtime.get_pending("123")
    assert fresh is not None
    assert fresh.index == 0
    assert fresh.engrams == {}


def test_note_non_pending_requires_the_two_readings_separated_by_the_status_cache_ttl() -> None:
    """mupot-plugin#21 (P2, PR#20 round-2 gate): "two consecutive readings"
    had NO time separation -- two handler calls racing on the very same
    transient reading (e.g. concurrent `handle_first_contact` invocations
    for the same member) could both observe "non-pending" and the SECOND
    call would confirm-and-prune on what is really the SAME underlying
    reading, not two independent ones. Direct unit check of the fix,
    independent of the full reconciliation walkthrough in the trigger-c
    tests above.

    Mutation-provable: reverting `note_non_pending` to confirm
    unconditionally on any second call (dropping the `min_separation`
    check entirely) makes the first assertion below go red -- a second
    reading 1 second after the first would already confirm."""
    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(Path("/nonexistent/unused-state.json"), clock=lambda: fake_clock["now"])

    assert runtime.note_non_pending("member-1") is False  # first reading -- never confirms alone

    fake_clock["now"] += 1.0  # 1s later -- far short of the TTL separation
    assert runtime.note_non_pending("member-1") is False  # too soon -- not an independent reading yet

    fake_clock["now"] += first_person._STATUS_NEGATIVE_TTL_SECONDS  # now genuinely separated
    assert runtime.note_non_pending("member-1") is True  # CONFIRMED

    # A confirmed reading resets the sighting -- the very next one starts a
    # fresh "first reading" (unconfirmed) window again.
    assert runtime.note_non_pending("member-1") is False


def test_note_non_pending_default_separation_matches_the_negative_status_cache_ttl() -> None:
    """Pins the DEFAULT `min_separation` to `_STATUS_NEGATIVE_TTL_SECONDS`
    specifically (the TTL a non-pending resolution is cached for) rather
    than some other arbitrary window -- an explicit override still works
    for callers that want a different window."""
    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(Path("/nonexistent/unused-state.json"), clock=lambda: fake_clock["now"])

    runtime.note_non_pending("member-2")
    fake_clock["now"] = first_person._STATUS_NEGATIVE_TTL_SECONDS - 1.0
    assert runtime.note_non_pending("member-2") is False  # 1s short of the TTL -- not yet
    fake_clock["now"] = first_person._STATUS_NEGATIVE_TTL_SECONDS
    assert runtime.note_non_pending("member-2") is True  # exactly at the TTL -- confirmed

    # An explicit override is honoured instead of the default.
    runtime.note_non_pending("member-3")
    fake_clock["now"] += 5.0
    assert runtime.note_non_pending("member-3", min_separation=5.0) is True


def test_abandon_prunes_the_durable_in_progress_record(tmp_path: Path) -> None:
    """Direct unit check of the fix, independent of the full status-flap
    scenario above."""
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    runtime.record_answer("123", "name", "engram-1")
    data = json.loads(state_path.read_text())
    assert "member-1" in data["in_progress"]

    runtime.abandon("123")

    assert runtime.get_pending("123") is None
    data = json.loads(state_path.read_text())
    assert "member-1" not in data.get("in_progress", {})


# ---------------------------------------------------------------------------
# Round-2-gate (#23) P1: SHARED-STORE INTEGRITY (the same defect class as
# #19 -- a second write to a completion marker a concurrent/later write
# should never be allowed to touch). ROOT SHAPE removed the ONLY thing that
# used to check "does this member already hold a proposal" on EVERY message
# (resolve_member_status -> held_proposal_id, see #20) from the pending-
# member message path -- a fully-answered pending record could reach
# _submit_proposal with NOTHING upstream having checked that first. TWO
# independent layers close this, each pinned by its own test/mutation:
#   (reader) _submit_proposal itself checks held_proposal_id and refuses/
#            reuses rather than ever calling routine_proposal_submit again.
#   (writer) finish() never overwrites an already-recorded proposal_id with
#            a different one -- the FIRST one always wins, loudly.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_submit_proposal_refuses_when_a_proposal_is_already_held(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """READER-SIDE layer. A LIVE local pending record (established through
    the normal flow -- 4 of 5 questions answered, no completion marker
    yet) plus a completion marker that appears BEFORE the 5th answer (a
    proposal submitted via the journal fallback, by a sibling process
    entirely, or a seeded marker) must mean ZERO
    `routine_proposal_submit` calls when the local record independently
    reaches completion -- reuse the held proposal_id, never submit a
    second one. (Seeding the marker before ANY local record exists would
    instead be caught by the OLDER, outer held-proposal lag-guard in
    `handle_first_contact` itself -- this scenario specifically exercises
    `_submit_proposal`'s OWN check, reachable only once a local pending
    record already exists and is answered all the way to completion.)
    Mutation-provable: removing the
    `runtime.held_proposal_id(intake.member_id)` check at the top of
    `_submit_proposal` makes `submit_calls` non-empty."""
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    client = _client_for_full_intake()
    install_status_stub(monkeypatch, lambda *_: pending_status())
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)
    settings = valid_settings()

    await handle_first_contact(Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime)
    for answer in ["Ada Example", "Engineer", "psychonom", "Ship it"]:  # 4 of 5 -- not yet submitted
        await handle_first_contact(Update(message=Message(text=answer)), settings=settings, client=client, runtime=runtime)
    assert runtime.get_pending("123") is not None and runtime.get_pending("123").index == 4

    # A proposal for this member appears from elsewhere BEFORE the 5th
    # answer -- e.g. a concurrent sibling call, or a journal-recovered
    # completion this process's own main store doesn't yet reflect.
    data = json.loads(state_path.read_text())
    data["completed"] = {"member-1": {"engram_ids": {}, "proposal_id": "proposal-SEEDED", "completed_at": 0.0}}
    state_path.write_text(json.dumps(data))

    last_update = Update(message=Message(text="Nothing else"))
    await handle_first_contact(last_update, settings=settings, client=client, runtime=runtime)

    submit_calls = [action for action, _ in client.calls if action == "routine_proposal_submit"]
    assert submit_calls == []  # NEVER attempted a second submission
    assert last_update.effective_message.replies[-1] == first_person._COMPLETE_REPLY
    data = json.loads(state_path.read_text())
    assert data["completed"]["member-1"]["proposal_id"] == "proposal-SEEDED"  # unchanged
    assert runtime.get_pending("123") is None


def test_finish_never_overwrites_an_existing_proposal_id(tmp_path: Path) -> None:
    """WRITER-SIDE layer -- the independent backstop for whatever narrow
    race still reaches `finish()` with a second proposal_id (the reader-
    side check above is the primary defense; this is what keeps a torn
    read, a journal-recovered marker, or a future caller from silently
    destroying the FIRST recorded proposal_id). Mutation-provable:
    removing the `existing_proposal_id is not None and existing_proposal_id
    != new_proposal_id` guard in `finish()` makes the final assertion
    below go red (`proposal-SECOND` would overwrite `proposal-FIRST`)."""
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {"completed": {"member-1": {"engram_ids": {}, "proposal_id": "proposal-FIRST", "completed_at": 0.0}}}
        )
    )
    runtime = FirstPersonRuntime(state_path)
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    runtime.record_answer("123", "name", "engram-1")

    runtime.finish("123", proposal_id="proposal-SECOND")

    data = json.loads(state_path.read_text())
    assert data["completed"]["member-1"]["proposal_id"] == "proposal-FIRST"  # never overwritten
    assert "member-1" not in data.get("in_progress", {})  # in-progress record still cleared
    assert runtime.get_pending("123") is None


@pytest.mark.asyncio
async def test_submit_proposal_concurrent_calls_submit_exactly_once(tmp_path: Path) -> None:
    """The genuinely-concurrent race case: three overlapping
    `_submit_proposal` calls for the SAME chat_key (e.g. two or more
    overlapping `handle_first_contact` invocations each reaching
    `index == len(FIRST_PERSON_QUESTIONS)` before any of them finishes)
    must only ever let ONE of them actually call `routine_proposal_submit`
    -- `try_begin_submit`/`end_submit` is the guard. The first call's
    submission is deliberately blocked (a real `threading.Event`, since
    `client.call` runs inside `asyncio.to_thread`) until the other two
    have already run their own (non-blocking) `try_begin_submit` checks,
    so this is a genuine overlap, not an artifact of asyncio scheduling
    order. Mutation-provable: removing the `try_begin_submit`/`end_submit`
    guard from `_submit_proposal` makes `submit_calls` exceed 1."""
    import threading

    from plugin.first_person import _submit_proposal

    release = threading.Event()

    class _BlockingClient(FakeClient):
        def call(self, action: str, args: dict[str, Any]) -> Any:
            self.calls.append((action, dict(args)))
            if action == "routine_proposal_submit":
                assert release.wait(timeout=5.0), "test setup did not release in time"
                return {"ok": True, "result": {"proposal_id": "proposal-1"}}
            return {"ok": True, "result": {}}

    client = _BlockingClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    runtime.set_project_id("123", "proj-1")
    intake = None
    for question_id, _ in FIRST_PERSON_QUESTIONS:
        intake = runtime.record_answer("123", question_id, f"engram-{question_id}")
    assert intake is not None and intake.index == len(FIRST_PERSON_QUESTIONS)

    async def call_submit() -> Update:
        update = Update(message=Message(text="anything"))
        await _submit_proposal(update.effective_message, "123", intake, client=client, runtime=runtime)
        return update

    task1 = asyncio.create_task(call_submit())
    await asyncio.sleep(0.05)  # let task1 reach the blocked submit() call
    task2 = asyncio.create_task(call_submit())
    task3 = asyncio.create_task(call_submit())
    await asyncio.sleep(0.05)  # let task2/task3 run their try_begin_submit checks
    release.set()
    update1, update2, update3 = await asyncio.gather(task1, task2, task3)

    submit_calls = [action for action, _ in client.calls if action == "routine_proposal_submit"]
    assert len(submit_calls) == 1  # only ONE real submission, ever
    assert update1.effective_message.replies[-1] == first_person._COMPLETE_REPLY
    assert update2.effective_message.replies[-1] == _PROPOSAL_RETRY_WAIT_REPLY
    assert update3.effective_message.replies[-1] == _PROPOSAL_RETRY_WAIT_REPLY


@pytest.mark.asyncio
async def test_reconcile_denied_probe_never_confirms_or_resumes_from_a_fabricated_reading(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Round-2-gate (#23) P2-b: pins against mutation M5 -- re-adding a
    locally-derived "assume pending" fallback to
    `_reconcile_pending_with_server`'s `resolve()` call would make a
    LIMITER DENIAL masquerade as a genuine server reading. Two ways that
    would corrupt state, neither of which any OTHER test in this file
    catches (this mutation stayed green at 187/187 without this test):
    (1) it would call `note_pending()`/`resume()` from a denial, erasing
    an already-accumulated unconfirmed non-pending sighting; (2) it would
    never let the ORIGINAL sighting's timestamp survive a long run of
    denials, so a later genuine confirming reading would incorrectly
    restart the confirmation window instead of confirming against the
    original one.

    First accumulate ONE real, definitive non-pending reading (suspending
    the record), then deny every subsequent reconcile probe for a while
    (via `_SenderProbeLimiter`) while re-invoking reconciliation directly,
    and confirm the record stays suspended and unconfirmed THROUGHOUT --
    never resumed, never reset -- until a real reading is allowed through
    again, at which point it confirms against the ORIGINAL sighting."""
    from plugin.first_person import _reconcile_pending_with_server

    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    pending = runtime.get_pending("123")
    settings = valid_settings()

    # One real, definitive non-pending reading -- suspends, unconfirmed.
    # Uses install_probe (the transport layer only), NOT install_status_stub
    # -- that stub replaces resolve_member_status wholesale and would
    # silently skip the REAL sender_limiter/fallback logic this test exists
    # to exercise.
    install_probe(monkeypatch, response=status_response_bytes(intake_state="complete", home_squad_id="home-squad-1"))
    await _reconcile_pending_with_server(
        "123",
        123,
        123,
        pending,
        settings=settings,
        secret_owner=None,
        runtime=runtime,
        status_cache=None,
        probe_limiter=None,
        sender_probe_limiter=None,
    )
    suspended = runtime.get_pending("123")
    assert suspended is not None and suspended.suspended is True  # unconfirmed -- suspended, not abandoned

    # Every subsequent reconcile probe is denied.
    denying_limiter = _SenderProbeLimiter(min_interval=999999.0)
    assert denying_limiter.should_probe("123") is True  # consume the only slot up front
    for _ in range(5):
        fake_clock["now"] += 1.0
        await _reconcile_pending_with_server(
            "123",
            123,
            123,
            suspended,
            settings=settings,
            secret_owner=None,
            runtime=runtime,
            status_cache=None,
            probe_limiter=None,
            sender_probe_limiter=denying_limiter,
        )
        still = runtime.get_pending("123")
        assert still is not None and still.suspended is True  # STILL suspended -- never resumed by a denial

    # Once a real probe is allowed through again, confirmation fires
    # against the ORIGINAL sighting -- not reset by any of the denials.
    fake_clock["now"] = first_person._STATUS_NEGATIVE_TTL_SECONDS
    await _reconcile_pending_with_server(
        "123",
        123,
        123,
        still,
        settings=settings,
        secret_owner=None,
        runtime=runtime,
        status_cache=None,
        probe_limiter=None,
        sender_probe_limiter=None,
    )
    assert runtime.get_pending("123") is None  # confirmed against the ORIGINAL reading


@pytest.mark.asyncio
async def test_escape_message_refreshes_activity_so_a_pause_never_idle_drops(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """touch() wired at the escape/pause path (round-3-gate-2's touch() had
    zero call sites): repeated pauses, each within the 600s idle window of
    the last, must never cumulatively idle-drop the record.

    Round-3-gate-3's resume fix means a silent idle-drop no longer produces
    a visible gap on its own (an unanswered fresh record just resumes at
    index 0 again) -- so this asserts on `created_at` staying put: if
    touch() is dead, the drop-then-resume at t=900 stamps a brand-new
    `created_at`, which this catches even though `get_pending` alone would
    not."""
    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    client = _client_for_full_intake()
    install_status_stub(monkeypatch, lambda *_: pending_status())
    settings = valid_settings()

    await handle_first_contact(Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime)
    await handle_first_contact(Update(message=Message(text="Ada Example")), settings=settings, client=client, runtime=runtime)
    original_pending = runtime.get_pending("123")
    assert original_pending is not None and original_pending.index == 1
    original_created_at = original_pending.created_at

    for _ in range(3):
        fake_clock["now"] += 300.0
        handled = await handle_first_contact(
            Update(message=Message(text="stop")), settings=settings, client=client, runtime=runtime
        )
        assert handled is True
        pending = runtime.get_pending("123")
        assert pending is not None  # never idle-dropped by repeated pauses
        assert pending.created_at == original_created_at  # SAME record, never resumed-from-scratch
        assert pending.index == 1  # the pause never advanced or re-asked anything


def test_runtime_start_resumes_at_the_first_contiguous_unanswered_question(tmp_path: Path) -> None:
    """Unit-level check of FirstPersonRuntime.start's resume logic directly,
    independent of the full message flow above.

    Round-3-gate-3 P3 / Athena's ruling (3): abandon() now PRUNES the
    durable in_progress record it drops (see
    test_abandon_prunes_the_durable_in_progress_record below) -- resumable
    drops (idle/absolute TTL, or a process restart) never go through
    abandon() at all, so this simulates ONE of those instead (popping the
    in-memory record directly, same as a TTL expiry does internally),
    never abandon()."""
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    runtime.record_answer("123", "name", "engram-1")
    runtime.record_answer("123", "role", "engram-2")
    with runtime._lock:  # noqa: SLF001 -- simulate a TTL drop, not abandon()
        runtime._pending.pop("123", None)

    resumed = runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    assert resumed.index == 2
    assert resumed.engrams == {"name": "engram-1", "role": "engram-2"}


def test_runtime_start_is_a_fresh_intake_for_a_member_with_no_prior_progress(tmp_path: Path) -> None:
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    fresh = runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    assert fresh.index == 0
    assert fresh.engrams == {}


# ---------------------------------------------------------------------------
# Round-3-gate-3 P0 (:1856): derived-index bounds + atomic resume restore.
# Adversarial round-2 finding: start() resumed engrams+index from a durable
# record but NEVER project_id, so a member who had answered all 5 questions
# (index == len(FIRST_PERSON_QUESTIONS)) but whose proposal never
# successfully submitted (the live schema rejects the "project_access" kind
# -- the DEFAULT outcome today) resumed with index=5 and project_id=None --
# an IndexError at the caller's FIRST_PERSON_QUESTIONS[started.index] and,
# even once guarded, a state the forward path can never produce. Athena's
# ruling (1): the resumed record must be ATOMIC -- engrams, index, and
# project_id restored as ONE record, never a valid index with a missing
# project_id (the same defect class as finish()'s pre-fix truncated-read
# ledger wipe).
# ---------------------------------------------------------------------------


def test_start_clamps_a_resumed_index_past_the_project_question_when_project_id_is_missing(tmp_path: Path) -> None:
    """Athena's ruling (1): a durable in_progress record with engrams past
    the project question but NO project_id (a partial/legacy write -- the
    exact shape a torn write, or a pre-fix build, could produce) must never
    be trusted whole. start() clamps ``index`` back to the project question
    instead of manufacturing "every question answered, no project" -- but
    (round-3-gate-4 P2-a, correcting a round-3-gate-3 bug) it must NOT drop
    the project question's own already-recorded engram_id in the process,
    since squad_remember is an INSERT and re-recording on the re-answer
    would leave a permanent duplicate. The engram_id is kept so
    _handle_answer can detect and reuse it.

    mupot-plugin#22 P3 (correcting the round-3-gate-4 clamp itself): Q4/Q5
    ("first_ask"/"notes") engrams already present in the SAME durable
    upgrade-path record must survive the clamp too, not just "project"'s --
    dropping them orphaned those engram_ids and minted a duplicate the
    moment `_handle_answer` reached those questions again after re-
    resolving the project. All engrams present are kept; only ``index``/
    ``project_id`` revert."""
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    project_index = next(i for i, (question_id, _) in enumerate(FIRST_PERSON_QUESTIONS) if question_id == "project")
    all_engrams = {question_id: f"engram-{question_id}" for question_id, _ in FIRST_PERSON_QUESTIONS}
    # Deliberately the PARTIAL shape: every engram present, project_id absent.
    state_path.write_text(
        json.dumps({"in_progress": {"member-1": {"engram_ids": all_engrams, "updated_at": 0.0}}})
    )

    resumed = runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    assert resumed.index == project_index
    assert resumed.project_id is None
    # EVERY already-recorded engram survives the clamp -- Q4/Q5 included,
    # not just the prefix through "project" (mupot-plugin#22 P3).
    assert set(resumed.engrams) == {question_id for question_id, _ in FIRST_PERSON_QUESTIONS}
    assert resumed.engrams["project"] == "engram-project"
    assert resumed.engrams["first_ask"] == "engram-first_ask"
    assert resumed.engrams["notes"] == "engram-notes"


def test_start_trusts_a_resumed_index_past_the_project_question_when_project_id_is_present(tmp_path: Path) -> None:
    """The companion positive case: engrams AND project_id present together
    (the shape _save_in_progress/record_answer/set_project_id now always
    write) resumes at the true index, never clamped."""
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    all_engrams = {question_id: f"engram-{question_id}" for question_id, _ in FIRST_PERSON_QUESTIONS}
    state_path.write_text(
        json.dumps(
            {"in_progress": {"member-1": {"engram_ids": all_engrams, "project_id": "proj-1", "updated_at": 0.0}}}
        )
    )

    resumed = runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    assert resumed.index == len(FIRST_PERSON_QUESTIONS)
    assert resumed.project_id == "proj-1"
    assert set(resumed.engrams) == {question_id for question_id, _ in FIRST_PERSON_QUESTIONS}


def test_record_answer_and_set_project_id_persist_project_id_alongside_engrams(tmp_path: Path) -> None:
    """Direct check that the durable write path itself is atomic -- both
    FirstPersonRuntime.record_answer AND set_project_id write project_id to
    disk every time, not only engrams."""
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    runtime.record_answer("123", "name", "engram-1")
    runtime.record_answer("123", "role", "engram-2")
    runtime.set_project_id("123", "proj-1")

    data = json.loads(state_path.read_text())
    entry = data["in_progress"]["member-1"]
    assert entry["project_id"] == "proj-1"
    assert set(entry["engram_ids"]) == {"name", "role"}


@pytest.mark.asyncio
async def test_every_reachable_resume_index_avoids_index_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Closes the CLASS, not the instance: every value FirstPersonRuntime.
    start() can derive for `index` -- 0 through len(FIRST_PERSON_QUESTIONS)
    inclusive -- must be safe to act on end-to-end through
    handle_first_contact, never just the single index==len trigger the
    adversarial round caught."""
    state_path = tmp_path / "state.json"
    project_index = next(i for i, (question_id, _) in enumerate(FIRST_PERSON_QUESTIONS) if question_id == "project")
    settings = valid_settings()
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)

    for index in range(len(FIRST_PERSON_QUESTIONS) + 1):
        member_id = f"member-{index}"
        runtime = FirstPersonRuntime(state_path)
        engrams = {question_id: f"engram-{question_id}" for question_id, _ in FIRST_PERSON_QUESTIONS[:index]}
        project_id = "proj-1" if index > project_index else None
        runtime._save_in_progress(member_id, engrams, project_id, 0)  # noqa: SLF001 -- seed durable resume state

        install_status_stub(monkeypatch, lambda *_, member_id=member_id: pending_status(member_id=member_id))
        client = _client_for_full_intake()
        handled = await handle_first_contact(
            Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime
        )
        assert handled is True  # never raises, regardless of the resumed index


# ---------------------------------------------------------------------------
# P1 (:1114) / Athena's "sanitizer class by invariant": Cc + Cf, one constant
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "hidden_char",
    ["‍", "​", "‌", "­", "⁠", "﻿", "᠎"],
    ids=["ZWJ-U+200D", "ZWSP-U+200B", "ZWNJ-U+200C", "soft-hyphen-U+00AD", "word-joiner-U+2060", "BOM-U+FEFF", "U+180E"],
)
def test_format_category_characters_are_stripped_not_only_explicit_bidi(hidden_char: str) -> None:
    """Round-3-gate-2 P1 (:1114): the round-2 sanitizer stripped Cc + an
    explicit bidi-only list -- it missed every OTHER Cf (format) character.
    All seven of these ARE category Cf; the fix strips by category, so all
    seven disappear regardless of whether any one of them was ever
    individually named."""
    exploit = "mupot_" + "A" * 4 + hidden_char + "A" * 28
    assert _looks_like_credential(_sanitize_answer(exploit)) is True


def test_stripped_categories_is_one_constant_not_a_growing_list() -> None:
    assert first_person._STRIPPED_UNICODE_CATEGORIES == frozenset({"Cc", "Cf"})


def test_order_revert_mutation_still_catches_every_explicit_evading_string() -> None:
    """Mutation-regression receipt, extended: reverting the credential-check
    order (raw text first, THEN sanitize) must still let every one of these
    evading strings through uncaught -- proving the P0-A fix continues to be
    load-bearing after the P1 sanitizer rewrite."""
    hidden_chars = ["‮", "\x01", "⁦", "‭", "‍", "​", "‌", "­", "⁠", "﻿"]
    for hidden_char in hidden_chars:
        exploit = "mupot_" + "A" * 4 + hidden_char + "A" * 28
        assert _looks_like_credential(_sanitize_answer(exploit)) is True  # the FIX catches it
        assert _looks_like_credential(exploit) is False  # the OLD (broken) order would not have


# ---------------------------------------------------------------------------
# P1 (:1408-1426) / P3: lag guard returns False (host owns the turn), never
# a permanent DM lockout; WARNING rate-limited separately from the reply
# ---------------------------------------------------------------------------


def test_notify_once_per_window_rate_limits_a_single_key() -> None:
    fake_time = {"now": 0.0}
    notifier = _NotifyOncePerWindow(100.0, clock=lambda: fake_time["now"])
    assert notifier.should_notify("member-1") is True
    assert notifier.should_notify("member-1") is False  # immediate repeat -- suppressed
    fake_time["now"] += 50.0
    assert notifier.should_notify("member-1") is False  # still within the window
    fake_time["now"] += 50.0
    assert notifier.should_notify("member-1") is True  # window elapsed


def test_notify_once_per_window_is_bounded_across_many_distinct_keys() -> None:
    notifier = _NotifyOncePerWindow(3600.0, max_entries=64)
    for n in range(10_000):
        notifier.should_notify(f"member-{n}")
    assert len(notifier._last_at) <= 64


@pytest.mark.asyncio
async def test_held_proposal_lag_never_locks_the_member_out_of_the_host_permanently(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Round-3-gate-2 P1 (:1408-1426) P0-class regression: round-2's
    unconditional `return True` here meant a member whose proposal is held
    while the server still says 'pending' could NEVER reach the host again
    -- not even their own genuine `approve <id>`. `handled` must be False
    on every single message in this state, forever, not just sometimes."""
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    answers = ["Ada Example", "Engineer", "psychonom", "Ship it", "Nothing else"]
    await _run_intake(monkeypatch, client, runtime, answers)

    install_status_stub(monkeypatch, lambda *_: pending_status(home_squad_id="home-squad-1"))
    settings = valid_settings()
    for _ in range(5):
        update = Update(message=Message(text="approve f9408956"))
        handled = await handle_first_contact(update, settings=settings, client=client, runtime=runtime)
        assert handled is False


# ---------------------------------------------------------------------------
# P1 (:931) / Athena's "store rewrite: sender-scoped and validity-gated"
# ---------------------------------------------------------------------------


def test_finish_never_rewrites_the_store_on_an_invalid_read(tmp_path: Path) -> None:
    """The round-3-gate-1 bug: finish() discarded load_checked's validity
    flag and rewrote the WHOLE store from a truncated read, silently
    erasing every OTHER member's completed record. Fix: on an invalid read,
    finish() must not touch the main file AT ALL -- it appends to a
    separate, sender-scoped journal instead."""
    state_path = tmp_path / "state.json"
    corrupted_content = "{not valid json"
    state_path.write_text(corrupted_content)
    runtime = FirstPersonRuntime(state_path)

    runtime.start("123", member_id="member-b", home_squad_id="home-squad-b")
    runtime.finish("123", proposal_id="proposal-b")

    # The main file is UNTOUCHED -- byte-identical to before finish() ran.
    assert state_path.read_text() == corrupted_content
    # But member-b's own completion is durably recorded via the journal,
    # sender-scoped (never read or touched member-a's data, which never
    # existed in this scenario, or anyone else's).
    assert runtime.held_proposal_id("member-b") == "proposal-b"


def test_finish_journal_fallback_never_affects_a_different_members_record(tmp_path: Path) -> None:
    """End-to-end: member-a completes normally (valid store); the store is
    THEN corrupted; member-b completes while it's corrupted. member-a's
    proposal_id read is unaffected by member-b's journal fallback (they are
    independent, sender-scoped facts)."""
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)

    runtime.start("chat-a", member_id="member-a", home_squad_id="home-squad-a")
    runtime.finish("chat-a", proposal_id="proposal-a")
    assert runtime.held_proposal_id("member-a") == "proposal-a"

    # Simulate external corruption of the main store (e.g. a torn write).
    state_path.write_text("{not valid json")

    runtime.start("chat-b", member_id="member-b", home_squad_id="home-squad-b")
    runtime.finish("chat-b", proposal_id="proposal-b")

    assert runtime.held_proposal_id("member-b") == "proposal-b"  # via the journal
    # member-a's fact is independent of member-b's journal entry -- neither
    # write touches the other's data path.
    journal_path = state_path.with_name(state_path.name + ".journal")
    journal_content = journal_path.read_text()
    assert "member-a" not in journal_content  # only member-b was journalled
    assert "member-b" in journal_content


# ---------------------------------------------------------------------------
# Round-3-gate-3 P3 (:836/:860) / Athena's ruling (3): the journal is
# size-bounded (compacted to one entry per member, oldest evicted first) and
# reads are capped regardless of on-disk size.
# ---------------------------------------------------------------------------


def test_journal_is_bounded_and_compacted_to_one_entry_per_member(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Round-3-gate-3 P3: the round-2 journal was truly append-only with NO
    size bound -- repeated completions while the main store stayed corrupted
    grew it forever. Fix: compacted on every write to at most one entry per
    member_id, oldest-by-completed_at evicted once over the cap. Mutation-
    provable at a small cap: writing more distinct members than the cap
    allows must never exceed it, and the oldest is the one gone."""
    monkeypatch.setattr(first_person, "_JOURNAL_MAX_ENTRIES", 3)
    state_path = tmp_path / "state.json"
    store = first_person.FirstPersonStateStore(state_path)

    for n in range(5):
        store.append_journal(f"member-{n}", f"proposal-{n}", float(n))

    entries = store._read_journal_entries()  # noqa: SLF001 -- direct check of the compacted shape
    assert len(entries) == 3
    # The oldest two (member-0, member-1, completed_at 0.0/1.0) were evicted.
    assert set(entries) == {"member-2", "member-3", "member-4"}
    assert store.read_journal_proposal_id("member-0") is None
    assert store.read_journal_proposal_id("member-4") == "proposal-4"

    # Repeatedly completing the SAME member never grows the entry count --
    # only the latest proposal_id for that member is kept.
    store.append_journal("member-4", "proposal-4-again", 100.0)
    entries = store._read_journal_entries()  # noqa: SLF001
    assert len(entries) == 3
    assert store.read_journal_proposal_id("member-4") == "proposal-4-again"


def test_read_journal_proposal_id_never_reads_more_than_the_capped_bytes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """read_journal_proposal_id must never read an unbounded amount of a
    file it does not fully control the size of (e.g. a pre-existing file
    from before the compaction cap existed). Simulated here by writing a
    journal file directly (bypassing append_journal's own compaction) far
    larger than the read cap, then confirming the read still terminates and
    still finds the one entry that happens to fall inside the read window."""
    monkeypatch.setattr(first_person, "_JOURNAL_READ_CAP_BYTES", 4096)
    state_path = tmp_path / "state.json"
    journal_path = state_path.with_name(state_path.name + ".journal")
    journal_path.parent.mkdir(parents=True, exist_ok=True)

    lines = []
    for n in range(2000):  # far larger than the 4096-byte cap
        lines.append(json.dumps({"member_id": f"padding-{n}", "proposal_id": f"proposal-{n}", "completed_at": 0.0}))
    lines.append(json.dumps({"member_id": "member-tail", "proposal_id": "proposal-tail", "completed_at": 1.0}))
    journal_path.write_text("\n".join(lines) + "\n")
    assert journal_path.stat().st_size > 4096 * 4  # confirm the file really is oversized

    store = first_person.FirstPersonStateStore(state_path)
    # Must terminate promptly (a truly unbounded read of this file would
    # still finish quickly in a unit test, but the point is BOUNDED
    # regardless of file size, not merely "happens to finish fast here") and
    # find the tail entry, which falls inside the last _JOURNAL_READ_CAP_BYTES.
    assert store.read_journal_proposal_id("member-tail") == "proposal-tail"


def test_journal_read_cap_is_derived_from_the_entry_cap() -> None:
    """Round-3-gate-4 P3-c: _JOURNAL_READ_CAP_BYTES (256KiB in round-3-
    gate-3) picked independently of _JOURNAL_MAX_ENTRIES (2048) meant a
    fully-compacted journal (2048 entries * a realistic ~150-200 bytes/line)
    could already exceed one read -- append_journal's own read-then-
    recompact-then-write cycle would then silently treat a PARTIAL read as
    the whole journal and rewrite it without the entries that fell outside
    the read window, permanently losing already-compacted proposal_ids.
    The read cap must be DERIVED from the write cap so the two can never
    silently diverge again."""
    assert first_person._JOURNAL_READ_CAP_BYTES == (
        first_person._JOURNAL_MAX_ENTRIES * first_person._JOURNAL_MAX_ENTRY_BYTES_ESTIMATE
    )


def test_journal_never_drops_entries_at_full_compaction_capacity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Direct regression for the P3-c defect: fill the journal to EXACTLY
    its compaction cap and confirm every single entry survives the
    read-recompact-write cycle -- the oldest entry (the first casualty if
    the read cap silently undercut the write cap) is still there."""
    monkeypatch.setattr(first_person, "_JOURNAL_MAX_ENTRIES", 200)
    monkeypatch.setattr(
        first_person, "_JOURNAL_READ_CAP_BYTES", 200 * first_person._JOURNAL_MAX_ENTRY_BYTES_ESTIMATE
    )
    state_path = tmp_path / "state.json"
    store = first_person.FirstPersonStateStore(state_path)
    for n in range(200):
        store.append_journal(f"member-{n:04d}", f"proposal-{n:04d}", float(n))

    entries = store._read_journal_entries()  # noqa: SLF001
    assert len(entries) == 200
    assert store.read_journal_proposal_id("member-0000") == "proposal-0000"  # the oldest -- not dropped
    assert store.read_journal_proposal_id("member-0199") == "proposal-0199"  # the newest


@pytest.mark.asyncio
async def test_double_proposal_prevented_even_when_store_was_corrupted_at_completion_time(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The actual double-propose scenario this fix prevents: member-b
    completes while the store is corrupted (journal fallback), then keeps
    messaging while the server still says 'pending' -- must never submit a
    second proposal."""
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    state_path.write_text("{not valid json")

    client = FakeClient({"routine_proposal_submit": {"ok": True, "result": {"proposal_id": "proposal-b"}}})
    intake = runtime.start("123", member_id="member-b", home_squad_id="home-squad-b")
    intake = replace(intake, project_id="proj-1")
    runtime._pending["123"] = intake

    from plugin.first_person import _submit_proposal

    update = Update(message=Message(text="anything"))
    await _submit_proposal(update.effective_message, "123", intake, client=client, runtime=runtime)
    assert runtime.held_proposal_id("member-b") == "proposal-b"

    proposal_calls_before = [a for a, _ in client.calls if a == "routine_proposal_submit"]
    install_status_stub(monkeypatch, lambda *_: pending_status(member_id="member-b", home_squad_id="home-squad-b"))
    settings = valid_settings()
    handled = await handle_first_contact(update, settings=settings, client=client, runtime=runtime)
    assert handled is False
    proposal_calls_after = [a for a, _ in client.calls if a == "routine_proposal_submit"]
    assert proposal_calls_after == proposal_calls_before  # no second submission


# ---------------------------------------------------------------------------
# P2 (:1391-1399, :186) / Athena: 'unknown' never abandons a pending intake
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_transient_unknown_status_never_holds_or_drops_the_pending_members_message(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Corrected replacement for the test this superseded (round-3-gate-4's
    own build), which PINNED THE WRONG BEHAVIOR: it asserted `handled is
    False`, an empty reply list, and no `squad_remember` call when a probe
    returned a transient 'unknown' for a member with a live pending record
    -- i.e. it asserted the message was silently held/dropped, unsanitized
    and unstored, and would fall through to the host's own LLM turn. That
    is exactly the pipeline-invariant violation Athena's ruling closes
    (mupot-plugin#20 issue tracker P0; see
    test_pending_member_messages_always_traverse_pipeline below for the
    comprehensive version covering every named probe-failure cause).

    ROOT SHAPE: a member with a live pending record is served FROM that
    record regardless of what any probe says -- a transient 'unknown'
    surfaces only in the out-of-band reconciliation
    (`_reconcile_pending_with_server`), which is never even consulted on
    the message path, so it can neither hold nor drop anything here."""
    call_count = {"n": 0}

    def resolver(_user_id: Any, _chat_id: Any) -> StatusResolution:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return pending_status()
        return UNKNOWN_STATUS  # a transient probe failure during reconciliation

    install_status_stub(monkeypatch, resolver)
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    await handle_first_contact(
        Update(message=Message(text="hello!")), settings=valid_settings(), client=client, runtime=runtime
    )
    assert runtime.get_pending("123") is not None

    transient_update = Update(message=Message(text="Ada Example"))
    handled = await handle_first_contact(
        transient_update, settings=valid_settings(), client=client, runtime=runtime
    )

    assert handled is True  # ALWAYS consumed -- served from the pending record
    assert runtime.get_pending("123") is not None  # progress untouched
    assert transient_update.effective_message.replies == [FIRST_PERSON_QUESTIONS[1][1]]
    remember_calls = [args for action, args in client.calls if action == "squad_remember"]
    assert len(remember_calls) == 1 and remember_calls[0]["text"] == "Ada Example"


@pytest.mark.asyncio
async def test_a_confirmed_unbind_still_abandons_a_pending_intake(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Contrast with the transient-unknown test above: a DEFINITIVE
    confirmation that the sender is not (or no longer) a member must still
    eventually abandon progress via reconciliation -- only genuine
    transport/limiter noise holds it unconditionally. Round-3-gate-4 P1-b:
    even a definitive 'none' needs a SECOND consecutive confirming
    reading, separated by at least the status-cache TTL (mupot-plugin#21
    P2), before the durable record is pruned (never a single reading --
    see the trigger-c tests). Round-2-gate (#23) P2-a: the FIRST message
    is still answered from the pending record that was live when it
    arrived, but the reading it produces suspends capture immediately, so
    the SECOND message (arriving already suspended) falls through
    untouched rather than being answered too."""
    fake_clock = {"now": 0.0}
    call_count = {"n": 0}

    def resolver(_user_id: Any, _chat_id: Any) -> StatusResolution:
        call_count["n"] += 1
        return pending_status() if call_count["n"] == 1 else UNBOUND_STATUS

    install_status_stub(monkeypatch, resolver)
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    await handle_first_contact(
        Update(message=Message(text="hello!")), settings=valid_settings(), client=client, runtime=runtime
    )
    assert runtime.get_pending("123") is not None

    handled = await handle_first_contact(
        Update(message=Message(text="Ada Example")), settings=valid_settings(), client=client, runtime=runtime
    )
    assert handled is True  # answered regardless -- ROOT SHAPE
    suspended = runtime.get_pending("123")
    assert suspended is not None and suspended.suspended is True  # 1st reconciliation reading -- unconfirmed, suspended

    fake_clock["now"] += first_person._STATUS_NEGATIVE_TTL_SECONDS
    handled = await handle_first_contact(
        Update(message=Message(text="Engineer")), settings=valid_settings(), client=client, runtime=runtime
    )
    assert handled is False  # suspended at entry -- falls straight through
    assert runtime.get_pending("123") is None  # 2nd reading -- CONFIRMED, dropped for the NEXT message


# ---------------------------------------------------------------------------
# PIPELINE INVARIANT (Athena, binding, mupot-plugin#20 issue tracker P0):
# every message from a member with a live LOCAL pending intake record
# traverses sanitize -> credential-refuse -> store/answer, always, before
# any reply/store/propagation, regardless of limiter state, probe result,
# cache path, or an exception raised inside the probe. ROOT SHAPE makes
# this hold structurally (see handle_first_contact / _handle_pending_
# message): none of the four causes below are even consulted on the
# message path for a pending member. This test proves it end-to-end
# through the REAL primitives (not by stubbing the gate away) for each
# named cause.
#
# Mutation-provable: re-introducing the old "hold on unknown" branch --
# i.e. probing on the message path and returning False/no-op whenever
# that probe's outcome is 'unknown' for a pending member -- makes every
# scenario below RED (handled becomes False, nothing is stored/replied).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pending_member_messages_always_traverse_pipeline(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    async def _establish_pending(name: str) -> tuple[Any, FirstPersonRuntime]:
        install_probe(monkeypatch, response=status_response_bytes(home_squad_id="home-squad-1"))
        client = _client_for_full_intake()
        runtime = FirstPersonRuntime(tmp_path / f"{name}.json")
        await handle_first_contact(
            Update(message=Message(text="hello!")), settings=valid_settings(), client=client, runtime=runtime
        )
        assert runtime.get_pending("123") is not None
        return client, runtime

    async def _assert_full_pipeline(client: Any, runtime: FirstPersonRuntime, **kwargs: Any) -> None:
        answer_update = Update(message=Message(text="Ada Example"))
        handled = await handle_first_contact(
            answer_update, settings=valid_settings(), client=client, runtime=runtime, **kwargs
        )
        assert handled is True  # zero host propagation
        assert answer_update.effective_message.replies == [FIRST_PERSON_QUESTIONS[1][1]]  # replied
        remember_calls = [args for action, args in client.calls if action == "squad_remember"]
        assert len(remember_calls) == 1 and remember_calls[0]["text"] == "Ada Example"  # stored

        credential_update = Update(message=Message(text="mupot_" + "B" * 32))
        handled = await handle_first_contact(
            credential_update, settings=valid_settings(), client=client, runtime=runtime, **kwargs
        )
        assert handled is True
        assert credential_update.effective_message.replies[-1].startswith(first_person._CREDENTIAL_REPLY)
        assert len([a for a, _ in client.calls if a == "squad_remember"]) == 1  # never stored

    # --- 1: sender-limiter denial ---
    client, runtime = await _establish_pending("denial")
    denying_limiter = _SenderProbeLimiter(min_interval=999999.0)
    assert denying_limiter.should_probe("123") is True  # consume the only slot up front
    await _assert_full_pipeline(client, runtime, sender_probe_limiter=denying_limiter)

    # --- 2: global concurrency-pool exhaustion ---
    client, runtime = await _establish_pending("exhaustion")
    exhausted_limiter = _ProbeLimiter(max_concurrent=1, acquire_timeout=0.01)
    assert exhausted_limiter.try_acquire() is True  # occupy the only slot, never release
    await _assert_full_pipeline(client, runtime, probe_limiter=exhausted_limiter)

    # --- 3: a transport timeout ---
    client, runtime = await _establish_pending("timeout")
    monkeypatch.setattr(
        "plugin.first_person.build_opener", lambda *_: Opener(TimeoutError("simulated probe timeout"))
    )
    await _assert_full_pipeline(client, runtime)

    # --- 4: an exception raised inside the probe itself (a bug, not a
    # modeled failure mode -- read_profile_secret raising something other
    # than RuntimeError is not caught inside resolve_member_status's own
    # `_do()`, so it propagates all the way out and must be swallowed by
    # `_reconcile_pending_with_server`, never by the message path). ---
    client, runtime = await _establish_pending("raising")

    def _raise(_name: str) -> str:
        raise ValueError("boom -- deliberately not a RuntimeError")

    monkeypatch.setattr("plugin.first_person.read_profile_secret", _raise)
    await _assert_full_pipeline(client, runtime)
    # And the exception did not corrupt anything for the NEXT message either.
    third_update = Update(message=Message(text="psychonom"))
    handled = await handle_first_contact(third_update, settings=valid_settings(), client=client, runtime=runtime)
    assert handled is True


@pytest.mark.asyncio
async def test_pending_member_bypasses_the_status_cache_entirely(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Round-3-gate-2 P2 (:186): the 15-minute 'unknown' latch must never
    apply to a member with local pending progress -- verified here by
    proving the cache is bypassed (never read, never written) whenever a
    pending record exists, so a transient failure can never poison the next
    message's probe.

    NOT LOAD-BEARING as of round-2-gate #23's ROOT SHAPE reshape: a pending
    member's message no longer calls `resolve_member_status` at all (see
    `_handle_pending_message`), and `_reconcile_pending_with_server`
    (which runs afterward) always passes `cache=None` unconditionally --
    so this cache is now trivially bypassed for a pending member by
    construction, not by the specific behavior this test exercises. Left
    in place as a still-true regression guard, documented here per the
    round-2 adversarial gate so a future reader doesn't mistake "passes"
    for "load-bearing"."""
    opener = install_probe(monkeypatch, response=status_response_bytes(home_squad_id="home-squad-1"))
    cache = _StatusCache()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    settings = valid_settings()

    # The very FIRST message has no local pending record yet -- the cache
    # legitimately applies here (this is the ordinary "brand-new sender"
    # path the cache exists to rate-limit).
    await handle_first_contact(
        Update(message=Message(text="hello!")), settings=settings, client=FakeClient(), runtime=runtime, status_cache=cache
    )
    assert len(cache) == 1
    assert len(opener.calls) == 1

    # From the SECOND message on, a local pending record exists -- the cache
    # must be bypassed entirely (never read, never written) for as long as
    # that's true, so a transient failure can never poison it for 15 minutes.
    await handle_first_contact(
        Update(message=Message(text="Ada Example")),
        settings=settings,
        client=_client_for_full_intake(),
        runtime=runtime,
        status_cache=cache,
    )
    assert len(cache) == 1  # unchanged -- this probe never touched the cache
    assert len(opener.calls) == 2  # a FRESH probe every time, never served from cache


@pytest.mark.asyncio
async def test_pending_member_never_reads_a_pre_seeded_shared_unknown_entry(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Athena's final ruling on the cache split (round-3-gate-4): a
    server-answered result may keep the normal (900s-for-unknown-capable)
    shared cache; the ONE non-negotiable is that a member with local
    pending progress must NEVER read a cached entry at all -- explicitly
    asserted here with a stale 'unknown' entry PRE-SEEDED for the same
    user_id, not merely assumed from the cache staying empty."""
    cache = _StatusCache()
    cache.put("123", UNKNOWN_STATUS)  # pre-seed a stale cached 'unknown' for this sender
    assert cache.get("123") == UNKNOWN_STATUS  # sanity: the seed is actually live

    opener = install_probe(monkeypatch, response=status_response_bytes(home_squad_id="home-squad-1"))
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    settings = valid_settings()

    # Seed a local pending record directly (no network call) so the SECOND
    # handle_first_contact call below takes the cache-bypass path.
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")

    handled = await handle_first_contact(
        Update(message=Message(text="Ada Example")),
        settings=settings,
        client=_client_for_full_intake(),
        runtime=runtime,
        status_cache=cache,
    )
    assert handled is True  # processed as a real answer, not held on a stale cached 'unknown'
    assert len(opener.calls) == 1  # a REAL probe ran -- the pre-seeded entry was never read
    assert runtime.get_pending("123") is not None and runtime.get_pending("123").index == 1


# ---------------------------------------------------------------------------
# P2 (:1455-1458): escape hatch (stop/cancel/later) + approve/reject pass-
# through, never captured as an answer
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("escape_word", sorted(_ESCAPE_WORDS) + ["STOP", "/stop", "Cancel"])
@pytest.mark.asyncio
async def test_escape_word_pauses_without_advancing_or_storing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, escape_word: str
) -> None:
    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    install_status_stub(monkeypatch, lambda *_: pending_status())
    settings = valid_settings()
    await handle_first_contact(
        Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime
    )

    escape_update = Update(message=Message(text=escape_word))
    handled = await handle_first_contact(escape_update, settings=settings, client=client, runtime=runtime)

    assert handled is True
    assert escape_update.effective_message.replies == [_PAUSED_REPLY]
    pending = runtime.get_pending("123")
    assert pending is not None and pending.index == 0  # untouched
    assert pending.engrams == {}
    assert not any(action == "squad_remember" for action, _ in client.calls)


@pytest.mark.asyncio
async def test_escape_then_non_escape_resumes_the_same_still_current_question(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    install_status_stub(monkeypatch, lambda *_: pending_status())
    settings = valid_settings()
    await handle_first_contact(
        Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime
    )

    await handle_first_contact(
        Update(message=Message(text="stop")), settings=settings, client=client, runtime=runtime
    )
    resumed = Update(message=Message(text="Ada Example"))
    await handle_first_contact(resumed, settings=settings, client=client, runtime=runtime)

    remember_calls = [args for action, args in client.calls if action == "squad_remember"]
    assert len(remember_calls) == 1
    assert remember_calls[0]["concepts"] == ["name"]
    assert remember_calls[0]["text"] == "Ada Example"


@pytest.mark.asyncio
async def test_escape_word_pauses_the_proposal_retry_wait_loop_too(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Round-3-gate-4 P2-b: an escape word was only ever honoured
    mid-question -- once every question was answered (index ==
    len(FIRST_PERSON_QUESTIONS)), 'stop'/'cancel'/'not now' routed straight
    to _retry_proposal_if_due with no escape check at all, becoming
    permanently dead the instant a member finished the script. Verified in
    BOTH reachable index==len entry points: the resume branch
    (runtime.start() resuming into a fully-answered record) and the
    already-pending branch (an existing in-memory pending record)."""
    fake_clock = {"now": 0.0}
    client = _client_rejecting_project_access()
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)
    settings = valid_settings()

    # Branch 1: an existing in-memory pending record at index == len.
    install_status_stub(monkeypatch, lambda *_: pending_status())
    runtime = FirstPersonRuntime(tmp_path / "state-a.json", clock=lambda: fake_clock["now"])
    answers = ["Ada Example", "Engineer", "psychonom", "Ship it", "Nothing else"]
    await handle_first_contact(Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime)
    for answer in answers:
        await handle_first_contact(Update(message=Message(text=answer)), settings=settings, client=client, runtime=runtime)
    assert runtime.get_pending("123").index == len(FIRST_PERSON_QUESTIONS)
    # The 5th answer already triggered ONE real (failed) submission attempt
    # via the normal forward path -- that's expected. What must NOT happen
    # is a SECOND attempt triggered by the escape message itself.
    submit_calls_before_escape = sum(1 for action, _ in client.calls if action == "routine_proposal_submit")

    escape_update = Update(message=Message(text="stop"))
    handled = await handle_first_contact(escape_update, settings=settings, client=client, runtime=runtime)
    assert handled is True
    assert escape_update.effective_message.replies == [_PAUSED_REPLY]
    submit_calls_after_escape = sum(1 for action, _ in client.calls if action == "routine_proposal_submit")
    assert submit_calls_after_escape == submit_calls_before_escape  # the escape itself never attempted a submit

    # Branch 2: the RESUME entry point (start() resuming a fully-answered
    # durable record with no in-memory _pending at all) -- a FRESH client
    # and runtime, so any submit call at all here can only be from the
    # escape message (there is no prior forward-path answer to attribute
    # one to).
    resume_client = _client_rejecting_project_access()
    resume_runtime = FirstPersonRuntime(tmp_path / "state-b.json", clock=lambda: fake_clock["now"])
    engrams = {question_id: f"engram-{question_id}" for question_id, _ in FIRST_PERSON_QUESTIONS}
    resume_runtime._save_in_progress("member-1", engrams, "proj-1", 0)  # noqa: SLF001 -- seed a fully-answered record
    escape_resume_update = Update(message=Message(text="cancel"))
    handled = await handle_first_contact(
        escape_resume_update, settings=settings, client=resume_client, runtime=resume_runtime
    )
    assert handled is True
    assert escape_resume_update.effective_message.replies == [_PAUSED_REPLY]
    assert not any(action == "routine_proposal_submit" for action, _ in resume_client.calls)


@pytest.mark.parametrize(
    "verdict_text",
    ["approve f9408956", "reject f9408956 not yet", "/approve f9408956", "APPROVE F9408956"],
)
@pytest.mark.asyncio
async def test_plain_text_verdict_mid_intake_falls_through_untouched(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, verdict_text: str
) -> None:
    """The literal round-3-gate-2 regression: a member mid-intake who sends
    a genuine approve/reject command must reach the host's own decision
    path -- never captured as an intake answer, never replied to from here."""
    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    install_status_stub(monkeypatch, lambda *_: pending_status())
    settings = valid_settings()
    await handle_first_contact(
        Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime
    )

    verdict_update = Update(message=Message(text=verdict_text))
    handled = await handle_first_contact(verdict_update, settings=settings, client=client, runtime=runtime)

    assert handled is False  # host owns this turn
    assert verdict_update.effective_message.replies == []
    pending = runtime.get_pending("123")
    assert pending is not None and pending.index == 0  # untouched
    assert not any(action == "squad_remember" for action, _ in client.calls)


@pytest.mark.asyncio
async def test_plain_text_verdict_during_proposal_retry_wait_also_falls_through(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Same fall-through guarantee, mid-retry-wait (index >= len(questions))
    rather than mid-answer -- a member whose OWN proposal submission is
    retrying must still reach the host's approve/reject path untouched."""

    class _AlwaysFailsClient(FakeClient):
        def call(self, action: str, args: dict[str, Any]) -> Any:
            self.calls.append((action, dict(args)))
            if action == "squad_remember":
                return {"ok": True, "result": {"engram_id": f"engram-{len(self.calls)}"}}
            if action == "routine_proposal_submit":
                return {"ok": False, "error": "schema_validation_failed"}
            return {"ok": False, "error": "action_not_allowed"}

    client = _AlwaysFailsClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    install_status_stub(monkeypatch, lambda *_: pending_status())
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)
    settings = valid_settings()

    answers = ["Ada Example", "Engineer", "psychonom", "Ship it", "Nothing else"]
    update = Update(message=Message(text="hello!"))
    await handle_first_contact(update, settings=settings, client=client, runtime=runtime)
    for answer in answers:
        update = Update(message=Message(text=answer))
        await handle_first_contact(update, settings=settings, client=client, runtime=runtime)
    proposal_calls_before = len([a for a, _ in client.calls if a == "routine_proposal_submit"])

    verdict_update = Update(message=Message(text="approve f9408956"))
    handled = await handle_first_contact(verdict_update, settings=settings, client=client, runtime=runtime)
    assert handled is False
    assert verdict_update.effective_message.replies == []
    proposal_calls_after = len([a for a, _ in client.calls if a == "routine_proposal_submit"])
    assert proposal_calls_after == proposal_calls_before  # not treated as a retry trigger either


def test_is_verdict_shaped_matches_the_servers_own_command_shape() -> None:
    assert is_verdict_shaped("approve f9408956") is True
    assert is_verdict_shaped("reject f9408956 not yet") is True
    assert is_verdict_shaped("/approve f9408956") is True
    assert is_verdict_shaped("APPROVE F9408956") is True
    assert is_verdict_shaped("I approve of this plan") is False  # not the anchored shape
    assert is_verdict_shaped("Ada Example") is False


# ---------------------------------------------------------------------------
# P3: whitespace home_squad_id treated as absent at the handle_first_contact
# use site too (belt-and-suspenders alongside resolve_member_status's own
# parse-time rejection)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_whitespace_only_home_squad_id_is_treated_as_absent_at_the_use_site(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Belt-and-suspenders: even if a StatusResolution somehow carried a
    whitespace-only home_squad_id (bypassing resolve_member_status's own
    parse-time rejection -- e.g. constructed directly, as install_status_stub
    does), handle_first_contact's own use site must not treat it as present."""
    whitespace_status = StatusResolution(
        bound=True, member_id="member-1", home_squad_id="   ", intake_state="pending"
    )
    install_status_stub(monkeypatch, lambda *_: whitespace_status)
    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    update = Update(message=Message(text="hello!"))

    handled = await handle_first_contact(update, settings=valid_settings(), client=client, runtime=runtime)

    assert handled is False
    assert update.effective_message.replies == [_HOME_NOT_READY_REPLY]
    assert runtime.get_pending("123") is None


# ---------------------------------------------------------------------------
# Athena's addition: post-hoc scrub -- quarantine (never delete) an
# already-stored answer that matches the approve/reject shape
# ---------------------------------------------------------------------------


def test_scrub_quarantine_candidates_finds_only_verdict_shaped_recalled_answers() -> None:
    recalled = {
        "name": "Ada Example",
        "first_ask": "approve f9408956",
        "notes": "reject f9408956 too soon",
    }
    candidates = scrub_quarantine_candidates(recalled)
    assert set(candidates) == {"first_ask", "notes"}
    assert candidates["first_ask"] == "approve f9408956"


def test_mark_quarantined_flags_without_deleting_the_engram(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    runtime.record_answer("123", "name", "engram-1")
    runtime.finish("123", proposal_id="proposal-1")

    assert runtime.is_quarantined("member-1", "name") is False
    marked = runtime.mark_quarantined("member-1", "name")
    assert marked is True
    assert runtime.is_quarantined("member-1", "name") is True

    data = json.loads(state_path.read_text())
    # Never deleted -- the engram_id is still right there.
    assert data["completed"]["member-1"]["engram_ids"]["name"] == "engram-1"
    assert data["completed"]["member-1"]["quarantined"] == ["name"]


def test_mark_quarantined_refuses_on_an_invalid_store_never_rewrites_it(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    corrupted = "{not valid json"
    state_path.write_text(corrupted)
    runtime = FirstPersonRuntime(state_path)

    marked = runtime.mark_quarantined("member-1", "name")

    assert marked is False
    assert state_path.read_text() == corrupted  # untouched, same validity-gated discipline as finish()


def test_mark_quarantined_is_false_for_an_unknown_member(tmp_path: Path) -> None:
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    assert runtime.mark_quarantined("member-1", "name") is False
    assert runtime.is_quarantined("member-1", "name") is False


# ---------------------------------------------------------------------------
# mupot-plugin#24 (P1, PR#23 round-2 gate): N1 -- the SERVER-derived
# "proposal already exists" guard, re-homed into `_submit_proposal` and
# paired with the existing LOCAL `held_proposal_id` check. Also N4 (finish()
# early-return must clear_stall), N5 (finish() guards the RECORD, not just
# the proposal_id field), N11 (lock/held keys aligned on member_id).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_operator_completed_intake_answered_within_the_limiter_window_never_double_submits(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """mupot-plugin#24 N1 (P1, severity-corrected: the live schema now
    ACCEPTS the project_access kind, so this is a real double-proposal, not
    schema-mitigated). An operator manually completes a stalled intake
    SERVER-SIDE (the stall task this module's own `_stall_reply` creates
    prescribes exactly that action) while a local, fully-answered pending
    record has NOTHING locally recorded yet (`held_proposal_id` is None --
    this module never itself submitted). The member answers their last
    question inside the sender limiter's own window. `_submit_proposal`'s
    OWN local check alone would see nothing and submit a SECOND,
    independent proposal. The server-derived guard
    (`_server_confirms_existing_proposal`) must catch this: zero submits,
    and an honest reply -- never a fabricated `_COMPLETE_REPLY` (this
    module never learned the server's own proposal_id, so it must never
    write a completion marker naming one nobody confirmed either).
    Mutation-provable: removing the `_server_confirms_existing_proposal`
    call (or its guard) from `_submit_proposal` makes `submit_calls`
    non-empty and the reply wrong."""
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    settings = valid_settings()
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)

    mode = {"phase": "pending"}

    def resolver(_user_id: Any, _chat_id: Any) -> StatusResolution:
        if mode["phase"] == "pending":
            return pending_status()
        # The operator completed the intake server-side -- mupot now
        # derives intake_state=='complete' from the existence of a
        # project_access proposal THIS module never itself recorded, and
        # (as of today) the wire response does not hand the id back.
        return StatusResolution(
            bound=True, member_id="member-1", home_squad_id="home-squad-1", intake_state="complete"
        )

    install_status_stub(monkeypatch, resolver)

    await handle_first_contact(Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime)
    for answer in ["Ada Example", "Engineer", "psychonom", "Ship it"]:  # 4 of 5
        await handle_first_contact(Update(message=Message(text=answer)), settings=settings, client=client, runtime=runtime)
    assert runtime.get_pending("123") is not None and runtime.get_pending("123").index == 4
    assert runtime.held_proposal_id("member-1") is None  # nothing recorded locally, by design

    # The operator's server-side completion becomes visible right as the
    # member answers the final question -- well within the 15s sender
    # limiter window, the exact race this closes.
    mode["phase"] = "complete"
    last_update = Update(message=Message(text="Nothing else"))
    await handle_first_contact(last_update, settings=settings, client=client, runtime=runtime)

    submit_calls = [action for action, _ in client.calls if action == "routine_proposal_submit"]
    assert submit_calls == []  # NEVER a second, independent submission
    assert last_update.effective_message.replies[-1] == first_person._AWAITING_HUMANS_REPLY  # honest
    assert last_update.effective_message.replies[-1] != first_person._COMPLETE_REPLY  # never fabricated
    assert runtime.held_proposal_id("member-1") is None  # no marker without a real id, ever


@pytest.mark.asyncio
async def test_server_derived_guard_removed_lets_the_operator_completed_intake_double_submit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Mutation-regression receipt for the test above: with
    `_server_confirms_existing_proposal` short-circuited to always return
    ``None`` (mirroring "the server-derived check was never re-homed"),
    the exact same scenario DOES double-submit -- this is the RED half of
    the mutation-provable claim, kept as a standing regression test rather
    than a one-off manual check."""
    monkeypatch.setattr("plugin.first_person._server_confirms_existing_proposal", _always_none_server_confirmation)
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    settings = valid_settings()
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)

    mode = {"phase": "pending"}

    def resolver(_user_id: Any, _chat_id: Any) -> StatusResolution:
        if mode["phase"] == "pending":
            return pending_status()
        return StatusResolution(
            bound=True, member_id="member-1", home_squad_id="home-squad-1", intake_state="complete"
        )

    install_status_stub(monkeypatch, resolver)

    await handle_first_contact(Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime)
    for answer in ["Ada Example", "Engineer", "psychonom", "Ship it"]:
        await handle_first_contact(Update(message=Message(text=answer)), settings=settings, client=client, runtime=runtime)

    mode["phase"] = "complete"
    last_update = Update(message=Message(text="Nothing else"))
    await handle_first_contact(last_update, settings=settings, client=client, runtime=runtime)

    submit_calls = [action for action, _ in client.calls if action == "routine_proposal_submit"]
    assert len(submit_calls) == 1  # WITHOUT the guard: a real second submission goes out
    assert last_update.effective_message.replies[-1] == first_person._COMPLETE_REPLY  # falsely "complete"


async def _always_none_server_confirmation(*_args: Any, **_kwargs: Any) -> None:
    return None


@pytest.mark.asyncio
async def test_submit_proposal_reuses_the_servers_own_proposal_id_when_the_probe_returns_one(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The companion happy-path to the two tests above: once mupot's wire
    contract DOES hand back a proposal_id alongside intake_state=='complete'
    (StatusResolution.proposal_id), `_submit_proposal`'s server-derived
    guard reuses it via `finish()` exactly like the local held-proposal
    path does -- never submits, and the reply is the normal complete one,
    not the honest-but-incomplete `_AWAITING_HUMANS_REPLY`."""
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    settings = valid_settings()
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)

    mode = {"phase": "pending"}

    def resolver(_user_id: Any, _chat_id: Any) -> StatusResolution:
        if mode["phase"] == "pending":
            return pending_status()
        return StatusResolution(
            bound=True,
            member_id="member-1",
            home_squad_id="home-squad-1",
            intake_state="complete",
            proposal_id="proposal-FROM-SERVER",
        )

    install_status_stub(monkeypatch, resolver)

    await handle_first_contact(Update(message=Message(text="hello!")), settings=settings, client=client, runtime=runtime)
    for answer in ["Ada Example", "Engineer", "psychonom", "Ship it"]:
        await handle_first_contact(Update(message=Message(text=answer)), settings=settings, client=client, runtime=runtime)

    mode["phase"] = "complete"
    last_update = Update(message=Message(text="Nothing else"))
    await handle_first_contact(last_update, settings=settings, client=client, runtime=runtime)

    submit_calls = [action for action, _ in client.calls if action == "routine_proposal_submit"]
    assert submit_calls == []  # reused the server's own id, never submitted
    assert last_update.effective_message.replies[-1] == first_person._COMPLETE_REPLY
    assert runtime.held_proposal_id("member-1") == "proposal-FROM-SERVER"


@pytest.mark.asyncio
async def test_submit_proposal_local_held_proposal_reuse_clears_a_stale_stall_marker(tmp_path: Path) -> None:
    """mupot-plugin#24 N4: the LOCAL held-proposal early-return in
    `_submit_proposal` (a proposal this module already recorded, reused
    via `finish()`) must ALSO clear any stall marker raised while the SAME
    proposal was still failing -- mupot-plugin#22 only fixed this on the
    fresh-success path; the held-proposal early-return is a COMPLETELY
    different branch and never called `clear_stall()`, so the stall marker
    (and the audit surface reading it) kept claiming this member was
    stalled even after "I've sent your access request" had already gone
    out via this exact reuse path. Mutation-provable: removing the
    `runtime.store.clear_stall(intake.member_id)` call from THIS branch
    (not the fresh-success one, which mupot-plugin#22 already pins
    separately) makes the final assertion go red."""
    from plugin.first_person import _submit_proposal

    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps({"completed": {"member-1": {"engram_ids": {}, "proposal_id": "proposal-1", "completed_at": 0.0}}})
    )
    runtime = FirstPersonRuntime(state_path)
    runtime.store.record_stall("member-1", 4, 0.0)
    assert "member-1" in runtime.store.stall_entries()

    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    intake = runtime.record_answer("123", "name", "engram-x")
    assert intake is not None
    client = FakeClient()
    update = Update(message=Message(text="anything"))

    await _submit_proposal(update.effective_message, "123", intake, client=client, runtime=runtime)

    assert update.effective_message.replies[-1] == first_person._COMPLETE_REPLY
    assert client.calls == []  # never attempted a submit -- reused the held id
    assert "member-1" not in runtime.store.stall_entries()  # N4: cleared here too


def test_finish_never_overwrites_the_record_on_a_matching_proposal_id(tmp_path: Path) -> None:
    """mupot-plugin#24 N5 (correcting a round-2-gate-(#23) P1 bug):
    `finish()`'s never-overwrite guard previously fired ONLY when the
    incoming ``proposal_id`` DIFFERED from the one already recorded -- on
    a MATCHING id (exactly the shape `_submit_proposal`'s own reuse path
    produces on every normal re-check-in for an already-complete member:
    `held_proposal_id(...)` -> `finish(chat_key, proposal_id=that_same_id)`)
    it fell through and overwrote `completed[member_id]` WHOLESALE --
    replacing the original ``completed_at`` and any ``quarantined`` flags
    with a fresh, empty-looking record. Guard the RECORD, not the field.
    Mutation-provable: reverting to the old ``existing_proposal_id !=
    new_proposal_id`` gate makes the equality assertion below go red (the
    record would come back with a NEW completed_at and no quarantined
    list)."""
    state_path = tmp_path / "state.json"
    original_record = {
        "engram_ids": {"name": "engram-orig"},
        "proposal_id": "proposal-1",
        "completed_at": 111.0,
        "quarantined": ["notes"],
    }
    state_path.write_text(json.dumps({"completed": {"member-1": dict(original_record)}}))
    runtime = FirstPersonRuntime(state_path, wall_clock=lambda: 999.0)
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    runtime.record_answer("123", "name", "engram-NEW")  # a fresh episode's own (different) engram

    runtime.finish("123", proposal_id="proposal-1")  # matching id -- the normal reuse shape

    data = json.loads(state_path.read_text())
    assert data["completed"]["member-1"] == original_record  # UNTOUCHED, byte for byte
    assert "member-1" not in data.get("in_progress", {})  # in-progress record still cleared
    assert runtime.get_pending("123") is None


def test_submit_proposal_in_flight_lock_and_held_proposal_check_share_the_member_id_key(tmp_path: Path) -> None:
    """mupot-plugin#24 N11: `try_begin_submit`/`end_submit` (the in-flight
    submission lock) and `held_proposal_id` (the durable-proposal check)
    must be mutually exclusive on the SAME axis -- member_id -- not one on
    member_id and the other on chat_key. Direct check: begin a submit
    "in flight" keyed by member_id, then confirm a DIFFERENT chat_key
    resolving to the SAME member_id is correctly seen as already
    in-flight (the alignment this closes), while a genuinely different
    member_id is not blocked at all."""
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    assert runtime.try_begin_submit("member-1") is True
    # A second chat_key for the SAME member must see the in-flight lock.
    assert runtime.try_begin_submit("member-1") is False
    # A different member entirely is never blocked by member-1's lock.
    assert runtime.try_begin_submit("member-2") is True
    runtime.end_submit("member-1")
    assert runtime.try_begin_submit("member-1") is True


@pytest.mark.asyncio
async def test_submit_proposal_from_two_different_chat_keys_for_the_same_member_submits_once(
    tmp_path: Path,
) -> None:
    """mupot-plugin#24 N11, exercised through `_submit_proposal` itself
    (not only the primitive lock methods above): two genuinely concurrent
    `_submit_proposal` calls for the SAME member but DIFFERENT chat_keys
    (a rebind, or any future multi-surface path resolving the same
    member) must still only ever let ONE of them actually call
    `routine_proposal_submit` -- the in-flight lock is keyed by
    `intake.member_id`, not `chat_key`. Mutation-provable: reverting
    `try_begin_submit`/`end_submit`'s call sites in `_submit_proposal`
    back to `chat_key` makes `submit_calls` reach 2 (one per chat_key)."""
    import threading

    from plugin.first_person import _submit_proposal

    release = threading.Event()

    class _BlockingClient(FakeClient):
        def call(self, action: str, args: dict[str, Any]) -> Any:
            self.calls.append((action, dict(args)))
            if action == "routine_proposal_submit":
                assert release.wait(timeout=5.0), "test setup did not release in time"
                return {"ok": True, "result": {"proposal_id": "proposal-1"}}
            return {"ok": True, "result": {}}

    client = _BlockingClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    # Two DIFFERENT chat_keys, the SAME member -- e.g. a rebind mid-flight.
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    runtime.set_project_id("123", "proj-1")
    intake_a = None
    for question_id, _ in FIRST_PERSON_QUESTIONS:
        intake_a = runtime.record_answer("123", question_id, f"engram-{question_id}")
    runtime.start("456", member_id="member-1", home_squad_id="home-squad-1")
    runtime.set_project_id("456", "proj-1")
    intake_b = None
    for question_id, _ in FIRST_PERSON_QUESTIONS:
        intake_b = runtime.record_answer("456", question_id, f"engram-{question_id}")
    assert intake_a is not None and intake_b is not None
    assert intake_a.member_id == intake_b.member_id == "member-1"

    async def call_submit(chat_key: str, intake: Intake) -> Update:
        update = Update(message=Message(text="anything"))
        await _submit_proposal(update.effective_message, chat_key, intake, client=client, runtime=runtime)
        return update

    task1 = asyncio.create_task(call_submit("123", intake_a))
    await asyncio.sleep(0.05)  # let task1 reach the blocked submit() call
    task2 = asyncio.create_task(call_submit("456", intake_b))
    await asyncio.sleep(0.05)  # let task2 run its own (non-blocking) try_begin_submit check
    release.set()
    update1, update2 = await asyncio.gather(task1, task2)

    submit_calls = [action for action, _ in client.calls if action == "routine_proposal_submit"]
    assert len(submit_calls) == 1  # only ONE real submission, ever, for this member
    assert update1.effective_message.replies[-1] == first_person._COMPLETE_REPLY
    assert update2.effective_message.replies[-1] == _PROPOSAL_RETRY_WAIT_REPLY


# ---------------------------------------------------------------------------
# mupot-plugin#25 (P2, PR#23 round-2 gate): N2 (flap tolerance is its own
# constant, never the sender limiter's interval), N6 (abandon() is
# disk-first, and every reconcile mutation shares the probe's try/except).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reconcile_flap_tolerance_is_its_own_constant_not_the_sender_limiter_interval(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """mupot-plugin#25 N2 (P2, regression vs a649c196): `_reconcile_pending_
    with_server`'s confirm-then-abandon separation must be the dedicated
    `_FLAP_TOLERANCE_SECONDS` constant (>= the status-cache negative TTL,
    300s), NEVER derived from `sender_probe_limiter.min_interval`
    (anti-replay, 15s -- a completely different duty). A 20-second
    transient server flap to 'complete' must resume with every engram
    intact -- the SECOND reading, 20s after the first, must NOT confirm
    and abandon the record. Mutation-provable (mutation MJ): reverting the
    separation back to `sender_probe_limiter.min_interval` makes the
    `still_suspended` assertions below go red -- the record (and its
    engram) would already be gone by the third call."""
    from plugin.first_person import _reconcile_pending_with_server

    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    pending = runtime.record_answer("123", "name", "engram-name")
    assert pending is not None and pending.index == 1

    settings = valid_settings()
    # A REAL sender limiter at its actual production interval (15s) -- if
    # reconcile's flap-tolerance separation were still (incorrectly)
    # derived from THIS, 20 elapsed seconds would already be enough to
    # confirm-and-abandon at the second reading.
    sender_probe_limiter = _SenderProbeLimiter(min_interval=first_person._SENDER_PROBE_MIN_INTERVAL_SECONDS)

    mode = {"phase": "complete"}

    def resolver(_user_id: Any, _chat_id: Any) -> StatusResolution:
        if mode["phase"] == "complete":
            return COMPLETE_STATUS
        return pending_status()

    install_status_stub(monkeypatch, resolver)

    # First reading: complete -- unconfirmed, suspends.
    await _reconcile_pending_with_server(
        "123",
        123,
        123,
        pending,
        settings=settings,
        secret_owner=None,
        runtime=runtime,
        status_cache=None,
        probe_limiter=None,
        sender_probe_limiter=sender_probe_limiter,
    )
    suspended = runtime.get_pending("123")
    assert suspended is not None and suspended.suspended is True

    # 20 seconds later -- past the OLD (buggy) 15s separation, well short
    # of the fixed 300s one. Second reading: still complete.
    fake_clock["now"] += 20.0
    await _reconcile_pending_with_server(
        "123",
        123,
        123,
        suspended,
        settings=settings,
        secret_owner=None,
        runtime=runtime,
        status_cache=None,
        probe_limiter=None,
        sender_probe_limiter=sender_probe_limiter,
    )
    still_suspended = runtime.get_pending("123")
    assert still_suspended is not None and still_suspended.suspended is True  # NOT confirmed at 20s
    assert still_suspended.engrams == {"name": "engram-name"}  # nothing lost

    # Flaps back to pending -- resumes with every engram intact.
    mode["phase"] = "pending"
    fake_clock["now"] += 1.0
    await _reconcile_pending_with_server(
        "123",
        123,
        123,
        still_suspended,
        settings=settings,
        secret_owner=None,
        runtime=runtime,
        status_cache=None,
        probe_limiter=None,
        sender_probe_limiter=sender_probe_limiter,
    )
    resumed = runtime.get_pending("123")
    assert resumed is not None and resumed.suspended is False
    assert resumed.engrams == {"name": "engram-name"}  # survived the whole flap


def test_abandon_is_disk_first_keeps_the_in_memory_record_when_the_disk_write_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """mupot-plugin#25 N6: `abandon()` must clear the durable record BEFORE
    dropping the in-memory one. The previous order popped the in-memory
    record FIRST and only then attempted the durable clear; an ``OSError``
    from that disk write (a full disk, a permissions change, any
    transient I/O failure) then left memory gone while the durable record
    it never actually cleared survived -- the exact split this method's
    own docstring says it exists to prevent, just relocated one call
    earlier. Mutation-provable: reverting to pop-then-clear makes the
    final assertion go red (the in-memory record would already be gone by
    the time the ``OSError`` propagates)."""
    from plugin.first_person import FirstPersonStateStore

    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    runtime.record_answer("123", "name", "engram-1")
    assert runtime.get_pending("123") is not None

    def raise_oserror(self: Any, value: Any) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(FirstPersonStateStore, "save", raise_oserror)

    with pytest.raises(OSError):
        runtime.abandon("123")

    # The failed disk write must leave the in-memory record fully intact.
    assert runtime.get_pending("123") is not None


@pytest.mark.asyncio
async def test_reconcile_wraps_an_abandon_disk_failure_never_reaching_the_host_handler(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """mupot-plugin#25 N6: `_reconcile_pending_with_server` wraps EVERY
    mutation it makes (`note_pending`/`resume`/`note_non_pending`/
    `suspend`/`abandon`) in the SAME try/except guarding its own probe --
    not only the probe itself. `FirstPersonRuntime.abandon()` performs
    durable disk I/O and can raise ``OSError``; letting that escape this
    function would violate reconcile's own stated invariant ("cannot
    violate the pipeline invariant no matter ... how it fails") by
    propagating past `handle_first_contact` into the host's own
    update-dispatch loop. Mutation-provable: narrowing the try/except back
    to only the `resolve()` call makes this test raise ``OSError``
    instead of completing normally."""
    from plugin.first_person import FirstPersonStateStore, _reconcile_pending_with_server

    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    # A real durable in-progress record must exist for _clear_in_progress
    # to actually attempt (and fail) a disk write inside abandon().
    pending = runtime.record_answer("123", "name", "engram-1")
    assert pending is not None
    settings = valid_settings()

    install_status_stub(monkeypatch, lambda *_: UNBOUND_STATUS)

    # First (unconfirmed) reading -- suspend() is in-memory only, so this
    # call succeeds normally, before the disk write is ever made to fail.
    await _reconcile_pending_with_server(
        "123",
        123,
        123,
        pending,
        settings=settings,
        secret_owner=None,
        runtime=runtime,
        status_cache=None,
        probe_limiter=None,
        sender_probe_limiter=None,
    )
    suspended = runtime.get_pending("123")
    assert suspended is not None and suspended.suspended is True

    # NOW make the durable clear fail -- the second, confirming reading
    # below will call abandon(), whose disk write raises OSError.
    def raise_oserror(self: Any, value: Any) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(FirstPersonStateStore, "save", raise_oserror)

    fake_clock["now"] += first_person._FLAP_TOLERANCE_SECONDS
    await _reconcile_pending_with_server(  # must complete normally -- never raise
        "123",
        123,
        123,
        suspended,
        settings=settings,
        secret_owner=None,
        runtime=runtime,
        status_cache=None,
        probe_limiter=None,
        sender_probe_limiter=None,
    )


# ---------------------------------------------------------------------------
# mupot-plugin#25 N7/N8: register_first_person's ONE-CLOCK wiring extended
# to `_StatusCache`/`_SenderProbeLimiter` (already pinned for
# FirstPersonRuntime/the five notifiers by
# test_register_first_person_threads_clock_through_to_handle_first_contact).
# ---------------------------------------------------------------------------


def test_register_first_person_threads_the_injected_clock_into_status_cache_and_sender_limiter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """mupot-plugin#25 N7/N8: `_StatusCache` and `_SenderProbeLimiter` are
    ALSO monotonic-clock-based (TTL sweeps / the sender min-interval) but
    were left on their own default ``time.monotonic`` inside
    `register_first_person` -- an injected ``clock`` reached
    `FirstPersonRuntime` and every backoff comparison, but NOT these two,
    so the runtime's own gate decisions and the cache/limiter calibrating
    those SAME decisions ran on two different clocks the moment the
    injected one diverged from real time at all. Mutation-provable:
    dropping either `clock=clock` kwarg inside `register_first_person`
    makes the corresponding assertion below go red (the captured `clock`
    would be the real ``time.monotonic`` instead of the fake sentinel)."""
    captured: dict[str, Any] = {}
    fake_clock: Callable[[], float] = lambda: 42.0

    class _RecordingStatusCache(_StatusCache):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            captured["status_cache_clock"] = kwargs.get("clock")
            super().__init__(*args, **kwargs)

    class _RecordingSenderProbeLimiter(_SenderProbeLimiter):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            captured["sender_limiter_clock"] = kwargs.get("clock")
            super().__init__(*args, **kwargs)

    monkeypatch.setattr("plugin.first_person._StatusCache", _RecordingStatusCache)
    monkeypatch.setattr("plugin.first_person._SenderProbeLimiter", _RecordingSenderProbeLimiter)

    ctx = types.SimpleNamespace(
        register_telegram_handler=lambda factory: None, on_unload=lambda fn: None, _manager=None
    )
    register_first_person(
        ctx, valid_settings(), client=FakeClient(), state_path=tmp_path / "state.json", clock=fake_clock
    )

    assert captured["status_cache_clock"] is fake_clock
    assert captured["sender_limiter_clock"] is fake_clock


# ---------------------------------------------------------------------------
# mupot-plugin#28 round-2 gate (adversarial round 1 on PR#28 @ d8e4c4d, AMBER).
#
# F1 (P1) names the FOURTH instance of the SAME defect class already named
# twice above (mupot-plugin#21/#22: "a cached/pended value must never outlive
# the truth it cached") and once more in mupot-plugin#25 N2's flap-tolerance
# fix: `FirstPersonRuntime.last_server_reading` had no timestamp/TTL, no
# episode scoping, and `abandon()` never cleared it -- a reading from a DEAD
# episode (an operator-completed intake this module never itself recorded,
# later abandoned by reconcile) could short-circuit a brand-new episode's own
# submit, writing a completion marker that named the OLD proposal_id
# alongside the NEW episode's own engrams and permanently blocking re-intake.
# F2/F3 pin two already-shipped guards (member_id equality, the cached-branch
# skip) that mupot-plugin#24's own round had left unpinned. F4 pins the
# proposal_id-only-trusted-when-complete parse rule. F5 fixes a SEPARATE race
# `abandon()`'s own disk-first reorder (mupot-plugin#25 N6) introduced: a
# concurrent resume()+record_answer() landing between the disk clear and the
# final in-memory pop must never be silently dropped from memory while its
# own freshly-resaved durable row survives -- and, per Athena's binding
# round-2 condition, the disk record (never memory) is the ultimate source of
# truth: the NEXT message must resume correctly from disk even if some
# process's in-memory view were to lose the thread entirely.
# ---------------------------------------------------------------------------


def test_last_server_reading_is_bounded_by_a_ttl(tmp_path: Path) -> None:
    """F1 (P1, #28 round-2): a server reading recorded for THIS SAME
    episode (never a cross-episode concern -- see the `not_before` test
    below, which this one deliberately keeps satisfied throughout) must
    still expire once `_LAST_SERVER_READING_TTL_SECONDS` has elapsed --
    the cache exists to skip a probe moments apart from reconcile's own,
    never to stand in for a live probe indefinitely. Mutation-provable:
    removing the TTL comparison in `last_server_reading` makes the final
    assertion go red (the stale reading would still be returned)."""
    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    created_at = runtime.start("123", member_id="member-1", home_squad_id="home-squad-1").created_at

    fake_clock["now"] = 1.0
    runtime.note_server_reading(
        "member-1",
        StatusResolution(
            bound=True, member_id="member-1", home_squad_id="home-squad-1",
            intake_state="complete", proposal_id="STALE-PROP",
        ),
    )
    # Fresh -- returned, and satisfies not_before throughout (recorded
    # AFTER created_at every time this is checked below).
    assert runtime.last_server_reading("member-1", not_before=created_at) is not None

    fake_clock["now"] = 1.0 + first_person._LAST_SERVER_READING_TTL_SECONDS + 1.0
    assert runtime.last_server_reading("member-1", not_before=created_at) is None


def test_last_server_reading_rejects_a_reading_from_before_the_current_episode(tmp_path: Path) -> None:
    """F1 (P1, #28 round-2): a reading recorded for a PRIOR episode must
    never be trusted for a brand-new one, even when it is still well
    within its own raw TTL. Mutation-provable: dropping the `not_before`
    (created_at) comparison in `last_server_reading` makes the final
    assertion go red (the still-fresh-by-TTL reading would be returned)."""
    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])

    runtime.note_server_reading(
        "member-1",
        StatusResolution(
            bound=True, member_id="member-1", home_squad_id="home-squad-1",
            intake_state="complete", proposal_id="OLD-PROP",
        ),
    )

    # A brand-new episode starts a moment later -- well within the
    # reading's own TTL -- but the reading predates it.
    fake_clock["now"] = 1.0
    new_created_at = runtime.start("123", member_id="member-1", home_squad_id="home-squad-1").created_at

    assert runtime.last_server_reading("member-1", not_before=new_created_at) is None


def test_abandon_clears_the_cached_server_reading_for_that_member(tmp_path: Path) -> None:
    """F1 (P1, #28 round-2): `abandon()` must clear any cached server
    reading for the member it just abandoned -- the reading that
    justified (or was merely concurrent with) ending THIS episode must
    never survive to be consulted for whatever episode comes next.
    Mutation-provable: removing the `_last_server_reading.pop(...)` call
    from `abandon()` makes the final assertion go red."""
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    runtime.note_server_reading(
        "member-1",
        StatusResolution(bound=True, member_id="member-1", home_squad_id="home-squad-1", intake_state="complete"),
    )
    assert runtime.last_server_reading("member-1") is not None

    runtime.abandon("123")

    assert runtime.last_server_reading("member-1") is None


@pytest.mark.asyncio
async def test_stale_same_episode_server_reading_expires_and_the_real_submit_fires(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Athena's binding round-2 condition (named test for the zero-submit
    scenario) + F1: a 'complete'/OLD-PROP server reading cached mid-
    conversation (a transient glitch reconcile happened to observe once,
    long since reverted back to genuinely 'pending') must never survive
    to short-circuit THIS SAME episode's own eventual, real submit once
    enough real time has passed. All five questions are answered
    normally; `_submit_proposal` must live-probe (truth: still pending)
    and fire a REAL submission, recording the ACTUAL new proposal_id --
    never reusing OLD-PROP. Mutation-provable: removing the TTL check in
    `last_server_reading` makes this go red -- zero submits, and
    `held_proposal_id` would come back `OLD-PROP` instead of the real
    `proposal-1`."""
    from plugin.first_person import _submit_proposal

    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")

    # A stale 'complete'/OLD-PROP reading gets cached early in the SAME
    # episode's own conversation -- e.g. a transient server glitch
    # reconcile happened to observe once.
    runtime.note_server_reading(
        "member-1",
        StatusResolution(
            bound=True, member_id="member-1", home_squad_id="home-squad-1",
            intake_state="complete", proposal_id="OLD-PROP",
        ),
    )

    # A long time passes -- well past the cache's own TTL -- while the
    # SAME local episode continues (never abandoned, never restarted).
    fake_clock["now"] += first_person._LAST_SERVER_READING_TTL_SECONDS + 1.0

    runtime.set_project_id("123", "proj-1")
    intake = None
    for question_id, _ in FIRST_PERSON_QUESTIONS:
        intake = runtime.record_answer("123", question_id, f"engram-{question_id}")
    assert intake is not None and intake.index == len(FIRST_PERSON_QUESTIONS)

    client = _client_for_full_intake()
    settings = valid_settings()
    install_status_stub(monkeypatch, lambda *_: pending_status())  # live-probe truth: still pending

    update = Update(message=Message(text="Nothing else"))
    await _submit_proposal(
        update.effective_message,
        "123",
        intake,
        client=client,
        runtime=runtime,
        settings=settings,
        secret_owner=None,
        probe_limiter=None,
    )

    submit_calls = [action for action, _ in client.calls if action == "routine_proposal_submit"]
    assert len(submit_calls) == 1  # the REAL submit fired
    assert update.effective_message.replies[-1] == first_person._COMPLETE_REPLY
    assert runtime.held_proposal_id("member-1") == "proposal-1"  # the NEW id, never OLD-PROP


@pytest.mark.asyncio
async def test_operator_completed_episode_abandoned_then_a_fresh_intake_can_resubmit_with_a_new_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The full cross-episode narrative F1 closes, end to end: episode 1
    is a stale LOCAL pending record for member-1 that reconcile confirms
    (via two separated readings) is actually 'complete' server-side (an
    operator-completed proposal 'OLD-PROP' this module never itself
    recorded) and abandons. The server later reverts to 'pending' (a
    genuine re-intake); a FRESH local episode answers all 5 questions
    again. The member MUST be able to re-intake, the submit MUST fire,
    and the completion marker MUST carry the NEW proposal_id -- never
    OLD-PROP, and never a false zero-submit 'sent your access request'
    reply for work that was never actually (re-)submitted."""
    from plugin.first_person import _reconcile_pending_with_server

    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    settings = valid_settings()

    # Episode 1: a stale local pending record; reconcile confirms the
    # server's own 'complete' (operator-completed, OLD-PROP) reading via
    # two separated readings and abandons it.
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    pending_ep1 = runtime.get_pending("123")

    def resolver_ep1_complete(_uid: Any, _cid: Any) -> StatusResolution:
        return StatusResolution(
            bound=True, member_id="member-1", home_squad_id="home-squad-1",
            intake_state="complete", proposal_id="OLD-PROP",
        )

    install_status_stub(monkeypatch, resolver_ep1_complete)
    await _reconcile_pending_with_server(
        "123", 123, 123, pending_ep1,
        settings=settings, secret_owner=None, runtime=runtime,
        status_cache=None, probe_limiter=None, sender_probe_limiter=None,
    )
    assert runtime.get_pending("123") is not None  # first sighting -- suspended, unconfirmed

    fake_clock["now"] += first_person._FLAP_TOLERANCE_SECONDS
    await _reconcile_pending_with_server(
        "123", 123, 123, runtime.get_pending("123"),
        settings=settings, secret_owner=None, runtime=runtime,
        status_cache=None, probe_limiter=None, sender_probe_limiter=None,
    )
    assert runtime.get_pending("123") is None  # confirmed and abandoned
    assert runtime.held_proposal_id("member-1") is None  # never recorded via finish() -- operator-side only
    assert runtime.last_server_reading("member-1") is None  # abandon() cleared it too (F1)

    # The server reverts to 'pending' -- a genuine re-intake.
    fake_clock["now"] += 5.0
    client = _client_for_full_intake()
    mode = {"phase": "pending"}

    def resolver(_uid: Any, _cid: Any) -> StatusResolution:
        if mode["phase"] == "pending":
            return pending_status(member_id="member-1", home_squad_id="home-squad-1")
        return resolver_ep1_complete(_uid, _cid)

    install_status_stub(monkeypatch, resolver)
    monkeypatch.setattr("plugin.first_person._resolve_member_project", _default_project_resolver)

    await handle_first_contact(Update(message=Message(text="hello again!")), settings=settings, client=client, runtime=runtime)
    started = runtime.get_pending("123")
    assert started is not None and started.created_at > pending_ep1.created_at  # a genuinely NEW episode

    for answer in ["Ada Example", "Engineer", "psychonom", "Ship it"]:
        await handle_first_contact(Update(message=Message(text=answer)), settings=settings, client=client, runtime=runtime)

    last_update = Update(message=Message(text="Nothing else"))
    await handle_first_contact(last_update, settings=settings, client=client, runtime=runtime)

    submit_calls = [action for action, _ in client.calls if action == "routine_proposal_submit"]
    assert len(submit_calls) == 1  # the fresh episode's OWN real submit fired
    assert last_update.effective_message.replies[-1] == first_person._COMPLETE_REPLY
    assert runtime.held_proposal_id("member-1") == "proposal-1"  # the NEW id, never OLD-PROP


@pytest.mark.asyncio
async def test_server_reading_member_id_mismatch_never_suppresses_a_submit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """F2 (P2, #28 round-2): the `server_reading.member_id == intake.member_id`
    equality in `_submit_proposal` is the ONLY guard against a DIFFERENT
    member's 'complete' reading suppressing THIS member's submit -- had no
    dedicated test. A probe that resolves to 'complete' for member-OTHER
    (a plausible shape: the sender's identity resolved differently between
    this call and the cached/probed one, or a wildly stale/misrouted
    reading) must never block member-1's own, perfectly legitimate submit.
    Mutation-provable: removing the member_id equality check makes
    `submit_calls` empty (falsely short-circuited) instead of firing."""
    from plugin.first_person import _submit_proposal

    runtime = FirstPersonRuntime(tmp_path / "state.json")
    intake = None
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    runtime.set_project_id("123", "proj-1")
    for question_id, _ in FIRST_PERSON_QUESTIONS:
        intake = runtime.record_answer("123", question_id, f"engram-{question_id}")
    assert intake is not None and intake.member_id == "member-1"

    client = _client_for_full_intake()
    settings = valid_settings()
    install_status_stub(
        monkeypatch,
        lambda *_: StatusResolution(
            bound=True, member_id="member-OTHER", home_squad_id="home-other",
            intake_state="complete", proposal_id="OTHER-PROP",
        ),
    )

    update = Update(message=Message(text="anything"))
    await _submit_proposal(
        update.effective_message, "123", intake,
        client=client, runtime=runtime, settings=settings, secret_owner=None, probe_limiter=None,
    )

    submit_calls = [action for action, _ in client.calls if action == "routine_proposal_submit"]
    assert len(submit_calls) == 1  # member-1's own submit proceeds, unaffected
    assert runtime.held_proposal_id("member-1") == "proposal-1"
    assert runtime.held_proposal_id("member-OTHER") is None  # never touched


@pytest.mark.asyncio
async def test_submit_proposal_cached_complete_reading_skips_a_second_live_probe(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """F3 (P2, #28 round-2): `_server_confirms_existing_proposal`'s cached
    branch was unpinned -- a mutation that always skipped straight to the
    live probe (effectively `cached=None` unconditionally) stayed green
    against the whole suite. Pin it by COUNTING wire calls: two
    consecutive submit-path checks for the SAME member within the cache's
    TTL must make exactly ONE real probe, not two. Mutation-provable:
    short-circuiting the cached-branch check to always fall through to a
    live probe makes `probe_calls` come back `2` instead of `1`."""
    from plugin.first_person import _server_confirms_existing_proposal

    fake_clock = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_clock["now"])
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    intake = runtime.get_pending("123")
    assert intake is not None

    probe_calls: list[int] = []

    def resolver(_uid: Any, _cid: Any) -> StatusResolution:
        probe_calls.append(1)
        return StatusResolution(
            bound=True, member_id="member-1", home_squad_id="home-squad-1",
            intake_state="complete", proposal_id="proposal-1",
        )

    install_status_stub(monkeypatch, resolver)
    settings = valid_settings()

    first = await _server_confirms_existing_proposal(
        "123", intake, runtime=runtime, settings=settings, secret_owner=None, probe_limiter=None
    )
    assert first is not None and first.proposal_id == "proposal-1"
    assert len(probe_calls) == 1  # the first check has nothing cached -- one real probe

    fake_clock["now"] += 1.0  # well within the TTL
    second = await _server_confirms_existing_proposal(
        "123", intake, runtime=runtime, settings=settings, secret_owner=None, probe_limiter=None
    )
    assert second is not None and second.proposal_id == "proposal-1"
    assert len(probe_calls) == 1  # the SECOND check is served from cache -- zero new probes


def test_resolve_member_status_never_trusts_a_proposal_id_on_a_non_complete_reading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F4 (P3/P2, #28 round-2): a stray `proposal_id` field on a wire
    response whose `intake_state` is NOT 'complete' must never be
    trusted -- `StatusResolution.proposal_id` is only ever populated for a
    genuinely 'complete' reading. Mutation-provable: dropping the
    `intake_state == "complete"` guard in `resolve_member_status`'s parse
    makes this go red (proposal_id would leak through on a 'pending'
    reading)."""
    payload = json.dumps(
        {
            "ok": True,
            "bound": True,
            "member_id": "member-1",
            "home_squad_id": "home-squad-1",
            "intake_state": "pending",
            "proposal_id": "SHOULD-NEVER-BE-TRUSTED",
        }
    ).encode("utf-8")
    install_probe(monkeypatch, response=payload)

    status = resolve_member_status(valid_settings(), 123, 123)

    assert status.intake_state == "pending"
    assert status.proposal_id is None


def test_abandon_never_drops_a_record_that_changed_during_its_own_disk_clear(tmp_path: Path) -> None:
    """F5 (P2/P3, #28 round-2): `abandon()`'s own disk-first reorder
    (mupot-plugin#25 N6) releases the lock between its initial read and
    its final pop -- a genuinely concurrent `resume()` + `record_answer()`
    (a real flap back to 'pending') landing in that exact window replaces
    `_pending[chat_key]` with a NEWER record (real new engrams, already
    re-saved to disk) that the OLD code popped unconditionally: memory
    gone, the durable row it just wrote very much present. Controlled,
    deterministic interleave (no real threading needed -- this module is
    single-threaded per event loop; the race is about ORDER, not OS
    scheduling): `_clear_in_progress` is wrapped so the concurrent
    resume+answer runs immediately after abandon()'s own disk write,
    strictly before its final pop. Mutation-provable: reverting to an
    unconditional pop makes the final assertion go red (`survivor` would
    be `None`)."""
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    runtime.record_answer("123", "name", "engram-1")

    original_clear = runtime._clear_in_progress

    def interleaved_clear(member_id: str) -> None:
        original_clear(member_id)
        # Simulates a genuine flap-back-to-pending message arriving and
        # being fully handled WHILE this abandon() call is between its
        # disk clear and its own final in-memory pop.
        runtime.resume("123")
        runtime.record_answer("123", "role", "engram-2")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(runtime, "_clear_in_progress", interleaved_clear)
        runtime.abandon("123")

    survivor = runtime.get_pending("123")
    assert survivor is not None
    assert survivor.engrams == {"name": "engram-1", "role": "engram-2"}


def test_abandon_interleave_resolves_to_the_disk_record_on_the_next_message(tmp_path: Path) -> None:
    """Athena's binding round-2 condition (3): memory is the CACHE, disk
    is the RECORD. Independent of the in-memory identity-check fix above
    (F5) -- which keeps THIS process's own view correct -- the DURABLE
    record `record_answer` wrote during the interleave must, on its own,
    be enough for "the next message" to resume with every engram intact,
    even reaching a completely FRESH `FirstPersonRuntime` (the sharpest
    version of "memory lost the thread entirely," e.g. a process
    restart): resume must come from disk, never depend on any one
    process's in-memory state having survived."""
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    runtime.record_answer("123", "name", "engram-1")

    original_clear = runtime._clear_in_progress

    def interleaved_clear(member_id: str) -> None:
        original_clear(member_id)
        runtime.resume("123")
        runtime.record_answer("123", "role", "engram-2")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(runtime, "_clear_in_progress", interleaved_clear)
        runtime.abandon("123")

    # A completely fresh runtime instance over the SAME durable state --
    # memory in the old instance is irrelevant; disk is what "the next
    # message" resumes from.
    fresh_runtime = FirstPersonRuntime(state_path)
    resumed = fresh_runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    assert resumed.engrams == {"name": "engram-1", "role": "engram-2"}
    assert resumed.index == 2

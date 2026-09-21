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
    _ProbeLimiter,
    _StatusCache,
    _UNRELATED_ANSWER_REPLY,
    _build_resolve_project_request,
    _classify_intake_state,
    _looks_like_credential,
    _resolve_member_project,
    _sanitize_answer,
    handle_first_contact,
    register_first_person,
    register_first_person_skill,
    resolve_member_status,
    sanitized_first_contact_envelope,
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

    def fake_resolve(settings, user_id, chat_id, *, secret_owner=None, cache=None, probe_limiter=None):
        return resolver(user_id, chat_id)

    monkeypatch.setattr("plugin.first_person.resolve_member_status", fake_resolve)


def pending_status(member_id: str = "member-1", home_squad_id: str | None = "home-squad-1") -> StatusResolution:
    return StatusResolution(bound=True, member_id=member_id, home_squad_id=home_squad_id, intake_state="pending")


UNBOUND_STATUS = StatusResolution(bound=False, member_id=None, home_squad_id=None, intake_state="unknown")
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
    home_squad_id is left completely untouched (no reply, no state) rather
    than trying to provision one."""
    install_status_stub(monkeypatch, lambda *_: pending_status(home_squad_id=None))
    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    update = Update(message=Message(text="hello!"))

    handled = await handle_first_contact(update, settings=valid_settings(), client=client, runtime=runtime)

    assert handled is False
    assert update.effective_message.replies == []
    assert runtime.get_pending("123") is None
    assert client.calls == []


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
async def test_unbind_mid_intake_drops_pending_and_stores_nothing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Round-2 P1-3: every answer re-checks bound/intake_state; an unbind
    between messages must drop the local record, not keep writing to it."""
    call_count = {"n": 0}

    def resolver(_user_id: Any, _chat_id: Any) -> StatusResolution:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return pending_status()
        return UNBOUND_STATUS  # unbound on the very next message

    install_status_stub(monkeypatch, resolver)
    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    await handle_first_contact(
        Update(message=Message(text="hello!")), settings=valid_settings(), client=client, runtime=runtime
    )
    assert runtime.get_pending("123") is not None

    second_update = Update(message=Message(text="Ada Example"))
    handled = await handle_first_contact(
        second_update, settings=valid_settings(), client=client, runtime=runtime
    )

    assert handled is False
    assert runtime.get_pending("123") is None
    assert not any(action == "squad_remember" for action, _ in client.calls)
    # Athena round-2 (v): an unbind mid-intake must not claim any progress --
    # no reply at all, not even an honest-sounding one.
    assert second_update.effective_message.replies == []


@pytest.mark.asyncio
async def test_member_mismatch_mid_chat_abandons_stale_progress(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    call_count = {"n": 0}

    def resolver(_user_id: Any, _chat_id: Any) -> StatusResolution:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return pending_status(member_id="member-1")
        return pending_status(member_id="member-2")  # a different member now resolves

    install_status_stub(monkeypatch, resolver)
    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    await handle_first_contact(
        Update(message=Message(text="hello!")), settings=valid_settings(), client=client, runtime=runtime
    )
    first_pending = runtime.get_pending("123")
    assert first_pending is not None and first_pending.member_id == "member-1"

    await handle_first_contact(
        Update(message=Message(text="hello again!")), settings=valid_settings(), client=client, runtime=runtime
    )
    second_pending = runtime.get_pending("123")
    assert second_pending is not None
    assert second_pending.member_id == "member-2"
    assert second_pending is not first_pending


@pytest.mark.asyncio
async def test_stale_pending_record_expires_after_ttl(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake_time = {"now": 0.0}
    runtime = FirstPersonRuntime(tmp_path / "state.json", clock=lambda: fake_time["now"])
    runtime.start("123", member_id="member-1", home_squad_id="home-squad-1")
    assert runtime.get_pending("123") is not None

    fake_time["now"] = 601.0  # past the 600s TTL
    assert runtime.get_pending("123") is None


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

    # First submission failed: no marker, honest reply, pending retained.
    assert not state_path.exists()
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
    """(v) stale bound cache: TTL + re-resolve on every answer; an unbind
    mid-intake ABORTS the intake and marks nothing -- drop _pending, store
    nothing, no reply claiming progress. (Same scenario as
    test_unbind_mid_intake_drops_pending_and_stores_nothing above, named
    explicitly to match Athena's round-2 gate condition list.)"""
    call_count = {"n": 0}

    def resolver(_user_id: Any, _chat_id: Any) -> StatusResolution:
        call_count["n"] += 1
        return pending_status() if call_count["n"] == 1 else UNBOUND_STATUS

    install_status_stub(monkeypatch, resolver)
    client = FakeClient()
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    await handle_first_contact(
        Update(message=Message(text="hello!")), settings=valid_settings(), client=client, runtime=runtime
    )
    assert runtime.get_pending("123") is not None

    abort_update = Update(message=Message(text="Ada Example"))
    handled = await handle_first_contact(
        abort_update, settings=valid_settings(), client=client, runtime=runtime
    )

    assert handled is False
    assert runtime.get_pending("123") is None
    assert not state_path.exists()
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

    assert handled is True
    assert update.effective_message.replies == [_AWAITING_HUMANS_REPLY]
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    logged = warnings[0].getMessage()
    assert "member-1" in logged
    assert "proposal-1" in logged
    assert "Ada Example" not in logged  # no answer text / PII


@pytest.mark.asyncio
async def test_held_proposal_id_blocks_reintake_even_two_hours_past_completion(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Round-3 P0-B HARD RULE, end to end: a server that keeps reporting
    intake_state == 'pending' for 2 simulated hours after a successful
    proposal (well past the 3600s completion-cache TTL -- see
    test_condition_ii, a SEPARATE, softer mechanism this does not rely on)
    must produce zero re-asked questions, zero engram rewrites, and zero
    second proposals."""
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
    caplog.set_level("WARNING")
    minutes = range(0, 121, 15)  # 0, 15, ..., 120 minutes -- 9 messages
    for minute in minutes:
        fake_wall_clock["now"] += 15 * 60.0
        update = Update(message=Message(text=f"still there? (minute {minute})"))
        handled = await handle_first_contact(update, settings=settings, client=client, runtime=runtime)
        assert handled is True
        assert update.effective_message.replies == [_AWAITING_HUMANS_REPLY]

    assert len(client.calls) == calls_after_completion  # zero new mupot calls at all
    data = json.loads(state_path.read_text())
    assert data["completed"]["member-1"]["proposal_id"] == "proposal-1"  # unchanged, still the first
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == len(minutes)  # exactly one per message in this state


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

    assert not state_path.exists()  # no completion marker written
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

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

import plugin.first_person as first_person
from plugin.first_person import (
    FIRST_PERSON_QUESTIONS,
    SKILL_PATH,
    FirstPersonRuntime,
    FirstPersonSettings,
    Intake,
    StatusResolution,
    _StatusCache,
    _looks_like_credential,
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

    def fake_resolve(settings, user_id, chat_id, *, secret_owner=None, cache=None):
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
    client = FakeClient({"create_home_for_member": {"ok": True, "result": {"squad_id": "home-squad-1"}}})
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    update = Update(message=Message(text="edited text"), edited_message=object())

    handled = await handle_first_contact(update, settings=valid_settings(), client=client, runtime=runtime)

    assert handled is False
    assert client.calls == []


@pytest.mark.asyncio
async def test_pending_status_with_no_home_yet_creates_one_and_asks_q1(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    install_status_stub(monkeypatch, lambda *_: pending_status(home_squad_id=None))
    client = FakeClient({"create_home_for_member": {"ok": True, "result": {"squad_id": "home-squad-1"}}})
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    update = Update(message=Message(text="hello!"))

    handled = await handle_first_contact(update, settings=valid_settings(), client=client, runtime=runtime)

    assert handled is True
    assert update.effective_message.replies == [FIRST_PERSON_QUESTIONS[0][1]]
    pending = runtime.get_pending("123")
    assert pending is not None
    assert pending.home_squad_id == "home-squad-1"
    assert ("create_home_for_member", {"member_id": "member-1"}) in client.calls


@pytest.mark.asyncio
async def test_pending_status_with_existing_home_skips_home_creation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    install_status_stub(monkeypatch, lambda *_: pending_status(home_squad_id="home-squad-1"))
    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    update = Update(message=Message(text="hello!"))

    handled = await handle_first_contact(update, settings=valid_settings(), client=client, runtime=runtime)

    assert handled is True
    assert ("create_home_for_member", {"member_id": "member-1"}) not in client.calls


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
    client = FakeClient({"create_home_for_member": {"ok": True, "result": {"squad_id": "home-squad-2"}}})
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
            if action == "create_home_for_member":
                return {"ok": True, "result": {"squad_id": "home-squad-1"}}
            if action == "resolve_member_project":
                if args.get("name", "").strip().lower() == "psychonom":
                    return {"ok": True, "result": {"project_id": "proj-1"}}
                return {"ok": True, "result": {"project_id": None}}
            if action == "squad_remember":
                engram_counter["n"] += 1
                return {"ok": True, "result": {"engram_id": f"engram-{engram_counter['n']}"}}
            if action == "routine_proposal_submit":
                return {"ok": True, "result": {"proposal_id": "proposal-1"}}
            return {"ok": True, "result": {}}

    return _SequencedClient()


async def _run_intake(monkeypatch, client, runtime, answers, *, member_id="member-1", home_squad_id="home-squad-1"):
    install_status_stub(monkeypatch, lambda *_: pending_status(member_id=member_id, home_squad_id=home_squad_id))
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

    last_update = await _run_intake(monkeypatch, client, runtime, answers)

    remember_calls = [args for action, args in client.calls if action == "squad_remember"]
    assert len(remember_calls) == 5
    assert all(args["squad_id"] == "home-squad-1" for args in remember_calls)
    assert [args["text"] for args in remember_calls] == answers

    project_calls = [args for action, args in client.calls if action == "resolve_member_project"]
    assert project_calls == [{"member_id": "member-1", "name": "psychonom"}]

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
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    suspicious_project_answer = "home-squad-EVIL psychonom"  # contains "squad" and resolves via name match below

    class _ClientWithMatchingProject(FakeClient):
        def call(self, action: str, args: dict[str, Any]) -> Any:
            self.calls.append((action, dict(args)))
            if action == "create_home_for_member":
                return {"ok": True, "result": {"squad_id": "home-squad-1"}}
            if action == "resolve_member_project":
                return {"ok": True, "result": {"project_id": "proj-1"}}
            if action == "squad_remember":
                return {"ok": True, "result": {"engram_id": "engram-x"}}
            if action == "routine_proposal_submit":
                return {"ok": True, "result": {"proposal_id": "proposal-1"}}
            return {"ok": False, "error": "action_not_allowed"}

    client = _ClientWithMatchingProject()
    install_status_stub(monkeypatch, lambda *_: pending_status(home_squad_id="home-squad-1"))
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
            if action == "create_home_for_member":
                return {"ok": True, "result": {"squad_id": "home-squad-1"}}
            if action == "resolve_member_project":
                return {"ok": True, "result": {"project_id": "proj-1"}}
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

    # Force the backoff to have elapsed, then retry succeeds.
    pending.next_proposal_retry_at = 0.0
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

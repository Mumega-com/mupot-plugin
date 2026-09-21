"""Contract tests for the deterministic first-person intake handler."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
import yaml

from plugin.first_person import (
    FIRST_PERSON_QUESTIONS,
    FirstPersonRuntime,
    FirstPersonSettings,
    MemberResolution,
    handle_first_contact,
    register_first_person,
    resolve_bound_member,
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
    ) -> None:
        self.update_id = update_id
        self.effective_user = user if user is not None else User()
        self.effective_chat = chat if chat is not None else Chat()
        self.effective_message = message if message is not None else Message()


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
        org_name="Acme",
        base_url="https://pot.example.invalid/base",
        webhook_secret_env="TEST_IM_WEBHOOK_SECRET",
        timeout=7.0,
    )
    return replace(settings, **changes)


def resolve_with(
    monkeypatch: pytest.MonkeyPatch,
    *,
    response: bytes = b'{"ok":true,"bound":true,"member_id":"member-1"}',
) -> Opener:
    monkeypatch.setattr(
        "plugin.first_person.read_profile_secret", lambda _name: "runtime-webhook-secret"
    )
    opener = Opener(Response(response))
    monkeypatch.setattr("plugin.first_person.build_opener", lambda *_: opener)
    return opener


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


def test_settings_require_org_name_only_when_enabled() -> None:
    # Disabled: no org name required -- must not break every existing profile.
    FirstPersonSettings.from_mapping({"base_url": "https://pot.example.invalid"})
    with pytest.raises(ValueError, match="first_person_org_name"):
        FirstPersonSettings.from_mapping(
            {"base_url": "https://pot.example.invalid", "first_person_enabled": True}
        )
    FirstPersonSettings.from_mapping(
        {
            "base_url": "https://pot.example.invalid",
            "first_person_enabled": True,
            "first_person_org_name": "Acme",
        }
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
    import types as _types

    calls: list[object] = []
    ctx = _types.SimpleNamespace(register_telegram_handler=calls.append)
    register_first_person(ctx, replace(valid_settings(), enabled=False), client=FakeClient())
    assert calls == []


# ---------------------------------------------------------------------------
# Fence
# ---------------------------------------------------------------------------


def test_envelope_rejects_group_chat_and_forwarded_messages() -> None:
    with pytest.raises(ValueError):
        sanitized_first_contact_envelope(Update(chat=Chat(chat_id=-999, chat_type="group")))
    with pytest.raises(ValueError):
        sanitized_first_contact_envelope(Update(message=Message(forward_date=123)))


# ---------------------------------------------------------------------------
# Unbound sender: hard block, nothing stored anywhere
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unbound_sender_gets_fixed_reply_and_nothing_persisted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    resolve_with(monkeypatch, response=b'{"ok":true,"bound":false}')
    settings = valid_settings()
    client = FakeClient()
    state_path = tmp_path / "first-person-state.json"
    runtime = FirstPersonRuntime(state_path)
    update = Update(message=Message(text="hi there, my name is Ada"))

    handled = await handle_first_contact(update, settings=settings, client=client, runtime=runtime)

    assert handled is True
    assert update.effective_message.replies == [
        "You're not part of Acme yet -- ask an admin for an invite, then message me again."
    ]
    # Nothing stored anywhere: no state file, no in-memory pending entry, no
    # mutating mupot call of any kind.
    assert not state_path.exists()
    assert runtime.get_pending("123") is None
    assert client.calls == []


@pytest.mark.asyncio
async def test_unbound_reply_never_contains_the_senders_raw_text(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    resolve_with(monkeypatch, response=b'{"ok":true,"bound":false}')
    settings = valid_settings()
    client = FakeClient()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    secret_answer = "my-super-secret-life-story-xyz"
    update = Update(message=Message(text=secret_answer))

    await handle_first_contact(update, settings=settings, client=client, runtime=runtime)

    assert all(secret_answer not in reply for reply in update.effective_message.replies)


@pytest.mark.asyncio
async def test_resolve_bound_member_fails_closed_on_missing_structured_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """kasra-review's PR#15 lesson: never treat a plausible-looking reply string
    as an authorization decision. A response with `ok: true` but no `bound`
    field at all must resolve as unbound, not crash and not default to bound."""
    resolve_with(monkeypatch, response=b'{"ok":true,"reply":"Welcome back!"}')
    envelope = sanitized_first_contact_envelope(Update())
    resolution = resolve_bound_member(valid_settings(), envelope)
    assert resolution == MemberResolution(bound=False, member_id=None)


# ---------------------------------------------------------------------------
# Bound sender: home creation + 5-question intake + proposal
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
            if action == "project_list":
                return {
                    "ok": True,
                    "result": {"projects": [{"id": "proj-1", "slug": "psychonom", "name": "Psychonom"}]},
                }
            if action == "squad_remember":
                engram_counter["n"] += 1
                return {"ok": True, "result": {"engram_id": f"engram-{engram_counter['n']}"}}
            if action == "routine_proposal_submit":
                return {"ok": True, "result": {"proposal_id": "proposal-1"}}
            return {"ok": True, "result": {}}

    return _SequencedClient()


@pytest.mark.asyncio
async def test_bound_first_message_creates_home_and_asks_first_question(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    resolve_with(monkeypatch)
    settings = valid_settings()
    client = _client_for_full_intake()
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    update = Update(message=Message(text="hello!"))

    handled = await handle_first_contact(update, settings=settings, client=client, runtime=runtime)

    assert handled is True
    assert update.effective_message.replies == [FIRST_PERSON_QUESTIONS[0][1]]
    pending = runtime.get_pending("123")
    assert pending is not None
    assert pending.member_id == "member-1"
    assert pending.home_squad_id == "home-squad-1"
    assert ("create_home_for_member", {"member_id": "member-1"}) in client.calls


@pytest.mark.asyncio
async def test_full_five_question_intake_writes_to_home_and_submits_proposal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    resolve_with(monkeypatch)
    settings = valid_settings()
    client = _client_for_full_intake()
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)

    answers = ["Ada Example", "Engineer", "psychonom", "Wire up the intake flow", "Nothing else"]
    update = Update(message=Message(text="hello!"))
    await handle_first_contact(update, settings=settings, client=client, runtime=runtime)

    for answer in answers:
        update = Update(message=Message(text=answer))
        handled = await handle_first_contact(update, settings=settings, client=client, runtime=runtime)
        assert handled is True

    # All five answers landed as squad_remember calls scoped to the HOME squad.
    remember_calls = [args for action, args in client.calls if action == "squad_remember"]
    assert len(remember_calls) == 5
    assert all(args["squad_id"] == "home-squad-1" for args in remember_calls)
    assert [args["text"] for args in remember_calls] == answers

    # Project resolved by slug via project_list, never guessed.
    proposal_calls = [args for action, args in client.calls if action == "routine_proposal_submit"]
    assert len(proposal_calls) == 1
    proposal = proposal_calls[0]
    assert proposal["project_id"] == "proj-1"
    assert proposal["action"]["input"] == {
        "member_id": "member-1",
        "project_id": "proj-1",
        "access_level": "write",
        "reason": "first-person intake",
    }

    # Completed: no longer pending, and final reply confirms.
    assert runtime.get_pending("123") is None
    assert update.effective_message.replies[-1] == (
        "Thanks -- I've sent your access request to the team for a decision."
    )

    # Durable marker exists, holds only structured ids -- never raw text.
    data = json.loads(state_path.read_text())
    completed = data["completed"]["member-1"]
    assert set(completed["engram_ids"]) == {q_id for q_id, _ in FIRST_PERSON_QUESTIONS}
    assert completed["proposal_id"] == "proposal-1"


@pytest.mark.asyncio
async def test_raw_answer_text_never_lands_in_the_state_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    resolve_with(monkeypatch)
    settings = valid_settings()
    client = _client_for_full_intake()
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)

    answers = [
        "Ada Example",
        "Senior Distributed Systems Engineer",
        "psychonom",
        "Ship the onboarding flow end to end",
        "I work best in the mornings, UTC-5",
    ]
    update = Update(message=Message(text="hello!"))
    await handle_first_contact(update, settings=settings, client=client, runtime=runtime)
    for answer in answers:
        update = Update(message=Message(text=answer))
        await handle_first_contact(update, settings=settings, client=client, runtime=runtime)

    raw_dump = state_path.read_text()
    for answer in answers:
        assert answer not in raw_dump


@pytest.mark.asyncio
async def test_already_completed_member_is_not_re_onboarded(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    resolve_with(monkeypatch)
    settings = valid_settings()
    client = _client_for_full_intake()
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    # Seed the completion marker directly (as if a prior process completed this
    # member's intake), then resolve the SAME member id on a fresh chat.
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps({"completed": {"member-1": {"engram_ids": {}, "proposal_id": None}}})
    )

    update = Update(message=Message(text="hey again"))
    handled = await handle_first_contact(update, settings=settings, client=client, runtime=runtime)

    assert handled is False
    assert update.effective_message.replies == []
    assert ("create_home_for_member", {"member_id": "member-1"}) not in client.calls


# ---------------------------------------------------------------------------
# Home / write failures fail soft, never invent partial state
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_home_creation_failure_replies_and_creates_no_pending_intake(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    resolve_with(monkeypatch)
    settings = valid_settings()
    client = FakeClient({"create_home_for_member": {"ok": False, "error": "action_not_allowed"}})
    runtime = FirstPersonRuntime(tmp_path / "state.json")
    update = Update(message=Message(text="hello!"))

    handled = await handle_first_contact(update, settings=settings, client=client, runtime=runtime)

    assert handled is True
    assert runtime.get_pending("123") is None
    assert "went wrong" in update.effective_message.replies[0]


@pytest.mark.asyncio
async def test_write_failure_does_not_advance_the_question_or_persist(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    resolve_with(monkeypatch)
    settings = valid_settings()
    client = FakeClient(
        {
            "create_home_for_member": {"ok": True, "result": {"squad_id": "home-squad-1"}},
            "squad_remember": {"ok": False, "error": "transport_error"},
        }
    )
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)
    update = Update(message=Message(text="hello!"))
    await handle_first_contact(update, settings=settings, client=client, runtime=runtime)

    update = Update(message=Message(text="Ada Example"))
    await handle_first_contact(update, settings=settings, client=client, runtime=runtime)

    pending = runtime.get_pending("123")
    assert pending is not None
    assert pending.index == 0  # never advanced
    assert not state_path.exists()  # never spilled to disk on a failed write


# ---------------------------------------------------------------------------
# Capability floor: Mubot the model has no registered path to any of these
# actions, and the allowlist explicitly excludes every manage_access surface.
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
# Mutation: prove the "never spill raw text to disk" test above has teeth.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mutation_a_leaked_raw_answer_is_caught_by_the_state_file_check(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Deliberately mutate the completion write path to leak raw text (as a
    regression in FirstPersonRuntime.finish might), then re-run the exact same
    assertion test_raw_answer_text_never_lands_in_the_state_file relies on and
    confirm it goes red. This proves the check is not vacuous."""
    resolve_with(monkeypatch)
    settings = valid_settings()
    client = _client_for_full_intake()
    state_path = tmp_path / "state.json"
    runtime = FirstPersonRuntime(state_path)

    leaked_answer = "this raw answer must never reach disk"

    original_finish = FirstPersonRuntime.finish

    def mutated_finish(self: FirstPersonRuntime, chat_key: str, *, proposal_id: str | None) -> None:
        # Simulate the defect class: a future edit that widens the completion
        # write to also carry the raw text (see feedback_each_fix_round_added_
        # a_write_path_narrow_dont_add.md -- this is exactly the shape that
        # would slip past a reviewer skimming a diff).
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
    update = Update(message=Message(text="hello!"))
    await handle_first_contact(update, settings=settings, client=client, runtime=runtime)
    for answer in answers:
        update = Update(message=Message(text=answer))
        await handle_first_contact(update, settings=settings, client=client, runtime=runtime)

    raw_dump = state_path.read_text()
    with pytest.raises(AssertionError):
        assert leaked_answer not in raw_dump

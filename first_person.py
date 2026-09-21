"""Deterministic first-contact intake for a person meeting Mubot for the first time.

FP-01 Slice 2 (mupot#1443, brief §2 Slice 2 + §2f). The whole flow is plugin code,
never model discretion: an LLM turn never runs during this conversation, so it
cannot leak a raw answer, cannot decide to skip the bound-member check, and never
holds a registered tool for any capability-granting surface. Every mutating call
here goes straight through :class:`~plugin.mupot_operator.MupotOperatorClient`
against ``FIRST_PERSON_ACTIONS`` -- a frozenset that does not and must not contain
``project_squad_set``, ``grant_agent_capability``, ``grant_gate_capability``, or any
other ``manage_access`` surface (see mupot_operator.py; this module proposes access,
it never grants it).

Architecture note: the actual conversation logic (:func:`handle_first_contact` and
its helpers) is a set of plain, PTB-independent module-level functions taking
explicit parameters -- mirroring ``telegram_inline_approval.py``'s
``_handle_callback`` shape, not a closure buried inside ``register_first_person``.
This is what makes the security-critical branches (unbound -> nothing stored,
raw-text write-through-then-drop) directly unit-testable without booting PTB.
``register_first_person`` itself is a thin adapter that wires this into a native
Telegram ``MessageHandler``.

Design ruling pinned into this build (Athena G-FP2-S2, seq 5064, 2026-09-21):
  (a) HOME-CREATION AUTHORITY: bind first. The member row exists only via the
      invite door or admin create; the Telegram bind itself is out of scope here
      (mupot#1411, migration 0154, already live) -- this module only ever asks
      "is this Telegram sender ALREADY bound", via the SAME authenticated
      ``/im/webhook`` envelope ``telegram_control.py``'s relay uses, never a local
      guess and never a string-match against server prose (see kasra-review's
      PR#15 pattern: "fails closed by accident" from comparing a rendered reply
      against local English literals -- this module reads only the structured
      ``bound``/``member_id`` fields documented as this PR's mupot-side contract).
      ``create_home_for_member`` runs only once that member's own first message
      resolves bound=True -- never speculatively, never for an unknown sender.
  (b) FIRST-DM RAW TEXT: write-through, no durable local copy. An answer lives in
      a local variable only until ``squad_remember``'s response itself carries an
      ``engram_id`` (never a read-back -- recall is eventually consistent in
      production; the engram_id in the write response IS the confirmation). Once
      confirmed, only ``{question_id: engram_id}`` is kept, and that only in the
      completion marker written once, at the end of the whole intake -- never a
      transcript file, never a state.json entry per answer, never a log line
      carrying the text.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urljoin
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .mupot_operator import MupotOperatorClient
from .profile_scope import (
    ProfileSecretOwner,
    read_profile_secret,
    require_supported_profile_runtime,
)
from .telegram_control import _required_telegram_id
from .telegram_fence import is_forwarded_telegram_message

logger = logging.getLogger(__name__)

# Single source of truth for the fixed 5-question script (brief deliverable 1).
# Mirrored verbatim in skills/first-person/SKILL.md's `questions` frontmatter --
# tests/test_first_person.py asserts the two never drift apart (see
# telegram_fence.py's docstring for why one predicate beats two copies).
FIRST_PERSON_QUESTIONS: tuple[tuple[str, str], ...] = (
    ("name", "What's your name?"),
    ("role", "What do you do?"),
    ("project", "Which project are you here for?"),
    ("first_ask", "What's the first thing you want done?"),
    (
        "notes",
        "Anything the team should know about you? (nothing about passwords, keys, "
        "or account details, please)",
    ),
)

_MAX_ANSWER_CHARS = 2000
_MAX_REQUEST_BYTES = 32 * 1024
_MAX_RESPONSE_BYTES = 64 * 1024
_FIRST_PERSON_HANDLER_GROUP = -5  # must run before Hermes's own core text handler (group 0)


def _unbound_reply(org_name: str) -> str:
    return f"You're not part of {org_name} yet -- ask an admin for an invite, then message me again."


_HOME_FAILURE_REPLY = "Something went wrong opening your space -- please try sending that again in a moment."
_WRITE_FAILURE_REPLY = "I couldn't save that -- please send it again."
_COMPLETE_REPLY = "Thanks -- I've sent your access request to the team for a decision."


def _project_not_found_reply(name: str) -> str:
    safe = name.strip()[:120] or "that"
    return f'I couldn\'t find a project called "{safe}" -- check the name with your captain and try again.'


@dataclass(frozen=True)
class FirstPersonSettings:
    """Non-secret configuration for the first-person intake handler.

    Deliberately self-contained (does not depend on ``telegram_control_enabled``):
    the resolution probe reuses the *shape* of telegram_control's authenticated
    envelope, not its enable flag or its running handler.
    """

    enabled: bool
    org_name: str
    base_url: str
    webhook_secret_env: str = "IM_WEBHOOK_SECRET"
    timeout: float = 20.0

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "FirstPersonSettings":
        enabled = value.get("first_person_enabled", False)
        if not isinstance(enabled, bool):
            raise ValueError("first_person_enabled must be a boolean")

        org_name = value.get("first_person_org_name", "")
        if not isinstance(org_name, str):
            raise ValueError("first_person_org_name must be a string")
        if enabled and not org_name.strip():
            raise ValueError("first_person_org_name is required when first_person_enabled is true")

        base_url = value.get("base_url")
        if not isinstance(base_url, str) or not base_url.strip():
            raise ValueError("first person base_url is required")

        env_name = value.get("first_person_webhook_secret_env", "IM_WEBHOOK_SECRET")
        if not isinstance(env_name, str):
            raise ValueError("first person webhook secret environment-variable name is invalid")

        timeout_value = value.get("first_person_timeout", value.get("timeout", 20.0))
        try:
            timeout = float(timeout_value)
        except (TypeError, ValueError) as exc:
            raise ValueError("first person timeout must be numeric") from exc

        settings = cls(
            enabled=enabled,
            org_name=org_name.strip(),
            base_url=base_url.strip(),
            webhook_secret_env=env_name.strip(),
            timeout=timeout,
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        from urllib.parse import urlparse

        if not isinstance(self.enabled, bool):
            raise ValueError("first_person_enabled must be a boolean")
        if len(self.org_name) > 200:
            raise ValueError("first_person_org_name must be a short string")
        if self.enabled and not self.org_name.strip():
            raise ValueError("first_person_org_name is required when first_person_enabled is true")

        if not isinstance(self.base_url, str) or self.base_url != self.base_url.strip():
            raise ValueError("first person base_url must be a valid HTTPS URL")
        parsed = urlparse(self.base_url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("first person base_url must be an absolute HTTPS URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("first person base_url must not contain credentials, query, or fragment")

        if not isinstance(self.webhook_secret_env, str) or not self.webhook_secret_env:
            raise ValueError("first person webhook secret environment-variable name is invalid")

        if (
            isinstance(self.timeout, bool)
            or not isinstance(self.timeout, (int, float))
            or not 1 <= float(self.timeout) <= 120
        ):
            raise ValueError("first person timeout must be between 1 and 120 seconds")


@dataclass(frozen=True)
class MemberResolution:
    bound: bool
    member_id: str | None


class _NoRedirect(HTTPRedirectHandler):
    """Never forward the webhook secret header to another URL.

    Deliberately its own copy rather than an import of telegram_control.py's
    identically-shaped guard -- same rationale telegram_inline_approval.py gives
    for re-implementing StateStore: this module must stay importable (and its
    fence testable) without pulling in a sibling module's own module-scope state.
    """

    def redirect_request(
        self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str
    ) -> None:
        return None


def sanitized_first_contact_envelope(update: Any) -> dict[str, Any]:
    """Validate + shape the SAME authenticated-DM fence telegram_control.py's
    ``_sanitized_envelope`` enforces (private chat, sender == chat, not forwarded)
    but for ANY first free-text message, not only a recognized ``/command``.

    Raises ``ValueError`` for anything outside first-person's authenticated scope
    -- the caller must treat that as "not my concern, let normal handling continue",
    never as "unbound" (a fence failure and an unbound member are different things;
    only a clean envelope that resolves ``bound=False`` gets the fixed refusal).
    """

    user = getattr(update, "effective_user", None)
    chat = getattr(update, "effective_chat", None)
    message = getattr(update, "effective_message", None)
    if user is None or chat is None or message is None:
        raise ValueError("telegram private message is required")

    if getattr(chat, "type", None) != "private":
        raise ValueError("first-person intake requires a private chat")
    user_id = _required_telegram_id(getattr(user, "id", None), "user id")
    chat_id = _required_telegram_id(getattr(chat, "id", None), "chat id")
    if str(user_id) != str(chat_id):
        raise ValueError("first-person intake requires a private user chat")

    if is_forwarded_telegram_message(message):
        raise ValueError("forwarded telegram messages are refused")

    update_id = getattr(update, "update_id", None)
    if isinstance(update_id, bool) or not isinstance(update_id, int) or update_id < 0:
        raise ValueError("telegram update id is invalid")

    text = getattr(message, "text", None)
    if not isinstance(text, str) or not text:
        raise ValueError("telegram message text is required")

    display_name = getattr(user, "full_name", "")
    if not isinstance(display_name, str):
        display_name = ""

    return {
        "update_id": update_id,
        "chat_id": chat_id,
        "user_id": user_id,
        "text": text,
        "display_name": display_name[:256],
    }


def _probe_body(envelope: Mapping[str, Any]) -> bytes:
    # Probe as /start -- the existing bind-existing-member entry point (mupot#1411).
    # Never the human's own raw text: this call exists only to ask "who is this",
    # never to relay anything the human typed.
    payload = {
        "update_id": envelope["update_id"],
        "message": {
            "from": {"id": envelope["user_id"], "first_name": envelope["display_name"]},
            "chat": {"id": envelope["chat_id"], "type": "private"},
            "text": "/start",
        },
    }
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def resolve_bound_member(
    settings: FirstPersonSettings,
    envelope: Mapping[str, Any],
    *,
    secret_owner: ProfileSecretOwner | None = None,
) -> MemberResolution:
    """Resolve the Telegram sender's bound member through the authenticated
    ``/im/webhook`` envelope -- the ONLY channel this module trusts for identity.

    Mupot-side contract this depends on (see PR body): the ``/start`` reply must
    additionally carry structured ``{"bound": bool, "member_id": str|null}``
    fields. This function never parses the human-readable ``reply`` string for an
    authorization decision (see kasra-review's PR#15 finding: comparing rendered
    prose against local literals fails open). Anything missing or malformed in
    those structured fields is treated as unbound -- fail closed on unknown.
    """

    require_supported_profile_runtime({})
    body = _probe_body(envelope)
    if len(body) > _MAX_REQUEST_BYTES:
        return MemberResolution(bound=False, member_id=None)

    def _do() -> MemberResolution:
        try:
            secret = read_profile_secret(settings.webhook_secret_env)
        except RuntimeError:
            return MemberResolution(bound=False, member_id=None)
        if len(secret) > 256:
            return MemberResolution(bound=False, member_id=None)

        request = Request(
            urljoin(settings.base_url.rstrip("/") + "/", "/im/webhook"),
            data=body,
            headers={
                "Content-Type": "application/json",
                "X-Telegram-Bot-Api-Secret-Token": secret,
            },
            method="POST",
        )
        try:
            with build_opener(_NoRedirect()).open(request, timeout=float(settings.timeout)) as response:
                raw = response.read(_MAX_RESPONSE_BYTES + 1)
        except Exception:
            return MemberResolution(bound=False, member_id=None)

        if len(raw) > _MAX_RESPONSE_BYTES:
            return MemberResolution(bound=False, member_id=None)
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return MemberResolution(bound=False, member_id=None)
        if not isinstance(parsed, dict) or parsed.get("ok") is not True:
            return MemberResolution(bound=False, member_id=None)

        bound = parsed.get("bound")
        member_id = parsed.get("member_id")
        if bound is True and isinstance(member_id, str) and member_id.strip():
            return MemberResolution(bound=True, member_id=member_id.strip())
        return MemberResolution(bound=False, member_id=None)

    if secret_owner is None:
        return _do()
    with secret_owner.activate():
        return _do()


# --------------------------------------------------------------------------
# Durable completion marker -- NEVER raw answer text, see module docstring (b).
# --------------------------------------------------------------------------


class FirstPersonStateStore:
    """Durable, atomically-written completion marker.

    Deliberately re-implements the temp-file + fsync + rename shape (matching
    ``mupot_gateway.adapter.StateStore`` and ``telegram_inline_approval``'s
    ``ApprovalReceiptStore``) in its OWN file, never the shared adapter
    ``state.json`` -- this module must work in restricted operator mode with no
    native gateway at all, and must never risk clobbering another consumer's keys
    in a shared dict.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path).expanduser()

    def load_checked(self) -> tuple[dict[str, Any], bool]:
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
            return (value, True) if isinstance(value, dict) else ({}, False)
        except FileNotFoundError:
            return {}, True
        except (OSError, ValueError):
            return {}, False

    def save(self, value: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(value, handle, ensure_ascii=False, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            os.chmod(self.path, 0o600)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


def default_state_path() -> Path:
    try:
        from hermes_constants import get_hermes_home

        return get_hermes_home() / "platforms" / "mupot" / "first-person-state.json"
    except Exception:
        return Path(os.path.expanduser("~/.hermes/platforms/mupot/first-person-state.json"))


@dataclass
class Intake:
    """In-memory-only intake progress. Never persisted as a whole -- only the
    final ``engrams``/``proposal_id`` are written, once, at completion."""

    member_id: str
    home_squad_id: str
    index: int = 0
    project_id: str | None = None
    engrams: dict[str, str] = field(default_factory=dict)


class FirstPersonRuntime:
    """Process-global, in-memory-first intake registry.

    An unbound Telegram sender gets NO entry here, ever -- ``handle_first_contact``
    returns before this is ever touched for that case.
    """

    def __init__(self, state_path: Path) -> None:
        self._lock = threading.Lock()
        self._pending: dict[str, Intake] = {}
        self.store = FirstPersonStateStore(state_path)

    def is_complete(self, member_id: str) -> bool:
        data, _valid = self.store.load_checked()
        completed = data.get("completed")
        return isinstance(completed, dict) and member_id in completed

    def get_pending(self, chat_key: str) -> Intake | None:
        with self._lock:
            return self._pending.get(chat_key)

    def start(self, chat_key: str, *, member_id: str, home_squad_id: str) -> Intake:
        with self._lock:
            intake = Intake(member_id=member_id, home_squad_id=home_squad_id)
            self._pending[chat_key] = intake
            return intake

    def record_answer(self, chat_key: str, question_id: str, engram_id: str) -> None:
        with self._lock:
            intake = self._pending.get(chat_key)
            if intake is None:
                return
            intake.engrams[question_id] = engram_id
            intake.index += 1

    def set_project_id(self, chat_key: str, project_id: str) -> None:
        with self._lock:
            intake = self._pending.get(chat_key)
            if intake is not None:
                intake.project_id = project_id

    def finish(self, chat_key: str, *, proposal_id: str | None) -> None:
        with self._lock:
            intake = self._pending.pop(chat_key, None)
        if intake is None:
            return
        data, _valid = self.store.load_checked()
        completed = data.get("completed")
        if not isinstance(completed, dict):
            completed = {}
        completed[intake.member_id] = {
            "engram_ids": dict(intake.engrams),
            "proposal_id": proposal_id,
        }
        data["completed"] = completed
        self.store.save(data)

    def abandon(self, chat_key: str) -> None:
        with self._lock:
            self._pending.pop(chat_key, None)


def _resolve_project_id(client: MupotOperatorClient, name: str) -> str | None:
    """Resolve a project reference typed by a human against ``project_list`` by
    slug or name, case-insensitively. Returns ``None`` on no match or on any
    transport/shape failure -- fail closed, never guess."""

    response = client.call("project_list", {})
    if not isinstance(response, dict) or response.get("ok") is not True:
        return None
    result = response.get("result")
    projects = result.get("projects") if isinstance(result, dict) else result
    if not isinstance(projects, list):
        return None
    needle = name.strip().lower()
    if not needle:
        return None
    for project in projects:
        if not isinstance(project, dict):
            continue
        slug = str(project.get("slug") or "").strip().lower()
        proj_name = str(project.get("name") or "").strip().lower()
        candidate_id = project.get("id") or project.get("project_id")
        if not isinstance(candidate_id, str) or not candidate_id.strip():
            continue
        if needle == slug or needle == proj_name:
            return candidate_id.strip()
    return None


# --------------------------------------------------------------------------
# Core conversation logic -- plain, PTB-independent, directly unit-testable.
# --------------------------------------------------------------------------


async def _handle_answer(
    message: Any,
    chat_key: str,
    intake: Intake,
    *,
    client: MupotOperatorClient,
    runtime: FirstPersonRuntime,
) -> None:
    raw_answer = getattr(message, "text", None)
    if not isinstance(raw_answer, str) or not raw_answer.strip():
        await message.reply_text(_WRITE_FAILURE_REPLY)
        return
    raw_answer = raw_answer.strip()[:_MAX_ANSWER_CHARS]
    question_id, _question_text = FIRST_PERSON_QUESTIONS[intake.index]

    if question_id == "project" and intake.project_id is None:
        def resolve_project() -> str | None:
            return _resolve_project_id(client, raw_answer)

        project_id = await asyncio.to_thread(resolve_project)
        # raw_answer used only to compose this reply; this call frame ends right after.
        if project_id is None:
            await message.reply_text(_project_not_found_reply(raw_answer))
            return
        runtime.set_project_id(chat_key, project_id)
        intake.project_id = project_id

    def remember() -> dict[str, Any]:
        return client.call(
            "squad_remember",
            {"squad_id": intake.home_squad_id, "text": raw_answer, "concepts": [question_id]},
        )

    response = await asyncio.to_thread(remember)
    # `raw_answer` is a local of this call frame; nothing else in the process
    # holds a reference to it, and it is never written anywhere below.
    engram_id = None
    result = response.get("result") if isinstance(response, dict) else None
    if response.get("ok") is True and isinstance(result, dict):
        candidate = result.get("engram_id")
        if isinstance(candidate, str) and candidate.strip():
            engram_id = candidate.strip()

    if engram_id is None:
        # No confirmed write: retry from memory next message, never spill to
        # disk. The pending intake is left exactly as it was.
        await message.reply_text(_WRITE_FAILURE_REPLY)
        return

    runtime.record_answer(chat_key, question_id, engram_id)

    if intake.index >= len(FIRST_PERSON_QUESTIONS):
        await _submit_proposal(message, chat_key, intake, client=client, runtime=runtime)
        return
    await message.reply_text(FIRST_PERSON_QUESTIONS[intake.index][1])


async def _submit_proposal(
    message: Any,
    chat_key: str,
    intake: Intake,
    *,
    client: MupotOperatorClient,
    runtime: FirstPersonRuntime,
) -> None:
    project_id = intake.project_id
    if not project_id:
        runtime.finish(chat_key, proposal_id=None)
        await message.reply_text(_COMPLETE_REPLY)
        return

    def submit() -> dict[str, Any]:
        digest = hashlib.sha256(
            f"first-person:{intake.member_id}:{project_id}".encode("utf-8")
        ).hexdigest()
        return client.call(
            "routine_proposal_submit",
            {
                "version": "routine.proposal/v1",
                "run_id": f"first-person-{intake.member_id}",
                "project_id": project_id,
                "situation_digest": digest,
                "summary": "First-person intake: propose write access for a new member.",
                "action": {
                    "key": "first-person-project-access",
                    # "project_access" is the mupot-side contract this build
                    # depends on -- see PR body. The live routine_proposal_submit
                    # schema checked during this build only accepts
                    # create_task/dispatch_flight/request_review/ask_human/
                    # no_action; it has no project-access kind yet.
                    "kind": "project_access",
                    "input": {
                        "member_id": intake.member_id,
                        "project_id": project_id,
                        "access_level": "write",
                        "reason": "first-person intake",
                    },
                },
            },
        )

    response = await asyncio.to_thread(submit)
    result = response.get("result") if isinstance(response, dict) else None
    proposal_id = None
    if response.get("ok") is True and isinstance(result, dict):
        candidate = result.get("proposal_id") or result.get("id")
        if isinstance(candidate, str) and candidate.strip():
            proposal_id = candidate.strip()
    runtime.finish(chat_key, proposal_id=proposal_id)
    await message.reply_text(_COMPLETE_REPLY)


async def handle_first_contact(
    update: Any,
    *,
    settings: FirstPersonSettings,
    client: MupotOperatorClient,
    runtime: FirstPersonRuntime,
    secret_owner: ProfileSecretOwner | None = None,
) -> bool:
    """Handle one inbound Telegram update for the first-person flow.

    Returns ``True`` iff this update was fully handled here (the caller must stop
    further propagation -- no LLM turn, no other handler should also see it) and
    ``False`` iff first-person has nothing to do with this update (not a fenced
    private DM, or the sender already completed intake) -- normal handling
    continues untouched.
    """

    message = getattr(update, "effective_message", None)
    if message is None:
        return False
    try:
        envelope = sanitized_first_contact_envelope(update)
    except ValueError:
        return False  # not a fenced private DM -- not first-person's concern

    chat_key = str(envelope["chat_id"])
    pending = runtime.get_pending(chat_key)
    if pending is not None:
        await _handle_answer(message, chat_key, pending, client=client, runtime=runtime)
        return True

    def resolve() -> MemberResolution:
        return resolve_bound_member(settings, envelope, secret_owner=secret_owner)

    resolution = await asyncio.to_thread(resolve)
    if not resolution.bound or not resolution.member_id:
        # Hard block (brief §2f, design ruling): store NOTHING anywhere for an
        # unbound sender -- no memory dict entry, no state.json, no log line
        # carrying anything from this message.
        await message.reply_text(_unbound_reply(settings.org_name))
        return True

    if runtime.is_complete(resolution.member_id):
        return False  # already onboarded -- let normal conversation handling continue

    def create_home() -> dict[str, Any]:
        return client.call("create_home_for_member", {"member_id": resolution.member_id})

    home_response = await asyncio.to_thread(create_home)
    home_result = home_response.get("result") if isinstance(home_response, dict) else None
    home_squad_id = home_result.get("squad_id") if isinstance(home_result, dict) else None
    if (
        not isinstance(home_response, dict)
        or home_response.get("ok") is not True
        or not isinstance(home_squad_id, str)
        or not home_squad_id.strip()
    ):
        await message.reply_text(_HOME_FAILURE_REPLY)
        return True

    runtime.start(chat_key, member_id=resolution.member_id, home_squad_id=home_squad_id.strip())
    await message.reply_text(FIRST_PERSON_QUESTIONS[0][1])
    return True


# --------------------------------------------------------------------------
# Registration -- mirrors telegram_control.register_telegram_control's
# factory/unload shape; the `handle` closure below is a thin PTB adapter over
# handle_first_contact, never the place where the logic itself lives.
# --------------------------------------------------------------------------


def register_first_person(
    ctx: Any,
    settings: FirstPersonSettings,
    *,
    client: MupotOperatorClient,
    secret_owner: ProfileSecretOwner | None = None,
    state_path: Path | None = None,
    runtime: FirstPersonRuntime | None = None,
) -> None:
    settings.validate()
    if not settings.enabled:
        return

    if runtime is None:
        runtime = FirstPersonRuntime(state_path or default_state_path())
    wired_applications: list[tuple[Any, Any]] = []

    def factory(application: Any, adapter: Any) -> None:
        # Hermes loads backend plugins without platform SDK dependencies. Keep the
        # optional PTB import inside the native factory invoked at connect (same
        # convention as telegram_control.py / telegram_inline_approval.py).
        from telegram.ext import ApplicationHandlerStop, MessageHandler, filters

        if any(existing is application for existing, _ in wired_applications):
            return

        async def handle(update: Any, context: Any) -> None:
            handled = await handle_first_contact(
                update, settings=settings, client=client, runtime=runtime, secret_owner=secret_owner
            )
            if handled:
                raise ApplicationHandlerStop

        handler = MessageHandler(filters.TEXT & ~filters.COMMAND, handle)
        application.add_handler(handler, group=_FIRST_PERSON_HANDLER_GROUP)
        wired_applications.append((application, handler))

    def unload() -> None:
        for application, handler in wired_applications:
            application.remove_handler(handler, group=_FIRST_PERSON_HANDLER_GROUP)
        wired_applications.clear()

        manager = getattr(ctx, "_manager", None)
        factories = getattr(manager, "_platform_handler_factories", None)
        if not isinstance(factories, dict):
            return
        plugin_name = getattr(getattr(ctx, "manifest", None), "name", None)
        telegram_factories = factories.get("telegram", [])
        telegram_factories[:] = [
            entry
            for entry in telegram_factories
            if not (entry[0] is factory and entry[1] == plugin_name)
        ]
        if not telegram_factories:
            factories.pop("telegram", None)

    ctx.register_telegram_handler(factory)
    on_unload = getattr(ctx, "on_unload", None)
    if callable(on_unload):
        on_unload(unload)

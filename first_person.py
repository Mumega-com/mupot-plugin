"""Deterministic first-contact intake for a person meeting Mubot for the first time.

FP-01 Slice 2 (mupot#1443, brief §2 Slice 2 + §2f). The whole flow is plugin code,
never model discretion: an LLM turn never runs during the in-flight capture of a raw
answer, so it cannot leak one by choice, cannot decide to skip the bound-member
check, and never holds a registered tool for any capability-granting surface. Every
mutating call here goes straight through
:class:`~plugin.mupot_operator.MupotOperatorClient` against ``FIRST_PERSON_ACTIONS``
-- a frozenset that does not and must not contain ``project_squad_set``,
``grant_agent_capability``, ``grant_gate_capability``, or any other ``manage_access``
surface (see mupot_operator.py; this module proposes access, it never grants it).

Architecture note: the actual conversation logic (:func:`handle_first_contact` and
its helpers) is a set of plain, PTB-independent module-level functions taking
explicit parameters -- mirroring ``telegram_inline_approval.py``'s
``_handle_callback`` shape, not a closure buried inside ``register_first_person``.
This is what makes the security-critical branches (unbound -> nothing stored,
raw-text write-through-then-drop, credential refusal) directly unit-testable
without booting PTB. ``register_first_person`` itself is a thin adapter that wires
this into a native Telegram ``MessageHandler``.

=== Round 2 reshape (adversarial gate, PR#17 @ 5990a2e4, RED: 3 P0 / 5 P1 / 3 P2) ===

The round-1 design decided "is this a first message" LOCALLY (an in-memory/disk
marker keyed by chat). That was the defect class: the handler preempted the
existing plain-text ``approve <id>`` path (#1425) and Hermes's own core LLM
handler for ANY message from ANY sender it hadn't locally seen complete --
including a bound, already-onboarded member approving a task, and including a
total stranger, neither of whom this module has any business intercepting.

Round 2 moves "is intake actually in progress for this member RIGHT NOW" onto
mupot itself. Every private DM re-checks a structured status
(:class:`StatusResolution`) the mupot side is adding to its authenticated
identity surface: ``{bound, member_id, home_squad_id, intake_state}``, where
``intake_state`` is ``"none" | "pending" | "complete"`` (server-decided; this
module never invents newness from its own state). Athena's round-1 ruling on
this reshape (2026-09-21) is binding: **code against those fields now, and
fail-safe for the intake (return ``False``, no reply, no
``ApplicationHandlerStop`` -- never consume the update; the host handler owns
the turn) whenever they are absent or malformed** -- exactly the same
fail-safe posture as "bound" resolving false. This PR must not merge before
the mupot-side contract PR lands
that field set (and the ``routine_proposal_submit`` ``project_access`` kind, and
``resolve_member_project``) -- see the PR body's mupot-side contract section.

Consuming an update (replying + raising ``ApplicationHandlerStop``) now happens
in exactly two cases: (1) ``intake_state == "pending"`` and no local progress yet
-- create the home if needed, ask question 1; (2) ``intake_state == "pending"``
and a local, not-yet-expired, member-matched intake is mid-conversation -- that
message IS the next answer (or a proposal-submission retry). Every other
combination (unbound, ``none``, ``complete``, unknown/malformed status, a stale
or member-mismatched local record) returns ``False`` -- untouched, unreplied,
unstored, and any stale local record is dropped.

Design ruling pinned into this build (Athena G-FP2-S2, seq 5064, 2026-09-21, plus
the round-2 correction above):
  (a) HOME-CREATION AUTHORITY: bind first. The member row exists only via the
      invite door or admin create; the Telegram bind itself is out of scope here
      (mupot#1411, migration 0154, already live). Home creation is mupot's job
      alone (brief 2f(a)) -- this module never invents a call for it (round-3:
      ``create_home_for_member`` was verified to have NO exposed MCP action or
      ``/im`` route on mupot's kasra/fp01-slice2-proposal-chain branch, only an
      internal TypeScript function called by that repo's own unit tests; see
      :func:`handle_first_contact`). A bound, pending member with a null
      ``home_squad_id`` is left completely untouched until a later status
      probe reports one -- never speculatively, never from a local guess,
      never from a synthesized ``/start`` per message (round-2 P1-1/P1-2:
      that was stranger-floodable and leaked the sender's display name to
      the bind surface on every message).
  (b) FIRST-DM RAW TEXT: write-through, no durable local copy. An answer lives in
      a local variable only until ``squad_remember``'s response itself carries an
      ``engram_id`` (never a read-back -- recall is eventually consistent in
      production; the engram_id in the write response IS the confirmation). Once
      confirmed, only ``{question_id: engram_id}`` is kept, and that only in the
      completion marker written once, at the very end of a SUCCESSFULLY submitted
      intake -- never a transcript file, never a state.json entry per answer,
      never a log line carrying the text (round-2 P0-3: a failed proposal
      submission must not write that marker either -- see :func:`_submit_proposal`).

=== Round 3 (successor to PR#17 @ ecea2501, RED: 2 P0 / 2 P1 / 2 P2 / 2 P3) ===

PR#17's round-2 gate cap was consumed at 2 P0 (adversarial round 2,
2026-09-21); this module continues from that exact head as a fresh PR
(kasra/first-person-skill-v2). Athena's rulings on the successor (binding,
gate-checked) are pinned inline at each fix site; summarized here:

  P0-A (credential-check pipeline order): the store must never receive an
  un-normalized byte sequence. The pipeline is NORMALIZE (NFKC + strip
  Cc/bidi/control chars, :func:`_sanitize_answer`) -> THEN credential-refuse
  on the NORMALIZED text (:func:`_looks_like_credential`) -> THEN memory
  (``squad_remember``). Checking credential-shape on the RAW text first (the
  round-2 bug) lets an embedded bidi/control char split a token across the
  regex's contiguous match, and the sanitizer that runs AFTER then deletes
  exactly those characters and reassembles a valid token. See
  :func:`_handle_answer`.

  P0-B (completion vs. server lag): mupot PR#1488 makes ``routine_proposal_
  submit`` itself the completion writer -- the server derives
  ``intake_state=='complete'`` from the EXISTENCE of the member's
  project_access proposal, so there is no separate completion write on the
  mupot side. This module holds the durable evidence of that existence
  locally (``proposal_id``, written ONLY once mupot accepts the submission --
  never speculatively). If the server still reports ``intake_state=='pending'``
  while this module already holds a ``proposal_id`` for that member (server
  lag, or a server-side bug), this is a HARD rule: never restart the intake,
  never ask a question, never submit a second proposal -- exactly one WARNING
  (member_id + the held proposal_id only, no PII) is logged, and the member is
  told the fixed line "Your request is awaiting the humans." See
  :class:`FirstPersonRuntime`'s ``held_proposal_id`` and
  :func:`handle_first_contact`.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import threading
import time
import unicodedata
from collections import OrderedDict
from dataclasses import dataclass, field, replace
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
_FIRST_PERSON_HANDLER_GROUP = -5

# How long a local, in-progress intake record survives with no forward progress
# before it is treated as abandoned (round-2 P1-3). A member who goes quiet
# mid-intake and comes back a week later starts clean, not mid-sentence.
_PENDING_TTL_SECONDS = 600.0

# How long the LOCAL completion marker is trusted as a dedupe cache before this
# module defers entirely back to the server's own intake_state (Athena's
# round-2 condition (ii): the marker is a cached view, never the record).
_COMPLETION_CACHE_TTL_SECONDS = 3600.0

# Status-lookup cache TTLs (round-2 P1-2: doubles as the per-user rate limit --
# a lookup served from cache never reaches the network). Positive results stay
# fresh enough to satisfy "every answer re-checks" without a network round trip
# per keystroke; negative (unbound / not-pending) results are cached far longer
# specifically to blunt a stranger hammering the bot. Round-3 ruling (4): a
# genuinely UNKNOWN result (transport failure, or the contract fields simply
# not live on this deployment) gets a HARDER, longer TTL still than an
# ordinary negative (bound-but-not-pending) result -- this is exactly the
# shape a brand-new stranger's very first probe takes, and it must not cost a
# fresh network round trip on every message from someone who will never
# resolve favorably soon.
_STATUS_POSITIVE_TTL_SECONDS = 15.0
_STATUS_NEGATIVE_TTL_SECONDS = 300.0
_STATUS_UNKNOWN_TTL_SECONDS = 900.0

# Round-3 P1-D / ruling (4): the status cache is now a bounded LRU -- a burst
# of distinct brand-new sender ids must never grow this dict without limit.
_STATUS_CACHE_MAX_ENTRIES = 2048
# How often (in `put()` calls) to sweep already-expired entries out of the
# cache, independent of the LRU eviction that only fires once the cache is at
# capacity -- keeps memory turnover honest well before the cache ever fills.
_STATUS_CACHE_SWEEP_EVERY = 256

# Round-3 P1-D / ruling (4): a status probe uses its OWN short timeout,
# distinct from (and never longer than) the operator-configured
# `FirstPersonSettings.timeout` (which may be configured up to 120s) -- a
# brand-new sender must never be able to park a thread on the shared
# `asyncio.to_thread` pool for anywhere near that long. Combined with the
# probe concurrency cap below, this bounds the WORST-CASE aggregate cost of a
# burst of first-time senders.
_STATUS_PROBE_TIMEOUT_SECONDS = 5.0

# Round-3 ruling (4): a global cap on how many status probes may be in flight
# on the shared thread pool at once, independent of the per-user_id TTL cache
# above (which only rate-limits repeats from the SAME sender) -- this bounds
# the aggregate cost across ALL senders at once. A probe that cannot acquire a
# slot within the wait window fails fast to "unknown" (fail-safe for the
# intake) rather than queuing indefinitely.
_STATUS_PROBE_MAX_CONCURRENCY = 32
_STATUS_PROBE_ACQUIRE_TIMEOUT_SECONDS = 1.0

# Proposal-submission retry backoff (round-2 P0-3): a failed submit must not be
# retried on literally the next keystroke.
_PROPOSAL_RETRY_BACKOFF_SECONDS: tuple[float, ...] = (5.0, 15.0, 60.0, 300.0)

# Discovery receipt (2026-09-21, read-only Hermes-source arm): a native
# (plugin.yaml, kind: backend) plugin like this one gets NO directory-scan
# auto-discovery for a skills/ folder -- only an explicit ctx.register_skill()
# call inside register(ctx) makes a bundled SKILL.md loadable at all
# (hermes_cli/plugins.py:994-1020 register_skill; hermes_cli/plugins_loader.py:393
# is the only in-tree caller pattern, for portable agent-plugin packages).
# `skills/mupot-operator/SKILL.md` ships in this repo but is NOT registered
# anywhere in __init__.py's register(ctx) today, so it is not actually a
# loadable skill on any live gateway (`hermes skills list` / `skill_view
# ('mupot:mupot-operator')` would not find it) -- fixing that is a separate,
# pre-existing issue this PR does not take on. This PR registers ONLY
# first-person, via register_first_person_skill below, called from
# __init__.py's register() gated on first_person_settings.enabled (mirrors
# every other conditional registration in that function).
SKILL_PATH = Path(__file__).resolve().parent / "skills" / "first-person" / "SKILL.md"
SKILL_DESCRIPTION = (
    "Mubot's first-contact intake: confirms a bound member via mupot's own "
    "status, opens their private home, asks five fixed questions, and "
    "proposes -- never grants -- project write access for a human to decide."
)


def register_first_person_skill(ctx: Any) -> None:
    """Register skills/first-person/SKILL.md so it resolves as
    'mupot:first-person' via skill_view()/skills_list(). Never raises: a
    skill-registration failure must not take down the rest of plugin
    registration."""
    register_skill = getattr(ctx, "register_skill", None)
    if not callable(register_skill):
        return
    try:
        register_skill("first-person", SKILL_PATH, SKILL_DESCRIPTION)
    except Exception:
        logger.warning("mupot plugin: could not register the first-person skill", exc_info=True)


_WRITE_FAILURE_REPLY = "I couldn't save that -- please send it again."
_COMPLETE_REPLY = "Thanks -- I've sent your access request to the team for a decision."
# Round-3 ruling (8): keep the refusal as-is (a false positive beats a leaked
# credential) -- the base64/hex heuristics in _CREDENTIAL_PATTERNS are
# deliberately left broad. What changes is the reply: it now documents the
# rephrase path so a genuine false positive (a long slug, a URL) has a way
# forward instead of a dead end.
_CREDENTIAL_REPLY = (
    "Please don't share passwords, tokens, or account details here -- if that "
    "wasn't a credential, try rephrasing without a long random-looking string."
)
_PROPOSAL_FAILED_REPLY = "I couldn't send your request yet -- I'll try again."
_PROPOSAL_RETRY_WAIT_REPLY = "Still working on sending your request -- I'll try again shortly."
# Round-3 ruling (6): a mid-intake message that is not a usable answer (empty
# or whitespace-only after stripping) is nudged with this fixed line rather
# than silently captured as an answer or falling through to the host -- the
# deterministic script owns a genuinely-pending turn; a nudge is honest about
# what's needed without guessing at intent.
_UNRELATED_ANSWER_REPLY = "Please answer the current question."
# Round-3 ruling (1) / P0-B: the fixed line for a member whose proposal is
# already held while the server still reports intake_state=='pending' (server
# lag, or a server-side bug) -- never a new question, never a second
# proposal, just this.
_AWAITING_HUMANS_REPLY = "Your request is awaiting the humans."


def _project_not_found_reply(name: str) -> str:
    safe = name.strip()[:120] or "that"
    return f'I couldn\'t find a project called "{safe}" -- check the name with your captain and try again.'


def _next_backoff(retry_count: int) -> float:
    index = min(max(retry_count, 0), len(_PROPOSAL_RETRY_BACKOFF_SECONDS) - 1)
    return _PROPOSAL_RETRY_BACKOFF_SECONDS[index]


# --------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class FirstPersonSettings:
    """Non-secret configuration for the first-person intake handler.

    Deliberately self-contained (does not depend on ``telegram_control_enabled``):
    the status probe reuses the shape of telegram_control's authenticated
    envelope, not its enable flag or its running handler.
    """

    enabled: bool
    base_url: str
    webhook_secret_env: str = "IM_WEBHOOK_SECRET"
    timeout: float = 20.0

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "FirstPersonSettings":
        enabled = value.get("first_person_enabled", False)
        if not isinstance(enabled, bool):
            raise ValueError("first_person_enabled must be a boolean")

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


# --------------------------------------------------------------------------
# Status resolution -- server-declared newness, never a local guess.
# --------------------------------------------------------------------------

_VALID_INTAKE_STATES = frozenset({"none", "pending", "complete"})


def _classify_intake_state(value: Any) -> str:
    """Normalize ANY shape the wire's ``intake_state`` field might carry to a
    known state string, defaulting to ``"unknown"`` for anything foreign.

    Round-3 P1-C / ruling (3): ``value not in _VALID_INTAKE_STATES`` raises
    ``TypeError`` when ``value`` is unhashable (a list or a dict) -- this
    classifies the TYPE first so a malformed wire value (list, dict, int,
    bool, None) always resolves to ``"unknown"`` and never raises.
    """
    if isinstance(value, str) and value in _VALID_INTAKE_STATES:
        return value
    return "unknown"


@dataclass(frozen=True)
class StatusResolution:
    bound: bool
    member_id: str | None
    home_squad_id: str | None
    intake_state: str  # "none" | "pending" | "complete" | "unknown"

    @property
    def is_pending(self) -> bool:
        return self.bound and self.member_id is not None and self.intake_state == "pending"


_UNKNOWN_STATUS = StatusResolution(bound=False, member_id=None, home_squad_id=None, intake_state="unknown")


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


class _StatusCache:
    """Per-user_id status cache. Round-2 P1-2: this cache IS the per-sender
    rate limit -- a lookup served from cache never reaches the network, so no
    user_id can trigger more than one real status probe per TTL window. Never
    stores display name or message text (only :class:`StatusResolution`).

    Round-3 P1-D / ruling (4): bounded LRU (``max_entries``) so a burst of
    distinct brand-new sender ids cannot grow this dict without limit --
    `OrderedDict` gives O(1) "most-recently-used" reordering on both read and
    write, and eviction always removes the LEAST-recently-used entry first.
    Also sweeps already-expired entries periodically (every
    ``sweep_every`` inserts) so memory turns over well before the cache is
    ever at capacity, independent of the LRU eviction that only fires once it
    is full.
    """

    def __init__(
        self,
        *,
        positive_ttl: float = _STATUS_POSITIVE_TTL_SECONDS,
        negative_ttl: float = _STATUS_NEGATIVE_TTL_SECONDS,
        unknown_ttl: float = _STATUS_UNKNOWN_TTL_SECONDS,
        max_entries: int = _STATUS_CACHE_MAX_ENTRIES,
        sweep_every: int = _STATUS_CACHE_SWEEP_EVERY,
        clock: Any = time.monotonic,
    ) -> None:
        self._entries: "OrderedDict[str, tuple[float, StatusResolution]]" = OrderedDict()
        self._positive_ttl = positive_ttl
        self._negative_ttl = negative_ttl
        self._unknown_ttl = unknown_ttl
        self._max_entries = max(1, max_entries)
        self._sweep_every = max(1, sweep_every)
        self._clock = clock
        self._lock = threading.Lock()
        self._puts_since_sweep = 0

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def get(self, user_id: str) -> StatusResolution | None:
        with self._lock:
            entry = self._entries.get(user_id)
            if entry is None:
                return None
            expires_at, resolution = entry
            if self._clock() >= expires_at:
                del self._entries[user_id]
                return None
            self._entries.move_to_end(user_id)
            return resolution

    def _ttl_for(self, resolution: StatusResolution) -> float:
        if resolution.is_pending:
            return self._positive_ttl
        if resolution.intake_state == "unknown":
            return self._unknown_ttl
        return self._negative_ttl

    def put(self, user_id: str, resolution: StatusResolution) -> None:
        ttl = self._ttl_for(resolution)
        with self._lock:
            self._entries[user_id] = (self._clock() + ttl, resolution)
            self._entries.move_to_end(user_id)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)  # evict least-recently-used

            self._puts_since_sweep += 1
            if self._puts_since_sweep >= self._sweep_every:
                self._puts_since_sweep = 0
                now = self._clock()
                expired = [key for key, (expires_at, _) in self._entries.items() if now >= expires_at]
                for key in expired:
                    del self._entries[key]


class _ProbeLimiter:
    """Global cap on concurrently in-flight status probes.

    Round-3 P1-D / ruling (4): the per-user_id cache above only rate-limits
    REPEAT lookups for the SAME sender; it does nothing to bound the
    aggregate cost of a burst of DISTINCT brand-new senders, each of whom
    gets exactly one real network call. This caps that aggregate concurrency
    on the shared ``asyncio.to_thread`` pool. A caller that cannot acquire a
    slot within ``acquire_timeout`` fails fast (never queues indefinitely) --
    the caller treats that exactly like any other probe failure: resolve to
    unknown, fail-safe for the intake.
    """

    def __init__(
        self,
        max_concurrent: int = _STATUS_PROBE_MAX_CONCURRENCY,
        acquire_timeout: float = _STATUS_PROBE_ACQUIRE_TIMEOUT_SECONDS,
    ) -> None:
        self._semaphore = threading.BoundedSemaphore(max(1, max_concurrent))
        self._acquire_timeout = acquire_timeout

    def try_acquire(self) -> bool:
        return self._semaphore.acquire(timeout=self._acquire_timeout)

    def release(self) -> None:
        self._semaphore.release()


def sanitized_first_contact_envelope(update: Any) -> dict[str, Any]:
    """Validate + shape the SAME authenticated-DM fence telegram_control.py's
    ``_sanitized_envelope`` enforces (private chat, sender == chat, not
    forwarded) -- for ANY private-DM text message, not only a recognized
    ``/command``.

    Round-2 M5 hardening: each of the three checks below (chat type, sender ==
    chat, not forwarded) is independently load-bearing -- the type check alone
    is never sufficient (see tests/test_first_person.py's dedicated fence
    tests, each breaking exactly one check at a time).

    Raises ``ValueError`` for anything outside first-person's authenticated
    scope -- the caller must treat that as "not my concern, let normal
    handling continue", never as "unbound".
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

    return {
        "update_id": update_id,
        "chat_id": chat_id,
        "user_id": user_id,
        "text": text,
    }


def _status_probe_body(user_id: Any, chat_id: Any) -> bytes:
    # Deliberately minimal: identity coordinates ONLY. Round-2 P1-1/P1-2: never
    # a synthesized /start (that relayed a full fake Telegram Update including
    # the sender's display name on every single message -- stranger-floodable
    # and an unnecessary leak to the bind surface). This is a dedicated,
    # read-only status probe, not a command execution.
    payload = {"kind": "first_person_status_probe", "user_id": user_id, "chat_id": chat_id}
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def resolve_member_status(
    settings: FirstPersonSettings,
    user_id: Any,
    chat_id: Any,
    *,
    secret_owner: ProfileSecretOwner | None = None,
    cache: _StatusCache | None = None,
    probe_limiter: "_ProbeLimiter | None" = None,
) -> StatusResolution:
    """Resolve the Telegram sender's bound member + intake progress through the
    authenticated ``/im/webhook`` surface -- the ONLY channel this module
    trusts for identity and for "is intake actually in progress".

    Mupot-side contract this depends on (see PR body -- Athena's round-1 ruling
    on this reshape, 2026-09-21, is binding: code against this now, fail-safe
    for the intake while it is absent): the probe response must carry
    ``{"ok": true, "bound": bool, "member_id": str|null, "home_squad_id":
    str|null, "intake_state": "none"|"pending"|"complete"}``. This function
    never parses a human-readable ``reply`` string for any decision (see
    kasra-review's PR#15 finding: comparing rendered prose against local
    literals fails open -- a different, accidental failure mode this module
    deliberately avoids). Anything missing, malformed, or simply not shipped
    yet resolves to ``intake_state="unknown"`` -- which the caller treats
    exactly like "not pending": fail-safe for the intake, never consume the
    update, the host handler owns the turn.
    """

    cache_key = str(user_id)
    if cache is not None:
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

    require_supported_profile_runtime({})
    body = _status_probe_body(user_id, chat_id)
    # Round-3 P1-D / ruling (4): the status probe uses its OWN short timeout,
    # never longer than the operator-configured settings.timeout (which may
    # be configured up to 120s) -- a brand-new sender must never be able to
    # park a thread on the shared asyncio.to_thread pool for anywhere near
    # that long.
    probe_timeout = min(float(settings.timeout), _STATUS_PROBE_TIMEOUT_SECONDS)

    def _do() -> StatusResolution:
        try:
            secret = read_profile_secret(settings.webhook_secret_env)
        except RuntimeError:
            return _UNKNOWN_STATUS
        if len(secret) > 256 or len(body) > _MAX_REQUEST_BYTES:
            return _UNKNOWN_STATUS

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
            with build_opener(_NoRedirect()).open(request, timeout=probe_timeout) as response:
                raw = response.read(_MAX_RESPONSE_BYTES + 1)
        except Exception:
            return _UNKNOWN_STATUS

        if len(raw) > _MAX_RESPONSE_BYTES:
            return _UNKNOWN_STATUS
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return _UNKNOWN_STATUS
        if not isinstance(parsed, dict) or parsed.get("ok") is not True:
            return _UNKNOWN_STATUS

        bound = parsed.get("bound")
        member_id = parsed.get("member_id")
        home_squad_id = parsed.get("home_squad_id")
        # Round-3 P1-C / ruling (3): classify the TYPE first -- `x in
        # frozenset(...)` raises TypeError for an unhashable wire value (a
        # list or a dict), which must resolve to "unknown", never raise.
        intake_state = _classify_intake_state(parsed.get("intake_state"))
        if not isinstance(bound, bool):
            return _UNKNOWN_STATUS
        if not bound:
            return StatusResolution(bound=False, member_id=None, home_squad_id=None, intake_state="unknown")
        if not isinstance(member_id, str) or not member_id.strip():
            return _UNKNOWN_STATUS
        if home_squad_id is not None and (not isinstance(home_squad_id, str) or not home_squad_id.strip()):
            return _UNKNOWN_STATUS
        if intake_state == "unknown":
            return _UNKNOWN_STATUS
        return StatusResolution(
            bound=True,
            member_id=member_id.strip(),
            home_squad_id=home_squad_id.strip() if isinstance(home_squad_id, str) else None,
            intake_state=intake_state,
        )

    def _do_with_limit() -> StatusResolution:
        if probe_limiter is None:
            return _do()
        if not probe_limiter.try_acquire():
            # Global concurrency cap exhausted -- fail fast, never queue.
            return _UNKNOWN_STATUS
        try:
            return _do()
        finally:
            probe_limiter.release()

    resolution = _do_with_limit() if secret_owner is None else _with_secret_owner(secret_owner, _do_with_limit)
    if cache is not None:
        cache.put(cache_key, resolution)
    return resolution


def _with_secret_owner(secret_owner: ProfileSecretOwner, fn: Any) -> Any:
    with secret_owner.activate():
        return fn()


# --------------------------------------------------------------------------
# Durable completion marker -- NEVER raw answer text, see module docstring (b).
# --------------------------------------------------------------------------


class FirstPersonStateStore:
    """Durable, atomically-written completion marker.

    Deliberately re-implements the temp-file + fsync + rename shape (matching
    ``mupot_gateway.adapter.StateStore`` and ``telegram_inline_approval``'s
    ``ApprovalReceiptStore``) in its OWN file, never the shared adapter
    ``state.json`` -- this module must work in restricted operator mode with no
    native gateway at all, and must never risk clobbering another consumer's
    keys in a shared dict.
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


@dataclass(frozen=True)
class Intake:
    """In-memory-only intake progress. Never persisted as a whole -- only the
    final ``engrams``/``proposal_id`` are written, once, at a SUCCESSFUL
    completion (round-2 P0-3: a failed proposal submission must not write this
    marker -- see :func:`_submit_proposal`).

    Round-3 P3-G / ruling (7): frozen. ``home_squad_id`` (and ``member_id``)
    are set exactly once, from ``StatusResolution.home_squad_id`` (server
    status is the ONLY source now -- see :func:`handle_first_contact`'s
    HOME-CREATION AUTHORITY note; this module has no call of its own that
    could mint one), in :meth:`FirstPersonRuntime.start`, and NOTHING in
    this module ever
    reassigns them afterward -- in particular never derives them from an
    answer's text (round-2 mutation M4). Progress fields (``index``,
    ``project_id``, ``engrams``, the proposal-retry pair) still change over
    time, but only via :class:`FirstPersonRuntime`'s methods, each of which
    builds a NEW ``Intake`` with ``dataclasses.replace`` and never passes
    ``member_id``/``home_squad_id`` as a changed kwarg -- so those two fields
    are structurally impossible to alter after construction, generically,
    regardless of what any answer's text contains.
    """

    member_id: str
    home_squad_id: str
    index: int = 0
    project_id: str | None = None
    engrams: dict[str, str] = field(default_factory=dict)
    created_at: float = field(default_factory=time.monotonic)
    proposal_retry_count: int = 0
    next_proposal_retry_at: float = 0.0


class FirstPersonRuntime:
    """Process-global, in-memory-first intake registry.

    An unbound Telegram sender -- or one whose status does not resolve
    ``intake_state == "pending"`` -- gets NO entry here, ever;
    ``handle_first_contact`` returns before this is ever touched for those
    cases, and drops any stale entry it finds.
    """

    def __init__(
        self,
        state_path: Path,
        *,
        clock: Any = time.monotonic,
        wall_clock: Any = time.time,
    ) -> None:
        self._lock = threading.Lock()
        self._pending: dict[str, Intake] = {}
        self.store = FirstPersonStateStore(state_path)
        self._clock = clock
        self._wall_clock = wall_clock

    def is_complete_or_unknown(self, member_id: str) -> bool:
        """Round-2 P1-4 + Athena's round-2 condition (ii): fail CLOSED on an
        invalid/truncated store (treat an unreadable store the same as
        "already handled", never as "safe to (re-)onboard"), but this local
        marker is a bounded-lifetime CACHED VIEW of server state, never the
        record itself. mupot's own ``intake_state`` (re-read on every message
        via :func:`resolve_member_status`) is what actually gates every
        decision to consume an update; this only exists as a short local
        dedupe window so a burst of messages within one completed intake
        cannot double-trigger a second intake before the server's own state
        has had a chance to reflect it. Once
        that window (``_COMPLETION_CACHE_TTL_SECONDS``) has passed, this
        defers entirely back to whatever the server says next.
        """
        data, valid = self.store.load_checked()
        if not valid:
            return True
        completed = data.get("completed")
        if not isinstance(completed, dict) or member_id not in completed:
            return False
        entry = completed[member_id]
        completed_at = entry.get("completed_at") if isinstance(entry, dict) else None
        if not isinstance(completed_at, (int, float)) or isinstance(completed_at, bool):
            return True  # no freshness info recorded -- fail closed, don't guess
        return (self._wall_clock() - completed_at) <= _COMPLETION_CACHE_TTL_SECONDS

    def held_proposal_id(self, member_id: str) -> str | None:
        """Durable read of a previously-submitted proposal_id for member_id,
        ignoring :attr:`_COMPLETION_CACHE_TTL_SECONDS` entirely.

        Round-3 P0-B / ruling (1): that TTL bounds how long the completion
        marker is trusted as a FRESHNESS cache (see
        :meth:`is_complete_or_unknown`); it must never bound the separate,
        permanent invariant that this module never submits a second proposal
        for a member it already holds one for. mupot PR#1488 derives
        ``intake_state=='complete'`` from the EXISTENCE of exactly this
        proposal -- if the server still reports ``'pending'`` while this
        module holds a real ``proposal_id`` here, the server is lagging or
        buggy, never a reason to re-onboard. Returns ``None`` on a missing
        entry, a missing/blank ``proposal_id`` field, or an unreadable store
        (deliberately permissive here -- :meth:`is_complete_or_unknown`'s
        TTL-bounded check is what fails closed on a corrupted store; this
        method only ever adds an EXTRA reason to refuse, never a reason to
        proceed that the other check wouldn't already allow).
        """
        data, valid = self.store.load_checked()
        if not valid:
            return None
        completed = data.get("completed")
        if not isinstance(completed, dict) or member_id not in completed:
            return None
        entry = completed[member_id]
        if not isinstance(entry, dict):
            return None
        proposal_id = entry.get("proposal_id")
        return proposal_id.strip() if isinstance(proposal_id, str) and proposal_id.strip() else None

    def get_pending(self, chat_key: str) -> Intake | None:
        with self._lock:
            intake = self._pending.get(chat_key)
            if intake is None:
                return None
            if self._clock() - intake.created_at > _PENDING_TTL_SECONDS:
                del self._pending[chat_key]
                return None
            return intake

    def start(self, chat_key: str, *, member_id: str, home_squad_id: str) -> Intake:
        with self._lock:
            intake = Intake(member_id=member_id, home_squad_id=home_squad_id, created_at=self._clock())
            self._pending[chat_key] = intake
            return intake

    def record_answer(self, chat_key: str, question_id: str, engram_id: str) -> Intake | None:
        """Advance progress by building a NEW frozen ``Intake`` (P3-G) --
        ``member_id``/``home_squad_id`` are always copied over unchanged from
        the current record, never passed as a kwarg here, so they cannot
        drift regardless of ``question_id``/``engram_id``. Returns the
        updated ``Intake``, or ``None`` if the pending record vanished
        (TTL/abandon race) -- the caller must treat that as "nothing left to
        advance", never retry the mutation."""
        with self._lock:
            intake = self._pending.get(chat_key)
            if intake is None:
                return None
            updated = replace(
                intake,
                engrams={**intake.engrams, question_id: engram_id},
                index=intake.index + 1,
            )
            self._pending[chat_key] = updated
            return updated

    def set_project_id(self, chat_key: str, project_id: str) -> Intake | None:
        """Same replace-in-place shape as :meth:`record_answer`, for the one
        other progress field mutated outside ``finish``/``abandon``."""
        with self._lock:
            intake = self._pending.get(chat_key)
            if intake is None:
                return None
            updated = replace(intake, project_id=project_id)
            self._pending[chat_key] = updated
            return updated

    def schedule_proposal_retry(self, chat_key: str, *, next_retry_at: float, retry_count: int) -> Intake | None:
        """Same replace-in-place shape as :meth:`record_answer`, for the
        proposal-retry backoff pair (round-2 P0-3)."""
        with self._lock:
            intake = self._pending.get(chat_key)
            if intake is None:
                return None
            updated = replace(intake, next_proposal_retry_at=next_retry_at, proposal_retry_count=retry_count)
            self._pending[chat_key] = updated
            return updated

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
            "completed_at": self._wall_clock(),
        }
        data["completed"] = completed
        self.store.save(data)

    def abandon(self, chat_key: str) -> None:
        with self._lock:
            self._pending.pop(chat_key, None)


def _build_resolve_project_request(
    settings: FirstPersonSettings,
    secret: str,
    envelope: Mapping[str, Any],
    query: str,
) -> Request:
    """Builds the ONE request this module ever sends to resolve a project
    reference. Isolated in this single function ON PURPOSE (Athena's ruling,
    2026-09-21) so that any future fence change to this route is a
    one-function edit.

    History: mupot#1488 first exposed ``POST /im/resolve-project`` keyed on a
    bare ``chat_id`` field. Adversarial review of #1488's grant chain found
    that fence is NO fence at all -- any holder of the shared webhook secret
    could supply an arbitrary ``chat_id`` and act as whichever member happens
    to be bound to it; a bare id is not proof of provenance the way an
    authenticated Telegram update is. Athena's ruling: the fix is ENVELOPE
    IDENTITY, not a per-intake token (a token idea was floated and withdrawn).
    This request now carries the SAME authenticated Telegram envelope shape
    ``telegram_control.py``'s ``_sanitized_envelope``/``relay_telegram_update``
    already relay to ``/im/webhook`` -- ``{update_id, message: {from: {id},
    chat: {id, type}, text}}`` -- built from THIS turn's own already-fenced
    envelope (:func:`sanitized_first_contact_envelope`), never a synthesized
    or stale one, plus a top-level ``query``. The server is expected to
    derive the member from the envelope exactly as ``/webhook`` does; this
    module never sends ``member_id`` or a bare ``chat_id`` as a selector.

    ASSUMPTION FLAG -- re-verify before this ships: coded against
    "envelope + query" per Athena's ruling. The successor branch
    (``kasra/fp01-slice2-proposal-chain-v2``) did not exist yet as of this
    build -- only ``kasra/fp01-slice2-proposal-chain``, whose
    ``/resolve-project`` still used the now-superseded bare-``chat_id``
    fence (verified by reading that branch's ``src/im/index.ts`` directly).
    Confirm this exact request shape against the successor's actual handler
    once it exists.

    ``message.from`` deliberately carries ONLY ``id`` -- no display-name field,
    matching every other first-person probe's "never send a display name"
    rule (round-2 P1-1/P1-2) -- and ``message.text`` is left empty: the
    project name the human typed travels ONLY in ``query``, never duplicated
    into the envelope's own text field.
    """
    body = {
        "update_id": envelope["update_id"],
        "message": {
            "from": {"id": envelope["user_id"]},
            "chat": {"id": envelope["chat_id"], "type": "private"},
            "text": "",
        },
        "query": query,
    }
    return Request(
        urljoin(settings.base_url.rstrip("/") + "/", "/im/resolve-project"),
        data=json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Telegram-Bot-Api-Secret-Token": secret,
        },
        method="POST",
    )


def _resolve_member_project(
    settings: FirstPersonSettings,
    envelope: Mapping[str, Any],
    query: str,
    *,
    secret_owner: ProfileSecretOwner | None = None,
) -> str | None:
    """Resolve a project reference typed by a human against ONLY the projects
    that member can read (round-2 P2-1: never Mubot's own operator-wide
    ``project_list``), via the authenticated ``/im/resolve-project`` surface
    -- see :func:`_build_resolve_project_request` for the exact request shape
    and the fence history. Response shape (mupot#1488, chat_id-fence version
    verified live; the successor's is assumed identical apart from the
    request's own identity fence): ``{bound: bool, member_id: str|null,
    projects: [{id, slug, name}, ...]}``. Fails closed (``None``) on any
    transport/shape failure, an unbound result, no match, or an AMBIGUOUS
    multi-candidate result with no exact slug match -- this module never
    guesses among fuzzy candidates; the member is re-asked instead."""

    def _do() -> str | None:
        try:
            secret = read_profile_secret(settings.webhook_secret_env)
        except RuntimeError:
            return None
        if len(secret) > 256:
            return None
        request = _build_resolve_project_request(settings, secret, envelope, query)
        if len(request.data or b"") > _MAX_REQUEST_BYTES:
            return None
        try:
            with build_opener(_NoRedirect()).open(
                request, timeout=min(float(settings.timeout), _STATUS_PROBE_TIMEOUT_SECONDS)
            ) as response:
                raw = response.read(_MAX_RESPONSE_BYTES + 1)
        except Exception:
            return None
        if len(raw) > _MAX_RESPONSE_BYTES:
            return None
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        if not isinstance(parsed, dict) or parsed.get("bound") is not True:
            return None
        projects = parsed.get("projects")
        if not isinstance(projects, list) or not projects:
            return None
        top = projects[0]
        if not isinstance(top, dict):
            return None
        exact = isinstance(top.get("slug"), str) and top["slug"].strip().lower() == query.strip().lower()
        if len(projects) > 1 and not exact:
            return None  # ambiguous -- never guess among fuzzy candidates
        candidate = top.get("id")
        return candidate.strip() if isinstance(candidate, str) and candidate.strip() else None

    return _do() if secret_owner is None else _with_secret_owner(secret_owner, _do)


# --------------------------------------------------------------------------
# Credential + control-character hygiene (round-2 P2-2 / P2-3)
# --------------------------------------------------------------------------

_CREDENTIAL_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"mupot_[A-Za-z0-9_-]{8,}"),
    re.compile(r"gh[oprsu]_[A-Za-z0-9]{20,}"),
    re.compile(r"sk-[A-Za-z0-9_-]{16,}"),
    re.compile(r"AKIA[A-Z0-9]{12,}"),
    re.compile(r"\b[0-9a-fA-F]{32,}\b"),
    re.compile(r"\b[A-Za-z0-9+/_-]{32,}={0,2}\b"),
)


def _looks_like_credential(text: str) -> bool:
    return any(pattern.search(text) for pattern in _CREDENTIAL_PATTERNS)


# Bidi override / isolate control points -- can make a pasted answer render as
# something other than what it contains.
_BIDI_CONTROL_CHARS = frozenset(
    "‪‫‬‭‮‎‏⁦⁧⁨⁩"
)


def _sanitize_answer(text: str) -> str:
    """Normalize FIRST: this is the front half of the round-3 P0-A pipeline
    invariant ("the store never receives an un-normalized byte sequence") --
    NFKC first (collapses compatibility/full-width lookalikes), then strip
    bidi-override/isolate and Cc control characters. Credential-refusal in
    :func:`_handle_answer` runs on THIS function's output, never on the raw
    text, so an obfuscated token (a real credential with an invisible
    character spliced into the middle) is caught: normalize reassembles it
    into its plain form BEFORE the credential check ever sees it, instead of
    the round-2 bug's order (check raw text, then normalize), which let the
    control character break the regex's contiguous match and only spliced
    the valid token back together afterward."""
    normalized = unicodedata.normalize("NFKC", text)
    cleaned_chars = []
    for ch in normalized:
        if ch in _BIDI_CONTROL_CHARS:
            continue
        if ch in ("\n", "\t"):
            cleaned_chars.append(ch)
            continue
        if unicodedata.category(ch) == "Cc":
            continue
        cleaned_chars.append(ch)
    return "".join(cleaned_chars).strip()[:_MAX_ANSWER_CHARS]


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
    settings: FirstPersonSettings,
    envelope: Mapping[str, Any],
    secret_owner: ProfileSecretOwner | None = None,
) -> None:
    raw_answer = getattr(message, "text", None)
    if not isinstance(raw_answer, str) or not raw_answer.strip():
        # Round-3 ruling (6): not a usable answer at all -- nudge, don't
        # capture, don't fall through.
        await message.reply_text(_UNRELATED_ANSWER_REPLY)
        return

    question_id, question_text = FIRST_PERSON_QUESTIONS[intake.index]

    # Round-3 P0-A / ruling (2): NORMALIZE FIRST, then credential-refuse on
    # the NORMALIZED text, then (if it passes) memory. This is the whole
    # pipeline invariant -- "the store never receives an un-normalized byte
    # sequence" -- checking credential-shape on `raw_answer` (the round-2
    # order) let an embedded bidi/control char split a token across the
    # regex's contiguous match; the sanitizer that ran AFTER then deleted
    # exactly that character and reassembled a valid token. See
    # tests/test_first_person.py's mutation-regression receipt for this.
    cleaned_answer = _sanitize_answer(raw_answer)
    if _looks_like_credential(cleaned_answer):
        # Refused before the normalized text ever reaches storage/logging --
        # no trace of it (raw or cleaned) survives this branch.
        await message.reply_text(f"{_CREDENTIAL_REPLY} {question_text}")
        return
    if not cleaned_answer:
        await message.reply_text(_WRITE_FAILURE_REPLY)
        return

    if question_id == "project" and intake.project_id is None:
        def resolve_project() -> str | None:
            return _resolve_member_project(settings, envelope, cleaned_answer, secret_owner=secret_owner)

        project_id = await asyncio.to_thread(resolve_project)
        # cleaned_answer used only to compose this reply; this call frame ends
        # right after -- nothing here writes it anywhere.
        if project_id is None:
            await message.reply_text(_project_not_found_reply(cleaned_answer))
            return
        updated = runtime.set_project_id(chat_key, project_id)
        if updated is None:
            return  # race: pending vanished (TTL/abandon) mid-resolution
        intake = updated

    def remember() -> dict[str, Any]:
        return client.call(
            "squad_remember",
            {"squad_id": intake.home_squad_id, "text": cleaned_answer, "concepts": [question_id]},
        )

    response = await asyncio.to_thread(remember)
    # `raw_answer`/`cleaned_answer` are locals of this call frame; nothing else
    # in the process holds a reference to either, and neither is ever written
    # anywhere below.
    engram_id = None
    result = response.get("result") if isinstance(response, dict) else None
    if isinstance(response, dict) and response.get("ok") is True and isinstance(result, dict):
        candidate = result.get("engram_id")
        if isinstance(candidate, str) and candidate.strip():
            engram_id = candidate.strip()

    if engram_id is None:
        # No confirmed write: retry from memory next message, never spill to
        # disk. The pending intake is left exactly as it was.
        await message.reply_text(_WRITE_FAILURE_REPLY)
        return

    updated = runtime.record_answer(chat_key, question_id, engram_id)
    if updated is None:
        return  # race: pending vanished (TTL/abandon) mid-write -- the
        # engram write-through already succeeded and is safe (write-through
        # only, never a transcript); nothing left here to advance.

    if updated.index >= len(FIRST_PERSON_QUESTIONS):
        await _submit_proposal(message, chat_key, updated, client=client, runtime=runtime)
        return
    await message.reply_text(FIRST_PERSON_QUESTIONS[updated.index][1])


async def _submit_proposal(
    message: Any,
    chat_key: str,
    intake: Intake,
    *,
    client: MupotOperatorClient,
    runtime: FirstPersonRuntime,
    clock: Any = time.monotonic,
) -> None:
    project_id = intake.project_id
    if not project_id:
        # Round-3 P2-E / ruling (5): kill the false-success branch. No
        # project ever resolved (should not normally happen -- question 3
        # gates on it), but this is a genuine failure, never a completion --
        # no marker without a real proposal_id, ever. Route it through the
        # exact same honest-failure-with-retry shape as a failed submission
        # below: honest reply, WARNING (member_id only, no PII), backoff.
        logger.warning(
            "mupot plugin: first-person proposal has no project_id member_id=%s "
            "-- treating as a failed submission, not completing",
            intake.member_id,
        )
        runtime.schedule_proposal_retry(
            chat_key,
            next_retry_at=clock() + _next_backoff(intake.proposal_retry_count),
            retry_count=intake.proposal_retry_count + 1,
        )
        await message.reply_text(_PROPOSAL_FAILED_REPLY)
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
                    # "project_access" is a mupot-side contract dependency --
                    # see PR body. The live routine_proposal_submit schema
                    # checked during this build only accepts
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

    try:
        response = await asyncio.to_thread(submit)
    except Exception as exc:
        response = {"ok": False, "error": type(exc).__name__}

    result = response.get("result") if isinstance(response, dict) else None
    proposal_id = None
    ok = isinstance(response, dict) and response.get("ok") is True
    if ok and isinstance(result, dict):
        candidate = result.get("proposal_id") or result.get("id")
        if isinstance(candidate, str) and candidate.strip():
            proposal_id = candidate.strip()

    if not ok or proposal_id is None:
        # Round-2 P0-3: success-shaped no-op forbidden. Never write the
        # completion marker on a failed (or unidentifiable) submission -- keep
        # the pending intake exactly as it is, tell the member the truth, log
        # at WARNING with no answer text and no PII (member/project ids only,
        # which the operator already holds), and back off before retrying.
        logger.warning(
            "mupot plugin: first-person proposal submission failed member_id=%s "
            "project_id=%s error=%s",
            intake.member_id,
            project_id,
            response.get("error") if isinstance(response, dict) else "unknown",
        )
        runtime.schedule_proposal_retry(
            chat_key,
            next_retry_at=clock() + _next_backoff(intake.proposal_retry_count),
            retry_count=intake.proposal_retry_count + 1,
        )
        await message.reply_text(_PROPOSAL_FAILED_REPLY)
        return

    runtime.finish(chat_key, proposal_id=proposal_id)
    await message.reply_text(_COMPLETE_REPLY)


async def _retry_proposal_if_due(
    message: Any,
    chat_key: str,
    intake: Intake,
    *,
    client: MupotOperatorClient,
    runtime: FirstPersonRuntime,
    clock: Any = time.monotonic,
) -> None:
    if clock() < intake.next_proposal_retry_at:
        await message.reply_text(_PROPOSAL_RETRY_WAIT_REPLY)
        return
    await _submit_proposal(message, chat_key, intake, client=client, runtime=runtime, clock=clock)


async def handle_first_contact(
    update: Any,
    *,
    settings: FirstPersonSettings,
    client: MupotOperatorClient,
    runtime: FirstPersonRuntime,
    secret_owner: ProfileSecretOwner | None = None,
    status_cache: _StatusCache | None = None,
    probe_limiter: "_ProbeLimiter | None" = None,
) -> bool:
    """Handle one inbound Telegram update for the first-person flow.

    Returns ``True`` iff this update was fully handled here (the caller must
    stop further propagation) and ``False`` iff first-person has nothing to do
    with this update -- normal handling (Hermes's own LLM turn, the plain-text
    ``approve <id>`` decision path, the group-99 platform observer) continues
    completely untouched.

    Round-2 gate (replaces round-1's local "have I seen this chat" check):
    consumes an update ONLY when the mupot-declared ``intake_state`` for this
    sender is ``"pending"`` -- never from local state alone. This is re-checked
    on EVERY message, including mid-intake ones (round-2 P1-3): an unbind, a
    server-side reset, or the contract fields simply not existing yet on a
    given deployment all fail-safe for the intake here, every time -- never
    consumed, the host handler owns the turn.

    Round-3 P0-B / ruling (1): ONE exception to "server says pending ->
    proceed" -- if this module already holds a ``proposal_id`` for the
    resolved member (see ``FirstPersonRuntime.held_proposal_id``), a
    ``pending`` report is treated as server lag/bug, not license to
    re-intake: the update IS consumed (``True``), but only to reply the fixed
    "awaiting the humans" line -- no question, no mupot mutation, no second
    proposal.
    """

    # Edited messages must never re-enter as the "next answer" -- the PTB
    # registration filters on filters.UpdateType.MESSAGE for this; this is the
    # belt-and-suspenders check for the pure-Python path this function runs in
    # tests without PTB in the loop at all.
    if getattr(update, "edited_message", None) is not None:
        return False

    message = getattr(update, "effective_message", None)
    if message is None:
        return False
    try:
        envelope = sanitized_first_contact_envelope(update)
    except ValueError:
        return False  # not a fenced private DM -- not first-person's concern

    chat_key = str(envelope["chat_id"])

    def resolve() -> StatusResolution:
        return resolve_member_status(
            settings,
            envelope["user_id"],
            envelope["chat_id"],
            secret_owner=secret_owner,
            cache=status_cache,
            probe_limiter=probe_limiter,
        )

    status = await asyncio.to_thread(resolve)
    pending = runtime.get_pending(chat_key)

    if not status.is_pending:
        # Unbound, never-started, already-onboarded, or the contract fields
        # aren't live on this deployment yet (intake_state=="unknown") --
        # every one of these is fail-safe for the intake: never consumed, the
        # host handler owns the turn. Drop any stale local record; store
        # nothing.
        if pending is not None:
            runtime.abandon(chat_key)
        return False

    if pending is not None and pending.member_id != status.member_id:
        # A different member now resolves for this chat (e.g. a rebind) --
        # never carry progress across identities.
        runtime.abandon(chat_key)
        pending = None

    if pending is None:
        held_proposal_id = runtime.held_proposal_id(status.member_id)
        if held_proposal_id is not None:
            # Round-3 P0-B / ruling (1) -- HARD RULE: this module already
            # holds a proposal for this member (mupot PR#1488: the server
            # derives intake_state=='complete' from the EXISTENCE of exactly
            # this proposal). The server still reporting 'pending' here is
            # server lag or a server-side bug -- never a reason to re-onboard.
            # Consume nothing mutating: no home creation, no question, no
            # second proposal. Exactly one WARNING (member_id + the held
            # proposal_id only, no PII), and the fixed reassurance line.
            logger.warning(
                "mupot plugin: server reports intake_state='pending' for "
                "member_id=%s while proposal_id=%s is already held -- "
                "treating as complete-with-proposal, not re-intaking",
                status.member_id,
                held_proposal_id,
            )
            await message.reply_text(_AWAITING_HUMANS_REPLY)
            return True

        if runtime.is_complete_or_unknown(status.member_id):
            return False

        if not status.home_squad_id:
            # Verified on mupot's kasra/fp01-slice2-proposal-chain, 2026-09-21:
            # createHomeForMember exists ONLY as an internal TypeScript
            # function (src/org/service.ts) -- grepping every call site in
            # that repo found none outside its own unit tests, which call it
            # directly by import. There is NO exposed MCP action and NO /im
            # route for it; `FIRST_PERSON_ACTIONS` no longer lists it (see
            # mupot_operator.py) precisely so nothing here can pretend
            # otherwise. Per brief 2f(a), home creation is gated by the
            # member's own first contact and is mupot's job alone -- this
            # module never invents a call for it. Fail-safe for the intake:
            # a bound, pending member with no home yet is left completely
            # untouched (no reply, no state) until a later status probe
            # reports a non-null home_squad_id.
            return False

        runtime.start(chat_key, member_id=status.member_id, home_squad_id=status.home_squad_id)
        await message.reply_text(FIRST_PERSON_QUESTIONS[0][1])
        return True

    if pending.index >= len(FIRST_PERSON_QUESTIONS):
        await _retry_proposal_if_due(message, chat_key, pending, client=client, runtime=runtime)
        return True

    await _handle_answer(
        message, chat_key, pending, client=client, runtime=runtime,
        settings=settings, envelope=envelope, secret_owner=secret_owner,
    )
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
    status_cache: _StatusCache | None = None,
    probe_limiter: "_ProbeLimiter | None" = None,
) -> None:
    settings.validate()
    if not settings.enabled:
        return

    if runtime is None:
        runtime = FirstPersonRuntime(state_path or default_state_path())
    if status_cache is None:
        status_cache = _StatusCache()
    if probe_limiter is None:
        probe_limiter = _ProbeLimiter()
    wired_applications: list[tuple[Any, Any]] = []

    def factory(application: Any, adapter: Any) -> None:
        # Hermes loads backend plugins without platform SDK dependencies. Keep
        # the optional PTB import inside the native factory invoked at connect
        # (same convention as telegram_control.py / telegram_inline_approval.py).
        from telegram.ext import ApplicationHandlerStop, MessageHandler, filters

        if any(existing is application for existing, _ in wired_applications):
            return

        async def handle(update: Any, context: Any) -> None:
            handled = await handle_first_contact(
                update,
                settings=settings,
                client=client,
                runtime=runtime,
                secret_owner=secret_owner,
                status_cache=status_cache,
                probe_limiter=probe_limiter,
            )
            if handled:
                raise ApplicationHandlerStop

        # UpdateType.MESSAGE excludes edited-message updates at the PTB
        # dispatch layer itself (round-2 fix: an edit must never re-enter as
        # the next answer).
        message_filter = filters.UpdateType.MESSAGE & filters.TEXT & ~filters.COMMAND
        handler = MessageHandler(message_filter, handle)
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

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

# Round-3-gate-2 P0: TWO separate timers, not one. `_PENDING_IDLE_TTL_SECONDS`
# is an IDLE timeout -- measured from `last_activity_at`, which is refreshed
# on every accepted answer (see FirstPersonRuntime.record_answer/touch) -- so
# a member answering thoughtfully every few minutes never trips it no matter
# how long the WHOLE conversation takes. The round-2 bug measured this same
# 600s window from `created_at` (set once, at the very first message): a
# member averaging more than 600s/5 = 120s per question got dropped and
# silently restarted at question 1 mid-conversation, with every subsequent
# answer then written to squad_remember under the WRONG question_id (whatever
# `index` the fresh restart happened to be at). `_PENDING_ABSOLUTE_TTL_SECONDS`
# is the separate hard cap on total session length regardless of activity, so
# a member who never quite goes idle cannot hold a local record open forever.
_PENDING_IDLE_TTL_SECONDS = 600.0
_PENDING_ABSOLUTE_TTL_SECONDS = 86400.0

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

# Round-3-gate-3 P1 (:1762): the per-user_id _StatusCache doubled as the ONLY
# per-sender rate limit on real network probes -- bypassing it entirely for a
# member with local pending progress (the fix for the round-3-gate-2 P2
# latch) silently removed that limit for exactly the population most likely
# to message repeatedly (an active, mid-intake member). This is its
# independent replacement: a minimum interval between real probes for the
# SAME user_id, enforced regardless of whether the shared cache is in play.
# Reused at the same cadence the cache's own positive TTL already provided
# (a pending member's status was already refreshed at most once per 15s).
_SENDER_PROBE_MIN_INTERVAL_SECONDS = _STATUS_POSITIVE_TTL_SECONDS

# Round-3-gate-3 P1 / Athena's ruling (2): the shared cache's TTL selection
# now splits on whether the server actually ANSWERED. A CONFIRMED result
# (bound=True with a real intake_state, or bound=False/"none" -- mupot said
# something definite) uses the normal positive/negative TTL. An "unknown"
# result -- a probe timeout, a malformed response, a secret-read failure, or
# the global _ProbeLimiter pool being momentarily exhausted -- is NEVER
# evidence about the sender at all, only about a transient condition on THIS
# process; caching it for the full _STATUS_UNKNOWN_TTL_SECONDS (900s) let one
# flood-causing sender (or one bad network blip) latch every OTHER sender's
# status into "unknown" for 15 minutes. resolve_member_status caches an
# "unknown" outcome, if at all, only for this much shorter window instead --
# long enough to blunt a tight repeat against a still-failing condition,
# nowhere near long enough to outlive whatever caused it. `_StatusCache`'s
# own `unknown_ttl` default (used when something calls `cache.put()`
# directly with an already-resolved value) is untouched by this -- this
# constant governs only resolve_member_status's OWN caching decision for a
# result it just produced.
_STATUS_UNKNOWN_SHORT_TTL_SECONDS = 30.0

# Round-3-gate-3 P3 (:836/:860): the completion journal is a rare fallback
# (only written when the main store is unreadable at finish() time), but
# nothing in this process may grow a local file without a bound. Compacted
# on every write to at most one entry per member_id; oldest-by-completed_at
# entries are dropped first once over this cap -- same bounded-LRU-by-another-
# name discipline as _StatusCache/_NotifyOncePerWindow above.
_JOURNAL_MAX_ENTRIES = 2048
# Round-3-gate-4 P3-c: the read cap MUST be derived from the write cap, not
# picked independently -- a fixed 256KiB read cap alongside a 2048-entry
# write cap meant a fully-compacted journal (2048 entries * a realistic
# ~150-200 bytes/line) could already exceed what one read would see, so
# append_journal's own read-then-recompact-then-write cycle would silently
# read only the TAIL of the file, treat that partial view as "everything",
# and rewrite the file WITHOUT the entries that fell outside the read
# window -- permanently losing already-compacted proposal_ids on the very
# next write, the opposite of what compaction is for. 512 bytes/entry is a
# generous per-line estimate (member_id + proposal_id + timestamp + JSON
# syntax) with headroom well beyond any realistic id length; deriving the
# read cap from the write cap this way means the two can never silently
# diverge again, whichever one changes.
_JOURNAL_MAX_ENTRY_BYTES_ESTIMATE = 512
_JOURNAL_READ_CAP_BYTES = _JOURNAL_MAX_ENTRIES * _JOURNAL_MAX_ENTRY_BYTES_ESTIMATE

# Proposal-submission retry backoff (round-2 P0-3): a failed submit must not be
# retried on literally the next keystroke.
_PROPOSAL_RETRY_BACKOFF_SECONDS: tuple[float, ...] = (5.0, 15.0, 60.0, 300.0)

# Round-3-gate-3 / Athena's ruling (4): the DEFECT this closes is the reply
# shape, not the underlying rejection -- the live routine_proposal_submit
# schema rejecting the "project_access" kind (see _submit_proposal's own
# comment) is a real, currently-permanent condition this module cannot fix
# from here. Retrying forever at the max backoff is fine (self-healing once
# the mupot-side contract ships); showing the member the IDENTICAL "I'll try
# again shortly" on literally every check-in forever, with no acknowledgment
# anything is actually stuck, is what made this read as a "permanent loop".
# Once the backoff table itself is exhausted (this many failed attempts),
# the member-facing reply switches to a distinct, bounded, honest one --
# rate-limited on its own window, same _NotifyOncePerWindow shape as the
# lag-guard's reassurance reply -- while retries keep happening in the
# background at the max backoff cadence.
_PROPOSAL_STALLED_AFTER_ATTEMPTS = len(_PROPOSAL_RETRY_BACKOFF_SECONDS)
_PROPOSAL_STALLED_REPLY_WINDOW_SECONDS = 86400.0

# Round-3-gate-2 P1 (:1408-1426) / P3: the held-proposal WARNING and its
# paired member-facing reply are rate-limited on SEPARATE windows -- a
# WARNING is an operational signal (cheap to want more often; capped at once
# per member per hour) while the reply is member-facing (capped at once per
# member per DAY, so a member who keeps messaging during a genuine server
# lag isn't told the same reassurance on every single message).
_LAG_WARNING_WINDOW_SECONDS = 3600.0
_LAG_REPLY_WINDOW_SECONDS = 86400.0

# Round-3-gate-2, Athena's addition: bound + pending + a null home_squad_id
# (mupot's own home-provisioning simply hasn't run yet for this member) gets
# a member-facing reply too, on the SAME "once per hour per member" cadence
# as the WARNING above -- never on every message, never inventing a home.
_HOME_NOT_READY_WINDOW_SECONDS = 3600.0

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
# Round-3-gate-3 / Athena's ruling (4), corrected round-3-gate-4 P1-a: once
# the retry backoff table itself is exhausted
# (_PROPOSAL_STALLED_AFTER_ATTEMPTS failed attempts), the member gets one of
# the two lines below instead of an unbounded repeat of the two ABOVE.
# _PROPOSAL_STALLED_REPLY is sent ONLY the moment a real task_create call
# (see :func:`_stall_reply`) actually succeeds -- the round-3-gate-3 build
# sent this exact sentence with no receiver anywhere, which is fabricating a
# receipt on a human-decision channel. If the flag call itself fails,
# _PROPOSAL_STALLED_REPLY_FLAG_FAILED is sent instead -- honest that nothing
# was actually flagged. Round-3-gate-4 P3-a: once stalled, the member NEVER
# reverts to _PROPOSAL_FAILED_REPLY/_PROPOSAL_RETRY_WAIT_REPLY again on a
# later check-in within the same stall -- _PROPOSAL_STILL_STALLED_REPLY is
# used for every subsequent rate-limited (see
# _PROPOSAL_STALLED_REPLY_WINDOW_SECONDS) check-in instead, so a member is
# never told "still working on it" as if nothing had been escalated.
_PROPOSAL_STALLED_REPLY = (
    "This is taking longer than it should -- I've flagged it so a human can "
    "look into your request directly."
)
_PROPOSAL_STALLED_REPLY_FLAG_FAILED = (
    "This is taking longer than it should, and I couldn't reach the team "
    "automatically -- please check in with them directly."
)
_PROPOSAL_STILL_STALLED_REPLY = (
    "Still waiting on a human to follow up on this -- I'll let you know the moment it moves."
)
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
# Round-3-gate-2, Athena's addition: bound + pending + no home yet (mupot's
# own home-provisioning hasn't run for this member) -- never invented,
# never a question asked without a real home id behind it.
_HOME_NOT_READY_REPLY = "Opening your space — one moment, I'll come back to you."
# Round-3-gate-2 P2 (:1455-1458): a fixed, small escape vocabulary pauses the
# intake instead of being captured as an answer to whatever question is
# current -- engrams/index are left exactly as they are; the very next
# non-escape message resumes at the same still-pending question.
_ESCAPE_WORDS = frozenset({"stop", "cancel", "later", "no thanks", "not now"})
_PAUSED_REPLY = "No problem -- message me whenever you're ready to continue."


def _is_escape_text(text: Any) -> bool:
    """Shared escape-word check (round-3-gate-4 P2-b): the proposal-retry-
    wait phase (index == len(FIRST_PERSON_QUESTIONS)) previously routed
    straight to :func:`_retry_proposal_if_due` with no escape check at all
    -- 'stop'/'cancel'/'not now' were only ever honoured mid-question, so
    an escape word became permanently dead the instant a member finished
    all 5 questions. Used by :func:`handle_first_contact` before routing to
    the retry-wait path, and mirrors the check :func:`_handle_answer` makes
    on the normalized text for an in-progress question."""
    if not isinstance(text, str):
        return False
    return _sanitize_answer(text).strip().lower().lstrip("/") in _ESCAPE_WORDS


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

    def put(self, user_id: str, resolution: StatusResolution, *, ttl_override: float | None = None) -> None:
        # Round-3-gate-3 P1: a caller that already knows this particular
        # resolution is a transient/capacity artifact (never fetched from a
        # real probe result) can force a short TTL instead of the resolution-
        # shaped default -- see resolve_member_status's pool_exhausted path.
        ttl = self._ttl_for(resolution) if ttl_override is None else ttl_override
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


class _SenderProbeLimiter:
    """Per-user_id minimum-interval limiter on REAL network probes.

    Round-3-gate-3 P1 (:1762): a pending member's local-progress bypass of
    the shared ``_StatusCache`` (``cache=None`` in ``handle_first_contact``'s
    ``resolve()``) intentionally forfeits the cache's freshness -- but the
    cache was also, incidentally, the only thing bounding how often the SAME
    sender could trigger a real network probe. This is the explicit,
    independent replacement: at most one real probe per ``user_id`` per
    ``min_interval``, enforced whether or not the shared cache is in play,
    so a single flooding sender cannot alone exhaust the global
    ``_ProbeLimiter`` pool and, downstream, cannot cause an innocent third
    party's own probe to fail fast. A denial here means "we chose not to ask
    again yet" -- resolve_member_status returns ``_UNKNOWN_STATUS`` without
    ever writing it to the shared cache (see resolve_member_status), so it
    never becomes a long-lived latch either.
    """

    def __init__(
        self,
        min_interval: float = _SENDER_PROBE_MIN_INTERVAL_SECONDS,
        *,
        max_entries: int = 2048,
        clock: Any = time.monotonic,
    ) -> None:
        self._min_interval = min_interval
        self._max_entries = max(1, max_entries)
        self._clock = clock
        self._last_at: "OrderedDict[str, float]" = OrderedDict()
        self._lock = threading.Lock()

    def should_probe(self, user_id: str) -> bool:
        with self._lock:
            now = self._clock()
            last = self._last_at.get(user_id)
            if last is not None:
                self._last_at.move_to_end(user_id)
                if (now - last) < self._min_interval:
                    return False
            self._last_at[user_id] = now
            self._last_at.move_to_end(user_id)
            while len(self._last_at) > self._max_entries:
                self._last_at.popitem(last=False)  # evict least-recently-used
            return True


class _NotifyOncePerWindow:
    """Bounded, per-key "have I already notified about this recently" tracker.

    Round-3-gate-2 P1 (:1408-1426): the held-proposal "awaiting the humans"
    reply and its paired WARNING must each fire at most once per their own
    window per member -- never on every single message, which round 2's
    unconditional per-message reply would have turned into either a
    permanent DM lockout (if paired with ``return True``) or, once fixed to
    ``return False``, spam on every message the member sends from then on.
    LRU-bounded (``max_entries``) for the same reason ``_StatusCache`` is:
    an unbounded number of distinct members must not grow this dict without
    limit. Wall-clock based (not monotonic) since these windows are measured
    in hours, and the whole point is "not more than once per real-world
    hour/day" -- a process restart legitimately resets the count, same as
    any other in-memory-only state in this module.
    """

    def __init__(self, window_seconds: float, *, max_entries: int = 2048, clock: Any = time.time) -> None:
        self._window = window_seconds
        self._max_entries = max(1, max_entries)
        self._clock = clock
        self._last_at: "OrderedDict[str, float]" = OrderedDict()
        self._lock = threading.Lock()

    def should_notify(self, key: str) -> bool:
        with self._lock:
            now = self._clock()
            last = self._last_at.get(key)
            if last is not None:
                self._last_at.move_to_end(key)
                if (now - last) < self._window:
                    return False
            self._last_at[key] = now
            self._last_at.move_to_end(key)
            while len(self._last_at) > self._max_entries:
                self._last_at.popitem(last=False)
            return True


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
    sender_limiter: "_SenderProbeLimiter | None" = None,
    fallback: StatusResolution | None = None,
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

    if sender_limiter is not None and not sender_limiter.should_probe(cache_key):
        # Round-3-gate-3 P1: independent per-sender rate limit on REAL probes
        # (see _SenderProbeLimiter) -- applies whether or not `cache` is set,
        # so a pending member's cache bypass (see handle_first_contact's
        # `resolve()`) no longer also means "no rate limit at all". A denial
        # here means "chose not to ask again yet", not "mupot says unknown";
        # never cached (a rate-limit denial is not information ABOUT the
        # sender, and caching it would recreate exactly the latch this is
        # meant to avoid).
        #
        # Round-3-gate-4 P0 (:803): the round-3-gate-3 version returned
        # _UNKNOWN_STATUS unconditionally here -- which handle_first_contact
        # treats as "hold, do nothing" for a mid-intake member, so two
        # ordinary answers typed within the limiter's own window (15s) meant
        # the SECOND one was silently never sanitized, credential-checked,
        # stored, or replied to, and fell through to the host's own LLM
        # turn. A denial is not new information; it must never reuse the
        # sentinel that means "the probe ran and found nothing". Callers
        # that already know a reasonable "nothing has changed" answer (see
        # handle_first_contact's `resolve()`, which passes the current
        # pending record's own last-known state) get THAT back instead.
        return fallback if fallback is not None else _UNKNOWN_STATUS

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
            # Round-3-gate-2 P2 (:1391-1399, :186): a CONFIRMED "you are not
            # a member" response is a DEFINITIVE result, not a transport
            # failure -- align with mupot's own memberIntakeEnvelope
            # semantics ("'none' -- chatId maps to no member at all") rather
            # than the generic "unknown" sentinel, which handle_first_contact
            # now treats specially (hold a pending record rather than
            # abandoning it) precisely BECAUSE it means "the response itself
            # was malformed/absent", never "mupot said no."
            return StatusResolution(bound=False, member_id=None, home_squad_id=None, intake_state="none")
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
        if resolution.intake_state == "unknown":
            # Round-3-gate-3 P1 / Athena's ruling (2): a probe-produced
            # "unknown" -- whatever the cause (timeout, malformed response,
            # secret-read failure, or the global _ProbeLimiter pool being
            # exhausted) -- is never cached with the resolution-shaped TTL
            # (which, for "unknown", is the LONGEST of the three -- 900s).
            # Only a server-ANSWERED state (confirmed bound/pending/complete,
            # or a confirmed "none") earns the normal TTL below; an
            # unanswered probe gets this much shorter window instead, so one
            # bad probe (or one flood-causing sender emptying the pool)
            # cannot latch every OTHER sender's status for 15 minutes.
            cache.put(cache_key, resolution, ttl_override=_STATUS_UNKNOWN_SHORT_TTL_SECONDS)
        else:
            cache.put(cache_key, resolution)
    return resolution


def _with_secret_owner(secret_owner: ProfileSecretOwner, fn: Any) -> Any:
    with secret_owner.activate():
        return fn()


# --------------------------------------------------------------------------
# Durable completion marker -- NEVER raw answer text, see module docstring (b).
# --------------------------------------------------------------------------


def _read_bounded_jsonl(path: Path) -> list[dict[str, Any]]:
    """Bounded, tolerant JSONL reader shared by the completion journal and
    the stall-marker file (round-3-gate-4 P1-a/P3-c). Never reads more than
    ``_JOURNAL_READ_CAP_BYTES`` regardless of on-disk file size, and only
    ever the TAIL of an oversized file; tolerant of a partially-written
    last line (an fsync'd append can still be torn by a concurrent crash on
    some filesystems) -- one bad line is skipped, not fatal to the read."""
    try:
        size = path.stat().st_size
    except OSError:
        return []
    try:
        with open(path, "rb") as handle:
            truncated_head = size > _JOURNAL_READ_CAP_BYTES
            if truncated_head:
                handle.seek(size - _JOURNAL_READ_CAP_BYTES)
            raw = handle.read(_JOURNAL_READ_CAP_BYTES + 1)
    except OSError:
        return []
    text = raw.decode("utf-8", errors="ignore")
    lines = text.splitlines()
    if truncated_head and lines:
        # The first line read after an interior seek is very likely a
        # partial line -- drop it rather than risk parsing a truncated
        # JSON object as a real (and possibly wrong) record.
        lines = lines[1:]
    parsed: list[dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if isinstance(entry, dict):
            parsed.append(entry)
    return parsed


def _write_bounded_jsonl(path: Path, entries: dict[str, dict[str, Any]], *, sort_key: str) -> None:
    """Compact-on-write atomic JSONL writer shared by the completion
    journal and the stall-marker file -- caps at ``_JOURNAL_MAX_ENTRIES``
    distinct ``member_id`` keys, oldest (by ``sort_key``) evicted first.
    Written atomically (temp file + fsync + rename), same shape as the main
    store's own ``save``."""
    if len(entries) > _JOURNAL_MAX_ENTRIES:
        oldest_first = sorted(entries.values(), key=lambda entry: entry.get(sort_key, 0.0))
        for stale in oldest_first[: len(entries) - _JOURNAL_MAX_ENTRIES]:
            entries.pop(stale["member_id"], None)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            for entry in entries.values():
                handle.write(json.dumps(entry, ensure_ascii=False, sort_keys=True))
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


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
        # Round-3-gate-2 P1 (:931): append-only, SENDER-SCOPED fallback --
        # written ONLY when the main store is unreadable at completion time.
        # Overwriting the main store's JSON built from a truncated/invalid
        # read would silently erase every OTHER member's completed record
        # (they would then read back as never-completed and double-propose).
        # Appending one line never touches, reads, or risks anyone else's
        # data -- see FirstPersonRuntime.finish/held_proposal_id.
        self.journal_path = self.path.with_name(self.path.name + ".journal")
        # Round-3-gate-4 P1-a: durable breadcrumb that a proposal has
        # stalled long enough to need human follow-up -- part of what makes
        # _PROPOSAL_STALLED_REPLY's "I've flagged it" claim true (see
        # first_person._stall_reply). Same file, bounded-compaction shape.
        self.stall_path = self.path.with_name(self.path.name + ".stalled")

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

    def _read_journal_entries(self) -> dict[str, dict[str, Any]]:
        """Bounded read (see :func:`_read_bounded_jsonl`), keeping at most
        one entry per ``member_id`` -- the LAST one seen scanning forward,
        matching the previous append-only "most recent wins" semantics."""
        entries: dict[str, dict[str, Any]] = {}
        for entry in _read_bounded_jsonl(self.journal_path):
            member_id = entry.get("member_id")
            proposal_id = entry.get("proposal_id")
            if not isinstance(member_id, str) or not member_id.strip():
                continue
            if not isinstance(proposal_id, str) or not proposal_id.strip():
                continue
            entries[member_id] = entry
        return entries

    def append_journal(self, member_id: str, proposal_id: str, completed_at: float) -> None:
        """Best-effort, compacted-on-write. Never raises -- the caller has
        already logged a WARNING; a journal write failure on top of a
        corrupted main store is not something retrying harder here would
        fix.

        Round-3-gate-3 P3 (:836): the previous shape was truly append-only
        with no size bound at all -- 200 completions during one corrupted-
        store window produced 201 growing lines forever. This reads the
        existing (bounded) entries, keeps at most one per member_id, adds/
        replaces this member's own entry, and if that leaves more than
        ``_JOURNAL_MAX_ENTRIES`` distinct members, drops the OLDEST (by
        ``completed_at``) first (see :func:`_write_bounded_jsonl`)."""
        try:
            entries = self._read_journal_entries()
            entries[member_id] = {
                "member_id": member_id,
                "proposal_id": proposal_id,
                "completed_at": completed_at,
            }
            _write_bounded_jsonl(self.journal_path, entries, sort_key="completed_at")
        except OSError:
            pass

    def read_journal_proposal_id(self, member_id: str) -> str | None:
        """Most recent proposal_id recorded for member_id, if any."""
        entry = self._read_journal_entries().get(member_id)
        if entry is None:
            return None
        candidate = entry.get("proposal_id")
        return candidate.strip() if isinstance(candidate, str) and candidate.strip() else None

    def record_stall(self, member_id: str, retry_count: int, stalled_at: float) -> None:
        """Round-3-gate-4 P1-a: durable, compacted-on-write breadcrumb that
        a proposal has stalled long enough to need human follow-up. This is
        NOT the primary flag mechanism (see :func:`_stall_reply`'s
        ``task_create`` call) -- it is the audit trail that survives even
        if the task_create call itself fails, and the thing that makes
        ``_PROPOSAL_STALLED_REPLY_FLAG_FAILED``'s honesty possible (an
        operator can still find every stall here). Never raises."""
        try:
            entries: dict[str, dict[str, Any]] = {}
            for entry in _read_bounded_jsonl(self.stall_path):
                candidate_id = entry.get("member_id")
                if isinstance(candidate_id, str) and candidate_id.strip():
                    entries[candidate_id] = entry
            entries[member_id] = {"member_id": member_id, "retry_count": retry_count, "stalled_at": stalled_at}
            _write_bounded_jsonl(self.stall_path, entries, sort_key="stalled_at")
        except OSError:
            pass

    def stall_entries(self) -> dict[str, dict[str, Any]]:
        """Read-only view of the durable stall breadcrumbs, for tests/audit."""
        entries: dict[str, dict[str, Any]] = {}
        for entry in _read_bounded_jsonl(self.stall_path):
            member_id = entry.get("member_id")
            if isinstance(member_id, str) and member_id.strip():
                entries[member_id] = entry
        return entries


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
    # Round-3-gate-2 P0: separate from created_at on purpose -- refreshed on
    # every accepted answer (see FirstPersonRuntime.record_answer/touch) so
    # the IDLE timeout measures silence, not total conversation length.
    last_activity_at: float = field(default_factory=time.monotonic)
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
        # Round-3-gate-4 P1-b: a single non-pending (`'none'`/`'complete'`)
        # reading is never enough to durably prune a member's in-progress
        # record -- see note_non_pending/note_pending below.
        self._terminal_sightings: "OrderedDict[str, float]" = OrderedDict()

    def wall_now(self) -> float:
        return self._wall_clock()

    def note_non_pending(self, member_id: str) -> bool:
        """Round-3-gate-4 P1-b: a transient status flap (a completion-
        detection race, or a server-side bug) reporting 'complete' or
        'none' for one message and 'pending' again the next must NEVER
        destroy durable in-progress engrams on that single reading --
        exactly the round-3-gate-3 P0 trigger (c) landmine, just one level
        earlier: pruning immediately on the FIRST non-pending reading is
        itself un-confirmed. Returns True (CONFIRMED) only on the SECOND
        consecutive non-pending reading for this member since the last
        pending one; the first reading is recorded and returns False
        (caller must leave the local AND durable record untouched -- a
        flap back to 'pending' before confirmation must resume with every
        engram intact, never re-ask an already-answered question)."""
        with self._lock:
            if member_id in self._terminal_sightings:
                del self._terminal_sightings[member_id]
                return True
            self._terminal_sightings[member_id] = self._clock()
            while len(self._terminal_sightings) > 2048:
                self._terminal_sightings.popitem(last=False)
            return False

    def note_pending(self, member_id: str) -> None:
        """A genuine, member-matched 'pending' reading resets any partial
        (unconfirmed) non-pending sighting recorded above -- a flap-back
        is not evidence toward a future confirmation."""
        with self._lock:
            self._terminal_sightings.pop(member_id, None)

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
        buggy, never a reason to re-onboard. Checks the main store first,
        then ALWAYS also the journal (round-3-gate-2 P1 :931: a completion
        recorded to the journal because the main store was corrupted at
        ``finish()`` time must keep blocking re-intake even later, once the
        main store looks fine again). Returns ``None`` only when NEITHER
        source has an entry -- :meth:`is_complete_or_unknown`'s TTL-bounded
        check is what fails closed on an otherwise-corrupted store; this
        method only ever adds an EXTRA reason to refuse, never a reason to
        proceed that the other check wouldn't already allow.
        """
        data, valid = self.store.load_checked()
        if valid:
            completed = data.get("completed")
            if isinstance(completed, dict) and member_id in completed:
                entry = completed[member_id]
                if isinstance(entry, dict):
                    proposal_id = entry.get("proposal_id")
                    if isinstance(proposal_id, str) and proposal_id.strip():
                        return proposal_id.strip()
        # Round-3-gate-2 P1 (:931): ALWAYS also check the journal, whether
        # the main store was valid-but-missing-this-member or outright
        # corrupted -- a completion recorded there during a prior corrupted
        # read (see finish()) must keep blocking re-intake even after the
        # main store is fixed/replaced.
        return self.store.read_journal_proposal_id(member_id)

    def _load_in_progress_state(self, member_id: str) -> tuple[dict[str, str], str | None, int]:
        """Durable, best-effort read of everything :meth:`start` needs to
        resume -- the {question_id: engram_id} map AND ``project_id`` --
        round-3-gate-2 P0/Athena's RESUME REBINDS: lets :meth:`start` resume
        at the first UNANSWERED question instead of re-asking (and
        re-labelling) already-answered ones after an idle/absolute-TTL drop.

        Round-3-gate-3 P0 (:1856): round-2's version returned ONLY the
        engram map -- ``project_id`` was never persisted at all, so a member
        who had already answered the "project" question but not yet
        completed (the live schema rejects the ``project_access`` proposal
        kind -- see :func:`_submit_proposal` -- so this is the DEFAULT
        outcome today) resumed with ``index`` reflecting all 5 engrams but
        ``project_id`` silently reset to ``None``: a state the forward path
        can never produce, and the exact partial restore that made the
        member's proposal retry fail forever. Both fields are now written
        together (see :meth:`_save_in_progress`) so a resume either has
        BOTH the answer for a question and (once past it) the project_id
        that answer resolved to, or neither.

        Round-3-gate-4 P3-b: ``proposal_retry_count`` is now part of the
        same durable record too -- without it, an idle-drop-then-resume (or
        a process restart) during the retry-wait phase silently reset the
        backoff counter to 0, letting a member's repeated idle-triggered
        resumes hammer a known-failing submission far more often than the
        backoff table intends. ``next_proposal_retry_at`` itself is NOT
        persisted -- it is a ``time.monotonic()`` value, meaningless across
        a process restart (a fresh process has its own monotonic epoch);
        :meth:`start` recomputes a fresh backoff window from the restored
        ``proposal_retry_count`` instead of trusting a stale timestamp.

        Never raises; a corrupted store just degrades this specific
        member's resume to "start over" (the durable proposal-dedupe
        guarantee in held_proposal_id/is_complete_or_unknown is unaffected
        -- this method is purely a UX aid, not a security-relevant gate)."""
        data, valid = self.store.load_checked()
        if not valid:
            return {}, None, 0
        in_progress = data.get("in_progress")
        if not isinstance(in_progress, dict) or member_id not in in_progress:
            return {}, None, 0
        entry = in_progress[member_id]
        if not isinstance(entry, dict):
            return {}, None, 0
        raw_engrams = entry.get("engram_ids")
        engrams = (
            {
                question_id: engram_id.strip()
                for question_id, engram_id in raw_engrams.items()
                if isinstance(question_id, str) and isinstance(engram_id, str) and engram_id.strip()
            }
            if isinstance(raw_engrams, dict)
            else {}
        )
        raw_project_id = entry.get("project_id")
        project_id = raw_project_id.strip() if isinstance(raw_project_id, str) and raw_project_id.strip() else None
        raw_retry_count = entry.get("proposal_retry_count")
        retry_count = (
            raw_retry_count if isinstance(raw_retry_count, int) and not isinstance(raw_retry_count, bool) and raw_retry_count >= 0 else 0
        )
        return engrams, project_id, retry_count

    def _save_in_progress(
        self, member_id: str, engrams: dict[str, str], project_id: str | None, proposal_retry_count: int
    ) -> None:
        """Sender-scoped, validity-gated (round-3-gate-2 P1 :931 discipline
        applied here too): read, validate, merge ONLY this member's
        in-progress record, write. On an invalid read this is a best-effort
        UX aid (see :meth:`_load_in_progress_state`), so it simply skips
        the write rather than journaling -- unlike the completion marker,
        losing one round of in-progress resume data is not a duplicate-
        proposal risk, just a degraded-to-"start over" resume next time.

        Round-3-gate-3 P0: ``project_id`` is now written alongside
        ``engrams`` on every call, not only the engram map -- see
        :meth:`_load_in_progress_state`'s docstring for why a partial write
        of just one of the two is itself the defect class. Round-3-gate-4
        P3-b: ``proposal_retry_count`` is written the same way."""
        data, valid = self.store.load_checked()
        if not valid:
            return
        in_progress = data.get("in_progress")
        if not isinstance(in_progress, dict):
            in_progress = {}
        in_progress[member_id] = {
            "engram_ids": dict(engrams),
            "project_id": project_id,
            "proposal_retry_count": proposal_retry_count,
            "updated_at": self._wall_clock(),
        }
        data["in_progress"] = in_progress
        self.store.save(data)

    def mark_quarantined(self, member_id: str, question_id: str) -> bool:
        """Post-hoc scrub (round-3-gate-2, Athena's addition): flags
        ``question_id``'s engram as quarantined in the durable completed
        record -- NEVER deletes the engram_id itself (a human/operator may
        still need it; see :func:`scrub_quarantine_candidates`). Sender-
        scoped and validity-gated exactly like :meth:`finish`: refuses (logs
        a WARNING) rather than rewriting the store from an invalid read.
        Returns True iff the flag was actually written."""
        data, valid = self.store.load_checked()
        if not valid:
            logger.warning(
                "mupot plugin: cannot mark member_id=%s question_id=%s quarantined -- "
                "completion store is unreadable",
                member_id,
                question_id,
            )
            return False
        completed = data.get("completed")
        if not isinstance(completed, dict) or member_id not in completed or not isinstance(completed[member_id], dict):
            return False
        quarantined = completed[member_id].get("quarantined")
        quarantined_set = set(quarantined) if isinstance(quarantined, list) else set()
        quarantined_set.add(question_id)
        completed[member_id]["quarantined"] = sorted(quarantined_set)
        data["completed"] = completed
        self.store.save(data)
        return True

    def is_quarantined(self, member_id: str, question_id: str) -> bool:
        data, valid = self.store.load_checked()
        if not valid:
            return False
        completed = data.get("completed")
        if not isinstance(completed, dict) or member_id not in completed:
            return False
        entry = completed[member_id]
        quarantined = entry.get("quarantined") if isinstance(entry, dict) else None
        return isinstance(quarantined, list) and question_id in quarantined

    def get_pending(self, chat_key: str) -> Intake | None:
        """Round-3-gate-2 P0: TWO independent expiries, either one drops the
        record -- an IDLE timeout measured from ``last_activity_at`` (a
        thoughtfully-paced conversation never trips this no matter how long
        the whole thing takes) and a hard ABSOLUTE cap measured from
        ``created_at`` (a conversation that never quite goes idle cannot
        hold a local record open forever)."""
        with self._lock:
            intake = self._pending.get(chat_key)
            if intake is None:
                return None
            now = self._clock()
            if now - intake.last_activity_at > _PENDING_IDLE_TTL_SECONDS:
                del self._pending[chat_key]
                return None
            if now - intake.created_at > _PENDING_ABSOLUTE_TTL_SECONDS:
                del self._pending[chat_key]
                return None
            return intake

    def touch(self, chat_key: str) -> Intake | None:
        """Refresh last_activity_at without changing anything else -- used
        for interactions with an existing pending record that are genuine
        engagement (an escape/pause message, a retry-wait check-in) but not
        themselves an "accepted answer" (see :meth:`record_answer`, which
        also refreshes activity as part of its own replace())."""
        with self._lock:
            intake = self._pending.get(chat_key)
            if intake is None:
                return None
            updated = replace(intake, last_activity_at=self._clock())
            self._pending[chat_key] = updated
            return updated

    def start(self, chat_key: str, *, member_id: str, home_squad_id: str) -> Intake:
        """Round-3-gate-2 P0 / Athena's RESUME REBINDS: if this member has a
        durable in-progress record (from a PRIOR local Intake that idle/
        absolute-TTL dropped, or a process restart), resume at the first
        UNANSWERED question -- never re-ask, never re-label, never re-write
        an engram for a question that already has one. The resumed
        ``engrams``/``index`` are trimmed to the longest CONTIGUOUS
        already-answered prefix of :data:`FIRST_PERSON_QUESTIONS` (strictly
        sequential asking means a gap should never occur, but this is
        defensive rather than trusting the persisted shape blindly).

        Round-3-gate-3 P0 (:1856): the returned ``index`` can legitimately
        equal ``len(FIRST_PERSON_QUESTIONS)`` -- ALL questions already
        answered, proposal submission never yet succeeded (the live schema
        rejects the ``project_access`` kind today, so this is the common
        case, not an edge case). Callers must route that case to the
        submit-or-retry path, never index ``FIRST_PERSON_QUESTIONS`` with
        it directly (see :func:`handle_first_contact`'s bounds check right
        after calling this). This method's own commitment
        (``self._pending[chat_key] = intake``) happens ONLY after every
        field -- including ``project_id`` -- has been computed and
        validated below; nothing after that point can raise, so a caller
        can never observe a record committed here that is missing a field
        the caller might then use unguarded.

        A resumed ``index`` past the "project" question is only trusted
        when the durable record ALSO carries a ``project_id`` -- both are
        written together by :meth:`_save_in_progress`/:meth:`record_answer`,
        so a mismatch here means a prior write was itself partial (e.g. an
        older build, or a torn write). Rather than manufacture "all
        questions answered, no project" -- a state the forward path can
        never produce -- this clamps back to the project question so the
        very next answer re-resolves it.

        Round-3-gate-4 P2-a (correcting a round-3-gate-3 bug): the clamp
        must NOT drop the "project" question's own already-recorded
        engram_id from the resumed ``engrams``. ``squad_remember`` is an
        INSERT, never an upsert -- the round-3-gate-3 comment claiming the
        old engram is "simply overwritten" on re-answer was FALSE: nothing
        anywhere deletes the prior engram, so re-recording on the re-answer
        would leave a permanent DUPLICATE in squad memory while this
        record's own view of it (the engram_id kept here) just moves on.
        The engram_id is kept in ``engrams`` (even though ``index`` itself
        reverts to point AT the project question, not past it) precisely so
        :func:`_handle_answer` can detect "this question already has a
        recorded engram" and reuse it -- re-deriving only ``project_id``
        from the fresh answer, never calling ``squad_remember`` again for a
        question already on disk."""
        prior_engrams, prior_project_id, prior_retry_count = self._load_in_progress_state(member_id)
        index = 0
        for question_id, _ in FIRST_PERSON_QUESTIONS:
            if question_id in prior_engrams:
                index += 1
            else:
                break

        project_question_index = next(
            (position for position, (question_id, _) in enumerate(FIRST_PERSON_QUESTIONS) if question_id == "project"),
            None,
        )
        project_id = prior_project_id
        engram_prefix_index = index
        if project_question_index is not None and index > project_question_index and project_id is None:
            index = project_question_index
            project_id = None
            # Keep engrams through the project question INCLUSIVE -- see
            # the docstring above. Everything strictly after "project" is
            # still dropped: those questions were only reachable in the
            # forward path once project_id existed, so an engram recorded
            # for one of them without a project_id ever having existed
            # would itself be a shape the forward path cannot produce.
            engram_prefix_index = project_question_index + 1

        engrams = {
            question_id: prior_engrams[question_id] for question_id, _ in FIRST_PERSON_QUESTIONS[:engram_prefix_index]
        }
        now = self._clock()
        # Round-3-gate-4 P3-b: restore proposal_retry_count directly (a
        # plain counter, safe across a restart) but recompute
        # next_proposal_retry_at from it via a FRESH backoff window from
        # `now` -- the durable record deliberately never carries the raw
        # monotonic timestamp, which would be meaningless once resumed in a
        # different process (a fresh process has its own monotonic epoch).
        # Only meaningful once every question is answered; harmless
        # (unused) otherwise.
        retry_count = prior_retry_count if index >= len(FIRST_PERSON_QUESTIONS) else 0
        next_retry_at = now + _next_backoff(retry_count) if retry_count > 0 else 0.0
        intake = Intake(
            member_id=member_id,
            home_squad_id=home_squad_id,
            index=index,
            project_id=project_id,
            engrams=engrams,
            created_at=now,
            last_activity_at=now,
            proposal_retry_count=retry_count,
            next_proposal_retry_at=next_retry_at,
        )
        with self._lock:
            self._pending[chat_key] = intake
        return intake

    def record_answer(self, chat_key: str, question_id: str, engram_id: str) -> Intake | None:
        """Advance progress by building a NEW frozen ``Intake`` (P3-G) --
        ``member_id``/``home_squad_id`` are always copied over unchanged from
        the current record, never passed as a kwarg here, so they cannot
        drift regardless of ``question_id``/``engram_id``. Returns the
        updated ``Intake``, or ``None`` if the pending record vanished
        (TTL/abandon race) -- the caller must treat that as "nothing left to
        advance", never retry the mutation. Refreshes ``last_activity_at``
        (round-3-gate-2 P0: an accepted answer IS activity) and durably
        persists the updated engram map (Athena's RESUME REBINDS) OUTSIDE
        the lock, same "lock only the in-memory dict, do disk I/O after"
        shape :meth:`finish` already uses."""
        with self._lock:
            intake = self._pending.get(chat_key)
            if intake is None:
                return None
            updated = replace(
                intake,
                engrams={**intake.engrams, question_id: engram_id},
                index=intake.index + 1,
                last_activity_at=self._clock(),
            )
            self._pending[chat_key] = updated
        self._save_in_progress(updated.member_id, updated.engrams, updated.project_id, updated.proposal_retry_count)
        return updated

    def set_project_id(self, chat_key: str, project_id: str) -> Intake | None:
        """Same replace-in-place shape as :meth:`record_answer`, for the one
        other progress field mutated outside ``finish``/``abandon``.

        Round-3-gate-3 P0: also persisted durably (same "lock only the
        in-memory dict, do disk I/O after" shape as :meth:`record_answer`)
        so a crash between resolving the project and recording the
        "project" question's own engram still leaves project_id resumable
        -- see :meth:`start`'s docstring for why a partial durable record
        (engrams without project_id, or vice versa) is the defect class
        this closes."""
        with self._lock:
            intake = self._pending.get(chat_key)
            if intake is None:
                return None
            updated = replace(intake, project_id=project_id)
            self._pending[chat_key] = updated
        self._save_in_progress(updated.member_id, updated.engrams, updated.project_id, updated.proposal_retry_count)
        return updated

    def schedule_proposal_retry(self, chat_key: str, *, next_retry_at: float, retry_count: int) -> Intake | None:
        """Same replace-in-place shape as :meth:`record_answer`, for the
        proposal-retry backoff pair (round-2 P0-3).

        Round-3-gate-4 P3-b: ``proposal_retry_count`` is now ALSO durably
        persisted (``next_proposal_retry_at`` deliberately is not -- it is
        a ``time.monotonic()`` value with no meaning across a process
        restart; see :meth:`start`'s recompute logic)."""
        with self._lock:
            intake = self._pending.get(chat_key)
            if intake is None:
                return None
            updated = replace(intake, next_proposal_retry_at=next_retry_at, proposal_retry_count=retry_count)
            self._pending[chat_key] = updated
        self._save_in_progress(updated.member_id, updated.engrams, updated.project_id, updated.proposal_retry_count)
        return updated

    def finish(self, chat_key: str, *, proposal_id: str | None) -> None:
        """Round-3-gate-2 P1 (:931): sender-scoped and validity-gated. On an
        INVALID read, this method must never rewrite the whole store from
        that truncated ``{}`` -- doing so (the round-3-gate-1 bug) silently
        erases every OTHER member's completed record, since ``load_checked``
        cannot return partial data on a parse failure (it always returns an
        empty dict alongside ``valid=False``). Instead: append this ONE
        member's own record to the journal (never touches, reads, or risks
        anyone else's data) and log a WARNING. The normal merge-and-save
        path only ever runs when the read was actually valid."""
        with self._lock:
            intake = self._pending.pop(chat_key, None)
        if intake is None:
            return
        data, valid = self.store.load_checked()
        if not valid:
            if proposal_id is not None:
                self.store.append_journal(intake.member_id, proposal_id, self._wall_clock())
            logger.warning(
                "mupot plugin: first-person completion store is unreadable -- "
                "recorded member_id=%s to the journal instead of rewriting the main store",
                intake.member_id,
            )
            return
        completed = data.get("completed")
        if not isinstance(completed, dict):
            completed = {}
        completed[intake.member_id] = {
            "engram_ids": dict(intake.engrams),
            "proposal_id": proposal_id,
            "completed_at": self._wall_clock(),
        }
        data["completed"] = completed
        in_progress = data.get("in_progress")
        if isinstance(in_progress, dict) and intake.member_id in in_progress:
            del in_progress[intake.member_id]
            data["in_progress"] = in_progress
        self.store.save(data)

    def abandon(self, chat_key: str) -> None:
        """Drop the in-memory record AND prune the durable in-progress resume
        record for that member.

        Round-3-gate-3 P3 (:1251): round-2-gate-2's version only popped the
        in-memory dict, leaving the durable {question_id: engram_id} record
        (and now project_id) on disk untouched. Callers invoke ``abandon()``
        specifically when local progress is no longer trustworthy for THIS
        chat -- a definitive non-pending status, or a member mismatch (see
        :func:`handle_first_contact`) -- and leaving the durable record
        behind was exactly the round-3-gate-2 P0's trigger (c): a status
        flap from 'pending' to 'complete' abandons the in-memory record
        here, and if the durable in_progress record survives, a LATER flap
        back to 'pending' resumes :meth:`start` from a stale record that
        this NEW pending episode never actually produced. TTL-based expiry
        (:meth:`get_pending`) deliberately does NOT route through here --
        that path (a member who is still genuinely mid-conversation, just
        idle) is the one case resume MUST keep working for, so it prunes
        nothing."""
        with self._lock:
            intake = self._pending.pop(chat_key, None)
        if intake is not None:
            self._clear_in_progress(intake.member_id)

    def _clear_in_progress(self, member_id: str) -> None:
        """Sender-scoped, validity-gated (same discipline as
        :meth:`_save_in_progress`/:meth:`finish`): on an invalid read, skip
        the write entirely rather than rewriting the whole store from a
        truncated read, which would silently erase every OTHER member's
        in-progress record."""
        data, valid = self.store.load_checked()
        if not valid:
            return
        in_progress = data.get("in_progress")
        if isinstance(in_progress, dict) and member_id in in_progress:
            del in_progress[member_id]
            data["in_progress"] = in_progress
            self.store.save(data)


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


# Round-3-gate-2, Athena's addition: strip by INVARIANT (Unicode general
# category), not a hand-maintained, ever-growing list of individual code
# points. Cc (control) and Cf (format) TOGETHER already cover every
# invisible/format character that can split a token past a contiguous regex
# match: bidi overrides/embeds/isolates (U+202A-U+202E, U+2066-U+2069,
# U+200E/U+200F), zero-width joiner/non-joiner/space (U+200D/U+200C/U+200B),
# soft hyphen (U+00AD), word joiner (U+2060), BOM/ZWNBSP (U+FEFF), and the
# Mongolian vowel separator (U+180E) -- every one of those IS category Cf,
# so this single class constant is a strict superset of round-2's
# hand-picked bidi-only list (removed; category coverage subsumes it). ONE
# constant this predicate is defined against, not a list that grows every
# time someone finds one more invisible character.
_STRIPPED_UNICODE_CATEGORIES = frozenset({"Cc", "Cf"})


def _sanitize_answer(text: str) -> str:
    """Normalize FIRST: this is the front half of the round-3 P0-A pipeline
    invariant ("the store never receives an un-normalized byte sequence") --
    NFKC first (collapses compatibility/full-width lookalikes), then strip
    every character in :data:`_STRIPPED_UNICODE_CATEGORIES`. Credential-
    refusal in :func:`_handle_answer` runs on THIS function's output, never
    on the raw text, so an obfuscated token (a real credential with an
    invisible character spliced into the middle) is caught: normalize
    reassembles it into its plain form BEFORE the credential check ever
    sees it, instead of the round-2 bug's order (check raw text, then
    normalize), which let the invisible character break the regex's
    contiguous match and only spliced the valid token back together
    afterward."""
    normalized = unicodedata.normalize("NFKC", text)
    cleaned_chars = []
    for ch in normalized:
        if ch in ("\n", "\t"):
            cleaned_chars.append(ch)
            continue
        if unicodedata.category(ch) in _STRIPPED_UNICODE_CATEGORIES:
            continue
        cleaned_chars.append(ch)
    return "".join(cleaned_chars).strip()[:_MAX_ANSWER_CHARS]


# --------------------------------------------------------------------------
# Escape hatch + plain-text verdict pass-through (round-3-gate-2 P2 :1455-1458)
# --------------------------------------------------------------------------

# Mirrors mupot's OWN server-side plain-text command shape (src/im/index.ts's
# parseIntent verdictMatch) exactly, on purpose: this module must recognize
# the SAME messages the host's own approve/reject decision path (#1425) will
# act on, so it can fall through untouched rather than capturing them as an
# intake answer.
_VERDICT_COMMAND_PATTERN = re.compile(r"^/?(approve|reject)\s+([A-Za-z0-9_-]{6,64})(?:\s+(.+))?$", re.IGNORECASE)


def is_verdict_shaped(text: str) -> bool:
    """Does `text` match the approve/reject command shape? Used to fall
    through untouched during live answer-capture (never store it -- see
    :func:`_handle_answer`) AND, offline, as the detection half of the
    post-hoc scrub (:func:`scrub_quarantine_candidates`) for anything that
    might have been captured under an EARLIER, buggy build before this
    check existed."""
    return bool(_VERDICT_COMMAND_PATTERN.match(text.strip()))


def scrub_quarantine_candidates(recalled_answers: Mapping[str, str]) -> dict[str, str]:
    """Post-hoc scrub (round-3-gate-2, Athena's addition): given
    ``{question_id: previously-recalled answer text}`` -- this module never
    holds raw answer text itself (write-through only, see the module
    docstring), so an audit of what is ALREADY stored can only run against
    text an operator has separately recalled (e.g. via ``squad_recall``
    against the member's home squad) and handed in here -- returns the
    subset whose text matches the approve/reject command shape. A caller
    should quarantine each of these via
    :meth:`FirstPersonRuntime.mark_quarantined` (flag, never delete) and
    warn the member once."""
    return {question_id: text for question_id, text in recalled_answers.items() if is_verdict_shaped(text)}


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
    clock: Any = time.monotonic,
    stalled_notifier: "_NotifyOncePerWindow | None" = None,
) -> bool:
    """Returns True iff this message was actually consumed by first-person
    (the caller's ``handle_first_contact`` should report ``True``/stop
    propagation) and False iff it was a plain-text approve/reject command
    that must fall through to the host's own decision path completely
    untouched (round-3-gate-2 P2 :1455-1458) -- never captured, never
    stored, never replied to from here."""
    raw_answer = getattr(message, "text", None)
    if not isinstance(raw_answer, str) or not raw_answer.strip():
        # Round-3 ruling (6): not a usable answer at all -- nudge, don't
        # capture, don't fall through.
        await message.reply_text(_UNRELATED_ANSWER_REPLY)
        return True

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

    # Round-3-gate-2 P2 (:1455-1458): checked on the NORMALIZED text, same
    # pipeline-order discipline as the credential check below -- an escape
    # word PAUSES (engrams/index untouched; the very next non-escape message
    # resumes the SAME still-current question), and a plain-text
    # approve/reject command is NEVER captured as an answer at all.
    if cleaned_answer.strip().lower().lstrip("/") in _ESCAPE_WORDS:
        # Round-3-gate-3 P3 (:1119): an escape/pause message is genuine
        # engagement, not silence -- refresh last_activity_at so a member
        # who pauses and checks back in repeatedly is never idle-TTL'd out
        # mid-pause (touch() previously had zero call sites anywhere).
        runtime.touch(chat_key)
        await message.reply_text(_PAUSED_REPLY)
        return True
    if is_verdict_shaped(cleaned_answer):
        return False

    if _looks_like_credential(cleaned_answer):
        # Refused before the normalized text ever reaches storage/logging --
        # no trace of it (raw or cleaned) survives this branch.
        await message.reply_text(f"{_CREDENTIAL_REPLY} {question_text}")
        return True
    if not cleaned_answer:
        await message.reply_text(_WRITE_FAILURE_REPLY)
        return True

    if question_id == "project" and intake.project_id is None:
        def resolve_project() -> str | None:
            return _resolve_member_project(settings, envelope, cleaned_answer, secret_owner=secret_owner)

        project_id = await asyncio.to_thread(resolve_project)
        # cleaned_answer used only to compose this reply; this call frame ends
        # right after -- nothing here writes it anywhere.
        if project_id is None:
            await message.reply_text(_project_not_found_reply(cleaned_answer))
            return True
        updated = runtime.set_project_id(chat_key, project_id)
        if updated is None:
            return True  # race: pending vanished (TTL/abandon) mid-resolution
        intake = updated

    existing_engram_id = intake.engrams.get(question_id)
    if existing_engram_id is not None:
        # Round-3-gate-4 P2-a: this question ALREADY has a recorded engram
        # -- reachable only via FirstPersonRuntime.start()'s resume clamp,
        # which deliberately keeps the "project" question's prior engram_id
        # even though it reverts `index` to re-ask that question (see its
        # docstring). squad_remember is an INSERT, never an upsert -- there
        # is no delete/update path for the OLD engram anywhere in this
        # module or the mupot side, so calling remember() again here would
        # leave a permanent duplicate answer in squad memory. Reuse the
        # existing engram_id and simply advance -- only project_id itself
        # (above) is re-derived from the fresh answer.
        updated = runtime.record_answer(chat_key, question_id, existing_engram_id)
        if updated is None:
            return True  # race: pending vanished (TTL/abandon) mid-write
        if updated.index >= len(FIRST_PERSON_QUESTIONS):
            await _submit_proposal(
                message, chat_key, updated, client=client, runtime=runtime, clock=clock, stalled_notifier=stalled_notifier
            )
            return True
        await message.reply_text(FIRST_PERSON_QUESTIONS[updated.index][1])
        return True

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
        return True

    updated = runtime.record_answer(chat_key, question_id, engram_id)
    if updated is None:
        # race: pending vanished (TTL/abandon) mid-write -- the engram
        # write-through already succeeded and is safe (write-through only,
        # never a transcript); nothing left here to advance.
        return True

    if updated.index >= len(FIRST_PERSON_QUESTIONS):
        # Round-3-gate-4 P3-d: this call was missing `stalled_notifier` --
        # the very FIRST submission attempt (right after the 5th answer)
        # would always use the un-rate-limited default, meaning the fresh-
        # completion path and the retry-wait path could each maintain their
        # own independent "have I notified" state for the SAME member.
        await _submit_proposal(
            message, chat_key, updated, client=client, runtime=runtime, clock=clock, stalled_notifier=stalled_notifier
        )
        return True
    await message.reply_text(FIRST_PERSON_QUESTIONS[updated.index][1])
    return True


async def _stall_reply(
    *,
    retry_count: int,
    member_id: str,
    project_id: str | None,
    client: MupotOperatorClient,
    runtime: FirstPersonRuntime,
    stalled_notifier: "_NotifyOncePerWindow | None",
) -> str:
    """Round-3-gate-3 / Athena's ruling (4), corrected round-3-gate-4 P1-a:
    the DEFECT when the server keeps rejecting a submission (today, always
    -- the live schema has no project_access kind) is the reply shape, not
    the rejection itself. An unbounded repeat of "I'll try again" forever
    reads as a permanent loop; sending "I've flagged it so a human can
    look" with NO receiver anywhere is fabricating a receipt on a human-
    decision channel -- exactly what the round-3-gate-3 build did.

    Once the retry backoff table is exhausted, this creates a REAL
    ``task_create`` item in this operator's own squad (never the member's
    home squad -- see mupot_operator.FIRST_PERSON_ACTIONS's note on why
    this is not a manage_access surface), correlated with a deterministic
    ``request_id`` and the durable stall marker below, and returns the
    reply that matches what ACTUALLY happened: the flagged line only if
    the task really was created, an honest "couldn't reach the team"
    line if it wasn't. The escalation itself (and any WARNING/task_create
    call) is rate-limited via ``stalled_notifier`` to at most once per
    ``_PROPOSAL_STALLED_REPLY_WINDOW_SECONDS`` -- round-3-gate-4 P3-a:
    every OTHER check-in during that window gets
    ``_PROPOSAL_STILL_STALLED_REPLY``, NEVER a revert to the unbounded
    ``_PROPOSAL_FAILED_REPLY``/``_PROPOSAL_RETRY_WAIT_REPLY`` pair used
    before the threshold."""
    if retry_count < _PROPOSAL_STALLED_AFTER_ATTEMPTS:
        return _PROPOSAL_FAILED_REPLY
    if stalled_notifier is not None and not stalled_notifier.should_notify(member_id):
        return _PROPOSAL_STILL_STALLED_REPLY

    request_id = hashlib.sha256(f"first-person-stall:{member_id}:{retry_count}".encode("utf-8")).hexdigest()[:32]
    runtime.store.record_stall(member_id, retry_count, runtime.wall_now())

    def create_task() -> dict[str, Any]:
        return client.call(
            "task_create",
            {
                "squad_id": client.settings.squad_id,
                "assignee_agent_id": client.settings.agent_id,
                "title": f"First-person intake stalled for member_id={member_id}",
                "done_when": (
                    "A human has reviewed why routine_proposal_submit keeps rejecting this "
                    "member's project-access proposal and either fixed the mupot-side contract "
                    "gap or manually completed the intake."
                ),
                "body": f"request_id={request_id} retry_count={retry_count} project_id={project_id}",
            },
        )

    try:
        response = await asyncio.to_thread(create_task)
    except Exception as exc:
        response = {"ok": False, "error": type(exc).__name__}

    flagged = isinstance(response, dict) and response.get("ok") is True
    if flagged:
        logger.warning(
            "mupot plugin: first-person proposal STALLED member_id=%s retry_count=%d "
            "request_id=%s -- flagged via task_create",
            member_id,
            retry_count,
            request_id,
        )
        return _PROPOSAL_STALLED_REPLY
    logger.warning(
        "mupot plugin: first-person proposal STALLED member_id=%s retry_count=%d "
        "request_id=%s -- task_create FAILED error=%s (durable stall marker recorded)",
        member_id,
        retry_count,
        request_id,
        response.get("error") if isinstance(response, dict) else "unknown",
    )
    return _PROPOSAL_STALLED_REPLY_FLAG_FAILED


async def _submit_proposal(
    message: Any,
    chat_key: str,
    intake: Intake,
    *,
    client: MupotOperatorClient,
    runtime: FirstPersonRuntime,
    clock: Any = time.monotonic,
    stalled_notifier: "_NotifyOncePerWindow | None" = None,
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
        retry_count = intake.proposal_retry_count + 1
        runtime.schedule_proposal_retry(
            chat_key,
            next_retry_at=clock() + _next_backoff(intake.proposal_retry_count),
            retry_count=retry_count,
        )
        await message.reply_text(
            await _stall_reply(
                retry_count=retry_count,
                member_id=intake.member_id,
                project_id=project_id,
                client=client,
                runtime=runtime,
                stalled_notifier=stalled_notifier,
            )
        )
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
        retry_count = intake.proposal_retry_count + 1
        runtime.schedule_proposal_retry(
            chat_key,
            next_retry_at=clock() + _next_backoff(intake.proposal_retry_count),
            retry_count=retry_count,
        )
        await message.reply_text(
            await _stall_reply(
                retry_count=retry_count,
                member_id=intake.member_id,
                project_id=project_id,
                client=client,
                runtime=runtime,
                stalled_notifier=stalled_notifier,
            )
        )
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
    stalled_notifier: "_NotifyOncePerWindow | None" = None,
) -> None:
    # Round-3-gate-3 P3 (:1119): a message during the proposal-retry-wait
    # phase is a genuine check-in, not silence -- refresh last_activity_at
    # first so repeated check-ins during a slow backoff cannot idle the
    # record out mid-wait (round-3-gate-2's trigger (b): "601s idle in the
    # retry-wait phase" was reachable specifically because touch() was never
    # called from anywhere).
    runtime.touch(chat_key)
    if clock() < intake.next_proposal_retry_at:
        # Athena's ruling (4): the same bounded/escalating reply shape as an
        # actual failed attempt -- a check-in during backoff must not repeat
        # the plain wait line forever once the backoff table is exhausted.
        if intake.proposal_retry_count >= _PROPOSAL_STALLED_AFTER_ATTEMPTS:
            reply = await _stall_reply(
                retry_count=intake.proposal_retry_count,
                member_id=intake.member_id,
                project_id=intake.project_id,
                client=client,
                runtime=runtime,
                stalled_notifier=stalled_notifier,
            )
        else:
            reply = _PROPOSAL_RETRY_WAIT_REPLY
        await message.reply_text(reply)
        return
    await _submit_proposal(
        message, chat_key, intake, client=client, runtime=runtime, clock=clock, stalled_notifier=stalled_notifier
    )


async def handle_first_contact(
    update: Any,
    *,
    settings: FirstPersonSettings,
    client: MupotOperatorClient,
    runtime: FirstPersonRuntime,
    secret_owner: ProfileSecretOwner | None = None,
    status_cache: _StatusCache | None = None,
    probe_limiter: "_ProbeLimiter | None" = None,
    sender_probe_limiter: "_SenderProbeLimiter | None" = None,
    lag_warning_notifier: "_NotifyOncePerWindow | None" = None,
    lag_reply_notifier: "_NotifyOncePerWindow | None" = None,
    home_wait_notifier: "_NotifyOncePerWindow | None" = None,
    stalled_notifier: "_NotifyOncePerWindow | None" = None,
    clock: Any = time.monotonic,
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

    Round-3-gate-2 P1 (:1408-1426) revision of the P0-B lag guard: if this
    module already holds a ``proposal_id`` for the resolved member (see
    ``FirstPersonRuntime.held_proposal_id``) while the server still reports
    ``pending``, this is server lag/bug -- still never a reason to re-intake
    -- but the update is now returned ``False`` (host owns the turn; a
    permanent ``True`` here was itself round-2's own DM-lockout bug) with a
    rate-limited reassurance reply (see ``_lag_reply_notifier``) and a
    separately rate-limited WARNING, rather than either on every message.
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
    # Round-3-gate-2 P2 (:1391-1399, :186): fetched BEFORE resolving status so
    # the cache can be bypassed for a member with genuine local progress --
    # see the `cache=` argument below.
    pending = runtime.get_pending(chat_key)

    # Round-3-gate-4 P0: a sender-limiter denial for a member who already
    # has a live pending record is not "no information" -- we necessarily
    # probed successfully for this exact member very recently (that is WHY
    # the limiter is now denying a repeat so soon), so the safe, bounded
    # fallback is "assume still pending, same identity" for the length of
    # the limiter's own short window, never "unknown, hold". A stranger (no
    # local pending record) gets no fallback and keeps the old fail-safe.
    fallback = (
        StatusResolution(
            bound=True, member_id=pending.member_id, home_squad_id=pending.home_squad_id, intake_state="pending"
        )
        if pending is not None
        else None
    )

    def resolve() -> StatusResolution:
        return resolve_member_status(
            settings,
            envelope["user_id"],
            envelope["chat_id"],
            secret_owner=secret_owner,
            # A member with a local pending record ALWAYS gets a fresh,
            # uncached probe: the per-user_id cache (including its 15-minute
            # "unknown" latch) exists to blunt a STRANGER hammering the bot,
            # never to delay noticing that a genuinely in-progress member's
            # status has changed. This also means a transient probe failure
            # for a pending member is never cached, so the very next message
            # re-probes live instead of replaying a stale "unknown" for the
            # whole latch window. The shared cache's incidental per-sender
            # rate-limiting is lost along with it for this branch -- that is
            # what `sender_limiter` restores, independently of `cache`
            # (round-3-gate-3 P1).
            cache=None if pending is not None else status_cache,
            probe_limiter=probe_limiter,
            sender_limiter=sender_probe_limiter,
            fallback=fallback,
        )

    status = await asyncio.to_thread(resolve)

    if not status.is_pending:
        if pending is not None and status.intake_state == "unknown":
            # Round-3-gate-2 P2 (:1391-1399): a TRANSIENT probe failure (or
            # the contract fields simply not present this one time) must
            # NEVER abandon genuine in-progress intake progress -- only a
            # DEFINITIVE non-pending status (unbound, 'none', 'complete')
            # does that. Hold the pending record exactly as it is; the next
            # message re-probes (see the cache bypass above).
            return False
        # Unbound, or a DEFINITIVE 'none'/'complete' -- fail-safe for the
        # intake: never consumed, the host handler owns the turn.
        if pending is not None:
            # Round-3-gate-4 P1-b: prune the local AND durable record only
            # once CONFIRMED (a second consecutive non-pending reading) --
            # a single reading leaves everything exactly as it is, so a
            # flap back to 'pending' next message resumes with every
            # engram intact.
            if runtime.note_non_pending(pending.member_id):
                runtime.abandon(chat_key)
        return False

    if pending is not None and pending.member_id != status.member_id:
        # A different member now resolves for this chat (e.g. a rebind) --
        # never carry progress across identities. This IS itself the
        # confirming signal (the SAME probe that just gave us status.
        # member_id also tells us it isn't pending.member_id) -- no second
        # reading needed.
        runtime.abandon(chat_key)
        pending = None

    if pending is not None:
        # A genuine, member-matched 'pending' reading -- reset any partial
        # non-pending sighting from a prior flap (round-3-gate-4 P1-b).
        runtime.note_pending(pending.member_id)

    if pending is None:
        held_proposal_id = runtime.held_proposal_id(status.member_id)
        if held_proposal_id is not None:
            # Round-3-gate-2 P1 (:1408-1426): this module already holds a
            # proposal for this member (mupot PR#1488: the server derives
            # intake_state=='complete' from the EXISTENCE of exactly this
            # proposal). The server still reporting 'pending' here is server
            # lag or a server-side bug -- never a reason to re-onboard. But
            # this is now a `return False`: the host still gets its own turn
            # (e.g. a real approve/reject from this same member) every
            # message -- only the WARNING and the reassurance reply are
            # rate-limited (separately), never the turn itself.
            if lag_warning_notifier is None or lag_warning_notifier.should_notify(status.member_id):
                logger.warning(
                    "mupot plugin: server reports intake_state='pending' for "
                    "member_id=%s while proposal_id=%s is already held -- "
                    "treating as complete-with-proposal, not re-intaking",
                    status.member_id,
                    held_proposal_id,
                )
            if lag_reply_notifier is None or lag_reply_notifier.should_notify(status.member_id):
                await message.reply_text(_AWAITING_HUMANS_REPLY)
            return False

        if runtime.is_complete_or_unknown(status.member_id):
            return False

        # Round-3-gate-2 P3: a whitespace-only home_squad_id is treated as
        # absent at THIS use site too, belt-and-suspenders alongside
        # resolve_member_status's own parse-time rejection of one.
        home_squad_id = status.home_squad_id
        if not isinstance(home_squad_id, str) or not home_squad_id.strip():
            # Verified on mupot's kasra/fp01-slice2-proposal-chain, 2026-09-21:
            # createHomeForMember exists ONLY as an internal TypeScript
            # function (src/org/service.ts) -- grepping every call site in
            # that repo found none outside its own unit tests, which call it
            # directly by import. There is NO exposed MCP action and NO /im
            # route for it; `FIRST_PERSON_ACTIONS` no longer lists it (see
            # mupot_operator.py) precisely so nothing here can pretend
            # otherwise. Per brief 2f(a), home creation is gated by the
            # member's own first contact and is mupot's job alone -- this
            # module never invents a home, never asks question 1 without a
            # real home id behind it. Athena's addition: a rate-limited
            # reassurance reply (once per hour per member) rather than pure
            # silence -- still consumes nothing, stores nothing, and retries
            # the status probe on the very next message.
            if home_wait_notifier is None or home_wait_notifier.should_notify(status.member_id):
                await message.reply_text(_HOME_NOT_READY_REPLY)
            return False

        started = runtime.start(chat_key, member_id=status.member_id, home_squad_id=home_squad_id)
        # Round-3-gate-3 P0 (:1856): `started.index` reflects any durable
        # prior progress FirstPersonRuntime.start() just resumed, and CAN
        # equal len(FIRST_PERSON_QUESTIONS) -- every question already
        # answered, but the proposal was never successfully submitted (the
        # live schema rejects the project_access kind today -- see
        # _submit_proposal -- so this is the common outcome, not an edge
        # case: a gateway restart, an idle drop during the retry-wait phase,
        # or a status flap all resume here with a fully-answered record).
        # Indexing FIRST_PERSON_QUESTIONS[started.index] unconditionally was
        # the round-2-gate-2 defect -- IndexError once index == len. Route
        # to the exact same submit-or-retry path an already-pending fully-
        # answered record uses below, never index past the last question.
        if started.index >= len(FIRST_PERSON_QUESTIONS):
            pending_text = getattr(message, "text", None)
            if _is_escape_text(pending_text):
                # Round-3-gate-4 P2-b: an escape word must pause the
                # retry-wait loop too, not just a mid-question turn.
                runtime.touch(chat_key)
                await message.reply_text(_PAUSED_REPLY)
                return True
            if isinstance(pending_text, str) and is_verdict_shaped(pending_text):
                # Round-3-gate-2 P2 (:1455-1458): never captured even here --
                # the host owns a genuine approve/reject regardless of where
                # this member's OWN intake happens to be.
                return False
            await _retry_proposal_if_due(
                message, chat_key, started, client=client, runtime=runtime, clock=clock, stalled_notifier=stalled_notifier
            )
            return True
        # Round-3-gate-2 P0 / Athena's RESUME REBINDS: ask the FIRST
        # UNANSWERED question. This message itself is the trigger that
        # opens/resumes the conversation, same convention as a fresh start's
        # first message -- never treated as an answer in the same turn.
        await message.reply_text(FIRST_PERSON_QUESTIONS[started.index][1])
        return True

    if pending.index >= len(FIRST_PERSON_QUESTIONS):
        pending_text = getattr(message, "text", None)
        if _is_escape_text(pending_text):
            # Round-3-gate-4 P2-b: same fix as the resume branch above.
            runtime.touch(chat_key)
            await message.reply_text(_PAUSED_REPLY)
            return True
        if isinstance(pending_text, str) and is_verdict_shaped(pending_text):
            # Round-3-gate-2 P2 (:1455-1458): never captured even in the
            # proposal-retry-wait phase -- the host owns a genuine
            # approve/reject regardless of where this member's OWN intake
            # happens to be.
            return False
        await _retry_proposal_if_due(
            message, chat_key, pending, client=client, runtime=runtime, clock=clock, stalled_notifier=stalled_notifier
        )
        return True

    handled = await _handle_answer(
        message, chat_key, pending, client=client, runtime=runtime,
        settings=settings, envelope=envelope, secret_owner=secret_owner,
        clock=clock, stalled_notifier=stalled_notifier,
    )
    return handled


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
    sender_probe_limiter: "_SenderProbeLimiter | None" = None,
    lag_warning_notifier: "_NotifyOncePerWindow | None" = None,
    lag_reply_notifier: "_NotifyOncePerWindow | None" = None,
    home_wait_notifier: "_NotifyOncePerWindow | None" = None,
    stalled_notifier: "_NotifyOncePerWindow | None" = None,
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
    if sender_probe_limiter is None:
        sender_probe_limiter = _SenderProbeLimiter()
    if lag_warning_notifier is None:
        lag_warning_notifier = _NotifyOncePerWindow(_LAG_WARNING_WINDOW_SECONDS)
    if lag_reply_notifier is None:
        lag_reply_notifier = _NotifyOncePerWindow(_LAG_REPLY_WINDOW_SECONDS)
    if home_wait_notifier is None:
        home_wait_notifier = _NotifyOncePerWindow(_HOME_NOT_READY_WINDOW_SECONDS)
    if stalled_notifier is None:
        stalled_notifier = _NotifyOncePerWindow(_PROPOSAL_STALLED_REPLY_WINDOW_SECONDS)
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
                sender_probe_limiter=sender_probe_limiter,
                lag_warning_notifier=lag_warning_notifier,
                lag_reply_notifier=lag_reply_notifier,
                home_wait_notifier=home_wait_notifier,
                stalled_notifier=stalled_notifier,
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

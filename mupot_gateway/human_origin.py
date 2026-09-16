"""Harness-stamped human message origin for mupot's ``human_origin`` verdict field.

The human talks to their own agent in natural language over a platform (Telegram
today); the HARNESS -- never the LLM -- must be the thing that stamps the inbound
message's origin onto the agent's ``task_verdict`` calls. Mupot resolves that
origin to the member and lets the call ride under the human's own identity
instead of the agent seat; without it, the call runs under the agent seat.

This is round 4. Round 3 replaced session-keyed claiming with a content+sender
equality bind at ``pre_llm_call`` -- correct predicate, wrong LIFETIME: a pending
capture whose binding turn failed to match (a reply-quote, an ``@``-reference, a
media caption) was silently re-queued and stayed spendable, by anyone who could
later present the same ``(session_key, user_id, sha256(text))``, for the entire
TTL. Since the "text" in question is the human's own approval word ("approve",
"yes", "ok") that is a real, if narrow, bearer-credential window -- not a custody
token. Round 4's rule, and the only thing that changed: **a pending record lives
until the NEXT ``pre_llm_call`` on its session, full stop.** It either binds to
that turn or it is burned right there, logged, never re-queued.

1. ``pre_gateway_dispatch`` (:func:`capture_human_origin`) -- unchanged trust
   fence (private, non-forwarded, self chat; shared forwarding check in
   ``telegram_fence.py``). The captured record carries a SHA-256 of the
   message's own text. TTL is now 2 minutes, not 10 -- explicitly a BACKSTOP
   for a session that never reaches ``pre_llm_call`` at all, not the mechanism
   that bounds a live credit's lifetime (that's rule 2 below).

2. ``pre_llm_call`` (:func:`bind_turn_custody`) fires once per turn, before the
   tool loop. On every call it POPS **every** pending record for the turn's
   session (not just scans them) -- the FIRST one whose ``sender_id``/text hash
   match this turn's own ``platform``/``sender_id``/``sha256(user_message)`` is
   bound to ``turn_id``; every other one is BURNED (dropped, never re-queued)
   and logged at WARNING with its ``message_id`` and a reason
   (``sender_mismatch``, ``text_mismatch``, or ``superseded`` for a later
   duplicate-content match). A turn that binds nothing still burns whatever was
   pending -- the record does not survive past the turn that was supposed to
   claim it, matched or not. A delegated subagent is refused outright via
   ``agent.delegation_context.is_delegated_child_context()`` before any of this,
   belt-and-braces on top of the content mismatch it would fail on anyway.

3. ``pre_tool_call`` (:func:`stamp_tool_call`) only ever READS whatever
   :func:`bind_turn_custody` already bound to THIS turn's ``turn_id`` -- no
   claiming, no FIFO. A model-supplied ``human_origin`` is stripped on **every**
   tool that looks like it belongs to mupot at all (any ``mcp__<configured mupot
   server>__*`` wire name, plus a small named fallback while the server name is
   still unresolved) -- not just the one tool this module stamps. It is only
   ever REPLACED with a bound origin for the exact, narrow stamp allowlist
   (:data:`HUMAN_ORIGIN_TOOL_NAMES` = ``{"task_verdict"}`` only -- mupot's
   server side (#1425) resolves ``human_origin`` per-tool, wired individually
   into ``toolTaskVerdict``, not via shared middleware across every tool; there
   is currently no other tool it is safe to stamp).

4. ``on_session_reset``/``on_session_end`` drop any pending/bound records for a
   session the moment Hermes itself considers it over, instead of relying
   solely on the TTL backstop.

Only Telegram is supported for now; every other platform is a recorded, not
silent, gap (one-time INFO log per platform name).

Documented residual (not fixed, by design -- see the PR thread): this remains a
content-and-sender EQUALITY PROOF over a short window, not a cryptographic
custody token. An injected/internal turn on the human's own session runs under
the human's own ``sender_id`` and ``platform`` (Hermes resolves both from the
turn's ``SessionSource``, which for an injected turn is a copy of the human's own
stored origin) -- the only remaining barrier is that no in-plugin injector today
emits a bare, attacker-chosen string equal to the human's own text (every
injector prepends a fixed, non-removable template). A future injector or
third-party ``pre_gateway_dispatch`` plugin that can emit an unwrapped string
would need to guess/replay the human's own recent words, and round 4's
bind-or-burn rule means it must do so as the very next ``pre_llm_call`` on that
session, before the human's own turn (if any) burns it first.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from collections import OrderedDict, deque
from typing import Any, Mapping, Optional

from ..telegram_fence import is_forwarded_telegram_message

logger = logging.getLogger(__name__)

SUPPORTED_PLATFORMS = frozenset({"telegram"})

# The ONLY tool this module ever STAMPS with a bound origin. Narrow and exact:
# mupot's server side (#1425) resolves human_origin per-tool, wired individually
# into toolTaskVerdict -- NOT shared middleware applied uniformly across every
# tool -- and today task_verdict is the only tool it resolves it on.
# needs_you_list was in this set through round 3; dropped in round 4
# (kasra-review round-3 P1-3): the #1425 server tree does not accept
# human_origin on it, so stamping it there would be inert at best. Reached as a
# bare mupot MCP tool (mupot_operator.py's build_operator_handlers has no
# generic passthrough -- every handler hand-picks its own payload fields; none
# forward human_origin), so pre_tool_call -- a GLOBAL Hermes lifecycle hook
# firing for every tool dispatch, not only this plugin's own registered tools
# -- is the only choke point that can see and stamp it.
HUMAN_ORIGIN_TOOL_NAMES = frozenset({"task_verdict"})

# Decision-adjacent mupot tools identified from the live kayhermes MCP schema
# cache (kasra-review round-3 P1-3) that must have a model-supplied
# human_origin STRIPPED even though this module never stamps them -- a model
# forging the field on any of these is exactly as much a forgery attempt as on
# task_verdict itself. Used only as a fallback while the configured server name
# is unresolved (see _looks_like_mupot_tool); once resolved, the broader "any
# tool under this configured mupot server" check below supersedes it for every
# tool that server exposes, not just these named ones.
_KNOWN_MUPOT_DECISION_TOOL_NAMES = frozenset({
    "task_verdict", "task_verdict_reverse", "needs_you_list",
    "approve_gate_edge", "advance_node", "objective_accept", "routine_run_answer",
})

# The mupot MCP server name this Hermes profile configures. UNSET (None) until
# mupot_gateway/adapter.py's adapter_factory calls set_mcp_server_name() with the
# SAME value MupotAdapter itself resolves (extra.get("mcp_server") or "mupot") --
# kasra-review round-2 P1-2: resolving lazily (rather than defaulting to "mupot"
# up front) means a governed call that somehow arrives before the platform adapter
# connects can only ever be STRIPPED (via _looks_like_mupot_tool, independent
# of server name for the named fallback list), never wrongly STAMPED under a
# guessed name.
DEFAULT_MCP_SERVER_NAME = "mupot"
_mcp_server_name: Optional[str] = None

# kasra-review round-2 P1-1: Hermes SANITIZES the server component of the wire name
# (tools/mcp_tool_schema.py's sanitize_mcp_name_component, re.sub(r"[^A-Za-z0-9_]",
# "_", ...)) -- imported lazily so a configured name like "mupot-prod" is compared
# through the IDENTICAL transform Hermes itself applies, not a hand-rolled guess.
# The regex fallback (used only when tools.mcp_tool_schema isn't importable, i.e.
# the plain non-native test suite) is copied verbatim from that function so the two
# can never silently drift on the character class.
_SANITIZE_FALLBACK_RE = re.compile(r"[^A-Za-z0-9_]")


def _sanitize_mcp_name_component(value: str) -> str:
    try:
        from tools.mcp_tool_schema import sanitize_mcp_name_component
        return sanitize_mcp_name_component(value)
    except Exception:
        return _SANITIZE_FALLBACK_RE.sub("_", str(value or ""))


def _hash_text(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8", errors="surrogatepass")).hexdigest()


# Bounded: a gateway running for weeks cannot grow these without limit.
#  - per-session pending queue: guards a burst of rapid messages before any turn
#    binds/burns them.
#  - total pending count across every session.
#  - total bound-record count across every (session_key, turn_id) pair.
# TTL is now ONLY a backstop for a session that never reaches pre_llm_call at all
# (round 4, kasra-review round-3 P0-1/Athena round-3): the mechanism that bounds
# a live credit's lifetime in the NORMAL case is bind_turn_custody's bind-or-burn
# rule (a pending record cannot survive past the very next pre_llm_call on its
# session, whether or not that turn actually binds it) -- not the clock. 2
# minutes is generous for the ordinary capture-then-turn-starts latency while
# being far too short to matter as an independent attack window on its own.
_MAX_PENDING_PER_SESSION = 8
_MAX_PENDING_TOTAL = 512
_MAX_BOUND = 512
_CAPTURE_TTL_SECONDS = 120.0

_WARNED_UNSUPPORTED_PLATFORMS: set[str] = set()

# session_id -> session_key, populated opportunistically the first time
# bind_turn_custody sees a session_id for a session_key (pre_llm_call is the only
# hook this module registers that receives BOTH). on_session_reset/on_session_end
# only ever receive session_id, never session_key, so this is what lets those two
# hooks find the right stash entries to drop. A session that captured a record but
# never ran any turn at all is not in this map -- the 10-minute TTL alone bounds
# that (extremely unlikely: capturing IS part of processing an inbound message,
# which necessarily starts a turn).
_MAX_SESSION_ID_MAP = 512
_SESSION_ID_TO_KEY: "OrderedDict[str, str]" = OrderedDict()


def _remember_session_id(session_id: str, session_key: str) -> None:
    if not session_id or not session_key:
        return
    _SESSION_ID_TO_KEY[session_id] = session_key
    _SESSION_ID_TO_KEY.move_to_end(session_id)
    while len(_SESSION_ID_TO_KEY) > _MAX_SESSION_ID_MAP:
        _SESSION_ID_TO_KEY.popitem(last=False)


class _OriginStash:
    """Turn-bound human-origin stash: PENDING captures, BOUND (custody-verified) records.

    ``capture()`` appends an unclaimed, unbound record to the calling session's FIFO
    (multiple pending captures can coexist within one turn's worth of latency --
    round 4's bind-or-burn rule means none of them can ever outlive the NEXT
    ``pre_llm_call`` on that session).

    ``bind(session_key, turn_id, sender_id, text_sha256, burn_callback)`` is the
    ONLY write path, and it is called EXACTLY ONCE per turn (from
    ``bind_turn_custody``). It unconditionally POPS every pending record for
    ``session_key`` -- not a scan, a drain. The FIRST one whose ``user_id``/
    ``text_sha256`` match this turn's own is bound to ``(session_key, turn_id)``;
    every other one is BURNED: dropped, never re-queued, and reported via
    ``burn_callback(record, reason)`` with ``reason`` one of ``"sender_mismatch"``,
    ``"text_mismatch"``, or ``"superseded"`` (a later record that ALSO matches,
    once the first match has already been bound -- round 3's identical-text id
    drift is closed by this as a side effect: only the oldest matching record is
    ever bound, and every other one, matching or not, is gone). A turn that binds
    NOTHING still burns whatever was pending: a captured-but-unbound record does
    not survive past the turn that was supposed to claim it.

    ``read(session_key, turn_id)`` is the ONLY read path (``pre_tool_call``): it
    returns whatever is bound to that exact turn, or ``None`` -- never touches the
    pending queue, never falls back to "whatever is left".
    """

    def __init__(
        self,
        max_pending_per_session: int = _MAX_PENDING_PER_SESSION,
        max_pending_total: int = _MAX_PENDING_TOTAL,
        max_bound: int = _MAX_BOUND,
        ttl_seconds: float = _CAPTURE_TTL_SECONDS,
    ) -> None:
        self._max_pending_per_session = max_pending_per_session
        self._max_pending_total = max_pending_total
        self._max_bound = max_bound
        self._ttl_seconds = ttl_seconds
        self._pending: "OrderedDict[str, deque[tuple[float, dict[str, Any]]]]" = OrderedDict()
        self._pending_count = 0
        self._bound: "OrderedDict[tuple[str, str], tuple[float, dict[str, Any]]]" = OrderedDict()

    def _expired(self, stamped_at: float) -> bool:
        return (time.monotonic() - stamped_at) > self._ttl_seconds

    def capture(self, session_key: str, record: Mapping[str, Any]) -> None:
        if not session_key:
            return
        dq = self._pending.setdefault(session_key, deque())
        dq.append((time.monotonic(), dict(record)))
        self._pending_count += 1
        while len(dq) > self._max_pending_per_session:
            dq.popleft()
            self._pending_count -= 1
        if not dq:
            self._pending.pop(session_key, None)
        else:
            self._pending.move_to_end(session_key)
        while self._pending_count > self._max_pending_total and self._pending:
            oldest_session, oldest_dq = next(iter(self._pending.items()))
            if oldest_dq:
                oldest_dq.popleft()
                self._pending_count -= 1
            if not oldest_dq:
                self._pending.pop(oldest_session, None)

    def bind(
        self, session_key: str, turn_id: str, *, sender_id: str, text_sha256: str,
        burn_callback: Optional[Any] = None,
    ) -> bool:
        """Drain every pending record for *session_key* unconditionally. The first
        one matching (*sender_id*, *text_sha256*) is bound to *turn_id*; every
        other one -- expired, mismatched, or a later duplicate match -- is burned
        and reported to ``burn_callback(record, reason)`` if given. Nothing
        pending is ever re-queued, whether or not this call ends up binding
        anything."""
        if not session_key or not turn_id:
            return False
        key = (session_key, turn_id)
        if key in self._bound:
            return True  # idempotent: pre_llm_call firing twice for one turn is a no-op
        dq = self._pending.pop(session_key, None)
        if dq:
            self._pending_count -= len(dq)
        if not dq:
            return False
        found: Optional[tuple[float, dict[str, Any]]] = None
        while dq:
            stamped_at, record = dq.popleft()
            if self._expired(stamped_at):
                if burn_callback is not None:
                    burn_callback(record, "expired")
                continue
            if (
                found is None
                and record.get("user_id") == sender_id
                and record.get("text_sha256") == text_sha256
            ):
                found = (stamped_at, record)
                continue  # bound below -- not a burn
            if record.get("user_id") != sender_id:
                reason = "sender_mismatch"
            elif record.get("text_sha256") != text_sha256:
                reason = "text_mismatch"
            else:
                reason = "superseded"  # matched, but an earlier record already won this bind
            if burn_callback is not None:
                burn_callback(record, reason)
        if found is None:
            return False
        self._bound[key] = found
        self._bound.move_to_end(key)
        while len(self._bound) > self._max_bound:
            self._bound.popitem(last=False)
        return True

    def read(self, session_key: str, turn_id: str) -> Optional[dict[str, Any]]:
        if not session_key or not turn_id:
            return None
        key = (session_key, turn_id)
        entry = self._bound.get(key)
        if entry is None:
            return None
        stamped_at, record = entry
        if self._expired(stamped_at):
            del self._bound[key]
            return None
        self._bound.move_to_end(key)
        return dict(record)

    def drop_session(self, session_key: str) -> None:
        if not session_key:
            return
        dq = self._pending.pop(session_key, None)
        if dq:
            self._pending_count -= len(dq)
        for key in [k for k in self._bound if k[0] == session_key]:
            del self._bound[key]

    def __len__(self) -> int:
        return self._pending_count + len(self._bound)

    def clear(self) -> None:
        self._pending.clear()
        self._pending_count = 0
        self._bound.clear()


# Process-global: one gateway process serves every concurrent session, exactly
# like adapter.py's own _ACTIVE_WATCHERS / gateway/run.py's _session_sources.
_STASH = _OriginStash()


def set_mcp_server_name(name: Optional[str]) -> None:
    """Called from mupot_gateway/adapter.py's adapter_factory with the SAME value
    MupotAdapter itself resolves. A falsy *name* explicitly UNSETS resolution
    (prefix matching refused entirely until set again) rather than falling back to
    a guessed default -- see the module-level ``_mcp_server_name`` docstring note."""
    global _mcp_server_name
    if not name:
        _mcp_server_name = None
        return
    _mcp_server_name = _sanitize_mcp_name_component(name)


def _looks_like_mupot_tool(tool_name: Any) -> bool:
    """Broad check used ONLY to decide whether a model-supplied ``human_origin``
    must be stripped (fail closed) -- never to decide whether to stamp. Round 4
    (kasra-review round-3 P1-3): widened from "looks like task_verdict on any
    server" to "belongs to the configured mupot server AT ALL" -- the live
    kayhermes schema cache exposes several other decision-adjacent tools
    (``task_verdict_reverse``, ``approve_gate_edge``, ``advance_node``,
    ``objective_accept``, ``routine_run_answer``) that a model could just as
    easily try to forge ``human_origin`` on, none of which the old bare-suffix
    check ever looked at. True for: any of :data:`_KNOWN_MUPOT_DECISION_TOOL_NAMES`
    by bare name; ANY ``mcp__<configured mupot server>__*`` wire name once the
    server is resolved (every tool that server exposes, not just named ones);
    or, while unresolved, an ``mcp__<anything>__`` prefix ending in one of the
    named decision tools (the same server-name-independent fallback round 3 had,
    now covering more names).
    """
    if not isinstance(tool_name, str):
        return False
    if tool_name in _KNOWN_MUPOT_DECISION_TOOL_NAMES:
        return True
    if _mcp_server_name and tool_name.startswith(f"mcp__{_mcp_server_name}__"):
        return True
    if tool_name.startswith("mcp__"):
        for name in _KNOWN_MUPOT_DECISION_TOOL_NAMES:
            if tool_name.endswith(f"__{name}"):
                return True
    return False


def _resolve_governed_tool_name(tool_name: Any) -> Optional[str]:
    """Return the canonical bare name when *tool_name* is exactly the bare name OR
    exactly ``mcp__<configured mupot server>__<bare name>`` (the wire name a live
    gateway with mupot registered as an MCP server actually emits, sanitized the
    same way Hermes sanitizes it); else ``None``. Exact match only, and refuses
    ALL prefix matching until :func:`set_mcp_server_name` has been called with a
    real value (kasra-review round-2 P1-2)."""
    if not isinstance(tool_name, str):
        return None
    if tool_name in HUMAN_ORIGIN_TOOL_NAMES:
        return tool_name
    if not _mcp_server_name:
        return None
    prefix = f"mcp__{_mcp_server_name}__"
    if tool_name.startswith(prefix):
        suffix = tool_name[len(prefix):]
        if suffix in HUMAN_ORIGIN_TOOL_NAMES:
            return suffix
    return None


def _platform_name(source: Any) -> str:
    platform = getattr(source, "platform", None)
    value = getattr(platform, "value", None)
    if isinstance(value, str) and value:
        return value
    return str(platform) if platform is not None else "unknown"


def _isoformat(value: Any) -> Optional[str]:
    if value is None:
        return None
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return isoformat()
        except Exception:
            return None
    return str(value)


def chat_type_value(source: Any) -> Optional[str]:
    value = getattr(source, "chat_type", None)
    return value if isinstance(value, str) else (str(value) if value is not None else None)


def _warn_unsupported_platform_once(platform: str) -> None:
    if platform in _WARNED_UNSUPPORTED_PLATFORMS:
        return
    _WARNED_UNSUPPORTED_PLATFORMS.add(platform)
    logger.info(
        "mupot plugin: human-origin capture is not yet supported for platform %r; "
        "task_verdict/needs_you_list calls from this platform run under the agent seat",
        platform,
    )


def _passes_trust_fence(event: Any, source: Any) -> bool:
    """The same private/self-chat/unforwarded invariant telegram_control.py's
    ``_sanitized_envelope`` enforces, expressed against Hermes's own normalized
    ``SessionSource`` fields. Runs BEFORE Hermes's own
    ``_is_user_authorized_for_source`` (``gateway/run_inbound.py`` line ~180 vs
    ~185) -- an unauthorized or unknown sender must never be able to write a
    record at all. Telegram's own DM invariant is ``chat_id == user_id``; a
    Telegram callback-query/inline-query source carries the RAW ``"private"``
    literal rather than Hermes's normalized ``"dm"`` (the main inbound
    ``build_event`` path normalizes ``private`` -> ``dm``; callback/inline paths
    build their own ad hoc source dicts and do not) -- this fence's literal
    ``"dm"`` comparison therefore also fails closed on those paths today (a real,
    documented coverage gap for inline-button approvals, not a security hole).
    """
    chat_type = getattr(source, "chat_type", None)
    if chat_type != "dm":
        return False
    user_id = getattr(source, "user_id", None)
    chat_id = getattr(source, "chat_id", None)
    if user_id is None or chat_id is None or str(user_id) != str(chat_id):
        return False
    if is_forwarded_telegram_message(getattr(event, "raw_message", None)):
        return False
    return True


def _session_key_for_source(gateway: Any, session_store: Any, source: Any) -> Optional[str]:
    """The exact key ``gateway/run.py``'s ``_set_session_env`` binds for this turn
    (verified against ``gateway/session_recovery.py``'s ``_generate_session_key``:
    same method, same object, called with the same ``source``)."""
    store = session_store if session_store is not None else getattr(gateway, "session_store", None)
    generate = getattr(store, "_generate_session_key", None)
    if not callable(generate):
        return None
    try:
        key = generate(source)
    except Exception:
        logger.debug(
            "mupot plugin: session_key derivation failed for human-origin capture", exc_info=True
        )
        return None
    return key or None


def capture_human_origin(
    *, event: Any, gateway: Any = None, session_store: Any = None, **_kwargs: Any
) -> None:
    """``pre_gateway_dispatch`` hook. Pure observer: always returns ``None`` (never
    skips or rewrites the inbound event) and never raises. Produces at most one
    PENDING (unbound) stash record per genuinely private, unforwarded, self-chat
    Telegram message; :func:`bind_turn_custody` is the only place that record is
    ever bound to a turn, and :func:`stamp_tool_call` the only place a bound record
    is ever read.
    """
    try:
        source = getattr(event, "source", None)
        if source is None:
            return None
        platform = _platform_name(source)
        if platform not in SUPPORTED_PLATFORMS:
            _warn_unsupported_platform_once(platform)
            return None
        if not _passes_trust_fence(event, source):
            return None
        session_key = _session_key_for_source(gateway, session_store, source)
        if not session_key:
            logger.debug("mupot plugin: no session_key resolvable; skipping human-origin capture")
            return None
        user_id = getattr(source, "user_id", None)
        chat_id = getattr(source, "chat_id", None)
        thread_id = getattr(source, "thread_id", None)
        # event.message_id, NOT source.message_id: SessionSource.message_id is the
        # "triggering message (pin/reply/react)" reference, not this message's own id.
        message_id = getattr(event, "message_id", None)
        text = getattr(event, "text", None)
        record = {
            "platform": platform,
            "user_id": str(user_id) if user_id is not None else None,
            "chat_id": str(chat_id) if chat_id is not None else None,
            "message_id": str(message_id) if message_id is not None else None,
            "timestamp": _isoformat(getattr(event, "timestamp", None)),
            "chat_type": chat_type_value(source),
            "thread_id": str(thread_id) if thread_id is not None else None,
            "forwarded": False,  # _passes_trust_fence already refused any forwarded message
            # Internal-only correlation key for bind(); stripped before ever being
            # returned as a stamped human_origin (see stamp_tool_call).
            "text_sha256": _hash_text(text if isinstance(text, str) else ""),
        }
        _STASH.capture(session_key, record)
    except Exception:
        logger.warning("mupot plugin: human-origin capture failed", exc_info=True)
    return None


def _current_session_key() -> Optional[str]:
    try:
        from tools.approval_context import get_current_session_key
    except Exception:
        return None
    try:
        key = get_current_session_key(default="")
    except Exception:
        return None
    return key or None


def _in_delegated_child_context() -> bool:
    """True while this turn/tool call is executing inside a delegated subagent
    (both ``tools/delegate_tool.py`` and ``tools/delegate_tool_child_run.py``
    spawn points wrap the ENTIRE child conversation -- every ``pre_llm_call`` and
    every tool call it makes -- in ``agent.delegation_context.delegated_child_context()``).
    Checked directly in :func:`bind_turn_custody` so a delegated child can never
    bind a human-origin record even in the narrow race where it would otherwise
    present matching content before the parent turn does. Fails closed to "not a
    child" on ImportError (the plain, non-native test suite) the same way
    :func:`_passes_trust_fence` treats a missing raw_message -- content+sender
    matching is still the primary defense either way.
    """
    try:
        from agent.delegation_context import is_delegated_child_context
    except Exception:
        return False
    try:
        return bool(is_delegated_child_context())
    except Exception:
        return False


def _log_burn(record: Mapping[str, Any], reason: str) -> None:
    """A pending capture that did not survive to be bound to a turn (kasra-review
    round-3 P1-2: this used to be silent -- the only signal a reply-quoted
    approval or a text-mismatch ever produced). Logged at WARNING, never at
    DEBUG: an operator investigating "my approval didn't count as mine" needs
    this to be findable."""
    try:
        logger.warning(
            "mupot plugin: burning unconsumed human-origin capture message_id=%s reason=%s",
            record.get("message_id"), reason,
        )
    except Exception:
        pass


def bind_turn_custody(
    *, session_id: str = "", task_id: str = "", turn_id: str = "", user_message: Any = None,
    conversation_history: Any = None, is_first_turn: Any = None, model: str = "",
    platform: str = "", parent_session_id: str = "", sender_id: str = "", **_kwargs: Any,
) -> None:
    """``pre_llm_call`` hook -- bind-or-burn (round 4).

    Binds a pending capture to THIS ``turn_id`` iff the turn's own fully-prepared
    inbound message (``user_message``) hashes to exactly the text a pending record
    was captured from, AND the turn's ``sender_id`` matches that record's
    ``user_id``. An internal/plugin-injected turn's ``user_message`` is the
    injected prompt text, never the human's own message, so it can never match; a
    cron/routine turn has no matching Telegram sender/text either; a delegated
    subagent is refused outright.

    Whether or not this call binds anything, EVERY other pending record for this
    session is burned right here (kasra-review round-3 P0-1: a record that fails
    to bind must never be re-queued to sit spendable by whatever turn asks next --
    a reply-quoted "approve", whose prepared text never matches the bare captured
    text, must not leave "approve" pending for up to the full TTL). Every burn is
    logged at WARNING with the burned record's ``message_id`` and a reason.

    Never raises: a bind failure just means nothing is stamped for this turn --
    fail open on the FEATURE (the turn proceeds normally, under the agent seat),
    fail closed on the ATTESTATION (no human_origin is ever fabricated, and the
    human sees no stamp -- visible downstream as the server's own
    ``applied:false`` on the verdict, not a crash or a silent wrong answer).
    """
    try:
        if platform != "telegram" or not turn_id:
            return None
        if _in_delegated_child_context():
            return None
        if not isinstance(user_message, str):
            return None
        session_key = _current_session_key()
        if not session_key:
            return None
        if session_id:
            _remember_session_id(str(session_id), session_key)
        _STASH.bind(
            session_key, turn_id,
            sender_id=str(sender_id or ""), text_sha256=_hash_text(user_message),
            burn_callback=_log_burn,
        )
    except Exception:
        logger.warning("mupot plugin: human-origin turn-binding failed", exc_info=True)
    return None


def stamp_tool_call(
    *, tool_name: str = "", args: Any = None, turn_id: str = "", **_kwargs: Any
) -> Optional[dict[str, Any]]:
    """``pre_tool_call`` hook. Reads only; never claims, never falls back to "the
    oldest pending record" -- that FIFO-claim mechanism is gone (round 3).

    A model-supplied ``human_origin`` is ALWAYS stripped on EVERY tool that looks
    like it belongs to mupot at all (:func:`_looks_like_mupot_tool`, widened in
    round 4 to cover every tool under the configured mupot server, not just the
    one this module stamps -- independent of server-name match, fail closed even
    when :func:`set_mcp_server_name` hasn't run yet or the configured name
    doesn't match); it is only REPLACED with a bound origin when the tool name is
    an EXACT match for the narrow stamp allowlist on the configured mupot server
    (:func:`_resolve_governed_tool_name`) AND :func:`bind_turn_custody` already
    bound a record to THIS turn_id. Never raises.
    """
    try:
        if not isinstance(args, dict):
            return None
        if not _looks_like_mupot_tool(tool_name):
            return None
        model_supplied = "human_origin" in args
        origin: Optional[dict[str, Any]] = None
        if _resolve_governed_tool_name(tool_name) is not None and turn_id:
            session_key = _current_session_key()
            if session_key:
                bound = _STASH.read(session_key, turn_id)
                if bound is not None:
                    origin = {k: v for k, v in bound.items() if k != "text_sha256"}
        if model_supplied:
            logger.warning(
                "mupot plugin: model supplied human_origin directly for tool '%s' -- "
                "this field is harness-stamped only; treating as a forgery attempt and %s",
                tool_name,
                "overwriting it with the bound origin" if origin is not None else "stripping it",
            )
        if origin is not None:
            return {"action": "modify", "args": {"human_origin": origin}}
        if model_supplied:
            args.pop("human_origin", None)
        return None
    except Exception:
        logger.warning("mupot plugin: human-origin stamping failed", exc_info=True)
        return None


def _on_session_boundary(*, session_id: str = "", **_kwargs: Any) -> None:
    """``on_session_reset``/``on_session_end`` hook: drop any pending/bound
    records for the session Hermes itself just ended, instead of relying solely on
    the TTL. Both hooks give only ``session_id`` (never ``session_key``), so this
    consults the map :func:`bind_turn_custody` opportunistically populates."""
    try:
        session_key = _SESSION_ID_TO_KEY.pop(str(session_id), None) if session_id else None
        if session_key:
            _STASH.drop_session(session_key)
    except Exception:
        logger.debug("mupot plugin: on_session_boundary cleanup failed", exc_info=True)
    return None


def register(ctx: Any) -> None:
    """Wire all four hooks. Called from the native gateway's own ``register()``
    (``mupot_gateway/adapter.py``), FIRST, before anything else is registered.
    A Hermes runtime that cannot ``register_hook`` (or whose hook registration
    itself fails) gets NO native-gateway registration at all -- refusing loudly
    beats leaving an unenforced identity-attestation surface running silently.
    """
    register_hook = getattr(ctx, "register_hook", None)
    if not callable(register_hook):
        logger.error(
            "mupot plugin: ctx has no register_hook -- this Hermes runtime cannot enforce "
            "human_origin integrity on task_verdict/needs_you_list (a model-supplied value "
            "would otherwise reach mupot unchecked). Refusing native-gateway registration "
            "entirely rather than degrade silently."
        )
        raise RuntimeError(
            "mupot native gateway requires a Hermes runtime with PluginContext.register_hook "
            "(human_origin attestation integrity cannot otherwise be enforced)"
        )
    try:
        register_hook("pre_gateway_dispatch", capture_human_origin)
        register_hook("pre_llm_call", bind_turn_custody)
        register_hook("pre_tool_call", stamp_tool_call)
        register_hook("on_session_reset", _on_session_boundary)
        register_hook("on_session_end", _on_session_boundary)
    except Exception:
        logger.error("mupot plugin: human-origin hook registration failed", exc_info=True)
        raise

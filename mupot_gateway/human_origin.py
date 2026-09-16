"""Harness-stamped human message origin for mupot's ``human_origin`` verdict field.

The human talks to their own agent in natural language over a platform (Telegram
today); the HARNESS -- never the LLM -- must be the thing that stamps the inbound
message's origin (platform, user id, chat id, message id, timestamp) onto the
agent's ``task_verdict``/``needs_you_list`` calls. Mupot resolves that origin to the
member and lets the call ride under the human's own identity instead of the agent
seat; without it, the call runs under the agent seat as today.

Two Hermes lifecycle hooks do the whole job, and BOTH classes an adversarial gate
found (kasra-review + Athena, 2026-09-16, PR#13) are addressed at the class level,
not per repro:

* ``pre_gateway_dispatch`` (:func:`capture_human_origin`) fires once per inbound
  ``MessageEvent``, straight off the platform adapter, BEFORE Hermes's own
  ``_is_user_authorized_for_source`` runs (``gateway/run_inbound.py``: the hook at
  line ~180, auth at ~185) -- so this module is its OWN trust fence, not a
  convenience filter: a message is captured only when it is a private,
  non-forwarded, self chat (``chat_type == "dm"`` and ``user_id == chat_id``,
  Telegram's own DM invariant), mirroring ``telegram_control.py``'s
  ``_sanitized_envelope`` gate exactly (shared forwarding-marker check lives in
  ``telegram_fence.py`` so the two never drift). A captured record is PENDING,
  not yet bound to any turn.
* ``pre_tool_call`` (:func:`stamp_tool_call`) fires once per tool dispatch, for
  ``task_verdict``/``needs_you_list`` only -- matched by bare name OR by the exact
  ``mcp__<mupot-server>__<tool>`` wire name Hermes's MCP tool registration emits
  (``tools/mcp_tool_schema.py``'s ``mcp_prefixed_tool_name``; a live gateway NEVER
  emits the bare name once mupot is configured as an MCP server, so bare-only
  matching is a silent, unfired hook -- kasra-review P0-1). The FIRST such call
  from a NEWLY-arrived turn CLAIMS the oldest pending capture for its session,
  binding it to that turn's ``turn_id`` (received directly in ``pre_tool_call``'s
  own kwargs); every LATER call sharing that exact ``turn_id`` keeps reading the
  same claimed origin, but no OTHER turn -- an internal/plugin-injected turn on
  the same session key (``gateway/run_inbound.py``'s
  ``_dispatch_plugin_message_injection``, and this plugin's own
  ``notifications.py`` activation path), a cron turn, or a delegated subagent
  (``tools/delegate_tool_child_run.py``, which additionally always runs inside
  ``agent.delegation_context.is_delegated_child_context()`` -- checked directly,
  belt-and-suspenders on top of the turn_id mismatch) -- can ever claim or read
  it (kasra-review P0-2, the subagent sub-case, and Athena's positive-custody-
  token framing). A model-supplied ``human_origin`` is always overwritten (or
  stripped, when nothing is claimable) and logged as a forgery attempt.

Only Telegram is supported for now. Every other platform is recorded (a one-time
log per platform name) as unsupported: ``task_verdict``/``needs_you_list`` calls
from those turns simply never carry a claimed origin and fall back to running
under the agent seat, exactly like a CLI turn, a cron turn, or a subagent turn.

Registration (:func:`register`) fails CLOSED: a Hermes runtime whose
``PluginContext`` cannot ``register_hook`` (or whose hook registration itself
raises) gets NO native-gateway registration at all, not a silent, unenforced
attestation surface -- there is no other choke point in this plugin able to see
``task_verdict``/``needs_you_list`` args before they leave the process (see
``mupot_operator.py``'s ``build_operator_handlers``: no handler there forwards a
raw, model-supplied ``human_origin`` at all; those two tools are reached as bare
mupot MCP tools, never through this plugin's own action allowlist).
"""

from __future__ import annotations

import logging
import time
from collections import OrderedDict, deque
from typing import Any, Mapping, Optional

from ..telegram_fence import is_forwarded_telegram_message

logger = logging.getLogger(__name__)

# Platforms the harness can currently attest a human origin for. Every other
# platform is a documented gap, not a silent one (see _warn_unsupported_platform).
SUPPORTED_PLATFORMS = frozenset({"telegram"})

# The plugin's own mupot tools that accept an optional ``human_origin`` object.
# Reached as bare mupot MCP tools (not wrapped by mupot_operator.py's action
# allowlist -- every handler in build_operator_handlers hand-picks its own
# payload fields; none forward human_origin), so pre_tool_call -- a GLOBAL
# Hermes lifecycle hook firing for every tool dispatch, not only this plugin's
# own registered tools -- is the only choke point that can see and stamp them.
HUMAN_ORIGIN_TOOL_NAMES = frozenset({"task_verdict", "needs_you_list"})

# The mupot MCP server name this Hermes profile configures (mcp_servers.<name>).
# Hermes registers every MCP tool as mcp__<server>__<tool> (tools/mcp_tool_schema.py's
# mcp_prefixed_tool_name) -- the REGISTRY name, not an alias; a live kayhermes gateway
# emits mcp__mupot__task_verdict, never the bare name. mupot_gateway/adapter.py's
# adapter_factory calls set_mcp_server_name() with the SAME value MupotAdapter itself
# resolves (extra.get("mcp_server") or "mupot"), so this only ever matches the
# configured mupot server's own tools -- never a same-named tool on a different MCP
# server, which is exactly why this is a name comparison and not a bare-suffix scan.
DEFAULT_MCP_SERVER_NAME = "mupot"
_mcp_server_name = DEFAULT_MCP_SERVER_NAME

# Bounded so a gateway that runs for weeks cannot grow this stash without limit.
# Two independent bounds:
#  - per-session pending queue (guards a burst of rapid messages before any turn
#    claims one -- see docstring's turn-binding note; FIFO, oldest claimed first);
#  - total claimed-record count across every (session_key, turn_id) pair.
# TTL is a BACKSTOP only (kasra-review's own framing): the primary defense is the
# turn_id binding, not the clock -- a stale, unclaimed pending capture or an
# abandoned claim eventually falls out on its own even if nothing ever reads it.
_MAX_PENDING_PER_SESSION = 8
_MAX_PENDING_TOTAL = 512
_MAX_CLAIMS = 512
_STASH_TTL_SECONDS = 1800.0

_WARNED_UNSUPPORTED_PLATFORMS: set[str] = set()


class _OriginStash:
    """Turn-bound human-origin stash.

    ``capture()`` appends an UNCLAIMED record to the calling session's FIFO
    (one inbound human DM message = one record; multiple rapid messages queue,
    oldest first -- so a same-user double-message never misattributes the
    SECOND message's id to the FIRST message's turn, kasra-review's P0-3b).

    ``claim(session_key, turn_id)`` is the ONLY read path. The first call for a
    given ``(session_key, turn_id)`` pops the oldest unclaimed record for that
    session and binds it to ``turn_id`` for the rest of that turn's lifetime;
    every subsequent call with the SAME ``turn_id`` returns the same bound
    record (so a turn that calls task_verdict more than once still gets
    stamped every time); a DIFFERENT ``turn_id`` on the same session -- an
    internal/injected turn, a cron turn, a delegated subagent -- sees only
    whatever is LEFT in the pending queue (nothing, once the legitimate turn
    has claimed its own), never a record another turn already claimed.
    """

    def __init__(
        self,
        max_pending_per_session: int = _MAX_PENDING_PER_SESSION,
        max_pending_total: int = _MAX_PENDING_TOTAL,
        max_claims: int = _MAX_CLAIMS,
        ttl_seconds: float = _STASH_TTL_SECONDS,
    ) -> None:
        self._max_pending_per_session = max_pending_per_session
        self._max_pending_total = max_pending_total
        self._max_claims = max_claims
        self._ttl_seconds = ttl_seconds
        self._pending: "OrderedDict[str, deque[tuple[float, dict[str, Any]]]]" = OrderedDict()
        self._pending_count = 0
        self._claims: "OrderedDict[tuple[str, str], tuple[float, dict[str, Any]]]" = OrderedDict()

    def _expired(self, stamped_at: float) -> bool:
        return (time.monotonic() - stamped_at) > self._ttl_seconds

    def capture(self, session_key: str, origin: Mapping[str, Any]) -> None:
        if not session_key:
            return
        dq = self._pending.setdefault(session_key, deque())
        dq.append((time.monotonic(), dict(origin)))
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

    def claim(self, session_key: str, turn_id: str) -> Optional[dict[str, Any]]:
        if not session_key or not turn_id:
            return None
        key = (session_key, turn_id)
        existing = self._claims.get(key)
        if existing is not None:
            stamped_at, origin = existing
            if self._expired(stamped_at):
                del self._claims[key]
            else:
                self._claims.move_to_end(key)
                return dict(origin)
        dq = self._pending.get(session_key)
        while dq:
            stamped_at, origin = dq.popleft()
            self._pending_count -= 1
            if not dq:
                self._pending.pop(session_key, None)
            if self._expired(stamped_at):
                continue
            self._claims[key] = (stamped_at, dict(origin))
            self._claims.move_to_end(key)
            while len(self._claims) > self._max_claims:
                self._claims.popitem(last=False)
            return dict(origin)
        return None

    def __len__(self) -> int:
        return self._pending_count + len(self._claims)

    def clear(self) -> None:
        self._pending.clear()
        self._pending_count = 0
        self._claims.clear()


# Process-global: one gateway process serves every concurrent session, exactly
# like adapter.py's own _ACTIVE_WATCHERS / gateway/run.py's _session_sources.
_STASH = _OriginStash()


def set_mcp_server_name(name: Optional[str]) -> None:
    """Called from mupot_gateway/adapter.py's adapter_factory with the SAME value
    MupotAdapter itself resolves (extra.get("mcp_server") or "mupot"), so tool-name
    matching below always tracks whatever this Hermes profile actually configured,
    never a hardcoded guess independent of the live adapter."""
    global _mcp_server_name
    _mcp_server_name = name or DEFAULT_MCP_SERVER_NAME


def _resolve_governed_tool_name(tool_name: Any) -> Optional[str]:
    """Return the canonical bare name ("task_verdict"/"needs_you_list") when
    *tool_name* is exactly that bare name OR exactly
    ``mcp__<configured mupot server>__<bare name>`` (the wire name a live gateway
    with mupot registered as an MCP server actually emits); else ``None``.

    Exact match only: no case-folding, no trimming, no alternate separators. A
    model, or a malicious same-named tool on a DIFFERENT MCP server, presenting
    a look-alike name must never be treated as governed.
    """
    if not isinstance(tool_name, str):
        return None
    if tool_name in HUMAN_ORIGIN_TOOL_NAMES:
        return tool_name
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
    """The SAME private/self-chat/unforwarded invariant telegram_control.py's
    ``_sanitized_envelope`` enforces (lines ~140-156 there), expressed against
    Hermes's own normalized ``SessionSource`` fields instead of the raw PTB
    ``Update`` (a different shape at this layer -- ``pre_gateway_dispatch``
    never sees the raw ``Update``, only ``MessageEvent``/``SessionSource``; the
    one piece that IS the same raw shape, the forwarding-marker check, is
    literally shared via ``telegram_fence.is_forwarded_telegram_message``).

    This gate runs BEFORE Hermes's own ``_is_user_authorized_for_source``
    (``pre_gateway_dispatch`` fires at ``gateway/run_inbound.py`` line ~180,
    auth at ~185) -- so it is this module's OWN authorization check, not a
    redundant belt-and-suspenders on top of Hermes's: an unauthorized or
    unknown sender, or a second participant in a group/thread that shares a
    session key with someone else (Hermes's own
    ``thread_sessions_per_user=False`` default drops the participant from the
    key for ANY threaded group message -- ``gateway/session.py``'s
    ``build_session_key``), must never be able to write a stash record at all.

    Telegram's own DM invariant is ``chat_id == user_id`` for a private
    one-on-one chat with the bot; requiring BOTH ``chat_type == "dm"`` and that
    equality closes the group/thread session-key-collision class outright, at
    the source, independent of the turn-binding claim() logic in
    :class:`_OriginStash`.
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
    skips or rewrites the inbound event) and never raises -- a capture failure must
    never be able to drop or corrupt the human's message. Produces at most one
    PENDING (unclaimed) stash record per genuinely private, unforwarded, self-chat
    Telegram message; :func:`stamp_tool_call` is the only place that record is ever
    bound to a turn.
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
        origin = {
            "platform": platform,
            "user_id": str(user_id) if user_id is not None else None,
            "chat_id": str(chat_id) if chat_id is not None else None,
            "message_id": str(message_id) if message_id is not None else None,
            "timestamp": _isoformat(getattr(event, "timestamp", None)),
            # Recorded (not just enforced) so mupot can independently re-check the
            # same invariant this fence already applied at capture time.
            "chat_type": chat_type_value(source),
            "thread_id": str(thread_id) if thread_id is not None else None,
            "forwarded": False,  # _passes_trust_fence already refused any forwarded message
        }
        _STASH.capture(session_key, origin)
    except Exception:
        logger.warning("mupot plugin: human-origin capture failed", exc_info=True)
    return None


def chat_type_value(source: Any) -> Optional[str]:
    value = getattr(source, "chat_type", None)
    return value if isinstance(value, str) else (str(value) if value is not None else None)


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
    """True while this tool call is executing inside a delegated subagent
    (``tools/delegate_tool_child_run.py``'s ``_run_with_thread_capture``, which
    wraps the ENTIRE child conversation -- every tool call it makes -- in
    ``agent.delegation_context.delegated_child_context()``). Checked directly so
    a delegated child can never claim (or read) a human-origin record even in the
    narrow race where it happens to present a turn_id before the parent turn's
    own task_verdict/needs_you_list call does. Fails closed: an ImportError (the
    plain, non-native test suite; a very old Hermes) is treated as "cannot prove
    this is NOT a delegated child" the same way :func:`_passes_trust_fence`
    treats a missing raw_message -- but only for THIS narrow signal, so it never
    masks the primary turn_id-binding defense when the module is simply
    unavailable.
    """
    try:
        from agent.delegation_context import is_delegated_child_context
    except Exception:
        return False
    try:
        return bool(is_delegated_child_context())
    except Exception:
        return False


def stamp_tool_call(
    *, tool_name: str = "", args: Any = None, turn_id: str = "", **_kwargs: Any
) -> Optional[dict[str, Any]]:
    """``pre_tool_call`` hook.

    A model-supplied ``human_origin`` is ALWAYS treated as a forgery attempt: this
    field is harness-stamped only, so any value already present in ``args`` is
    logged at WARNING regardless of whether it happens to match the real origin.

    Returns a ``{"action": "modify", "args": {"human_origin": ...}}`` directive
    only when THIS EXACT turn (``turn_id``, received directly in ``pre_tool_call``'s
    own kwargs -- never the tool call's own ``session_id`` kwarg, which is
    ``agent.session_id``, a DB row id that can rotate on compression and is shared
    across a session's turns rather than unique per turn) successfully claims a
    pending origin for its session (see :class:`_OriginStash`). No ``turn_id``, no
    session_key, a delegated-subagent context, or nothing left in the pending
    queue for this session all resolve to "nothing to stamp" identically.

    When nothing is claimable, the ``modify`` contract cannot express "delete this
    key" (it only shallow-merges keys onto the original args), so a forged value is
    instead removed by mutating *args* in place -- the exact same dict object
    ``model_tools.py``'s ``_pre_dispatch_guards`` and ``agent/tool_executor.py``'s
    ``_pre_tool_block`` both fall back to using when no hook returns a ``modify``
    directive, so the removal is visible either way. Never raises: a hook failure
    must fail open (tool proceeds unstamped) rather than block a real human
    decision.
    """
    try:
        governed = _resolve_governed_tool_name(tool_name)
        if governed is None or not isinstance(args, dict):
            return None
        model_supplied = "human_origin" in args
        origin: Optional[dict[str, Any]] = None
        if turn_id and not _in_delegated_child_context():
            session_key = _current_session_key()
            if session_key:
                origin = _STASH.claim(session_key, turn_id)
        if model_supplied:
            logger.warning(
                "mupot plugin: model supplied human_origin directly for tool '%s' -- "
                "this field is harness-stamped only; treating as a forgery attempt and %s",
                tool_name,
                "overwriting it with the claimed origin" if origin is not None else "stripping it",
            )
        if origin is not None:
            return {"action": "modify", "args": {"human_origin": origin}}
        if model_supplied:
            args.pop("human_origin", None)
        return None
    except Exception:
        logger.warning("mupot plugin: human-origin stamping failed", exc_info=True)
        return None


def register(ctx: Any) -> None:
    """Wire both hooks. Called from the native gateway's own ``register()``
    (``mupot_gateway/adapter.py``), FIRST, before anything else is registered --
    these hooks are meaningless without a live platform adapter feeding
    ``pre_gateway_dispatch`` real ``MessageEvent``s, but more importantly: this is
    the ONLY choke point this plugin has for keeping a model-supplied
    ``human_origin`` from reaching mupot verbatim on ``task_verdict``/
    ``needs_you_list``. A Hermes runtime that cannot ``register_hook`` (or whose
    hook registration itself fails) gets NO native-gateway registration at all --
    refusing loudly beats leaving an unenforced identity-attestation surface
    running silently (kasra-review P1-1: the previous ``getattr(ctx,
    "register_tool", None)``-style optional-degrade pattern protects a
    convenience tool; it is not a precedent for an identity attestation).
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
        register_hook("pre_tool_call", stamp_tool_call)
    except Exception:
        logger.error("mupot plugin: human-origin hook registration failed", exc_info=True)
        raise

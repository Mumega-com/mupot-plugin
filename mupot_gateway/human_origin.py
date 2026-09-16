"""Harness-stamped human message origin for mupot's ``human_origin`` verdict field.

The human talks to their own agent in natural language over a platform (Telegram
today); the HARNESS -- never the LLM -- must be the thing that stamps the inbound
message's origin (platform, user id, chat id, message id, timestamp) onto the
agent's ``task_verdict``/``needs_you_list`` calls. Mupot resolves that origin to the
member and lets the call ride under the human's own identity instead of the agent
seat; without it, the call runs under the agent seat as today.

Two Hermes lifecycle hooks do the whole job:

* ``pre_gateway_dispatch`` (:func:`capture_human_origin`) fires once per inbound
  ``MessageEvent``, straight off the platform adapter, before the LLM ever sees the
  turn. It reads the native ids off ``event``/``event.source`` -- never off text --
  and stashes them keyed by the SAME ``session_key`` the gateway binds for the turn
  (``session_store._generate_session_key(source)``, see ``gateway/run.py``'s
  ``_set_session_env``).
* ``pre_tool_call`` (:func:`stamp_tool_call`) fires once per tool dispatch, deep
  inside the turn. It reads back the stashed origin via
  ``tools.approval_context.get_current_session_key()`` -- the SAME session_key,
  bound into a contextvar for the whole turn by ``gateway/run_turn.py``'s
  ``_set_session_env`` call, well before any tool executes -- and overwrites
  (never trusts) whatever ``human_origin`` the model itself supplied.

Only Telegram is supported for now. Every other platform is recorded (a one-time
log per platform name) as unsupported: ``task_verdict``/``needs_you_list`` calls
from those turns simply never carry a stashed origin and fall back to running
under the agent seat, exactly like a CLI turn, a cron turn, or a subagent turn.
"""

from __future__ import annotations

import logging
import time
from collections import OrderedDict
from typing import Any, Mapping, Optional

logger = logging.getLogger(__name__)

# Platforms the harness can currently attest a human origin for. Every other
# platform is a documented gap, not a silent one (see _warn_unsupported_platform).
SUPPORTED_PLATFORMS = frozenset({"telegram"})

# The plugin's own mupot tools that accept an optional ``human_origin`` object.
# Both are mupot MCP tools reachable directly from a Hermes turn (not wrapped by
# this plugin's mupot_operator.py action allowlist -- see module docstring in
# ``mupot_operator.py``'s ``build_operator_handlers``: every one of its handlers
# builds an explicit, hand-picked payload and none of them forward a raw
# ``human_origin`` field today), so this hook -- a global Hermes ``pre_tool_call``
# lifecycle hook that fires for every tool dispatch, not only this plugin's own
# registered tools -- is the only choke point that can see and stamp their args
# before they leave the process.
HUMAN_ORIGIN_TOOL_NAMES = frozenset({"task_verdict", "needs_you_list"})

# Bounded so a gateway that runs for weeks cannot grow this stash without limit:
# a turn that stashes an origin and is then abandoned (crash, restart, a chat the
# agent never replies in) must eventually fall out on its own. 512 mirrors the
# same cap gateway/run.py's own _cache_session_source uses for its session-source
# LRU. 30 minutes is generous for a slow multi-tool-call turn while still being
# far shorter than any realistic session lifetime.
_MAX_STASH_ENTRIES = 512
_STASH_TTL_SECONDS = 1800.0

_WARNED_UNSUPPORTED_PLATFORMS: set[str] = set()


class _OriginStash:
    """Bounded (size + TTL) ``session_key`` -> captured human-origin dict map.

    One turn stashes at most one entry (``pre_gateway_dispatch`` fires once per
    inbound message); ``pre_tool_call`` may read it back any number of times
    within that turn, so :meth:`get` does NOT pop on read -- only :meth:`put`'s
    size cap and :meth:`get`'s TTL check ever evict -- letting a turn that makes
    several ``task_verdict``/``needs_you_list`` calls stamp every one of them.
    """

    def __init__(
        self, max_entries: int = _MAX_STASH_ENTRIES, ttl_seconds: float = _STASH_TTL_SECONDS
    ) -> None:
        self._max_entries = max_entries
        self._ttl_seconds = ttl_seconds
        self._entries: "OrderedDict[str, tuple[float, dict[str, Any]]]" = OrderedDict()

    def put(self, session_key: str, origin: Mapping[str, Any]) -> None:
        if not session_key:
            return
        self._entries[session_key] = (time.monotonic(), dict(origin))
        self._entries.move_to_end(session_key)
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)

    def get(self, session_key: str) -> Optional[dict[str, Any]]:
        if not session_key:
            return None
        entry = self._entries.get(session_key)
        if entry is None:
            return None
        stamped_at, origin = entry
        if (time.monotonic() - stamped_at) > self._ttl_seconds:
            self._entries.pop(session_key, None)
            return None
        return dict(origin)

    def __len__(self) -> int:
        return len(self._entries)

    def clear(self) -> None:
        self._entries.clear()


# Process-global: one gateway process serves every concurrent session, exactly
# like adapter.py's own _ACTIVE_WATCHERS / gateway/run.py's _session_sources.
_STASH = _OriginStash()


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


def _session_key_for_source(gateway: Any, session_store: Any, source: Any) -> Optional[str]:
    """The exact key ``gateway/run.py``'s ``_set_session_env`` binds for this turn
    (verified against ``gateway/session_recovery.py``'s ``_generate_session_key``:
    same method, same object, called with the same ``source``). Deriving it any
    other way (e.g. reimplementing ``build_session_key`` here) risks drifting from
    the runner's own profile/group-session config and stashing under a key
    ``get_current_session_key()`` will never see."""
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
    never be able to drop or corrupt the human's message.
    """
    try:
        source = getattr(event, "source", None)
        if source is None:
            return None
        platform = _platform_name(source)
        if platform not in SUPPORTED_PLATFORMS:
            _warn_unsupported_platform_once(platform)
            return None
        session_key = _session_key_for_source(gateway, session_store, source)
        if not session_key:
            logger.debug("mupot plugin: no session_key resolvable; skipping human-origin capture")
            return None
        user_id = getattr(source, "user_id", None)
        chat_id = getattr(source, "chat_id", None)
        # event.message_id, NOT source.message_id: SessionSource.message_id is the
        # "triggering message (pin/reply/react)" reference, not this message's own id.
        message_id = getattr(event, "message_id", None)
        origin = {
            "platform": platform,
            "user_id": str(user_id) if user_id is not None else None,
            "chat_id": str(chat_id) if chat_id is not None else None,
            "message_id": str(message_id) if message_id is not None else None,
            "timestamp": _isoformat(getattr(event, "timestamp", None)),
        }
        _STASH.put(session_key, origin)
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


def stamp_tool_call(
    *, tool_name: str = "", args: Any = None, **_kwargs: Any
) -> Optional[dict[str, Any]]:
    """``pre_tool_call`` hook.

    A model-supplied ``human_origin`` is ALWAYS treated as a forgery attempt: this
    field is harness-stamped only, so any value already present in ``args`` is
    logged at WARNING regardless of whether it happens to match the real origin.

    Returns a ``{"action": "modify", "args": {"human_origin": ...}}`` directive
    when a captured origin exists for this turn's session (overwriting any
    model-supplied value). When none exists, the ``modify`` contract cannot express
    "delete this key" (it only shallow-merges keys onto the original args), so a
    forged value is instead removed by mutating *args* in place -- the exact same
    dict object ``model_tools.py``'s ``_pre_dispatch_guards`` and
    ``agent/tool_executor.py``'s ``_pre_tool_block`` both fall back to using when no
    hook returns a ``modify`` directive, so the removal is visible either way.
    Never raises: a hook failure must fail open (tool proceeds unstamped) rather
    than block a real human decision.
    """
    try:
        if tool_name not in HUMAN_ORIGIN_TOOL_NAMES or not isinstance(args, dict):
            return None
        model_supplied = "human_origin" in args
        session_key = _current_session_key()
        origin = _STASH.get(session_key) if session_key else None
        if model_supplied:
            logger.warning(
                "mupot plugin: model supplied human_origin directly for tool '%s' -- "
                "this field is harness-stamped only; treating as a forgery attempt and %s",
                tool_name,
                "overwriting it with the captured origin" if origin is not None else "stripping it",
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
    """Wire both hooks. Called only from the native gateway's own ``register()``
    (``mupot_gateway/adapter.py``), i.e. only when ``native_gateway_enabled`` is
    set -- these hooks are meaningless without a live platform adapter feeding
    ``pre_gateway_dispatch`` real ``MessageEvent``s.

    ``register_hook`` is optional on *ctx* (mirrors the ``register_tool =
    getattr(ctx, "register_tool", None)`` pattern this module's own ``register()``
    already uses for ``mupot_gateway_status``): a real Hermes ``PluginContext``
    always has it, but the minimal fakes several existing native tests build to
    exercise unrelated behavior (the gateway_status tool, platform registration)
    do not, and must not be forced to grow one just because this feature landed.
    """
    register_hook = getattr(ctx, "register_hook", None)
    if not callable(register_hook):
        logger.debug("mupot plugin: ctx has no register_hook; human-origin capture disabled")
        return
    register_hook("pre_gateway_dispatch", capture_human_origin)
    register_hook("pre_tool_call", stamp_tool_call)

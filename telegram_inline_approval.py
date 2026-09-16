"""Deterministic Telegram inline-button approval for a pending task verdict.

Companion to ``telegram_control.py``'s deterministic ``/needs``/``/approve``/
``/reject`` command relay: this module attaches an Approve / Reject / Details
inline keyboard to a pending-decision prompt sent to the owner's private
Telegram chat, and handles the resulting button press deterministically --
entirely outside the LLM, the same way the slash commands are a relay, not a
model tool call.

Design notes (why it looks the way it does):

* Callback data is an opaque one-time token ``mv:<nonce>`` (>=128 bits of
  entropy). The nonce is a bearer key into a plugin-side, in-process store
  that maps it to ``{task_id, verdict, chat_id, user_id, prompt_message_id,
  version, issued_at, expires_at}``. Task id and verdict are NEVER encoded in
  the callback data itself -- a forged or replayed update must not be able to
  infer, let alone redirect, a decision just by reading Telegram's own
  update.

* Claiming a token is CLAIM-OR-BURN, not read-then-write: ``_ApprovalTokenStore
  .claim()`` marks a record used atomically, under one lock, in the same
  critical section that checks it is unused and unexpired. This mirrors the
  hard-won lesson from mupot_gateway/human_origin.py's own adversarial-gate
  history (PR #13, rounds 3-4): a match-to-consume design that leaves the
  matched record alive when the match "succeeds" degenerates from an
  attestation into a bearer credential the instant two callers can race it.

* The fence (private chat, sender is the chat owner) is evaluated against the
  raw PTB ``CallbackQuery``, not through
  ``mupot_gateway.human_origin._passes_trust_fence`` -- that function's own
  docstring documents it fails closed (refuses) on a callback-query source
  because Telegram callback updates carry the RAW ``"private"`` chat-type
  literal, not Hermes's normalized ``"dm"``. This module's fence maps the raw
  literal at its own entry point instead of reusing a predicate written for a
  different (normalized) input shape.

* The verdict is submitted through :class:`mupot_operator.MupotOperatorClient`
  on the agent's own bearer token -- never through the LLM, never through a
  registered model tool. ``human_origin`` is built directly from the trusted
  Telegram callback fields (the callback's own ``from_user.id``/``chat.id``
  and the prompt message's id), not through human_origin.py's
  capture/bind-turn-custody pipeline (which exists to attest a *text message*
  that travels through an LLM turn; a button press has no such turn to bind
  to and needs none -- the callback_query itself is Telegram's own
  authenticated assertion of who pressed it).

* Feature is OFF by default (``telegram_inline_approval_enabled: false``).
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Mapping, Optional

logger = logging.getLogger(__name__)


CALLBACK_PREFIX = "mv:"
TOKEN_VERSION = 1
DEFAULT_TOKEN_TTL_SECONDS = 600.0
_MIN_TOKEN_TTL_SECONDS = 1.0
_MAX_TOKEN_TTL_SECONDS = 600.0
_NONCE_BYTES = 18  # secrets.token_urlsafe(18) ~ 144 bits of entropy
_MAX_PENDING_TOKENS = 512
_MAX_RECEIPTS = 500

VERDICTS = ("approve", "reject")
_ALL_BUTTON_KINDS = ("approve", "reject", "details")

_REFUSAL_TEXT: Mapping[str, str] = {
    "unknown": "This button is no longer valid.",
    "expired": "This decision has expired. Use /needs to see it again.",
    "used": "This decision was already made.",
    "version_mismatch": "This button is from an older message. Use /needs to see the current one.",
    "not_bound": "This button is no longer valid.",
    "fence": "This decision can only be made in your own private chat.",
}


def _isoformat_utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


# --------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class TelegramInlineApprovalSettings:
    enabled: bool = False
    token_ttl_seconds: float = DEFAULT_TOKEN_TTL_SECONDS
    receipts_path: str = ""

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "TelegramInlineApprovalSettings":
        enabled = value.get("telegram_inline_approval_enabled", False)
        if not isinstance(enabled, bool):
            raise ValueError("telegram_inline_approval_enabled must be a boolean")

        ttl_value = value.get(
            "telegram_inline_approval_token_ttl_seconds", DEFAULT_TOKEN_TTL_SECONDS
        )
        try:
            ttl_seconds = float(ttl_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "telegram_inline_approval_token_ttl_seconds must be numeric"
            ) from exc
        if (
            isinstance(ttl_value, bool)
            or not (_MIN_TOKEN_TTL_SECONDS <= ttl_seconds <= _MAX_TOKEN_TTL_SECONDS)
        ):
            raise ValueError(
                "telegram_inline_approval_token_ttl_seconds must be between "
                f"{_MIN_TOKEN_TTL_SECONDS} and {_MAX_TOKEN_TTL_SECONDS} seconds"
            )

        receipts_path = value.get("telegram_inline_approval_receipts_path", "")
        if not isinstance(receipts_path, str):
            raise ValueError("telegram_inline_approval_receipts_path must be a string")

        return cls(
            enabled=enabled,
            token_ttl_seconds=ttl_seconds,
            receipts_path=receipts_path.strip(),
        )


def _default_receipts_path() -> Path:
    try:
        from hermes_constants import get_hermes_home

        return get_hermes_home() / "platforms" / "mupot" / "telegram-inline-approval-receipts.json"
    except Exception:
        return Path(os.path.expanduser("~/.hermes/telegram-inline-approval-receipts.json"))


# --------------------------------------------------------------------------
# Durable receipt ledger
# --------------------------------------------------------------------------


class ApprovalReceiptStore:
    """Durable, atomically-written per-press provenance ledger.

    Deliberately re-implements ``mupot_gateway.adapter.StateStore``'s
    temp-file + fsync + rename shape rather than importing that module: the
    real ``adapter.py`` pulls in the full Hermes gateway import chain
    (``gateway.config``, ``httpx``, ...) at module scope, which this
    plugin's lighter-weight ``operator``-only mode (no native gateway) must
    not be forced to pay for.
    """

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path).expanduser()

    def load_checked(self) -> tuple[dict[str, Any], bool]:
        import json

        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
            return (value, True) if isinstance(value, dict) else ({}, False)
        except FileNotFoundError:
            return {}, True
        except (OSError, ValueError):
            return {}, False

    def save(self, value: dict[str, Any]) -> None:
        import json

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


def _persist_receipt(
    store: ApprovalReceiptStore,
    *,
    nonce: str,
    record: "_PendingApproval",
    applied: bool,
    reason: str,
    verdict_id: Optional[str],
) -> None:
    try:
        data, _valid = store.load_checked()
        receipts = data.get("receipts")
        if not isinstance(receipts, dict):
            receipts = {}
        receipts[nonce] = {
            "task_id": record.task_id,
            "verdict": record.verdict,
            "chat_id": record.chat_id,
            "user_id": record.user_id,
            "applied": applied,
            "reason": reason,
            "verdict_id": verdict_id,
            "recorded_at": _isoformat_utcnow(),
        }
        if len(receipts) > _MAX_RECEIPTS:
            for key in list(receipts.keys())[: len(receipts) - _MAX_RECEIPTS]:
                del receipts[key]
        data["receipts"] = receipts
        store.save(data)
    except Exception:
        logger.warning(
            "mupot plugin: inline-approval receipt persistence failed nonce=%s",
            nonce[:8],
            exc_info=True,
        )


# --------------------------------------------------------------------------
# Token store: claim-or-burn, single-use, TTL-bounded
# --------------------------------------------------------------------------


@dataclass
class _PendingApproval:
    task_id: str
    verdict: str
    chat_id: str
    user_id: str
    version: int
    issued_at: float
    expires_at: float
    prompt_message_id: Optional[str] = None
    used: bool = False


class _ApprovalTokenStore:
    """In-process, thread-safe, TTL-bounded, single-use nonce store.

    In-memory only (not persisted): a token's whole lifetime is <=10
    minutes, so surviving a gateway restart is not a requirement and would
    only widen the window an attacker could try to resurrect a stale token
    from disk. Durable provenance of what a token *did* once claimed lives
    in :class:`ApprovalReceiptStore`, which is a completely separate ledger.
    """

    def __init__(self, *, max_entries: int = _MAX_PENDING_TOKENS) -> None:
        self._lock = threading.Lock()
        self._entries: "OrderedDict[str, _PendingApproval]" = OrderedDict()
        self._max_entries = max_entries

    def mint_triplet(
        self,
        *,
        task_id: str,
        chat_id: Any,
        user_id: Any,
        ttl_seconds: float = DEFAULT_TOKEN_TTL_SECONDS,
    ) -> dict[str, str]:
        """Mint one fresh, unbound (no prompt_message_id yet) nonce per button
        kind. Call :meth:`bind_message` once the prompt has actually been sent
        and its message id is known, or :meth:`burn` if the send failed."""
        bounded_ttl = max(
            _MIN_TOKEN_TTL_SECONDS, min(_MAX_TOKEN_TTL_SECONDS, float(ttl_seconds))
        )
        now = time.monotonic()
        expires_at = now + bounded_ttl
        nonces: dict[str, str] = {}
        with self._lock:
            for verdict in _ALL_BUTTON_KINDS:
                nonce = secrets.token_urlsafe(_NONCE_BYTES)
                self._entries[nonce] = _PendingApproval(
                    task_id=str(task_id),
                    verdict=verdict,
                    chat_id=str(chat_id),
                    user_id=str(user_id),
                    version=TOKEN_VERSION,
                    issued_at=now,
                    expires_at=expires_at,
                )
                self._entries.move_to_end(nonce)
                nonces[verdict] = nonce
            self._evict_overflow_locked()
        return nonces

    def _evict_overflow_locked(self) -> None:
        while len(self._entries) > self._max_entries:
            nonce, record = self._entries.popitem(last=False)
            logger.warning(
                "mupot plugin: inline-approval token store overflow, evicting "
                "nonce=%s task_id=%s verdict=%s",
                nonce[:8],
                record.task_id,
                record.verdict,
            )

    def bind_message(self, nonces: Iterable[str], message_id: Any) -> None:
        with self._lock:
            for nonce in nonces:
                record = self._entries.get(nonce)
                if record is not None:
                    record.prompt_message_id = str(message_id)

    def burn(self, nonces: Iterable[str], *, reason: str) -> None:
        with self._lock:
            for nonce in nonces:
                record = self._entries.pop(nonce, None)
                if record is not None:
                    logger.warning(
                        "mupot plugin: inline-approval token burned before use "
                        "nonce=%s task_id=%s verdict=%s reason=%s",
                        nonce[:8],
                        record.task_id,
                        record.verdict,
                        reason,
                    )

    def claim(self, nonce: str) -> tuple[str, Optional[_PendingApproval]]:
        """Atomically validate-and-consume. Never returns a usable record twice
        for the same nonce -- the ``used`` flip happens inside the same locked
        section as every other check, so two concurrent presses of the same
        button can never both see ``"ok"``."""
        with self._lock:
            record = self._entries.get(nonce)
            if record is None:
                return "unknown", None
            if record.version != TOKEN_VERSION:
                return "version_mismatch", None
            if time.monotonic() >= record.expires_at:
                del self._entries[nonce]
                return "expired", None
            if record.used:
                return "used", None
            if record.prompt_message_id is None:
                return "not_bound", None
            record.used = True
            return "ok", record

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)


_STORE = _ApprovalTokenStore()


def _build_keyboard(nonces: Mapping[str, str]) -> Any:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Approve", callback_data=CALLBACK_PREFIX + nonces["approve"]
                ),
                InlineKeyboardButton(
                    "❌ Reject", callback_data=CALLBACK_PREFIX + nonces["reject"]
                ),
                InlineKeyboardButton(
                    "ℹ️ Details", callback_data=CALLBACK_PREFIX + nonces["details"]
                ),
            ]
        ]
    )


def build_approval_keyboard(
    *,
    task_id: str,
    chat_id: Any,
    user_id: Any,
    ttl_seconds: float = DEFAULT_TOKEN_TTL_SECONDS,
    store: _ApprovalTokenStore = _STORE,
) -> tuple[Any, Callable[[Optional[Any]], None]]:
    """Mint a fresh Approve/Reject/Details keyboard for *task_id*.

    Returns ``(reply_markup, on_sent)``. The caller MUST call
    ``on_sent(message_id)`` with the id of the message the keyboard was
    actually attached to (or ``on_sent(None)`` if the send failed) --
    :func:`_ApprovalTokenStore.claim` refuses every token until it is bound
    to a real prompt message id, since ``human_origin.message_id`` (the
    server's own one-decision-per-message replay key) must name that exact
    message.
    """
    nonces = store.mint_triplet(
        task_id=task_id, chat_id=chat_id, user_id=user_id, ttl_seconds=ttl_seconds
    )
    keyboard = _build_keyboard(nonces)

    def on_sent(message_id: Optional[Any]) -> None:
        if message_id is None:
            store.burn(nonces.values(), reason="send_returned_no_message_id")
        else:
            store.bind_message(nonces.values(), message_id)

    return keyboard, on_sent


async def send_approval_prompt(
    adapter_send: Callable[..., Awaitable[Any]],
    *,
    chat_id: Any,
    user_id: Any,
    task_id: str,
    text: str,
    ttl_seconds: float = DEFAULT_TOKEN_TTL_SECONDS,
    store: _ApprovalTokenStore = _STORE,
) -> Any:
    """Send *text* to *chat_id* with a fresh Approve/Reject/Details keyboard
    attached, binding the minted tokens to the resulting message id (or
    burning them if the send did not yield one)."""
    keyboard, on_sent = build_approval_keyboard(
        task_id=task_id, chat_id=chat_id, user_id=user_id, ttl_seconds=ttl_seconds, store=store
    )
    result = await adapter_send(chat_id=chat_id, text=text, reply_markup=keyboard)
    message_id = getattr(result, "message_id", None)
    if message_id is None and isinstance(result, Mapping):
        message_id = result.get("message_id")
    on_sent(message_id)
    return result


# --------------------------------------------------------------------------
# needs_you_list-driven keyboard for the /needs deterministic command
# --------------------------------------------------------------------------


def _single_pending_task_id(result: Any) -> Optional[str]:
    """Best-effort extraction of exactly one pending task id from a
    ``needs_you_list`` action result.

    Deliberately conservative: v1 only attaches a keyboard when there is
    EXACTLY one pending decision (the documented pilot flow -- "/needs ->
    /approve f9408956"). Any other shape (more than one task, zero tasks, an
    unrecognized payload shape) returns ``None`` and /needs falls back to its
    existing plain-text reply, unchanged. The exact `needs_you_list` result
    schema was not available to verify against a live server in this build
    (branch-only, no deploy) -- this reads the shapes that are consistent
    with every other operator action result seen in this plugin
    (``{"ok": true, "result": ...}``) and degrades to "no keyboard" rather
    than guessing on an unrecognized one.
    """
    if not isinstance(result, Mapping) or result.get("ok") is not True:
        return None
    payload = result.get("result")
    candidates: Any = None
    if isinstance(payload, list):
        candidates = payload
    elif isinstance(payload, Mapping):
        for key in ("tasks", "items", "results", "pending"):
            value = payload.get(key)
            if isinstance(value, list):
                candidates = value
                break
    if not isinstance(candidates, list) or len(candidates) != 1:
        return None
    entry = candidates[0]
    if not isinstance(entry, Mapping):
        return None
    task_id = entry.get("task_id") or entry.get("id")
    if not isinstance(task_id, str) or not task_id.strip():
        return None
    return task_id.strip()


async def maybe_build_needs_keyboard(
    client: Any,
    *,
    secret_owner: Any,
    update: Any,
    store: _ApprovalTokenStore = _STORE,
    ttl_seconds: float = DEFAULT_TOKEN_TTL_SECONDS,
) -> Optional[tuple[Any, Callable[[Optional[Any]], None]]]:
    """Build an Approve/Reject/Details keyboard for a ``/needs`` reply, when
    (and only when) exactly one task is pending. Never raises -- any failure
    degrades to ``None`` (plain-text reply only)."""

    def fetch() -> Any:
        if secret_owner is None:
            return client.call("needs_you_list", {})
        with secret_owner.activate():
            return client.call("needs_you_list", {})

    try:
        result = await asyncio.to_thread(fetch)
    except Exception:
        logger.warning(
            "mupot plugin: needs_you_list call for inline keyboard failed", exc_info=True
        )
        return None

    task_id = _single_pending_task_id(result)
    if task_id is None:
        return None

    chat = getattr(update, "effective_chat", None)
    user = getattr(update, "effective_user", None)
    chat_id = getattr(chat, "id", None)
    user_id = getattr(user, "id", None)
    if chat_id is None or user_id is None or str(user_id) != str(chat_id):
        return None

    return build_approval_keyboard(
        task_id=task_id, chat_id=chat_id, user_id=user_id, ttl_seconds=ttl_seconds, store=store
    )


# --------------------------------------------------------------------------
# Fence
# --------------------------------------------------------------------------


def _passes_callback_fence(query: Any) -> bool:
    """Private, self ("this chat is a DM with the presser") chat only.

    Telegram callback-query updates carry the RAW ``"private"`` chat-type
    literal (never Hermes's normalized ``"dm"``) -- see
    ``mupot_gateway.human_origin._passes_trust_fence``'s own docstring, which
    documents this exact gap for inline-button approvals. Mapped here, at
    this entry, instead of widening that function (written for a different,
    normalized input shape) to also accept the raw literal.
    """
    message = getattr(query, "message", None)
    chat = getattr(message, "chat", None) if message is not None else None
    if getattr(chat, "type", None) != "private":
        return False
    from_user = getattr(query, "from_user", None)
    user_id = getattr(from_user, "id", None)
    chat_id = getattr(chat, "id", None)
    if user_id is None or chat_id is None:
        return False
    return str(user_id) == str(chat_id)


# --------------------------------------------------------------------------
# Verdict submission
# --------------------------------------------------------------------------


def _origin_outcome(result: Any) -> tuple[bool, str]:
    if not isinstance(result, Mapping):
        return False, "invalid_response"
    if result.get("ok") is not True:
        return False, str(result.get("error") or "call_failed")
    inner = result.get("result")
    origin = inner.get("human_origin") if isinstance(inner, Mapping) else None
    if not isinstance(origin, Mapping):
        return False, "no_human_origin_in_response"
    applied = origin.get("applied") is True
    reason = str(origin.get("reason") or ("ok" if applied else "unknown"))
    return applied, reason


def _extract_verdict_id(result: Any) -> Optional[str]:
    if not isinstance(result, Mapping):
        return None
    inner = result.get("result")
    if not isinstance(inner, Mapping):
        return None
    value = inner.get("verdict_id") or inner.get("id")
    return str(value) if isinstance(value, (str, int)) else None


async def _submit_verdict(
    query: Any,
    record: _PendingApproval,
    nonce: str,
    *,
    client: Any,
    secret_owner: Any,
    store: ApprovalReceiptStore,
    answer: Callable[..., Awaitable[None]],
) -> None:
    verdict = record.verdict
    human_origin = {
        "channel": "telegram",
        "user_id": record.user_id,
        "chat_id": record.chat_id,
        "message_id": record.prompt_message_id,
        "message_at": _isoformat_utcnow(),
        "text": f"{verdict} {record.task_id}",
    }
    args = {"task_id": record.task_id, "verdict": verdict, "human_origin": human_origin}

    def submit() -> Any:
        if secret_owner is None:
            return client.call("task_verdict", args)
        with secret_owner.activate():
            return client.call("task_verdict", args)

    try:
        result = await asyncio.to_thread(submit)
    except Exception as exc:
        logger.warning(
            "mupot plugin: inline-approval task_verdict call raised nonce=%s "
            "task_id=%s verdict=%s error=%s",
            nonce[:8],
            record.task_id,
            verdict,
            type(exc).__name__,
        )
        result = {"ok": False, "error": "transport_error"}

    applied, reason = _origin_outcome(result)
    _persist_receipt(
        store,
        nonce=nonce,
        record=record,
        applied=applied,
        reason=reason,
        verdict_id=_extract_verdict_id(result),
    )

    if result.get("ok") is True and applied:
        await answer("Recorded.")
    else:
        logger.warning(
            "mupot plugin: inline-approval verdict not applied nonce=%s task_id=%s "
            "verdict=%s reason=%s",
            nonce[:8],
            record.task_id,
            verdict,
            reason,
        )
        await answer(
            "This couldn't be recorded -- it may already have a decision.",
            show_alert=True,
        )

    try:
        message = getattr(query, "message", None)
        if message is not None:
            await message.edit_reply_markup(reply_markup=None)
    except Exception:
        logger.warning(
            "mupot plugin: inline-approval reply-markup clear failed", exc_info=True
        )


def _details_text(record: _PendingApproval) -> str:
    return (
        f"Task {record.task_id}\n"
        "Use Approve or Reject on this message to decide. Details never "
        "changes anything."
    )


async def _handle_callback(
    update: Any,
    *,
    client: Any,
    secret_owner: Any,
    receipt_store: ApprovalReceiptStore,
    token_store: _ApprovalTokenStore = _STORE,
) -> None:
    query = getattr(update, "callback_query", None)
    if query is None:
        return

    data = getattr(query, "data", None)
    nonce = (
        data[len(CALLBACK_PREFIX):]
        if isinstance(data, str) and data.startswith(CALLBACK_PREFIX)
        else ""
    )

    async def _answer(text: Optional[str] = None, *, show_alert: bool = False) -> None:
        try:
            await query.answer(text=text, show_alert=show_alert)
        except Exception:
            logger.warning(
                "mupot plugin: inline-approval answerCallbackQuery failed", exc_info=True
            )

    if not nonce:
        logger.warning(
            "mupot plugin: inline-approval callback received with no recognizable "
            "token (forged or stripped callback data)"
        )
        await _answer(_REFUSAL_TEXT["unknown"], show_alert=True)
        return

    if not _passes_callback_fence(query):
        message = getattr(query, "message", None)
        chat = getattr(message, "chat", None) if message is not None else None
        from_user = getattr(query, "from_user", None)
        logger.warning(
            "mupot plugin: inline-approval callback refused by fence chat_id=%s "
            "chat_type=%s from_user_id=%s",
            getattr(chat, "id", None),
            getattr(chat, "type", None),
            getattr(from_user, "id", None),
        )
        await _answer(_REFUSAL_TEXT["fence"], show_alert=True)
        return

    status, record = token_store.claim(nonce)
    if status != "ok" or record is None:
        logger.warning(
            "mupot plugin: inline-approval callback refused nonce=%s reason=%s",
            nonce[:8],
            status,
        )
        await _answer(
            _REFUSAL_TEXT.get(status, "This decision is no longer available."),
            show_alert=True,
        )
        return

    if record.verdict == "details":
        await _answer(_details_text(record), show_alert=True)
        return

    await _submit_verdict(
        query,
        record,
        nonce,
        client=client,
        secret_owner=secret_owner,
        store=receipt_store,
        answer=_answer,
    )


# --------------------------------------------------------------------------
# Registration -- mirrors telegram_control.register_telegram_control's
# factory/unload shape exactly (same Hermes handler-ownership caveats apply).
# --------------------------------------------------------------------------


def register_telegram_inline_approval(
    ctx: Any,
    settings: TelegramInlineApprovalSettings,
    *,
    client: Any,
    secret_owner: Any = None,
    receipt_store: Optional[ApprovalReceiptStore] = None,
    token_store: _ApprovalTokenStore = _STORE,
) -> None:
    if not settings.enabled:
        return

    if receipt_store is None:
        path = settings.receipts_path or str(_default_receipts_path())
        receipt_store = ApprovalReceiptStore(path)

    wired_applications: list[tuple[Any, list[Any]]] = []

    def factory(application: Any, adapter: Any) -> None:
        from telegram.ext import CallbackQueryHandler

        if any(existing is application for existing, _ in wired_applications):
            return

        async def handle(update: Any, context: Any) -> None:
            await _handle_callback(
                update,
                client=client,
                secret_owner=secret_owner,
                receipt_store=receipt_store,
                token_store=token_store,
            )

        # Scoped pattern: PTB's CallbackQueryHandler(pattern=...) re.match()s
        # against callback_query.data. Hermes core registers its own
        # catch-all CallbackQueryHandler; an unscoped handler here would
        # swallow every core callback flow (PTB dispatches first-match per
        # handler group). "^mv:" ensures this handler only ever claims
        # updates this module itself minted the callback_data for.
        handler = CallbackQueryHandler(handle, pattern=f"^{CALLBACK_PREFIX}")
        application.add_handler(handler)
        wired_applications.append((application, [handler]))

    def unload() -> None:
        for application, handlers in wired_applications:
            for handler in handlers:
                application.remove_handler(handler, group=0)
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

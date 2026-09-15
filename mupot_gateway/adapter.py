from __future__ import annotations

import asyncio
import copy
import hashlib
import httpx
import json
import logging
import math
import os
import re
import secrets
import time
from collections import deque
from contextlib import nullcontext
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Awaitable, Callable, Iterable, Mapping, Optional

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    ProcessingOutcome,
    SendResult,
)
from gateway.session import SessionSource
from hermes_constants import get_hermes_home
from ..mupot_operator import validate_operator_identity
from ..profile_scope import (
    ProfileSecretOwner,
    read_profile_secret,
    require_supported_profile_runtime,
)
from .lease_ownership import (
    ATTEMPT_ID_RE as _LEASE_ATTEMPT_ID_RE,
    AckOwnershipError,
    attempt_ack_ownership,
    legacy_ack_ownership,
    validate_ack_ownership,
)

logger = logging.getLogger(__name__)


_ABSOLUTE_MCP_RESPONSE_LIMIT = 1024 * 1024
_SENSITIVE_MCP_RESPONSE_LIMIT = 64 * 1024
_SENSITIVE_MCP_TOOLS = frozenset({"send", "inbox_ack", "inbox_consumer_status"})
_GENERIC_MCP_PROTOCOL_ERROR = "Mupot MCP request failed"
_GENERIC_MCP_TRANSPORT_ERROR = "Mupot request failed"
_GENERIC_DELIVERY_CONTEXT_ERROR = "Mupot delivery context unavailable"
_DELIVERY_CONTEXT_METADATA_KEY = "_mupot_delivery_context"
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_REPLY_OUTBOX_VERSION = 2
_LEGACY_REPLY_OUTBOX_VERSION = 1
_LEASE_ATTEMPT_MARKER_VERSION = 3
# OPEN-D (kasra re-gate round 3, 2026-09-15): safety margin a configured
# `lease_seconds` must clear above `turn_timeout + mcp_tool_timeout`, same
# value the unconfigured default (see the constructor) already uses.
_LEASE_SAFETY_MARGIN_SECONDS = 60.0
# OPEN-D: the SERVER's own hard ceiling on `delivery_attempts` before it
# stops redelivering entirely (mupot/src/agents/messages.ts:879,
# `MAX_DELIVERY_ATTEMPTS`). An operator-configured `max_delivery_attempts`
# above this is not a bigger local retry budget -- it is dead configuration
# past attempt 5: the server never delivers a 6th attempt for this adapter
# to see, so `_resolve_turn_failure` would defer forever waiting for a
# `delivery_attempts` value that never arrives, with no local DLQ row and no
# `mupot_gateway_status` visibility (the exact "durable brick, just moved"
# shape this whole class of fix exists to close).
_SERVER_MAX_DELIVERY_ATTEMPTS = 5
# _LEASE_ATTEMPT_ID_RE itself is imported above from .lease_ownership (single shared
# pattern -- see the comment there) rather than redefined here.
_PROFILE_OWNER_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{64}$")
_STRANDED_NOTIFICATION_STATUSES = frozenset({"activation_unknown", "transport_unknown"})
_LEASE_ATTEMPT_STATES = frozenset(
    {"leased", "empty", "cancelled", "expired", "acked"}
)
_IMMUTABLE_REPLY_SOURCE_FIELDS = (
    "id",
    "seq",
    "tenant",
    "to_agent",
    "target_seat",
    "from_agent",
    "from_member",
    "kind",
    "body",
    "request_id",
    "in_reply_to",
    "created_at",
    "project_id",
    "fenced_delivery_id",
    "body_length",
    "checksum_sha256",
    "expects_reply",
    "reply_basis",
)


@dataclass(frozen=True, slots=True)
class DeliveryContext:
    """Immutable authority for one leased message processing generation."""

    source_id: str
    generation: int
    sender: str
    project: Optional[str]
    request_id: Optional[str]
    session_key: str
    attempt_id: Optional[str]


@dataclass(slots=True)
class _LiveDelivery:
    context: DeliveryContext
    expires_at: float
    source: Mapping[str, Any]
    # Class fix (2026-09-15, round 2, see _TurnFailureDeferred): `expires_at`
    # alone (the effective min of the two) cannot tell a genuine lease expiry
    # (item 1 -- turn NOT at fault) apart from a turn that outlived
    # `turn_timeout` while the message's own lease was still live (item 2 --
    # a bounded turn failure). Keep both raw deadlines so the timeout branch
    # in `_deliver` can compare them directly instead of guessing from the
    # single collapsed value.
    turn_deadline: float
    lease_deadline: Optional[float]
    completion_event: asyncio.Event = field(default_factory=asyncio.Event)
    outcome: Optional[ProcessingOutcome] = None
    invalidated: bool = False
    cancellation_started: bool = False


_delivery_context: ContextVar[Optional[DeliveryContext]] = ContextVar(
    "mupot_delivery_context",
    default=None,
)


class MupotProtocolError(RuntimeError):
    """A permanent, detail-free refusal of an invalid MCP response."""


class MupotTransportError(RuntimeError):
    """A detail-free transport failure whose server outcome may be unknown."""


class MupotSafeRetryError(RuntimeError):
    """A connection failure proven to occur before any request bytes were sent."""


class _DeliveryDeferred(Exception):
    """Base for "try again later", never a protocol violation.

    A pause, an outlived lease, a mid-turn race -- none of these are state
    transitions; they are TEMPORAL conditions. Folding any of them into
    `_protocol_error()` (which `_poll_loop` and `reconcile_inbox_polling`
    both treat as a permanent violation and answer with
    `_quarantine_inbox_polling()` -- a durable, `connect()`-refusing state
    that outlives the condition and requires manual
    `reconcile_inbox_polling()`) is the exact bug this base class exists to
    close as a CLASS rather than one named site at a time (see
    `_EstopDeferred`, added 2026-09-14 for e-stop; `_LeaseExpiredDeferred`,
    added 2026-09-15 after the SAME "temporal condition folded into a
    protocol violation" mechanism recurred for a message whose own
    visibility lease expired mid-turn).

    Every caller of `_process_leased_message` (the live poll loop and
    `reconcile_inbox_polling`) must recognise every subclass of this as
    "deferred, not violated": release the lease to expire for natural
    redelivery, and never fold it into "message not marked processed =>
    protocol error". Each subclass sets `reason` to a short human-readable
    string used only for logging at the catch site -- the raise site logs
    its own detailed context already.

    Round 4 (2026-09-15, Athena second eye, head c68e0839, P0 + MED "runbook
    :398"): **this is now the single canonical statement of the four-class
    model** -- every delivery attempt lands in exactly one row below, and
    `docs/telegram-onboarding-runbook.md` links here rather than restating
    it (a prior copy drifted: it said Failed-bounded left `pending` AS-IS
    below the cap and that the poll loop "keeps running either way" at the
    cap, both false once `_resolve_turn_failure` existed -- a table that
    lives in exactly one place, in the code the reader is already looking
    at to trust it, cannot drift again).

    | Outcome | Trigger | Durable representation | Who reads it |
    | --- | --- | --- | --- |
    | **Delivered** | Handler succeeds with human custody -- OR a `handler_error` whose error reply already reached the human WITH custody (`reply_outbox[id]` staged, non-`"complete"`, when `_resolve_turn_failure` runs -- Hermes's own crash handler calls `send()` before the outcome is known) | `processed` list + `reply_outbox[id].status="complete"`; `pending: null` once `_replay_reply_outbox` completes it on the NEXT tick (see `_TurnFailureDeferred`'s custody guard: `pending` is deliberately left set for exactly one more tick so that replay can match it) | `_process_leased_message`'s "already processed" short-circuit; operator via `processed`/`terminal_receipts` |
    | **Deferred** | Message's own lease expired before ACK (turn not at fault) | `pending: null` (cleared -- nothing here was ever candidate reply-outbox custody); no `lease_reconciliation` marker | Poll loop releases the lease and continues; server redelivers. `connect()`'s `_legacy_pending_ambiguous` check confirms `pending` is clean on restart |
    | **Failed-bounded** | Turn exceeds `turn_timeout` with the lease still live; handler returns no custody; handler raises WITHOUT the error reply ever reaching custody (`no_custody`/`handler_error`-without-custody, see the Delivered row for the custody case) | Below `max_delivery_attempts` (default `_SERVER_MAX_DELIVERY_ATTEMPTS`): `pending` cleared (same custody guard as above -- only stays set when a non-complete reply is staged), `_TurnFailureDeferred` raised, no ack. At the cap: `dlq` entry `{message, reason}` appended (idempotent), `pending` cleared (same guard), source is **NOT acked** -- an ack sets `read_at`, which the pot's own reaper requires `NULL` to ever set `dead_lettered_at` -- `_TurnFailureDeferred(terminal=True)` raised instead so the lease is left to expire naturally and the pot's own reaper (which runs on every subsequent `inbox_lease` call for this agent, before leasing) dead-letters it server-side | `adapter.turn_failure_dlq_summary()` / `mupot_gateway_status`'s `turn_failure_dlq` field; poll loop releases the lease and continues exactly like any other deferral -- it never blocks on this class |
    | **Violation** | Owner fingerprint mismatch, attempt/tenant conflict, tampered state, "processed" never set despite a successful ack (`_protocol_error()`) | `lease_reconciliation` marker (`required: true`, version 1/2/3) | `adapter.lease_reconciliation_status()` / `mupot_gateway_status`'s `lease_reconciliation` field; `connect()` refuses (self-heals once via `reconcile_inbox_polling()` for a genuine tombstone, else needs the manual runbook procedure) |
    """

    reason: str = "deferred"


class _EstopDeferred(_DeliveryDeferred):
    """Work was deferred because Hermes's global e-stop is engaged.

    A pause is a TEMPORAL condition, never a state transition: it must never
    surface as `_protocol_error()` (which the poll loop treats as a permanent
    protocol violation and answers by calling `_quarantine_inbox_polling()` --
    a durable, `connect()`-refusing state that outlives the pause and requires
    manual `reconcile_inbox_polling()`).

    Re-gate #3 (2026-09-14) found the round-1/2 fix gated only three NAMED
    functions (`_deliver`, `_handle_routine_event`, `_handle_ack_envelope`),
    while `_poll_loop` still ran `_replay_routine_events`/`_replay_reply_outbox`/
    `_flush_notifications` unconditionally every tick, and three more branches
    inside `_process_leased_message` (routine-events-disabled quarantine,
    already-processed re-ack, sender-policy DLQ) acked/committed without ever
    reaching a gated function at all -- each round fixed the named site, the
    next round found the next ungated one. The round-4 fix (this class) closes
    the CLASS instead of the next site: `_poll_loop` checks `_estop_engaged()`
    as its very first statement (nothing -- no replay, no flush, no lease --
    runs in a paused tick), and every remaining primitive that can consume a
    source or ship output to a human/peer is its own choke point that raises
    `_EstopDeferred` before touching any durable state or making any
    consuming/egress network call (read-only preflight
    `inbox_consumer_status`/`inbox_lease_reconcile` may still be issued --
    those never consume or ship anything). As of round 5 (kasra-review
    re-gate #4, 2026-09-14 -- see `_maybe_start_inbox_stream`'s `deliver()`
    in plugin/__init__.py) this covers every choke point on the two NATIVE
    receive/reply surfaces this class was built for -- the native gateway's
    own inbox lease consume/ack/commit and peer `send`, plus the legacy
    inbox-stream `deliver()`'s inject_message:
      - `_deliver`, `_handle_routine_event`, `_handle_ack_envelope` (unchanged
        from round 3 -- still needed for the mid-lease race and for tests that
        call them directly).
      - `_process_leased_message` itself, at its very first statement, before
        dispatching to ANY branch -- this is what closes the three branches
        above structurally: a future branch added to this dispatcher inherits
        the gate for free instead of needing its own copy.
      - `_refuse_ack_if_estop_engaged()`, called from both `_ack_expected` and
        `_ack_persisted_ownership` -- the only two functions that ever call
        `inbox_lease_ack`/`inbox_ack`. Every `_commit()` call site in this
        module is reached only immediately after one of these two succeeds,
        so gating the ack transitively gates the commit with no separate
        check needed.
      - `_transmit_final_reply`, before the peer `send` MCP call (used by both
        `_replay_reply_outbox` and the live `send()` interim path).
      - `notifications.flush()`, at each of its three sinks (activation
        injector, `deliver_text`, `mirror_text`) independently.
      - `plugin/__init__.py`'s legacy inbox-stream `deliver()` closure, its
        own choke point (config-exclusive with this module, see that
        function's docstring for why it drops rather than defers).

    (F2, kasra-review re-gate #5, 2026-09-14: this docstring previously said
    "every inject/consume/egress primitive in this plugin", which overstated
    the boundary. Explicitly NOT gated by any of the above, and not required
    to be:
      - `mupot_operator.py`'s `send` and `inbox` (with `consume=True`) tool
        handlers -- these are ordinary MCP tool calls a human's live session
        invokes directly, the same class of thing Hermes's own global e-stop
        does not stop for an ALREADY-RUNNING session (`hermes pause` blocks
        NEW cron/kanban/gateway-turn dispatch, per `agent/estop.py`'s module
        docstring; it was never a kill switch for tool calls mid-session).
        Gating a human-invoked tool call here would make `hermes pause` do
        something Hermes itself does not do anywhere else.
      - `plugin/__init__.py`'s provisioner `on_session_start` reminder
        (around line 129) -- a static, non-attacker-influenced string with no
        untrusted body in it, injected at most once per session; there is
        nothing here for a pause to defer.
      - Human `/approve` control traffic while paused is Hermes's own
        concern, not this plugin's: `agent/estop.py`'s `check_paused()` /
        the gateway's own turn dispatch gate the human's next turn, this
        class only gates the native-receive/legacy-inbox-stream surfaces
        listed above.)
    See `_DeliveryDeferred` for the shared contract every caller of
    `_process_leased_message` must honour for this and every sibling class.
    """

    reason = "Hermes global emergency stop is engaged"


class _LeaseExpiredDeferred(_DeliveryDeferred):
    """A leased message's own visibility lease expired before its turn finished.

    `lease_seconds` is derived from `turn_timeout` (plus margin), but a turn
    can legitimately run past that budget -- an MCP tool call blocking near
    its own timeout, plus normal agent-turn overhead -- with no protocol
    violation anywhere: Mupot's own visibility lease is exactly what makes
    this safe, redelivering the message once nothing acked it in time.

    Live incident (kayhermes gateway, plugin 6c86c2b0, 2026-09-15): an
    `mcp_tool` call blocked 180s inside a turn, the message's lease expired
    before the turn could ACK, and `_deliver` returned (via the
    `_expire_if_needed` early-check, or via the turn-timeout branch after
    `asyncio.wait_for`) WITHOUT raising -- so `_poll_loop`'s "message not in
    `processed`" check folded a routine, expected expiry into
    `_protocol_error()` -> `_quarantine_inbox_polling()`, a state that
    survived every subsequent operator restart because nothing ever called
    `reconcile_inbox_polling()` automatically (see `connect()`'s handling of
    `self._lease_quarantined`). SAME class as `_EstopDeferred`, same fix
    shape: raise this instead of returning, so `_poll_loop` and
    `reconcile_inbox_polling` release the lease and let the server redeliver
    instead of quarantining.

    Kasra gate round 2 (2026-09-15, head 5046ea79) BLOCK-1 + BLOCK-2:

    - BLOCK-1: `_deliver` wrote `pending = {"message": message}` at the top,
      BEFORE either raise site, and left it set on every deferral. On
      restart, the constructor's `_legacy_pending_ambiguous` check (a
      pending record whose source id is not staged in `reply_outbox` is
      "ambiguous legacy work") read that survivor as ambiguous crash state
      and `connect()` refused -- BEFORE the `_lease_quarantined` auto-
      reconcile branch even runs, since the pending-ambiguity check is
      earlier in `_connect_with_active_scope`. The brick moved from a
      `lease_reconciliation` marker to `pending`, silently. Fix: both raise
      sites now clear `pending` back to `None` before raising -- a lease
      expiry with no lease-bound turn work in flight is, unlike a real
      crash, NOT ambiguous: nothing here was ever a candidate for
      `reply_outbox` custody, so there is nothing to fence for a human.
    - BLOCK-2: this class now covers ONLY the case where the message's own
      lease deadline is the constraint that was hit -- compared explicitly
      against the turn's own deadline at the timeout site in `_deliver`
      (`_LiveDelivery.lease_deadline` vs `.turn_deadline`), not inferred
      from the single collapsed `expires_at`. A turn that exceeds
      `turn_timeout` while the lease is still live is a TURN failure, not a
      lease expiry -- see `_TurnFailureDeferred`, which is bounded by
      `delivery_attempts` instead of retried forever.

    See `_DeliveryDeferred` for the shared contract every caller of
    `_process_leased_message` must honour for this and every sibling class.
    """

    reason = "the message's own visibility lease expired before its turn completed"


class _TurnFailureDeferred(_DeliveryDeferred):
    """The AGENT TURN itself failed while the message's own lease was live.

    Kasra gate round 2 (2026-09-15, head 5046ea79) BLOCK-2 + WARN: three
    distinct outcomes were previously folded into either an unbounded
    `_LeaseExpiredDeferred` retry loop (turn_timeout with a live lease -- A/B
    measured 1 turn pre-fix vs 11+ and climbing, no operator signal) or a
    silent bare `return` that `_poll_loop` then read as "message not in
    processed" and answered with `_quarantine_inbox_polling()` (handler
    returned `None`/no custody; handler raised, surfaced by the base
    adapter as `ProcessingOutcome.FAILURE`). None of these is a lease
    expiry and none is a protocol violation -- they are TURN failures, and
    the fix bounds them by the message's own `delivery_attempts` (already
    validated non-negative-int by `validate_lease_attempt_result`, see
    module top) instead of either extreme:

      - `kind="turn_timeout"`: `_deliver`'s post-`asyncio.wait_for` timeout
        branch, when the LEASE deadline was not the binding constraint
        (`runtime.lease_deadline is None or runtime.lease_deadline >
        runtime.turn_deadline`) -- the turn hung past its own budget with
        plenty of lease left.
      - `kind="no_custody"`: the handler completed
        (`ProcessingOutcome.SUCCESS`) but produced nothing with human
        custody to commit -- previously a silent `return` with no signal.
      - `kind="handler_error"`: the base adapter recorded
        `ProcessingOutcome.FAILURE` (handler raised, caught by
        `BasePlatformAdapter`'s own `except BaseException` and turned into
        this outcome) -- previously the same silent `return`.

    Below `self.max_delivery_attempts`, `_deliver` raises this (see
    `_resolve_turn_failure`) so `_poll_loop`/`reconcile_inbox_polling`
    release the lease and let the server redeliver, same as any
    `_DeliveryDeferred`.

    Kasra re-gate round 3 (2026-09-15, head 7295e057) BLOCK-A: this raise
    used to leave `pending` set (the theory being that a hung or raising
    handler MAY have taken real side effects mid-turn, so it should fence
    for a human exactly like a genuine crash would -- previously pinned by
    `test_timeout_invalidates_generation_before_bounded_cancellation`).
    That reasoning does not hold: `pending` is the "a turn is running RIGHT
    NOW in this process" record and nothing else, `delivery_attempts` (the
    SERVER's count, not local state) is what tracks how many times this has
    been tried, and this raise site is only ever reached after the turn has
    already ended (successfully far enough to observe its outcome, or
    definitively timed out) -- there is no live turn left to describe.
    Leaving `pending` set instead reproduced the exact durable-brick shape
    this whole fix exists to close: the next restart's
    `_legacy_pending_ambiguous` check read the survivor as ambiguous crash
    state and `connect()` refused forever. Fix: `_resolve_turn_failure`
    clears `pending` before raising, for all three `kind`s, same as the
    lease-expiry sites.

    Round 4 (2026-09-15, Athena second eye, head c68e0839) P0: the round-3
    fix above cleared `pending` UNCONDITIONALLY, which reintroduced the
    exact same class of brick through a different door for `handler_error`
    specifically. Hermes's `BasePlatformAdapter` (`gateway/platforms/base.py`,
    pinned Hermes rev) catches a raising handler itself, BEFORE this
    function ever runs, and calls `_notify_turn_error` -> `self.send(...)`
    to tell the human the turn failed -- that call stages
    `reply_outbox[message_id]` at a non-`"complete"` status (`"custodied"`,
    once a real receipt exists) via the ordinary `_prepare_final_reply` /
    `_transmit_final_reply` path, same as any other reply. The round-3
    clear then wiped `pending` regardless, so the VERY NEXT poll tick's
    `_replay_reply_outbox` (statement #2 of every iteration, BEFORE the next
    `inbox_lease`) found a non-complete record whose source no longer
    matched `pending` (now `None`) and flipped it to
    `"reconciliation_required"` + raised a genuine `_protocol_error()` --
    poll dead, `connect()` refused on every subsequent restart at its FIRST
    gate, before `_is_lease_reconcile_reachable()` even runs. Kasra's
    decision (round 4): an error reply that already reached the human WITH
    custody is **Delivered**, not a crash to fence for one -- it must be
    allowed to finish through the ordinary reply-replay path, not raced
    into `reconciliation_required` by a premature `pending` clear. Fix:
    `_resolve_turn_failure` (both branches, below-cap and at-cap) clears
    `pending` UNLESS `reply_outbox` already has a non-`"complete"` entry
    staged for this exact `message_id` -- `_compute_legacy_pending_
    ambiguous`'s own predicate already treats a `pending` whose source IS
    staged as never ambiguous, so leaving it set here costs nothing: the
    next tick's `_replay_reply_outbox` matches it (same message dict, same
    `_reply_source_fingerprint`), transmits/completes the reply, and
    `_commit()` clears `pending` itself once the reply is genuinely done.
    `no_custody`/`turn_timeout` cannot reach `_notify_turn_error` (Hermes
    only calls it from its own handler-exception catch), so the guard is a
    no-op for those `kind`s -- correct, not merely harmless: nothing else in
    this codebase stages a reply for a source without also marking it
    `"complete"` on the same success path that also commits, so the guard
    can only ever hold `pending` for the genuine `handler_error`-with-
    custody race, never mask a real crash-ambiguity case.

    At the cap, `_deliver` still does not retry, but (round 4, MED "cap
    surface") it no longer acks: an ack sets `read_at`, and the pot's own
    reaper (`mupot/src/agents/messages.ts`, dead-letter step) requires
    `read_at IS NULL` to ever set `dead_lettered_at` on this row -- acking
    here moved the dead-letter FACT from the pot (migration 0090's purpose)
    to this host's local `dlq` list only. Instead: append the same `dlq`
    row (idempotent by message id, unchanged), apply the same custody
    guard, and raise this exception with `terminal=True` -- the lease is
    left to expire naturally, same as any other deferral, and the pot's own
    reaper (which runs on every subsequent `inbox_lease` call for this
    agent, before handing out a lease) dead-letters the row server-side the
    next time this adapter polls, once its own lease has expired. This
    cannot reintroduce unbounded local re-execution: `max_delivery_attempts`
    now defaults to `_SERVER_MAX_DELIVERY_ATTEMPTS`, so by the time this
    branch runs the server's own `leasable` predicate already excludes this
    row from ever being handed out again once the reaper marks it (and the
    reaper's own `delivery_attempts >= MAX_DELIVERY_ATTEMPTS` check is, by
    construction, already true here).
    """

    def __init__(self, message_id: str, kind: str, *, terminal: bool = False) -> None:
        super().__init__(message_id)
        self.kind = kind
        self.terminal = terminal
        self.reason = (
            f"turn failure ({kind}), at max_delivery_attempts -- deferred for "
            "the pot's own reaper to dead-letter, not locally acked"
            if terminal
            else f"turn failure ({kind}), attempt below max_delivery_attempts"
        )


_TURN_FAILURE_DLQ_REASONS = frozenset({"turn_timeout", "no_custody", "handler_error"})


def _protocol_error() -> MupotProtocolError:
    return MupotProtocolError(_GENERIC_MCP_PROTOCOL_ERROR)


def _decode_text_wrapper(value: Any, tool: str) -> Any:
    if not isinstance(value, str):
        raise _protocol_error()
    try:
        wrapper = json.loads(value)
    except (json.JSONDecodeError, UnicodeError, TypeError):
        raise _protocol_error() from None
    if (
        not isinstance(wrapper, dict)
        or set(wrapper) != {"ok", "tool", "result"}
        or wrapper.get("ok") is not True
        or wrapper.get("tool") != tool
    ):
        raise _protocol_error()
    return wrapper["result"]


def decode_mcp_result(
    payload: Any,
    request_id: int,
    tool: str,
    arguments: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Validate one correlated JSON-RPC tools/call result and unwrap its data."""
    if (
        not isinstance(payload, dict)
        or payload.get("jsonrpc") != "2.0"
        or type(payload.get("id")) is not int
        or payload.get("id") != request_id
    ):
        raise _protocol_error()

    has_result = "result" in payload
    has_error = "error" in payload
    if has_result == has_error or has_error:
        raise _protocol_error()

    result = payload.get("result")
    if not isinstance(result, dict):
        raise _protocol_error()
    if "isError" in result and result.get("isError") is not False:
        raise _protocol_error()

    has_content = "content" in result
    has_structured = "structuredContent" in result
    if not has_content and not has_structured:
        raise _protocol_error()

    text_value: Any = None
    if has_content:
        content = result.get("content")
        if not isinstance(content, list):
            raise _protocol_error()
        if len(content) == 0:
            if not has_structured:
                raise _protocol_error()
        elif len(content) == 1:
            item = content[0]
            if not isinstance(item, dict) or item.get("type") != "text":
                raise _protocol_error()
            text_value = _decode_text_wrapper(item.get("text"), tool)
        else:
            raise _protocol_error()

    structured_value = result.get("structuredContent") if has_structured else None
    if has_structured and not isinstance(structured_value, dict):
        raise _protocol_error()
    if text_value is not None and not isinstance(text_value, dict):
        raise _protocol_error()
    if text_value is not None and has_structured and text_value != structured_value:
        raise _protocol_error()

    value = structured_value if has_structured else text_value
    if not isinstance(value, dict):
        raise _protocol_error()
    if tool in {"inbox_lease", "inbox_lease_reconcile"}:
        attempt_id = (arguments or {}).get("attempt_id")
        if not isinstance(attempt_id, str):
            raise _protocol_error()
        return validate_lease_attempt_result(value, attempt_id)
    if tool == "inbox_lease_ack":
        attempt_id = (arguments or {}).get("attempt_id")
        if not isinstance(attempt_id, str):
            raise _protocol_error()
        return validate_lease_attempt_ack(value, attempt_id)
    return value


def validate_send_receipt(
    result: Any,
    to: str,
    project_id: Optional[str],
) -> dict[str, Any]:
    """Require proof that Mupot durably accepted the exact outbound message."""
    if not isinstance(result, dict):
        raise _protocol_error()
    delivery_id = result.get("id")
    sequence = result.get("seq")
    if not isinstance(delivery_id, str) or not delivery_id.strip():
        raise _protocol_error()
    if type(sequence) is not int or sequence <= 0:
        raise _protocol_error()
    if type(result.get("duplicate")) is not bool:
        raise _protocol_error()
    if result.get("to") != to:
        raise _protocol_error()
    if "project_id" not in result or result.get("project_id") != project_id:
        raise _protocol_error()
    return result


def validate_lease_attempt_result(
    result: Any,
    attempt_id: str,
    expected_scope: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    """Validate the server-authoritative outcome for one exact lease attempt."""
    if (
        not isinstance(result, dict)
        or set(result) != {
            "tenant",
            "agent_id",
            "effective_inbox_seat",
            "attempt_id",
            "state",
            "lease_expires_at",
            "messages",
            "consumed",
        }
        or not _LEASE_ATTEMPT_ID_RE.fullmatch(attempt_id)
        or result.get("attempt_id") != attempt_id
        or result.get("state") not in _LEASE_ATTEMPT_STATES
        or result.get("consumed") is not False
        or not isinstance(result.get("messages"), list)
    ):
        raise _protocol_error()
    scope = _attempt_scope_echo(result)
    if scope is None or expected_scope is not None and any(
        scope[field] != expected_scope.get(field)
        for field in ("tenant", "agent_id", "effective_inbox_seat")
    ):
        raise _protocol_error()
    state = result["state"]
    messages = result["messages"]
    lease_expires_at = result.get("lease_expires_at")
    if state != "leased":
        if lease_expires_at is not None or messages:
            raise _protocol_error()
        return result
    if (
        not isinstance(lease_expires_at, str)
        or not lease_expires_at.strip()
        or len(messages) != 1
        or not isinstance(messages[0], dict)
    ):
        raise _protocol_error()
    message = messages[0]
    if (
        not isinstance(message.get("id"), str)
        or not message["id"].strip()
        or type(message.get("seq")) is not int
        or message["seq"] <= 0
        or type(message.get("delivery_attempts")) is not int
        or message["delivery_attempts"] <= 0
        or message.get("lease_expires_at") != lease_expires_at
        or not isinstance(message.get("from_agent"), str)
        or not message["from_agent"].strip()
        or not isinstance(message.get("from_member"), str)
        or not message["from_member"].strip()
        or not isinstance(message.get("kind"), str)
        or not isinstance(message.get("body"), str)
        or not isinstance(message.get("created_at"), str)
        or not message["created_at"].strip()
    ):
        raise _protocol_error()
    return result


def validate_lease_attempt_ack(
    result: Any,
    attempt_id: str,
    expected_scope: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    if (
        not isinstance(result, dict)
        or set(result) != {
            "tenant",
            "agent_id",
            "effective_inbox_seat",
            "attempt_id",
            "state",
            "consumed",
        }
        or result.get("attempt_id") != attempt_id
        or not _LEASE_ATTEMPT_ID_RE.fullmatch(attempt_id)
        or result.get("state") not in _LEASE_ATTEMPT_STATES
        or type(result.get("consumed")) is not bool
    ):
        raise _protocol_error()
    scope = _attempt_scope_echo(result)
    if scope is None or expected_scope is not None and any(
        scope[field] != expected_scope.get(field)
        for field in ("tenant", "agent_id", "effective_inbox_seat")
    ):
        raise _protocol_error()
    return result


def _attempt_scope_echo(value: Any) -> Optional[dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    tenant = value.get("tenant")
    agent_id = value.get("agent_id")
    seat = value.get("effective_inbox_seat")
    if (
        not isinstance(tenant, str)
        or not tenant.strip()
        or tenant != tenant.strip()
        or not isinstance(agent_id, str)
        or not agent_id.strip()
        or agent_id != agent_id.strip()
        or not (
            seat is None
            or isinstance(seat, str) and bool(seat.strip()) and seat == seat.strip()
        )
    ):
        return None
    return {
        "tenant": tenant,
        "agent_id": agent_id,
        "effective_inbox_seat": seat,
    }


def _profile_owner_fingerprint(
    owner: Any,
    *,
    validate: bool = False,
) -> Optional[str]:
    if owner is None:
        try:
            owner = ProfileSecretOwner.from_active_home()
        except Exception:
            return None
    try:
        validated = getattr(owner, "validated_fingerprint", None)
        value = validated() if validate and callable(validated) else owner.fingerprint
    except Exception:
        return None
    if not isinstance(value, str) or not _PROFILE_OWNER_FINGERPRINT_RE.fullmatch(
        value
    ):
        return None
    return value


def _consumer_fence_proof(
    value: Any,
    expected_agent_id: Optional[str] = None,
    expected_tenant: Optional[str] = None,
    *,
    strict_scope: bool = False,
) -> Optional[dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    agent_id = value.get("agent_id")
    mode = value.get("mode")
    generation = value.get("generation")
    if (
        not isinstance(agent_id, str)
        or not agent_id.strip()
        or agent_id != agent_id.strip()
        or (expected_agent_id is not None and agent_id != expected_agent_id)
        or mode not in {"bearer_only", "gateway"}
        or type(generation) is not int
        or generation < 0
        or value.get("key_matches") is not True
    ):
        return None
    proof = {"agent_id": agent_id, "mode": mode, "generation": generation}
    if not strict_scope:
        return proof
    scope = _attempt_scope_echo(value)
    if (
        value.get("strict_scope") is not True
        or scope is None
        or expected_tenant is not None
        and scope["tenant"] != expected_tenant
    ):
        return None
    return {**scope, "mode": mode, "generation": generation}


def _lease_reconciliation_proof(value: Any) -> Optional[dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    version = value.get("version")
    common = {
        "version",
        "required",
        "agent_id",
        "mode",
        "generation",
    }
    if version == 1:
        if set(value) != common | {"reconcile_after"}:
            return None
    elif version == 2:
        if set(value) != common | {"attempt_id"}:
            return None
    elif version == _LEASE_ATTEMPT_MARKER_VERSION:
        if set(value) != common | {
            "tenant",
            "effective_inbox_seat",
            "profile_owner_fingerprint",
            "attempt_id",
        }:
            return None
    else:
        return None
    fence = _consumer_fence_proof(
        {
            "agent_id": value.get("agent_id"),
            "mode": value.get("mode"),
            "generation": value.get("generation"),
            "key_matches": True,
        }
    )
    if value.get("required") is not True or fence is None:
        return None
    if version in {2, _LEASE_ATTEMPT_MARKER_VERSION}:
        attempt_id = value.get("attempt_id")
        if not isinstance(attempt_id, str) or not _LEASE_ATTEMPT_ID_RE.fullmatch(
            attempt_id
        ):
            return None
        if version == 2:
            return {"version": version, **fence, "attempt_id": attempt_id}
        scope = _attempt_scope_echo(value)
        owner_fingerprint = value.get("profile_owner_fingerprint")
        if (
            scope is None
            or not isinstance(owner_fingerprint, str)
            or not _PROFILE_OWNER_FINGERPRINT_RE.fullmatch(owner_fingerprint)
        ):
            return None
        return {
            "version": version,
            **scope,
            "mode": fence["mode"],
            "generation": fence["generation"],
            "profile_owner_fingerprint": owner_fingerprint,
            "attempt_id": attempt_id,
        }

    deadline = value.get("reconcile_after")
    if (
        not isinstance(deadline, (int, float))
        or isinstance(deadline, bool)
        or not math.isfinite(deadline)
        or deadline < 0
    ):
        return None
    return {"version": version, **fence, "reconcile_after": float(deadline)}


def _response_limit(tool: str) -> int:
    if tool in _SENSITIVE_MCP_TOOLS:
        return _SENSITIVE_MCP_RESPONSE_LIMIT
    return _ABSOLUTE_MCP_RESPONSE_LIMIT


async def _read_mcp_response(response: httpx.Response, tool: str) -> Any:
    limit = _response_limit(tool)
    content_type = str(response.headers.get("content-type") or "")
    media_type = content_type.split(";", 1)[0].strip().lower()
    if not (
        media_type == "application/json"
        or (media_type.startswith("application/") and media_type.endswith("+json"))
    ):
        raise _protocol_error()

    encoding = str(response.headers.get("content-encoding") or "").strip().lower()
    if encoding and any(value.strip() != "identity" for value in encoding.split(",")):
        raise _protocol_error()

    declared: Optional[int] = None
    raw_length = response.headers.get("content-length")
    if raw_length is not None:
        text_length = str(raw_length).strip()
        if not text_length.isascii() or not text_length.isdigit():
            raise _protocol_error()
        declared = int(text_length)
        if declared > limit:
            raise _protocol_error()

    if response.status_code < 200 or response.status_code >= 300:
        raise _protocol_error()

    chunks: list[bytes] = []
    total = 0
    async for chunk in response.aiter_raw():
        total += len(chunk)
        if total > limit:
            raise _protocol_error()
        chunks.append(chunk)
    if declared is not None and total != declared:
        raise _protocol_error()

    try:
        return json.loads(b"".join(chunks).decode("utf-8"))
    except (json.JSONDecodeError, UnicodeError):
        raise _protocol_error() from None


def normalize_agent(value: Any) -> str:
    # Verified against Mumega-com/mupot src/agents/messages.ts (sendAgentMessage: fromAgent
    # is auth.boundAgentId, a raw member_tokens.agent_id UUID -- see src/mcp/index.ts) and
    # src/agents/inbox-routes.ts (the delivered from_agent column is that same value):
    # mupot never emits an "agent:"-prefixed from_agent. The strip below is defensive only
    # (for a deployer hand-typing allowed_agents entries as "agent:kasra"), a no-op against
    # real traffic, and must not be read as evidence mupot performs this normalization for
    # us. Lower-casing matters for real traffic: agents.slug is lowercase-only, and mupot's
    # own system-constant senders (e.g. "mupot-flights") are lowercase too.
    text = str(value or "").strip().lower()
    return text.split(":", 1)[1].strip() if text.startswith("agent:") else text


def should_accept_message(
    message: dict[str, Any], allowed_agents: Iterable[str]
) -> bool:
    sender = normalize_agent(message.get("from_agent"))
    allowed = {normalize_agent(value) for value in allowed_agents if normalize_agent(value)}
    return bool(sender and sender in allowed and str(message.get("body") or "").strip())


_ESTOP_IMPORT_WARNED = False

# Set once agent.estop.is_engaged() itself has raised (a genuine check
# failure, not a real pause) and _estop_engaged() has failed SAFE for it;
# cleared the next time a check succeeds, so a NEW span of failures warns
# again -- once per distinct failure window, not once per tick/poll.
_ESTOP_CHECK_FAILSAFE_WARNED = False

# Choke-point sites that have already logged an INFO line for the CURRENT
# pause window (kasra-review re-gate #4, 2026-09-14: 4 of the 12 gate sites
# refused silently -- _refuse_ack_if_estop_engaged, _transmit_final_reply,
# _process_leased_message's own top gate, and send()'s interim path).
# Cleared in full by _clear_estop_pause_log_sites() the next time
# _estop_engaged() observes the sentinel lifted, so a NEW pause logs again
# per site -- once per pause window, not once per message/tick.
_ESTOP_PAUSE_LOG_SITES: set[str] = set()


def _note_estop_pause_once(site: str, message: str, level: int = logging.INFO) -> None:
    """Log `message` at `level` the first time `site` refuses within this pause.

    A pause is one shared, global sentinel (`agent.estop`): a single paused
    window can refuse many messages across many choke points before it
    lifts. Logging unconditionally at every refusal floods the log the
    moment a burst of traffic (or a long pause) hits; logging nothing at all
    is what re-gate #4 flagged as 4 of 12 choke points refusing silently.
    This is the middle ground: once per site per pause window.

    `level` defaults to INFO (the original round-4 behaviour); F5/F6
    (kasra-review re-gate #5, 2026-09-14) added the WARNING call in `send()`
    for a refused FINAL reply specifically -- pass a distinct `site` for that
    call so it tracks its own once-per-window state independent of any INFO
    already logged for the same pause by another choke point.
    """
    if site in _ESTOP_PAUSE_LOG_SITES:
        return
    _ESTOP_PAUSE_LOG_SITES.add(site)
    logger.log(level, message)


def _clear_estop_pause_log_sites() -> None:
    _ESTOP_PAUSE_LOG_SITES.clear()


def _estop_engaged() -> bool:
    """Enforce Hermes's own global emergency stop for a mupot-originated turn.

    build_mupot_event always sets event.internal=True so a mupot turn queues as its own
    turn instead of interrupting/steering whatever the human is doing (Hermes's own
    busy-routing distinction). But at the pinned Hermes rev, gateway/run_inbound.py:174
    returns for any internal event before it ever reaches the global e-stop gate at :233
    -- `hermes pause` silently does not stop mupot traffic through that path. Rather than
    drop internal=True (which would also skip _is_user_authorized_for_source and route
    mupot's synthetic, unpaired sources through end-user auth they were never designed to
    satisfy), enforce the same property directly here.

    As of round 4 (kasra-review re-gate #3, 2026-09-14) this is checked at ALL consume/
    egress primitives, not a named list of functions -- see _EstopDeferred's docstring
    for the exhaustive, current list and why a per-site list kept going stale (rounds
    1-3 each fixed the previously-named sites and the next re-gate found the next
    ungated one). In short: the top of every `_poll_loop` iteration, `_deliver`,
    `_handle_routine_event`, `_handle_ack_envelope`, `_process_leased_message` itself,
    both ack primitives (`_ack_expected`/`_ack_persisted_ownership` via
    `_refuse_ack_if_estop_engaged`), the peer `send` choke point
    (`_transmit_final_reply`, plus the live `send()` interim path), and each of
    `notifications.flush()`'s three sinks (activation injector, `deliver_text`,
    `mirror_text`) independently.
    """
    global _ESTOP_IMPORT_WARNED, _ESTOP_CHECK_FAILSAFE_WARNED
    try:
        from agent.estop import is_engaged
    except ImportError:
        # Silent fail-open here would defeat `hermes pause` for all mupot traffic
        # without a trace. This is the NORMAL, expected path outside the native
        # Hermes runtime (the plain test.sh suite has no `agent` package at all),
        # so warn once per process rather than once per call -- every _deliver,
        # every routine event, and every notification flush would otherwise log.
        if not _ESTOP_IMPORT_WARNED:
            _ESTOP_IMPORT_WARNED = True
            logger.warning(
                "[mupot] agent.estop is not importable; failing OPEN (treating "
                "Hermes's global emergency stop as NOT engaged) for every mupot "
                "e-stop check until it becomes importable again. Expected outside "
                "the native Hermes runtime (e.g. scripts/test.sh); inside a native "
                "profile this means agent.estop has moved or been removed and "
                "`hermes pause` no longer stops mupot traffic."
            )
        return False
    try:
        engaged = bool(is_engaged())
    except Exception as exc:
        # P2 (kasra-review re-gate #4, 2026-09-14): this fail-safe returned
        # True with zero log records -- a real is_engaged() failure (as
        # opposed to a genuine pause) was indistinguishable from an ordinary
        # pause in the logs. Fail SAFE exactly as before (block dispatch
        # rather than silently let a mupot turn through while a global pause
        # cannot be confirmed lifted), but now say so: once per distinct
        # failure window (this is an error condition, not a tick/poll -- do
        # NOT log on every call while is_engaged() keeps failing), cleared
        # the next time a check succeeds so a NEW failure span warns again.
        if not _ESTOP_CHECK_FAILSAFE_WARNED:
            _ESTOP_CHECK_FAILSAFE_WARNED = True
            logger.warning(
                "[mupot] agent.estop.is_engaged() raised %s; failing SAFE "
                "(treating Hermes's global emergency stop as ENGAGED) until "
                "it succeeds again",
                type(exc).__name__,
            )
        return True
    _ESTOP_CHECK_FAILSAFE_WARNED = False
    if not engaged:
        _clear_estop_pause_log_sites()
    return engaged


def _refuse_ack_if_estop_engaged(message_id: str) -> None:
    """Shared choke point for every `inbox_lease_ack`/`inbox_ack` call site.

    `_ack_expected` and `_ack_persisted_ownership` are the only two functions
    in this module that ever call `inbox_lease_ack` or `inbox_ack` (round 3
    gated `_deliver`/`_handle_routine_event`/`_handle_ack_envelope` themselves,
    but three OTHER call sites inside `_process_leased_message` -- the
    routine-events-disabled quarantine branch, the already-processed re-ack
    branch, and the sender-policy DLQ branch -- call `_ack_expected` directly
    and never passed through any of those three). Every `_commit()` call site
    in this module is reached only immediately after one of these two ack
    functions succeeds, so refusing here transitively refuses the commit too
    -- no separate check is needed in `_commit` itself. Mutating this one
    function is therefore the single place that can silently reopen every
    ack/commit call site in the module at once, which is deliberate: one
    property, one guard, not N copies that can individually drift stale.
    """
    if _estop_engaged():
        # P2 (kasra-review re-gate #4, 2026-09-14): this choke point refused
        # silently -- an operator watching the log had no signal that an ack
        # was being held back for a pause specifically, as opposed to any
        # other reason a message might not yet be acked. Once per pause
        # window (not once per refused message): every ack this module ever
        # attempts funnels through here, so a busy paused window could
        # otherwise log once per message.
        _note_estop_pause_once(
            "_refuse_ack_if_estop_engaged",
            "[mupot] refusing ack/commit: Hermes global emergency stop is engaged",
        )
        raise _EstopDeferred(message_id)


def is_ack_envelope(message: dict[str, Any]) -> bool:
    return str(message.get("kind") or "").strip().lower() == "ack"


def is_terminal_ack(message: dict[str, Any]) -> bool:
    """Recognize a complete authenticated terminal ACK envelope."""
    return (
        is_ack_envelope(message)
        and message.get("expects_reply") is False
        and bool(str(message.get("id") or "").strip())
    )


def _reply_source_fingerprint(source: Mapping[str, Any]) -> str:
    stable = {name: source.get(name) for name in _IMMUTABLE_REPLY_SOURCE_FIELDS}
    payload = json.dumps(
        stable,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _final_request_id(source_id: str) -> str:
    candidate = f"resp-{source_id}"
    if _REQUEST_ID_RE.fullmatch(candidate):
        return candidate
    digest = hashlib.sha256(source_id.encode("utf-8")).hexdigest()
    return f"resp-{digest}"


def _progress_request_id(source_id: str, body: str) -> str:
    digest = hashlib.sha256((source_id + body).encode("utf-8")).hexdigest()
    return f"prog-{digest}"


def _platform_for_mupot() -> Platform:
    try:
        return Platform("mupot")
    except ValueError:
        return Platform.LOCAL


def build_mupot_event(
    message: dict[str, Any], platform: Optional[Platform] = None
) -> MessageEvent:
    sender = normalize_agent(message.get("from_agent"))
    project_id = str(message.get("project_id") or "").strip() or None
    message_id = str(message.get("id") or "")
    source = SessionSource(
        platform=platform or _platform_for_mupot(),
        chat_id=sender,
        chat_name=f"Mupot agent {sender}",
        chat_type="dm",
        user_id=sender,
        user_name=sender,
        # Project events share a governed project session. Unscoped control
        # messages receive an isolated session so an old/stalled DM turn
        # cannot redirect or absorb a new command.
        thread_id=project_id or message_id,
        scope_id="mupot",
        message_id=message_id,
    )
    return MessageEvent(
        text=str(message.get("body") or ""),
        message_type=MessageType.TEXT,
        source=source,
        raw_message=message,
        message_id=message_id,
        # Mark internal for Hermes busy-routing only: queue this as a distinct turn
        # instead of interrupting/steering another command and emitting a misleading
        # busy response as the correlated ACK. This does NOT mean the message already
        # passed Hermes's own authorization/e-stop gates -- at the pinned Hermes rev,
        # internal=True makes gateway/run_inbound.py:174 skip both of those entirely
        # (see _estop_engaged's docstring). should_accept_message's allowlist check
        # (run in _process_leased_message before _deliver is ever reached) and the
        # explicit _estop_engaged() check in _deliver are what actually gate this.
        internal=True,
        metadata={
            "seq": message.get("seq"),
            "request_id": message.get("request_id"),
            "in_reply_to": message.get("in_reply_to"),
            "project_id": project_id,
            "mupot_message_id": message_id,
        },
    )


class StateStore:
    def __init__(self, path: Path):
        self.path = Path(path).expanduser()

    def load(self) -> dict[str, Any]:
        value, _valid = self.load_checked()
        return value

    def load_checked(self) -> tuple[dict[str, Any], bool]:
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
            return (value, True) if isinstance(value, dict) else ({}, False)
        except FileNotFoundError:
            return {}, True
        except (json.JSONDecodeError, OSError):
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
            directory_flag = getattr(os, "O_DIRECTORY", None)
            if directory_flag is not None:
                directory_fd = os.open(
                    self.path.parent,
                    os.O_RDONLY | directory_flag,
                )
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


# OPEN-E (kasra re-gate round 3, 2026-09-15): the ONE fallback value for
# every way `_configured_mcp_tool_timeout` can fail to learn a real,
# operator-configured number -- the server absent from `mcp_servers`
# entirely, present but missing an explicit `timeout` key, or any exception
# reading/resolving the config at all. These used to be two DIFFERENT
# hardcoded numbers (300.0 for "absent"/"any exception", 30.0 for
# "configured but no timeout key") with no reasoning tying them together --
# picking the SMALLER of the two as a per-server default risks under-sizing
# `lease_seconds` (see OPEN-D) for a server an operator only partially
# configured, reproducing the exact class of incident this whole fix exists
# to close. One named constant, the LARGER/safer of the two former values.
_DEFAULT_MCP_TOOL_TIMEOUT_SECONDS = 300.0


def _configured_mcp_tool_timeout(server_name: str) -> float:
    """Read the mupot MCP server's own configured request timeout.

    Item 7 (kasra-review re-gate round 2, 2026-09-15): the SAME source
    `HermesMCPClient._ensure_client` (below) uses to build its httpx
    client's timeout, via `load_config()["mcp_servers"][server_name]` and
    `_resolve_mcp_server_config`. Used as `MupotAdapter`'s default for
    `mcp_tool_timeout` (lease sizing, see the constructor) instead of a
    hardcoded guess -- an operator can still override via `mcp_tool_timeout`
    in `extra`. Any failure here (server not yet configured, config file
    unreadable, import error in a unit-test sandbox with no `hermes_cli`
    package on the path) is non-fatal: falls back to
    `_DEFAULT_MCP_TOOL_TIMEOUT_SECONDS`.

    Scope (OPEN-E): this reads ONLY the mupot MCP server's own budget
    (`self.server_name`, i.e. `extra["mcp_server"]`, default `"mupot"`) --
    not any other MCP server's configured timeout, and not Hermes's own
    global default MCP timeout if one exists. An operator running several
    MCP servers with different budgets gets lease sizing based on the one
    this adapter actually calls through.
    """
    try:
        from hermes_cli.config import load_config
        from hermes_cli.mcp_config import _resolve_mcp_server_config

        raw_cfg = (load_config().get("mcp_servers") or {}).get(server_name)
        if not isinstance(raw_cfg, dict):
            return _DEFAULT_MCP_TOOL_TIMEOUT_SECONDS
        cfg = _resolve_mcp_server_config(raw_cfg)
        return float(cfg.get("timeout") or _DEFAULT_MCP_TOOL_TIMEOUT_SECONDS)
    except Exception:
        return _DEFAULT_MCP_TOOL_TIMEOUT_SECONDS


class HermesMCPClient:
    def __init__(
        self,
        server_name: str,
        *,
        secret_owner: ProfileSecretOwner | None = None,
    ):
        self.server_name = server_name
        self._secret_owner = secret_owner
        self._client: Optional[httpx.AsyncClient] = None
        self._url: Optional[str] = None
        self._headers: dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._request_id = 0

    def _profile_scope(self):
        if self._secret_owner is None:
            return nullcontext()
        return self._secret_owner.activate()

    async def _ensure_client_locked(self) -> None:
        with self._profile_scope():
            self._ensure_client()

    def _ensure_client(self) -> None:
        require_supported_profile_runtime({})
        # Validate availability at native connect as well as at request time.
        # The value is deliberately not cached here; call() observes rotation.
        read_profile_secret("MUPOT_AGENT_TOKEN")
        if self._client is None or self._client.is_closed:
            from hermes_cli.config import load_config
            from hermes_cli.mcp_config import _resolve_mcp_server_config

            raw_cfg = (load_config().get("mcp_servers") or {}).get(self.server_name)
            if not isinstance(raw_cfg, dict):
                raise RuntimeError(f"MCP server {self.server_name!r} is not configured")
            cfg = _resolve_mcp_server_config(raw_cfg)
            self._url = cfg.get("url")
            if not self._url:
                raise RuntimeError(f"MCP server {self.server_name!r} missing url")
            self._headers = dict(cfg.get("headers") or {})
            # Authorization is always supplied per request from Hermes's active
            # profile scope. Never retain a resolved/global fallback header.
            self._headers = {
                name: value
                for name, value in self._headers.items()
                if name.lower() not in {"authorization", "accept-encoding"}
            }
            timeout_sec = float(cfg.get("timeout") or 30.0)
            self._client = httpx.AsyncClient(
                headers=self._headers,
                timeout=timeout_sec,
            )

    async def connect(self) -> None:
        async with self._lock:
            await self._ensure_client_locked()

    async def call(self, tool: str, arguments: dict[str, Any]) -> Any:
        async with self._lock:
            with self._profile_scope():
                self._ensure_client()
                self._request_id += 1
                req_id = self._request_id
                body = {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "method": "tools/call",
                    "params": {
                        "name": tool,
                        "arguments": arguments,
                    },
                }
                token = read_profile_secret("MUPOT_AGENT_TOKEN")
                headers = {
                    **self._headers,
                    "Authorization": f"Bearer {token}",
                    "Accept-Encoding": "identity",
                }
                client = self._client
                url = self._url
                if client is None or url is None:
                    raise MupotTransportError(_GENERIC_MCP_TRANSPORT_ERROR)
                try:
                    async with client.stream(
                        "POST", url, json=body, headers=headers
                    ) as response:
                        payload = await _read_mcp_response(response, tool)
                except MupotProtocolError:
                    raise
                except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
                    logger.warning("[mupot] MCP connection failed before request send")
                    try:
                        await client.aclose()
                    except Exception:
                        pass
                    self._client = None
                    raise MupotSafeRetryError(_GENERIC_MCP_TRANSPORT_ERROR) from None
                except Exception:
                    logger.warning("[mupot] MCP request failed; resetting client")
                    try:
                        await client.aclose()
                    except Exception:
                        pass
                    self._client = None
                    raise MupotTransportError(_GENERIC_MCP_TRANSPORT_ERROR) from None

                return decode_mcp_result(payload, req_id, tool, arguments)

    async def close(self) -> None:
        async with self._lock:
            client, self._client = self._client, None
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    pass


class MupotAdapter(BasePlatformAdapter):
    supports_async_delivery = True
    interactive_resume = False

    def __init__(
        self,
        config: PlatformConfig,
        client_factory: Callable[[str], Any] = HermesMCPClient,
        message_injector: Optional[Callable[..., bool]] = None,
        secret_owner: ProfileSecretOwner | None = None,
    ) -> None:
        super().__init__(config, _platform_for_mupot())
        extra = config.extra or {}
        allowed = extra.get("allowed_agents")
        if allowed is None:
            # Key genuinely absent (not configured) -- fall back to the historical
            # default operator roster. An explicit empty value ("" or []) means
            # deny-all and must NOT fall through to this default: `x or DEFAULT`
            # previously treated "configured empty" the same as "not configured",
            # which fails OPEN to the default four agents instead of closed.
            allowed = "hadi-codex,hadi-codex-cli,kasra,hermes"
        if isinstance(allowed, str):
            allowed = [item.strip() for item in allowed.split(",")]
        elif not isinstance(allowed, (list, tuple, set, frozenset)):
            # A non-iterable value (int, bool, dict, ...) previously fell straight
            # into `for item in allowed` below and raised a raw, unhelpful
            # TypeError. Match the style already used for native_gateway_enabled /
            # routine_events_enabled elsewhere in this constructor: a clear
            # config-error message at the point of misconfiguration.
            raise ValueError(
                "allowed_agents must be a comma-separated string or a list of "
                f"agent names, got {type(allowed).__name__}"
            )
        # P3 hardening (kasra-review re-gate #2, 2026-09-14): a non-string entry
        # inside an otherwise-valid list (e.g. [123, "kasra"]) previously reached
        # normalize_agent()'s `str(value or "")`, which silently stringifies it
        # into a plausible-looking agent name instead of failing the
        # misconfiguration loudly, same class as the non-iterable check above.
        for item in allowed:
            if not isinstance(item, str):
                raise ValueError(
                    "allowed_agents entries must be strings, got "
                    f"{type(item).__name__}: {item!r}"
                )
        self.allowed_agents = {
            normalize_agent(item) for item in allowed if normalize_agent(item)
        }
        # NOTE on internal=True (see build_mupot_event): at the pinned Hermes rev,
        # gateway/run_inbound.py:174 returns for any event.internal before it ever
        # reaches _is_user_authorized_for_source (:185) or the global e-stop gate
        # (:233) -- Hermes's own authorization and `hermes pause` never run for a
        # mupot turn. `self.allowed_agents` (should_accept_message, checked before
        # _deliver is ever called) is therefore the ONLY sender fence, and the
        # e-stop is enforced explicitly at every call site that reaches a source
        # ACK/consume or a human-session injection (see _estop_engaged) instead of
        # relying on Hermes's bypassed gate. There used to be a stale
        # `extra["allow_from"] = sorted(self.allowed_agents)` here implying Hermes
        # consults a second, canonical copy of this allowlist. That is FALSE as a
        # global claim: gateway/authz_mixin.py's _adapter_extra_allowlist_authorizes,
        # gateway/config_loader.py, and gateway/pairing.py all read extra["allow_from"]
        # at the pinned rev. The claim only holds SCOPED to this adapter's own call
        # path: that reader lives on the non-internal auth branch, and
        # build_mupot_event always sets internal=True, which skips that branch
        # entirely today. If internal=True is ever dropped from build_mupot_event,
        # this allowlist must be revisited -- Hermes would then consult
        # extra["allow_from"] on its own authz path and a caller-supplied identity
        # could ride in through it. The dead-line removal above is correct only
        # because of that scoping, not because the tree-wide grep is empty.
        self.server_name = str(extra.get("mcp_server") or "mupot")
        # Round 3 (kasra re-gate BLOCK-C): set BEFORE the `mcp_tool_timeout`
        # resolution below, which needs `self._profile_scope()` to read
        # `_resolve_mcp_server_config` inside the correct (possibly
        # multi-tenant) secret scope rather than the ambient process one.
        self._secret_owner = secret_owner
        self.expected_agent_id = extra.get("expected_agent_id")
        self.expected_tenant = extra.get("expected_tenant")
        self.poll_interval = max(0.01, float(extra.get("poll_interval") or 2.0))
        self.rpc_timeout = max(5.0, float(extra.get("rpc_timeout") or 20.0))
        self.turn_timeout = max(10.0, float(extra.get("turn_timeout") or 300.0))
        self.cancel_timeout = max(0.01, float(extra.get("cancel_timeout") or 6.0))
        # Lease sizing (class fix, 2026-09-15, see _LeaseExpiredDeferred): a turn's
        # REAL maximum duration is not just `turn_timeout` -- a turn can spend up
        # to the mupot MCP server's own per-call request budget (the kayhermes
        # incident this closes ran with it configured to 180s) blocked inside a
        # single tool call, on top of ordinary turn overhead. `lease_seconds =
        # turn_timeout + 60` alone (the pre-fix default) is smaller than
        # `turn_timeout + mcp_tool_timeout` whenever `mcp_tool_timeout > 60`, so
        # a single slow-but-legitimate tool call could always outlive the lease.
        #
        # HOWEVER (item 2, kasra-review re-gate round 2, 2026-09-15) --
        # `turn_timeout`, not the lease, is the constraint that actually binds a
        # slow-tool-call turn in practice: `_deliver`'s timeout branch classifies
        # a timeout by comparing the LEASE's own deadline against `turn_timeout`'s
        # deadline (see `_LiveDelivery.lease_deadline`/`.turn_deadline` and
        # `_lease_is_binding`) -- sizing the lease generously here just means
        # `turn_timeout` reaches its own deadline FIRST in the common case, which
        # is now a bounded turn failure (`_TurnFailureDeferred`, kind
        # "turn_timeout"), not a lease expiry. A turn that legitimately needs
        # longer than `turn_timeout` for ordinary work needs `turn_timeout`
        # raised, not a bigger lease -- the lease only needs to outlive
        # `turn_timeout` by a safety margin so a merely-slow (not hung) turn
        # completes and ACKs before the SERVER, independently, redelivers it out
        # from under the still-running turn.
        #
        # Item 7 (kasra-review re-gate round 2, 2026-09-15): the round-1 fix's
        # own comment here claimed this adapter "has no way to read [the
        # Hermes-side MCP timeout] directly" and hardcoded a 300s default --
        # false. `HermesMCPClient._ensure_client` (below) already reads the
        # mupot MCP server's own configured `timeout` via
        # `load_config()["mcp_servers"][name]`; `_configured_mcp_tool_timeout`
        # reuses that exact path so the default here reflects the ACTUAL
        # configured per-call budget for this deployment, not a guess an
        # operator has to notice and override by hand. An explicit
        # `mcp_tool_timeout` in `extra` still wins when an operator wants to
        # size the lease against something other than the raw MCP client
        # timeout (e.g. a known-slower specific tool).
        #
        # Round 3 (kasra re-gate BLOCK-C): only resolved from config (never
        # called at all when `extra["mcp_tool_timeout"]` is already
        # explicit) and, when it IS resolved, done inside `self._profile_
        # scope()` -- `_configured_mcp_tool_timeout` -> `load_config()` ->
        # `hermes_cli.mcp_config._resolve_mcp_server_config` loads secrets
        # from `.env` into `os.environ` as a side effect (confirmed:
        # `MUPOT_AGENT_TOKEN` appeared in `os.environ` after construction,
        # even under `scripts/test.sh`'s own `unset MUPOT_AGENT_TOKEN ...`).
        # Calling this OUTSIDE any profile scope reads the AMBIENT Hermes
        # home (`~/.hermes`, not necessarily this adapter's own scoped
        # `secret_owner`) and leaves whatever it loads sitting in
        # `os.environ` permanently -- `HermesMCPClient._ensure_client`
        # already gets this right via its own `_profile_scope()`; this
        # constructor read was the one place that didn't.
        explicit_mcp_tool_timeout = extra.get("mcp_tool_timeout")
        if explicit_mcp_tool_timeout:
            resolved_mcp_tool_timeout = explicit_mcp_tool_timeout
        else:
            # `self._profile_scope()` entering (`secret_owner.activate()`)
            # can itself raise -- same class of failure
            # `_configured_mcp_tool_timeout`'s own try/except already
            # treats as non-fatal (falls back to the default) for every
            # OTHER way this read can fail. A scope that is not (yet)
            # available at construction time must not make constructing
            # the adapter itself fail -- `connect()` already treats a
            # failing `secret_owner.activate()` as a normal, recoverable
            # refusal (see `_connect_with_active_scope`'s own
            # `except Exception` around it), not a fatal error.
            try:
                with self._profile_scope():
                    resolved_mcp_tool_timeout = _configured_mcp_tool_timeout(
                        self.server_name
                    )
            except Exception as exc:
                # LOW (Athena second eye, round 4, 2026-09-15): this used to
                # be a bare `except Exception: <fallback>` with no trace at
                # all -- fine as a fail-open default, but a silent one, so a
                # genuinely broken profile scope at construction time left
                # no record anywhere that the fallback (rather than the
                # configured value) is what `lease_seconds` was derived
                # from. `debug`, not `warning`: the comment above already
                # establishes this is an expected, routine fallback (a scope
                # not yet available at construction time), not an
                # operator-actionable condition.
                logger.debug(
                    "[mupot] falling back to default mcp_tool_timeout=%s: "
                    "profile scope unavailable at construction (%s)",
                    _DEFAULT_MCP_TOOL_TIMEOUT_SECONDS,
                    exc,
                )
                resolved_mcp_tool_timeout = _DEFAULT_MCP_TOOL_TIMEOUT_SECONDS
        self.mcp_tool_timeout = max(0.0, float(resolved_mcp_tool_timeout))
        minimum_safe_lease = (
            self.turn_timeout + self.mcp_tool_timeout + _LEASE_SAFETY_MARGIN_SECONDS
        )
        explicit_lease_seconds = extra.get("lease_seconds")
        requested_lease = float(explicit_lease_seconds or minimum_safe_lease)
        # OPEN-D (kasra re-gate round 3, 2026-09-15): the unconfigured
        # default already computes `turn_timeout + mcp_tool_timeout +
        # margin`, but an EXPLICIT `lease_seconds` in `extra` bypassed that
        # arithmetic entirely and went straight to the `[1, 3600]` clamp
        # below -- an operator (or a stale/copied config) could set a
        # `lease_seconds` smaller than a legitimately-slow turn's own
        # budget, reproducing the exact live incident this whole class of
        # fix exists to close (the message's own visibility lease expiring
        # before the turn could finish) on every single turn, not just an
        # unlucky slow one. Clamp up rather than refuse construction --
        # self-healing, consistent with this fix's whole "recover
        # automatically" bias -- but log loudly: a smaller explicit value
        # is silently discarded, and an operator who set it deliberately
        # for some other reason needs to know it did not take effect.
        if explicit_lease_seconds is not None and requested_lease < minimum_safe_lease:
            logger.warning(
                "[mupot] configured lease_seconds=%.1f is smaller than the safe "
                "minimum turn_timeout(%.1f) + mcp_tool_timeout(%.1f) + "
                "margin(%.1f) = %.1f; clamping up to the safe minimum instead "
                "of honoring the smaller explicit value",
                requested_lease,
                self.turn_timeout,
                self.mcp_tool_timeout,
                _LEASE_SAFETY_MARGIN_SECONDS,
                minimum_safe_lease,
            )
            requested_lease = minimum_safe_lease
        self.lease_seconds = max(1, min(3600, int(requested_lease)))
        if self.lease_seconds < minimum_safe_lease:
            # The [1, 3600] ceiling above is the server's own hard cap on a
            # lease, not negotiable here -- but a `turn_timeout`/
            # `mcp_tool_timeout` combination large enough to exceed it means
            # the safety property this whole class of fix relies on
            # (lease outlives the turn) cannot hold for this configuration.
            # Nothing to clamp UP to any further; surface it loudly instead
            # of leaving an operator to discover it via a live incident.
            logger.warning(
                "[mupot] lease_seconds=%s is capped at the server's 3600s "
                "ceiling but turn_timeout(%.1f) + mcp_tool_timeout(%.1f) + "
                "margin(%.1f) = %.1f exceeds it; a turn using its own full "
                "budget may still outlive its own message lease",
                self.lease_seconds,
                self.turn_timeout,
                self.mcp_tool_timeout,
                _LEASE_SAFETY_MARGIN_SECONDS,
                minimum_safe_lease,
            )
        # Bound for _TurnFailureDeferred's class of defect (round 2,
        # 2026-09-15, see that class's docstring): a turn_timeout, a
        # no-custody handler result, or a handler exception -- ALL with the
        # message's own lease still live -- must not retry forever. Below
        # this many `delivery_attempts` (the server's own count, validated
        # non-negative-int elsewhere in this module), defer for natural
        # lease-driven redelivery; at it, DLQ (terminal, no ack -- round 4,
        # see `_TurnFailureDeferred`'s docstring).
        #
        # OPEN-D (kasra re-gate round 3, 2026-09-15): also clamped DOWN to
        # `_SERVER_MAX_DELIVERY_ATTEMPTS` -- see that constant's own
        # docstring for why a value above the server's own hard ceiling is
        # dead configuration, not a bigger retry budget.
        #
        # Round 4 (2026-09-15, Athena second eye, MED "cap surface"): the
        # UNCONFIGURED default is now `_SERVER_MAX_DELIVERY_ATTEMPTS` itself
        # (was a bare `3`) -- a local cap smaller than the server's own
        # ceiling used to let this adapter DLQ a message the server would
        # have kept redelivering for two more attempts, and (before the
        # no-ack fix above) previously also acked at that smaller local cap,
        # which set `read_at` and permanently prevented the server's own
        # reaper from ever dead-lettering the row itself (it requires
        # `read_at IS NULL`). Matching the server's ceiling by default means
        # the local cap and the server's own dead-letter threshold coincide.
        self.max_delivery_attempts = max(
            1,
            min(
                _SERVER_MAX_DELIVERY_ATTEMPTS,
                int(extra.get("max_delivery_attempts") or _SERVER_MAX_DELIVERY_ATTEMPTS),
            ),
        )
        state_path = extra.get("state_path") or str(get_hermes_home() / "platforms" / "mupot" / "state.json")
        self.store = StateStore(Path(str(state_path)))
        loaded, state_valid = self.store.load_checked()
        self.notification_recipients = dict(extra.get("notification_recipients") or {})
        self.notification_activate = extra.get("notification_activate") is True
        routine_events_enabled = extra.get("routine_events_enabled", False)
        if not isinstance(routine_events_enabled, bool):
            raise ValueError("routine_events_enabled must be a boolean")
        self.routine_events_enabled = routine_events_enabled
        self.message_injector = message_injector
        self._profile_owner_fingerprint = _profile_owner_fingerprint(secret_owner)
        self._state: dict[str, Any] = copy.deepcopy(loaded) if state_valid else {}
        loaded_reply_outbox = loaded.get("reply_outbox")
        reply_outbox_valid = loaded_reply_outbox is None or isinstance(
            loaded_reply_outbox, dict
        )
        loaded_routine_receipts = loaded.get("routine_event_receipts")
        loaded_routine_quarantine = loaded.get("routine_event_quarantine")
        routine_state_valid = (
            (loaded_routine_receipts is None or isinstance(loaded_routine_receipts, dict))
            and (
                loaded_routine_quarantine is None
                or isinstance(loaded_routine_quarantine, dict)
            )
        )
        pending = copy.deepcopy(loaded.get("pending"))
        self._state.update({
            # Mupot owns retry timing through visibility leases. Never replay
            # a stale local in-flight record by rerunning the model after a
            # crash. Preserve it so a prepared terminal envelope can finish,
            # or so ambiguous legacy work can remain visibly fenced.
            "pending": pending,
            "processed": list(loaded.get("processed") or [])[-1000:],
            "dlq": list(loaded.get("dlq") or [])[-100:],
            "terminal_receipts": list(loaded.get("terminal_receipts") or [])[-100:],
            "notification_outbox": dict(loaded.get("notification_outbox") or {}),
            "routine_event_receipts": copy.deepcopy(loaded_routine_receipts)
            if isinstance(loaded_routine_receipts, dict)
            else {},
            "routine_event_quarantine": copy.deepcopy(loaded_routine_quarantine)
            if isinstance(loaded_routine_quarantine, dict)
            else {},
            "reply_outbox": (
                copy.deepcopy(loaded_reply_outbox)
                if isinstance(loaded_reply_outbox, dict)
                else {}
            ),
            "lease_reconciliation": (
                loaded.get("lease_reconciliation")
                if state_valid
                else {"state_invalid": True}
            ),
        })
        # Keep durable inbox lease/ACK traffic isolated from outbound sends.
        # Cancelling a timed-out MCP send can close that SDK session; it must
        # never poison the authoritative consumer transport.
        self._client = client_factory(self.server_name)
        self._send_client = client_factory(self.server_name)
        self._poll_task: Optional[asyncio.Task] = None
        self._delivery_generation = 0
        self._live_generations: dict[int, _LiveDelivery] = {}
        self._consumer_fence: Optional[dict[str, Any]] = None
        self._lease_quarantined = self._state["lease_reconciliation"] is not None
        # Item 6 (kasra-review re-gate round 2, 2026-09-15): `connect()`'s
        # auto-reconcile (see the `_lease_quarantined` branch below) can run
        # a full leased-message turn -- bounded by `max_delivery_attempts`,
        # same as the live poll loop -- BEFORE `_running` is ever set True,
        # so `is_connected` (== `self._running`) already reports `False`
        # correctly for that whole window. What was missing is any signal
        # that a restart is DOING something rather than merely not-yet-
        # connected; `mupot_gateway_status` surfaces this flag for exactly
        # that gap.
        self._reconciling = False
        self._reply_state_invalid = not state_valid or not reply_outbox_valid
        self._routine_state_invalid = not state_valid or not routine_state_valid
        pending_routine_records: list[dict[str, Any]] = []
        if not self._routine_state_invalid:
            try:
                from .routine_events import pending_routine_receipts

                pending_routine_records = pending_routine_receipts(self._state)
            except Exception:
                self._routine_state_invalid = True
        self._routine_reconciliation_required = bool(
            pending_routine_records
        ) and not self.routine_events_enabled
        pending_message = pending.get("message") if isinstance(pending, dict) else None
        pending_id = (
            str(pending_message.get("id") or "").strip()
            if isinstance(pending_message, dict)
            else ""
        )
        self._legacy_pending_ambiguous = self._compute_legacy_pending_ambiguous()
        self._reply_reconciliation_required = any(
            not isinstance(record, dict)
            or record.get("status") == "reconciliation_required"
            or (
                record.get("version") == _LEGACY_REPLY_OUTBOX_VERSION
                and record.get("status") != "complete"
            )
            for record in self._state["reply_outbox"].values()
        )
        for source_id, record in self._state["reply_outbox"].items():
            try:
                validated = self._validated_reply_record(source_id, record)
                if (
                    pending_id == source_id
                    and isinstance(pending_message, dict)
                    and validated["source_fingerprint"]
                    != _reply_source_fingerprint(pending_message)
                ):
                    self._reply_state_invalid = True
            except MupotProtocolError:
                self._reply_state_invalid = True
        self._log_stranded_notifications_at_startup()

    def _profile_scope(self):
        """Same shape as `HermesMCPClient._profile_scope`: a no-op context
        when there is no scoped `secret_owner` (e.g. a unit test constructing
        this adapter directly), or `self._secret_owner.activate()` otherwise.
        Round 3 (kasra re-gate BLOCK-C): every read of `.env`/secret-bearing
        config this constructor performs must go through this, not the
        ambient process environment -- see the `mcp_tool_timeout` resolution
        above for the incident this closes.
        """
        if self._secret_owner is None:
            return nullcontext()
        return self._secret_owner.activate()

    def stranded_notifications(self) -> list[dict[str, Any]]:
        """Notices parked in a terminal state that nothing else ever reconciles.

        ``activation_unknown``/``transport_unknown`` are written by notifications.flush
        when an activation or a send crashes mid-flight, and its source has already been
        inbox_lease_ack'd -- refused activation from here on strands the human-wait
        silently unless something actually looks at these. "Preserved for inspection"
        with no inspector is a known anti-pattern; this method (and the startup log
        below) are that inspector.
        """
        stranded = []
        for source_id, notice in self._state.get("notification_outbox", {}).items():
            if (
                isinstance(notice, dict)
                and notice.get("status") in _STRANDED_NOTIFICATION_STATUSES
            ):
                stranded.append({
                    "source_id": source_id,
                    "status": notice.get("status"),
                    "activation_status": notice.get("activation_status"),
                    "delivery_status": notice.get("delivery_status"),
                    "last_error": notice.get("last_error"),
                })
        return stranded

    def turn_failure_dlq_summary(self) -> list[dict[str, Any]]:
        """Operator-facing view of `_TurnFailureDeferred`'s terminal disposition.

        Item 2 (kasra-review re-gate round 2, 2026-09-15): a message
        dead-lettered at `max_delivery_attempts` (turn_timeout, no_custody,
        or handler_error -- see `_resolve_turn_failure`) must be visible
        here, not just in `state.json`'s `dlq` list -- the whole point of
        bounding retries instead of quarantining is that the operator finds
        out from a tool, not from reading the durable state file by hand.
        Only entries tagged with one of this class's own reasons are
        included; other DLQ reasons (e.g. `invalid_ack_envelope`,
        `sender_policy`) are pre-existing and out of scope for this surface.
        """
        summary = []
        for entry in self._state.get("dlq", []):
            if not isinstance(entry, dict):
                continue
            reason = entry.get("reason")
            if reason not in _TURN_FAILURE_DLQ_REASONS:
                continue
            message = entry.get("message") if isinstance(entry.get("message"), dict) else {}
            summary.append({
                "source_id": str(message.get("id") or ""),
                "reason": reason,
            })
        return summary

    def lease_reconciliation_status(self) -> Optional[dict[str, Any]]:
        """Operator-facing view of a durable inbox-lease quarantine, or ``None``.

        Before this fix (2026-09-15, see `_LeaseExpiredDeferred`), nothing
        surfaced a `lease_reconciliation` marker anywhere an operator could
        see it short of reading `state.json` directly -- `mupot_gateway_status`
        reported `{"ok": true, ...}` for an adapter that was fully constructed
        but could never actually `connect()`. Report presence, the version
        (marker shapes 1/2/3 mean different things -- see
        `_lease_reconciliation_proof`), the attempt id when a v3 marker has
        one, and whether `connect()` will now attempt to self-heal it
        automatically (only true `_lease_quarantined` markers are attempted;
        `connect()` reconciles once per call, so a mismatch/countercase
        still requires the manual `reconcile_inbox_polling()` procedure).

        M10 (kasra-review re-gate round 2, 2026-09-15): this previously read
        `bool(proof.get("required"))` -- but `_lease_reconciliation_proof`'s
        returned dict never carries a `required` key at all (it is consumed
        as a gate condition inside that function, not passed through), so
        `proof.get("required")` was ALWAYS `None` and this method reported
        `"required": False` for every genuinely live marker. Zero tests
        exercised this against a real marker (every one only checked the
        `None`/absent case), so 116 tests stayed green when the whole method
        was mutated to always return `None` -- and the same 116 stayed green
        against the SHIPPED code's own real defect, because nothing checked
        the value when non-`None`. `_lease_reconciliation_proof` only ever
        returns non-`None` when the marker's `required` field is exactly
        `True` (see its own gate: `value.get("required") is not True ->
        return None`), so `True` here is a fact about `proof`'s existence,
        not a re-derived guess.
        """
        marker = self._state.get("lease_reconciliation")
        proof = _lease_reconciliation_proof(marker)
        if proof is None:
            return None
        return {
            "required": True,
            "version": proof.get("version"),
            "attempt_id": proof.get("attempt_id"),
            # Round 3 (kasra re-gate BLOCK-B): computed by the SAME
            # predicate `connect()` uses to decide whether it reaches the
            # reconcile attempt (`_is_lease_reconcile_reachable`), not a
            # separate re-derivation of `_lease_quarantined` alone -- see
            # that method's docstring for why the two must not drift.
            "connect_will_attempt_auto_reconcile": self._is_lease_reconcile_reachable(),
        }

    def _log_stranded_notifications_at_startup(self) -> None:
        stranded = self.stranded_notifications()
        if stranded:
            logger.warning(
                "[mupot] %d notification(s) loaded in a stranded terminal state "
                "(activation_unknown/transport_unknown) and require operator "
                "reconciliation: %s",
                len(stranded),
                sorted(item["source_id"] for item in stranded),
            )

    def set_message_handler(
        self,
        handler: Callable[[MessageEvent], Awaitable[Any]],
    ) -> None:
        async def context_bound_handler(event: MessageEvent) -> Any:
            runtime = self._runtime_for_event(event)
            if runtime is None or self._expire_if_needed(runtime):
                return None
            _delivery_context.set(runtime.context)
            return await handler(event)

        super().set_message_handler(context_bound_handler)

    @staticmethod
    def _optional_text(value: Any) -> Optional[str]:
        text = str(value or "").strip()
        return text or None

    def _delivery_deadlines(
        self, message: dict[str, Any]
    ) -> tuple[float, Optional[float]]:
        """Return `(turn_deadline, lease_deadline)` -- the two RAW deadlines,
        not just their collapsed minimum.

        Class fix (2026-09-15, round 2, see `_TurnFailureDeferred`): kasra-
        review's re-gate found that collapsing these into one `expires_at`
        (the pre-round-2 shape of this method) meant `_deliver`'s timeout
        branch could not tell "the lease died" (item 1, not the turn's
        fault) apart from "the turn hung with the lease still live" (item 2,
        a bounded turn failure) -- it always assumed the former. Both
        deadlines are kept on `_LiveDelivery` so the timeout branch can
        compare them directly.
        """
        turn_deadline = time.time() + self.turn_timeout
        raw_expiry = message.get("lease_expires_at")
        if raw_expiry is None:
            return turn_deadline, None
        if not isinstance(raw_expiry, str) or not raw_expiry.strip():
            raise _protocol_error()
        try:
            parsed = datetime.fromisoformat(raw_expiry.strip().replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError("timezone required")
            lease_deadline = parsed.timestamp()
        except (OverflowError, TypeError, ValueError):
            raise _protocol_error() from None
        if not math.isfinite(lease_deadline):
            raise _protocol_error()
        return turn_deadline, lease_deadline

    @staticmethod
    def _lease_is_binding(turn_deadline: float, lease_deadline: Optional[float]) -> bool:
        """True when the message's own lease -- not `turn_timeout` -- is the
        constraint that governs `min(turn_deadline, lease_deadline)`.

        OPEN-D (kasra re-gate round 3, 2026-09-15): strict `<`, not `<=`. At
        an EXACT tie, classify as a TURN failure (`_TurnFailureDeferred`,
        bounded by `delivery_attempts` and DLQ'd at the cap), not a lease
        expiry (`_LeaseExpiredDeferred`, retried unboundedly by design --
        see that class's own docstring). Neither reading is more "correct"
        at an exact tie -- there is no way to know from here which deadline
        would have fired first -- so this picks the side that fails
        BOUNDED rather than the side that fails OPEN to unbounded
        redelivery, same bias as everywhere else in this class of fix.
        """
        return lease_deadline is not None and lease_deadline < turn_deadline

    def _begin_delivery(
        self,
        message: dict[str, Any],
        attempt_id: Optional[str] = None,
    ) -> tuple[MessageEvent, _LiveDelivery]:
        event = build_mupot_event(message, self.platform)
        source_id = str(message.get("id") or "").strip()
        sender = normalize_agent(message.get("from_agent"))
        session_key = str(self._event_session_key(event) or "").strip()
        if not source_id or not sender or not session_key:
            raise _protocol_error()
        self._delivery_generation += 1
        context = DeliveryContext(
            source_id=source_id,
            generation=self._delivery_generation,
            sender=sender,
            project=self._optional_text(message.get("project_id")),
            request_id=self._optional_text(message.get("request_id")),
            session_key=session_key,
            attempt_id=attempt_id,
        )
        turn_deadline, lease_deadline = self._delivery_deadlines(message)
        runtime = _LiveDelivery(
            context=context,
            expires_at=min(turn_deadline, lease_deadline)
            if lease_deadline is not None
            else turn_deadline,
            turn_deadline=turn_deadline,
            lease_deadline=lease_deadline,
            source=MappingProxyType(copy.deepcopy(message)),
        )
        metadata = dict(event.metadata or {})
        metadata[_DELIVERY_CONTEXT_METADATA_KEY] = context
        event.metadata = metadata
        self._live_generations[context.generation] = runtime
        return event, runtime

    def _runtime_for_event(self, event: MessageEvent) -> Optional[_LiveDelivery]:
        metadata = event.metadata or {}
        marker = metadata.get(_DELIVERY_CONTEXT_METADATA_KEY)
        if not isinstance(marker, DeliveryContext):
            return None
        runtime = self._live_generations.get(marker.generation)
        if runtime is None or runtime.context is not marker or runtime.invalidated:
            return None
        raw_message = event.raw_message
        if not isinstance(raw_message, dict):
            return None
        if (
            str(event.message_id or "").strip() != marker.source_id
            or str(raw_message.get("id") or "").strip() != marker.source_id
            or normalize_agent(event.source.chat_id) != marker.sender
            or normalize_agent(raw_message.get("from_agent")) != marker.sender
            or self._optional_text(metadata.get("project_id")) != marker.project
            or self._optional_text(raw_message.get("project_id")) != marker.project
            or self._optional_text(metadata.get("request_id")) != marker.request_id
            or self._optional_text(raw_message.get("request_id")) != marker.request_id
            or str(self._event_session_key(event) or "").strip() != marker.session_key
        ):
            return None
        return runtime

    def _runtime_for_current_context(self) -> Optional[_LiveDelivery]:
        context = _delivery_context.get()
        if context is None:
            return None
        runtime = self._live_generations.get(context.generation)
        if runtime is None or runtime.context is not context or runtime.invalidated:
            return None
        if self._expire_if_needed(runtime):
            return None
        return runtime

    def _invalidate_delivery(
        self,
        runtime: _LiveDelivery,
        outcome: ProcessingOutcome = ProcessingOutcome.FAILURE,
    ) -> None:
        current = self._live_generations.get(runtime.context.generation)
        if current is runtime:
            self._live_generations.pop(runtime.context.generation, None)
        runtime.invalidated = True
        if runtime.outcome is None:
            runtime.outcome = outcome
        runtime.completion_event.set()

    def _expire_if_needed(self, runtime: _LiveDelivery) -> bool:
        if runtime.invalidated or time.time() >= runtime.expires_at:
            self._invalidate_delivery(runtime)
            return True
        return False

    async def _cancel_delivery_processing(self, runtime: _LiveDelivery) -> None:
        if runtime.cancellation_started:
            return
        runtime.cancellation_started = True
        try:
            await asyncio.wait_for(
                self.cancel_session_processing(runtime.context.session_key),
                timeout=self.cancel_timeout,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "[mupot] bounded session cancellation did not complete session=%s",
                runtime.context.session_key,
            )

    def _delivery_ack_ownership(
        self,
        attempt_id: Optional[str],
    ) -> dict[str, Any]:
        if attempt_id is None:
            return legacy_ack_ownership()
        marker = _lease_reconciliation_proof(
            self._state.get("lease_reconciliation")
        )
        current_owner = _profile_owner_fingerprint(
            self._secret_owner,
            validate=True,
        )
        if (
            marker is None
            or marker.get("version") != _LEASE_ATTEMPT_MARKER_VERSION
            or marker.get("attempt_id") != attempt_id
            or current_owner != self._profile_owner_fingerprint
            or marker.get("profile_owner_fingerprint") != current_owner
        ):
            raise _protocol_error()
        try:
            return attempt_ack_ownership(marker)
        except AckOwnershipError:
            raise _protocol_error() from None

    async def _ack_persisted_ownership(
        self,
        expected_id: str,
        ownership_value: Any,
    ) -> None:
        # Choke point (see _refuse_ack_if_estop_engaged's docstring): this is
        # the ONLY place in the module that calls `inbox_lease_ack` for an
        # attempt-based ownership record (the `_ack_expected` fallback below
        # covers the legacy path). Refuse before validating ownership or
        # calling the consumer status/reconcile preflight, so nothing is
        # touched while paused.
        _refuse_ack_if_estop_engaged(expected_id)
        try:
            ownership = validate_ack_ownership(ownership_value)
        except AckOwnershipError:
            self._lease_quarantined = True
            raise _protocol_error() from None
        if ownership["kind"] == "legacy_non_attempt":
            await self._ack_expected(expected_id)
            return
        ownership = await self._preflight_persisted_ownership(
            ownership,
            require_attempt=True,
        )
        try:
            payload = await self._call_consumer(
                "inbox_lease_ack",
                {"attempt_id": ownership["attempt_id"]},
            )
            receipt = validate_lease_attempt_ack(
                payload,
                ownership["attempt_id"],
                ownership,
            )
            if receipt["state"] != "acked" or receipt["consumed"] is not True:
                raise _protocol_error()
        except asyncio.CancelledError:
            raise
        except Exception:
            self._lease_quarantined = True
            self._set_fatal_error(
                "mupot_inbox_attempt_ack_reconciliation_required",
                "Mupot inbox attempt acknowledgement requires reconciliation",
                retryable=False,
            )
            raise

    async def _preflight_persisted_ownership(
        self,
        ownership_value: Any,
        *,
        require_attempt: bool = False,
    ) -> dict[str, Any]:
        """Validate replay ownership without sending, reconciling, or ACKing."""
        try:
            ownership = validate_ack_ownership(ownership_value)
            if ownership["kind"] != "attempt":
                if require_attempt:
                    raise _protocol_error()
                return ownership
            current_owner = _profile_owner_fingerprint(
                self._secret_owner,
                validate=True,
            )
            if (
                current_owner != self._profile_owner_fingerprint
                or ownership["profile_owner_fingerprint"] != current_owner
            ):
                raise _protocol_error()
            status = await self._call_consumer(
                "inbox_consumer_status",
                {"strict_scope": True},
            )
            proof = _consumer_fence_proof(
                status,
                ownership["agent_id"],
                ownership["tenant"],
                strict_scope=True,
            )
            if proof is None or any(
                proof[field] != ownership[field]
                for field in (
                    "tenant",
                    "agent_id",
                    "effective_inbox_seat",
                    "mode",
                    "generation",
                )
            ):
                raise _protocol_error()
            return ownership
        except asyncio.CancelledError:
            raise
        except Exception:
            self._lease_quarantined = True
            self._set_fatal_error(
                "mupot_inbox_replay_preflight_required",
                "Mupot inbox replay ownership requires reconciliation",
                retryable=False,
            )
            raise

    def _persist_reply_record(
        self,
        source_id: str,
        record: dict[str, Any],
    ) -> dict[str, Any]:
        durable, valid = self.store.load_checked()
        if not valid:
            raise _protocol_error()
        candidate = copy.deepcopy(durable if self.store.path.exists() else self._state)
        outbox = candidate.setdefault("reply_outbox", {})
        if not isinstance(outbox, dict):
            raise _protocol_error()
        outbox[source_id] = copy.deepcopy(record)
        self.store.save(candidate)
        readback, readback_valid = self.store.load_checked()
        durable_outbox = readback.get("reply_outbox")
        if (
            not readback_valid
            or not isinstance(durable_outbox, dict)
            or durable_outbox.get(source_id) != record
        ):
            raise _protocol_error()
        self._state = readback
        return copy.deepcopy(record)

    def _validated_reply_record(
        self,
        source_id: str,
        record: Any,
        source: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        if not isinstance(record, dict):
            raise _protocol_error()
        arguments = record.get("arguments")
        receipt = record.get("receipt")
        version = record.get("version")
        if (
            version not in {_LEGACY_REPLY_OUTBOX_VERSION, _REPLY_OUTBOX_VERSION}
            or record.get("source_id") != source_id
            or not isinstance(record.get("source"), dict)
            or not isinstance(record.get("source_fingerprint"), str)
            or not isinstance(arguments, dict)
            or not {
                "to",
                "body",
                "kind",
                "request_id",
                "in_reply_to",
            }.issubset(arguments)
            or set(arguments) - {
                "to",
                "body",
                "kind",
                "project_id",
                "request_id",
                "in_reply_to",
            }
            or arguments.get("kind") != "ack"
            or not isinstance(arguments.get("to"), str)
            or not arguments["to"].strip()
            or not str(arguments.get("body") or "").strip()
            or not isinstance(arguments.get("request_id"), str)
            or _REQUEST_ID_RE.fullmatch(arguments["request_id"]) is None
            or arguments["request_id"] != _final_request_id(source_id)
            or arguments.get("in_reply_to") != source_id
            or (
                "project_id" in arguments
                and (
                    not isinstance(arguments["project_id"], str)
                    or not arguments["project_id"].strip()
                )
            )
            or record.get("status")
            not in {
                "prepared",
                "sent",
                "custodied",
                "complete",
                "reconciliation_required",
            }
            or (receipt is not None and not isinstance(receipt, dict))
        ):
            raise _protocol_error()
        if version == _REPLY_OUTBOX_VERSION:
            try:
                validate_ack_ownership(record.get("ack_ownership"))
            except AckOwnershipError:
                raise _protocol_error() from None
        elif "ack_ownership" in record:
            raise _protocol_error()
        persisted_source = record["source"]
        if record["source_fingerprint"] != _reply_source_fingerprint(persisted_source):
            raise _protocol_error()
        if source is not None and record["source_fingerprint"] != _reply_source_fingerprint(
            source
        ):
            raise _protocol_error()
        status = record["status"]
        if status == "prepared" and receipt is not None:
            raise _protocol_error()
        if status in {"sent", "custodied", "complete"}:
            validate_send_receipt(
                receipt,
                arguments["to"],
                arguments.get("project_id"),
            )
        return copy.deepcopy(record)

    def _prepare_final_reply(
        self,
        runtime: _LiveDelivery,
        recipient: str,
        content: str,
    ) -> dict[str, Any]:
        source_id = runtime.context.source_id
        durable, valid = self.store.load_checked()
        if not valid:
            raise _protocol_error()
        outbox = durable.get("reply_outbox")
        if outbox is not None and not isinstance(outbox, dict):
            raise _protocol_error()
        existing = (outbox or {}).get(source_id)
        if existing is not None:
            return self._validated_reply_record(source_id, existing, runtime.source)
        if not content.strip():
            raise _protocol_error()
        arguments: dict[str, Any] = {
            "to": recipient,
            "body": content,
            "kind": "ack",
            "request_id": _final_request_id(source_id),
            "in_reply_to": source_id,
        }
        if runtime.context.project:
            arguments["project_id"] = runtime.context.project
        source = copy.deepcopy(dict(runtime.source))
        ack_ownership = self._delivery_ack_ownership(runtime.context.attempt_id)
        record = {
            "version": _REPLY_OUTBOX_VERSION,
            "source_id": source_id,
            "source": source,
            "source_fingerprint": _reply_source_fingerprint(source),
            "ack_ownership": ack_ownership,
            "arguments": arguments,
            "status": "prepared",
            "receipt": None,
        }
        return self._persist_reply_record(source_id, record)

    def _update_reply_record(
        self,
        source_id: str,
        record: dict[str, Any],
        *,
        status: str,
        receipt: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        updated = copy.deepcopy(record)
        updated["status"] = status
        if receipt is not None:
            updated["receipt"] = copy.deepcopy(receipt)
        persisted = self._persist_reply_record(source_id, updated)
        self._reply_reconciliation_required = status == "reconciliation_required" or any(
            isinstance(value, dict)
            and value.get("status") == "reconciliation_required"
            for key, value in self._state.get("reply_outbox", {}).items()
            if key != source_id
        )
        return persisted

    async def _transmit_final_reply(
        self,
        record: dict[str, Any],
    ) -> dict[str, Any]:
        source_id = record["source_id"]
        record = self._validated_reply_record(source_id, record)
        if record["status"] == "reconciliation_required":
            raise _protocol_error()
        if record["status"] == "prepared":
            # Choke point for the peer `send` MCP tool (kasra-review re-gate
            # #3, 2026-09-14): `_replay_reply_outbox` calls this for a reply
            # PREPARED before `hermes pause` was pressed -- proven live in
            # re-gate #3 that without this check the prepared, immutable
            # envelope is transmitted to the mupot peer during the pause.
            # Refuse before the network call; the record stays "prepared" so
            # the exact same envelope is retried, verbatim, once resumed.
            # (The live `send()` interim path shares this same choke point.)
            if _estop_engaged():
                # P2 (kasra-review re-gate #4, 2026-09-14): refused silently
                # before -- once per pause window, not once per replayed/
                # retried reply, so a backlog of prepared replies during a
                # long pause does not flood the log.
                _note_estop_pause_once(
                    "_transmit_final_reply",
                    "[mupot] refusing peer send: Hermes global emergency "
                    "stop is engaged",
                )
                raise _EstopDeferred(source_id)
            try:
                result = await asyncio.wait_for(
                    self._send_client.call("send", copy.deepcopy(record["arguments"])),
                    timeout=self.rpc_timeout,
                )
                receipt = validate_send_receipt(
                    result,
                    record["arguments"]["to"],
                    record["arguments"].get("project_id"),
                )
            except MupotProtocolError:
                self._update_reply_record(
                    source_id,
                    record,
                    status="reconciliation_required",
                )
                raise
            record = self._update_reply_record(
                source_id,
                record,
                status="sent",
                receipt=receipt,
            )
        if record["status"] == "sent":
            from .notifications import enqueue

            enqueue(
                self._state,
                self.store,
                record["source"],
                record["arguments"]["body"],
            )
            # enqueue performs its own durable readback. Persisting this status
            # after it means source consumption can require both custody proofs.
            durable_record = self._state.get("reply_outbox", {}).get(source_id)
            record = self._validated_reply_record(source_id, durable_record)
            record = self._update_reply_record(
                source_id,
                record,
                status="custodied",
            )
        final_receipt = record.get("receipt")
        if not isinstance(final_receipt, dict):
            raise _protocol_error()
        return final_receipt

    def _reply_has_human_custody(self, source_id: str) -> bool:
        record = self._state.get("reply_outbox", {}).get(source_id)
        notice = self._state.get("notification_outbox", {}).get(source_id)
        return bool(
            isinstance(record, dict)
            and record.get("status") in {"custodied", "complete"}
            and isinstance(record.get("receipt"), dict)
            and isinstance(notice, dict)
            and notice.get("custody_status") == "durable"
        )

    def _mark_reply_complete(self, source_id: str) -> None:
        record = self._state.get("reply_outbox", {}).get(source_id)
        validated = self._validated_reply_record(source_id, record)
        if validated["status"] != "complete":
            self._update_reply_record(source_id, validated, status="complete")

    async def _replay_reply_outbox(self) -> None:
        outbox = self._state.get("reply_outbox")
        if not isinstance(outbox, dict):
            raise _protocol_error()
        for source_id in list(outbox):
            record = self._validated_reply_record(source_id, outbox[source_id])
            if record["status"] == "complete":
                continue
            if record["version"] == _LEGACY_REPLY_OUTBOX_VERSION:
                self._reply_reconciliation_required = True
                raise _protocol_error()
            if record["status"] == "reconciliation_required":
                self._reply_reconciliation_required = True
                raise _protocol_error()
            if source_id in self._state.get("processed", []):
                self._mark_reply_complete(source_id)
                continue
            pending = self._state.get("pending")
            pending_message = pending.get("message") if isinstance(pending, dict) else None
            if (
                not isinstance(pending_message, dict)
                or str(pending_message.get("id") or "").strip() != source_id
                or _reply_source_fingerprint(pending_message)
                != record["source_fingerprint"]
            ):
                self._update_reply_record(
                    source_id,
                    record,
                    status="reconciliation_required",
                )
                raise _protocol_error()
            if record["status"] == "prepared":
                await self._preflight_persisted_ownership(
                    record["ack_ownership"],
                    require_attempt=True,
                )
            await self._transmit_final_reply(record)
            if not self._reply_has_human_custody(source_id):
                raise _protocol_error()
            await self._ack_persisted_ownership(
                source_id,
                record["ack_ownership"],
            )
            self._commit(source_id)
            self._mark_reply_complete(source_id)

    def _compute_legacy_pending_ambiguous(self) -> bool:
        """Is the CURRENT `self._state["pending"]` genuinely ambiguous crash
        state (v1/legacy detection, pre-dating the v3 `lease_reconciliation`
        marker)?

        Round 3 (kasra re-gate BLOCK-B): this used to be computed exactly
        once, in `__init__`, off the state loaded from disk at construction
        time. That is stale the moment anything else in this process
        mutates `self._state["pending"]` afterwards -- in particular,
        `_reconcile_inbox_polling_with_active_scope`'s own terminal-tombstone
        branch drops an unrelated, unstaged `pending` itself (see that
        method: "nothing was consumed and nothing is waiting to be delivered
        for the pending source ... drop it rather than leave it to fence a
        future, unrelated crash as ambiguous") -- a real, existing "deferral
        tombstone" that can explain away an ambiguous `pending` the
        constructor saw. `connect()` re-calls this method AFTER attempting
        that reconcile (not the cached `__init__`-time attribute) so a
        tombstone applied during THIS `connect()` call is picked up
        immediately, in the same call, rather than requiring a second
        restart. `self._legacy_pending_ambiguous` (the cached attribute) is
        left as the `__init__`-time snapshot for anything that wants to know
        what was true when the adapter was constructed.
        """
        pending = self._state.get("pending")
        pending_message = pending.get("message") if isinstance(pending, dict) else None
        pending_id = (
            str(pending_message.get("id") or "").strip()
            if isinstance(pending_message, dict)
            else ""
        )
        return pending is not None and (
            not pending_id or pending_id not in self._state.get("reply_outbox", {})
        )

    def _is_lease_reconcile_reachable(self) -> bool:
        """Would `connect()`, for CURRENT state, actually attempt the
        `_lease_quarantined` auto-reconcile step?

        Round 3 (kasra re-gate BLOCK-B): `lease_reconciliation_status()`'s
        `connect_will_attempt_auto_reconcile` field used to be a bare
        `bool(self._lease_quarantined)` -- true whenever a v3 marker
        existed, regardless of whether an EARLIER `connect()` gate (reply
        state invalid, Routine reconciliation required) would refuse first
        and never let execution reach the reconcile attempt at all. That let
        status report a self-heal as imminent when it structurally could not
        run. This is the ONE function both `_connect_with_active_scope` and
        `lease_reconciliation_status()` call, so the two can never drift out
        of sync again -- reflects only the gates that run BEFORE the
        reconcile attempt in `connect()`'s current order; deliberately does
        NOT include `_legacy_pending_ambiguous`, which (same fix) now runs
        AFTER the reconcile attempt, not before it.

        Round 4 (2026-09-15, Athena second eye, head c68e0839, B3 residual):
        `_connect_with_active_scope`'s OWN first gate -- profile-owner
        fingerprint unavailable or mismatched -- refuses before this
        function is ever consulted, but this predicate did not account for
        it, so a marker under a mismatched profile still reported
        `connect_will_attempt_auto_reconcile: true` even though `connect()`
        would refuse at that very first `if`, never reaching the reconcile
        attempt at all. Re-derives the identical check `_connect_with_
        active_scope` runs (`_profile_owner_fingerprint(self._secret_owner,
        validate=True)` against the constructor-time snapshot) rather than
        reading a cached flag, for the same reason `_legacy_pending_
        ambiguous` is re-derived live elsewhere in this module: this can be
        called at any time, not just at `connect()`.
        """
        current_owner_fingerprint = _profile_owner_fingerprint(
            self._secret_owner,
            validate=True,
        )
        return bool(self._lease_quarantined) and not (
            self._profile_owner_fingerprint is None
            or current_owner_fingerprint != self._profile_owner_fingerprint
            or self._reply_state_invalid
            or self._reply_reconciliation_required
            or self._routine_state_invalid
            or self._routine_reconciliation_required
        )

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if self._secret_owner is not None:
            try:
                with self._secret_owner.activate():
                    return await self._connect_with_active_scope(
                        is_reconnect=is_reconnect
                    )
            except Exception:
                logger.error("[mupot] connect failed: profile scope unavailable")
                self._mark_disconnected()
                return False
        return await self._connect_with_active_scope(is_reconnect=is_reconnect)

    async def _connect_with_active_scope(self, *, is_reconnect: bool = False) -> bool:
        if (
            self._profile_owner_fingerprint is None
            or _profile_owner_fingerprint(self._secret_owner, validate=True)
            != self._profile_owner_fingerprint
        ):
            logger.error("[mupot] connect blocked; profile owner unavailable")
            return False
        if self._reply_state_invalid or self._reply_reconciliation_required:
            logger.error("[mupot] connect blocked; reply reconciliation required")
            return False
        if self._routine_state_invalid:
            logger.error("[mupot] connect blocked; Routine event reconciliation required")
            return False
        if self._routine_reconciliation_required:
            logger.error(
                "[mupot] connect blocked; Routine events are disabled with pending custody"
            )
            return False
        # Round 3 (kasra re-gate BLOCK-B): the `_lease_quarantined` auto-
        # reconcile attempt below used to run AFTER the legacy-pending-
        # ambiguous check that used to be bundled into the first `if` above.
        # A restart carrying BOTH a stale `pending` (ambiguous) AND a v3
        # `lease_reconciliation` marker refused at the pending check and
        # NEVER reached this block, so the marker's self-heal never got a
        # chance to run even though `lease_reconciliation_status()` reported
        # `connect_will_attempt_auto_reconcile: True` -- an operator fixing
        # the pending ambiguity by hand would then hit a SECOND, separate
        # manual-reconcile requirement that a prior restart could already
        # have cleared on its own. Moved here, before the pending check, so
        # the two failure classes are independent: this always gets its
        # shot regardless of what the pending check below decides.
        if self._is_lease_reconcile_reachable():
            # Class fix (2026-09-15): a `required: true` marker left by
            # `_LeaseExpiredDeferred`'s class of defect (or any other
            # genuinely-empty terminal tombstone -- see
            # `_reconcile_inbox_polling_with_active_scope`) is, by
            # construction, SELF-HEALING: the server is the authority on
            # whether anything was actually consumed, and reconciliation
            # only ever clears when it proves nothing was. Before this fix,
            # a `required: true` marker blocked `connect()` forever, across
            # every operator restart, because nothing ever called
            # `reconcile_inbox_polling()` -- the live incident this closes
            # (kayhermes gateway, 2026-09-15) needed a human to notice and
            # invoke it manually. Attempt it once, here, before refusing: if
            # it clears, proceed exactly as if never quarantined; if a real
            # countercase holds (reply still staged, attempt/owner/tenant
            # mismatch, readback failure), it returns False and this refuses
            # precisely as before.
            self._reconciling = True
            try:
                if not await self._reconcile_inbox_polling_with_active_scope():
                    logger.error("[mupot] connect blocked; inbox reconciliation required")
                    return False
            finally:
                self._reconciling = False
            logger.info(
                "[mupot] inbox lease reconciliation cleared automatically at connect"
            )
        # Re-evaluated live, not the `__init__`-time `self._legacy_pending_
        # ambiguous` snapshot: the reconcile attempt just above can itself
        # have cleared `pending` via its own terminal-tombstone branch (see
        # `_compute_legacy_pending_ambiguous`'s docstring) in THIS call.
        self._legacy_pending_ambiguous = self._compute_legacy_pending_ambiguous()
        if self._legacy_pending_ambiguous:
            logger.error("[mupot] connect blocked; reply reconciliation required")
            return False
        try:
            require_supported_profile_runtime({})
            if self._running:
                return True
            await self._client.connect()
            await self._send_client.connect()
            if self.expected_agent_id or self.expected_tenant:
                boot = await asyncio.wait_for(self._client.call("boot_context", {}), timeout=self.rpc_timeout)
                if (not self.expected_agent_id or not self.expected_tenant or not isinstance(boot, dict)
                        or validate_operator_identity(boot, expected_tenant=self.expected_tenant,
                                                      agent_id=self.expected_agent_id) is not None
                        or boot.get("channel") != "workspace"):
                    raise RuntimeError("Mupot native gateway identity or tenant mismatch")
            fence = await asyncio.wait_for(
                self._client.call("inbox_consumer_status", {"strict_scope": True}),
                timeout=self.rpc_timeout,
            )
            proof = _consumer_fence_proof(
                fence,
                self.expected_agent_id,
                self.expected_tenant,
                strict_scope=True,
            )
            if proof is None:
                raise _protocol_error()
            self._consumer_fence = proof
            self._mark_connected()
            self._poll_task = asyncio.create_task(
                self._poll_loop(), name="hermes-mupot-inbox-poller"
            )
            logger.info(
                "[mupot] connected consumer_mode=%s allowed=%s",
                fence.get("mode"),
                sorted(self.allowed_agents),
            )
            return True
        except Exception as exc:
            logger.error("[mupot] connect failed: %s", exc, exc_info=True)
            self._mark_disconnected()
            try:
                await self._client.close()
            except Exception:
                pass
            try:
                await self._send_client.close()
            except Exception:
                pass
            return False

    async def disconnect(self) -> None:
        self._running = False
        live = list(self._live_generations.values())
        for runtime in live:
            self._invalidate_delivery(runtime)
        await asyncio.gather(
            *(self._cancel_delivery_processing(runtime) for runtime in live),
            return_exceptions=True,
        )
        task, self._poll_task = self._poll_task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self.cancel_background_tasks()
        await self._client.close()
        if self._send_client is not self._client:
            await self._send_client.close()
        self._mark_disconnected()

    async def _call_consumer(
        self,
        tool: str,
        arguments: dict[str, Any],
        before_attempt: Optional[Callable[[], None]] = None,
    ) -> Any:
        """Retry once only when transport proves no request bytes were sent."""
        for attempt in range(2):
            if before_attempt is not None:
                before_attempt()
            try:
                return await asyncio.wait_for(
                    self._client.call(tool, arguments),
                    timeout=self.rpc_timeout,
                )
            except MupotSafeRetryError:
                if attempt == 0:
                    logger.warning(
                        "[mupot] retrying consumer request after safe pre-send failure"
                    )
                    continue
                raise
        raise AssertionError("unreachable")

    @staticmethod
    def _new_lease_attempt_id() -> str:
        attempt_id = secrets.token_urlsafe(24)
        if not _LEASE_ATTEMPT_ID_RE.fullmatch(attempt_id):
            raise _protocol_error()
        return attempt_id

    def _persist_prelease_fence(self, attempt_id: str) -> None:
        proof = self._consumer_fence
        owner_fingerprint = self._profile_owner_fingerprint
        if (
            proof is None
            or owner_fingerprint is None
            or _profile_owner_fingerprint(self._secret_owner, validate=True)
            != owner_fingerprint
            or not _LEASE_ATTEMPT_ID_RE.fullmatch(attempt_id)
        ):
            raise _protocol_error()
        fenced = dict(self._state)
        fenced["lease_reconciliation"] = {
            "version": _LEASE_ATTEMPT_MARKER_VERSION,
            "required": True,
            **proof,
            "profile_owner_fingerprint": owner_fingerprint,
            "attempt_id": attempt_id,
        }
        self.store.save(fenced)
        self._state = fenced

    def _clear_lease_fence(self) -> None:
        cleared = dict(self._state)
        cleared.pop("lease_reconciliation", None)
        self.store.save(cleared)
        self._state = cleared

    def _quarantine_inbox_polling(self) -> None:
        if _lease_reconciliation_proof(
            self._state.get("lease_reconciliation")
        ) is not None:
            self._lease_quarantined = True
            self._set_fatal_error(
                "mupot_inbox_reconciliation_required",
                "Mupot inbox polling requires reconciliation",
                retryable=False,
            )
            logger.error("[mupot] inbox polling quarantined; reconciliation required")
            return
        proof = self._consumer_fence
        if proof is None:
            self._lease_quarantined = True
            self._set_fatal_error(
                "mupot_inbox_reconciliation_state_missing",
                "Mupot inbox reconciliation state is unavailable",
                retryable=False,
            )
            logger.error("[mupot] inbox polling quarantined without fence state")
            return
        self._state["lease_reconciliation"] = {
            "version": 1,
            "required": True,
            "agent_id": proof["agent_id"],
            "mode": proof["mode"],
            "generation": proof["generation"],
            "reconcile_after": 0,
        }
        self._lease_quarantined = True
        try:
            self.store.save(self._state)
        except Exception:
            self._set_fatal_error(
                "mupot_inbox_reconciliation_persistence_failed",
                "Mupot inbox reconciliation state could not be persisted",
                retryable=False,
            )
            logger.error("[mupot] inbox polling quarantine persistence failed")
            return
        self._set_fatal_error(
            "mupot_inbox_reconciliation_required",
            "Mupot inbox polling requires reconciliation",
            retryable=False,
        )
        logger.error("[mupot] inbox polling quarantined; reconciliation required")

    async def reconcile_inbox_polling(self) -> bool:
        """Reconcile a durable attempt through its authoritative server receipt."""
        if self._secret_owner is not None:
            try:
                with self._secret_owner.activate():
                    return await self._reconcile_inbox_polling_with_active_scope()
            except Exception:
                logger.error("[mupot] inbox reconciliation profile scope failed")
                return False
        return await self._reconcile_inbox_polling_with_active_scope()

    async def _reconcile_inbox_polling_with_active_scope(self) -> bool:
        marker = _lease_reconciliation_proof(
            self._state.get("lease_reconciliation")
        )
        current_owner_fingerprint = _profile_owner_fingerprint(
            self._secret_owner,
            validate=True,
        )
        if (
            not self._lease_quarantined
            or marker is None
            or marker.get("version") != _LEASE_ATTEMPT_MARKER_VERSION
            or current_owner_fingerprint != self._profile_owner_fingerprint
            or marker.get("profile_owner_fingerprint")
            != current_owner_fingerprint
        ):
            return False
        try:
            require_supported_profile_runtime({})
            await self._client.connect()
            value = await asyncio.wait_for(
                self._client.call("inbox_consumer_status", {"strict_scope": True}),
                timeout=self.rpc_timeout,
            )
        except Exception:
            logger.error("[mupot] inbox reconciliation readback failed")
            return False
        proof = _consumer_fence_proof(
            value,
            marker["agent_id"],
            marker["tenant"],
            strict_scope=True,
        )
        if proof is None or any(
            proof[field] != marker[field]
            for field in (
                "tenant",
                "agent_id",
                "effective_inbox_seat",
                "mode",
                "generation",
            )
        ):
            logger.error("[mupot] inbox reconciliation readback mismatch")
            return False
        try:
            result = await asyncio.wait_for(
                self._client.call(
                    "inbox_lease_reconcile",
                    {"attempt_id": marker["attempt_id"]},
                ),
                timeout=self.rpc_timeout,
            )
            outcome = validate_lease_attempt_result(
                result,
                marker["attempt_id"],
                marker,
            )
            if outcome["state"] == "leased":
                message = outcome["messages"][0]
                try:
                    await self._process_leased_message(
                        message,
                        attempt_id=marker["attempt_id"],
                    )
                except _DeliveryDeferred as deferred:
                    # Same class as _poll_loop's own handling: a pause, or a
                    # lease that expires while an operator is reconciling an
                    # existing quarantine, is not itself a NEW protocol
                    # violation. Leave the quarantine marker exactly as it was
                    # (nothing here proves the original attempt's outcome one
                    # way or the other) and let the caller retry
                    # `reconcile_inbox_polling()` once the condition clears,
                    # instead of logging this as a failed reconciliation.
                    logger.info(
                        "[mupot] inbox reconciliation deferred: %s", deferred.reason
                    )
                    return False
                message_id = message["id"]
                if message_id not in self._state.get("processed", []):
                    raise _protocol_error()
            else:
                # Terminal tombstone (empty/cancelled/expired/acked): the
                # attempt itself is gone and the server confirms nothing was
                # consumed through it -- but the attempt id alone says
                # nothing about the PENDING SOURCE it may have held mid-turn
                # (class fix, 2026-09-15, see _LeaseExpiredDeferred). Only
                # `reply_outbox` says whether a reply is still staged for
                # that source. Clearing here while one is staged would let a
                # later `_replay_reply_outbox` transmit or ack using
                # ownership tied to an attempt this call just tombstoned --
                # preserve the fence for a human whenever that risk is real,
                # and self-heal only the genuinely empty case.
                pending = self._state.get("pending")
                pending_message = (
                    pending.get("message") if isinstance(pending, dict) else None
                )
                pending_id = (
                    str(pending_message.get("id") or "").strip()
                    if isinstance(pending_message, dict)
                    else ""
                )
                if pending_id and pending_id in self._state.get("reply_outbox", {}):
                    logger.error(
                        "[mupot] inbox reconciliation preserved: reply_outbox "
                        "holds a record for the pending source=%s of a terminal, "
                        "unconsumed attempt=%s",
                        pending_id,
                        marker["attempt_id"],
                    )
                    return False
                # Nothing was consumed and nothing is waiting to be delivered
                # for the pending source: it can never resume (the attempt
                # tombstoned), so drop it rather than leave it to fence a
                # future, unrelated crash as ambiguous.
                self._state["pending"] = None
            self._clear_lease_fence()
        except Exception:
            logger.error("[mupot] inbox attempt reconciliation failed")
            return False
        self._consumer_fence = proof
        self._lease_quarantined = False
        self._fatal_error_code = self._fatal_error_message = None
        self._fatal_error_retryable = True
        self._mark_disconnected()
        return True

    async def _poll_loop(self) -> None:
        while self._running:
            # FOURTH named gap (kasra-review re-gate #3, 2026-09-14): checking
            # `_estop_engaged()` only right before `inbox_lease` (as rounds 1-3
            # did) left `_replay_routine_events`/`_replay_reply_outbox`/
            # `_flush_notifications` -- statements #1-#3 of every iteration --
            # running UNCONDITIONALLY while paused: real ACKs, real peer
            # sends, real Telegram/mirror egress, proven live in re-gate #3.
            # A pause is a TEMPORAL condition on the WHOLE iteration, not one
            # stage of it. Checking here, as the very first statement, means
            # a paused tick does nothing at all but sleep/backoff -- no
            # replay, no flush, no lease attempt is even started. This is the
            # PRIMARY defense; the individual `_estop_engaged()`/
            # `_refuse_ack_if_estop_engaged()` checks inside `_deliver`,
            # `_handle_routine_event`, `_handle_ack_envelope`,
            # `_process_leased_message`, `_ack_expected`,
            # `_ack_persisted_ownership`, `_transmit_final_reply`, and
            # `notifications.flush()` are the SECOND layer: they catch the
            # narrow mid-iteration race (pause engages after this check
            # passes but before the tick finishes) and protect any direct
            # caller that bypasses `_poll_loop` entirely (existing unit tests
            # call `_deliver` etc. directly; `reconcile_inbox_polling` calls
            # `_process_leased_message` directly).
            if _estop_engaged():
                logger.info(
                    "[mupot] inbox polling paused: Hermes global emergency stop is "
                    "engaged; skipping this tick entirely (no replay, no flush, no "
                    "lease attempt)"
                )
                await asyncio.sleep(self.poll_interval)
                continue
            try:
                await self._replay_routine_events()
            except _EstopDeferred:
                # The e-stop engaged mid-iteration, after the top-of-loop check
                # above passed but while `_replay_routine_events` was replaying a
                # pending receipt (via `_ack_persisted_ownership`, itself gated).
                # This is the SAME class as the lease-time race below: defer and
                # retry next tick, never treat it as the routine-event
                # reconciliation failure the bare `except Exception` below is for.
                #
                # Item 4 (kasra-review re-gate round 2, 2026-09-15): kept
                # narrow to `_EstopDeferred` deliberately, not widened to the
                # `_DeliveryDeferred` base -- `_replay_routine_events` only
                # ever reaches `_ack_persisted_ownership`'s choke point
                # (`_refuse_ack_if_estop_engaged`), never `_deliver`, so
                # `_LeaseExpiredDeferred`/`_TurnFailureDeferred` are
                # structurally impossible here. Widening would silently
                # accept a class of exception this function can never
                # actually raise, which is the wrong shape to leave for a
                # future reader to reason about.
                logger.info(
                    "[mupot] routine event replay deferred mid-iteration: Hermes "
                    "global emergency stop is engaged"
                )
                await asyncio.sleep(self.poll_interval)
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._set_fatal_error(
                    "mupot_routine_event_reconciliation_required",
                    "Mupot Routine event reconciliation is required",
                    retryable=False,
                )
                logger.error("[mupot] Routine event replay requires reconciliation: %s", exc)
                return
            try:
                await self._replay_reply_outbox()
            except _EstopDeferred:
                # Same class, for `_replay_reply_outbox`'s peer `send` choke
                # point (`_transmit_final_reply`). Without this explicit clause
                # this would fall into the bare `except Exception` below and log
                # as an ordinary "reply replay deferred" warning rather than the
                # pause it actually is -- functionally harmless (both paths
                # sleep and continue) but mislabeled, and the wrong shape to
                # extend if this function ever needs pause-specific bookkeeping.
                #
                # Item 4 (kasra-review re-gate round 2, 2026-09-15): kept
                # narrow deliberately, same reasoning as
                # `_replay_routine_events`'s catch above -- `_replay_reply_outbox`
                # only ever reaches `_transmit_final_reply`'s choke point, never
                # `_deliver`, so the lease-expiry/turn-failure subclasses cannot
                # occur here.
                logger.info(
                    "[mupot] reply outbox replay deferred mid-iteration: Hermes "
                    "global emergency stop is engaged"
                )
                await asyncio.sleep(self.poll_interval)
                continue
            except asyncio.CancelledError:
                raise
            except MupotProtocolError:
                self._reply_reconciliation_required = True
                self._set_fatal_error(
                    "mupot_reply_reconciliation_required",
                    "Mupot reply reconciliation is required",
                    retryable=False,
                )
                logger.error("[mupot] reply replay requires reconciliation")
                return
            except Exception as exc:
                # The exact immutable envelope remains durable. Do not lease
                # new work while its terminal response is unresolved.
                logger.warning("[mupot] reply replay deferred: %s", exc)
                await asyncio.sleep(self.poll_interval)
                continue
            try:
                await self._flush_notifications()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # notifications.flush()'s own choke points (activation injector,
                # deliver_text, mirror_text) each raise RetryLater (an existing,
                # non-pause-specific retry signal) and catch it internally --
                # a pause never propagates out of flush() as an exception here.
                logger.warning(
                    "[mupot] notification flush error: %s", exc, exc_info=True
                )
                await asyncio.sleep(self.poll_interval)
                continue

            try:
                attempt_id = self._new_lease_attempt_id()
                arguments = {
                    "limit": 1,
                    "lease_seconds": self.lease_seconds,
                    "attempt_id": attempt_id,
                }
                payload = await self._call_consumer(
                    "inbox_lease",
                    arguments,
                    before_attempt=lambda: self._persist_prelease_fence(attempt_id),
                )
                outcome = validate_lease_attempt_result(
                    payload,
                    attempt_id,
                    self._consumer_fence,
                )
                if outcome["state"] == "leased":
                    message = outcome["messages"][0]
                    message_id = str(message.get("id") or "")
                    logger.info(
                        "[mupot] leased message=%s seq=%s attempts=%s request_id=%s",
                        message_id,
                        message.get("seq"),
                        message.get("delivery_attempts"),
                        message.get("request_id"),
                    )
                    try:
                        await self._process_leased_message(message, attempt_id=attempt_id)
                    except _DeliveryDeferred as deferred:
                        # A pause engaging between this iteration's pre-lease check
                        # above and the message actually being handled is a narrow
                        # race (not the common case), and a message's own lease
                        # expiring mid-turn is the common case for a slow turn --
                        # but both are the SAME class: neither is a protocol error.
                        # `_handle_routine_event`/`_handle_ack_envelope` raise
                        # `_EstopDeferred` before touching any durable state, but
                        # (F1, kasra-review re-gate round 2, 2026-09-15: this
                        # comment previously claimed the same for `_deliver`
                        # unconditionally, which is false) `_deliver` itself
                        # writes `pending` to durable state BEFORE it can raise
                        # `_LeaseExpiredDeferred`/`_TurnFailureDeferred` -- (round
                        # 3, kasra re-gate BLOCK-A) both of those raise sites now
                        # explicitly clear `pending` back to `None` before
                        # raising, same invariant as every other
                        # `_DeliveryDeferred` site (see each class's own
                        # docstring), rather than never having written it at
                        # all. `_process_leased_message`'s own sender_policy DLQ
                        # branch and `_handle_ack_envelope`'s invalid_ack_envelope
                        # branch each write a DLQ row BEFORE reaching the ack
                        # that can raise this (P3, kasra-review re-gate #4,
                        # 2026-09-14): that write is idempotent by message id, so
                        # redelivery after this exact race re-enters the same
                        # branch without duplicating the row. Release this
                        # attempt's lease fence (no reconciliation is owed for a
                        # lease we chose to abandon, as opposed to one a genuine
                        # protocol violation left dangling) and let the server-side
                        # lease expire on its own so `inbox_lease` redelivers the
                        # exact same message once the deferred condition clears.
                        logger.info(
                            "[mupot] deferring leased message=%s mid-poll: %s; "
                            "leaving lease to expire for redelivery",
                            message_id,
                            deferred.reason,
                        )
                        self._clear_lease_fence()
                        await asyncio.sleep(self.poll_interval)
                        continue
                    if message_id not in self._state.get("processed", []):
                        raise _protocol_error()
                self._clear_lease_fence()
            except asyncio.CancelledError:
                raise
            except Exception:
                self._quarantine_inbox_polling()
                return
            await asyncio.sleep(self.poll_interval)

    async def _process_leased_message(
        self,
        message: dict[str, Any],
        attempt_id: Optional[str] = None,
    ) -> None:
        """Route one authenticated leased row without widening peer authority."""
        from .routine_events import is_routine_event_candidate, quarantine_routine_event

        message_id = str(message.get("id") or "")
        if _estop_engaged():
            # Structural choke point (kasra-review re-gate #3, 2026-09-14):
            # the routine-events-disabled quarantine branch below and the
            # sender-policy DLQ branch further down each quarantine/DLQ and
            # then _ack_expected() a message WITHOUT ever reaching _deliver,
            # _handle_routine_event, or _handle_ack_envelope -- neither of
            # those two branches' own `_estop_engaged()` gate (they have
            # none) ever ran for them. Refusing here, before this function
            # dispatches to ANY branch, closes all three ungated branches
            # (routine_events_disabled, already-processed re-ack,
            # sender_policy DLQ) plus the two named functions at once, and
            # means a future branch added to this dispatcher inherits the
            # gate automatically rather than needing its own copy.
            #
            # P2 (kasra-review re-gate #4, 2026-09-14): refused silently
            # before -- once per pause window, not once per leased message,
            # so a busy paused poll loop does not flood the log.
            _note_estop_pause_once(
                "_process_leased_message",
                "[mupot] refusing leased message dispatch: Hermes global "
                "emergency stop is engaged",
            )
            raise _EstopDeferred(message_id)
        if is_routine_event_candidate(message):
            if self.routine_events_enabled:
                await self._handle_routine_event(message, attempt_id=attempt_id)
            else:
                quarantine_routine_event(
                    self._state, self.store, message, "routine_events_disabled"
                )
                await self._ack_expected(message_id, attempt_id=attempt_id)
                self._commit(message_id)
            return
        if message_id in self._state["processed"]:
            await self._ack_expected(message_id, attempt_id=attempt_id)
            return
        if is_ack_envelope(message) and should_accept_message(
            message, self.allowed_agents
        ):
            await self._handle_ack_envelope(message, attempt_id=attempt_id)
            return
        if should_accept_message(message, self.allowed_agents):
            await self._deliver(message, attempt_id=attempt_id)
            return
        # P3 (kasra-review re-gate #4, 2026-09-14): a pause landing between
        # this DLQ write and the ack below (raised by _ack_expected's own
        # _refuse_ack_if_estop_engaged choke point) leaves this branch
        # unacked, so the message is redelivered once resumed and
        # _process_leased_message runs this exact branch again -- append
        # only if this message id is not already in the DLQ, so redelivery
        # after a mid-write pause produces exactly one row, not a duplicate.
        dlq = self._state["dlq"]
        if not any(
            str((entry.get("message") or {}).get("id") or "") == message_id
            for entry in dlq
        ):
            dlq.append({"message": message, "reason": "sender_policy"})
        self._state["dlq"] = dlq[-100:]
        self.store.save(self._state)
        await self._ack_expected(message_id, attempt_id=attempt_id)
        self._commit(message_id)

    async def _handle_routine_event(
        self,
        message: dict[str, Any],
        attempt_id: Optional[str] = None,
    ) -> None:
        """Take custody, ACK exactly one source, then durably mark it processed."""
        from .notifications import enqueue
        from .routine_events import (
            RoutineEventValidationError,
            mark_routine_processed,
            persist_routine_receipt,
            quarantine_routine_event,
            validate_routine_event,
        )

        if not self.routine_events_enabled:
            raise RuntimeError("Mupot Routine events are disabled")
        message_id = str(message.get("id") or "")
        if _estop_engaged():
            # SECOND named gap from the kasra-review re-gate (2026-09-14): the
            # round-1 fix only checked _estop_engaged() in _deliver. This
            # function is reached from _process_leased_message WITHOUT ever
            # going through _deliver, and -- unlike a peer message -- it
            # doesn't just risk an unwanted injection: it ACKs (irreversibly
            # consumes) the Routine source itself via _ack_persisted_ownership
            # below, before the human ever sees the notice (activation happens
            # later, in a separate _flush_notifications poll). Proven live with
            # the real agent/estop.py sentinel: under `hermes pause`, this
            # path acked the source AND (via the later flush) injected into the
            # human's session -- only the unrelated _deliver peer-turn path was
            # blocked. Refuse before validating, persisting, enqueuing, or
            # acking anything: leave the source entirely untouched so Mupot's
            # own visibility lease expires and redelivers it once `hermes
            # resume` lifts the pause. THIRD named gap (re-gate #2): a bare
            # `return` here left the message unmarked-processed, which
            # `_poll_loop` then read as a protocol violation and answered with
            # `_quarantine_inbox_polling()` -- a DURABLE, `connect()`-refusing
            # state that survives the pause and needs a manual
            # `reconcile_inbox_polling()` to clear. Raising `_EstopDeferred`
            # instead lets `_poll_loop` recognise "deferred by a pause" and
            # release the lease without ever recording a "pending"
            # reconciliation state.
            logger.warning(
                "[mupot] deferring Routine event message=%s: Hermes global "
                "emergency stop is engaged",
                message_id,
            )
            raise _EstopDeferred(message_id)
        try:
            event = validate_routine_event(message)
        except RoutineEventValidationError:
            quarantine_routine_event(
                self._state, self.store, message, "invalid_routine_event"
            )
            await self._ack_expected(message_id, attempt_id=attempt_id)
            self._commit(message_id)
            return

        ack_ownership = self._delivery_ack_ownership(attempt_id)
        persist_routine_receipt(
            self._state,
            self.store,
            message,
            event,
            ack_ownership,
        )
        enqueue(
            self._state,
            self.store,
            message,
            event.notice,
            activation_required=True,
            activation_after_processed=True,
        )
        await self._ack_persisted_ownership(event.source_id, ack_ownership)
        mark_routine_processed(self._state, self.store, event.source_id)

    async def _replay_routine_events(self) -> None:
        """Close crash windows by retrying the exact source ACK before activation."""
        if not self.routine_events_enabled:
            return
        from .notifications import enqueue
        from .routine_events import (
            mark_routine_processed,
            pending_routine_receipts,
            validate_routine_event,
        )

        for record in pending_routine_receipts(self._state):
            source = record["source"]
            event = validate_routine_event(source)
            enqueue(
                self._state,
                self.store,
                source,
                event.notice,
                activation_required=True,
                activation_after_processed=True,
            )
            await self._ack_persisted_ownership(
                event.source_id,
                record["ack_ownership"],
            )
            mark_routine_processed(self._state, self.store, event.source_id)

    async def _handle_ack_envelope(
        self,
        message: dict[str, Any],
        attempt_id: Optional[str] = None,
    ) -> None:
        """Quarantine incomplete ACKs; persist complete receipts before ACKing."""
        message_id = str(message.get("id") or "")
        if _estop_engaged():
            # Same class as _handle_routine_event's gate above: a peer terminal-ACK
            # body is enqueue()'d into the notification outbox verbatim (see
            # notifications.py's flush() for how that body reaches a human) and
            # this function ACKs the source itself, neither of which goes through
            # _deliver. Refuse before touching any durable state, same as _deliver
            # and _handle_routine_event -- and raise _EstopDeferred rather than
            # return bare, so the poll loop treats this as a pause to wait out,
            # never as a protocol violation to quarantine.
            logger.warning(
                "[mupot] deferring ack envelope message=%s: Hermes global "
                "emergency stop is engaged",
                message_id,
            )
            raise _EstopDeferred(message_id)
        if not is_terminal_ack(message):
            # P3 (kasra-review re-gate #4, 2026-09-14): same class as the
            # sender_policy DLQ write in _process_leased_message above -- a
            # pause between this write and the ack below (_ack_expected's
            # own _refuse_ack_if_estop_engaged choke point) leaves the
            # message unacked, so it redelivers and this branch runs again.
            # Append only if not already present, so redelivery after a
            # mid-write pause produces exactly one row.
            quarantined = self._state.get("dlq") or []
            if not any(
                str((entry.get("message") or {}).get("id") or "") == message_id
                for entry in quarantined
            ):
                quarantined.append({"message": dict(message), "reason": "invalid_ack_envelope"})
            self._state["dlq"] = quarantined[-100:]
            self.store.save(self._state)
            if message_id:
                await self._ack_expected(message_id, attempt_id=attempt_id)
                self._commit(message_id)
            return
        receipts = deque(self._state.get("terminal_receipts") or [], maxlen=100)
        if not any(str(receipt.get("id") or "") == message_id for receipt in receipts):
            receipts.append(dict(message))
        self._state["terminal_receipts"] = list(receipts)
        # A persistence error occurs before inbox_ack, leaving the lease
        # available for retry and preserving the source receipt.
        self.store.save(self._state)
        if self.notification_recipients:
            from .notifications import enqueue
            enqueue(self._state, self.store, message, str(message.get("body") or ""))
        await self._ack_expected(message_id, attempt_id=attempt_id)
        self._commit(message_id)

    async def _deliver(
        self,
        message: dict[str, Any],
        attempt_id: Optional[str] = None,
    ) -> None:
        message_id = str(message.get("id") or "")
        if _estop_engaged():
            # Refuse before touching any durable state: leave the message unacked so
            # Mupot's own visibility lease expires and redelivers it once `hermes
            # resume` lifts the pause. Raise _EstopDeferred (not a bare return) so
            # the poll loop treats this as a pause to wait out, never as a protocol
            # violation that needs `_quarantine_inbox_polling()`.
            logger.warning(
                "[mupot] deferring leased message=%s: Hermes global emergency stop is engaged",
                message_id,
            )
            raise _EstopDeferred(message_id)
        self._state["pending"] = {"message": message}
        self.store.save(self._state)
        event, runtime = self._begin_delivery(message, attempt_id=attempt_id)
        if self._expire_if_needed(runtime):
            # Class fix (2026-09-15, see _LeaseExpiredDeferred): the lease was
            # already gone before the turn could even start -- a deferral, not
            # a violation, and the turn never ran so there is no ambiguity
            # about side effects to fence.
            #
            # Round 2 (kasra-review re-gate BLOCK-1): pending was previously
            # left exactly as set above, which meant a restart read it as
            # ambiguous legacy work (`_legacy_pending_ambiguous`) and
            # `connect()` refused forever -- the same durable-brick shape
            # this whole fix exists to close, just moved to a different
            # field. Clear it: nothing about this deferral is a candidate
            # for `reply_outbox` custody.
            logger.warning("[mupot] refusing expired leased message=%s", message_id)
            self._state["pending"] = None
            self.store.save(self._state)
            raise _LeaseExpiredDeferred(message_id)
        token = _delivery_context.set(runtime.context)
        try:
            await self.handle_message(event)
        finally:
            _delivery_context.reset(token)
        timed_out = False
        try:
            remaining = max(0.0, runtime.expires_at - time.time())
            await asyncio.wait_for(runtime.completion_event.wait(), remaining)
        except asyncio.TimeoutError:
            timed_out = True
            self._invalidate_delivery(runtime)
            logger.error("[mupot] turn timeout message=%s", message_id)
        except asyncio.CancelledError:
            self._invalidate_delivery(runtime, ProcessingOutcome.CANCELLED)
            await self._cancel_delivery_processing(runtime)
            self.store.save(self._state)
            raise
        if runtime.invalidated and not timed_out and runtime.outcome != ProcessingOutcome.SUCCESS:
            # Invalidated by something other than this call's own wait timing
            # out -- e.g. `disconnect()` invalidating every live generation
            # directly while this delivery was still in flight. Not a lease
            # expiry; unchanged from pre-fix behavior.
            await self._cancel_delivery_processing(runtime)
            self.store.save(self._state)
            return
        if timed_out:
            await self._cancel_delivery_processing(runtime)
            if self._lease_is_binding(runtime.turn_deadline, runtime.lease_deadline):
                # The message's own lease deadline, not `turn_timeout`, was
                # what `asyncio.wait_for`'s `remaining` actually hit -- a
                # genuine lease expiry, never the turn's fault. Same
                # BLOCK-1 fix as the pre-turn branch above: clear `pending`
                # so a restart does not read this as ambiguous crash state.
                logger.warning(
                    "[mupot] refusing leased message=%s: its own visibility "
                    "lease expired mid-turn",
                    message_id,
                )
                self._state["pending"] = None
                self.store.save(self._state)
                raise _LeaseExpiredDeferred(message_id)
            # Round 2 (kasra-review re-gate BLOCK-2): `turn_timeout` was the
            # binding constraint here, WITH THE LEASE STILL LIVE -- a turn
            # that hung, not a message whose lease ran out. Pre-round-2 this
            # unconditionally raised `_LeaseExpiredDeferred` too, which
            # `_poll_loop` releases and the server immediately redelivers --
            # an unbounded re-execution loop for a turn that will very
            # likely hang again (A/B measured 1 turn before this fix vs 11+
            # and climbing). Bounded here by `_resolve_turn_failure` instead,
            # which (round 3, BLOCK-A) clears `pending` itself before
            # deferring -- see that function's docstring for why a below-cap
            # deferral is not the crash-ambiguity case `pending` exists to
            # fence.
            await self._resolve_turn_failure(message, attempt_id, "turn_timeout")
            return
        self._invalidate_delivery(
            runtime,
            runtime.outcome or ProcessingOutcome.FAILURE,
        )
        if runtime.outcome == ProcessingOutcome.SUCCESS:
            if not self._reply_has_human_custody(message_id):
                # Hermes treats an empty response as a successful no-op. A
                # peer request is not consumable until a terminal ACK has a
                # concrete Mupot receipt and its human notice has custody.
                #
                # Round 2 (kasra-review re-gate WARN): this used to be a bare
                # `return` -- unacked, unprocessed, and NOT raised as a
                # deferral, so `_poll_loop`'s "message not in processed"
                # check folded it into `_protocol_error()` ->
                # `_quarantine_inbox_polling()`, exactly the class of defect
                # this whole fix exists to close. It is a turn failure
                # (`no_custody`), bounded the same way `turn_timeout` is.
                await self._resolve_turn_failure(message, attempt_id, "no_custody")
                return
            durable_record = self._validated_reply_record(
                message_id,
                self._state.get("reply_outbox", {}).get(message_id),
                runtime.source,
            )
            await self._ack_persisted_ownership(
                message_id,
                durable_record["ack_ownership"],
            )
            self._commit(message_id)
            self._mark_reply_complete(message_id)
            return
        # Round 2 (kasra-review re-gate WARN): `runtime.outcome` here is
        # `ProcessingOutcome.FAILURE` -- the handler raised and
        # `BasePlatformAdapter`'s own `except BaseException` recorded the
        # failure outcome via `on_processing_complete` rather than letting
        # the exception propagate through this function. This used to be a
        # bare `return` with the same quarantine consequence as the
        # `no_custody` case above; same bounded turn-failure treatment.
        await self._resolve_turn_failure(message, attempt_id, "handler_error")

    async def _resolve_turn_failure(
        self,
        message: dict[str, Any],
        attempt_id: Optional[str],
        kind: str,
    ) -> None:
        """Bound a TURN failure (never a lease expiry) by `delivery_attempts`.

        See `_TurnFailureDeferred` for the full class rationale (round 4,
        Athena second eye, head c68e0839: the `pending` custody guard and
        the at-cap no-ack change are both documented there in full).

        Round 3 (kasra re-gate BLOCK-A): `pending` is not a side-effect
        ledger -- it is the "a turn is running right now in this process"
        record and NOTHING else. This raise, like every `_DeliveryDeferred`
        raise site, means the turn is no longer running in this process, so
        `pending` is cleared here, same as the lease-expiry sites above --
        UNLESS (round 4) a non-`"complete"` `reply_outbox` entry is already
        staged for this exact `message_id`, in which case clearing it would
        race the very next tick's `_replay_reply_outbox` into treating a
        genuinely-delivered error reply as an orphaned, unmatched record and
        escalating it to `reconciliation_required`. Below
        `self.max_delivery_attempts`, defer for natural lease-driven
        redelivery (raise, same shape as any `_DeliveryDeferred`). At the
        cap, do not retry further, but (round 4) do not ack either -- see
        `_TurnFailureDeferred`'s docstring for why acking here would hide
        the dead-letter from the pot's own reaper. DLQ it with `kind` as the
        reason, log one WARNING, and raise `terminal=True` so the poll loop
        treats this exactly like any other deferral: release the lease and
        move on, never a durable quarantine and never unbounded
        re-execution (the pot's own reaper takes it from here).
        """
        message_id = str(message.get("id") or "")
        raw_attempts = message.get("delivery_attempts")
        attempts = raw_attempts if isinstance(raw_attempts, int) and raw_attempts > 0 else 1
        staged = self._state.get("reply_outbox", {}).get(message_id)
        staged_reply_open = isinstance(staged, dict) and staged.get("status") != "complete"
        if attempts < self.max_delivery_attempts:
            logger.warning(
                "[mupot] deferring leased message=%s after turn failure kind=%s "
                "(delivery_attempts=%s/%s); leaving lease to expire for redelivery",
                message_id,
                kind,
                attempts,
                self.max_delivery_attempts,
            )
            if not staged_reply_open:
                self._state["pending"] = None
            self.store.save(self._state)
            raise _TurnFailureDeferred(message_id, kind)
        logger.warning(
            "[mupot] leased message=%s reached max_delivery_attempts=%s after "
            "turn failure kind=%s -- deferring for the pot's own reaper to "
            "dead-letter (not acking: an ack sets read_at, which the pot's "
            "reaper requires NULL to ever set dead_lettered_at)",
            message_id,
            self.max_delivery_attempts,
            kind,
        )
        dlq = list(self._state.get("dlq") or [])
        if not any(
            str((entry.get("message") or {}).get("id") or "") == message_id
            for entry in dlq
        ):
            dlq.append({"message": dict(message), "reason": kind})
        self._state["dlq"] = dlq[-100:]
        if not staged_reply_open:
            self._state["pending"] = None
        self.store.save(self._state)
        raise _TurnFailureDeferred(message_id, kind, terminal=True)

    async def on_processing_start(self, event: MessageEvent) -> None:
        runtime = self._runtime_for_event(event)
        if runtime is None or self._expire_if_needed(runtime):
            _delivery_context.set(None)
            return
        _delivery_context.set(runtime.context)

    async def _ack_expected(
        self,
        expected_id: str,
        attempt_id: Optional[str] = None,
    ) -> None:
        # Choke point (see _refuse_ack_if_estop_engaged's docstring): covers
        # both the attempt-based inbox_lease_ack branch below AND the legacy
        # inbox_ack branch, plus every caller (the three ungated
        # _process_leased_message branches this now closes, and
        # _ack_persisted_ownership's own legacy fallback).
        _refuse_ack_if_estop_engaged(expected_id)
        if attempt_id is not None:
            marker = _lease_reconciliation_proof(
                self._state.get("lease_reconciliation")
            )
            current_owner_fingerprint = _profile_owner_fingerprint(
                self._secret_owner,
                validate=True,
            )
            if (
                marker is None
                or marker.get("version") != _LEASE_ATTEMPT_MARKER_VERSION
                or marker.get("attempt_id") != attempt_id
                or current_owner_fingerprint != self._profile_owner_fingerprint
                or marker.get("profile_owner_fingerprint")
                != current_owner_fingerprint
            ):
                raise _protocol_error()
            payload = await self._call_consumer(
                "inbox_lease_ack",
                {"attempt_id": attempt_id},
            )
            receipt = validate_lease_attempt_ack(payload, attempt_id, marker)
            if receipt["state"] != "acked" or receipt["consumed"] is not True:
                raise _protocol_error()
            logger.info(
                "[mupot] inbox_lease_ack attempt=%s state=acked",
                attempt_id,
            )
            return
        payload = await self._call_consumer(
            "inbox_ack",
            {"ids": [expected_id]},
        )
        if not isinstance(payload, dict):
            raise _protocol_error()
        categories: list[set[str]] = []
        for name in ("acked", "already_read", "refused"):
            values = payload.get(name)
            if (
                not isinstance(values, list)
                or any(not isinstance(value, str) or not value.strip() for value in values)
            ):
                raise _protocol_error()
            category = set(values)
            if category - {expected_id}:
                raise _protocol_error()
            categories.append(category)
        acked, already_read, refused = categories
        matched = sum(expected_id in category for category in categories)
        if expected_id in refused or matched != 1:
            raise _protocol_error()
        state = "acked" if expected_id in acked else "already_read"
        logger.info("[mupot] inbox_ack message=%s state=%s", expected_id, state)

    def _commit(self, message_id: str) -> None:
        processed = deque(self._state.get("processed") or [], maxlen=1000)
        if message_id not in processed:
            processed.append(message_id)
        self._state["processed"] = list(processed)
        self._state["pending"] = None
        self.store.save(self._state)

    async def on_processing_complete(
        self, event: MessageEvent, outcome: ProcessingOutcome
    ) -> None:
        runtime = self._runtime_for_event(event)
        current = _delivery_context.get()
        if (
            runtime is None
            or current is None
            or runtime.context is not current
            or self._expire_if_needed(runtime)
        ):
            logger.debug(
                "[mupot] ignoring unbound lifecycle completion event=%s outcome=%s",
                event.message_id,
                outcome,
            )
            return
        logger.info(
            "[mupot] lifecycle complete message=%s outcome=%s",
            runtime.context.source_id,
            outcome,
        )
        runtime.outcome = outcome
        runtime.completion_event.set()

    async def _flush_notifications(self) -> None:
        from .notifications import flush
        if (
            self.notification_activate or self.routine_events_enabled
        ) and self.message_injector is None:
            raise RuntimeError("Mupot notification activation has no native plugin injector")
        await flush(
            self._state,
            self.store,
            self.notification_recipients,
            activate=(
                self.message_injector
                if self.notification_activate or self.routine_events_enabled
                else None
            ),
            activation_default=self.notification_activate,
        )

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        runtime = self._runtime_for_current_context()
        context = runtime.context if runtime is not None else None
        recipient = normalize_agent(chat_id)
        metadata = metadata or {}
        context_consistent = (
            context is not None
            and recipient == context.sender
            and (reply_to is None or str(reply_to) == context.source_id)
            and (
                "project_id" not in metadata
                or self._optional_text(metadata.get("project_id")) == context.project
            )
            and (
                "request_id" not in metadata
                or self._optional_text(metadata.get("request_id")) == context.request_id
            )
            and (
                "mupot_message_id" not in metadata
                or str(metadata.get("mupot_message_id") or "") == context.source_id
            )
        )
        if not context_consistent:
            logger.warning("[mupot] response send refused without live delivery context")
            return SendResult(
                success=False,
                error=_GENERIC_DELIVERY_CONTEXT_ERROR,
                retryable=False,
                error_kind="unknown",
            )
        assert runtime is not None
        assert context is not None
        inbound_id = context.source_id
        interim = metadata.get("_interim_send") is True
        arguments: dict[str, Any] = {}
        try:
            if interim:
                # Same peer `send` choke point as _transmit_final_reply's
                # "prepared" branch above: this is the OTHER call site that
                # ever invokes the `send` MCP tool. Refuse before building the
                # progress-ACK arguments or touching the network while paused;
                # `send()`'s own except-Exception below turns this into an
                # ordinary retryable SendResult failure for Hermes. Its error
                # string carries "estop_paused" (see the except clause below)
                # so the reason is visible in Hermes's own log line, not just
                # here.
                if _estop_engaged():
                    # P2 (kasra-review re-gate #4, 2026-09-14): refused
                    # silently before -- once per pause window, not once per
                    # interim send attempt.
                    _note_estop_pause_once(
                        "send_interim",
                        "[mupot] refusing interim progress-ACK send: Hermes "
                        "global emergency stop is engaged",
                    )
                    raise _EstopDeferred(inbound_id)
                arguments = {
                    "to": recipient,
                    "body": str(content),
                    "kind": "ack",
                    "request_id": _progress_request_id(inbound_id, str(content)),
                    "in_reply_to": inbound_id,
                }
                if context.project:
                    arguments["project_id"] = context.project
                result = await asyncio.wait_for(
                    self._send_client.call("send", arguments),
                    timeout=self.rpc_timeout,
                )
                receipt = validate_send_receipt(
                    result,
                    arguments["to"],
                    arguments.get("project_id"),
                )
            else:
                record = self._prepare_final_reply(runtime, recipient, str(content))
                arguments = record["arguments"]
                receipt = await self._transmit_final_reply(record)
            return SendResult(
                success=True,
                message_id=receipt["id"],
                raw_response=receipt,
            )
        except _EstopDeferred as exc:
            # Item 4 (kasra-review re-gate round 2, 2026-09-15): kept narrow
            # to `_EstopDeferred` deliberately, not widened to the
            # `_DeliveryDeferred` base -- `send()` only ever raises this
            # itself (the interim-send gate above) or receives it from
            # `_transmit_final_reply`'s own choke point; neither path ever
            # calls `_deliver`, so `_LeaseExpiredDeferred`/
            # `_TurnFailureDeferred` are structurally impossible here.
            #
            # P2 (kasra-review re-gate #4, 2026-09-14): before this fix, a
            # pause refusal fell into the generic `except Exception` below
            # and surfaced as `error=str(exc)` -- a bare message/source id
            # with no mention of a pause, so Hermes's own log line for the
            # refused send gave no clue why. The choke point that raised
            # this (this function's own interim-send gate, or
            # `_transmit_final_reply`'s "prepared" gate for the non-interim
            # branch) already logged the pause once per pause window at
            # INFO; this is just the SendResult surface, retryable once
            # resumed.
            #
            # F5/F6 (kasra-review re-gate #5, 2026-09-14): that INFO line is
            # easy to miss for a FINAL reply specifically -- landing at
            # `response send failed ... error=src-final` with no mention of
            # a pause was the OLD (pre-round-4) behaviour, and the WARNING
            # this except clause used to fall through to at
            # `except Exception` below is now unreachable for the pause
            # case. A refused interim progress-ACK is low-stakes (Hermes
            # gets no update, but nothing was ever waiting on it); a refused
            # FINAL reply means the human/peer never gets an answer at all.
            # Emit one additional WARNING per pause window, naming the
            # request id, for that case -- a distinct site so it tracks its
            # own once-per-window state independent of the INFO above.
            if not interim:
                _note_estop_pause_once(
                    "send_final_reply_deferred_warn",
                    f"[mupot] refusing final reply request_id={context.request_id}: "
                    "Hermes global emergency stop is engaged -- this plugin "
                    "schedules no retry; the reply surfaces to Hermes only as "
                    "a retryable SendResult failure",
                    level=logging.WARNING,
                )
            return SendResult(
                success=False,
                error=f"estop_paused: Hermes global emergency stop is engaged (message={exc})",
                retryable=True,
                error_kind="transient",
            )
        except Exception as exc:
            permanent = isinstance(exc, MupotProtocolError)
            logger.warning(
                "[mupot] response send failed message=%s request_id=%s error=%s",
                inbound_id,
                arguments.get("request_id"),
                exc,
            )
            return SendResult(
                success=False,
                error=str(exc),
                retryable=not permanent,
                error_kind="unknown" if permanent else "transient",
            )

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        agent = normalize_agent(chat_id)
        return {"name": agent, "type": "dm", "chat_id": agent}


def _requirements_available() -> bool:
    try:
        import mcp  # noqa: F401
        return True
    except ImportError:
        return False


def _is_connected(config: PlatformConfig) -> bool:
    extra = config.extra or {}
    return bool(config.enabled and extra.get("mcp_server"))


def _apply_yaml_config(_yaml: dict, platform: dict) -> dict:
    return {
        key: value
        for key, value in platform.items()
        if key not in {
            "enabled",
            "home_channel",
            "reply_to_mode",
            "typing_indicator",
            "gateway_restart_notification",
        }
    }


def register(
    ctx,
    *,
    expected_agent_id=None,
    expected_tenant=None,
    secret_owner: ProfileSecretOwner | None = None,
) -> None:
    # Populated by adapter_factory once Hermes actually connects the platform, so the
    # status tool below can report on the live instance's local state (stranded
    # notifications) without a second, independent path into the adapter's storage.
    _live_adapter: list["MupotAdapter"] = []

    def adapter_factory(config):
        extra = dict(config.extra or {})
        if expected_agent_id is not None or expected_tenant is not None:
            extra.update(expected_agent_id=expected_agent_id, expected_tenant=expected_tenant)

        def client_factory(server_name: str) -> HermesMCPClient:
            return HermesMCPClient(server_name, secret_owner=secret_owner)

        instance = MupotAdapter(
            replace(config, extra=extra),
            client_factory=client_factory,
            message_injector=ctx.inject_message,
            secret_owner=secret_owner,
        )
        _live_adapter[:] = [instance]

        # mupot_gateway_status must never report a DEAD adapter's stale state as
        # if it were live: wrap disconnect() so the moment Hermes tears this
        # instance down, the status tool's `_live_adapter[0] if _live_adapter
        # else None` lookup goes back to None (native_gateway_not_connected)
        # instead of continuing to answer from an adapter that no longer polls,
        # acks, or activates anything.
        original_disconnect = instance.disconnect

        async def _disconnect_and_clear_live_adapter() -> None:
            try:
                await original_disconnect()
            finally:
                if _live_adapter and _live_adapter[0] is instance:
                    _live_adapter.clear()

        instance.disconnect = _disconnect_and_clear_live_adapter  # type: ignore[method-assign]
        return instance

    ctx.register_platform(
        name="mupot",
        label="Mupot",
        adapter_factory=adapter_factory,
        check_fn=_requirements_available,
        is_connected=_is_connected,
        apply_yaml_config_fn=_apply_yaml_config,
        emoji="🪴",
        platform_hint=(
            "You are responding over Mupot as your authenticated agent identity. "
            "Respect project_id and task/flight governance. When request_id is "
            "present, begin the response with {ack_for:<request_id>}. Reply directly "
            "and include concrete evidence rather than acknowledgement loops."
        ),
    )

    def gateway_status(args: dict[str, Any], **_metadata: Any) -> str:
        # tools/registry.py's dispatch() always calls entry.handler(args, **kwargs)
        # with task_id/session_id/user_task (see model_tools.py's _execute_tool
        # dispatch_kwargs) — **_metadata absorbs those the same way
        # mupot_operator.py's build_operator_handlers wrapper does; without it any
        # real Hermes dispatch raises "unexpected keyword argument 'task_id'".
        adapter = _live_adapter[0] if _live_adapter else None
        if adapter is None:
            value = {"ok": False, "error": "native_gateway_not_connected"}
        else:
            # An adapter can exist here (constructed, registered) while unable
            # to actually poll -- e.g. a durable `lease_reconciliation` marker
            # refusing `connect()` -- so `"ok": true` alone was a blind spot
            # (2026-09-15 kayhermes incident): the tool reported healthy while
            # `connect()` refused on every operator restart. Surface both.
            value = {
                "ok": True,
                "connected": adapter.is_connected,
                # Item 6 (kasra-review re-gate round 2, 2026-09-15):
                # `connected` alone cannot distinguish "not yet attempted"
                # from "connect() is mid-way through its bounded
                # auto-reconcile turn right now" -- both report `False`.
                "reconciling": adapter._reconciling,
                "lease_reconciliation": adapter.lease_reconciliation_status(),
                "stranded_notifications": adapter.stranded_notifications(),
                "turn_failure_dlq": adapter.turn_failure_dlq_summary(),
            }
        return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)

    register_tool = getattr(ctx, "register_tool", None)
    if callable(register_tool):
        register_tool(
            name="mupot_gateway_status",
            handler=gateway_status,
            schema={
                "name": "mupot_gateway_status",
                "description": (
                    "Report native Mupot gateway health that nothing else surfaces, "
                    "including notifications stranded in activation_unknown or "
                    "transport_unknown (interrupted mid-flight; require operator "
                    "reconciliation)."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
            },
            toolset="mupot-operator",
        )

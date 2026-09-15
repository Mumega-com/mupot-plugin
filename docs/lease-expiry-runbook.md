# Lease expiry runbook

## Incident this closes (2026-09-15)

A live MCP tool call blocked near its own timeout (~180s). The message's
lease expired while `_deliver` was still waiting on it. `_deliver` returned
silently at both its expiry checkpoints, so `_poll_loop` read "message not
in processed" as a genuine protocol violation and called
`_quarantine_inbox_polling()` — a durable `lease_reconciliation` marker that
then refused `connect()` across every subsequent restart, because nothing
ever called `reconcile_inbox_polling()` for it. Four bricks in one day.

## Round 2 (2026-09-15): turn ended without custody is one class, not two

Round 1 fixed only the message's own lease expiring. Athena's gate executed
the class against this PR's OWN shipped 17:06 incident fixture
(`tests/fixtures/state.json.bak-quarantine-20260915170649`) and found a
second, un-fixed instance: that fixture's `lease_expires_at` is ~41s AFTER
the quarantine snapshot's own mtime — the lease was still LIVE when
`turn_timeout` (300s, smaller than `lease_seconds` on that host) fired
first. Round 1 explicitly filed that branch as "Failed-bounded, out of this
PR's class, unchanged base behaviour" — but master and round 1 are
IDENTICAL there: `_deliver` still returns silently, `_poll_loop` still reads
that as a violation. Turn ended without custody is turn ended without
custody regardless of which deadline fired first; both are now the same
deferral (`_DeliveryDeferred`, `reason` is `"lease_expired"` or
`"turn_timeout"`), sharing one predicate (`_reply_staged_incomplete`) for
whether it is safe to drop `pending`.

## Round 4 (2026-09-15): the class was still only 2 of 5 exits

The adversarial gate on round 3 (BLOCK-2) counted every terminal exit of
`_deliver` and found it has FIVE, not two: the message's own lease expiring
and `turn_timeout`-with-a-live-lease were fixed above, but three more
custody-less exits still `return`ed silently into the exact same
`_poll_loop` "message not in processed" → `_quarantine_inbox_polling()`
path — the SAME incident class, unfixed on three branches:

- **`empty_output`** — `outcome == SUCCESS` but no reply ever reached human
  custody. Hermes scores an empty/no-op response as a successful turn
  (`processing_ok = not bool(response)` when nothing was ever sent), so a
  handler that produces no reply at all looked identical to a genuine
  success with nothing to defer.
- **`handler_error`** — the handler raised. Hermes's own turn-error handling
  catches this and (separately) tries to tell the human the turn failed —
  but `_deliver` itself still just fell through a bare `return` on the
  resulting `FAILURE` outcome.
- **`runtime_invalidated`** — `runtime.invalidated` with a non-`SUCCESS`
  outcome, reachable from `disconnect()` mid-turn or `_expire_if_needed`
  inside `_active_delivery`.

All five exits now raise the same `_DeliveryDeferred`, with the same
`_reply_staged_incomplete` gate on whether it is safe to drop `pending`. No
new state, no new write path — three more strings
(`empty_output`/`handler_error`/`runtime_invalidated`) added to
`_DeliveryDeferred._REASONS` is the only thing that grew.

## Round 5 (2026-09-15): a silent stall is worse than a loud brick, and a dead attempt is a deferral

The adversarial gate on round 4 found the class fix had introduced a new,
quieter failure shape rather than closing the incident entirely:

- **BLOCK-1 (P0) — the reply-replay-stall wedge.** `_poll_loop`'s statement
  order is: replay routine events, replay the reply outbox, flush
  notifications, THEN `inbox_lease`. `_replay_reply_outbox`'s bare
  `except Exception` clause used to `continue` on ANY non-protocol failure
  (e.g. the peer's `send` transport being down) — skipping `inbox_lease` for
  the rest of that tick. If the same staged reply keeps failing to
  transmit, it fails on EVERY tick, forever, and `inbox_lease` never runs
  again for ANY message — with `mupot_gateway_status` reporting
  `connected: true`, no marker, and no field anywhere naming the stall.
  Reachable with a perfectly successful handler whose reply simply cannot
  reach the peer. Fixed by falling through instead of skipping: a failing
  replay no longer blocks `inbox_lease` for the rest of the tick (leasing
  message N+1 has never depended on message N's reply having transmitted —
  records are independent, keyed by their own `source_id`). The failing
  record's own retry stays bounded to at most one attempt per tick, same as
  before; no new rate limit was needed. The stall is now visible via
  `mupot_gateway_status.reply_replay_failures` — a non-persisted,
  in-memory-only counter (there is nothing durable to derive it from: the
  underlying failure, e.g. a transport exception, is never written to
  `reply_outbox`), incremented on each replay failure and reset to 0 the
  next time replay succeeds.

  **A real, separate downstream effect this does NOT fix (out of scope for
  this round):** once a SECOND message is delivered while the first
  record's reply is still unresolved, `_deliver` overwrites the single-slot
  `pending` marker for the new message. `_replay_reply_outbox`'s own
  "genuine crash ambiguity" check then no longer matches the orphaned first
  record against `pending`, and — correctly, per its existing design —
  escalates it to `reconciliation_required`, ending the poll loop. This is
  a PRE-EXISTING code path, not something this round introduced, and it
  fails LOUDLY (`reply_reconciliation_required: true`, a fatal error code)
  rather than silently, which is the property this round's fix cares about.
  It does mean a stuck reply's window to recover on its own is bounded by
  "until the next different message arrives," not indefinite.

- **BLOCK-2 (P1) — the restart flap.** A custodied reply (human custody
  already achieved) whose only attempt died mid-turn (e.g. `disconnect()`
  before its `inbox_lease_ack` ran) used to durably quarantine on the very
  first replay tick of every subsequent restart — and because the
  quarantine flag `_ack_persisted_ownership` set was never persisted to
  `state.json`, each fresh restart's `connect()` still returned `True`
  (nothing on disk said otherwise) and then died immediately, identically,
  forever: an infinite "looks healthy, dies right away" flap invisible to
  anything that only checks `connect()`'s return value. Fixed narrowly:
  `_ack_persisted_ownership` now treats a well-formed `inbox_lease_ack`
  response reporting `expired`/`cancelled`/`empty` (the server plainly
  saying this exact attempt can no longer be acked — a resolved,
  unambiguous outcome, not a transport failure) as a deferral rather than a
  durable ambiguity: since every caller already proved human custody exists
  before reaching this call, it is safe to commit locally exactly as if the
  ack had succeeded. No new state key: `processed` already dedups a future
  redelivery of the same message through `_process_leased_message`'s
  existing "already processed" branch, which acks under whatever attempt is
  CURRENT at that time via `_ack_expected` — the stale `ack_ownership`
  recorded on the reply record is simply never consulted again once the
  message is marked processed. A response reporting `leased` (a nonsensical
  answer to an ack call) is deliberately NOT included in this deferral —
  that stays a durable fence, same as any other malformed/unexpected ack
  response.

- **P2 — duplicate peer `send`.** The `empty_output`/`handler_error` exits
  were the only two of the five that never called
  `_cancel_delivery_processing(runtime)` — both left Hermes's own
  background session task free to keep running, which could race a peer
  `send` against `_replay_reply_outbox`'s own retry of the identical
  `request_id` (reproduced: a raising handler plus a slow peer `send`
  produced two `send` calls carrying the same `request_id` for one crashed
  turn). Now calls it, matching the other three exits — verified: exactly
  one send per `request_id`. (The `empty_output` exit's own call is
  structurally consistent with the other four but has no independently
  reachable duplicate-send scenario the way `handler_error`'s does — there
  is no analogous "Hermes auto-apology" mechanism for a clean empty return
  — so unlike `handler_error`'s, this specific call has no dedicated
  mutation-kill test; disclosed here rather than silently claimed.)
- **P2 — `lease_reconciliation_status()` under-reporting.** `required` used
  to be exactly `marker is not None` — but `_lease_quarantined` can be set
  (e.g. by `_ack_persisted_ownership`'s own remaining ambiguous-failure
  path, or `_quarantine_inbox_polling`'s persistence-failure branch)
  without a marker ever being written. Either state genuinely refuses
  `connect()`; `required` now also reflects `self._lease_quarantined`
  directly.
- **P2-3, re-examined — operator `reconcile_inbox_polling(execute_leased=True)`
  on a still-`leased` attempt.** Declined as a code change; documented
  instead (see "Operator procedure" below) — see why under item 5 there.
- **P2 — `lease_seconds` vs `turn_timeout` clamp.** Declined as a code
  change; see "Declined this round" below.

## The three states

Every inbox-polling outcome lands in exactly one of these. Only the last one
is durable and requires anything from an operator. There is no
"Failed-bounded" state distinct from Deferred any more: **every way a turn
can end without a reply reaching human custody is now Deferred** — handler
error and empty output included. Nothing here is bounded LOCALLY; only
Mupot's own server-side redelivery counter bounds it (see below).

| State | Trigger | Durable representation | Who reads it |
|---|---|---|---|
| **Delivered** | Handler succeeds, reply reaches human custody, ack+commit. **Includes `handler_error` when a live peer/notification transport is present**: Hermes's own `_notify_turn_error` fires from the same exception handler and calls `send()` on the crashing turn's behalf; when that auto-apology send succeeds, it IS the peer reply and the human notice both, and the very next replay tick acks+commits it normally — a decision request terminally consumed by an error handler, by design (PR #9, `c9b79aa`), not a bug and not this row's Deferred case. Only when that auto-apology's own send ALSO fails does `handler_error` land in Deferred below with nothing staged. | `processed` list, `reply_outbox[id].status == "complete"`. | Nothing further — done. |
| **Deferred** | Any of the five `_DeliveryDeferred` reasons (the message's own lease expires, `turn_timeout` fires while that lease is still live, the handler raises, the handler produces no reply at all, or the delivery runtime is invalidated mid-turn), or `hermes pause` is engaged (`_EstopDeferred`). | Nothing new written for this outcome itself — `pending` is cleared unless a non-complete reply record is already staged for the source (`_reply_staged_incomplete`), in which case `pending` is left untouched so that staged reply can still complete via replay. | `_poll_loop` releases the fence and lets Mupot redeliver. **Bounded server-side, not locally**: Mupot's own reaper dead-letters the message once its `delivery_attempts` counter reaches the server's `MAX_DELIVERY_ATTEMPTS` (5) — checked inline before every claim, so a message that keeps deferring is redelivered at most 5 times, then silently moved to the server's dead-letter state **with no notification to the human** that it happened. Nothing in this plugin watches for that; an operator who suspects a message is looping should check the redelivery count/dead-letter state on the Mupot side directly, not this plugin's own state.json (local attempt bounding is a separate, explicitly out-of-scope concern — issue #10). **A reply stuck failing to transmit (round 5) no longer blocks other messages from being leased** (see BLOCK-1 above) — watch `mupot_gateway_status.reply_replay_failures` (a non-persisted, in-process counter) for this specific shape rather than `poll_running`/`is_connected`, which stay green throughout. |
| **Violation** | A genuine protocol error, reserved for exactly four fence kinds: (1) a **tampered**/malformed server response that fails schema or fence-proof validation, (2) an **owner mismatch** (the active secret scope's fingerprint no longer matches the one the marker/pending state was fenced under), (3) an **attempt conflict** (the durable marker's `attempt_id`/`version` doesn't match what the current call is trying to reconcile), or (4) an **unknown-attempt** ack/commit (a message id or attempt id the local state has no record of) — **and, separately, the two `*_reconciliation_required` fatal paths below**, which are NOT `lease_reconciliation` markers but are equally durable/fatal for the running process. | `lease_reconciliation` marker (durable, `connect()`-refusing) for the four fence kinds above. **Two other, unrelated fatal paths land here too, cross-referenced because both are easy to mistake for the marker above:** (a) `mupot_reply_reconciliation_required` / `self._reply_reconciliation_required` — set by `_replay_reply_outbox`'s "genuine crash ambiguity" escalation (an orphaned non-complete reply record that no longer matches `pending`) or a legacy v1 record; recomputed at every `__init__` from `reply_outbox` record statuses, so IS effectively persisted (via the `"reconciliation_required"` status written onto the record itself), unlike (b). (b) `mupot_inbox_attempt_ack_reconciliation_required` / a second `_lease_quarantined = True` set inside `_ack_persisted_ownership`'s own genuinely-ambiguous-failure branch (a transport exception mid-ack, or a well-formed but non-`acked`/non-dead-attempt response such as `leased`) — this one is IN-MEMORY ONLY, not persisted to any key, which is exactly why the pre-round-5 "restart flap" (BLOCK-2) was invisible across restarts: `connect()` re-derives `_lease_quarantined` fresh from `state["lease_reconciliation"]` alone, so this flavor of quarantine silently resets to `False` on every fresh process even though the identical failure re-fires on the very next replay tick. | `reconcile_inbox_polling()` — auto-attempted once by `connect()` itself, but ONLY on a clean tombstone (see below); a genuinely still-fenced attempt is left exactly as it was. Path (a) above has **no code path that clears it** (see Operator procedure). Path (b) clears itself the moment a fresh restart's replay either succeeds outright or hits round 5's own dead-attempt deferral; only a GENUINELY ambiguous ack failure (not one of `expired`/`cancelled`/`empty`) re-quarantines it. |

## `connect()`'s automatic self-heal: what it will and will not do over the network

`connect()` attempts one bounded `reconcile_inbox_polling()`-equivalent call
whenever a `lease_reconciliation` marker is present and `hermes pause` is
not engaged. This always makes the two READ-ONLY preflight calls
(`inbox_consumer_status`, `inbox_lease_reconcile`) needed to ask the server
what actually happened to the fenced attempt — those never consume or ship
anything on their own, so making them is not a new egress concern. What the
answer determines:

- **Clean tombstone** (server says `empty`/`cancelled`/`expired`/`acked`,
  nothing left to consume) **and** no non-complete reply is staged for the
  marker's `pending` source: self-heals — clears the marker (and the stale
  `pending`, if any) with zero further network calls.
- **Still `leased`** (the server says the attempt genuinely still holds an
  outstanding message): `connect()`'s automatic call refuses to act on this
  — it does **not** call `_process_leased_message`/`_deliver`, does **not**
  run the model, does **not** ack anything. It logs and leaves the marker
  exactly as it was; `connect()` still refuses overall. Re-executing a turn
  as a side effect of a supposedly bounded, safe bootstrap call was the
  actual P1 here (round 1 did this unconditionally) — only an operator's own
  explicit `reconcile_inbox_polling()` call may process a still-`leased`
  attempt on purpose.
- **A reply is still staged, non-complete, for the `pending` source**: never
  clears, regardless of the attempt's server-side state — a countercase, see
  the operator procedure below.

## Operator procedure

- **Deferred (the common case): do nothing.** `mupot_gateway_status` reports
  `lease_reconciliation.required: false` once it clears on its own. No
  state.json edit is ever needed or safe to make here.
- **Violation (a real `lease_reconciliation` marker persists across a
  restart):**
  1. Check `mupot_gateway_status` — `lease_reconciliation.required` and
     `.attempt_id` tell you a marker is present and which attempt it is;
     `reply_reconciliation_required` tells you separately whether a reply
     record itself needs attention.
  2. `connect()` already attempts one bounded self-heal automatically, but
     (see above) only clears on a clean tombstone. If it was a clean
     tombstone, it clears on its own — just restart the gateway once and
     check status again.
  3. If it is still present after a restart, a **countercase** is holding
     it: the attempt is still genuinely `leased` server-side (wait, or use
     `reconcile_inbox_polling()` yourself deliberately, understanding that
     it may process the outstanding message), a reply is genuinely still
     staged for the `pending` source (do not touch it — let it replay), or
     the owner/attempt/tenant scope has rotated out from under a stale
     marker. Only in the countercase does this ever need a manual
     `state.json` edit, and only to remove the `lease_reconciliation` key —
     never `pending` or `reply_outbox`, and never while a reply for that
     source is still in flight.
  4. Never hand-mark a stuck message id `processed`. `_mark_reply_complete`
     now refuses to force a record to `"complete"` without a validated
     receipt (it would otherwise persist cleanly but fail the next load's
     validation, bricking `connect()` invisibly with no signal anywhere).
- **`reply_reconciliation_required` is `true` in `mupot_gateway_status`:**
  this is a SEPARATE signal from `lease_reconciliation` — a `reply_outbox`
  record's own status is `"reconciliation_required"` (set when
  `_replay_reply_outbox` finds a record it cannot safely resolve: a legacy
  v1 record, or one already marked `"reconciliation_required"` from a prior
  ambiguous-pending refusal). **There is currently no code path that clears
  this on its own** — `_replay_reply_outbox` re-raises on that exact status
  forever, and nothing in this plugin ever transitions a record out of it
  (F-C, adversarial gate, PR #11 round 3: "the signal is surfaced, the
  recovery is not"). The runbook states this plainly rather than inventing
  a recovery path that does not exist in code:
  1. Find the record: it is the entry in `reply_outbox` (in `state.json`)
     whose `status` is `"reconciliation_required"`; its key is the source
     message id.
  2. Determine independently — via Mupot's own delivery/receipt history, or
     by asking the human directly — whether that reply actually reached
     them. Never guess from local state alone; the whole reason this status
     exists is that the local record could not tell.
  3. If (and only if) you have confirmed the reply never reached the human
     and never will via this record, manually edit `state.json` and change
     that record's `"status"` to `"invalid_receipt"` — the one other
     terminal status `_replay_reply_outbox` and `_mark_reply_complete`
     already skip permanently and never re-walk (see `invalid_reply_receipts()`
     in `mupot_gateway_status`). Do not set it to `"complete"` — that
     requires a validated receipt this record does not have, and will brick
     the next `connect()` load (see point 4 above).
  4. If you have confirmed the reply DID reach the human (e.g. Mupot's own
     history shows a delivered receipt this record just never captured),
     leave the record as `"reconciliation_required"` and open an issue
     against this class instead of hand-editing state to `"complete"` —
     forcing completeness onto a record without a receipt is exactly the
     invisible-brick failure mode point 4 above exists to prevent.
- **`mupot_gateway_status.reply_replay_failures` is non-zero (round 5):** a
  specific staged reply is failing to transmit on every poll tick (most
  commonly: the peer `send` transport is down). This is a **Deferred**
  signal, not a fence — `inbox_lease` and every other message keep working
  normally throughout (that is exactly what round 5 fixed: this used to
  silently stop `inbox_lease` entirely instead). Diagnose the transport
  issue directly; there is nothing to reconcile in `state.json` for this by
  itself. It resets to 0 on its own the next time that record's replay
  succeeds. Watch also for it turning into a `reply_reconciliation_required`
  Violation shortly after a SECOND message is delivered (see BLOCK-1's
  downstream-effect note above) — that is the same underlying stuck record,
  now escalated once it can no longer match `pending`.
- **Calling `reconcile_inbox_polling()` yourself on a still-`leased`
  attempt (item 5, declined as a code change):** each call burns one real
  agent turn against the still-outstanding message and is not guaranteed to
  make progress — the attempt may still be genuinely claimed elsewhere, in
  which case the marker is left exactly as it was and only the log
  changes, or the turn may defer again for an unrelated reason (any of the
  five `_DeliveryDeferred` reasons), in which case the SAME nothing-changed
  outcome results. This is not a bug: `execute_leased=True` exists
  precisely so an operator can choose to force this on purpose, and
  `test_exact_leased_attempt_is_processed_and_acked_before_clear` pins that
  a genuinely still-outstanding `leased` attempt CAN complete successfully
  through exactly this call — the code cannot distinguish "will succeed
  this time" from "is hopeless" in advance without trying, so there is no
  safe way to auto-refuse only the hopeless case. Prefer waiting for the
  attempt to expire naturally (`connect()`'s own automatic self-heal, or a
  later `reconcile_inbox_polling()` call once the server reports a clean
  tombstone) unless there is a specific reason to force it now.

## What changed vs. before this fix

1. A turn ending without custody — the message's own lease expiring at
   either `_deliver` checkpoint, OR `turn_timeout` firing first while that
   lease is still live — now raises `_DeliveryDeferred` (an `_EstopDeferred`
   subclass; `_LeaseExpiredDeferred` is kept as a back-compat alias for the
   same class) instead of returning silently — never acked, never marked
   processed, never quarantined, either way.
2. `_replay_reply_outbox` tolerates a `pending` that no longer names a
   record which already holds a validated send receipt (status
   sent/custodied/complete) — that record completes via ordinary replay
   instead of escalating to `reconciliation_required`.
3. `reconcile_inbox_polling` clears the marker for an exact-scope
   expired/empty/cancelled/acked tombstone when no non-complete reply is
   staged for the attempt's `pending` source; it still fails closed on a
   staged reply or any owner/attempt/tenant/scope mismatch. Both `_deliver`'s
   deferral sites and this self-heal check now share one predicate
   (`_reply_staged_incomplete`) for "is a non-complete reply staged for this
   message" — round 1 had two different answers to that question that could
   disagree (Athena F1).
4. `connect()` attempts that reconcile once, automatically, before its
   ambiguous-pending refusal — the marker no longer requires an operator or
   a script attached to the live process to ever clear on a clean tombstone.
   It never re-executes a turn while doing so — a still-`leased` outcome is
   left exactly as it was (see above); only an operator's own explicit
   `reconcile_inbox_polling()` call may process one on purpose.
5. `lease_seconds` still defaults to `turn_timeout + mcp_tool_timeout + 60`
   (was `turn_timeout + 60`) — the formula is unchanged, but it is no longer
   load-bearing for the incident class this PR closes: after item 1 above, a
   live lease no longer needs to physically outlast `turn_timeout` for
   `_deliver` to defer safely, since a turn_timeout-with-a-live-lease now
   defers on its own. What the fix here is scoped to is HOW the configured
   MCP tool timeout is read: resolving it now happens inside the
   constructor's own secret scope, so it no longer leaks `.env` into the
   ambient process environment as a side effect of construction (P1-5).

   **Latency this raises (F-D, adversarial gate, PR #11 round 3, undocumented
   until now):** on the live `kayhermes` profile (`timeout: 180` → `mcp_tool_
   timeout=180`, `turn_timeout=300`), this formula raises the live lease from
   `turn_timeout + 60 = 360s` to `turn_timeout + mcp_tool_timeout + 60 = 540s`
   — a genuine 180s (3 min) increase in how long a single deferred attempt's
   lease stays live before Mupot can redeliver it. Concretely: a message that
   keeps deferring can now hide from redelivery for up to ~9 minutes per
   attempt (540s), and takes up to ~45 minutes (5 attempts × 9 min) to reach
   the server's `MAX_DELIVERY_ATTEMPTS` reaper bound and dead-letter — up
   from ~30 minutes (5 × 360s) before this round. This is strictly safer
   than the pre-fix behaviour (a wedged `connect()` that never redelivered
   at all), but it is a real, measurable increase in worst-case latency for
   anyone watching redelivery timing, not a free change.
6. `mupot_gateway_status` reports `connected`, `lease_reconciliation`
   (`required` + `attempt_id`), and `reply_reconciliation_required`, so the
   marker's and the reply outbox's live state are finally visible without a
   REPL attached to the process.
7. (Round 4) The three remaining custody-less `_deliver` exits — empty
   output, handler error, delivery-runtime invalidation — now raise
   `_DeliveryDeferred` the same as the two above, with three new `reason`
   values (`empty_output`, `handler_error`, `runtime_invalidated`).
   `reconcile_inbox_polling`'s own leased-attempt branch gained its own
   `except _DeliveryDeferred` clause (mirroring `_poll_loop`'s existing one)
   so its deferral log names the true cause instead of always claiming
   `hermes pause` is engaged. `_reply_staged_incomplete` now also excludes
   `invalid_receipt` (not just `complete`) from "still staged" — a clean
   tombstone with a matching `invalid_receipt` reply record could not
   previously clear `pending` across a restart at all.
8. (Round 5) `_poll_loop`'s reply-replay-failure handling no longer skips
   `inbox_lease` for the rest of the tick (BLOCK-1) — see that section
   above. `mupot_gateway_status` gained `reply_replay_failures` (a
   non-persisted, in-process counter). `_ack_persisted_ownership` now
   treats a well-formed `expired`/`cancelled`/`empty` ack response as a
   deferral rather than a durable ambiguity (BLOCK-2) — no new state key.
   The `empty_output`/`handler_error` exits now also call
   `_cancel_delivery_processing(runtime)`, matching the other three exits
   (closes a duplicate-peer-`send` race). `lease_reconciliation_status()`'s
   `required` field now also reflects `self._lease_quarantined` directly,
   not only a parseable marker.
9. **Declined this round, documented instead of code-changed:**
   - `lease_seconds` is NOT clamped to a floor of `turn_timeout + 60`. The
     derived/default formula (`turn_timeout + mcp_tool_timeout + 60`)
     already satisfies this floor unconditionally (`mcp_tool_timeout >= 0`),
     so the clamp would be a pure no-op there; it would only ever bite an
     EXPLICIT `lease_seconds` override — but
     `test_lease_seconds_explicit_config_bypasses_the_formula` is a
     round-2, deliberately-named, rationale-backed pin that an explicit
     value bypasses every formula-side computation, and several of this
     PR's own tests (e.g. the pre-turn/post-timeout lease-expiry scenarios)
     rely on being able to set `lease_seconds` smaller than `turn_timeout`
     on purpose to construct exactly the class this PR fixes. Silently
     overriding an explicit operator/test value contradicts that intent
     for near-zero live benefit (unreachable on the current default
     profile: `300 < 540`). If this is ever revisited, it needs a decision
     about whether "explicit config bypasses the formula" still holds, not
     a silent clamp.
   - `reconcile_inbox_polling(execute_leased=True)` still executes a turn
     against a still-`leased` attempt with no guarantee of progress — see
     the Operator procedure entry above for why this stays a documented
     property rather than a refusal: `test_exact_leased_attempt_is_
     processed_and_acked_before_clear` pins that this call CAN legitimately
     succeed on a genuinely still-outstanding attempt, so there is no safe
     way to distinguish "will succeed" from "is hopeless" without trying.

## Forensic note: 3 of 5 incident snapshots are the pre-lease fence shape

Of the five real `state.json.bak-quarantine-*` snapshots this fix's test
suite replays (`tests/fixtures/`), three (`...-021900`, `...-155821`,
`...-170106`) share one specific shape: `pending: None` with a durable v3
`lease_reconciliation` marker present. That is exactly what
`_persist_prelease_fence()` writes — the marker is persisted BEFORE the
`inbox_lease` RPC call is even attempted, as the `before_attempt` hook on
`_call_consumer`. In all three, the RPC itself never completed (a separate,
transport-level brick cause from the turn-ended-without-custody class this
PR fixes), leaving the fence marker durable with nothing behind it to
protect. `connect()`'s self-heal (item 4 above) recovers all three cleanly
on first connect — an unclaimed win of that fix, not something this PR
specifically targeted.

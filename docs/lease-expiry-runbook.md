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

## The three states

Every inbox-polling outcome lands in exactly one of these. Only the last one
is durable and requires anything from an operator. There is no
"Failed-bounded" state distinct from Deferred any more: **every way a turn
can end without a reply reaching human custody is now Deferred** — handler
error and empty output included. Nothing here is bounded LOCALLY; only
Mupot's own server-side redelivery counter bounds it (see below).

| State | Trigger | Durable representation | Who reads it |
|---|---|---|---|
| **Delivered** | Handler succeeds, reply reaches human custody, ack+commit. | `processed` list, `reply_outbox[id].status == "complete"`. | Nothing further — done. |
| **Deferred** | Any of the five `_DeliveryDeferred` reasons (the message's own lease expires, `turn_timeout` fires while that lease is still live, the handler raises, the handler produces no reply at all, or the delivery runtime is invalidated mid-turn), or `hermes pause` is engaged (`_EstopDeferred`). | Nothing new written for this outcome itself — `pending` is cleared unless a non-complete reply record is already staged for the source (`_reply_staged_incomplete`), in which case `pending` is left untouched so that staged reply can still complete via replay. | `_poll_loop` releases the fence and lets Mupot redeliver. **Bounded server-side, not locally**: Mupot's own reaper dead-letters the message once its `delivery_attempts` counter reaches the server's `MAX_DELIVERY_ATTEMPTS` (5) — checked inline before every claim, so a message that keeps deferring is redelivered at most 5 times, then silently moved to the server's dead-letter state **with no notification to the human** that it happened. Nothing in this plugin watches for that; an operator who suspects a message is looping should check the redelivery count/dead-letter state on the Mupot side directly, not this plugin's own state.json (local attempt bounding is a separate, explicitly out-of-scope concern — issue #10). |
| **Violation** | A genuine protocol error, reserved for exactly four fence kinds: (1) a **tampered**/malformed server response that fails schema or fence-proof validation, (2) an **owner mismatch** (the active secret scope's fingerprint no longer matches the one the marker/pending state was fenced under), (3) an **attempt conflict** (the durable marker's `attempt_id`/`version` doesn't match what the current call is trying to reconcile), or (4) an **unknown-attempt** ack/commit (a message id or attempt id the local state has no record of). | `lease_reconciliation` marker (durable, `connect()`-refusing). | `reconcile_inbox_polling()` — auto-attempted once by `connect()` itself, but ONLY on a clean tombstone (see below); a genuinely still-fenced attempt is left exactly as it was. |

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

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

## The four states

Every inbox-polling outcome lands in exactly one of these. Only the last one
is durable and requires anything from an operator.

| State | Trigger | Durable representation | Who reads it |
|---|---|---|---|
| **Delivered** | Handler succeeds, reply reaches human custody, ack+commit. | `processed` list, `reply_outbox[id].status == "complete"`. | Nothing further — done. |
| **Deferred** | The message's own lease expires, `turn_timeout` fires while that lease is still live (both: `_DeliveryDeferred`), or `hermes pause` is engaged (`_EstopDeferred`). | Nothing new written for this outcome itself — `pending` is cleared unless a non-complete reply record is already staged for the source (`_reply_staged_incomplete`), in which case `pending` is left untouched so that staged reply can still complete via replay. | `_poll_loop` releases the fence and lets Mupot redeliver; the pot's own reaper dead-letters at its server-side `MAX_DELIVERY_ATTEMPTS` if this recurs (local attempt bounding is a separate, explicitly out-of-scope concern — issue #10). |
| **Failed-bounded** | Handler error, empty output — anything that is NOT a timeout/lease-expiry deferral and NOT a clean success. | `pending` stays set (genuine crash ambiguity: was a reply staged before or after the crash?) until the next `_replay_reply_outbox`/`_deliver` retry resolves it. | Next retry; `_replay_reply_outbox` on the next `connect()`/poll tick. |
| **Violation** | Malformed server response, scope mismatch, or any genuine protocol error. | `lease_reconciliation` marker (durable, `connect()`-refusing). | `reconcile_inbox_polling()` — auto-attempted once by `connect()` itself, but ONLY on a clean tombstone (see below); a genuinely still-fenced attempt is left exactly as it was. |

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
6. `mupot_gateway_status` reports `connected`, `lease_reconciliation`
   (`required` + `attempt_id`), and `reply_reconciliation_required`, so the
   marker's and the reply outbox's live state are finally visible without a
   REPL attached to the process.

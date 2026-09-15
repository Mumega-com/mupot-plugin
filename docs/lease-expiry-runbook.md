# Lease expiry runbook

## Incident this closes (2026-09-15)

A live MCP tool call blocked near its own timeout (~180s). The message's
lease expired while `_deliver` was still waiting on it. `_deliver` returned
silently at both its expiry checkpoints, so `_poll_loop` read "message not
in processed" as a genuine protocol violation and called
`_quarantine_inbox_polling()` — a durable `lease_reconciliation` marker that
then refused `connect()` across every subsequent restart, because nothing
ever called `reconcile_inbox_polling()` for it. Four bricks in one day.

## The four states

Every inbox-polling outcome lands in exactly one of these. Only the last one
is durable and requires anything from an operator.

| State | Trigger | Durable representation | Who reads it |
|---|---|---|---|
| **Delivered** | Handler succeeds, reply reaches human custody, ack+commit. | `processed` list, `reply_outbox[id].status == "complete"`. | Nothing further — done. |
| **Deferred** | The message's own lease expires (`_LeaseExpiredDeferred`), or `hermes pause` is engaged (`_EstopDeferred`). | Nothing durable — `pending` cleared unless a validated reply receipt is staged for the source. | `_poll_loop` releases the fence and lets Mupot redeliver. |
| **Failed-bounded** | Turn timeout with a still-*live* lease, handler error, empty output. | `pending` stays set (real crash ambiguity) — **out of this PR's class**; unchanged base behaviour, tracked separately. | Next `_replay_reply_outbox`/`_deliver` retry. |
| **Violation** | Malformed server response, scope mismatch, or any genuine protocol error. | `lease_reconciliation` marker (durable, `connect()`-refusing). | `reconcile_inbox_polling()`, now auto-attempted once by `connect()` itself. |

## Operator procedure

- **Deferred (the common case): do nothing.** `mupot_gateway_status` reports
  `lease_reconciliation.required: false` once it clears on its own. No
  state.json edit is ever needed or safe to make here.
- **Violation (a real `lease_reconciliation` marker persists across a
  restart):**
  1. Check `mupot_gateway_status` — `lease_reconciliation.required` and
     `.attempt_id` tell you a marker is present and which attempt it is.
  2. `connect()` already attempts one bounded self-heal automatically. If it
     was a clean tombstone (the attempt is over server-side and nothing was
     staged for its `pending` source), it clears on its own — just restart
     the gateway once and check status again.
  3. If it is still present after a restart, a **countercase** is holding
     it: a reply is genuinely still staged for the `pending` source (do not
     touch it — let it replay), or the owner/attempt/tenant scope has
     rotated out from under a stale marker. Only in the countercase does
     this ever need a manual `state.json` edit, and only to remove the
     `lease_reconciliation` key — never `pending` or `reply_outbox`, and
     never while a reply for that source is still in flight.
  4. Never hand-mark a stuck message id `processed`. `_mark_reply_complete`
     now refuses to force a record to `"complete"` without a validated
     receipt (it would otherwise persist cleanly but fail the next load's
     validation, bricking `connect()` invisibly with no signal anywhere).

## What changed vs. before this fix

1. Lease expiry at both `_deliver` checkpoints now raises
   `_LeaseExpiredDeferred` (a `_EstopDeferred` subclass) instead of
   returning silently — never acked, never marked processed, never
   quarantined.
2. `_replay_reply_outbox` tolerates a `pending` that no longer names a
   record which already holds a validated send receipt (status
   sent/custodied/complete) — that record completes via ordinary replay
   instead of escalating to `reconciliation_required`.
3. `reconcile_inbox_polling` clears the marker for an exact-scope
   expired/empty/cancelled/acked tombstone when no non-complete reply is
   staged for the attempt's `pending` source; it still fails closed on a
   staged reply or any owner/attempt/tenant/scope mismatch.
4. `connect()` attempts that reconcile once, automatically, before its
   ambiguous-pending refusal — the marker no longer requires an operator or
   a script attached to the live process to ever clear.
5. `lease_seconds` now defaults to `turn_timeout + mcp_tool_timeout + 60`
   (was `turn_timeout + 60`), so a real tool call taking close to its own
   configured timeout no longer routinely outlives the lease that was
   supposed to cover it.
6. `mupot_gateway_status` reports `connected` and `lease_reconciliation`
   (`required` + `attempt_id`), so the marker's live state is finally
   visible without a REPL attached to the process.

Explicitly **not** changed: a turn timeout while the message's own lease is
still live is a separate incident class (Failed-bounded above) and keeps
today's base behaviour. Tracked separately; not fixed in this PR.

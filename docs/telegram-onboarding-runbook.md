# Telegram onboarding through the Mupot Hermes plugin

This runbook configures one Hermes profile to relay bounded Telegram project-control
commands to production Mupot and return Mupot results to the same private conversation.
Mupot remains the source of identity, membership, project visibility, roles, decisions,
gate rules, and receipts. Hermes transports commands and results; it does not authorize
them locally.

This document covers the plugin/profile side of onboarding. The Mupot server operator must
also follow the reviewed server-side project-onboarding runbook for the participant-specific
squad, exact project edge, invitation, receipt reconciliation, suspension, and rollback.

## Authority boundary

Onboarding grants a participant only the selected capability on the dedicated squad linked
to the intended project. It grants no:

- merge, deploy, or publish authority;
- spending authority;
- organization-admin authority;
- bearer, agent, workspace, bot, or webhook token;
- independent gate authority.

An `/approve` or `/reject` command is effective only if Mupot independently confirms the
current member, project edge, gate grant, required surface capability, task state, and
conflict-of-interest rules. A displayed button, prior role, pairing receipt, or Telegram
message is not a durable grant.

## Prerequisites and stop conditions

Do not configure or pilot a live profile until all of these have separate receipts:

1. Independent review and required checks pass on the exact plugin and Mupot server heads.
2. Merge and deployment are authorized for those exact heads and the intended tenant.
3. `https://mupot.mumega.com/health` reports the expected clean deployed commit.
4. The remote migration ledger contains `0152_telegram_project_onboarding.sql` followed by
   `0153_inbox_lease_attempt_reconciliation.sql`. Migration 0152 owns onboarding/webhook
   receipts; migration 0153 owns the strict-scope lease-attempt and attempt-ACK receipts.
5. `IM_WEBHOOK_SECRET` is configured at both the Mupot Worker and Hermes profile without
   reading or recording its value.
6. One dedicated participant squad is linked to exactly one intended project, and the
   reverse project-edge readback proves no additional linked project.
7. The project's Routine has an explicit executor, budget, retry/stop policy, and human
   decision route.
8. Read back the materialized Routine run and verify its `assigned_agent_id` exactly equals
   the native Hermes profile's configured operator `agent_id`. This equality, not the
   receiver configuration alone, connects the run's outbound message to this profile and
   its private Telegram conversation. If the values differ, stop the pilot, correct the
   assignment through the approved Mupot Routine/run administration surface, and repeat the
   readback before claiming automatic result delivery.

Stop if any identity, deployed SHA, migration, project edge, receiver owner, bot owner,
credential binding, or pending receipt is ambiguous. Local tests are not deployment or
pilot evidence.

## One-profile topology

Use exactly one native Mupot receiver and one Telegram bot per Hermes profile. The
consolidated `mupot` plugin owns both the receiver and the five deterministic command
handlers. Do not install `platforms/mupot` or `mupot-platform`, start the legacy inbox
watcher, register a second native receiver, or attach a second Telegram bot to the same
profile.

Copy [`../examples/native-gateway-config.yaml`](../examples/native-gateway-config.yaml) into
the dedicated profile and retain these exact production settings:

```yaml
plugins:
  enabled: [mupot]
  disabled: [platforms/mupot, mupot-platform]
  entries:
    mupot:
      allow_gateway_injection: true
      settings:
        mode: operator
        operator:
          native_gateway_enabled: true
          inbox_watch_enabled: false
          base_url: https://mupot.mumega.com
          telegram_control_enabled: true
          telegram_control_webhook_secret_env: IM_WEBHOOK_SECRET

mupot:
  enabled: true
  routine_events_enabled: true
  allowed_agents: [<ALLOWED_PEER_AGENT_ID>]
```

Set the Mupot MCP URL to `https://mupot.mumega.com/mcp`. Replace only the documented
tenant, squad, agent, human approval-owner, peer-agent, state-path, and Telegram user-ID
placeholders. Preserve the exact existing state path when consolidating a split receiver;
starting with a new empty ledger loses reconciliation context.

Place `MUPOT_AGENT_TOKEN`, `IM_WEBHOOK_SECRET`, and the Telegram bot token in the profile's
approved protected environment. Never store values in YAML, shell history, logs,
screenshots, receipts, or commits. The environment-variable name is configured in YAML;
the secret value is not.

## Profile preflight

Before restart or participant invitation:

1. Verify the profile contains exactly one enabled `mupot` plugin, one configured Telegram
   bot, and no legacy Mupot platform copy.
2. Verify `native_gateway_enabled: true`, `inbox_watch_enabled: false`, and
   `mupot.routine_events_enabled: true`; the plugin rejects simultaneous native and legacy
   receivers. Keep `mupot-routines` out of the peer `allowed_agents` list.
   `allowed_agents` entries are lowercased and have one leading `agent:` prefix stripped
   before comparison (a hand-typed `agent:Kasra` and `kasra` match the same entry). A
   configured empty value means deny-all and is honored as written; only a genuinely
   absent key falls back to the default roster
   (`hadi-codex,hadi-codex-cli,kasra,hermes`) — set it explicitly rather than relying on
   that default for a real deployment.
3. Verify the credential-free HTTPS origin is exactly `https://mupot.mumega.com`, with no
   user info, query, or fragment.
4. Verify the configured tenant and welded agent ID against authenticated Mupot readback.
5. Verify the notification recipient is the participant's immutable Telegram user ID and
   the destination is an active private conversation.
6. Verify the retained state file parses and its processed, pending, notification, and
   attempt-v3 reconciliation records have been inventoried without exposing message bodies
   or secrets. A pre-v3 marker or owner mismatch remains fenced for manual reconciliation.
7. Confirm no turn or delivery is active before the one planned full gateway restart.

Use a full gateway restart for initial installation or split-receiver consolidation. Forced
plugin reload can leave ownership or old stream threads ambiguous. After restart, verify a
new process, one registered receiver, five scoped Telegram command handlers, healthy Mupot
receive, and the same retained state path. Do not invite a participant during this check.

## Participant flow

1. **Deliver the code out of band.** An authorized Mupot operator creates the short-lived,
   single-use project invitation and sends only its returned pairing code through the
   approved direct channel. The participant receives no token or credential.
2. **Redeem privately.** The participant opens a direct chat with the one approved bot and
   sends `/start <pairing-code>`. Forwarded commands, groups, sender/chat mismatch, expired
   or reused codes, and an identity already bound elsewhere must have no onboarding effect.
3. **Confirm scope.** Stop unless `/start` identifies the intended project. Its response is
   not authoritative role evidence. The operator separately reads back an active, tokenless
   member and only the approved squad capability; that readback confirms the role.
4. **Inspect attention.** The participant sends `/needs <project-id>` (or `/needs` across
   their accessible projects). The reply must contain only role-authorized items and
   server-provided actions. A different project must be absent or refused.
5. **Make one decision.** Use exactly one real pending decision:
   - `/answer <run-id> <choice>` with the exact, case-sensitive choice; or
   - `/approve <task-id>` / `/reject <task-id> <reason>` only when the participant holds
     the existing independent gate authority Mupot requires.
6. **Prove the effect.** Reconcile the completed Telegram receipt with the Routine answer
   receipt or task verdict. A Telegram `200` alone is not completion proof.
7. **Verify return-path assignment.** Read back this materialized run's
   `assigned_agent_id` and the native profile's configured operator `agent_id`; require
   exact equality. If they differ, stop, correct the assignment through the approved Mupot
   surface, and repeat the readback. Do not infer this binding from receiver configuration.
8. **Observe continuation.** Only after that equality is proven, Mupot continues the
   Routine through the assigned executor and the native receiver must activate the same
   private conversation once. Record the resulting queued, running, blocked, failed, or
   completed update's channel delivery receipt separately from scheduling/activation
   acceptance.

The receipts are deliberately non-substitutable:

1. Routine custody proves the server stored the human wait.
2. Exact scope-bound attempt ACK proves the plugin consumed that reconciled leased envelope.
3. `activation_queued` proves Hermes accepted private-session scheduling.
4. A transport receipt plus conversation-mirror readback proves channel delivery.
5. A Telegram webhook receipt plus Routine answer or task verdict proves the human decision.
6. Terminal Routine/task evidence proves domain completion.

Ordinary Telegram text is not captured by these handlers and continues through Hermes.
Only `/start`, `/needs`, `/answer`, `/approve`, and `/reject` use the deterministic relay.

## Duplicate, stale, and conflicting commands

- An identical replay of the same Telegram update ID, principal, and command returns the
  stored response and must not repeat the domain effect.
- Reusing an update ID with different text, principal, or forwarding metadata is a conflict
  and must return `409 update_conflict` without a new effect.
- An invalid choice, unauthorized or wrong-project command, terminal/stale decision, or
  domain duplicate must be refused without a second answer or verdict.
- `processing`, `unknown`, or a completed receipt without a readable response is uncertain
  and returns `409 update_in_progress`. It is not permission to delete the receipt or issue
  a replacement decision.

For an uncertain read-only request, a new `/needs` update can inspect current state. For
`/answer`, `/approve`, or `/reject`, reconcile the Routine/task domain receipt first and do
not manufacture another decision under a new update ID.

## Notification retry and restart

When a Routine enters human wait, Mupot durably records that state before attempting its
project-attributed notification. `notification_pending: true` means the wait exists but
delivery is unproven. Correct recipient liveness or project access, then replay the same
Routine action so Mupot reuses its stable `routine-human:` request ID. Do not send an ad hoc
replacement with a new request ID.

On gateway interruption:

1. Stop new commands if receiver or credential ownership is uncertain.
2. Preserve the profile state and Mupot webhook/domain receipts; never delete them to force
   replay.
3. Confirm whether any turn or native delivery was active, then perform one full restart.
4. Verify one receiver, five scoped handlers, the retained state path, Mupot connectivity,
   and private-conversation binding.
5. Reconcile every pending or ambiguous update against both the Telegram receipt and the
   Routine/task state before retrying.
6. Retry only through the defined idempotent path: identical Telegram transport replay or
   same Routine action/stable request ID. Record activation, channel delivery, and domain
   completion as separate receipts.

An activation receipt is also state-specific. `activating` and `activation_unknown` mean the
external scheduling outcome is uncertain and must not be replayed automatically.
`activation_queued` proves only that Hermes accepted scheduling; it is not channel delivery
or a completed turn. If saving the queued state fails after acceptance, the earlier durable
uncertain state remains the replay fence. Routine activation eligibility is read from the
exact durable processed Routine receipt, not the bounded recent `processed` ID window.

### Operating on a stranded notification

Call the `mupot_gateway_status` MCP tool to see any notification the receiver could not
resolve: `stranded_notifications` lists every source ID currently at status
`activation_unknown` (Hermes accepted or refused scheduling, but the receiver crashed or
lost the response before it could persist which) or `transport_unknown` (the same
ambiguity for a direct channel send). Both are deliberately durable, deliberately
un-retried terminal states, not bugs: an automatic replay would risk delivering the same
human notice twice with no way to tell.

To operate on a stranded ID:

1. Read the notice's own text (from the retained state file's `notification_outbox` entry,
   never re-derived) and independently confirm, from the platform side (the Telegram chat
   history, or the linked human directly), whether the message actually arrived.
2. If it arrived: leave the record as-is. It is evidence, not a queue entry — nothing else
   in the plugin acts on it, and it will not be resent.
3. If it did NOT arrive and the underlying need is still live: drive a fresh, ordinary
   notification through its normal path (a new Routine action reusing the stable
   `routine-human:` request ID, or a new peer terminal ACK) rather than editing or
   resurrecting the stranded record — this repository does not ship a "force retry" tool
   for `activation_unknown`/`transport_unknown` by design, to avoid ever double-delivering
   under uncertainty.
4. Only after confirming delivery status either way, note the reconciliation in your own
   operator log; the stranded record itself is not cleared automatically and its presence
   in future `mupot_gateway_status` calls is expected until the underlying state file is
   rotated or pruned by the normal completed-notice retention (oldest completed records are
   dropped once more than 999 accumulate).

## Emergency stop (`hermes pause`)

`hermes pause` engages Hermes's own global emergency-stop sentinel
(`agent/estop.py`). The receiver checks this sentinel at every consume and
egress primitive, not one named code path: the top of every poll iteration
(so a paused tick does nothing at all — no routine-event replay, no
reply-outbox replay, no notification flush, no inbox lease attempt), both
inbox ACK primitives (`inbox_lease_ack` and `inbox_ack`, covering every
branch that consumes a leased message — including the routine-events-disabled
quarantine and sender-policy dead-letter branches, which never call
`_deliver`/`_handle_routine_event`/`_handle_ack_envelope` themselves), the
peer `send` MCP call (both a live in-turn reply and a replayed prepared
reply), and each of the three notification sinks independently (the
activation injector into a human's live Hermes session, the external
Telegram send, and the conversation mirror write). A pause is always a
**temporal condition** the receiver waits out, never a state transition it
records: nothing is leased, ACKed, sent, delivered, mirrored, or injected
while paused, and no durable quarantine/reconciliation marker is written for
the pause itself. `hermes resume` picks delivery back up on the very next
poll cycle with no operator action required.

**The legacy (non-native) inbox-stream receiver drops, not defers, while
paused.** `inbox_watch_enabled` mode's `deliver()` callback also checks the
same sentinel before injecting a batch into a human's live Hermes session,
but that stream has no lease/redelivery surface — its cursor and seen-keys
are already advanced for a batch before `deliver()` ever runs. A refusal
there therefore drops the batch (logged once per pause window), it does not
retry it once `hermes resume` lifts the pause. This is a deliberate,
accepted tradeoff for the legacy path, not a bug: switch to the native
gateway (`native_gateway_enabled`) if pause-safe redelivery matters for your
deployment.

**Lease release is expiry-only.** The receiver never calls a server-side
"release this lease early" operation when it defers a message for a pause —
it lets the in-flight visibility lease run out naturally so the exact same
message is redelivered once resumed. This means the worst-case redelivery
latency after `hermes resume` is bounded by whatever `lease_seconds` was in
effect for that lease, not by how quickly the pause is lifted: `lease_seconds`
defaults to `turn_timeout + mcp_tool_timeout + 60s` (as of 2026-09-15 — see
below; it was `turn_timeout + 60s` before that) and is clamped to `[1, 3600]`
seconds (`mupot_gateway/adapter.py` — the config's `lease_seconds` extra can
override the default within that range). A message leased immediately before
`hermes pause` can take up to that many seconds to redeliver after
`hermes resume`, even though the pause itself may have lasted only moments.
This is a deliberate trade — avoiding a second, more invasive release-path
primitive — not an oversight; plan any time-sensitive pause/resume operation
with that ceiling in mind.

**`reconcile_inbox_polling()` respects an engaged pause.** If an operator (or
an automated reconciliation) calls it while `hermes pause` is still engaged,
it returns `False` without ACKing anything and without clearing the durable
lease-quarantine marker that required reconciliation in the first place —
the marker is left exactly as it was so the same reconciliation can be
retried once `hermes resume` lifts the pause.

### Lease expiry is a deferral, not a violation (2026-09-15)

A turn's real maximum duration is not just `turn_timeout` — a single MCP tool
call inside that turn can legitimately block for up to Hermes's own
`mcp.tool_call` timeout budget before the turn can even react. Before
2026-09-15, `lease_seconds` was sized as `turn_timeout + 60s` alone, so any
turn that ran past that (a slow tool call, ordinary turn overhead pushing
past a tight margin) reached its own message's `lease_expires_at` before it
could ACK. The receiver folded that ordinary, expected expiry into
`_protocol_error()` → `_quarantine_inbox_polling()` — a **durable**,
`connect()`-refusing state, exactly the same failure shape as the e-stop bug
fixed on 2026-09-14, now recurring for lease expiry. A gateway hitting this
stayed refused across every subsequent operator restart, because nothing
ever called `reconcile_inbox_polling()` automatically.

Two changes close this as a class (`_LeaseExpiredDeferred` in
`mupot_gateway/adapter.py`):

1. **A message's own lease expiring mid-turn is now a deferral, never a
   violation.** `_deliver` raises `_LeaseExpiredDeferred` instead of
   returning silently, both when the lease was already gone before the turn
   started and when the turn's own wait against `runtime.expires_at` times
   out. `_poll_loop` and `reconcile_inbox_polling` release the lease fence
   and let the server redeliver, exactly as they already did for an engaged
   pause — no quarantine, no ack, no "message not marked processed" error.
2. **Lease sizing now accounts for the real ceiling.** `lease_seconds`
   defaults to `turn_timeout + mcp_tool_timeout + 60s`. `mcp_tool_timeout`
   defaults to the mupot MCP server's own configured `timeout` (read via the
   same `load_config()["mcp_servers"][name]` path `HermesMCPClient` uses to
   build its own client, updated round 2 below) — override it explicitly in
   the `mupot` platform's `extra` config only if you want the lease sized
   against something other than the raw MCP client timeout (the 2026-09-15
   incident this closes ran with the mupot MCP server's `timeout` at 180s).
   **`turn_timeout`, not the lease, is what actually binds a slow-tool-call
   turn in practice** (see round 2's classification below) — a turn that
   legitimately needs longer than `turn_timeout` needs `turn_timeout`
   raised, not a bigger lease.

**`connect()` now self-heals a genuinely-empty quarantine automatically.**
When a durable `lease_reconciliation` marker (`required: true`) is present,
`connect()` attempts `reconcile_inbox_polling()` once, before refusing. If
the reconcile call proves the attempt is a terminal tombstone
(empty/cancelled/expired) with nothing consumed and nothing staged in
`reply_outbox` for whatever source it may have held, the marker clears and
`connect()` proceeds normally — no operator action needed. `connect()`
refuses, exactly as before, only for a genuine countercase: a reply is still
staged for the pending source, the marker predates v3 or belongs to a
different profile owner, the tenant/agent/seat readback no longer matches,
or the reconciliation RPC itself fails (network, malformed response). Those
still require the manual procedure below.

**`mupot_gateway_status` now reports the marker.** Its payload gained
`connected` (whether the adapter is actually polling right now) and
`lease_reconciliation` (`null` when no marker is present; otherwise
`{required, version, attempt_id, connect_will_attempt_auto_reconcile}`) —
before this, a fully-quarantined adapter that could never `connect()` still
reported `{"ok": true}` with no way to see why nothing was being received.
Round 2 (2026-09-15) added `reconciling` (true while `connect()`'s bounded
auto-reconcile turn is in flight — see below) and `turn_failure_dlq` (the
Failed-bounded class's own terminal disposition — see `_DeliveryDeferred`'s
docstring in the code for the classification table).

**Manual procedure, when the marker survives an automatic `connect()`
attempt (or you need to clear it without restarting):**

1. Call `mupot_gateway_status`. If `lease_reconciliation` is non-null, note
   its `attempt_id` and whether `connect_will_attempt_auto_reconcile` is
   `true` (that field mirrors `_lease_quarantined`; a `false` here alongside
   a non-null marker means the marker predates v3 or fails an owner check
   and will never self-heal — skip straight to restoring a known-good
   `state.json` for that case).
2. If `connect_will_attempt_auto_reconcile` is `true`, the next
   `connect()` (a normal gateway restart, or however your deployment
   triggers a reconnect) will attempt reconciliation on its own — try that
   first before anything more invasive.
3. If it still does not clear (logged as `"inbox reconciliation preserved"`
   or `"inbox attempt reconciliation failed"`), a real countercase holds.
   Read the log line: a preserved reply means a reply is genuinely still
   staged for the pending source and needs human review of the retained
   `state.json`'s `reply_outbox`/`pending` entries before anything is
   discarded; a failed readback means the current `inbox_consumer_status`
   no longer matches what the marker recorded (profile/tenant/seat/mode
   changed) and needs an operator to confirm which is authoritative before
   editing state by hand.

### Round 2 (2026-09-15): every delivery outcome has exactly one class

Kasra gate re-gate round 2 (head 5046ea79) found that round 1's fix, while
correct for a *clean* lease expiry, left two more outcomes able to reach a
durable brick or an unbounded loop by a different door. The class is:
**every delivery attempt lands in exactly one of four outcomes, each with
one durable representation.**

**The canonical table lives in exactly one place: `_DeliveryDeferred`'s own
docstring in `mupot_gateway/adapter.py`** (round 4, Athena second eye, head
c68e0839, MED "runbook :398" — a copy of this table living here too had
drifted out of sync with the code twice already: it said a Failed-bounded
deferral left `pending` AS-IS below the cap, and that the poll loop "keeps
running either way" at the cap, both false once `_resolve_turn_failure`
existed. Read the code's own docstring for the current, authoritative
version of the table — this runbook intentionally does not restate it).

BLOCK-1 fix (the brick moved, not closed): `_deliver` writes `pending =
{"message": message}` before either `_LeaseExpiredDeferred` raise site can
run, and round 1 left it there unconditionally. On restart, the
constructor's `_legacy_pending_ambiguous` check reads a `pending` record
whose source id isn't staged in `reply_outbox` as ambiguous crash state, and
`connect()` refuses — **before** the `_lease_quarantined` auto-reconcile
branch even runs. The 2026-09-15 incident's own symptom (durable refusal
surviving every restart) would have recurred through `pending` instead of
`lease_reconciliation`, invisibly, since `mupot_gateway_status` had no field
for it either. Both `_LeaseExpiredDeferred` raise sites now clear `pending`
before raising (round 4: UNLESS a non-`"complete"` `reply_outbox` entry is
already staged for that exact source — see `_TurnFailureDeferred`'s
docstring for the `handler_error`-with-custody race this guards against).

New knob: **`max_delivery_attempts`** (`extra`, default
`_SERVER_MAX_DELIVERY_ATTEMPTS` — currently `5`, matching the pot's own
`MAX_DELIVERY_ATTEMPTS`, round 4) bounds failed-bounded retries using the
message's own `delivery_attempts` count — the same field Mupot's server
increments on every redelivery. This is *independent* of
`lease_seconds`/`turn_timeout`: a hung or raising turn retries at most this
many times total, then defers terminally (round 4: without acking — see
below) regardless of how generously the lease is sized.

**At the cap, this adapter no longer acks (round 4, MED "cap surface").** An
ack sets `read_at` on the pot's row, and the pot's own reaper
(`mupot/src/agents/messages.ts`, dead-letter step) requires `read_at IS
NULL` to ever set `dead_lettered_at` — acking at the cap (round 1-3
behavior) permanently prevented the pot from ever recording the dead-letter
itself, moving that fact to this host's local `dlq` file only. The local
`dlq` row is still recorded (unchanged, idempotent by message id) for
operator visibility; the lease is instead left to expire naturally so the
pot's own reaper — which runs on every subsequent `inbox_lease` call for
this agent, before handing out a lease — dead-letters the row server-side
the next time this adapter polls. Matching `max_delivery_attempts`'s default
to the pot's own ceiling means the two thresholds coincide by construction.

**`connect()`'s auto-reconcile can still run a full bounded turn** before
`_running` is set `True` (unchanged from round 1 — see the self-healing
paragraph above) — it goes through the same `_process_leased_message` →
`_deliver` → `_resolve_turn_failure` path as the live poll loop, so it is
bounded by `max_delivery_attempts` exactly the same way. While it runs,
`mupot_gateway_status.connected` is `false` (the adapter isn't `_running`
yet) **and** `reconciling` is `true` — before this, `connected: false` alone
could not distinguish "not yet attempted" from "actively working on it".
Note: this means `connect()` can make REAL egress (at minimum
`inbox_lease_reconcile`, possibly `inbox_lease_ack`/`send` if a leased
attempt is genuinely resolved during reconcile) BEFORE `is_connected`
reports `true` — do not read `is_connected: false` as "no network activity
has happened yet".

**Round 4 notes (Athena second eye, head c68e0839):**

- **B3:** `connect_will_attempt_auto_reconcile` is now `false` whenever the
  profile-owner fingerprint is unavailable or mismatched, even if a v3
  marker exists — `connect()`'s own first gate refuses before the reconcile
  attempt is ever reachable in that case, and the status field now agrees
  (`_is_lease_reconcile_reachable()` re-derives the exact same check).
- `_clear_lease_fence()` (called after every poll-loop iteration that
  finishes or defers cleanly) **removes** the `lease_reconciliation` key
  entirely, it does not set it to `null` — `"lease_reconciliation" not in
  state` is the clean-state shape on disk; `mupot_gateway_status` and
  `lease_reconciliation_status()` both still report it as absent (`None`/
  not present) either way, so no operator-facing behavior differs, but a
  hand-inspection of `state.json` should not expect a `"lease_reconciliation":
  null` key to be literally present.

**Diagnostic entry points, one per class — the operator never edits
`state.json` for Deferred or Failed-bounded:**

- **Delivered** — nothing to do; visible in `processed`/`terminal_receipts`.
- **Deferred** — nothing to do; the poll loop and the server handle
  redelivery on their own. If it recurs constantly for the same source,
  check `turn_timeout` vs. the mupot server's actual `lease_seconds`
  headroom, not `state.json`.
- **Failed-bounded** — call `mupot_gateway_status`; read `turn_failure_dlq`
  for `{source_id, reason}`. `reason` tells you where to look: `turn_timeout`
  → the handler hangs (check the specific tool call it was blocked on, raise
  `turn_timeout` or `max_delivery_attempts` if the work is legitimately
  slow); `no_custody` → the handler is returning nothing actionable for that
  source (application bug, not a plugin bug); `handler_error` → read the
  Hermes error log around that source id for the raised exception (note:
  IF that error reply reached the human with custody, this is actually
  Delivered, not Failed-bounded — see `_DeliveryDeferred`'s docstring). At the
  cap (round 4): this adapter does not ack, so the message is deliberately
  NOT `processed` locally — the pot's own reaper resolves the dead-letter
  server-side once the row's lease naturally expires; there is nothing in
  `state.json` for an operator to safely touch here either way.
- **Violation** — the manual procedure above; this is the ONLY class where
  reading (and, in the documented last-resort case, editing) `state.json`
  by hand is ever the correct next step.

## Suspension, revocation, and rollback

When access must stop, suspend the exact Mupot member first so subsequent Telegram lookup
fails closed and current web sessions are revoked. Then revoke the exact participant-squad
capability and read back both suspended status and capability absence. A command displayed
before revocation has no continuing authority.

After revocation, verify `/needs` exposes no unauthorized action and a representative
`/answer` or verdict is refused without domain mutation. Preserve all Telegram, Routine,
verdict, message, and delivery receipts.

If the transport boundary is suspect, disable new Telegram ingress. Rotate
`IM_WEBHOOK_SECRET` at the Worker and profile ends as one separately authorized operation;
a mismatch must keep the endpoint sealed. Never record either value. Preserve the receiver
state and webhook receipts. Do not roll back the additive server migration or delete receipt
rows.

Remove the project edge only under separate operator approval after proving the squad is
dedicated and no participant, task, Routine, or provider binding depends on it. A refusal is
a stop signal, not permission to widen a predicate or edit around the invariant.

## Evidence checklist

- [ ] Exact plugin and Mupot server heads have independent review and required checks.
- [ ] Merge/deployment approvals, deployed `/health` SHA, and ordered migration-ledger
      readback for 0152 then 0153 are attached separately.
- [ ] The profile has one receiver, one Telegram bot, no legacy Mupot platform, and the
      exact production origin/config keys.
- [ ] No secret value appears in configuration or evidence.
- [ ] The dedicated squad reverse edge reaches only the intended project.
- [ ] Pairing occurred once in the intended private chat; `/start` confirmed the project and
      separate authoritative member/capability readback confirmed the role.
- [ ] `/needs` proves role-scoped visibility and actions.
- [ ] One exact answer or independently authorized verdict has both Telegram and domain
      receipts.
- [ ] The materialized Routine run's `assigned_agent_id` exactly equals the configured
      native profile `agent_id`; receiver configuration alone was not treated as proof.
- [ ] Routine custody, scope-bound attempt ACK, activation scheduling, channel delivery,
      human decision, and domain completion each have separate receipts.
- [ ] Identical replay, conflict, stale/terminal, wrong-project, and unauthorized cases have
      no duplicate effect.
- [ ] Suspension, exact capability revocation, and post-revocation denial are proven.
- [ ] Restart/retry preserves state and uses the same idempotency keys.
- [ ] No merge, deploy, publish, spending, organization-admin, token, or independent gate
      authority is inferred from onboarding.

Until every live item is checked, report this as locally verified plugin documentation and
test evidence only, not a deployed onboarding or pilot completion.

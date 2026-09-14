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
defaults to `turn_timeout + 60s` and is clamped to `[1, 3600]` seconds
(`mupot_gateway/adapter.py` — the config's `lease_seconds` extra can override
the default within that range). A message leased immediately before
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

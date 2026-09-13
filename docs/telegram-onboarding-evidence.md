# Telegram onboarding local acceptance evidence

Date: 2026-09-13 UTC

This receipt is bounded to repository tests. It does not prove merge, deployment, remote
migration application, live profile configuration, credential binding, participant invite,
or pilot completion.

## Exact local contracts

- Plugin head before this repair slice: `a6553296cf31695ce6a3d09b1d91a92a003afce0`.
- Mupot server checkout: `22c778d88d7378a1c4df164552bd541a1be1f812`
  (attempt contract introduced at `80001a11c29d93a5dd83f09f87eeaff92514f851`),
  clean tracked tree.
- Hermes native contract: `233757037df1f03f9fe1cfddc097acd5ad7f7510`.

## Cross-repository acceptance

`scripts/test-integration.sh` hard-pins the Mupot server commit with no environment override
and refuses a moved or dirty server checkout. Its Vitest harness applies the server's
migration chain through `makeReadyRoutineFixture`, creates a
real `ask_human` Routine wait, installs a synthetic agent-bound bearer digest in the local
D1 fixture, and exposes the server's registered MCP application on a loopback HTTP bridge.
The discovered native Python plugin uses its real `HermesMCPClient` to issue JSON-RPC for
`boot_context`, strict consumer status, attempt lease, and attempt ACK. No prebuilt result is
injected across the process boundary.

The plugin consumer proves durable Routine custody and notice custody, one scope-bound
attempt ACK, one private-session activation acceptance, no peer model turn, and no outbound
send to the synthetic Routine source. The harness observes the real server attempt tombstone
at `state=acked`, matches the random attempt ID emitted on the HTTP wire, and reads back
`agent_messages.read_at` from the same database. It also reads the plugin's durable Routine
custody record and requires its attempt, strict scope, and profile-owner fingerprint to
match that wire attempt before accepting the result.
It separately proves the participant's squad capability reaches the intended project, the
Routine's responsible squad matches that edge, and the materialized run's
`assigned_agent_id` equals the `agent_id` obtained by discovering the plugin through a real
temporary Hermes `config.yaml` and constructing the native adapter through its registered
platform factory. A mismatched profile stops before plugin custody, attempt ACK, peer work,
or activation and leaves the server message unread. The matched path also proves `/needs`
exposes the pending question and one exact `/answer` records the human decision.

Fresh verification passed the two cross-repository tests 2/2, standalone pytest 220 plus 12
subtests, standalone unittest 27/27, local native 264/264, and clean-detached pinned-Hermes
native 264/264. Ruff, mypy, Python compilation, shell parsing, four-file YAML parsing,
relative-link checks, working and full-base whitespace checks, and credential-shape checks
also passed. The current lease-attempt commands are retained in the ignored hostile Task 2
report; the earlier integration mutation results remain in the Task 8 report.

Seven temporary source mutations each produced the expected focused RED: process-global
secret fallback, uncorrelated JSON-RPC acceptance, skipped notice persistence, removed
recipient/generation binding, nonterminal progress kind, server request-kind human wait,
and synthetic-member spoof acceptance. The disposable worktrees were removed afterwards.

## Receipt meanings

1. Server Routine custody proves only that the human wait exists.
2. Exact scope-bound attempt ACK proves only that the reconciled leased envelope was consumed.
3. `activation_queued` proves only that Hermes accepted private-session scheduling.
4. A channel transport receipt plus conversation-mirror readback proves channel delivery.
5. A Telegram webhook receipt plus Routine answer or task verdict proves the human decision.
6. Terminal Routine/task evidence proves domain completion.

No receipt above substitutes for a later one.

## Unproven gates

Independent exact-head review and required remote CI, push or PR update, merge, deployment,
remote migration-ledger readback, live profile and credential configuration, participant
invitation, and pilot execution remain separately controlled and unproven.

## Final attempt-v3 and notification-fence verification

The final cross-repository run used the real plugin HTTP client against the registered Mupot
MCP application at server head `22c778d88d7378a1c4df164552bd541a1be1f812`
(attempt code `80001a11c29d93a5dd83f09f87eeaff92514f851`) and migration order
`0152_telegram_project_onboarding.sql` then
`0153_inbox_lease_attempt_reconciliation.sql`. It passed 2/2 cases and observed strict scope,
random attempt lease, exact attempt ACK, the `acked` server tombstone, consumed message
readback, matched assigned/profile agent identity, and one activation acceptance. The
mismatched profile performed no custody or ACK.

Five plugin mutations were applied separately in a disposable detached worktree, each
produced the expected RED, and each was restored:

1. Accepting a non-strict consumer-scope echo allowed a reconciliation network write; the
   strict-scope no-write test failed.
2. Ignoring the attempt-v3 profile-owner fingerprint let another profile reconcile the same
   server scope; the owner-swap test failed.
3. Downgrading the durable pre-activation state to `pending` caused accepted scheduling plus
   queued-save failure to become replayable; the save-failure test failed.
4. Requiring the bounded recent `processed` list instead of the exact durable Routine receipt
   broke activation after processed-window eviction; the eviction test failed.
5. Changing the native HTTP MCP transport from POST to GET made the matched cross-repository
   case fail before `boot_context`, proving the acceptance uses the actual HTTP boundary.

After restoration both disposable trees were diff-clean. The plugin's final release matrix
also passed full-base changed-file Ruff, mypy over eight source files, Python compilation,
shell parsing, four YAML files, four onboarding-document link checks, full-base whitespace,
and the repository no-secrets scan. The broader repository Ruff invocation still reports
pre-existing lint debt outside this branch's changed paths; it is not represented as green.

The exact plugin head remains local in this receipt. Current-head push/PR checks, independent
review, merge, deployment, ordered remote migration readback for 0152/0153, protected live
profile/webhook configuration, transport/mirror receipt, invitation, and the real non-admin
Telegram pilot remain separately gated and unproven.

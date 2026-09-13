# Telegram onboarding local acceptance evidence

Date: 2026-09-13 UTC

This receipt is bounded to repository tests. It does not prove merge, deployment, remote
migration application, live profile configuration, credential binding, participant invite,
or pilot completion.

## Exact local contracts

- Plugin head before this evidence slice: `a3ba6ce225afcc6f2cf0df4b81bfb454f4a73ff8`.
- Mupot server checkout: `80001a11c29d93a5dd83f09f87eeaff92514f851`,
  clean tracked tree.
- Hermes native contract: `233757037df1f03f9fe1cfddc097acd5ad7f7510`.

## Cross-repository acceptance

`scripts/test-integration.sh` hard-pins the Mupot server commit with no environment override
and refuses a moved or dirty server checkout. Its Vitest harness applies the server's
migration chain through `makeReadyRoutineFixture`, creates a
real `ask_human` Routine wait, leases the stored `routine.human-wait/v1` envelope under a
durable attempt ID, and reconciles that attempt through the server contract. It passes the
server's exact scoped lease-reconciliation result to a reconstructed native Python plugin
adapter. This test does not claim byte identity for an HTTP or MCP serialization that it
does not observe.

The plugin consumer proves durable Routine custody and notice custody, one scope-bound
attempt ACK, one private-session activation acceptance, no peer model turn, and no outbound
send to the synthetic Routine source. The harness applies that ACK through the real server
`ackAgentInboxLeaseAttempt` seam in the same database, requires `state=acked` with
`consumed=true`, and reads back `agent_messages.read_at`.
It separately proves the participant's squad capability reaches the intended project, the
Routine's responsible squad matches that edge, and the materialized run's
`assigned_agent_id` equals the `agent_id` obtained by discovering the plugin through a real
temporary Hermes `config.yaml` and constructing the native adapter through its registered
platform factory. A mismatched profile stops before plugin custody, attempt ACK, peer work,
or activation and leaves the server message unread. The matched path also proves `/needs`
exposes the pending question and one exact `/answer` records the human decision.

Fresh verification passed the two cross-repository tests 2/2, standalone pytest 220 plus 12
subtests, standalone unittest 27/27, local native 234/234, and clean-detached pinned-Hermes
native 234/234. Ruff, mypy, Python compilation, shell parsing, four-file YAML parsing,
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

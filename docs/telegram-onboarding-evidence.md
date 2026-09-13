# Telegram onboarding local acceptance evidence

Date: 2026-09-13 UTC

This receipt is bounded to repository tests. It does not prove merge, deployment, remote
migration application, live profile configuration, credential binding, participant invite,
or pilot completion.

## Exact local contracts

- Plugin head before this evidence slice: `37b1659239bbf7319d8ff5ece7b8b783be643954`.
- Mupot server checkout: `38be07b6f3dfc4408b977f530358886d8599d6c1`,
  clean tracked tree.
- Hermes native contract: `233757037df1f03f9fe1cfddc097acd5ad7f7510`.

## Cross-repository acceptance

`scripts/test-integration.sh` refuses a moved or dirty Mupot server checkout. Its Vitest
harness applies the server's migration chain through `makeReadyRoutineFixture`, creates a
real `ask_human` Routine wait, leases the stored `routine.human-wait/v1` envelope, and passes
that exact envelope to the native Python plugin adapter.

The plugin consumer proves durable Routine custody and notice custody, one exact-ID source
ACK, one private-session activation acceptance, no peer model turn, and no outbound send to
the synthetic Routine source. The harness then applies that exact ACK ID through the real
server `ackAgentMessages` seam in the same database and reads back `agent_messages.read_at`.
It separately proves the participant's squad capability reaches the intended project, the
Routine's responsible squad matches that edge, the materialized run's `assigned_agent_id`
equals the independently configured native profile `agent_id`, `/needs` exposes the pending
question, and one exact `/answer` records the human decision.

Fresh verification passed the cross-repository test 1/1, standalone pytest 220 plus 12
subtests, standalone unittest 27/27, local native 181/181, and clean-detached pinned-Hermes
native 181/181. Ruff, mypy, Python compilation, shell parsing, four-file YAML parsing,
relative-link checks, working and full-base whitespace checks, and credential-shape checks
also passed. The exact commands and mutation results are retained in the ignored hostile
Task 8 report.

Seven temporary source mutations each produced the expected focused RED: process-global
secret fallback, uncorrelated JSON-RPC acceptance, skipped notice persistence, removed
recipient/generation binding, nonterminal progress kind, server request-kind human wait,
and synthetic-member spoof acceptance. The disposable worktrees were removed afterwards.

## Receipt meanings

1. Server Routine custody proves only that the human wait exists.
2. Exact source ACK proves only that the leased envelope was consumed.
3. `activation_queued` proves only that Hermes accepted private-session scheduling.
4. A channel transport receipt plus conversation-mirror readback proves channel delivery.
5. A Telegram webhook receipt plus Routine answer or task verdict proves the human decision.
6. Terminal Routine/task evidence proves domain completion.

No receipt above substitutes for a later one.

## Unproven gates

Independent exact-head review and required remote CI, push or PR update, merge, deployment,
remote migration-ledger readback, live profile and credential configuration, participant
invitation, and pilot execution remain separately controlled and unproven.

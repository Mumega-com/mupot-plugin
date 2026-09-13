# Human project control through Hermes and Telegram

## Operating model

Mupot owns projects, membership, roles, task/routine state, approval rules, and receipts.
Hermes supplies the persistent agent runtime; a bound operator such as Mubot or
KayHermes communicates with people and performs permitted work through Mupot. The
model/harness does not need to be embedded in the Mupot server.

The plugin is the reusable integration. It must not require a hand-maintained copy of
the receive adapter beside every installation. A person should be able to participate
through their conversation while Mupot remains authoritative about their role.

## What the current integration establishes

- Restricted operator tools are present; agent identity and permissions are checked.
- The native receiver, ACK/receipt handling, and human conversation activation are
  packaged in this repository.
- When explicitly enabled, deterministic Telegram handlers relay only `/start`, `/needs`,
  `/answer`, `/approve`, and `/reject` from a private, unforwarded chat to Mupot's
  `/im/webhook`. They return Mupot's bounded reply without starting an LLM turn. Ordinary
  text remains Hermes-owned.
- Native tests establish locally that the scoped command handlers coexist with ordinary
  Hermes text handling and that a Mupot result activates the same private conversation
  once. This is repository evidence, not proof of a deployed profile or live participant.
- The previous split-plugin Telegram deployment verified Mupot receipt/reply,
  automatic activation of the existing private conversation, and native channel
  delivery. Each consolidated-package rollout must reverify that chain; copied code
  and passing local tests alone are not proof of its live installation.
- Existing Mupot routines already provide proposed actions and human questions. Their
  existence or enabled status does not establish that a complete project loop is
  configured, executing successfully, or suitable for onboarding other people.

## What full onboarding still requires

| Human need | Required behavior and proof |
|---|---|
| Join a project | An authorized inviter selects the project/role; a single-use invitation links the platform's immutable human identity to the intended Mupot member. |
| Know what needs attention | Retrieve the linked person's authorized project state and decisions; visibility and available actions follow their current role. |
| Choose or approve | Correlate the response to the current pending decision. Mupot resolves the human principal and applies its existing answer/verdict rules. An agent's bearer is not human consent. |
| Direct normal work | Goals, scope changes, and assignments become inspectable Mupot state; routine execution is attributed to its configured operator/runtime. |
| Pause or stop | Stop/cancel reaches the active execution path and produces an honest receipt; a message acknowledgment alone is insufficient. |
| Understand the result | Return relevant evidence in the human conversation; distinguish queued, running, blocked, failed, and completed work. |
| Share safely | A second participant cannot read another participant's private context or approve outside their role. Revoked roles take effect on subsequent actions. |

The plugin now packages the Telegram relay and native result-return path locally. The
complete human onboarding loop still depends on reviewed and deployed Mupot server support,
the server migration ledger, exact profile/webhook configuration, project/routine bindings,
and a live non-admin pilot. Enabling handlers or notifications alone does not prove that
loop. Ordinary project members receive no agent token or administrator access.

Use exactly one native Mupot receiver and one Telegram bot per Hermes profile. The same
profile owns deterministic commands and return delivery; a second receiver or bot creates
ambiguous ownership and is unsupported. The hosted production origin is
`https://mupot.mumega.com`, configured with the exact operator keys
`telegram_control_enabled: true` and
`telegram_control_webhook_secret_env: IM_WEBHOOK_SECRET`. Secret values stay in the
protected profile environment, never in YAML.

## Internal machinery setup

Configure an explicit persistent executor binding for each coordination responsibility
(which may reuse an existing Hermes harness). Connect the project's existing routines
to that runtime with appropriate scope, budgets, retry/stop policy, and human decision
routing. Before claiming automatic return to the same private conversation, read back the
materialized Routine run and require its `assigned_agent_id` to equal the native profile's
configured operator `agent_id`. If they differ, stop and correct the assignment through the
approved Mupot surface; receiver configuration is not assignment proof. Verify dispatch,
runtime receipt, actual work, human response where required, and terminal readback. Do not
infer a working internal coordinator from a roster label, an enabled cron entry, or a
successful communication canary.

## First onboarding milestone

Use one existing project and one invited non-admin participant. Prove invitation,
role-scoped attention retrieval, one real pending decision, execution by the assigned
harness, and return of the result in Telegram. Repeat with a participant who lacks the
role and with a revoked/stale decision; both must be refused without side effects.
Reuse Mupot's existing questions and approvals instead of adding a competing system.

The participant flow is deliberately explicit: deliver the single-use pairing code out of
band; redeem it with private `/start`; confirm the returned project and role; inspect
`/needs`; issue one exact `/answer` or an independently authorized verdict; then observe
Routine continuation and automatic result delivery. Identical transport replays must not
repeat the effect; stale, terminal, unauthorized, and conflicting commands must fail
closed. Suspension and capability revocation must deny subsequent commands. After a
restart, timeout, or ambiguous reply, reconcile the durable Telegram and domain receipts
before retrying a decision.

This flow grants no merge, deploy, publish, spending, organization-admin, token, or
independent gate authority. See the
[plugin onboarding runbook](telegram-onboarding-runbook.md) for the bounded profile and
operator procedure.

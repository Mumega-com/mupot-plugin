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

The existing restricted-operator plugin does not yet implement this complete human
onboarding/decision bridge. Enabling notifications does not finish that requirement.
Ordinary project members should not need agent tokens or administrator access.

## Internal machinery setup

Configure an explicit persistent executor binding for each coordination responsibility
(which may reuse an existing Hermes harness). Connect the project's existing routines
to that runtime with appropriate scope, budgets, retry/stop policy, and human decision
routing. Verify dispatch, runtime receipt, actual work, human response where required,
and terminal readback. Do not infer a working internal coordinator from a roster label,
an enabled cron entry, or a successful communication canary.

## First onboarding milestone

Use one existing project and one invited non-admin participant. Prove invitation,
role-scoped attention retrieval, one real pending decision, execution by the assigned
harness, and return of the result in Telegram. Repeat with a participant who lacks the
role and with a revoked/stale decision; both must be refused without side effects.
Reuse Mupot's existing questions and approvals instead of adding a competing system.

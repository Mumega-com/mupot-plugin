# Mupot Hermes Plugin

Provision a [Mupot](https://github.com/Mumega-com/mupot) deployment or attach Hermes as a
restricted, agent-bound operator. Version 0.3 separates these trust zones by profile:

- `provisioner` mode: human-controlled Cloudflare setup;
- `operator` mode: a narrow Mupot task/evidence/approval-request surface, with an
  optional squad-agent manager extension.

Do not combine the modes in one Hermes profile.

## Native gateway receive and human conversation activation

The native Mupot receiver and human-channel routing are owned by this repository
under `mupot_gateway/`. Installing the operator plugin is sufficient; do not also
install a separate `platforms/mupot` copy. See
[`examples/native-gateway-config.yaml`](examples/native-gateway-config.yaml).

Enable `settings.operator.native_gateway_enabled: true` to register the Mupot
platform alongside the existing restricted operator tools. It is mutually exclusive
with `inbox_watch_enabled`; enabling both is rejected before registration. Existing
CLI/SSE-stream configurations remain opt-in and unchanged when native mode is off.

The native receiver uses Mupot's server-authoritative `inbox_lease` attempt receipts and
attempt-bound `inbox_lease_ack`. Its current attempt-v3 state requires server migration
`0153_inbox_lease_attempt_reconciliation.sql`, applied after
`0152_telegram_project_onboarding.sql`. It first reads the strict tenant/agent/effective-seat
consumer scope, durably records that scope with one random attempt ID and the owning
profile's non-secret immutable fingerprint, and reuses the ID for the single proven-safe
transport retry. An ambiguous or restarted attempt is resolved through
`inbox_lease_reconcile` only from the same profile owner. Token rotation inside that profile
remains valid; a different profile presenting the same server scope stays fenced. Only an
exact scope-matching
`leased` tuple is processed, and only an exact `acked`/`consumed:true` attempt receipt permits
local commit and marker clearing. Terminal non-consumed or mismatched receipts remain fenced.
Before any source ACK, peer reply and Routine custody records also persist the originating
attempt, strict scope, and profile owner. Restart replay revalidates those facts and remains
on `inbox_lease_ack`; an expired attempt can never fall through and consume a newer lease.
Before replaying a prepared outbound final, the adapter performs that owner and strict-scope
preflight read-only and sends nothing on any mismatch or ambiguous status response. The
successful order remains send receipt, durable human-notice custody, then exact attempt ACK.
Only records durably identified as non-attempt work use `inbox_ack`; ambiguous older outbox
records and older reconciliation markers stay fenced for manual recovery. The receiver also
verifies the operator's expected agent/tenant, preserves
message correlation, and consumes a source only after successful handling. Terminal ACKs
are preserved without generating another peer reply. It does not start an SOS connection.

Enable `mupot.routine_events_enabled: true` for the dedicated authenticated
`routine.human-wait/v1` receive path. This opt-in does not add `mupot-routines` to
`allowed_agents`: Routine events never start a peer model turn or send to their synthetic
source. Their human notice becomes eligible for private-session activation only after
durable custody, an exact scope-bound attempt ACK, and the local processed marker.

For human updates, configure `mupot.notification_recipients` with the immutable user
ID for each linked platform. Only matching active private conversations are eligible.
With `mupot.notification_activate: true` and
`plugins.entries.mupot.allow_gateway_injection: true`, Hermes's native plugin API
starts a normal turn in the selected conversation. The native gateway rechecks
authorization and prevents the injected event from executing human slash approvals.
The previous split deployment verified Telegram; other platforms require their own
identity binding and end-to-end verification.

Keep each receipt at its own boundary: server Routine custody proves the human wait exists;
the scope-bound attempt ACK proves only that the reconciled leased envelope was consumed;
`activation_queued` proves
only that Hermes accepted private-session scheduling; a channel receipt plus conversation
mirror readback proves channel delivery; a Telegram webhook receipt plus Routine answer or
task verdict proves the human decision; and terminal Routine/task evidence proves domain
completion. None substitutes for another. Interrupted or ambiguous sends are retained for
reconciliation rather than blindly replayed. This integration does not grant the agent
human decision authority.

`activating` and `activation_unknown` are durable no-replay states. If Hermes accepts
scheduling but persisting `activation_queued` fails, the receiver retains the earlier
uncertain state and requires operator reconciliation. Routine activation is authorized by
the exact durable processed Routine receipt, not by the bounded recent `processed` list.

For the broader project-onboarding and human-control scope, see
[`docs/human-project-control.md`](docs/human-project-control.md).

## Telegram project onboarding

The optional deterministic Telegram control surface relays exactly `/start`, `/needs`,
`/answer`, `/approve`, and `/reject` from a private, unforwarded chat to Mupot. These
commands do not start an LLM turn: Mupot resolves the immutable Telegram identity,
project visibility, role, pending decision, conflicts, and current authorization. Ordinary
Telegram text remains owned by Hermes.

Use one native Mupot receiver and one Telegram bot per Hermes profile. Do not install the
legacy split Mupot platform beside this plugin, register a second receiver, or attach a
second bot to the same profile. The production profile settings are:

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

`allowed_agents` accepts either a list of agent names or a single comma-separated
string; each entry is lowercased and has one leading `agent:` prefix stripped (so
`agent:Kasra` and `kasra` are the same allowlist entry — defensive normalization for
hand-typed config, not evidence that Mupot itself ever emits a prefixed sender). An
explicit empty value (`[]`, `""`, or a list of only blank/whitespace entries) means
**deny all peers** and is honored as written. Only a genuinely **absent** key falls
back to the default four-agent roster (`hadi-codex,hadi-codex-cli,kasra,hermes`) — do
not rely on that default; set `allowed_agents` explicitly for any real deployment. A
non-string, non-list value (a bare number or boolean, for example) is rejected at
construction with a clear config error rather than an unrelated `TypeError` later.

Keep `IM_WEBHOOK_SECRET`, the Telegram bot token, and `MUPOT_AGENT_TOKEN` in the
profile's protected environment; never put values in YAML. Enabling Telegram control is a
local relay configuration, not a capability grant.

The participant receives a one-time pairing code through an approved out-of-band channel
and sends `/start <pairing-code>` in the approved bot's private chat. That response confirms
the project; it is not authoritative role evidence. An operator must separately read back
the active member and exact squad capability before `/needs` is treated as role-scoped.
The participant then submits one exact `/answer <run-id> <choice>` or, only when
independently authorized by the existing gate, `/approve <task-id>` or
`/reject <task-id> <reason>`. Mupot records the decision, continues the Routine, and the
native receiver returns the resulting update to the same private conversation automatically.

Duplicate transport updates replay the stored response without a second effect. Stale or
terminal decisions, invalid choices, unauthorized actions, and conflicting reuse of an
update ID are refused without creating a new decision. Suspension and capability
revocation are rechecked on every later command. Restart or timeout is an uncertain state:
reconcile the Mupot receipt/domain state before issuing another decision; do not bypass the
durable fence with a new command or delete receipt state.

Onboarding grants no merge, deploy, publish, spending, organization-admin, token, or
independent gate authority. See the [Telegram onboarding runbook](docs/telegram-onboarding-runbook.md)
for setup, verification, retry, revocation, and rollback steps. A live pilot remains gated
on independent review, exact deployment proof, migration readback, and protected webhook
configuration.

**This deterministic command relay is a fallback, not the primary path.** It exists for
participants who are not bound to an owned Hermes agent seat. For an owned member, plain
natural language through their own agent is the primary, always-live decision channel — see
"Human-origin attestation" below. The relay also has a known gap: Hermes's own Telegram
polling-connection rebuild (a transient reconnect after a network hiccup) re-registers only
Hermes's core message/command handlers, never a plugin's own `CommandHandler`s
(`plugins/platforms/telegram/adapter.py`'s `_register_handlers`, called from
`_initialize_app_with_retries` on every rebuilt polling `Application`) — so after such a
rebuild, `/start` `/needs` `/answer` `/approve` `/reject` can silently stop responding until
the whole gateway process restarts, with nothing in this plugin able to detect or repair it
(there is no hook back into that rebuild path). `register_telegram_control` logs a WARNING
naming this limitation whenever `telegram_control_enabled: true` is configured.

## Human-origin attestation for `task_verdict` / `needs_you_list`

The primary decision channel for an owned member is not a deterministic command at all: they
talk to their own agent in plain natural language, and the **harness** — never the LLM —
stamps the triggering message's origin onto the agent's `task_verdict`/`needs_you_list` calls.
Mupot resolves that `human_origin` object to the member and runs the call under the human's
own identity instead of the agent seat; without it (a CLI turn, a cron turn, a subagent turn,
or a turn on a platform this plugin doesn't yet support), the call runs under the agent seat
exactly as before this feature existed.

This is implemented as two Hermes lifecycle hooks in `mupot_gateway/human_origin.py`,
registered FIRST (before the platform adapter or any tool) from `mupot_gateway/adapter.py`'s
`register()`, only when `native_gateway_enabled: true`. Registration fails **closed**: a
Hermes runtime whose `PluginContext` cannot `register_hook` (or whose hook registration
itself raises) gets no native-gateway registration at all — there is no other choke point in
this plugin able to keep a model-supplied `human_origin` from reaching mupot verbatim on
`task_verdict`/`needs_you_list`.

- `pre_gateway_dispatch` fires once per inbound message, straight off the platform adapter,
  before Hermes's own sender-authorization check runs. It is therefore this module's own
  trust fence, not a convenience filter: a message is captured only when it is a private,
  non-forwarded, self chat (`chat_type == "dm"` and `user_id == chat_id` — Telegram's own DM
  invariant, mirroring `telegram_control.py`'s own private/unforwarded gate). A captured
  record — including a SHA-256 of the message's own text — is *pending* for up to 10 minutes,
  not yet bound to any turn.
- `pre_llm_call` fires once per turn, before the tool loop, and is the positive per-turn
  custody token: it binds a pending capture to the CURRENT `turn_id` iff the turn's own
  fully-prepared inbound text hashes to exactly the captured message's text AND the turn's
  sender matches. An internal/plugin-injected turn's text is the injected notification prompt,
  never the human's own message, so it can never bind; a cron turn and a delegated subagent
  (checked directly against Hermes's delegated-child-context marker) can't either. Two earlier
  designs (a session-keyed slot, then a per-session queue) were both still "whichever turn asks
  first on this session wins" — this one requires the asking turn to *prove* it is processing
  the exact message the record came from.
- `pre_tool_call` fires once per tool dispatch and only ever *reads* what `pre_llm_call` already
  bound to that exact turn — it never claims anything itself. It matches
  `task_verdict`/`needs_you_list` both by bare name and by the exact
  `mcp__<configured mupot server>__<tool>` wire name a live gateway with mupot registered as an
  MCP server actually emits (sanitized the same way Hermes sanitizes a configured server name
  with punctuation in it). A model-supplied `human_origin` is always treated as a forgery
  attempt (logged at WARNING) and stripped for ANY tool whose name looks like a governed one —
  regardless of server-name match, so a misconfigured/unresolved server name can only ever cause
  a missed stamp, never a passthrough — and only replaced with the bound origin on an exact
  match with a turn that actually bound one.
- `on_session_reset`/`on_session_end` drop any pending or bound record for a session the moment
  Hermes itself ends it, rather than relying solely on the 10-minute window.

Only Telegram is supported today. Every other platform is a recorded, not silent, gap: the
first inbound message on an unsupported platform logs one INFO line naming it, and every
`task_verdict`/`needs_you_list` call from that turn simply runs under the agent seat.

### Testing with Hermes

`./scripts/test.sh` runs the standalone operator/provisioner and legacy stream tests.
Native gateway tests use the actual Hermes runtime and its isolated test runner:

```bash
HERMES_SOURCE=/path/to/hermes-agent bash scripts/test-native.sh
```

The cross-repository acceptance additionally requires clean, pinned Mupot and Hermes
checkouts and uses no credentials:

```bash
MUPOT_SERVER_SOURCE=/path/to/mupot \
HERMES_SOURCE=/path/to/hermes-agent \
HERMES_PYTHON=/usr/bin/python3 \
  bash scripts/test-integration.sh
```

The bounded local receipt is recorded in
[`docs/telegram-onboarding-evidence.md`](docs/telegram-onboarding-evidence.md).

The CI native job pins Hermes commit
`233757037df1f03f9fe1cfddc097acd5ad7f7510`; it exercises plugin registration,
lease/ACK handling, private recipient selection, delivery/mirroring, retry recovery,
and the normal conversation activation path. No API credentials are required.

### Consolidating an existing split installation

1. Preserve the old adapter/config and their receipt state outside plugin discovery.
2. Update this `mupot` plugin; enable native mode and the `mupot` injection permission.
3. Disable obsolete `platforms/mupot` and `mupot-platform` plugin entries so exactly
   one plugin owns the Mupot platform. Keep the configured state path to retain ACK
   and notification receipts. If the split adapter used its old default, explicitly
   set that exact old path; the new default is profile-local. Verify processed IDs
   and pending/notification records before and after the switch.
4. Restart the gateway after checking that no turn/delivery is active, then verify
   one authenticated source message, correlated ACK, native human-conversation turn,
   and channel delivery receipt.

Use a full gateway restart for this migration, not forced plugin reload. Existing
legacy stream threads belong to the old process; a restart makes the single-receiver
transition explicit. Native registration refuses a known active legacy stream.

## Restricted operator mode

Copy `examples/operator-config.yaml` into an isolated Hermes profile, replace every
placeholder, and put the agent-bound secret in that profile's protected environment:

```text
MUPOT_AGENT_TOKEN=<agent-bound-token>
```

The plugin verifies the configured tenant and welded `bound_agent_id` before work and
fails closed if the token has owner/admin ladder authority. By default it does not
register permission, credential minting, verdict, publishing, spend, outbound
communication, deletion, or generic HTTP tools.

### Optional squad manager extension

Only a trusted main Hermes profile should enable agent management:

```yaml
plugins:
  entries:
    mupot:
      settings:
        mode: operator
        operator:
          base_url: https://your-pot.example
          expected_tenant: your-tenant
          squad_id: squad-id
          agent_id: manager-agent-id
          approval_owner: human-owner-member-id
          pubsub_peer_agent_ids:
            - isolated-dme-agent-id
          agent_manager_enabled: true
```

Enabling the setting only registers the local tools. Mupot independently requires the
authenticated member to have both membership on that exact squad and the free-text
surface grant `agents:manage`. Before every management action, the plugin verifies its
normal welded identity and then calls the scoped `agent_manager_status` handshake. The
requested action is not sent if either proof fails.

Manager-created agents are always active `member` agents. The published example enables
neither manager mode nor credential management. Lifecycle effects write append-only,
attributed audit receipts. Credential-management tools require a second explicit local
setting and matching server capability; keep them disabled unless they have passed a
separate security review for the target deployment.

> **v0.2 ships the real CF provisioner.** `mupot_provision` with `confirm=True, dry_run=False`
> calls the Cloudflare API directly (pure stdlib urllib — no extra deps) to create D1 databases
> and KV namespaces, then writes `wrangler.<slug>.toml` with the resolved resource IDs.
> Default (`dry_run=True`) emits a plan without touching Cloudflare. Requires
> `MUPOT_CF_API_TOKEN` and `MUPOT_CF_ACCOUNT_ID` in the environment for apply mode.

## Install

```bash
hermes plugins install Mumega-com/mupot-plugin
```

This standalone repository is the Hermes install target. Version `0.3.0` mirrors the
restricted operator implementation reviewed in `Mumega-com/mupot` commit `e8acf8b`.

On a server, create a dedicated Hermes profile, install the plugin into that profile,
copy `examples/operator-config.yaml`, and set `MUPOT_AGENT_TOKEN` through the server's
secret manager or a profile-local file with mode `0600`. Never commit the token.

To verify the complete plugin locally:

```bash
./scripts/test.sh
```

### Companion CF skill tap (for Hermes users)

```bash
hermes skills install cloudflare/skills
```

### Skill-only install (Claude Code, OpenClaw, Codex, etc.)

If you don't use Hermes, drop the bundled skill anywhere your agent loads skills:

```bash
cp -r skills/mupot-operator ~/.claude/skills/
```

## Deploy to Cloudflare (no CLI)

For users who want to deploy a mupot instance directly:

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/Mumega-com/mupot)

*(Reads `wrangler.example.toml`, provisions bindings, deploys. Zero-code path.)*

## Tool surfaces

| Mode | Tool | What it does |
|------|------|-------------|
| provisioner | `mupot_provision` | Idempotent Cloudflare provisioner. |
| provisioner | `mupot_status` | Probe `/health` → `{ok, tenant, url}`. |
| provisioner | `mupot_brain_enable` | Plan the DMN brain profile and schedule. |
| operator | `mupot_operator_status` | Verify tenant, welded identity, and restricted privilege. |
| operator | `mupot_operator_check_in` | Record on-demand Hermes presence. |
| operator | `mupot_operator_task_board` | Read only the configured squad board. |
| operator | `mupot_operator_task_create` | Create a self-assigned scoped task. |
| operator | `mupot_operator_task_claim` | Claim permitted work as the configured identity. |
| operator | `mupot_operator_record_finding` | Record evidence while work is active or blocked. |
| operator | `mupot_operator_request_approval` | Route findings to the configured human; cannot decide the verdict. |
| operator | `mupot_operator_complete_task` | Complete ungated work; Mupot still enforces unresolved gates. |
| operator | `mupot_operator_send` | Send a durable, idempotent mailbox message to an explicitly configured peer agent. |
| operator | `mupot_operator_inbox` | Peek this welded agent's inbox; consume only when explicitly requested after acceptance. |
| manager (opt-in) | `mupot_agent_manager_list` | List configured-squad agents and non-secret token metadata. |
| manager (opt-in) | `mupot_agent_manager_create` | Create an active member agent in the configured squad. |
| manager (opt-in) | `mupot_agent_manager_set_status` | Pause or resume an agent in the configured squad. |
| manager (opt-in) | `mupot_agent_manager_mint_token` | Mint a show-once member token welded to an agent. |
| manager (opt-in) | `mupot_agent_manager_revoke_token` | Revoke an agent-bound token by ID. |

## Provisioner scope / deferred

**In v0.2 (real CF provisioner):**
- Real apply: CF REST API via pure stdlib urllib (no extra deps) — creates D1 + KV idempotently
- Idempotent list-guard: paginates all existing resources before creating (no double-create)
- `wrangler.<slug>.toml` written with resolved D1 + KV IDs after apply
- Optional `wrangler deploy` via subprocess (Risk 4: version-check gate first)
- Token security: never in argv, repr, error messages, or toml output
- Brain profile + cron plan emission (real-file cron, not symlink)
- Deploy-to-Cloudflare button

**Deferred (v0.3+):**
- CF OAuth one-click (pending Mumega OAuth app public approval)
- Full SDK provisioner (no wrangler dependency): `client.workers.scripts.update()`
- OAuth secret automation
- R2 / Vectorize / Queues provisioning (add via re-run)
- `mupot_revoke_token` post-provision cleanup (needs token ID at mint time)
- `pot_registry` / `pot_owners` migrations for the "Your Pots" console

## Operator inbox watcher (v0.3.1)

Operator mode can run a background **inbox watcher** that surfaces new SOS-bus
and Mupot-inbox messages into the live Hermes conversation (`inject_message`),
with a macOS notification fallback. It exists because the desktop gateway does
not route SOS/Mupot inbox traffic on its own.

Enable per profile:

```yaml
plugins:
  entries:
    mupot:
      settings:
        mode: operator
        operator:
          # ... base_url, expected_tenant, squad_id, agent_id, approval_owner ...
          inbox_watch_enabled: true          # opt-in; default off
          inbox_watch_poll_seconds: 30       # floor 10
          inbox_watch_sources: [mupot, sos]  # or a subset
          inbox_watch_sos_token_env: CYRUS_SOS_TOKEN
```

Guarantees: peek-only (never consumes — consuming stays the agent's explicit
act via `mupot_operator_inbox`); first poll baselines the existing backlog so
enabling never replays history; the delivered watermark advances only on
actual delivery, so throttled items are retried, not lost; per-source backoff
on transport errors; state file is per Hermes home. SOS transport note: the
bus WAF rejects urllib's default User-Agent (Error 1010); the watcher sends a
browser UA.

## Key risks

| Risk | Mitigation |
|------|-----------|
| CF token on disk | Least-scoped token (5 groups). Rotate after provision. CF OAuth coming. |
| Migration drift | ALWAYS `--dry-run` first. Tool emits this as a required step, never auto-applies. |
| Brain token scope | Must be `task:read + priority:write` only. NOT `mcp:*`. **Operator's responsibility** — the plugin documents the requirement but cannot enforce token scope. |
| Cron symlink | Real file only. Symlink → silent non-execution. Tool template uses real file. |
| Workers slot | Free tier = 100. Tool warns near limit. Centralised Workers-for-Platforms rejected (breaks sovereignty). |

## CF API token (required for apply mode)

You need a **scoped** token — NOT your Global API Key.

Create one at:
```
https://dash.cloudflare.com/profile/api-tokens
```

Minimum permissions required:
- Workers Scripts: Edit
- D1: Edit
- Workers KV Storage: Edit
- Account Settings: Read

## Development

```bash
./scripts/test.sh
```

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

The native receiver uses Mupot's existing `inbox_lease`/`inbox_ack` interface, verifies
the operator's expected agent/tenant before receiving, preserves message correlation,
and consumes a source only after successful handling. Terminal ACKs are preserved
without generating another peer reply. It does not start an SOS connection.

For human updates, configure `mupot.notification_recipients` with the immutable user
ID for each linked platform. Only matching active private conversations are eligible.
With `mupot.notification_activate: true` and
`plugins.entries.mupot.allow_gateway_injection: true`, Hermes's native plugin API
starts a normal turn in the selected conversation. The native gateway rechecks
authorization and prevents the injected event from executing human slash approvals.
The previous split deployment verified Telegram; other platforms require their own
identity binding and end-to-end verification.

Scheduling acceptance is recorded as `activation_queued`, not as delivered work.
Conversation and native delivery receipts establish completion separately.
Interrupted/ambiguous sends are retained for reconciliation rather than blindly
replayed. This integration does not grant the agent human decision authority.

For the broader project-onboarding and human-control scope, see
[`docs/human-project-control.md`](docs/human-project-control.md).

### Testing with Hermes

`./scripts/test.sh` runs the standalone operator/provisioner and legacy stream tests.
Native gateway tests use the actual Hermes runtime and its isolated test runner:

```bash
HERMES_SOURCE=/path/to/hermes-agent bash scripts/test-native.sh
```

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

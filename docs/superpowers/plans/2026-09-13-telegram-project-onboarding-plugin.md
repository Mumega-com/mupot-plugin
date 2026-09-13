# Telegram Project Onboarding — Hermes Plugin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic Telegram join, attention, and decision commands to the consolidated Mupot plugin while Mupot remains the human authority.

**Architecture:** Register narrowly matched native Telegram handlers through `PluginContext.register_telegram_handler`. The handlers derive immutable Telegram update/user/chat IDs and relay a bounded envelope to Mupot `/im/webhook`; they do not interpret consent or authorize locally. The existing native Mupot receiver activates the same private conversation for outbound decisions/results.

**Tech Stack:** Python 3.11–3.13, Hermes plugin API, python-telegram-bot, pytest/unittest, pinned Hermes native CI.

**Spec:** `docs/human-project-control.md`

## Global Constraints

- Match only `/start`, `/needs`, `/answer`, `/approve`, and `/reject`; core Hermes handles everything else.
- Accept onboarding/decisions only in a private chat where immutable user ID equals chat ID.
- Mupot resolves membership, project visibility, role, gate authority, decision state, and idempotency.
- Never log invite codes, webhook secret, tokens, or approval payloads.
- Deterministic commands return Mupot’s bounded response and do not start an LLM turn.
- Preserve native Mupot receive, ACK, activation, and state migration behavior from PR #6.

---

### Task 1: Add deterministic Telegram control handlers

**Files:**
- Create: `telegram_control.py`
- Modify: `__init__.py`
- Modify: `plugin.yaml`
- Test: `tests/test_telegram_control.py`

**Interfaces:**
- Produce `TelegramControlSettings`, `relay_telegram_update(settings, update)`, and `register_telegram_control(ctx, settings)`.
- Consume `ctx.register_telegram_handler(factory)` and Mupot `/im/webhook`.

- [ ] Write failing tests for explicit opt-in, HTTPS base URL, secret env-name validation, handler registration, private-chat/user equality, forwarding refusal, bounded request/response, redacted failure, and five exact commands.
- [ ] Run `./scripts/test.sh`; verify RED.
- [ ] Implement settings:

```py
@dataclass(frozen=True)
class TelegramControlSettings:
    enabled: bool
    base_url: str
    webhook_secret_env: str = "IM_WEBHOOK_SECRET"
    timeout: float = 20.0
```

- [ ] In the native factory, register scoped PTB `CommandHandler`s without a catch-all callback. Build the Mupot envelope only from `update.update_id`, `effective_user.id`, `effective_chat.id`, display name, text, and forwarding markers; POST with `X-Telegram-Bot-Api-Secret-Token`; return only the bounded `reply`.
- [ ] Register the handlers before operator/native-platform side effects, so invalid configuration leaves no partial surface.
- [ ] Run `./scripts/test.sh`; expect exit 0. Commit: `feat(telegram): relay project control to Mupot`.

### Task 2: Prove native handler isolation and activation coexistence

**Files:**
- Create: `tests/native/test_telegram_control_native.py`
- Modify: `tests/native/test_notifications.py`
- Modify: `scripts/test-native.sh`

- [ ] Write a failing real-plugin-discovery test in temporary `HERMES_HOME`: `/start` and `/answer` invoke plugin handlers once, core catch-all/LLM handler is untouched, ordinary text reaches Hermes, and an outbound Mupot event activates that same private session once.
- [ ] Run `HERMES_SOURCE=/home/mumega/.hermes/hermes-agent HERMES_PYTHON=/usr/bin/python3 bash scripts/test-native.sh -q`; verify RED.
- [ ] Correct PTB handler group/order and plugin unload ownership. Keep SDK imports inside the native factory.
- [ ] Run native and standalone suites; expect exit 0. Commit: `test(telegram): verify native project control`.

### Task 3: Document, package, and run the pilot

**Files:**
- Modify: `README.md`
- Modify: `examples/native-gateway-config.yaml`
- Modify: `docs/human-project-control.md`
- Create: `docs/telegram-onboarding-runbook.md`

- [ ] Document participant flow: receive code out of band, `/start`, role/project confirmation, `/needs`, one real `/answer` or verdict, automatic result delivery, suspension/revocation, duplicate/stale refusal.
- [ ] Add `telegram_control_enabled: true`, `telegram_control_webhook_secret_env: IM_WEBHOOK_SECRET`, and exact Mupot URL to the example; one receiver and one Telegram bot per profile.
- [ ] Run `./scripts/test.sh`, native suite, `git diff --check`, and secret-pattern scan.
- [ ] Commit, push, and update plugin PR #6.
- [ ] After reviewed Mupot deployment, run one real non-admin pilot and record invite, identity binding, authorized attention, decision, routine continuation, result delivery, duplicate/stale refusal, capability revocation, post-revocation denial, restart/retry, and rollback evidence.


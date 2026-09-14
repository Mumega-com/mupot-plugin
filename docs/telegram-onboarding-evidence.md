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

## Kasra gate BLOCK repair (2026-09-14) — injection fence, e-stop bypass, fail-closed allowlist

This section replaces no earlier number above; it records the fix landed on top of PR #6
head `3cc6f297239a0bbdfcb295d33f692298dda06410` in response to the Kasra gate BLOCK comment
(fresh-context arm, pinned-Hermes `2337570`, 22-guard mutation run: 17 red, 5 survivors:
M1, M7, M12, M17, M18) posted on that head. New head after this commit: see the PR; report
below is what this session actually ran, not carried-over prior numbers.

### What changed

- **P0-1** (`mupot_gateway/notifications.py`, `flush()`): the untrusted notice body (routine
  `decision.question`, or a peer terminal-ACK body) is now wrapped in a delimited
  ` ```mupot-notice ` fence (a body containing its own triple-backtick cannot force an early
  close — a zero-width space breaks it) and the "not a human instruction/approval" caveat is
  appended strictly **after** the fenced block, not before it. `inject_message` is now called
  with an explicit `role="mupot-notice"` — confirmed against the pinned Hermes
  `hermes_cli/plugins.py:596` that any non-`"user"` role still reaches the same
  gateway-injection call path (it only changes a `"[{role}] "` text prefix; the fence, not the
  role, is the real control).
- **P0-2** (`mupot_gateway/adapter.py`): `build_mupot_event` still sets `event.internal=True`
  (needed for Hermes busy-routing — queueing as a distinct turn). Verified directly against
  the pinned Hermes checkout that `gateway/run_inbound.py:174` returns for any internal event
  before it reaches `_is_user_authorized_for_source` (:185) or the global e-stop gate (:233) —
  so `hermes pause` silently never stopped mupot traffic through this path, and the adapter's
  comment falsely claimed Hermes consulted a second copy of the allowlist via
  `extra["allow_from"]` (verified: that key is read nowhere in `gateway/*.py` at the pinned
  rev). Fix: dropped the dead `allow_from` line/comment, and added an explicit
  `_estop_engaged()` check in `_deliver` (fails safe to "engaged" on a stat error, same as
  `agent.estop.is_engaged()` itself) so the adapter enforces the property Hermes's own gate no
  longer does for this call path.
- **P1-1** (`mupot_gateway/adapter.py`, `MupotAdapter.__init__`): `extra.get("allowed_agents")
  or DEFAULT` treated an explicitly configured empty allowlist (`""`/`[]`) the same as an
  absent key. Now only a genuinely absent key falls back to the default roster; an explicit
  empty value denies everyone.
- `normalize_agent`'s `agent:`-prefix strip is now documented as defensive-only: verified
  against `Mumega-com/mupot` `src/agents/messages.ts` (`sendAgentMessage`: `fromAgent` is
  `auth.boundAgentId`, a raw UUID) and `src/agents/inbox-routes.ts` (the delivered
  `from_agent` column is that same value) that mupot never emits an `agent:`-prefixed
  `from_agent`.
- Deduped `_LEASE_ATTEMPT_ID_RE` (adapter.py) against `lease_ownership.py`'s identical pattern
  into one shared `ATTEMPT_ID_RE`.
- **P2 inspector**: `activation_unknown`/`transport_unknown` notices (a "preserved for
  inspection" terminal state that nothing previously looked at) are now logged at WARNING with
  a count on adapter startup, and exposed through a new `mupot_gateway_status` tool
  (`MupotAdapter.stranded_notifications()`).

### Mutation table (this session)

Ran from a clean commit (`git checkout -- <file>` after every row; tree diff-clean before and
after this table). "RED" means the exact test(s) named failed against the mutated line;
"GREEN" means the full relevant suite passed once reverted.

| # | Guard | File:line | Before fix (unfixed code) | After fix |
|---|---|---|---|---|
| M1 | Peer allowlist gate on delivery | `adapter.py` `if should_accept_message(...)` → `if True:` | **RED** — `test_unlisted_sender_is_quarantined_never_delivered` failed (handler received the attacker body) | **GREEN** — reverted, full native suite green |
| M7 | Competing-receiver guard | `__init__.py:205` `if native_gateway and _ACTIVE_WATCHERS:` → `if False:` | **RED** — `test_switching_to_native_receive_with_an_active_legacy_stream_is_refused` failed | **GREEN** |
| M12 | Telegram command allowlist refusal | `telegram_control.py:164` → `if False:` | **RED** — both `test_relay_refuses_an_unsupported_command_before_network` and `test_native_callback_replies_with_refusal_for_an_unsupported_command` failed | **GREEN** |
| M17 | Lease attempt_id format check | `lease_ownership.py:74` (`or _ATTEMPT_ID_RE.fullmatch(...) is None`) → `or False` | **RED** — 9 of the new `tests/test_lease_ownership.py` cases failed (all malformed-string cases; non-string cases still caught by the adjoining `isinstance` check) | **GREEN** |
| M18 | `_NoRedirect` refuses a redirect | `telegram_control.py:39-51` `redirect_request` → `return super().redirect_request(...)` (follows the redirect) | **RED** — `test_no_redirect_refuses_a_real_302_and_never_forwards_the_secret_header` failed (real local HTTP redirector + target servers; the request was actually forwarded, target returned 501 for the wrong method instead of the refusal proving it was never reached) | **GREEN** |
| new | P0-1 injection fence | `notifications.py` `flush()` `event = (...)` reverted to the original unfenced/caveat-before-body construction | **RED** — new `test_attacker_controlled_decision_question_is_fenced_not_an_instruction` AND the pre-existing `test_activation_queues_existing_human_conversation_instead_of_passive_send` both failed | **GREEN** |
| new | P0-2 e-stop enforcement | `adapter.py` `_deliver`'s `if _estop_engaged():` → `if False:` | **RED** — `test_estop_engaged_blocks_dispatch_before_any_state_mutation` failed (handler ran while a fake `agent.estop.is_engaged()` reported paused) | **GREEN** |

All 7 rows above were exercised directly by me this session (temporary in-place edit, real
pytest run against the pinned-rev-matching Hermes clone or the plain suite as appropriate,
`git checkout -- <file>` to restore, confirmed `git status --short` empty afterward). This
covers 5 of the review's 22 guards (the ones explicitly named for this repair) plus the 2 new
guards added for the P0-1/P0-2 fixes; it does not re-run the other 17 guards from the original
22-guard list, which were not touched by this change.

### Real test counts (this session)

- `scripts/test.sh` (isolated venv, `pytest==9.1.1`/`pytest-asyncio==1.3.0` per
  `requirements-test.txt`, system Python untouched): **239 passed, 12 subtests passed** (pytest)
  + **27/27** (`unittest` `test_operator`), 0 failures.
- `scripts/test-native.sh` (isolated venv against a fresh `NousResearch/hermes-agent` clone
  pinned to `233757037df1f03f9fe1cfddc097acd5ad7f7510` — the same rev CI pins, and the same
  rev as the read-only reference checkout supplied for this task; the reference checkout
  itself was never written to): **276 tests passed, 0 failed**, across all 9 files in
  `tests/native/`.

### Not done / could not verify

- `scripts/test-integration.sh` (cross-repo Mupot server + plugin harness) was **not** run
  this session — it requires a separately pinned Mupot server checkout with its own Vitest
  install (`MUPOT_SERVER_SOURCE`) that was not provisioned for this task. The numbers in the
  "Final attempt-v3" section above are from an earlier session and are not re-verified here.
- Only 5 of the original 22 mutation guards plus the 2 new ones were re-run; the other 17 were
  unaffected by this change and were not re-verified in this session (they were last verified
  RED/GREEN by the reviewing arm, not by me).
- `ruff`/`mypy`/full-base lint sweeps mentioned in the section above were not re-run this
  session; only `py_compile` (all changed `.py` files) and the two test scripts were used to
  gate this commit.

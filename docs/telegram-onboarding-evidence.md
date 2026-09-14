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

## Kasra gate BLOCK repair, round 2 (2026-09-14)

The round-1 repair above was individually plausible on every named point but was BLOCKed
again on re-gate: the fence escape and the e-stop gate each fixed the reported repro without
fixing the underlying class. This round's fixes are described in the `fix(security):` commit
following `ab2b9d82`. Method note: fixed a stale-argument bug in my first mutation script (a
copy-paste of the escaped-vs-literal `​` sequence did not match the source file byte-for-
byte on the first attempt, giving a false "0 occurrences" before I corrected it) — verified
`occurrences == 1` before writing every mutation below, per the standing "assert occurrence
count == 1 before writing" mutation-harness rule.

### What was actually wrong (re-gate verdict on head `ab2b9d82`)

1. **P0-1 residual**: `text.replace("```", "`​``")` is left-to-right, non-overlapping —
   any backtick run whose length is not itself a multiple of 3 (4, 5, 6, 9, ANSI-wrapped or
   not) left an unconsumed backtick that recombined with the replacement into a fresh literal
   run of 3, reopening the exact fence-escape hole the round-1 fix believed it had closed.
2. **P0-2 residual**: `_estop_engaged()` was added to `MupotAdapter._deliver` only.
   `_flush_notifications` (the only caller of `notifications.flush()`, which is the only
   caller of the message injector) and `_handle_routine_event` (which ACKs — irreversibly
   consumes — the Routine source) both reach those effects without ever going through
   `_deliver`. Proven live with the real `agent/estop.py` sentinel: under `hermes pause`, the
   routine path still called `inbox_ack` and still injected into the human's session.
3. **P1**: `test_estop_engaged_fails_open_to_false_when_hermes_estop_is_unimportable` ran in
   the NATIVE suite, where `agent.estop` genuinely is importable, so
   `monkeypatch.delitem(sys.modules, "agent.estop")` just forced a successful re-import; the
   `except ImportError: return False` branch had zero real coverage (proved: replacing it with
   `raise AssertionError` left the whole native suite green).
4. Comment/docs hygiene: the `allow_from` removal comment claimed a false global negative
   (`gateway/authz_mixin.py`, `config_loader.py`, and `pairing.py` all read it); `allowed_agents`
   normalization was undocumented; `mupot_gateway_status` was missing from `plugin.yaml`'s
   `provides_tools`; a non-iterable `allowed_agents` raised a raw `TypeError`; the legacy
   `_maybe_start_inbox_stream` injector was still unfenced; `_live_adapter` was never cleared
   on disconnect.

### Fix shape (class fix, not repro-shaped)

- **Fence**: escape every backtick character individually
  (`text.replace("`", "`" + "​")`), not just literal 3-backtick runs. No two backtick
  characters can ever be adjacent in the escaped output, so no run of even 2 backticks
  survives — strictly stronger than "no run of ≥3", and independent of run length or
  surrounding ANSI/CR/LF/ZWSP content. `__init__.py`'s legacy `_maybe_start_inbox_stream`
  injector now routes through the same `_fenced_untrusted_block` helper instead of injecting
  unfenced text — one escaping predicate in the codebase, not two.
- **E-stop**: moved the enforcement to the actual choke points instead of duplicating a check
  at whichever call site a review happened to name. `notifications.py`'s `flush()` gates its
  own single call to `activate()` (the message injector) — every present producer (the peer
  terminal-ACK branch, the Routine event branch) and any future one inherits this for free.
  `_handle_routine_event` and `_handle_ack_envelope` both gate at their own top, before any
  state mutation (mirroring `_deliver`'s existing guard), so neither ACKs/consumes its source
  while paused. `_deliver`'s pre-existing gate is untouched (regression-checked below).
- **P1 test**: rewritten to force a real `ImportError` via `builtins.__import__` rather than
  deleting an already-importable module from `sys.modules`. The fail-open branch now logs a
  WARNING once per process (asserted in the test via a forced-fresh `_ESTOP_IMPORT_WARNED`
  reset), so a future `agent.estop` removal/move is never silent.
- **Hygiene**: `allow_from` comment rewritten to the correct, scoped claim (read on Hermes's
  non-internal auth path, which `internal=True` skips today — revisit if that ever changes);
  its guarding test renamed to match; `allowed_agents` non-list/non-string now raises a clear
  `ValueError`; `mupot_gateway_status` declared in `plugin.yaml`; `_live_adapter` cleared on
  `disconnect()`; README/runbook/example document `allowed_agents` normalization and a new
  runbook section covers operating on a stranded notification.

### Mutation table (round 2, this session)

Every row: temporary in-place edit on a clean, already-committed tree (commit `ff6e9d8`),
real pytest run (native suite against the pinned-rev-matching `hermes-runtime-testcopy` clone,
or the plain suite as appropriate), `git checkout -- <file>` to restore, `git status --short`
confirmed empty before moving to the next row.

| # | Guard | File:line | Mutation | Result |
|---|---|---|---|---|
| new | P0-1 fence, run-length class | `notifications.py` `_fenced_untrusted_block`: `safe = text.replace(...)` → `safe = text` (no-op) | **RED** — all 14 new parametrized tests failed: `test_fence_survives_every_backtick_run_length_kasra_review_regate` (7 cases: run-3/4/5/6/9, ansi-prefixed-run-4, mixed-cr-lf-zwsp) in `test_routine_events.py`, plus 7 unit-level cases (`test_fenced_untrusted_block_escapes_*`) in `test_notifications.py` | **GREEN** — reverted, both suites green |
| new | P0-2 routine-path gate | `adapter.py` `_handle_routine_event`'s `if _estop_engaged():` → `if False:` | **RED** — `test_real_estop_sentinel_blocks_routine_ack_and_injection_then_resumes` failed (real `agent.estop` sentinel engaged; `client.calls` had an `inbox_ack` call instead of `[]`) | **GREEN** |
| new | P0-2 flush() single choke point | `notifications.py` `flush()`'s `if _estop_engaged():` (before calling `activate`) → `if False:` | **RED** — `test_flush_real_estop_sentinel_blocks_activation_at_the_single_choke_point` failed (`activations` had 1 entry instead of `[]`) — proves the fix is structural at the injector call site, independent of which producer enqueued the notice | **GREEN** |
| new | P0-2 ack-envelope gate | `adapter.py` `_handle_ack_envelope`'s `if _estop_engaged():` → `if False:` | **RED** — `test_real_estop_sentinel_blocks_ack_envelope_injection_then_resumes` (real sentinel) AND `test_estop_engaged_defers_ack_envelope_before_any_state_mutation` (faked sentinel) both failed | **GREEN** |
| new | P1 fail-open branch body | `adapter.py` `_estop_engaged`'s `except ImportError: ...; return False` → `except ImportError: raise AssertionError(...)` | **RED** — `test_estop_engaged_fails_open_to_false_when_hermes_estop_is_unimportable` failed with the injected `AssertionError`, proving the rewritten test actually executes this branch (the required "delete-the-branch-body" honesty check) | **GREEN** |
| M1 (no-regression) | Peer allowlist gate | `adapter.py` `should_accept_message` → `return True` unconditionally | **RED** — `test_unlisted_sender_is_quarantined_never_delivered` failed | **GREEN** |
| M15/M16/M20/M22-class (no-regression) | P1-1 fail-open allowlist | `adapter.py` `__init__`: `allowed = extra.get("allowed_agents")` restored to `... or "hadi-codex,hadi-codex-cli,kasra,hermes"` (the round-1 defect) | **RED** — `test_explicit_empty_allowed_agents_denies_everyone` (both parametrized cases) failed, plus 2 of this round's new `test_allowed_agents_non_iterable_value_raises_clear_config_error` cases (`0`, `False` — falsy values masked by `or`) | **GREEN** |
| new | `allowed_agents` type validation | `adapter.py`: `elif not isinstance(allowed, (list, tuple, set, frozenset)):` → `elif False:` | **RED** — all 6 parametrized cases of `test_allowed_agents_non_iterable_value_raises_clear_config_error` failed with `DID NOT RAISE ValueError` | **GREEN** |
| new | `_live_adapter` cleared on disconnect | `adapter.py` `register()`: dropped the `instance.disconnect = _disconnect_and_clear_live_adapter` reassignment | **RED** — `test_gateway_status_clears_to_disconnected_after_adapter_disconnect` failed (`ok: True` after `disconnect()` instead of `native_gateway_not_connected`) | **GREEN** |
| new | `allow_from` dead-line regression | `adapter.py` `__init__`: re-added `extra["allow_from"] = sorted(self.allowed_agents)` after the removal | **RED** — `test_allow_from_is_not_derived_on_the_internal_true_path` (the renamed guard) failed | **GREEN** |

All 10 rows above were executed directly this session, not asserted. `git status --short` was
empty (clean tree) before the first mutation and after the last restore.

### Real test counts (round 2, this session)

- `scripts/test.sh`: **240 passed, 12 subtests passed** (pytest, up from the round-1 baseline
  of 239 — the net-new `test_native_registration.py` case) + **27/27** (`unittest`
  `test_operator`), 0 failures.
- `scripts/test-native.sh` (fresh `HERMES_SOURCE`/`HERMES_PYTHON` pointed at the same pinned
  `233757037df1f03f9fe1cfddc097acd5ad7f7510` clone used for verification, not the read-only
  reference checkout): **302 tests passed, 0 failed**, across all 9 files in `tests/native/`
  (up from the round-1 baseline of 276 — the 26 net-new tests from this round: 14 fence
  tests, 3 real-sentinel end-to-end tests, the rewritten P1 test, the ack-envelope estop test,
  6 `allowed_agents` type-validation cases, the renamed `allow_from` test, and the
  `_live_adapter` disconnect test).

### Not done / could not verify (round 2)

- `scripts/test-integration.sh` still not run this round for the same reason as round 1 (no
  provisioned `MUPOT_SERVER_SOURCE`).
- Only M1 and the P1-1 fail-open allowlist property (the two named "allowlist / no-regression"
  guards explicitly re-run per this round's brief) were re-verified from the ORIGINAL 22-guard
  list; M7/M12/M17/M18 (round-1's other four named guards) were not re-run this round since
  none of this round's diff touches `__init__.py`'s `_ACTIVE_WATCHERS` guard,
  `telegram_control.py`, or `lease_ownership.py` — unaffected by this change. The full native
  and plain suites both ran clean (0 failures) both before and after every mutation restore,
  which is the strongest available evidence those untouched guards were not disturbed, but it
  is not the same as an individually re-executed mutation for each one.
- `ruff`/`mypy` were not re-run this round; `py_compile` on the changed files plus both test
  scripts (each run to completion, clean, both before mutation testing and after every
  restore) gated this commit.
- The `_poll_loop`-level interaction between an estop-deferred message and the loop's own
  post-processing bookkeeping (`if message_id not in self._state.get("processed", []): raise
  _protocol_error()`, which on an unhandled exception quarantines the whole adapter via
  `_quarantine_inbox_polling()`) was NOT independently re-verified this round for the new
  `_handle_routine_event`/`_handle_ack_envelope` gates. This is the same pre-existing shape as
  `_deliver`'s round-1 gate (which also returns without marking the message processed) — not a
  new regression introduced this round — but nobody has proven whether a real `hermes pause`
  driven through the FULL `_poll_loop` (rather than calling `_handle_routine_event` /
  `_handle_ack_envelope` / `_deliver` directly, as every estop test in this repo including this
  round's does) quarantines the adapter instead of benignly deferring. Stated plainly rather
  than silently assumed safe: this is a real open question for a future pass, not a fixed and
  verified property.

## Kasra re-gate #2 repair, round 3 (2026-09-14)

Round 2 closed the fence-escape and gated all three e-stop call sites, but re-gate #2 (head
`936baa61`) BLOCKed again: exactly the open question flagged at the end of round 2's section
above turned out to be a real, live P0 — proven through the real `_poll_loop`, not a direct
call to a gated method — plus a new P1 (fencing was never shared across `flush()`'s three
sinks). This round's fixes are in the `fix(security):` commit following `936baa61`.

### What was actually wrong (re-gate #2 verdict on head `936baa61`)

1. **P0 — a pause durably quarantines the inbox.** `adapter.py`'s `_poll_loop`: after
   `_process_leased_message` returns, `if message_id not in self._state.get("processed", []):
   raise _protocol_error()`. All three e-stop gates (`_handle_routine_event`,
   `_handle_ack_envelope`, `_deliver`) returned bare on pause without marking the message
   processed, so every one of them hit this branch, which then called
   `_quarantine_inbox_polling()` — a durable state that: sets `_lease_quarantined = True`,
   persists `lease_reconciliation` to `state.json`, and makes the poll task `return` (exit).
   `connect()` then refuses (`if self._lease_quarantined: return False`), and
   `reconcile_inbox_polling()` itself fails while still paused. Proven live: engaging the REAL
   `agent/estop.py` sentinel and driving a message through the REAL `_poll_loop` (not a direct
   call) gave zero activations, a dead poll task, and a refused reconnect after `hermes
   resume`. Round 2's own two doc-comments (`adapter.py`'s `_handle_routine_event` gate,
   `notifications.py`'s `flush()` choke-point comment) both asserted "no reconciliation state
   is recorded" — falsified by this exact mechanism.
2. **P1 — `flush()` fenced only the activation branch.** `deliver_text` (Telegram) and
   `mirror_text` (conversation mirror) shipped `notice["text"]` completely raw — no fence, no
   caveat. `mirror_to_session` was called with no `role` kwarg, defaulting to `"assistant"`;
   Hermes's own `gateway/mirror.py:34-38` documents that non-agent text mirrored at that
   default role replays as a genuine agent turn, not a quoted inbound message.
3. **P3 (cheap):** `routine_events._SOURCE_ID_RE` admitted backtick/`[`/`]` (defense-in-depth
   only — mupot generates ids via `crypto.randomUUID`, so not attacker-reachable today);
   `allowed_agents` list entries that were not strings (e.g. `[123, "kasra"]`) silently
   stringified through `normalize_agent`'s `str(value or "")` instead of failing loudly.

### Fix shape (class fix, not repro-shaped)

- **P0**: a pause is a temporal condition, never a state transition, enforced at two points:
  (a) `_poll_loop` now checks `_estop_engaged()` at the top of every iteration, BEFORE
  `inbox_lease` is ever called — nothing is leased, acked, injected, or written while paused,
  which also means the `routine_events_disabled`-quarantine and sender-policy-DLQ branches
  inside `_process_leased_message` can never fire while paused (neither can run without a
  lease first — verified directly, see mutation table). (b) For the narrow race where the
  e-stop engages AFTER the pre-lease check but BEFORE the leased message is handled,
  `_deliver`/`_handle_routine_event`/`_handle_ack_envelope` now `raise` a new `_EstopDeferred`
  exception (not a bare `return`). Both `_poll_loop` and `reconcile_inbox_polling`'s own copy
  of the same "process then check processed then `_protocol_error()`" pattern catch
  `_EstopDeferred` specifically, release the lease fence, and let the caller continue/retry —
  never raising `_protocol_error()` or calling `_quarantine_inbox_polling()` for a pause.
  Corrected the two falsified comments.
- **P1**: `flush()` now builds one `fenced_text = _fenced_untrusted_block(notice["text"]) +
  _UNTRUSTED_CAVEAT` per notice, ONCE, before branching into activation vs. deliver/mirror —
  all three sinks consume the identical string. `_UNTRUSTED_CAVEAT` is a new, lighter,
  human-readable caveat (distinct from `_fenced_untrusted_block` itself, which stays
  fence-only and unit-test-pinned) appended after the real closing fence, so Telegram/mirror
  recipients get a caveat too, not just the activation branch's longer agent-facing prose.
  `mirror_text` now passes `role="user"` to `mirror_to_session`.
- **P3**: `_SOURCE_ID_RE` tightened to `^[^\s`\[\]]{1,128}$`; `allowed_agents` list
  construction now raises `ValueError` for any non-string entry before it ever reaches
  `normalize_agent`.

### Mutation table (round 3, this session)

Every row: temporary in-place edit (assert occurrence count == 1 before writing) on a clean,
already-committed tree (commit `40f8a10`), real pytest run (native suite against the
pinned-rev-matching `hermes-runtime-testcopy` clone, or the plain suite as appropriate),
`git checkout -- <file>` to restore, `git status --short` confirmed empty before moving to
the next row. Kasra-review's own private numbering (F/E/I/L/V/M) for the guards it asked to
be re-verified could not be recovered exactly (not documented anywhere retrievable in this
repo, same limitation noted in round 2's own table) — mapped here to the closest-matching
guard by content, and the actual line/behavior mutated is stated plainly for each row instead
of asserted from the label alone.

| # | Guard (best-effort label) | File:line | Mutation | Result |
|---|---|---|---|---|
| F1 | Fence escape (run-length class, carried from round 2) | `notifications.py` `_fenced_untrusted_block`: `safe = text.replace(...)` → `safe = text` | **RED** — 8 tests failed: all 5 `test_fenced_untrusted_block_escapes_every_backtick_run_length` cases, `..._ansi_prefixed_run`, `..._mixed_cr_lf_zwsp_body`, and the new `test_flush_fences_and_shares_one_string_across_deliver_and_mirror` | **GREEN** |
| F2 | One fenced string shared by all 3 `flush()` sinks (NEW, P1) | `notifications.py` `flush()`: `fenced_text = _fenced_untrusted_block(...) + _UNTRUSTED_CAVEAT` → `fenced_text = notice["text"]` (raw) | **RED** — `test_activation_queues_existing_human_conversation_instead_of_passive_send` (pre-existing) AND `test_flush_fences_and_shares_one_string_across_deliver_and_mirror` (new) both failed | **GREEN** |
| — | Mirror role (NEW, P1) | `notifications.py` `mirror_text`: dropped `role="user"` from the `mirror_to_session` call (defaults to `"assistant"`) | **RED** — `test_flush_fences_and_shares_one_string_across_deliver_and_mirror` failed (`mirrored["role"] == "assistant"`) | **GREEN** |
| — | Pre-lease pause check (NEW, P0 part a) | `adapter.py` `_poll_loop`: `if _estop_engaged():` (before `inbox_lease`) → `if False:` | **RED** — both new real-poll-loop tests (`test_real_poll_loop_pauses_before_lease_then_resumes` [routine] and `..._deliver` [peer]) failed: `inbox_lease` was called while paused | **GREEN** |
| — | `_protocol_error` restored on the pause path (NEW, P0 part b/c — the exact mutation the brief named) | `adapter.py` `_poll_loop`: `except _EstopDeferred:` (mid-message handler) → `except KeyError:` (bypasses the defer handling, `_EstopDeferred` falls through to the outer `except Exception: self._quarantine_inbox_polling(); return`) | **RED** — both new mid-message tests (`test_real_poll_loop_defers_mid_message_then_resumes_without_quarantine` [routine] and `..._deliver`) failed: poll task ended, `[mupot] inbox polling quarantined; reconciliation required` logged | **GREEN** |
| E1 | `_handle_routine_event` e-stop gate | `adapter.py`: `if _estop_engaged():` (routine) → `if False:` | **RED** — `test_real_estop_sentinel_blocks_routine_ack_and_injection_then_resumes` (real sentinel, direct call) AND `test_real_poll_loop_defers_mid_message_then_resumes_without_quarantine` (real poll loop) both failed | **GREEN** |
| E2 | `_handle_ack_envelope` e-stop gate | `adapter.py`: `if _estop_engaged():` (ack-envelope) → `if False:` | **RED** — `test_estop_engaged_defers_ack_envelope_before_any_state_mutation` (faked sentinel) AND `test_real_estop_sentinel_blocks_ack_envelope_injection_then_resumes` (real sentinel) both failed | **GREEN** |
| E3 | `_deliver` e-stop gate | `adapter.py`: `if _estop_engaged():` (deliver) → `if False:` | **RED** — `test_estop_engaged_blocks_dispatch_before_any_state_mutation` (faked sentinel) AND `test_real_poll_loop_defers_mid_message_deliver_then_resumes_without_quarantine` (real poll loop) both failed | **GREEN** |
| E4 | `flush()` single choke point e-stop gate | `notifications.py` `flush()`: `if _estop_engaged():` (before `activate`) → `if False:` | **RED** — `test_flush_real_estop_sentinel_blocks_activation_at_the_single_choke_point` failed (`activations` had 1 entry) | **GREEN** |
| V1 | P1 residual: fail-open branch body is non-vacuous | `adapter.py` `_estop_engaged`'s `except ImportError: ...; return False` → `...; raise AssertionError(...)` | **RED** — `test_estop_engaged_fails_open_to_false_when_hermes_estop_is_unimportable` failed with the injected `AssertionError`, proving the branch actually executes | **GREEN** |
| M1 | Peer allowlist gate (no-regression) | `adapter.py` `_process_leased_message`: `if should_accept_message(...)` → `if True:` | **RED** — `test_unlisted_sender_is_quarantined_never_delivered` failed (attacker body reached the handler) | **GREEN** |
| I1 | `allowed_agents` non-iterable type check | `adapter.py`: `elif not isinstance(allowed, (list, tuple, set, frozenset)):` → `elif False:` | **RED** — all 6 `test_allowed_agents_non_iterable_value_raises_clear_config_error` cases failed (`DID NOT RAISE`) | **GREEN** |
| I2 / M16 | `allowed_agents` fail-open regression | `adapter.py` `__init__`: `allowed = extra.get("allowed_agents")` → `... or "hadi-codex,hadi-codex-cli,kasra,hermes"` (the original round-1 defect) | **RED** — both `test_explicit_empty_allowed_agents_denies_everyone` cases AND 2 of `test_allowed_agents_non_iterable_value_raises_clear_config_error` (`0`, `False`) failed | **GREEN** |
| I3 | `allowed_agents` non-string list entry (NEW, P3) | `adapter.py`: removed the `for item in allowed: if not isinstance(item, str): raise ValueError(...)` block | **RED** — all 7 `test_allowed_agents_non_string_list_entry_raises_clear_config_error` cases failed (`DID NOT RAISE`) | **GREEN** |
| — | `_SOURCE_ID_RE` backtick/bracket exclusion (NEW, P3) | `routine_events.py`: `_SOURCE_ID_RE` reverted to `^[^\s]{1,128}$` (no backtick/`[`/`]` exclusion) | **RED** — all 4 `test_source_id_regex_rejects_backtick_and_bracket_characters` cases failed (`DID NOT RAISE`) | **GREEN** |
| M7 | Competing-receiver guard | `__init__.py`: `if native_gateway and _ACTIVE_WATCHERS:` → `if False:` | **RED** — `test_switching_to_native_receive_with_an_active_legacy_stream_is_refused` failed | **GREEN** |
| M12 | Telegram command allowlist | `telegram_control.py`: `if command not in {...}:` → `if False:` | **RED** — both `test_relay_refuses_an_unsupported_command_before_network` and `test_native_callback_replies_with_refusal_for_an_unsupported_command` failed | **GREEN** |
| M17 | Lease attempt_id format check | `lease_ownership.py`: `or _ATTEMPT_ID_RE.fullmatch(attempt_id) is None` → `or False` | **RED** — 9 `tests/test_lease_ownership.py` cases failed | **GREEN** |
| M18 | `_NoRedirect` refuses a redirect | `telegram_control.py`: `redirect_request` → follows the redirect via `super()` | **RED** — `test_no_redirect_refuses_a_real_302_and_never_forwards_the_secret_header` failed (real local HTTP redirector; request actually forwarded, wrong-method 501 instead of the refusal) | **GREEN** |
| L1 | Legacy `_maybe_start_inbox_stream` shares the fence helper | `__init__.py`: `fenced = (...)` (fence-wrapped) → `fenced = text` (raw) | **RED** — `test_maybe_start_inbox_stream_routes_deliver_through_shared_fence_helper` failed | **GREEN** |
| L2 | `_live_adapter` cleared on disconnect | `adapter.py` `register()`: dropped the `instance.disconnect = _disconnect_and_clear_live_adapter` reassignment | **RED** — `test_gateway_status_clears_to_disconnected_after_adapter_disconnect` failed | **GREEN** |
| — | `allow_from` dead-line regression | `adapter.py` `__init__`: re-added `extra["allow_from"] = sorted(self.allowed_agents)` | **RED** — `test_allow_from_is_not_derived_on_the_internal_true_path` failed | **GREEN** |

21 rows above, all executed directly this session (temporary in-place edit, real pytest run,
restore, confirmed clean). This covers: both fence-sharing guards (F1/F2) plus the mirror-role
guard; the new pause/liveness class in full (pre-lease check, the `_protocol_error`-restored
mutation the brief explicitly named, and all 4 individual e-stop call-site gates E1-E4); the
P1 vacuous-test-fix guard (V1); the peer allowlist no-regression guard (M1); all `allowed_agents`
validation guards (I1/I2/I3, including the new P3 fix); the new `_SOURCE_ID_RE` P3 guard; and
the four round-1 guards this round's diff does not touch but the brief named explicitly
(M7/M12/M17/M18), each individually re-mutated rather than inferred safe from a green suite.

### Real test counts (round 3, this session)

- `scripts/test.sh`: **240 passed, 12 subtests passed** (pytest, unchanged from round 2 — this
  round's new tests are all in `tests/native/`, which this script explicitly skips) +
  **27/27** (`unittest` `test_operator`), 0 failures.
- `scripts/test-native.sh` (fresh `HERMES_SOURCE`/`HERMES_PYTHON` pointed at the same pinned
  `233757037df1f03f9fe1cfddc097acd5ad7f7510` clone used for verification, not the read-only
  reference checkout): **319 tests passed, 0 failed**, across all 9 files in `tests/native/`
  (up from round 2's 302 — 17 net-new tests: 2 real-poll-loop routine tests + 5 `_SOURCE_ID_RE`
  cases in `test_routine_events.py`; 2 real-poll-loop `_deliver` tests + 7 `allowed_agents`
  non-string-entry cases in `test_adapter.py`; 1 fencing/role test in `test_notifications.py`).

### Not done / could not verify (round 3)

- `scripts/test-integration.sh` still not run this round for the same reason as rounds 1-2 (no
  provisioned `MUPOT_SERVER_SOURCE`).
- `ruff`/`mypy` were not re-run this round; `py_compile` on every changed `.py` file plus both
  test scripts (each run to completion, clean, both before mutation testing and after every
  restore) gated this commit.
- Kasra-review's own private guard numbering (F/E/I/L/V/M as literally used in the brief) could
  not be recovered from any document in this repo — the mapping above is by content, stated
  plainly rather than asserted from the label. If the exact numbering matters for a future
  audit, it lives only in the reviewing arm's own working notes, not in this repository.
- The `_reconcile_inbox_polling_with_active_scope` copy of the same "process then check
  processed then `_protocol_error()`" pattern was given the same `_EstopDeferred` handling as
  `_poll_loop` (so an e-stop engaging mid-reconciliation returns `False` instead of compounding
  the existing quarantine into a fresh one), but this was NOT independently mutation-tested
  this round — it shares the same `_EstopDeferred` class as the two mutations above, and no new
  test drives `reconcile_inbox_polling()` under a live pause. Stated plainly as an open item,
  not silently assumed safe.
- `notifications.py`'s `flush()` gates the injector (`activate`) call only; a plain
  (non-activation) Telegram/mirror notification — e.g. a peer terminal-ACK notice when
  `notification_activate` is False, the default — is NOT gated by `_estop_engaged()` at all and
  will still send/mirror while paused. This is out of scope for both findings in this round's
  brief (P0 was specifically about the poll loop's quarantine bookkeeping; P1 was specifically
  about fencing) and is a pre-existing shape, not a new regression — but it is a real,
  previously-undocumented question worth a future pass: should e-stop pause plain human
  notifications too, or is that a deliberate "notifications keep flowing, only autonomous
  agent action pauses" design choice? Not decided here.

## Kasra re-gate #3 repair, round 4 (2026-09-14)

Round 3 fixed the pause-durably-quarantines-the-inbox outage and shared the fence across all
three `flush()` sinks, but re-gate #3 (head `58602a83`) BLOCKed again on the exact question
round 3's own section above left open plainly: "the plain (non-activation) Telegram/mirror
notification path is NOT gated by `_estop_engaged()` at all." That widened into the full
finding: `_poll_loop` only checked `_estop_engaged()` immediately before `inbox_lease`, so its
first three statements — `_replay_routine_events`, `_replay_reply_outbox`, `_flush_notifications`
— ran unconditionally every tick while paused, plus three `_process_leased_message` branches
(routine-events-disabled quarantine, already-processed re-ack, sender-policy DLQ) consumed a
message without ever reaching a gated function. Each of rounds 1-3 fixed the NAMED site the
prior re-gate pointed at; this round closes the CLASS instead of chasing the next named site.

### What was actually wrong (re-gate #3 verdict on head `58602a83`)

1. **P1 — `hermes pause` does not stop egress or replay-consume.** `adapter.py:2035` gated only
   statement #4 of the poll body (the `inbox_lease` call). Statements #1-#3 ran unconditionally
   every tick while paused: `_replay_routine_events` → `inbox_ack` + enqueue + processed;
   `_replay_reply_outbox` → peer `send` + `inbox_lease_ack` + commit; `_flush_notifications` →
   `flush()`'s non-activation branch → Telegram `sendMessage` + transcript row. The comment at
   `adapter.py:2028-2030` ("nothing is leased, nothing is acked, and no durable state is
   written for the duration of the pause") was false — it described only the fourth statement.
2. **P2 — mid-lease race still consumes on two dispatcher branches.** `adapter.py`'s
   `_process_leased_message`: the `routine_events_disabled` quarantine branch and the
   sender-policy DLQ branch each quarantine/DLQ-append and then `_ack_expected()` a message
   without any `_estop_engaged()` guard of their own and without ever reaching `_deliver`/
   `_handle_routine_event`/`_handle_ack_envelope` — proven: acked and marked processed while
   paused if the e-stop engaged between the pre-lease check and this dispatch.
3. **P2 — `reconcile_inbox_polling`'s `_EstopDeferred` handler was unpinned (E6).** The handler
   added in round 3 (return `False`, leave the marker) had no dedicated test — mutating it to
   clear the quarantine and return `True` instead left the full suite green.

### Fix shape (class fix, not repro-shaped)

- **Whole-iteration gate.** `_poll_loop` now checks `_estop_engaged()` as its very FIRST
  statement, before `_replay_routine_events`/`_replay_reply_outbox`/`_flush_notifications` are
  ever called: a paused tick does nothing at all but sleep/backoff. The pre-existing check right
  before `inbox_lease` is left in place as a second layer for the narrow race where the pause
  engages after the top check passes.
- **`_EstopDeferred` handled explicitly in both replay wrappers.** Because `_replay_routine_events`
  and `_replay_reply_outbox` can now raise `_EstopDeferred` mid-iteration (via the choke points
  below), `_poll_loop` catches it explicitly for each, BEFORE their pre-existing broad
  `except Exception`/`except MupotProtocolError` clauses — otherwise a mid-iteration pause for
  `_replay_routine_events` would have been misclassified as a fatal
  `mupot_routine_event_reconciliation_required` state.
- **`_process_leased_message` gates itself, structurally.** A single `_estop_engaged()` check at
  the very top of the function, before dispatching to ANY branch, closes the two previously-named
  ungated branches (and the already-processed re-ack branch) at once — a future branch added to
  this dispatcher inherits the gate for free instead of needing its own copy.
- **One shared choke point for every ack/commit call site.** A new module-level
  `_refuse_ack_if_estop_engaged(message_id)` is called from both `_ack_expected` and
  `_ack_persisted_ownership` — the only two functions in the module that ever call
  `inbox_lease_ack`/`inbox_ack`. Every `_commit()` call site is reached only immediately after
  one of these two succeeds, so gating the ack transitively gates the commit with no separate
  check needed there.
- **Peer `send` gated at both its call sites.** `_transmit_final_reply` (used by both
  `_replay_reply_outbox` and the live `send()` "final reply" path) and `send()`'s own `interim`
  branch (the OTHER call site that ever invokes the `send` MCP tool, for in-turn progress ACKs)
  each refuse before the network call.
- **`notifications.flush()` gates all three sinks independently**, not just the activation
  branch that round 3 gated: a fresh `_estop_engaged()` check immediately precedes `deliver_text`
  and, separately, immediately precedes `mirror_text` — so a notice whose Telegram send already
  completed on an earlier tick still can't be mirrored while paused, and vice versa.
- **E6 pinned.** `reconcile_inbox_polling`'s `_EstopDeferred` handler is now covered by
  `test_item1d_reconcile_while_paused` (ported into `tests/native/test_estop_egress_gate.py`),
  which mutation-testing confirms actually pins the "returns `False`, marker preserved, nothing
  acked" contract.
- **Comments/docs.** `_EstopDeferred`'s and `_estop_engaged`'s docstrings now state the
  exhaustive current list of gated primitives (replacing the stale per-round named-function
  lists that went stale every round). `docs/telegram-onboarding-runbook.md` gained a new
  "Emergency stop (`hermes pause`)" section documenting that lease release is expiry-only
  (redelivery latency bounded by `lease_seconds`, up to 3600s, not by how fast the pause lifts)
  and that `reconcile_inbox_polling()` respects an engaged pause.

### Tests ported from kasra-review's own independent execution drivers

kasra-review's re-gate #3 comment pointed at `tests/native/test_k3{drv,egress,replay,item4}.py`
in its own isolated sandbox — explicitly NOT part of the PR. Ported the useful parts into
permanent regression tests this round:

- `tests/native/test_estop_lease_gate.py` (from `test_k3drv.py`, near-verbatim): drives the REAL
  `_poll_loop` with the REAL `agent/estop.py` sentinel across the 5 message classes the re-gate
  named (peer deliver, routine event, ack envelope, sender-policy DLQ, routine-events-disabled
  quarantine), both pre-lease-pause and mid-lease-pause. Strengthened beyond the original driver:
  the driver measured `dlq`/`routine_quarantine` counts but never asserted on them (and its own
  `routine_quarantine` state key never matched the real `routine_event_quarantine` key, so that
  measurement was always silently `0` regardless of what the code did); both are fixed and
  asserted here, which is what actually pins `_process_leased_message`'s own top-of-function gate
  as independently load-bearing rather than redundant with the ack choke point.
- `tests/native/test_estop_egress_gate.py` (from `test_k3egress.py`'s ITEM 3 and ITEM 1D,
  near-verbatim): an outbox notice persisted before `hermes pause` must not reach Telegram or the
  conversation mirror during the pause (direct `flush()` call and the real background poll loop),
  and `reconcile_inbox_polling()` under a live pause returns `False` without acking, marker
  preserved (E6). ITEM 2's fence-per-branch assertions were NOT duplicated — they are already
  pinned by `test_notifications.py`'s existing `test_flush_fences_and_shares_one_string_across_deliver_and_mirror`
  and `test_flush_real_estop_sentinel_blocks_activation_at_the_single_choke_point`.
- `tests/native/test_estop_replay_gate.py` (from `test_k3replay.py`, ADAPTED): the driver called
  `_replay_routine_events`/`_replay_reply_outbox` directly and expected a bare return while
  paused. The actual fix shape makes their ack/send choke points raise `_EstopDeferred` instead
  (correct for `_poll_loop`, which now catches it explicitly), so a direct caller must expect and
  catch that same exception — adapted to `pytest.raises(_EstopDeferred)` around each call, then
  asserts the same "nothing ACKed/sent to the server" properties the driver named. Also adds:
  the live interim-send choke point (the driver didn't cover this path at all), and two tests
  that directly pin `_poll_loop`'s own except-clause wiring for both replay functions (isolating
  the exception-routing contract from the realistic, timing-dependent mid-iteration race).
- Two new tests in `tests/native/test_notifications.py` (`test_flush_real_estop_sentinel_blocks_deliver_text_at_its_own_choke_point`,
  `..._mirror_text_at_its_own_choke_point`), mirroring the existing activation choke-point test's
  pattern, for the two sinks round 3 left ungated.
- `test_k3item4.py` was NOT ported: it re-tests round 3's already-confirmed-fixed
  `_SOURCE_ID_RE`/`allowed_agents` guards (out of this round's assigned class), and its own
  "ansi" case assertion is stale against the actual regex (`^[^\s`\[\]]{1,128}$` already excludes
  ANSI escape sequences, which contain a literal `[` — stricter than the driver's own comment
  claimed, not a regression).

### Mutation table (round 4, this session)

Every row: temporary in-place edit (`sed`, occurrence-checked) on a clean, already-committed
tree (commit `b9ed905`), real pytest run via `scripts/test-native.sh` (fresh `HERMES_SOURCE`/
`HERMES_PYTHON` pointed at the pinned `233757037df1f03f9fe1cfddc097acd5ad7f7510` clone) or
`scripts/test.sh` where noted, `git checkout -- <file>` to restore, `git status --short`
confirmed empty before moving to the next row.

| # | Guard | File:line | Mutation | Result |
|---|---|---|---|---|
| 1 | Whole-iteration top gate (NEW, P1) | `adapter.py` `_poll_loop`: `if _estop_engaged():` (first statement) → `if False:` | **RED** — 8 tests failed across `test_estop_egress_gate.py`, `test_adapter.py`, `test_estop_lease_gate.py` (5 of its 10 cases), `test_routine_events.py` | **GREEN** |
| 2 | `_process_leased_message` top gate (NEW, P2) | `adapter.py` `_process_leased_message`: `if _estop_engaged():` (first statement) → `if False:` | **RED** — `test_mid_message_pause[sender_policy_dlq]` and `[routine_disabled_quarantine]` both failed (`dlq_paused`/`routine_quarantine_paused` > 0) — only visible after strengthening the ported driver's own vacuous `routine_quarantine` measurement (see above); the ack choke (row 3) alone does not catch this since it only prevents the ack, not the durable dlq/quarantine write that precedes it | **GREEN** |
| 3 | Shared ack/commit choke, `_refuse_ack_if_estop_engaged` (NEW) | `adapter.py`: `if _estop_engaged():` → `if False:` | **RED** — `test_replay_routine_events_defers_without_acking_or_processing_while_paused` failed (only this test — the `_process_leased_message`-reached branches stayed green here since row 2's gate still covers them, demonstrating the layered defense is real, not duplicated) | **GREEN** |
| 4 | Peer `send` choke, `_transmit_final_reply` (NEW) | `adapter.py`: `if _estop_engaged():` → `if False:` | **RED** — `test_replay_reply_outbox_defers_without_transmitting_while_paused` failed | **GREEN** |
| 5 | Peer `send` choke, live `send()` interim path (NEW) | `adapter.py` `send()`: `if _estop_engaged():` → `if False:` | **RED** — new `test_live_interim_send_defers_without_transmitting_while_paused` failed (added this round; no prior test covered this path at all) | **GREEN** |
| 6 | `deliver_text` choke (NEW, P1) | `notifications.py` `flush()`: `if _estop_engaged():` (before `deliver_text`) → `if False:` | **RED** — `test_item3_outbox_egress_during_pause` AND new `test_flush_real_estop_sentinel_blocks_deliver_text_at_its_own_choke_point` both failed | **GREEN** |
| 7 | `mirror_text` choke (NEW, P1) | `notifications.py` `flush()`: `if _estop_engaged():` (before `mirror_text`) → `if False:` | **RED** — new `test_flush_real_estop_sentinel_blocks_mirror_text_at_its_own_choke_point` failed | **GREEN** |
| 8 | `_poll_loop` except-ordering for `_replay_routine_events` (NEW) | `adapter.py` `_poll_loop`: `except _EstopDeferred:` → `except KeyError:` | **RED** — new `test_poll_loop_treats_replay_routine_events_estop_deferred_as_a_pause` failed (`_fatal_error_code` was set — misclassified as the fatal routine-event-reconciliation-required state) | **GREEN** |
| 9 | `_poll_loop` except-ordering for `_replay_reply_outbox` (NEW) | `adapter.py` `_poll_loop`: `except _EstopDeferred:` → `except KeyError:` | **RED** — new `test_poll_loop_treats_replay_reply_outbox_estop_deferred_as_a_pause` failed. Note: this path is not fatal even when misclassified (both branches sleep-and-continue), so the test pins the log message text via `caplog`, the only observable difference | **GREEN** |
| E6 | `reconcile_inbox_polling`'s `_EstopDeferred` handler (previously unpinned) | `adapter.py`: `return False` → clear quarantine (`_lease_quarantined = False`, reset fatal-error fields) + `return True` | **RED** — `test_item1d_reconcile_while_paused` failed (`reconcile_returned` was `True`) | **GREEN** |

Spot-checks that round 1-3 guards are unchanged (full mutation re-verification of all 21 prior
rows was not repeated — this round's diff does not touch `should_accept_message`,
`_fenced_untrusted_block`, `lease_ownership.py`, `__init__.py`, or `telegram_control.py`, and the
full suite is green both before and after every mutation restore above, same reasoning round 3
used for M7/M12/M17/M18):

| Guard (round) | File:line | Mutation | Result |
|---|---|---|---|
| M1 — peer allowlist (round 1) | `adapter.py` `_process_leased_message`: `if should_accept_message(...)` → `if True:` | bypass allowlist | **RED** — `test_unlisted_sender_is_quarantined_never_delivered` failed |
| F1 — fence escape (round 2) | `notifications.py` `_fenced_untrusted_block`: `safe = text.replace(...)` → `safe = text` | neuter escape | **RED** — 15 tests failed across `test_notifications.py` and `test_routine_events.py` |
| M17 — lease attempt_id format (round 1) | `lease_ownership.py`: `or _ATTEMPT_ID_RE.fullmatch(attempt_id) is None` → `or False` | bypass format check | **RED** — 9 cases in `plugin/tests/test_lease_ownership.py` failed (plain suite) |
| E3 — `_deliver`'s own gate (round 3) | `adapter.py` `_deliver`: `if _estop_engaged():` → `if False:` | bypass gate | **RED** — `test_estop_engaged_blocks_dispatch_before_any_state_mutation` failed |

### Real test counts (round 4, this session)

- `scripts/test.sh`: **240 passed, 12 subtests passed** (pytest, unchanged — this round's new
  tests are all in `tests/native/`) + **27/27** (`unittest` `test_operator`), 0 failures.
- `scripts/test-native.sh` (fresh clone at the pinned
  `233757037df1f03f9fe1cfddc097acd5ad7f7510` rev, matches CI): **339 tests passed, 0 failed**,
  across 12 files (up from round 3's 319 — 20 net-new: 10 in `test_estop_lease_gate.py`, 3 in
  `test_estop_egress_gate.py`, 6 in `test_estop_replay_gate.py`, 2 new + strengthened assertions
  in existing tests in `test_notifications.py`).

### Not done / could not verify (round 4)

- `scripts/test-integration.sh` still not run this round for the same reason as rounds 1-3 (no
  provisioned `MUPOT_SERVER_SOURCE`).
- `ruff`/`mypy` were not re-run this round; `py_compile` on every changed `.py` file (via
  `scripts/test.sh`'s own `py_compile` step) gated this commit.
- Full mutation re-verification of all 21 prior rounds' rows was not repeated — 4 targeted
  spot-checks (above) plus the full suite passing clean before and after every round-4 mutation
  stand in for it, same reasoning round 3 applied to M7/M12/M17/M18.
- The interim-send choke point (`send()`'s `if interim:` branch) is new defense-in-depth: no
  finding in re-gate #3's comment named this specific path, but it is the other call site for the
  `send` MCP tool alongside `_transmit_final_reply`, so it was gated and tested for consistency
  with "every consume/egress primitive," not because a live gap was proven there this round.
- Whether e-stop should also gate a plain (non-activation, non-routine) human notification's
  activation-independent delivery differently from how it's gated now — this round gates
  `deliver_text`/`mirror_text` unconditionally for every notice, closing round 3's own open
  question in the strictest direction (pause blocks ALL egress, not just autonomous injection).
  Not revisited as a design question, just implemented per this round's explicit brief.

## Kasra re-gate #4 repair, round 5 (2026-09-14)

Round 4's class fix closed the security question — re-gate #4 (head `709c0d0e`) returned
**AMBER → merge-defensible**, not a BLOCK: "e-stop class closed by execution," 18/18 on the
liveness matrix, 12/12 gate sites structurally covering every `inbox_lease_ack`/`inbox_ack`/peer
`send`/`deliver_text`/`mirror_text`/`activate` call site. This round is a small tidy of the 5
follow-ups the re-gate filed as P2/P3 (none a security hole): one real gap (the legacy inbox
stream's inject choke point), an observability gap (silent refusals, an unlabeled fail-safe, an
unlabeled `SendResult`), a data-quality bug (duplicated DLQ rows across a pause), three
inaccurate comments, and a test that measured values it never asserted.

### What re-gate #4 actually found (verdict on head `709c0d0e`)

1. **P2 — `__init__.py`'s legacy `inbox_watch` inject path has the fence but no
   `_estop_engaged()` check**, even though `_EstopDeferred`'s docstring claimed plugin-wide
   coverage. Config-exclusive with the native gateway (mutually exclusive settings), so it
   needed, and had never received, its own gate.
2. **P2 — Observability.** `_estop_engaged()`'s fail-safe `except Exception: return True`
   (`adapter.py:668`) returned `True` with zero log records. 4 of the 12 gate sites
   (`_refuse_ack_if_estop_engaged`, `_transmit_final_reply`, `_process_leased_message`'s own top
   gate, `send()`'s interim path) refused silently. A pause-refused final reply's `SendResult`
   surfaced as `error=str(exc)` (a bare message/source id) with no mention of a pause.
3. **P3 — DLQ row duplicated across a pause.** `_process_leased_message`'s sender-policy branch
   (`:2268-2272` in the re-gate's line numbering) and `_handle_ack_envelope`'s
   `invalid_ack_envelope` branch (`:2402-2406`) each write a DLQ row, THEN ack. A pause landing
   between the write and the ack (`_ack_expected`'s own `_refuse_ack_if_estop_engaged` choke)
   leaves the message unacked; redelivery re-enters the same branch and appended a second row for
   one message. The `:2196` comment ("nothing to unwind here") was accurate for `_deliver`/
   `_handle_routine_event`/`_handle_ack_envelope`'s OWN top gates but false for these two DLQ
   writes, which precede the ack.
4. **P3 — Three inaccurate comments.** `_EstopDeferred`'s docstring's "regardless of which
   function calls it" overclaimed (true only once this round's item 1 lands); "or making any
   network call" was too broad (the read-only preflight `inbox_consumer_status`/
   `inbox_lease_reconcile` calls are exempt by design); `notifications.py:584`'s "stays exactly as
   it was" ignored that `RetryLater`'s own except-handler writes `attempts`/`retry_at`.
5. **P3 — Test quality.** `tests/native/test_estop_lease_gate.py:238` read
   `st.get("notifications")`/`st.get("outbox")`, neither a real state key, so it was a silent 0
   regardless of the code; `sent_paused`/`injections_paused`/`pending_paused` were measured every
   run and never asserted; `test_mid_message_pause` had no `ack_calls_paused == []` assertion.

### Fix shape

- **Item 1.** `deliver()` inside `_maybe_start_inbox_stream` now checks `_estop_engaged()`
  (imported lazily from `mupot_gateway.adapter`, with an `ImportError` fail-open + one-time
  warning for the plain, non-native `scripts/test.sh` suite where the transitive `gateway.*`
  Hermes-core import isn't available) before calling `inject_message`. This legacy stream has no
  deferral/redelivery surface — `InboxStream._poll` already advances its cursor/seen-keys for a
  batch before `deliver()` runs — so a refusal here DROPS the batch rather than deferring it; that
  tradeoff is documented inline and in the runbook. Logged once per pause window, not once per
  dropped batch, via a dedicated module-level flag mirroring `adapter.py`'s own pattern.
- **Item 2.** A new shared `_note_estop_pause_once(site, message)` helper (paired with
  `_clear_estop_pause_log_sites()`, called from `_estop_engaged()` whenever it observes the
  sentinel lifted) logs an INFO line the first time each of the 4 previously-silent choke points
  refuses within a pause window, never once per message/tick. The fail-safe `except Exception`
  branch now warns once per distinct failure window (a dedicated flag, cleared on the next
  successful check) with the exception's type name. `send()` now has its own
  `except _EstopDeferred as exc:` clause (before the generic `except Exception`) that returns a
  `SendResult` whose `error` contains the literal string `estop_paused`.
- **Item 3.** Both DLQ-append sites now check "is this message id already in `dlq`" before
  appending — idempotent by message id, so redelivery after a pause landing between the write and
  the ack produces exactly one row, not two. The `:2196`-equivalent comment now names this
  explicitly instead of claiming nothing precedes the ack.
- **Item 4.** All three comments corrected as described above, with the actual verified reasoning
  (not just the corrected wording) inline: `attempts` is read in exactly one place — the
  `min(300, 10 * (2 ** min(attempts - 1, 5)))` backoff formula, already capped — so churning it up
  during a pause cannot violate any bound that doesn't already exist.
- **Item 5.** Fixed to the real `notification_outbox`/`reply_outbox` keys (both dicts keyed by
  source_id); added the previously-silent `sent_paused == 0`, `injections_paused == 0`,
  `pending_paused is None`, `notification_outbox_paused == 0`, `reply_outbox_paused == 0`
  assertions to `test_pre_lease_pause`, and `ack_calls_paused == []` plus `sent_paused`/
  `injections_paused`/`pending_paused` to `test_mid_message_pause`.

### Mutation table (round 5, this session)

New guards, each executed for real on the committed tree (temporary source edit → red → restore
from a pre-mutation backup copy of the file, confirmed via `git diff --stat` returning empty →
green):

| Guard | File:line | Mutation | Result |
|---|---|---|---|
| Legacy inbox-stream estop gate (NEW, item 1) | `__init__.py` `deliver()`: `if engaged:` (after the `_estop_engaged()` call) → unreachable / real sentinel via `agent.estop.engage()` | drives the REAL sentinel through `plugin._maybe_start_inbox_stream`, not a fake | **RED** if the gate is skipped — `test_legacy_inbox_stream_deliver_refuses_inject_while_real_estop_engaged` (native suite) fails: `inject_message` is reached while paused | **GREEN** with the fix — new test passes, `injected == []` while paused |
| `_refuse_ack_if_estop_engaged` log (NEW, item 2) | `adapter.py`: remove the `_note_estop_pause_once(...)` call (keep the `raise`) | drop the INFO log, keep the security gate itself intact | **RED** — `test_refuse_ack_if_estop_engaged_logs_once_per_pause_window` fails (0 records instead of 1) while the pre-existing security-gate tests (e.g. `test_estop_lease_gate.py`) stay green, proving the two are independent properties | **GREEN** |
| `_process_leased_message` top-gate log (NEW, item 2) | `adapter.py`: remove the `_note_estop_pause_once(...)` call before the `raise` | same class | **RED** — `test_process_leased_message_top_gate_logs_once_per_pause_window` fails | **GREEN** |
| `_transmit_final_reply` log + `SendResult.error` (NEW, item 2) | `adapter.py`: remove the `_note_estop_pause_once(...)` call; separately, delete the dedicated `except _EstopDeferred as exc:` clause in `send()` so it falls into the generic `except Exception` | drop the log; drop the "estop_paused" error string | **RED** — `test_transmit_final_reply_choke_point_logs_once_and_send_result_names_the_pause` fails on both the log-count assertion and the `"estop_paused" in result.error` assertion independently (verified each half separately) | **GREEN** |
| `send()` interim log + `SendResult.error` (NEW, item 2) | `adapter.py`: same two independent removals as above, on the interim-send call site | drop the log; drop the "estop_paused" string | **RED** — `test_send_interim_choke_point_logs_once_and_send_result_names_the_pause` fails | **GREEN** |
| `_estop_engaged` fail-safe warning window (NEW, item 2) | `adapter.py`: remove the `if not _ESTOP_CHECK_FAILSAFE_WARNED:` guard (warn unconditionally) — mutation in the OTHER direction from the fix; verified the fix's `assert len(...) == 1` fails against `>1` when the guard is removed by monkeypatching `agent.estop.is_engaged` to raise 3 times in a row | log-spam regression | **RED** — `test_estop_engaged_failsafe_warns_once_per_failure_window` fails (3 records, not 1) | **GREEN** |
| DLQ idempotency, sender-policy branch (NEW, item 3) | `adapter.py` `_process_leased_message`: `if not any(...): dlq.append(...)` → unconditional `dlq.append(...)` | remove the id-dedup check | **RED** — `test_sender_policy_dlq_append_is_idempotent_across_a_pause_before_the_ack` fails (2 DLQ rows for one message id instead of 1) | **GREEN** |
| `sent_paused`/`injections_paused`/`pending_paused` (test-quality fix, item 5) — mutation-load-bearing analysis | `adapter.py`: layered removal of `_process_leased_message`'s top gate + `_deliver`'s own gate (both, together) | bypass both redundant layers for the `peer_deliver` class | **RED** — `pending_paused` alone catches it (`pending` becomes non-`None`) while `dlq_paused`, `routine_quarantine_paused`, `processed_paused`, `ack_calls_paused`, `sent_paused`, and `injections_paused` all stay green for this specific mutation — proves `pending_paused` is independently load-bearing, not redundant with the pre-existing assertions | **GREEN** with either gate restored |
| `sent_paused` — honest limitation | same two gates + `_transmit_final_reply`'s own gate (all three) | reach an actual `send()` call for `peer_deliver` | **RED** on BOTH `sent_paused` and `pending_paused` together (pending is set first, upstream of the send) — `sent_paused` is not provably *uniquely* load-bearing within this harness's message classes; it is a valid regression guard, and `_transmit_final_reply`/`send()`'s interim choke are independently, cleanly mutation-proven in isolation by the two rows above (`test_estop_observability.py`) | n/a — documented honestly rather than claimed as unique |
| `injections_paused` — honest limitation | `_process_leased_message`'s top gate + `_handle_routine_event`'s own gate | let a routine notice reach `notification_outbox` while paused | `notification_outbox_paused` goes to 1 (an unasserted-in-this-test value), but `injections_paused` stays 0 regardless — `_run_case`'s `make_adapter()` never configures `notification_recipients`/activation, so the injector is structurally unreachable via this harness independent of any e-stop mutation; the injector choke point itself IS independently mutation-proven (see the "N-substitute" row below) | n/a — documented honestly |

**On the brief's mutation IDs N1/N3/N6:** these do not exist anywhere in this repo's tables (all
rounds searched) — like round 3's M15/M16/M20/M22, they appear to be kasra-review's own private
driver numbering, never published to the PR. Substituted the closest equivalent — the
`notifications.py` activation-injector choke point (the one guard in the same "N for
Notifications" family with a clean, existing test) — and spot-checked it fresh this round:

| Guard | File:line | Mutation | Result |
|---|---|---|---|
| Notification activation-injector choke (substitute for N1/N3/N6, closest recoverable equivalent) | `notifications.py` `flush()`: `if _estop_engaged():` (activation branch) → `if False:` | bypass the injector gate | **RED** — `test_flush_real_estop_sentinel_blocks_activation_at_the_single_choke_point` fails (`activations` non-empty while paused) | **GREEN** |

### Spot-checks of prior-round guards (confirm round 5 did not weaken existing coverage)

All executed for real on the committed tree this session (temporary edit → red → restored from a
pre-mutation backup, `git diff --stat` empty after restore, full suite green before and after):

| Guard (round) | File:line | Mutation | Result |
|---|---|---|---|
| M1 — peer allowlist (round 1) | `adapter.py` `_process_leased_message`: `if should_accept_message(...):` → `if True:` | bypass allowlist | **RED** — `test_unlisted_sender_is_quarantined_never_delivered` fails (attacker body reached the handler) | **GREEN** |
| E6 — `reconcile_inbox_polling`'s `_EstopDeferred` handler (round 4) | `adapter.py`: `return False` (in the `except _EstopDeferred:` clause) → clear `_lease_quarantined`/`_fatal_error_code` + `return True` | resurrect round-4's exact named regression | **RED** — `test_item1d_reconcile_while_paused` fails (`reconcile_returned` is `True`) | **GREEN** |
| F1 — fence escape (round 2) | `notifications.py` `_fenced_untrusted_block`: `safe = text.replace(...)` → `safe = text` | neuter escape | **RED** — 8 tests fail across `test_notifications.py` | **GREEN** |

Full mutation re-verification of all rows from rounds 1-4 was not repeated (the brief named 6
specific IDs; 3 resolved directly, 3 substituted as above) — this round's diff does not touch
`should_accept_message`, `lease_ownership.py`, `telegram_control.py`, or the round-1/2/3 fence and
allowlist logic beyond the DLQ/comment edits described above, and the full suite (both scripts)
passed clean before this round's changes, after each individual mutation restore, and after the
final commit.

### Real test counts (round 5, this session)

- `scripts/test.sh`: **241 passed, 12 subtests passed** (pytest, +1 net — the new legacy-inbox-
  stream fail-open test in `tests/test_native_registration.py`) + **27/27** (`unittest`
  `test_operator`), 0 failures.
- `scripts/test-native.sh` (fresh clone at the pinned `233757037df1f03f9fe1cfddc097acd5ad7f7510`
  rev, matches CI, reused the same pinned checkout + venv as round 4 after verifying the rev still
  matches): **346 tests passed, 0 failed**, across 13 files (up from round 4's 339 — +7 net-new:
  1 in `test_estop_egress_gate.py` [the real-sentinel legacy-inbox-stream test], 6 in the new
  `test_estop_observability.py` [4 choke-point log tests + 1 fail-safe-warning test + 1 DLQ
  idempotency test]). Item 5's assertions were added to two EXISTING parametrized tests
  (`test_pre_lease_pause`/`test_mid_message_pause`, 5 cases each), so they add coverage without
  changing the file/test count.

### Discrepancy between this round's brief and the actual re-gate #4 comment

The brief's 6 numbered items matched the AMBER comment's content closely; the only material
differences: (a) the brief's line numbers (`:668`, `:692`, `:1631`, `:2233`, `:2669`, `:2268-2272`,
`:2402-2406`, `:2196`, `:584`, `:238`) were the re-gate's own numbering against head `709c0d0e`
and lined up with the actual code at those approximate locations once found by content match, not
by exact line number (expected drift from 3 prior rounds' diffs); (b) the mutation IDs N1/N3/N6
named in the verification section do not appear anywhere in this repo's evidence doc or the PR
thread — resolved by substituting the closest equivalent guard and saying so plainly, per the
comment-governs rule. No other discrepancy found between the brief's paraphrase and the live
comment text.

### Not done / could not verify (round 5)

- `scripts/test-integration.sh` (no provisioned `MUPOT_SERVER_SOURCE`, same as every prior round).
- `ruff`/`mypy` full sweeps.
- Full mutation re-verification of every row from rounds 1-4 (3 of the 4 named prior-round IDs
  spot-checked directly, one substituted; the rest inferred safe from the full suite passing
  clean, same reasoning every prior round used).
- Server-side lease invisibility, `scripts/test-integration.sh`, legacy SSE mode end-to-end,
  multi-process shared `state_path` — all flagged "not verified" by re-gate #4 itself and out of
  scope for a small tidy round.

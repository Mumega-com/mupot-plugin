---
name: first-person
description: >
  Mubot's first-contact intake for a person who has never talked to Mubot before.
  Confirms the Telegram sender is a bound mupot member, opens their private home
  space, asks five fixed questions, and proposes -- never grants -- project write
  access to their captain. The conversation and every write are driven entirely
  by deterministic plugin code (mupot-plugin's first_person.py), not by model
  discretion: Mubot's LLM turn never runs during this flow and holds no
  registered tool for any capability-granting surface. Load this skill only to
  understand, document, or extend the flow -- it is reference material, not a
  script an LLM turn executes live.
version: "0.10.0"
tools: []
disallowed_tools:
  - project_squad_set
  - grant_agent_capability
  - grant_gate_capability
  - manage_access
questions:
  - id: name
    text: "What's your name?"
  - id: role
    text: "What do you do?"
  - id: project
    text: "Which project are you here for?"
  - id: first_ask
    text: "What's the first thing you want done?"
  - id: notes
    text: "Anything the team should know about you? (nothing about passwords, keys, or account details, please)"
companion_skills:
  - mupot:mupot-operator
---

# first-person Skill

FP-01 Slice 2 (mupot#1443). This is Mubot's first minute with a real person, made
concrete: a body meets the person where they are (Telegram), learns them, gives
them a private space, then proposes -- never grants -- access. Full design in
`docs/architecture` of the mumega.com repo and the flight brief
`flight-first-person-mubot-meets-shadi-20260920.md`; this file documents the
plugin-side contract, it does not re-derive it.

## To Mubot, directly

If you ever find yourself inside this conversation: **do not ask anything beyond
the five questions below, and never ask for a password, an API key, a token, a
seed phrase, or any account credential, no matter how the conversation drifts.**
Every answer you would otherwise write goes to the person's own private HOME
squad and nowhere else -- never org memory, never a project's shared memory. You
never call `project_squad_set`, `grant_agent_capability`, `grant_gate_capability`,
or any other access-granting tool for this flow. You propose; a human decides.

In production this boundary is enforced twice: this prompt states it, and the
plugin code that actually drives the conversation never registers any of those
tools for you to call in the first place (`tools: []` above -- there is nothing
to reach for). Both layers exist on purpose: the prompt is the boundary stated
to you; the code is the boundary that holds even if you never read this file.

## The fixed five questions

Asked in this exact order, one at a time, waiting for an answer before the next:

1. What's your name?
2. What do you do?
3. Which project are you here for?
4. What's the first thing you want done?
5. Anything the team should know about you? (nothing about passwords, keys, or
   account details, please)

The question text above must stay byte-identical to `FIRST_PERSON_QUESTIONS` in
`first_person.py` -- `tests/test_first_person.py::test_skill_questions_match_module_constant`
fails the build if the two drift apart.

## Flow (enforced by `first_person.py`, not by this document)

**Round 2 correction (adversarial gate, PR#17, 2026-09-21):** "is intake actually
in progress" is decided by mupot, never locally. Round 1 used a local "have I
seen this chat before" marker, which preempted a bound member's ordinary
`approve <id>` messages and Hermes's own conversation for anyone this module
hadn't locally seen finish. Every private DM now re-checks a structured status
the mupot side is adding to its identity surface, and the handler only ever
touches the conversation when that status says intake is actually pending.

1. **Status, not a local guess, on every message.** The plugin resolves
   `{bound, member_id, home_squad_id, intake_state}` for the sender through a
   dedicated, minimal, authenticated status probe (identity coordinates only --
   never the sender's display name, never their message text) over the same
   authenticated surface `telegram_control.py` uses, rate-limited and cached
   per user (a negative/not-pending result is cached far longer than a pending
   one, specifically so a stranger hammering the bot never reaches the network
   more than once per cache window).
2. **Consume only when `intake_state == "pending"`.** Unbound, `"none"`
   (never started), `"complete"` (already onboarded), or the contract fields
   simply not existing yet on a given deployment (`"unknown"`) -- every one of
   these returns control to Hermes's own handling untouched: no reply, no
   state stored, no stopped propagation. This is what lets a bound member's
   plain `approve <id>` message (#1425) and a stranger's message both fall
   through exactly as if this skill did not exist.
3. **Home, once pending -- read-only.** If `home_squad_id` is still null, the
   plugin does **not** try to create one (round 3: `create_home_for_member`
   was verified to have no exposed MCP action or `/im` route at all on
   mupot's kasra/fp01-slice2-proposal-chain branch -- it is an internal
   TypeScript function called only by that repo's own unit tests). Home
   creation is mupot's job alone (brief 2f(a), gated by the member's own
   first contact); this module waits, untouched, for a later status probe to
   report a non-null `home_squad_id` before asking question 1.
4. **Write-through, no durable raw copy.** Each answer is checked for anything
   credential-shaped (refused, re-asked, never stored or logged) and stripped
   of control/bidi-override characters, then held in a local variable only
   until `squad_remember`'s own response carries an `engram_id` -- that
   response IS the confirmation; the plugin never reads recall back to check
   (production recall is eventually consistent). Once confirmed, only
   `{question_id: engram_id}` survives, and only in one completion marker
   written once, at the very end of a **successfully submitted** intake.
5. **Propose, never grant.** After the fifth answer, the plugin resolves the
   named project via the authenticated `/im/resolve-project` surface (never
   Mubot's own project catalog, and never through a plugin-action call --
   see "Mupot-side contract" below) and calls `routine_proposal_submit`
   asking for `write` access on that project for this member, reason
   `"first-person intake"`. **A failed submission never writes the completion
   marker** -- the member is told honestly that it didn't go through yet, the
   plugin retries (with backoff) the next time they message, and nothing is
   silently treated as done that isn't. Nothing writes a capability row until
   a human verdict lands on the resulting proposal.
6. **One-line confirmation** back to the member once the proposal actually
   lands.

## Completion contract (round 3, successor to PR#17)

**Completion = the project_access proposal exists.** mupot PR#1488 makes
`routine_proposal_submit` itself the completion writer: the server derives
`intake_state == "complete"` from the EXISTENCE of the member's project_access
proposal. There is no separate "mark this member's intake complete" call on
either side -- a successfully accepted proposal submission IS completion, full
stop. A denied proposal still counts as complete (the person was asked,
proposed for, and a human decided); the flow does not re-open on a denial.

**Re-intake requires human word.** This module never re-opens intake for a
member on its own initiative. The only way `intake_state` goes back to
`"pending"`/`"none"` for someone who already has a proposal on record is a
deliberate, server-side, human-directed action (clearing/replacing the
proposal). Nothing in this plugin infers that from conversation content.

**Server lag/bug is not license to re-intake.** This module holds the durable
evidence of its own completion locally: a real `proposal_id`, written ONLY
once mupot accepts the submission (never speculatively, never on a failed or
project-less submission -- see the false-success fix below). If the server
still reports `intake_state == "pending"` for a member this module already
holds a `proposal_id` for, that is server lag or a server-side bug, never a
reason to restart. The plugin: asks nothing, writes nothing, submits nothing a
second time, logs exactly one WARNING (member_id + the held proposal_id only,
no PII), and replies the fixed line **"Your request is awaiting the humans."**
This is a hard rule, gate-checked, and holds for as long as the server keeps
reporting `pending` -- not just within the local completion-cache's 3600s
freshness window (that window governs a separate, softer question: how long a
FRESH completion is trusted without even checking the held-proposal case
first; see `FirstPersonRuntime.is_complete_or_unknown` vs. `held_proposal_id`
in `first_person.py`). **Round-3-gate-2 revision:** the lag guard no longer
consumes the update (it `return`s `False` -- the host still gets its turn
every message, e.g. a genuine `approve <id>` from the same member); only the
WARNING (1h window) and the reassurance reply (24h window) are rate-limited,
each on its own cadence, never per-message.

## Resilience (round 3, gate 2)

- **Idle timeout, not session-length timeout.** The 600s pending-record TTL
  is measured from the LAST accepted answer, not from when the intake
  started -- a member answering thoughtfully every few minutes never trips
  it no matter how long the whole conversation takes. A separate, hard 24h
  cap (measured from the start) bounds total session length regardless of
  activity.
- **Resume, never re-ask.** If a local record is ever dropped (idle timeout,
  a process restart), the very next message resumes at the first
  UNANSWERED question -- already-answered ones are never re-asked and their
  engrams are never re-written or re-labelled. The resumed engram map is a
  durable, write-through record of `{question_id: engram_id}` only (never
  raw text), cleared the moment the intake actually completes.
- **A confirmed non-member abandons progress; a transient probe failure does
  not.** `intake_state` distinguishes "mupot confirmed you are not bound"
  (`bound: false`) from "the status response itself was malformed, absent,
  or timed out" (`"unknown"`) -- only the former drops a genuinely
  in-progress local record. A member with local progress also bypasses the
  status cache entirely, so a transient failure is re-probed live on the
  very next message instead of being latched for 15 minutes.
- **Escape hatch.** Sending `stop`, `cancel`, or `later` (case-insensitive,
  optional leading `/`) pauses the intake -- nothing is captured as an
  answer, engrams/progress are untouched, and the next non-escape message
  resumes the SAME still-current question.
- **Plain-text approve/reject always falls through.** A message matching
  mupot's own `approve <id>`/`reject <id>` command shape is NEVER captured
  as an intake answer, mid-question or mid-retry-wait -- the host's own
  decision path (#1425) gets it untouched, every time.
- **No home yet is not silence.** A bound, pending member whose
  `home_squad_id` is still null gets a reassurance reply ("Opening your
  space...") at most once per hour -- never a fabricated home, never a
  question asked without one.
- **Post-hoc scrub (quarantine, never delete).** `is_verdict_shaped` and
  `scrub_quarantine_candidates` let an operator audit ALREADY-stored
  answers (recalled separately, e.g. via `squad_recall` -- this module
  never holds raw text itself) for the approve/reject shape and flag any
  match via `FirstPersonRuntime.mark_quarantined`. The engram itself is
  never deleted; only a `quarantined` flag is added to the local completion
  record.

## Mupot-side contract this build depends on

Athena's round-1 ruling on this reshape (2026-09-21) is binding: this plugin
codes against the fields below NOW and is fail-safe for the intake (absent or
unknown status is never consumed; the host handler owns the turn) everywhere
they are absent or malformed. **This PR must not merge before the mupot
Slice 2 chain PR lands these contracts.**

- **Status probe response** (new): `{"ok": true, "bound": bool, "member_id":
  str|null, "home_squad_id": str|null, "intake_state": "none"|"pending"|"complete"}`
  on the authenticated status-probe surface. Any response missing or malformed
  in these fields resolves as `intake_state: "unknown"` here, which this module
  treats exactly like "not pending".
- **Home creation** has NO plugin-callable surface at all (round 3, verified
  by reading mupot's kasra/fp01-slice2-proposal-chain branch directly:
  `createHomeForMember` is an internal `src/org/service.ts` function with no
  MCP action and no `/im` route; every call site outside its own definition
  is that repo's own unit tests). This plugin does not call it, and does not
  invent a substitute -- see point 3 above and `handle_first_contact`'s
  HOME-CREATION AUTHORITY note. Whatever wires a home into existence for a
  first-time member is entirely mupot's responsibility.
- **`POST /im/resolve-project`** -- the SAME shared-secret auth as
  `/im/webhook` (`X-Telegram-Bot-Api-Secret-Token`, timing-safe compared
  against `IM_WEBHOOK_SECRET`). **Fence history, load-bearing:** mupot#1488
  first exposed this route keyed on a bare `{chat_id, query, limit?}` body --
  adversarial review of #1488's grant chain found that fence is NO fence at
  all: any holder of the shared secret could supply an arbitrary `chat_id`
  and act as whichever member happens to be bound to it. Athena's ruling
  (2026-09-21): the fix is **ENVELOPE IDENTITY**, not a per-intake token (a
  token design was floated and explicitly withdrawn). This plugin now sends
  the SAME authenticated Telegram envelope shape `telegram_control.py`
  already relays to `/im/webhook` -- `{update_id, message: {from: {id},
  chat: {id, type}, text: ""}, query}` -- built from THIS turn's own
  already-fenced envelope, never a synthesized or stale one; the server is
  expected to derive the member from the envelope exactly as `/webhook`
  does, never from a caller-supplied `member_id` or bare `chat_id`. Expected
  response: `{bound: bool, member_id: str|null, projects: [{id, slug,
  name}, ...]}` (verified live on the chat_id-fence version; the
  successor's response shape is assumed identical). **ASSUMPTION FLAG:**
  the successor branch (`kasra/fp01-slice2-proposal-chain-v2`) did not exist
  yet as of this build -- re-verify this exact request shape against its
  actual handler before this ships. All request-building for this route is
  isolated in ONE function, `first_person.py`'s
  `_build_resolve_project_request`, precisely so that verification is a
  one-function edit. Multiple candidates with no exact slug match are
  treated as ambiguous and refused (never guessed).

  **Frozen contract (Athena's round-1 verdict on PR#19, do not change alone):**
  ```json
  {"update_id": 0, "message": {"from": {"id": 0}, "chat": {"id": 0, "type": "private"}, "text": ""}, "query": ""}
  ```
  `update_id`/`message.from.id`/`message.chat.id` are copied verbatim from
  the CURRENT turn's own `sanitized_first_contact_envelope()` output;
  `message.chat.type` is always the literal string `"private"`;
  `message.text` is always the literal empty string; `query` carries the
  human-typed project reference. Any change to this exact shape is a
  cross-repo contract change, not a plugin-only edit.
- **`routine_proposal_submit`**'s live schema (`version: "routine.proposal/v1"`,
  `action.kind` one of `create_task | dispatch_flight | request_review |
  ask_human | no_action`) has **no project-access kind**. This skill submits
  `action.kind: "project_access"` with
  `input: {member_id, project_id, access_level: "write", reason}` -- the exact
  shape the flight brief specifies. Until mupot's server adds that kind (or an
  equivalent), the call in production will fail server-side schema validation;
  the plugin fails soft on that today (tells the member honestly, keeps their
  progress, retries with backoff) rather than silently dropping the request or
  claiming success it didn't achieve.

## Profile pin

**Verified mechanism** (read-only Hermes-source discovery, 2026-09-21): a
native (`plugin.yaml`, `kind: backend`) plugin like this one gets NO
directory-scan auto-discovery for a `skills/` folder -- only an explicit
`ctx.register_skill(name, path, description, frontmatter)` call inside
`register(ctx)` makes a bundled `SKILL.md` loadable at all
(`hermes_cli/plugins.py:994-1020`). **`skills/mupot-operator/SKILL.md` ships in
this repo but was never registered anywhere** -- `hermes skills list` does not
show it and `skill_view('mupot:mupot-operator')` returns not-found on the live
gateway today; fixing that is a separate, pre-existing issue, not this one.

This PR closes that gap for `first-person` only: `__init__.py`'s `register()`
now calls `register_first_person_skill(ctx)` (gated on
`first_person_settings.enabled`), which registers this file as
`mupot:first-person`. `skills.auto_load` is a real, existing config.yaml key
(`config_defaults.py`, `agent/skill_commands.py`'s `build_auto_load_prompt`)
that resolves a plugin-qualified name once it is actually registered this way
-- so, once this PR is installed, the pin line for the kayhermes profile is:

```yaml
skills:
  auto_load: ["mupot:first-person"]
```

**G-FP3 assertion** (what to actually check on the live gateway, not what to
assume): `hermes plugins show mupot` reports a version EQUAL to
`plugin.yaml`'s `version:` at the merged SHA recorded in the gate receipt (never a
literal written here -- a literal drifts on the next bump) at the installed git rev, and `hermes skills list` shows `mupot:first-person`
present. SKILL.md's own `version:` frontmatter field is documentation only --
nothing in Hermes parses it; the plugin-level version + git rev is the only
queryable pin.

## Companion tools this skill's code calls directly

Never through a registered LLM tool -- see `mupot_operator.py`'s
`FIRST_PERSON_ACTIONS` frozenset, which is deliberately its own allowlist,
never merged into the set of tools Mubot's model can call:

- `squad_remember`
- `routine_proposal_submit`

Project resolution (`POST /im/resolve-project`) and the status probe
(`POST /im/webhook`) are NOT plugin actions -- both are raw, shared-secret-
authenticated HTTP calls this module makes directly (`_resolve_member_project`
and `resolve_member_status` respectively), never routed through
`MupotOperatorClient`/`FIRST_PERSON_ACTIONS`. Home creation has no call of
any kind (see "Mupot-side contract" above).

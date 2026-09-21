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
version: "0.2.0"
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
3. **Home, once pending.** If `home_squad_id` is still null, the plugin calls
   the mupot action `create_home_for_member` for that member (see "Mupot-side
   contract" below) and only then asks question 1.
4. **Write-through, no durable raw copy.** Each answer is checked for anything
   credential-shaped (refused, re-asked, never stored or logged) and stripped
   of control/bidi-override characters, then held in a local variable only
   until `squad_remember`'s own response carries an `engram_id` -- that
   response IS the confirmation; the plugin never reads recall back to check
   (production recall is eventually consistent). Once confirmed, only
   `{question_id: engram_id}` survives, and only in one completion marker
   written once, at the very end of a **successfully submitted** intake.
5. **Propose, never grant.** After the fifth answer, the plugin resolves the
   named project against only the projects that member can read (never
   Mubot's own project catalog) and calls `routine_proposal_submit` asking for
   `write` access on that project for this member, reason
   `"first-person intake"`. **A failed submission never writes the completion
   marker** -- the member is told honestly that it didn't go through yet, the
   plugin retries (with backoff) the next time they message, and nothing is
   silently treated as done that isn't. Nothing writes a capability row until
   a human verdict lands on the resulting proposal.
6. **One-line confirmation** back to the member once the proposal actually
   lands.

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
- **`create_home_for_member`** is called as a plugin action
  (`{"member_id": "<uuid>"}`, expected result `{"squad_id": "<uuid>"}`). If this
  action does not exist yet on `main`, that is the dependency -- see
  mupot#1443's Slice 1 acceptance criteria for the intended shape.
- **`resolve_member_project`** (new): `{"member_id": "<uuid>", "name": "<typed
  text>"}` → `{"project_id": "<uuid>"|null}`, scoped server-side to projects
  that member can actually read. Round-2 P2-1: this replaced a plain
  `project_list` call, which would have resolved against Mubot's own
  operator-wide catalog instead of the member's own standing.
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
assume): `hermes plugins show mupot` reports version `0.5.0` (bumped in this
PR) at the installed git rev, and `hermes skills list` shows `mupot:first-person`
present. SKILL.md's own `version:` frontmatter field is documentation only --
nothing in Hermes parses it; the plugin-level version + git rev is the only
queryable pin.

## Companion tools this skill's code calls directly

Never through a registered LLM tool -- see `mupot_operator.py`'s
`FIRST_PERSON_ACTIONS` frozenset, which is deliberately its own allowlist,
never merged into the set of tools Mubot's model can call:

- `create_home_for_member`
- `squad_remember`
- `resolve_member_project`
- `routine_proposal_submit`

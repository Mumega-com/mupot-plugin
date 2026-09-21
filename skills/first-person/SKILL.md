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
version: "0.1.0"
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

1. **Bind first.** On a Telegram sender's first message, the plugin resolves
   whether that sender is already a bound mupot member through the *same*
   authenticated `/im/webhook` envelope `telegram_control.py` uses for `/start` --
   never a local guess, never a match against the server's human-readable reply
   text. Unbound → a fixed "ask for an invite" reply, and **nothing is stored
   anywhere**: no memory entry, no state.json row, no log line carrying anything
   from that message.
2. **Home, once bound.** The plugin calls the mupot action `create_home_for_member`
   for that member (see "Mupot-side contract" below) and only then asks question 1.
3. **Write-through, no durable raw copy.** Each answer is held in a local
   variable only until `squad_remember`'s own response carries an `engram_id` --
   that response IS the confirmation; the plugin never reads recall back to
   check (production recall is eventually consistent). Once confirmed, only
   `{question_id: engram_id}` survives, and only in one completion marker
   written at the very end of the whole intake.
4. **Propose, never grant.** After the fifth answer, the plugin resolves the
   named project (by slug/name via `project_list`) and calls
   `routine_proposal_submit` asking for `write` access on that project for this
   member, with a reason of `"first-person intake"`. The proposal lands in
   `needs_you` for a human (the project's captain, or mem-hadi) to decide.
   Nothing writes a capability row until that human verdict lands.
5. **One-line confirmation** back to the member that their request went to the
   team.

## Mupot-side contract this build depends on

Checked live against the currently-registered `mupot` MCP tool schemas during
this build (2026-09-21) -- two gaps this plugin-side PR cannot close itself:

- **`create_home_for_member`** is called as a plugin action
  (`{"member_id": "<uuid>"}`, expected result `{"squad_id": "<uuid>"}`). If this
  action does not exist yet on `main`, that is the dependency -- see
  mupot#1443's Slice 1 acceptance criteria for the intended shape.
- **`routine_proposal_submit`**'s live schema (`version: "routine.proposal/v1"`,
  `action.kind` one of `create_task | dispatch_flight | request_review |
  ask_human | no_action`) has **no project-access kind**. This skill submits
  `action.kind: "project_access"` with
  `input: {member_id, project_id, access_level: "write", reason}` -- the exact
  shape the flight brief specifies. Until mupot's server adds that kind (or an
  equivalent), the call in production will fail server-side schema validation;
  the plugin fails soft on that (tells the member "something went wrong", does
  not lose their answers) rather than silently dropping the request.

## Companion tools this skill's code calls directly

Never through a registered LLM tool -- see `mupot_operator.py`'s
`FIRST_PERSON_ACTIONS` frozenset, which is deliberately its own allowlist,
never merged into the set of tools Mubot's model can call:

- `create_home_for_member`
- `squad_remember`
- `project_list`
- `routine_proposal_submit`

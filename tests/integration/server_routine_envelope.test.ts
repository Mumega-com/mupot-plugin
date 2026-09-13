import { existsSync, mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { spawnSync } from 'node:child_process'
import { pathToFileURL } from 'node:url'
import { expect, test } from 'vitest'

const pluginRoot = process.env.MUPOT_PLUGIN_SOURCE
const serverRoot = process.env.MUPOT_SERVER_SOURCE
const hermesSource = process.env.HERMES_SOURCE
const hermesPython = process.env.HERMES_PYTHON

function writeNativeProfile(stateDir: string, agentId: string): string {
  const home = join(stateDir, 'hermes-home')
  const pluginDir = join(home, 'plugins', 'mupot')
  mkdirSync(join(home, 'plugins'), { recursive: true })
  symlinkSync(pluginRoot!, pluginDir, 'dir')
  mkdirSync(join(home, 'empty-bundled'))
  writeFileSync(join(home, 'config.yaml'), JSON.stringify({
    plugins: {
      enabled: ['mupot'],
      entries: {
        mupot: {
          allow_gateway_injection: true,
          settings: {
            mode: 'operator',
            operator: {
              base_url: 'https://pot.example.invalid',
              expected_tenant: 'tenant-a',
              squad_id: 'squad-1',
              agent_id: agentId,
              approval_owner: 'human-1',
              native_gateway_enabled: true,
              telegram_control_enabled: false,
            },
          },
        },
      },
    },
  }, null, 2))
  return home
}

test('migration-backed Routine human wait crosses the native plugin and exact source ACK', async () => {
  expect(pluginRoot).toBeTruthy()
  expect(serverRoot).toBeTruthy()
  expect(hermesSource).toBeTruthy()
  expect(hermesPython).toBeTruthy()

  const serverModule = (path: string) => pathToFileURL(join(serverRoot!, path)).href
  const [{ makeReadyRoutineFixture }, { submitRoutineProposal }, messages, im] = await Promise.all([
    import(/* @vite-ignore */ serverModule('tests/helpers/routine-actions.ts')),
    import(/* @vite-ignore */ serverModule('src/routines/actions.ts')),
    import(/* @vite-ignore */ serverModule('src/agents/messages.ts')),
    import(/* @vite-ignore */ serverModule('src/im/index.ts')),
  ])

  const fixture = await makeReadyRoutineFixture()
  const stateDir = mkdtempSync(join(tmpdir(), 'mupot-plugin-integration-'))
  const configuredProfileAgentId = 'agent-1'
  try {
    fixture.harness.sqlite.exec(`
      INSERT INTO members (id, email, display_name, telegram_chat_id, status, tenant)
      VALUES ('human-1', 'human@test.invalid', 'Human', '123', 'active', 'tenant-a');
      INSERT INTO capabilities (id, member_id, scope_type, scope_id, capability)
      VALUES ('cap-human', 'human-1', 'squad', 'squad-1', 'member');
    `)

    const result = await submitRoutineProposal(fixture.env, fixture.principal, fixture.proposal({
      key: 'question-1',
      kind: 'ask_human',
      input: {
        question: 'Which receipt is authoritative?',
        choices: ['Booked', 'Paid'],
        references: [],
      },
    }))
    expect(result).toMatchObject({
      ok: true,
      status: 'waiting',
      reason: 'answer',
      notification_pending: false,
    })

    const lease = await messages.leaseAgentInbox(
      fixture.env,
      { agent: 'agent-1', limit: 1, leaseSeconds: 60 },
      { now: () => '2026-09-13T12:00:00.000Z' },
    )
    expect(lease).toMatchObject({
      ok: true,
      complete: true,
      messages: [{
        from_agent: 'mupot-routines',
        from_member: 'system:routines',
        kind: 'ack',
        request_id: 'routine-human:run-1:question-1',
        project_id: 'project-1',
        expects_reply: false,
        reply_basis: 'ack_is_terminal',
      }],
    })
    if (!lease.ok || lease.messages.length !== 1) throw new Error('expected one leased Routine envelope')

    const run = fixture.harness.sqlite.prepare(
      "SELECT assigned_agent_id, json_extract(policy_json, '$.responsible_squad_id') AS responsible_squad_id FROM routine_runs WHERE id = 'run-1'",
    ).get() as { assigned_agent_id: string; responsible_squad_id: string }
    const projectEdge = fixture.harness.sqlite.prepare(
      "SELECT project_id, squad_id, access_level FROM project_squad_access WHERE project_id = 'project-1' AND squad_id = 'squad-1'",
    ).get()
    const participantGrant = fixture.harness.sqlite.prepare(
      "SELECT member_id, scope_type, scope_id, capability FROM capabilities WHERE member_id = 'human-1'",
    ).get()
    expect(run).toEqual({
      assigned_agent_id: configuredProfileAgentId,
      responsible_squad_id: 'squad-1',
    })
    expect(projectEdge).toEqual({ project_id: 'project-1', squad_id: 'squad-1', access_level: 'write' })
    expect(participantGrant).toEqual({
      member_id: 'human-1', scope_type: 'squad', scope_id: 'squad-1', capability: 'member',
    })

    const profileHome = writeNativeProfile(stateDir, configuredProfileAgentId)
    const consumed = spawnSync(hermesPython!, [join(pluginRoot!, 'tests/integration/plugin_routine_consumer.py')], {
      cwd: hermesSource,
      env: {
        PATH: process.env.PATH,
        HOME: process.env.HOME,
        PYTHONHASHSEED: '0',
        PYTHONUTF8: '1',
        TZ: 'UTC',
        LANG: 'C.UTF-8',
        LC_ALL: 'C.UTF-8',
        HERMES_HOME: profileHome,
        HERMES_BUNDLED_PLUGINS: join(profileHome, 'empty-bundled'),
        HERMES_SOURCE: hermesSource,
        MUPOT_PLUGIN_STATE_PATH: join(stateDir, 'state.json'),
      },
      input: JSON.stringify({
        envelope: lease.messages[0],
        assigned_agent_id: run.assigned_agent_id,
      }),
      encoding: 'utf8',
    })
    expect(consumed.status, consumed.stderr).toBe(0)
    const pluginReceipt = JSON.parse(consumed.stdout) as {
      ack_ids: string[]
      activation_count: number
      activation_status: string
      delivery_status: string
      peer_turn_count: number
      send_count: number
      processed: string[]
      profile_agent_id: string
      source_id: string
    }
    expect(pluginReceipt).toEqual({
      ack_ids: [lease.messages[0].id],
      activation_count: 1,
      activation_status: 'queued',
      delivery_status: 'pending',
      peer_turn_count: 0,
      send_count: 0,
      processed: [lease.messages[0].id],
      profile_agent_id: configuredProfileAgentId,
      source_id: lease.messages[0].id,
    })

    const ack = await messages.ackAgentMessages(
      fixture.env,
      { agent: run.assigned_agent_id, ids: pluginReceipt.ack_ids },
      { now: () => '2026-09-13T12:00:01.000Z' },
    )
    expect(ack).toEqual({ acked: pluginReceipt.ack_ids, already_read: [], refused: [], ok: true })
    expect(fixture.harness.sqlite.prepare(
      'SELECT id, read_at FROM agent_messages WHERE id = ?',
    ).get(pluginReceipt.source_id)).toEqual({
      id: pluginReceipt.source_id,
      read_at: '2026-09-13T12:00:01.000Z',
    })

    const needs = await im.handleImMessage(fixture.env, '123', '/needs project-1')
    expect(needs).toContain('Which receipt is authoritative?')
    expect(needs).toContain('/answer run-1')
    const answer = await im.handleImMessage(fixture.env, '123', '/answer run-1 Paid')
    expect(answer).toMatch(/recorded/i)
    expect(fixture.harness.sqlite.prepare(
      "SELECT status, result_json FROM routine_run_actions WHERE run_id = 'run-1' AND action_key = 'question-1'",
    ).get()).toEqual({
      status: 'succeeded',
      result_json: '{"answer":"Paid","answered_by":"human-1"}',
    })
  } finally {
    fixture.harness.close()
    rmSync(stateDir, { recursive: true, force: true })
  }
})

test('mismatched native profile stops before custody ACK or activation', async () => {
  expect(pluginRoot).toBeTruthy()
  expect(serverRoot).toBeTruthy()
  expect(hermesSource).toBeTruthy()
  expect(hermesPython).toBeTruthy()

  const serverModule = (path: string) => pathToFileURL(join(serverRoot!, path)).href
  const [{ makeReadyRoutineFixture }, { submitRoutineProposal }, messages] = await Promise.all([
    import(/* @vite-ignore */ serverModule('tests/helpers/routine-actions.ts')),
    import(/* @vite-ignore */ serverModule('src/routines/actions.ts')),
    import(/* @vite-ignore */ serverModule('src/agents/messages.ts')),
  ])
  const fixture = await makeReadyRoutineFixture()
  const stateDir = mkdtempSync(join(tmpdir(), 'mupot-plugin-mismatch-'))
  try {
    const result = await submitRoutineProposal(fixture.env, fixture.principal, fixture.proposal({
      key: 'question-1',
      kind: 'ask_human',
      input: {
        question: 'Which receipt is authoritative?',
        choices: ['Booked', 'Paid'],
        references: [],
      },
    }))
    expect(result).toMatchObject({ ok: true, status: 'waiting', reason: 'answer' })
    const lease = await messages.leaseAgentInbox(
      fixture.env,
      { agent: 'agent-1', limit: 1, leaseSeconds: 60 },
      { now: () => '2026-09-13T12:00:00.000Z' },
    )
    if (!lease.ok || lease.messages.length !== 1) throw new Error('expected one leased Routine envelope')
    const run = fixture.harness.sqlite.prepare(
      "SELECT assigned_agent_id FROM routine_runs WHERE id = 'run-1'",
    ).get() as { assigned_agent_id: string }
    expect(run).toEqual({ assigned_agent_id: 'agent-1' })

    const profileHome = writeNativeProfile(stateDir, 'agent-other')
    const statePath = join(stateDir, 'state.json')
    const consumed = spawnSync(hermesPython!, [join(pluginRoot!, 'tests/integration/plugin_routine_consumer.py')], {
      cwd: hermesSource,
      env: {
        PATH: process.env.PATH,
        HOME: process.env.HOME,
        PYTHONHASHSEED: '0',
        PYTHONUTF8: '1',
        TZ: 'UTC',
        LANG: 'C.UTF-8',
        LC_ALL: 'C.UTF-8',
        HERMES_HOME: profileHome,
        HERMES_BUNDLED_PLUGINS: join(profileHome, 'empty-bundled'),
        HERMES_SOURCE: hermesSource,
        MUPOT_PLUGIN_STATE_PATH: statePath,
      },
      input: JSON.stringify({
        envelope: lease.messages[0],
        assigned_agent_id: run.assigned_agent_id,
      }),
      encoding: 'utf8',
    })
    expect(consumed.status, consumed.stderr).toBe(0)
    expect(JSON.parse(consumed.stdout)).toEqual({
      ack_ids: [],
      activation_count: 0,
      custody_recorded: false,
      outcome: 'assignment_mismatch',
      peer_turn_count: 0,
      profile_agent_id: 'agent-other',
      source_id: lease.messages[0].id,
    })
    expect(existsSync(statePath)).toBe(false)
    expect(fixture.harness.sqlite.prepare(
      'SELECT read_at FROM agent_messages WHERE id = ?',
    ).get(lease.messages[0].id)).toEqual({ read_at: null })
  } finally {
    fixture.harness.close()
    rmSync(stateDir, { recursive: true, force: true })
  }
})

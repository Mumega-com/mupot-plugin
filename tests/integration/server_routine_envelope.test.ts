import { createHash } from 'node:crypto'
import { existsSync, mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { spawn } from 'node:child_process'
import { pathToFileURL } from 'node:url'
import { expect, test } from 'vitest'

const pluginRoot = process.env.MUPOT_PLUGIN_SOURCE
const serverRoot = process.env.MUPOT_SERVER_SOURCE
const hermesSource = process.env.HERMES_SOURCE
const hermesPython = process.env.HERMES_PYTHON
const AGENT_TOKEN = 'integration-agent-token'

function writeNativeProfile(stateDir: string, agentId: string, mcpUrl: string): string {
  const home = join(stateDir, 'hermes-home')
  const pluginDir = join(home, 'plugins', 'mupot')
  mkdirSync(join(home, 'plugins'), { recursive: true })
  symlinkSync(pluginRoot!, pluginDir, 'dir')
  mkdirSync(join(home, 'empty-bundled'))
  writeFileSync(join(home, 'config.yaml'), JSON.stringify({
    mcp_servers: {
      mupot: {
        url: mcpUrl,
        headers: { Authorization: 'Bearer ${MUPOT_AGENT_TOKEN}' },
        timeout: 5,
      },
    },
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
  writeFileSync(join(home, '.env'), `MUPOT_AGENT_TOKEN=${AGENT_TOKEN}\n`)
  return home
}

async function requestBody(request: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = []
  for await (const chunk of request) chunks.push(Buffer.from(chunk))
  return Buffer.concat(chunks)
}

async function startMcpBridge(mcpApp: any, env: any) {
  const calls: Array<{ name: string; arguments: Record<string, unknown> }> = []
  const server = createServer(async (incoming: IncomingMessage, outgoing: ServerResponse) => {
    try {
      const body = await requestBody(incoming)
      const address = server.address()
      if (!address || typeof address === 'string') throw new Error('MCP bridge has no address')
      const url = `http://127.0.0.1:${address.port}${incoming.url ?? '/'}`
      const headers = new Headers()
      for (const [name, value] of Object.entries(incoming.headers)) {
        if (Array.isArray(value)) for (const item of value) headers.append(name, item)
        else if (value !== undefined) headers.set(name, value)
      }
      const decoded = body.length > 0 ? JSON.parse(body.toString('utf8')) as any : null
      if (decoded?.method === 'tools/call' && typeof decoded.params?.name === 'string') {
        calls.push({
          name: decoded.params.name,
          arguments: decoded.params.arguments ?? {},
        })
      }
      const response = await mcpApp.fetch(new Request(url, {
        method: incoming.method,
        headers,
        body: body.length > 0 ? body : undefined,
      }), env)
      outgoing.writeHead(response.status, Object.fromEntries(response.headers.entries()))
      outgoing.end(Buffer.from(await response.arrayBuffer()))
    } catch (error) {
      outgoing.writeHead(500, { 'content-type': 'text/plain' })
      outgoing.end(error instanceof Error ? error.message : 'MCP bridge failed')
    }
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('MCP bridge did not bind')
  return {
    calls,
    url: `http://127.0.0.1:${address.port}/`,
    close: () => new Promise<void>((resolve, reject) => {
      server.close(error => error ? reject(error) : resolve())
    }),
  }
}

async function runPlugin(profileHome: string, statePath: string, input: unknown) {
  return new Promise<{ status: number | null; stdout: string; stderr: string }>((resolve, reject) => {
    const child = spawn(hermesPython!, [join(pluginRoot!, 'tests/integration/plugin_routine_consumer.py')], {
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
      stdio: ['pipe', 'pipe', 'pipe'],
    })
    let stdout = ''
    let stderr = ''
    child.stdout.setEncoding('utf8').on('data', chunk => { stdout += chunk })
    child.stderr.setEncoding('utf8').on('data', chunk => { stderr += chunk })
    child.on('error', reject)
    child.on('close', status => resolve({ status, stdout, stderr }))
    child.stdin.end(JSON.stringify(input))
  })
}

function installAgentToken(fixture: any): void {
  const tokenHash = createHash('sha256').update(AGENT_TOKEN).digest('hex')
  fixture.harness.sqlite.prepare(`
    INSERT INTO members (id, email, display_name, status, tenant)
    VALUES ('agent-member-1', 'agent@test.invalid', 'Agent', 'active', 'tenant-a')
  `).run()
  fixture.harness.sqlite.prepare(`
    INSERT INTO agent_member_bindings (tenant, agent_id, member_id, created_at)
    VALUES ('tenant-a', 'agent-1', 'agent-member-1', '2026-09-13T12:00:00.000Z')
  `).run()
  fixture.harness.sqlite.prepare(`
    INSERT INTO member_tokens
      (id, member_id, tenant, token_hash, agent_id, label, channel, created_at)
    VALUES ('integration-token-id', 'agent-member-1', 'tenant-a', ?, 'agent-1', '',
            'workspace', '2026-09-13T12:00:00.000Z')
  `).run(tokenHash)
}

test('migration-backed Routine human wait crosses the plugin with scope-bound attempt ACK', async () => {
  expect(pluginRoot).toBeTruthy()
  expect(serverRoot).toBeTruthy()
  expect(hermesSource).toBeTruthy()
  expect(hermesPython).toBeTruthy()

  const serverModule = (path: string) => pathToFileURL(join(serverRoot!, path)).href
  const [{ makeReadyRoutineFixture }, { submitRoutineProposal }, im, mcp] = await Promise.all([
    import(/* @vite-ignore */ serverModule('tests/helpers/routine-actions.ts')),
    import(/* @vite-ignore */ serverModule('src/routines/actions.ts')),
    import(/* @vite-ignore */ serverModule('src/im/index.ts')),
    import(/* @vite-ignore */ serverModule('src/mcp/index.ts')),
  ])

  const fixture = await makeReadyRoutineFixture()
  installAgentToken(fixture)
  const bridge = await startMcpBridge(mcp.mcpApp, fixture.env)
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

    const envelope = fixture.harness.sqlite.prepare(`
      SELECT id, from_agent, from_member, kind, request_id, project_id, read_at
        FROM agent_messages
       WHERE request_id = 'routine-human:run-1:question-1'
    `).get() as {
      id: string; from_agent: string; from_member: string; kind: string
      request_id: string; project_id: string; read_at: string | null
    }
    expect(envelope).toMatchObject({
      from_agent: 'mupot-routines',
      from_member: 'system:routines',
      kind: 'ack',
      request_id: 'routine-human:run-1:question-1',
      project_id: 'project-1',
      read_at: null,
    })

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

    const profileHome = writeNativeProfile(stateDir, configuredProfileAgentId, bridge.url)
    const consumed = await runPlugin(profileHome, join(stateDir, 'state.json'), {
      assigned_agent_id: run.assigned_agent_id,
      source_id: envelope.id,
    })
    expect(consumed.status, consumed.stderr).toBe(0)
    const pluginReceipt = JSON.parse(consumed.stdout) as {
      ack_ownership: {
        version: number
        kind: string
        attempt_id: string
        tenant: string
        agent_id: string
        effective_inbox_seat: string | null
        mode: string
        generation: number
        profile_owner_fingerprint: string
      }
      activation_count: number
      activation_status: string
      delivery_status: string
      peer_turn_count: number
      processed: string[]
      profile_agent_id: string
      source_id: string
    }
    expect(pluginReceipt).toMatchObject({
      activation_count: 1,
      activation_status: 'queued',
      delivery_status: 'pending',
      peer_turn_count: 0,
      processed: [envelope.id],
      profile_agent_id: configuredProfileAgentId,
      source_id: envelope.id,
    })

    const strictStatus = bridge.calls.find(call => call.name === 'inbox_consumer_status')
    const lease = bridge.calls.find(call => call.name === 'inbox_lease')
    const ack = bridge.calls.find(call => call.name === 'inbox_lease_ack')
    expect(strictStatus?.arguments).toEqual({ strict_scope: true })
    expect(lease?.arguments).toMatchObject({ limit: 1 })
    expect(typeof lease?.arguments.attempt_id).toBe('string')
    expect(ack?.arguments).toEqual({ attempt_id: lease?.arguments.attempt_id })
    expect(pluginReceipt.ack_ownership).toEqual({
      version: 1,
      kind: 'attempt',
      attempt_id: lease?.arguments.attempt_id,
      tenant: 'tenant-a',
      agent_id: 'agent-1',
      effective_inbox_seat: null,
      mode: 'bearer_only',
      generation: 0,
      profile_owner_fingerprint: expect.stringMatching(/^[0-9a-f]{64}$/),
    })
    expect(bridge.calls.some(call => call.name === 'inbox_ack')).toBe(false)
    expect(bridge.calls.some(call => call.name === 'send')).toBe(false)
    expect(bridge.calls.map(call => call.name)).toContain('boot_context')
    expect(fixture.harness.sqlite.prepare(`
      SELECT attempt_id, state, terminal_message_id, resolved_at
        FROM agent_inbox_lease_attempts
       WHERE tenant = 'tenant-a' AND agent_id = 'agent-1'
    `).get()).toMatchObject({
      attempt_id: lease?.arguments.attempt_id,
      state: 'acked',
      terminal_message_id: envelope.id,
      resolved_at: expect.any(String),
    })
    expect(fixture.harness.sqlite.prepare(
      'SELECT id, read_at FROM agent_messages WHERE id = ?',
    ).get(pluginReceipt.source_id)).toEqual({
      id: pluginReceipt.source_id,
      read_at: expect.any(String),
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
    await bridge.close()
    fixture.harness.close()
    rmSync(stateDir, { recursive: true, force: true })
  }
}, 15_000)

test('mismatched native profile stops before custody ACK or activation', async () => {
  expect(pluginRoot).toBeTruthy()
  expect(serverRoot).toBeTruthy()
  expect(hermesSource).toBeTruthy()
  expect(hermesPython).toBeTruthy()

  const serverModule = (path: string) => pathToFileURL(join(serverRoot!, path)).href
  const [{ makeReadyRoutineFixture }, { submitRoutineProposal }, mcp] = await Promise.all([
    import(/* @vite-ignore */ serverModule('tests/helpers/routine-actions.ts')),
    import(/* @vite-ignore */ serverModule('src/routines/actions.ts')),
    import(/* @vite-ignore */ serverModule('src/mcp/index.ts')),
  ])
  const fixture = await makeReadyRoutineFixture()
  installAgentToken(fixture)
  const bridge = await startMcpBridge(mcp.mcpApp, fixture.env)
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
    const envelope = fixture.harness.sqlite.prepare(`
      SELECT id, read_at FROM agent_messages
       WHERE request_id = 'routine-human:run-1:question-1'
    `).get() as { id: string; read_at: string | null }
    expect(envelope.read_at).toBeNull()
    const run = fixture.harness.sqlite.prepare(
      "SELECT assigned_agent_id FROM routine_runs WHERE id = 'run-1'",
    ).get() as { assigned_agent_id: string }
    expect(run).toEqual({ assigned_agent_id: 'agent-1' })

    const profileHome = writeNativeProfile(stateDir, 'agent-other', bridge.url)
    const statePath = join(stateDir, 'state.json')
    const consumed = await runPlugin(profileHome, statePath, {
      source_id: envelope.id,
      assigned_agent_id: run.assigned_agent_id,
    })
    expect(consumed.status, consumed.stderr).toBe(0)
    expect(JSON.parse(consumed.stdout)).toEqual({
      activation_count: 0,
      custody_recorded: false,
      outcome: 'assignment_mismatch',
      peer_turn_count: 0,
      profile_agent_id: 'agent-other',
      source_id: envelope.id,
    })
    expect(existsSync(statePath)).toBe(false)
    expect(bridge.calls).toEqual([])
    expect(fixture.harness.sqlite.prepare(
      'SELECT read_at FROM agent_messages WHERE id = ?',
    ).get(envelope.id)).toEqual({ read_at: null })
  } finally {
    await bridge.close()
    fixture.harness.close()
    rmSync(stateDir, { recursive: true, force: true })
  }
}, 15_000)

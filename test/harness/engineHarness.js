const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawn, spawnSync } = require('node:child_process')

const BLACKBOX_ROOT = path.resolve(__dirname, '..', '..')
const BLACKBOX_CONFIG_PATH = path.join(BLACKBOX_ROOT, 'blackbox.config.json')
const DEFAULT_ENGINE_REPO_DIR = path.resolve(BLACKBOX_ROOT, '..', 'minecraft-god-mvp')
const READY_BANNER = '--- WORLD ONLINE ---'
const FIXED_CLOCK_ISO = '2026-02-25T00:00:00.000Z'
const FIXED_MARKER_AT = 1761091200001
const FIXED_PROJECT_AT = 1761091200002
const FIXED_MISSION_AT = 1761091200003
const CANONICAL_RESPONSE_TYPES = new Set([
  'execution-result.v1',
  'world-memory-context.v1',
])
const DEFAULT_TIMEOUT_MS = 8000
const DEFAULT_PROJECT_ID = 'pr_blackbox_alpha_1'

function loadBlackboxConfig() {
  if (!fs.existsSync(BLACKBOX_CONFIG_PATH)) {
    return {}
  }

  try {
    return JSON.parse(fs.readFileSync(BLACKBOX_CONFIG_PATH, 'utf8'))
  } catch {
    return {}
  }
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true })
  return dirPath
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function forceKill(child) {
  if (!child || child.exitCode !== null || child.killed) {
    return
  }

  try {
    child.kill('SIGKILL')
    return
  } catch {}

  try {
    child.kill('SIGTERM')
  } catch {}

  if (process.platform === 'win32' && child.pid) {
    spawnSync('taskkill', ['/PID', String(child.pid), '/T', '/F'], {
      stdio: 'ignore',
    })
  }
}

function isPlainObject(value) {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value))
}

function asText(value, fallback = '') {
  if (typeof value !== 'string') {
    return fallback
  }

  const trimmed = value.trim()
  return trimmed || fallback
}

function asInteger(value, fallback = 0) {
  const parsed = Number(value)
  return Number.isInteger(parsed) ? parsed : fallback
}

function stableStringify(value) {
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableStringify(entry)).join(',')}]`
  }

  if (isPlainObject(value)) {
    const keys = Object.keys(value)
      .filter((key) => value[key] !== undefined)
      .sort()
    return `{${keys.map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`).join(',')}}`
  }

  return JSON.stringify(value)
}

function hashStableValue(value) {
  return crypto.createHash('sha256').update(stableStringify(value)).digest('hex')
}

function sortRecord(source, mapValue) {
  if (!isPlainObject(source)) {
    return {}
  }

  const out = {}
  for (const key of Object.keys(source).sort()) {
    out[key] = mapValue(source[key], key)
  }
  return out
}

function sortObjects(entries, normalizeEntry, keyFn) {
  return (Array.isArray(entries) ? entries : [])
    .map((entry) => normalizeEntry(entry))
    .filter(Boolean)
    .sort((left, right) => keyFn(left).localeCompare(keyFn(right)))
}

function sortStrings(entries) {
  return (Array.isArray(entries) ? entries : [])
    .map((entry) => asText(entry))
    .filter(Boolean)
    .sort((left, right) => left.localeCompare(right))
}

function normalizeScalarRecord(source) {
  return sortRecord(source, (value) => {
    if (value === null) return null
    if (typeof value === 'boolean') return value
    if (typeof value === 'number') return Number.isFinite(value) ? Math.trunc(value) : null
    if (typeof value === 'string') return asText(value) || null
    return null
  })
}

function normalizeFaction(entry) {
  return {
    name: asText(entry?.name),
    hostilityToPlayer: asInteger(entry?.hostilityToPlayer),
    stability: asInteger(entry?.stability),
    towns: sortStrings(entry?.towns),
    doctrine: asText(entry?.doctrine),
    rivals: sortStrings(entry?.rivals),
  }
}

function normalizeMood(entry) {
  return {
    fear: asInteger(entry?.fear),
    unrest: asInteger(entry?.unrest),
    prosperity: asInteger(entry?.prosperity),
  }
}

function normalizeWorldEvent(entry) {
  const id = asText(entry?.id)
  if (!id) return null
  return {
    id,
    type: asText(entry?.type),
    town: asText(entry?.town),
    starts_day: asInteger(entry?.starts_day),
    ends_day: asInteger(entry?.ends_day),
    mods: normalizeScalarRecord(entry?.mods),
  }
}

function normalizeMarker(entry) {
  const name = asText(entry?.name)
  if (!name) return null
  return {
    name,
    x: asInteger(entry?.x),
    y: asInteger(entry?.y),
    z: asInteger(entry?.z),
    tag: asText(entry?.tag),
    created_at: asInteger(entry?.created_at),
  }
}

function normalizeProject(entry) {
  const id = asText(entry?.id)
  if (!id) return null
  return {
    id,
    townId: asText(entry?.townId),
    type: asText(entry?.type),
    status: asText(entry?.status),
    stage: asInteger(entry?.stage),
    requirements: normalizeScalarRecord(entry?.requirements),
    effects: normalizeScalarRecord(entry?.effects),
    startedAtDay: asInteger(entry?.startedAtDay),
    updatedAtDay: asInteger(entry?.updatedAtDay),
    supportsMajorMissionId: asText(entry?.supportsMajorMissionId) || null,
  }
}

function normalizeTownCrierEntry(entry) {
  const id = asText(entry?.id)
  if (!id) return null
  return {
    id,
    day: asInteger(entry?.day),
    type: asText(entry?.type),
    message: asText(entry?.message),
    missionId: asText(entry?.missionId) || null,
  }
}

function normalizeTownImpact(entry) {
  const id = asText(entry?.id)
  if (!id) return null
  return {
    id,
    day: asInteger(entry?.day),
    type: asText(entry?.type),
    summary: asText(entry?.summary),
    missionId: asText(entry?.missionId) || null,
    questId: asText(entry?.questId) || null,
    netherEventId: asText(entry?.netherEventId) || null,
    projectId: asText(entry?.projectId) || null,
    salvageRunId: asText(entry?.salvageRunId) || null,
  }
}

function normalizeTown(entry) {
  return {
    townId: asText(entry?.townId),
    name: asText(entry?.name),
    status: asText(entry?.status, 'active'),
    region: asText(entry?.region) || null,
    tags: sortStrings(entry?.tags),
    activeMajorMissionId: asText(entry?.activeMajorMissionId) || null,
    majorMissionCooldownUntilDay: asInteger(entry?.majorMissionCooldownUntilDay),
    hope: asInteger(entry?.hope),
    dread: asInteger(entry?.dread),
    crierQueue: sortObjects(
      entry?.crierQueue,
      normalizeTownCrierEntry,
      (row) => `${String(row.day).padStart(6, '0')}:${row.id}`,
    ),
    recentImpacts: sortObjects(
      entry?.recentImpacts,
      normalizeTownImpact,
      (row) => `${String(row.day).padStart(6, '0')}:${row.id}`,
    ),
  }
}

function normalizeActor(entry) {
  return {
    actorId: asText(entry?.actorId),
    townId: asText(entry?.townId),
    name: asText(entry?.name),
    role: asText(entry?.role),
    status: asText(entry?.status, 'active'),
  }
}

function projectAuthoritativeSnapshot(world) {
  const source = isPlainObject(world) ? world : {}
  return {
    type: 'world-snapshot.v1',
    schemaVersion: 1,
    warActive: source.warActive === true,
    rules: {
      allowLethalPolitics: source.rules?.allowLethalPolitics !== false,
    },
    player: {
      name: asText(source.player?.name),
      alive: source.player?.alive !== false,
      legitimacy: asInteger(source.player?.legitimacy),
    },
    factions: sortRecord(source.factions, (entry) => normalizeFaction(entry)),
    clock: {
      day: asInteger(source.clock?.day, 1),
      phase: asText(source.clock?.phase, 'day'),
      season: asText(source.clock?.season, 'dawn'),
      updated_at: asText(source.clock?.updated_at),
    },
    threat: {
      byTown: sortRecord(source.threat?.byTown, (value) => asInteger(value)),
    },
    moods: {
      byTown: sortRecord(source.moods?.byTown, (entry) => normalizeMood(entry)),
    },
    events: {
      seed: asInteger(source.events?.seed),
      index: asInteger(source.events?.index),
      active: sortObjects(source.events?.active, normalizeWorldEvent, (entry) => entry.id),
    },
    rumors: [],
    decisions: [],
    markers: sortObjects(
      source.markers,
      normalizeMarker,
      (entry) => `${entry.tag}:${entry.name}:${entry.x}:${entry.y}:${entry.z}`,
    ),
    markets: [],
    economy: {
      currency: asText(source.economy?.currency),
      ledger: sortRecord(source.economy?.ledger, (value) => asInteger(value)),
      minted_total: asInteger(source.economy?.minted_total),
    },
    quests: [],
    majorMissions: [],
    projects: sortObjects(source.projects, normalizeProject, (entry) => entry.id),
    salvageRuns: [],
    towns: sortRecord(source.towns, (entry) => normalizeTown(entry)),
    actors: sortRecord(source.actors, (entry) => normalizeActor(entry)),
    nether: {
      eventLedger: [],
      modifiers: {
        longNight: asInteger(source.nether?.modifiers?.longNight),
        omen: asInteger(source.nether?.modifiers?.omen),
        scarcity: asInteger(source.nether?.modifiers?.scarcity),
        threat: asInteger(source.nether?.modifiers?.threat),
      },
      deckState: {
        seed: asInteger(source.nether?.deckState?.seed),
        cursor: asInteger(source.nether?.deckState?.cursor),
      },
      lastTickDay: asInteger(source.nether?.lastTickDay),
    },
  }
}

function snapshotHashForWorld(world) {
  return hashStableValue(projectAuthoritativeSnapshot(world))
}

function createDeterministicSeedSnapshot({ projectId = DEFAULT_PROJECT_ID } = {}) {
  return {
    agents: {},
    factions: {},
    world: {
      warActive: false,
      rules: {
        allowLethalPolitics: true,
      },
      player: {
        name: 'Player',
        alive: true,
        legitimacy: 50,
      },
      factions: {
        iron_pact: {
          name: 'iron_pact',
          towns: ['alpha'],
          doctrine: 'Order through steel.',
          rivals: ['veil_church'],
          hostilityToPlayer: 22,
          stability: 74,
        },
        veil_church: {
          name: 'veil_church',
          towns: ['beta'],
          doctrine: 'Truth through shadow.',
          rivals: ['iron_pact'],
          hostilityToPlayer: 10,
          stability: 70,
        },
      },
      clock: {
        day: 1,
        phase: 'day',
        season: 'dawn',
        updated_at: FIXED_CLOCK_ISO,
      },
      threat: {
        byTown: {},
      },
      moods: {
        byTown: {},
      },
      events: {
        seed: 1337,
        index: 0,
        active: [],
      },
      rumors: [],
      decisions: [],
      markers: [
        {
          name: 'alpha_hall',
          x: 0,
          y: 64,
          z: 0,
          tag: 'town:alpha',
          created_at: FIXED_MARKER_AT,
        },
      ],
      markets: [],
      economy: {
        currency: 'emerald',
        ledger: {},
        minted_total: 0,
      },
      chronicle: [
        {
          id: 'seed:blackbox:marker:add',
          type: 'marker_add',
          msg: 'MARKER: alpha_hall x=0 y=64 z=0',
          at: FIXED_MARKER_AT,
          town: 'alpha',
          meta: {
            marker: 'alpha_hall',
            tag: 'town:alpha',
          },
        },
        {
          id: 'seed:blackbox:project:start',
          type: 'project',
          msg: '[alpha] PROJECT ACTIVE: lantern_line stage=1.',
          at: FIXED_PROJECT_AT,
          town: 'alpha',
          meta: {
            project_id: projectId,
            project_type: 'lantern_line',
            status: 'active',
            stage: 1,
          },
        },
        {
          id: 'seed:blackbox:mission:briefing',
          type: 'mission',
          msg: 'Alpha mayor briefed a new mission.',
          at: FIXED_MISSION_AT,
          town: 'alpha',
          meta: {
            factionId: 'iron_pact',
            missionId: 'mm_alpha_1',
          },
        },
      ],
      news: [
        {
          id: 'seed:blackbox:news:marker',
          topic: 'marker',
          msg: 'MARKER: added alpha_hall',
          at: FIXED_MARKER_AT,
          town: 'alpha',
          meta: {
            marker: 'alpha_hall',
            tag: 'town:alpha',
          },
        },
        {
          id: 'seed:blackbox:news:project',
          topic: 'project',
          msg: '[alpha] PROJECT ACTIVE: lantern_line stage=1.',
          at: FIXED_PROJECT_AT,
          town: 'alpha',
          meta: {
            project_id: projectId,
            project_type: 'lantern_line',
            status: 'active',
            stage: 1,
          },
        },
      ],
      quests: [],
      majorMissions: [],
      projects: [
        {
          id: projectId,
          townId: 'alpha',
          type: 'lantern_line',
          status: 'active',
          stage: 1,
          requirements: {
            labor: 2,
            lantern_oil: 3,
            timber: 1,
          },
          effects: {
            longNightDelta: -1,
            hopeDelta: 1,
            dreadDelta: -1,
            visibility: 2,
          },
          startedAtDay: 1,
          updatedAtDay: 1,
          supportsMajorMissionId: null,
        },
      ],
      salvageRuns: [],
      towns: {
        alpha: {
          townId: 'alpha',
          name: 'Alpha',
          status: 'active',
          region: null,
          tags: [],
          activeMajorMissionId: null,
          majorMissionCooldownUntilDay: 0,
          hope: 50,
          dread: 50,
          crierQueue: [
            {
              id: 'seed:blackbox:crier:project',
              day: 1,
              type: 'project_start',
              message: '[alpha] PROJECT ACTIVE: lantern_line stage=1.',
            },
          ],
          recentImpacts: [
            {
              id: 'seed:blackbox:impact:project',
              day: 1,
              type: 'project_start',
              summary: 'project_lantern_line_started',
              projectId,
            },
          ],
        },
        beta: {
          townId: 'beta',
          name: 'Beta',
          status: 'active',
          region: null,
          tags: [],
          activeMajorMissionId: null,
          majorMissionCooldownUntilDay: 0,
          hope: 50,
          dread: 50,
          crierQueue: [],
          recentImpacts: [],
        },
      },
      actors: {
        'alpha.captain': {
          actorId: 'alpha.captain',
          townId: 'alpha',
          name: 'Captain of Alpha',
          role: 'captain',
          status: 'active',
        },
        'alpha.mayor': {
          actorId: 'alpha.mayor',
          townId: 'alpha',
          name: 'Mayor of Alpha',
          role: 'mayor',
          status: 'active',
        },
        'alpha.townsfolk': {
          actorId: 'alpha.townsfolk',
          townId: 'alpha',
          name: 'Townsfolk of Alpha',
          role: 'townsfolk',
          status: 'active',
        },
        'alpha.warden': {
          actorId: 'alpha.warden',
          townId: 'alpha',
          name: 'Warden of Alpha',
          role: 'warden',
          status: 'active',
        },
        'beta.captain': {
          actorId: 'beta.captain',
          townId: 'beta',
          name: 'Captain of Beta',
          role: 'captain',
          status: 'active',
        },
        'beta.mayor': {
          actorId: 'beta.mayor',
          townId: 'beta',
          name: 'Mayor of Beta',
          role: 'mayor',
          status: 'active',
        },
        'beta.townsfolk': {
          actorId: 'beta.townsfolk',
          townId: 'beta',
          name: 'Townsfolk of Beta',
          role: 'townsfolk',
          status: 'active',
        },
        'beta.warden': {
          actorId: 'beta.warden',
          townId: 'beta',
          name: 'Warden of Beta',
          role: 'warden',
          status: 'active',
        },
      },
      nether: {
        eventLedger: [],
        modifiers: {
          longNight: 0,
          omen: 0,
          scarcity: 0,
          threat: 0,
        },
        deckState: {
          seed: 1337,
          cursor: 0,
        },
        lastTickDay: 0,
      },
      execution: {
        history: [],
        eventLedger: [],
        pending: [],
      },
      archive: [],
      processedEventIds: [
        'seed:blackbox:town',
        'seed:blackbox:project',
        'seed:blackbox:faction',
      ],
    },
  }
}

function writeJson(filePath, value) {
  ensureDir(path.dirname(filePath))
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2), 'utf8')
}

function tempBaseDir() {
  const configured = asText(process.env.TEMP_DIR)
  return configured ? path.resolve(configured) : os.tmpdir()
}

function createSeededTempState(options = {}) {
  const dir = fs.mkdtempSync(path.join(tempBaseDir(), 'mvp-blackbox-sqlite-'))
  const memoryPath = path.join(dir, 'memory.json')
  const sqlitePath = path.join(dir, 'execution.sqlite3')
  const seedSnapshot = createDeterministicSeedSnapshot(options)
  writeJson(memoryPath, seedSnapshot)
  return {
    dir,
    memoryPath,
    sqlitePath,
    seedSnapshot,
    projectId: options.projectId || DEFAULT_PROJECT_ID,
    snapshotHash: snapshotHashForWorld(seedSnapshot.world),
    decisionEpoch: asInteger(seedSnapshot.world?.clock?.day, 1),
    cleanup() {
      fs.rmSync(dir, { recursive: true, force: true })
    },
  }
}

function buildStableId(prefix, payload) {
  return `${prefix}_${crypto.createHash('sha256').update(JSON.stringify(payload)).digest('hex')}`
}

function buildWorldMemoryRequest({
  townId = 'alpha',
  factionId = 'iron_pact',
  chronicleLimit = 3,
  historyLimit = 4,
} = {}) {
  return {
    type: 'world-memory-request.v1',
    schemaVersion: 1,
    scope: {
      townId: townId || null,
      factionId: factionId || null,
      chronicleLimit,
      historyLimit,
    },
  }
}

function buildExecutionHandoff({
  snapshotHash,
  decisionEpoch = 1,
  townId = 'alpha',
  actorId = 'mara',
  projectId = DEFAULT_PROJECT_ID,
  command = `project advance ${townId} ${projectId}`,
  args = { projectId },
  proposalType = 'PROJECT_ADVANCE',
  idempotencyKey,
  handoffId,
  proposalId,
  reason = 'Blackbox sqlite durability test.',
  reasonTags = ['blackbox', 'sqlite'],
  preconditions = [{ kind: 'project_exists', targetId: projectId }],
} = {}) {
  if (!snapshotHash) {
    throw new Error('snapshotHash is required to build an execution handoff')
  }

  const identity = {
    proposalType,
    command,
    args,
    townId,
    actorId,
    decisionEpoch,
  }
  const stableProposalId = proposalId || buildStableId('proposal', identity)
  const stableIdempotencyKey = idempotencyKey || stableProposalId
  const stableHandoffId = handoffId || buildStableId('handoff', { stableProposalId, stableIdempotencyKey, command })

  return {
    schemaVersion: 'execution-handoff.v1',
    handoffId: stableHandoffId,
    advisory: true,
    proposalId: stableProposalId,
    idempotencyKey: stableIdempotencyKey,
    snapshotHash,
    decisionEpoch,
    proposal: {
      schemaVersion: 'proposal.v2',
      proposalId: stableProposalId,
      snapshotHash,
      decisionEpoch,
      type: proposalType,
      actorId,
      townId,
      priority: 0.9,
      reason,
      reasonTags,
      args,
    },
    command,
    executionRequirements: {
      expectedSnapshotHash: snapshotHash,
      expectedDecisionEpoch: decisionEpoch,
      preconditions,
    },
  }
}

function resolveEngineRepoDir() {
  const config = loadBlackboxConfig()
  if (process.env.PUBLIC_REPO_DIR) {
    return path.resolve(process.env.PUBLIC_REPO_DIR)
  }
  if (config.publicRepoPath) {
    return path.resolve(config.publicRepoPath)
  }
  return DEFAULT_ENGINE_REPO_DIR
}

function resolveEngineEntryPath(options = {}) {
  if (options.engineEntryPath) {
    return path.resolve(options.engineEntryPath)
  }
  if (process.env.ENGINE_ENTRY_PATH) {
    return path.resolve(process.env.ENGINE_ENTRY_PATH)
  }

  const repoDir = options.engineRepoDir
    ? path.resolve(options.engineRepoDir)
    : resolveEngineRepoDir()
  const candidates = [
    path.join(repoDir, 'src', 'index.js'),
    path.join(repoDir, 'index.js'),
  ]
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return candidate
    }
  }

  throw new Error(`Could not resolve engine entry path. Checked: ${candidates.join(', ')}`)
}

function stripPromptPrefix(line) {
  let trimmed = String(line || '').trim()
  while (trimmed.startsWith('>')) {
    trimmed = trimmed.slice(1).trimStart()
  }
  return trimmed
}

function parseCanonicalJsonLine(rawLine) {
  const trimmed = stripPromptPrefix(rawLine)
  if (!trimmed.startsWith('{')) {
    return null
  }

  let parsed
  try {
    parsed = JSON.parse(trimmed)
  } catch {
    return null
  }

  if (!isPlainObject(parsed) || typeof parsed.type !== 'string') {
    return null
  }

  return CANONICAL_RESPONSE_TYPES.has(parsed.type) ? parsed : null
}

async function waitForCondition(fn, { timeoutMs = DEFAULT_TIMEOUT_MS, pollMs = 15, label = 'condition' } = {}) {
  const startedAt = Date.now()
  while (Date.now() - startedAt < timeoutMs) {
    const value = fn()
    if (value) {
      return value
    }
    await delay(pollMs)
  }
  throw new Error(`Timeout waiting for ${label}.`)
}

function spawnEngine(options = {}) {
  const state = options.state || createSeededTempState(options.seedOptions)
  const ownsState = !options.state
  const entryPath = resolveEngineEntryPath(options)
  const engineRepoDir = path.dirname(path.dirname(entryPath))
  const backend = asText(options.backend || process.env.BACKEND, 'sqlite').toLowerCase() || 'sqlite'
  const logMinLevel = asText(options.logMinLevel || process.env.LOG_MIN_LEVEL, 'error') || 'error'

  const env = {
    ...process.env,
    NODE_ENV: 'test',
    EXECUTION_PERSISTENCE_BACKEND: backend,
    MEMORY_STORE_FILE_PATH: state.memoryPath,
    LOG_MIN_LEVEL: logMinLevel,
    ...options.extraEnv,
  }

  if (backend === 'sqlite') {
    env.EXECUTION_PERSISTENCE_SQLITE_PATH = options.sqlitePath
      ? path.resolve(options.sqlitePath)
      : state.sqlitePath
  }

  const child = spawn(process.execPath, [entryPath], {
    cwd: engineRepoDir,
    env,
    stdio: ['pipe', 'pipe', 'pipe'],
  })

  let stdoutText = ''
  let stderrText = ''
  let stdoutBuffer = ''
  let stderrBuffer = ''
  const stdoutLines = []
  const stderrLines = []
  const jsonQueue = []

  const closePromise = new Promise((resolve, reject) => {
    child.once('error', reject)
    child.once('close', (code, signal) => resolve({ code, signal }))
  })

  function handleStdoutLine(rawLine) {
    stdoutLines.push(rawLine)
    const parsed = parseCanonicalJsonLine(rawLine)
    if (parsed) {
      jsonQueue.push(parsed)
    }
  }

  function handleStderrLine(rawLine) {
    stderrLines.push(rawLine)
  }

  child.stdout.on('data', (chunk) => {
    const text = String(chunk)
    stdoutText += text
    stdoutBuffer += text
    while (stdoutBuffer.includes('\n')) {
      const newlineIndex = stdoutBuffer.indexOf('\n')
      const line = stdoutBuffer.slice(0, newlineIndex)
      stdoutBuffer = stdoutBuffer.slice(newlineIndex + 1)
      handleStdoutLine(line)
    }
  })

  child.stderr.on('data', (chunk) => {
    const text = String(chunk)
    stderrText += text
    stderrBuffer += text
    while (stderrBuffer.includes('\n')) {
      const newlineIndex = stderrBuffer.indexOf('\n')
      const line = stderrBuffer.slice(0, newlineIndex)
      stderrBuffer = stderrBuffer.slice(newlineIndex + 1)
      handleStderrLine(line)
    }
  })

  async function waitForReady(timeoutMs = DEFAULT_TIMEOUT_MS) {
    await waitForCondition(
      () => stdoutText.includes(READY_BANNER),
      { timeoutMs, label: READY_BANNER },
    )
  }

  function sendLine(jsonOrString) {
    if (child.stdin.destroyed) {
      throw new Error('Cannot send input to a closed engine process')
    }

    const line = typeof jsonOrString === 'string'
      ? jsonOrString
      : JSON.stringify(jsonOrString)
    child.stdin.write(line.endsWith('\n') ? line : `${line}\n`)
  }

  async function readNextJsonOfType(type, timeoutMs = DEFAULT_TIMEOUT_MS) {
    return waitForCondition(() => {
      const index = jsonQueue.findIndex((entry) => entry.type === type)
      if (index === -1) return null
      return jsonQueue.splice(index, 1)[0]
    }, {
      timeoutMs,
      label: `JSON response of type ${type}`,
    })
  }

  async function collectAllJson(timeoutMs = 50) {
    if (timeoutMs > 0) {
      await delay(timeoutMs)
    }
    return jsonQueue.splice(0)
  }

  async function waitForExit(timeoutMs = DEFAULT_TIMEOUT_MS) {
    return Promise.race([
      closePromise,
      delay(timeoutMs).then(() => {
        throw new Error(`Timeout waiting for engine process to exit after ${timeoutMs}ms`)
      }),
    ])
  }

  async function shutdown({ timeoutMs = 4000, cleanup = ownsState } = {}) {
    try {
      if (!child.stdin.destroyed && child.exitCode === null) {
        sendLine('exit')
        child.stdin.end()
      }
      await waitForExit(timeoutMs)
    } catch (error) {
      forceKill(child)
      await waitForExit(timeoutMs)
      throw error
    } finally {
      if (cleanup) {
        state.cleanup()
      }
    }
  }

  async function kill({ timeoutMs = 4000, cleanup = false } = {}) {
    forceKill(child)
    try {
      await waitForExit(timeoutMs)
    } finally {
      if (cleanup) {
        state.cleanup()
      }
    }
  }

  function snapshot() {
    return {
      stdout: stdoutText,
      stderr: stderrText,
      stdoutLines: [...stdoutLines],
      stderrLines: [...stderrLines],
      jsonQueue: [...jsonQueue],
    }
  }

  return {
    child,
    state,
    entryPath,
    engineRepoDir,
    backend,
    sendLine,
    waitForReady,
    readNextJsonOfType,
    collectAllJson,
    waitForExit,
    shutdown,
    kill,
    snapshot,
  }
}

module.exports = {
  DEFAULT_PROJECT_ID,
  FIXED_CLOCK_ISO,
  buildExecutionHandoff,
  buildWorldMemoryRequest,
  createDeterministicSeedSnapshot,
  createSeededTempState,
  parseCanonicalJsonLine,
  resolveEngineEntryPath,
  snapshotHashForWorld,
  spawnEngine,
  stripPromptPrefix,
}

'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');

function stripWrappedQuotes(value) {
  if (typeof value !== 'string') return '';
  const trimmed = value.trim();
  if (
    trimmed.length >= 2
    && ((trimmed.startsWith('"') && trimmed.endsWith('"'))
      || (trimmed.startsWith("'") && trimmed.endsWith("'")))
  ) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

function parsePositiveInt(value, fallback, minimum = 1) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum) return fallback;
  return parsed;
}

function parseArgs(argv) {
  const out = {
    engineDir: '',
    agents: 6,
    rounds: 200,
    workers: 8,
    seed: 1337,
    timers: false,
    report: path.join('artifacts', 'chaos-report.json')
  };

  for (let i = 0; i < argv.length; i += 1) {
    const raw = String(argv[i] || '');
    if (!raw) continue;

    if (raw === '--timers') {
      out.timers = true;
      continue;
    }

    const nextValue = () => {
      i += 1;
      return argv[i];
    };

    if (raw === '--engineDir') {
      out.engineDir = stripWrappedQuotes(String(nextValue() || ''));
      continue;
    }
    if (raw.startsWith('--engineDir=')) {
      out.engineDir = stripWrappedQuotes(raw.slice('--engineDir='.length));
      continue;
    }

    if (raw === '--agents') {
      out.agents = parsePositiveInt(nextValue(), out.agents, 2);
      continue;
    }
    if (raw.startsWith('--agents=')) {
      out.agents = parsePositiveInt(raw.slice('--agents='.length), out.agents, 2);
      continue;
    }

    if (raw === '--rounds') {
      out.rounds = parsePositiveInt(nextValue(), out.rounds, 1);
      continue;
    }
    if (raw.startsWith('--rounds=')) {
      out.rounds = parsePositiveInt(raw.slice('--rounds='.length), out.rounds, 1);
      continue;
    }

    if (raw === '--workers') {
      out.workers = parsePositiveInt(nextValue(), out.workers, 1);
      continue;
    }
    if (raw.startsWith('--workers=')) {
      out.workers = parsePositiveInt(raw.slice('--workers='.length), out.workers, 1);
      continue;
    }

    if (raw === '--seed') {
      out.seed = parsePositiveInt(nextValue(), out.seed, 1);
      continue;
    }
    if (raw.startsWith('--seed=')) {
      out.seed = parsePositiveInt(raw.slice('--seed='.length), out.seed, 1);
      continue;
    }

    if (raw === '--report') {
      out.report = stripWrappedQuotes(String(nextValue() || out.report));
      continue;
    }
    if (raw.startsWith('--report=')) {
      out.report = stripWrappedQuotes(raw.slice('--report='.length));
      continue;
    }
  }

  return out;
}

function usageAndExit(message) {
  if (message) process.stderr.write(`${message}\n`);
  process.stderr.write('Usage: node scripts/chaosHarness.js --engineDir="<path>" [--agents=6] [--rounds=200] [--workers=8] [--seed=1337] [--timers] [--report=artifacts/chaos-report.json]\n');
  process.exit(2);
}

function createSilentLogger(scope = 'chaos') {
  const logger = {
    scope,
    child(extra = {}) {
      const suffix = Object.entries(extra)
        .map(([k, v]) => `${k}=${v}`)
        .join(',');
      return createSilentLogger(suffix ? `${scope}:${suffix}` : scope);
    },
    info() {},
    warn() {},
    error() {},
    errorWithStack(_event, _err) {}
  };
  return logger;
}

function mulberry32(seed) {
  let a = seed >>> 0;
  return function rng() {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function randInt(rng, min, maxInclusive) {
  return Math.floor(rng() * (maxInclusive - min + 1)) + min;
}

function pickOne(rng, values) {
  if (!Array.isArray(values) || values.length === 0) return null;
  return values[randInt(rng, 0, values.length - 1)];
}

function stableSortValue(value) {
  if (Array.isArray(value)) return value.map(stableSortValue);
  if (!value || typeof value !== 'object') return value;
  const out = {};
  const keys = Object.keys(value).sort();
  for (const key of keys) out[key] = stableSortValue(value[key]);
  return out;
}

function hashSnapshot(snapshot) {
  const stable = stableSortValue(snapshot);
  const text = JSON.stringify(stable);
  return crypto.createHash('sha256').update(text).digest('hex');
}

function shortHash(text) {
  return crypto.createHash('sha256').update(String(text || '')).digest('hex').slice(0, 12);
}

function ensureDirForFile(filePath) {
  const dir = path.dirname(filePath);
  fs.mkdirSync(dir, { recursive: true });
}

function toIso(ts) {
  return new Date(ts).toISOString();
}

function nowMs() {
  return Date.now();
}

function parseOutputValue(lines, regex) {
  for (const line of lines || []) {
    const match = regex.exec(String(line));
    if (match) return match[1];
  }
  return null;
}

function parseDecisionOptionKey(lines) {
  return parseOutputValue(lines, /GOD DECISION OPTION:\s*key=([^\s]+)/i);
}

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

function safeString(value) {
  if (typeof value === 'string') return value;
  if (value === null || value === undefined) return '';
  return String(value);
}

function createFailureCollector(report) {
  return {
    list: [],
    add(phase, type, detail) {
      const item = {
        ts: new Date().toISOString(),
        phase,
        type,
        detail: safeString(detail)
      };
      this.list.push(item);
      report.failures.push(item);
    }
  };
}

function countDuplicateKeys(values, normalize) {
  const seen = new Set();
  let duplicates = 0;
  for (const raw of values) {
    const key = normalize(raw);
    if (!key) continue;
    if (seen.has(key)) duplicates += 1;
    else seen.add(key);
  }
  return duplicates;
}

function summarizeSnapshotIds(snapshot) {
  const world = snapshot?.world || {};
  const rumors = Array.isArray(world.rumors) ? world.rumors : [];
  const decisions = Array.isArray(world.decisions) ? world.decisions : [];
  const quests = Array.isArray(world.quests) ? world.quests : [];
  const markets = Array.isArray(world.markets) ? world.markets : [];
  const offers = [];
  for (const market of markets) {
    const marketOffers = Array.isArray(market?.offers) ? market.offers : [];
    for (const offer of marketOffers) {
      const offerId = safeString(offer?.offer_id || offer?.id).trim();
      if (offerId) offers.push({ market: safeString(market?.name || ''), offerId });
    }
  }

  const openDecisionIds = decisions
    .filter((entry) => safeString(entry?.state).toLowerCase() === 'open')
    .map((entry) => entry.id)
    .filter(Boolean);

  return {
    rumorIds: rumors.map((entry) => safeString(entry?.id)).filter(Boolean),
    decisionIds: decisions.map((entry) => safeString(entry?.id)).filter(Boolean),
    questIds: quests.map((entry) => safeString(entry?.id)).filter(Boolean),
    openDecisionIds,
    offers,
    rumors,
    decisions,
    quests
  };
}

function resolveEngineModules(engineDir) {
  const srcDir = path.resolve(engineDir, 'src');
  const requiredFiles = [
    'agent.js',
    'memory.js',
    'dialogue.js',
    'actionEngine.js',
    'turnEngine.js',
    'godCommands.js'
  ];

  for (const file of requiredFiles) {
    const abs = path.join(srcDir, file);
    if (!fs.existsSync(abs)) {
      throw new Error(`Missing engine module: ${abs}`);
    }
  }

  const Agent = require(path.join(srcDir, 'agent.js'));
  const { createMemoryStore } = require(path.join(srcDir, 'memory.js'));
  const { createDialogueService } = require(path.join(srcDir, 'dialogue.js'));
  const { createActionEngine } = require(path.join(srcDir, 'actionEngine.js'));
  const { createTurnEngine } = require(path.join(srcDir, 'turnEngine.js'));
  const { createGodCommandService } = require(path.join(srcDir, 'godCommands.js'));

  return {
    Agent,
    createMemoryStore,
    createDialogueService,
    createActionEngine,
    createTurnEngine,
    createGodCommandService
  };
}

async function main() {
  const startedAt = nowMs();
  const opts = parseArgs(process.argv.slice(2));

  if (!opts.engineDir) usageAndExit('Missing required argument: --engineDir');

  const engineDir = path.resolve(opts.engineDir);
  if (!fs.existsSync(engineDir) || !fs.statSync(engineDir).isDirectory()) {
    usageAndExit(`Invalid --engineDir: ${engineDir}`);
  }

  const reportPath = path.resolve(process.cwd(), opts.report);
  ensureDirForFile(reportPath);

  const report = {
    startedAt: toIso(startedAt),
    endedAt: null,
    durationMs: null,
    options: cloneJson(opts),
    resolved: {
      engineDir,
      reportPath
    },
    memory: {
      tempDir: null,
      filePath: null
    },
    phases: {},
    failures: [],
    invariants: {},
    runtime: {
      peakHeapMb: 0
    },
    overall: 'FAIL'
  };

  const fail = createFailureCollector(report);

  let unhandledRejections = 0;
  const onUnhandledRejection = (reason) => {
    unhandledRejections += 1;
    fail.add('runtime', 'unhandled_rejection', reason instanceof Error ? reason.message : String(reason));
  };
  process.on('unhandledRejection', onUnhandledRejection);

  let peakHeapBytes = process.memoryUsage().heapUsed;
  const sampleHeap = () => {
    const current = process.memoryUsage().heapUsed;
    if (current > peakHeapBytes) peakHeapBytes = current;
  };

  const phaseStatsTemplate = () => ({
    sent: 0,
    applied: 0,
    ignored: 0,
    errors: 0
  });

  const startPhase = (name) => {
    const phase = {
      name,
      startedAt: new Date().toISOString(),
      endedAt: null,
      durationMs: 0,
      stats: phaseStatsTemplate(),
      failures: [],
      events: [],
      notes: []
    };
    report.phases[name] = phase;
    return phase;
  };

  const finishPhase = (phase, phaseStartedAt) => {
    phase.endedAt = new Date().toISOString();
    phase.durationMs = nowMs() - phaseStartedAt;
  };

  const notePhaseFailure = (phase, type, detail) => {
    const item = {
      type,
      detail: safeString(detail),
      ts: new Date().toISOString()
    };
    phase.failures.push(item);
    fail.add(phase.name, type, detail);
  };

  const phaseEvent = (phase, payload) => {
    phase.events.push({
      ts: new Date().toISOString(),
      ...payload
    });
  };

  const modules = resolveEngineModules(engineDir);
  const logger = createSilentLogger('chaos');
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-chaos-harness-'));
  const memoryFile = path.join(tempDir, 'memory.json');
  report.memory.tempDir = tempDir;
  report.memory.filePath = memoryFile;

  const memoryStore = modules.createMemoryStore({
    filePath: memoryFile,
    logger: logger.child({ subsystem: 'memory' }),
    enableTxTimers: !!opts.timers
  });

  const dialogueService = modules.createDialogueService({
    memoryStore,
    logger: logger.child({ subsystem: 'dialogue' }),
    openaiClient: null
  });
  const actionEngine = modules.createActionEngine({
    memoryStore,
    logger: logger.child({ subsystem: 'action_engine' })
  });
  const turnEngine = modules.createTurnEngine({
    memoryStore,
    actionEngine,
    logger: logger.child({ subsystem: 'turn_engine' })
  });
  void dialogueService;
  void turnEngine;

  const godCommandService = modules.createGodCommandService({
    memoryStore,
    logger: logger.child({ subsystem: 'god_commands' })
  });

  memoryStore.loadAllMemory();

  const agents = [];
  agents.push(new modules.Agent({ name: 'Mara', role: 'Scout', faction: 'Pilgrims' }));
  agents.push(new modules.Agent({ name: 'Eli', role: 'Guard', faction: 'Pilgrims' }));
  const generatedCount = Math.max(0, opts.agents - 2);
  const factions = ['Pilgrims', 'IronPact', 'VeilChurch', 'Wardens'];
  for (let i = 0; i < generatedCount; i += 1) {
    const name = `Agent_${String(i + 1).padStart(3, '0')}`;
    agents.push(new modules.Agent({
      name,
      role: 'Scout',
      faction: factions[i % factions.length]
    }));
  }
  const agentNames = agents.map((agent) => agent.name);

  const counters = {
    READONLY_HASH_VIOLATIONS: 0,
    IDEMPOTENCY_VIOLATIONS: 0,
    NEGATIVE_LEDGER_BALANCE_VIOLATIONS: 0,
    FEED_CAP_VIOLATIONS: 0,
    UNIQUE_ID_VIOLATIONS: 0,
    TITLE_DUPLICATE_VIOLATIONS: 0
  };

  const trackSnapshotHealth = (snapshot, phaseName) => {
    const world = snapshot?.world || {};
    const ledger = (world.economy && typeof world.economy.ledger === 'object' && !Array.isArray(world.economy.ledger))
      ? world.economy.ledger
      : {};
    for (const [agentName, balanceRaw] of Object.entries(ledger)) {
      const balance = Number(balanceRaw);
      if (Number.isFinite(balance) && balance < 0) {
        counters.NEGATIVE_LEDGER_BALANCE_VIOLATIONS += 1;
        fail.add(phaseName, 'negative_ledger_balance', `${agentName}:${balance}`);
      }
    }

    const newsLength = Array.isArray(world.news) ? world.news.length : 0;
    const chronicleLength = Array.isArray(world.chronicle) ? world.chronicle.length : 0;
    if (newsLength > 200) {
      counters.FEED_CAP_VIOLATIONS += 1;
      fail.add(phaseName, 'feed_cap_news', `news=${newsLength}`);
    }
    if (chronicleLength > 200) {
      counters.FEED_CAP_VIOLATIONS += 1;
      fail.add(phaseName, 'feed_cap_chronicle', `chronicle=${chronicleLength}`);
    }
  };

  const recordUniqueIdViolations = (snapshot, phaseName) => {
    const summary = summarizeSnapshotIds(snapshot);
    const rumorDupes = countDuplicateKeys(summary.rumorIds, (v) => v.toLowerCase());
    const decisionDupes = countDuplicateKeys(summary.decisionIds, (v) => v.toLowerCase());
    const questDupes = countDuplicateKeys(summary.questIds, (v) => v.toLowerCase());
    const totalDupes = rumorDupes + decisionDupes + questDupes;
    counters.UNIQUE_ID_VIOLATIONS = totalDupes;
    if (totalDupes > 0) {
      fail.add(phaseName, 'unique_id_violation', `rumor=${rumorDupes} decision=${decisionDupes} quest=${questDupes}`);
    }
  };

  const recordTitleDuplicateViolations = (snapshot, phaseName) => {
    let dupes = 0;
    for (const [agentName, record] of Object.entries(snapshot?.agents || {})) {
      const titles = Array.isArray(record?.profile?.titles) ? record.profile.titles : [];
      const seen = new Set();
      for (const title of titles) {
        const key = safeString(title).trim().toLowerCase();
        if (!key) continue;
        if (seen.has(key)) {
          dupes += 1;
          fail.add(phaseName, 'title_duplicate', `${agentName}:${title}`);
        } else {
          seen.add(key);
        }
      }
    }
    counters.TITLE_DUPLICATE_VIOLATIONS = dupes;
  };

  let opSeq = 0;
  const operationId = (phaseName, label) => {
    opSeq += 1;
    return `bb-chaos:${phaseName}:${shortHash(`${opts.seed}:${opSeq}:${label}`)}`;
  };

  const runCommand = async (phase, command, opId, options = {}) => {
    const started = nowMs();
    let result = null;
    let error = null;
    try {
      result = await godCommandService.applyGodCommand({
        agents,
        command,
        operationId: opId
      });
    } catch (err) {
      error = err;
    }

    sampleHeap();
    phase.stats.sent += 1;
    const elapsedMs = nowMs() - started;
    const event = {
      type: 'command',
      command,
      operationId: opId,
      elapsedMs,
      result: result
        ? {
          applied: !!result.applied,
          reason: result.reason || null,
          outputLines: Array.isArray(result.outputLines) ? result.outputLines.slice(0, 8) : []
        }
        : null,
      error: error
        ? (error instanceof Error ? error.message : String(error))
        : null
    };
    phase.events.push(event);

    if (error) {
      phase.stats.errors += 1;
      notePhaseFailure(phase, 'command_error', `${command} :: ${event.error}`);
      if (!options.allowFailure) throw error;
      return { result: null, error };
    }

    if (result && result.applied) phase.stats.applied += 1;
    else phase.stats.ignored += 1;

    if (options.requireApplied && (!result || !result.applied)) {
      notePhaseFailure(phase, 'command_not_applied', `${command} :: ${result ? result.reason : 'unknown'}`);
    }

    if (options.onResult) options.onResult(result);
    return { result, error: null };
  };

  const ensureRumorId = () => {
    const snapshot = memoryStore.getSnapshot();
    const rumors = Array.isArray(snapshot?.world?.rumors) ? snapshot.world.rumors : [];
    return rumors.length > 0 ? safeString(rumors[rumors.length - 1].id) : '';
  };

  const ensureDecisionId = () => {
    const snapshot = memoryStore.getSnapshot();
    const decisions = Array.isArray(snapshot?.world?.decisions) ? snapshot.world.decisions : [];
    return decisions.length > 0 ? safeString(decisions[decisions.length - 1].id) : '';
  };

  const ensureQuestId = (filterFn = null) => {
    const snapshot = memoryStore.getSnapshot();
    const quests = Array.isArray(snapshot?.world?.quests) ? snapshot.world.quests : [];
    const rows = typeof filterFn === 'function' ? quests.filter(filterFn) : quests;
    return rows.length > 0 ? safeString(rows[rows.length - 1].id) : '';
  };

  const ensureOffer = () => {
    const snapshot = memoryStore.getSnapshot();
    const markets = Array.isArray(snapshot?.world?.markets) ? snapshot.world.markets : [];
    for (const market of markets) {
      const marketName = safeString(market?.name);
      const offers = Array.isArray(market?.offers) ? market.offers : [];
      const active = offers.find((offer) => offer && offer.active !== false && safeString(offer.offer_id));
      if (active) return { marketName, offerId: safeString(active.offer_id) };
    }
    return { marketName: '', offerId: '' };
  };

  const runReplayCheck = async (phase, label, command) => {
    const opId = operationId(phase.name, `replay:${label}:${command}`);
    const before = hashSnapshot(memoryStore.getSnapshot());
    const first = await runCommand(phase, command, opId, { allowFailure: false });
    const afterFirstSnapshot = memoryStore.getSnapshot();
    const afterFirst = hashSnapshot(afterFirstSnapshot);
    const second = await runCommand(phase, command, opId, { allowFailure: false });
    const afterSecondSnapshot = memoryStore.getSnapshot();
    const afterSecond = hashSnapshot(afterSecondSnapshot);

    if (afterSecond !== afterFirst || (second.result && second.result.applied)) {
      counters.IDEMPOTENCY_VIOLATIONS += 1;
      notePhaseFailure(
        phase,
        'idempotency_violation',
        `${label} cmd="${command}" firstApplied=${first.result ? first.result.applied : 'null'} secondApplied=${second.result ? second.result.applied : 'null'}`
      );
    }

    if (before === afterFirst && first.result && first.result.applied) {
      notePhaseFailure(phase, 'unexpected_no_change_on_first_apply', command);
    }
  };

  const runReadOnlyHashCheck = async (phase, label, command) => {
    const beforeSnapshot = memoryStore.getSnapshot();
    const beforeHash = hashSnapshot(beforeSnapshot);
    await runCommand(phase, command, operationId(phase.name, `readonly:${label}`), { allowFailure: false });
    const afterSnapshot = memoryStore.getSnapshot();
    const afterHash = hashSnapshot(afterSnapshot);
    if (beforeHash !== afterHash) {
      counters.READONLY_HASH_VIOLATIONS += 1;
      notePhaseFailure(phase, 'readonly_hash_violation', `${command}`);
    }
  };

  const tryRunPhase = async (phaseName, fn) => {
    const phase = startPhase(phaseName);
    const phaseStartedAt = nowMs();
    try {
      await fn(phase);
    } catch (err) {
      notePhaseFailure(phase, 'phase_exception', err instanceof Error ? err.stack || err.message : String(err));
    } finally {
      finishPhase(phase, phaseStartedAt);
    }
  };

  await tryRunPhase('setup', async (phase) => {
    await runCommand(phase, 'mark add alpha_hall 0 64 0 town:alpha', operationId(phase.name, 'town-alpha'), { requireApplied: true });
    await runCommand(phase, 'mark add beta_hall 40 64 10 town:beta', operationId(phase.name, 'town-beta'), { requireApplied: true });
    await runCommand(phase, 'market add bazaar alpha_hall', operationId(phase.name, 'market-bazaar'), { requireApplied: true });
    await runCommand(phase, 'market add beta_market beta_hall', operationId(phase.name, 'market-beta'), { requireApplied: true });
    await runCommand(phase, 'job set Mara guard alpha_hall', operationId(phase.name, 'job-mara'), { requireApplied: true });
    await runCommand(phase, 'job set Eli scout beta_hall', operationId(phase.name, 'job-eli'), { requireApplied: true });
    await runCommand(phase, 'mint Mara 120', operationId(phase.name, 'mint-mara'), { requireApplied: true });
    await runCommand(phase, 'mint Eli 120', operationId(phase.name, 'mint-eli'), { requireApplied: true });
    for (const generatedAgent of agentNames.slice(2)) {
      await runCommand(phase, `mint ${generatedAgent} 60`, operationId(phase.name, `mint-${generatedAgent}`), { allowFailure: true });
    }
    await runCommand(phase, 'offer add bazaar Eli sell 12 3', operationId(phase.name, 'offer-seed'), { requireApplied: true });
    await runCommand(phase, `event seed ${opts.seed}`, operationId(phase.name, 'event-seed'), { requireApplied: true });

    const snapshot = memoryStore.getSnapshot();
    trackSnapshotHealth(snapshot, phase.name);
    phaseEvent(phase, { type: 'snapshot', summary: summarizeSnapshotIds(snapshot) });
  });

  await tryRunPhase('rumors', async (phase) => {
    await runCommand(phase, 'quest offer alpha visit_town alpha 1', operationId(phase.name, 'resolve-quest'), { requireApplied: true });
    const resolveQuestId = ensureQuestId();
    if (!resolveQuestId) notePhaseFailure(phase, 'missing_quest_id', 'quest offer for rumor resolve');

    let rumorId = '';
    await runCommand(phase, 'rumor spawn alpha supernatural 2 mist_shapes 2', operationId(phase.name, 'spawn-primary'), {
      requireApplied: true,
      onResult(result) {
        rumorId = parseOutputValue(result?.outputLines || [], /GOD RUMOR SPAWN:\s*id=([^\s]+)/i) || '';
      }
    });
    if (!rumorId) rumorId = ensureRumorId();

    await runCommand(phase, 'rumor list alpha 10', operationId(phase.name, 'list-primary'), { allowFailure: false });
    if (rumorId) {
      await runCommand(phase, `rumor show ${rumorId}`, operationId(phase.name, 'show-primary'), { allowFailure: false });
      if (resolveQuestId) {
        await runCommand(phase, `rumor resolve ${rumorId} ${resolveQuestId}`, operationId(phase.name, 'resolve-primary'), { requireApplied: true });
      }
      await runCommand(phase, `rumor clear ${rumorId}`, operationId(phase.name, 'clear-primary'), { allowFailure: true });
    }

    await runCommand(phase, 'rumor spawn alpha political 2 levy_accusations 2', operationId(phase.name, 'spawn-side'), { requireApplied: true });
    const sideRumorId = ensureRumorId();
    if (sideRumorId) {
      await runCommand(phase, `rumor quest ${sideRumorId}`, operationId(phase.name, 'rumor-quest-side'), { requireApplied: true });
    }

    const snapshot = memoryStore.getSnapshot();
    trackSnapshotHealth(snapshot, phase.name);
    phaseEvent(phase, { type: 'snapshot', summary: summarizeSnapshotIds(snapshot) });
  });

  await tryRunPhase('nightfall_decisions', async (phase) => {
    await runCommand(phase, `event seed ${opts.seed + 101}`, operationId(phase.name, 'event-seed-nightfall'), { requireApplied: true });
    await runCommand(phase, 'clock advance 1', operationId(phase.name, 'nightfall-1'), { requireApplied: true });

    let decisionId = ensureDecisionId();
    if (!decisionId) {
      await runCommand(phase, 'event draw alpha', operationId(phase.name, 'fallback-event-draw'), { allowFailure: true });
      decisionId = ensureDecisionId();
    }

    await runCommand(phase, 'decision list alpha', operationId(phase.name, 'decision-list-1'), { allowFailure: false });
    if (decisionId) {
      const show = await runCommand(phase, `decision show ${decisionId}`, operationId(phase.name, 'decision-show-1'), { allowFailure: false });
      const optionKey = parseDecisionOptionKey(show.result?.outputLines || []);
      if (optionKey) {
        await runCommand(phase, `decision choose ${decisionId} ${optionKey}`, operationId(phase.name, 'decision-choose-1'), { allowFailure: true });
      } else {
        notePhaseFailure(phase, 'missing_decision_option', `decision=${decisionId}`);
      }
    } else {
      notePhaseFailure(phase, 'missing_decision_id', 'no decision after nightfall');
    }

    await runCommand(phase, 'clock advance 1', operationId(phase.name, 'nightfall-2'), { requireApplied: true });
    await runCommand(phase, 'clock advance 1', operationId(phase.name, 'nightfall-3'), { requireApplied: true });

    const decisionSnapshot = memoryStore.getSnapshot();
    const openDecision = (Array.isArray(decisionSnapshot?.world?.decisions) ? decisionSnapshot.world.decisions : [])
      .find((entry) => safeString(entry?.state).toLowerCase() === 'open');
    if (openDecision && openDecision.id) {
      await runCommand(phase, `decision expire ${openDecision.id}`, operationId(phase.name, 'decision-expire-1'), { allowFailure: true });
    } else {
      notePhaseFailure(phase, 'missing_open_decision_for_expire', 'no open decision found');
    }

    const snapshot = memoryStore.getSnapshot();
    trackSnapshotHealth(snapshot, phase.name);
    phaseEvent(phase, { type: 'snapshot', summary: summarizeSnapshotIds(snapshot) });
  });

  await tryRunPhase('side_quests', async (phase) => {
    const completeVisitSideQuest = async (kind, templateKey, label, replayVisit) => {
      await runCommand(phase, `rumor spawn alpha ${kind} 2 ${templateKey} 2`, operationId(phase.name, `spawn-${label}`), { requireApplied: true });
      const rumorId = ensureRumorId();
      if (!rumorId) {
        notePhaseFailure(phase, 'missing_rumor_for_side_quest', label);
        return '';
      }
      await runCommand(phase, `rumor quest ${rumorId}`, operationId(phase.name, `rumor-quest-${label}`), { requireApplied: true });
      const questId = ensureQuestId((entry) => safeString(entry?.type).toLowerCase() === 'rumor_task');
      if (!questId) {
        notePhaseFailure(phase, 'missing_side_quest_id', label);
        return '';
      }
      await runCommand(phase, `quest accept Mara ${questId}`, operationId(phase.name, `quest-accept-${label}`), { requireApplied: true });
      if (replayVisit) {
        await runReplayCheck(phase, `visit-${label}`, `quest visit ${questId}`);
      } else {
        await runCommand(phase, `quest visit ${questId}`, operationId(phase.name, `quest-visit-${label}`), { allowFailure: true });
      }
      return questId;
    };

    const visitQuestA = await completeVisitSideQuest('supernatural', 'mist_shapes', 'visit-a', true);
    const visitQuestB = await completeVisitSideQuest('political', 'levy_accusations', 'visit-b', false);
    if (!visitQuestA || !visitQuestB) {
      notePhaseFailure(phase, 'visit_side_quest_incomplete', 'missing visit quest completion');
    }

    await runCommand(phase, 'rumor spawn alpha grounded 2 missing_goods 2', operationId(phase.name, 'spawn-trade'), { requireApplied: true });
    const tradeRumorId = ensureRumorId();
    if (tradeRumorId) {
      await runCommand(phase, `rumor quest ${tradeRumorId}`, operationId(phase.name, 'rumor-quest-trade'), { requireApplied: true });
      const tradeQuestId = ensureQuestId((entry) => {
        const rumorTask = safeString(entry?.objective?.rumor_task).toLowerCase();
        return safeString(entry?.type).toLowerCase() === 'rumor_task' && rumorTask === 'rumor_trade';
      });
      if (!tradeQuestId) {
        notePhaseFailure(phase, 'missing_trade_side_quest', 'no rumor_trade quest');
      } else {
        await runCommand(phase, `quest accept Mara ${tradeQuestId}`, operationId(phase.name, 'quest-accept-trade'), { requireApplied: true });
        await runCommand(phase, 'offer add bazaar Eli sell 3 2', operationId(phase.name, 'offer-trade-side'), { requireApplied: true });
        const offer = ensureOffer();
        if (offer.marketName && offer.offerId) {
          await runReplayCheck(phase, 'trade-side', `trade ${offer.marketName} ${offer.offerId} Mara 1`);
        } else {
          notePhaseFailure(phase, 'missing_offer_for_trade_side_quest', 'no offer available');
        }
      }
    } else {
      notePhaseFailure(phase, 'missing_trade_rumor', 'grounded rumor not found');
    }

    const snapshot = memoryStore.getSnapshot();
    const maraTitles = Array.isArray(snapshot?.agents?.Mara?.profile?.titles)
      ? snapshot.agents.Mara.profile.titles
      : [];
    if (!maraTitles.some((title) => safeString(title).toLowerCase() === 'wanderer')) {
      notePhaseFailure(phase, 'missing_wanderer_title', 'Mara did not earn Wanderer');
    }
    trackSnapshotHealth(snapshot, phase.name);
    phaseEvent(phase, { type: 'snapshot', summary: summarizeSnapshotIds(snapshot) });
  });

  await tryRunPhase('rep_titles_traits', async (phase) => {
    await runCommand(phase, 'trait set Mara courage 3', operationId(phase.name, 'trait-set-mara'), { requireApplied: true });
    await runCommand(phase, 'trait Mara', operationId(phase.name, 'trait-show-mara'), { allowFailure: false });
    await runCommand(phase, 'title Mara', operationId(phase.name, 'title-show-mara-initial'), { allowFailure: false });
    await runReplayCheck(phase, 'title-grant-night-watch', 'title grant Mara Night Watch');
    await runCommand(phase, 'title revoke Mara Night Watch', operationId(phase.name, 'title-revoke-night-watch'), { allowFailure: true });
    await runReplayCheck(phase, 'rep-pact-friend', 'rep add Mara iron_pact 5');
    await runReplayCheck(phase, 'rep-veil-initiate', 'rep add Mara veil_church 5');
    await runCommand(phase, 'title Mara', operationId(phase.name, 'title-show-mara-final'), { allowFailure: false });

    const snapshot = memoryStore.getSnapshot();
    const maraTitles = Array.isArray(snapshot?.agents?.Mara?.profile?.titles)
      ? snapshot.agents.Mara.profile.titles.map((item) => safeString(item))
      : [];
    const mustHave = ['Pact Friend', 'Veil Initiate', 'Wanderer'];
    for (const required of mustHave) {
      if (!maraTitles.some((title) => title.toLowerCase() === required.toLowerCase())) {
        notePhaseFailure(phase, 'missing_required_title', required);
      }
    }
    trackSnapshotHealth(snapshot, phase.name);
    phaseEvent(phase, { type: 'snapshot', summary: summarizeSnapshotIds(snapshot) });
  });

  await tryRunPhase('readonly_hash', async (phase) => {
    let rumorId = ensureRumorId();
    if (!rumorId) {
      await runCommand(phase, 'rumor spawn alpha supernatural 2 mist_shapes 2', operationId(phase.name, 'seed-rumor-readonly'), { allowFailure: true });
      rumorId = ensureRumorId();
    }

    let decisionId = ensureDecisionId();
    if (!decisionId) {
      await runCommand(phase, 'event draw alpha', operationId(phase.name, 'seed-decision-readonly'), { allowFailure: true });
      decisionId = ensureDecisionId();
    }

    await runReadOnlyHashCheck(phase, 'town-list', 'town list');
    await runReadOnlyHashCheck(phase, 'town-board', 'town board alpha 10');
    await runReadOnlyHashCheck(phase, 'rumor-list', 'rumor list alpha 10');
    if (rumorId) await runReadOnlyHashCheck(phase, 'rumor-show', `rumor show ${rumorId}`);
    else notePhaseFailure(phase, 'missing_rumor_for_readonly_show', 'rumor show skipped');
    await runReadOnlyHashCheck(phase, 'decision-list', 'decision list alpha');
    if (decisionId) await runReadOnlyHashCheck(phase, 'decision-show', `decision show ${decisionId}`);
    else notePhaseFailure(phase, 'missing_decision_for_readonly_show', 'decision show skipped');
    await runReadOnlyHashCheck(phase, 'trait-show', 'trait Mara');
    await runReadOnlyHashCheck(phase, 'title-show', 'title Mara');
    await runReadOnlyHashCheck(phase, 'news-tail', 'news tail 20');
    await runReadOnlyHashCheck(phase, 'chronicle-tail', 'chronicle tail 20');

    const snapshot = memoryStore.getSnapshot();
    trackSnapshotHealth(snapshot, phase.name);
    phaseEvent(phase, { type: 'snapshot', summary: summarizeSnapshotIds(snapshot) });
  });

  await tryRunPhase('concurrency', async (phase) => {
    const workers = Math.max(1, opts.workers);
    const rounds = Math.max(1, opts.rounds);
    const kinds = ['grounded', 'supernatural', 'political'];
    const templates = {
      grounded: ['missing_goods', 'dock_counts'],
      supernatural: ['mist_shapes', 'relic_prophecy'],
      political: ['levy_accusations', 'guild_blame']
    };
    const traits = ['courage', 'greed', 'faith'];
    const factionsForRep = ['iron_pact', 'veil_church'];

    const buildConcurrentCommand = (rng) => {
      const snapshot = memoryStore.getSnapshot();
      const ids = summarizeSnapshotIds(snapshot);
      const world = snapshot?.world || {};

      const reads = [
        'town list',
        'town board alpha 5',
        'rumor list alpha 5',
        'decision list alpha',
        'news tail 5',
        'chronicle tail 5',
        'trait Mara',
        'title Mara',
        'quest list alpha'
      ];

      const writes = [];
      const kind = pickOne(rng, kinds);
      const template = pickOne(rng, templates[kind] || ['missing_goods']);
      writes.push(`rumor spawn alpha ${kind} ${randInt(rng, 1, 3)} ${template} ${randInt(rng, 1, 3)}`);
      writes.push(`rep add ${pickOne(rng, agentNames)} ${pickOne(rng, factionsForRep)} ${randInt(rng, 1, 2)}`);
      writes.push(`trait set ${pickOne(rng, agentNames)} ${pickOne(rng, traits)} ${randInt(rng, 0, 3)}`);
      writes.push(`quest offer alpha visit_town alpha ${randInt(rng, 0, 3)}`);
      writes.push(`quest offer alpha trade_n 1 bazaar ${randInt(rng, 0, 3)}`);
      writes.push(`offer add bazaar ${pickOne(rng, ['Mara', 'Eli'])} sell ${randInt(rng, 1, 2)} ${randInt(rng, 1, 6)}`);
      writes.push('clock advance 1');

      if (ids.rumorIds.length > 0) {
        writes.push(`rumor quest ${pickOne(rng, ids.rumorIds)}`);
        reads.push(`rumor show ${pickOne(rng, ids.rumorIds)}`);
      }

      if (ids.questIds.length > 0) {
        const questId = pickOne(rng, ids.questIds);
        writes.push(`quest accept ${pickOne(rng, ['Mara', 'Eli'])} ${questId}`);
        writes.push(`quest visit ${questId}`);
        writes.push(`quest complete ${questId}`);
      }

      if (ids.offers.length > 0) {
        const chosen = pickOne(rng, ids.offers);
        if (chosen && chosen.market && chosen.offerId) {
          writes.push(`trade ${chosen.market} ${chosen.offerId} ${pickOne(rng, ['Mara', 'Eli'])} 1`);
        }
      }

      const openDecision = (Array.isArray(world.decisions) ? world.decisions : [])
        .find((entry) => safeString(entry?.state).toLowerCase() === 'open');
      if (openDecision && openDecision.id) {
        const options = Array.isArray(openDecision.options) ? openDecision.options : [];
        const chosenOption = options.length > 0
          ? safeString(options[0]?.key || options[0]?.label || '')
          : '';
        if (chosenOption) writes.push(`decision choose ${openDecision.id} ${chosenOption}`);
        writes.push(`decision expire ${openDecision.id}`);
      }

      const useRead = rng() < 0.35;
      if (useRead) return pickOne(rng, reads);
      return pickOne(rng, writes.concat(reads));
    };

    const workerTasks = [];
    for (let w = 0; w < workers; w += 1) {
      const workerRng = mulberry32((opts.seed + (w + 1) * 9973) >>> 0);
      workerTasks.push((async () => {
        for (let i = w; i < rounds; i += workers) {
          const command = buildConcurrentCommand(workerRng) || 'town list';
          const opId = operationId(phase.name, `worker:${w}:i:${i}:${command}`);
          await runCommand(phase, command, opId, { allowFailure: true });
          if (i % 20 === 0) {
            const snapshot = memoryStore.getSnapshot();
            trackSnapshotHealth(snapshot, phase.name);
          }
        }
      })());
    }

    await Promise.all(workerTasks);
    const snapshot = memoryStore.getSnapshot();
    trackSnapshotHealth(snapshot, phase.name);
    phaseEvent(phase, { type: 'snapshot', summary: summarizeSnapshotIds(snapshot) });
  });

  await tryRunPhase('replay', async (phase) => {
    await runReplayCheck(phase, 'replay-rep', 'rep add Mara iron_pact 1');
    await runReplayCheck(phase, 'replay-rumor-spawn', 'rumor spawn alpha supernatural 2 mist_shapes 2');
    await runReplayCheck(phase, 'replay-trait-set', 'trait set Eli faith 2');
    await runReplayCheck(phase, 'replay-quest-offer', 'quest offer alpha visit_town alpha 1');

    const decisionId = ensureDecisionId();
    if (decisionId) {
      const show = await runCommand(phase, `decision show ${decisionId}`, operationId(phase.name, 'replay-decision-show'), { allowFailure: true });
      const optionKey = parseDecisionOptionKey(show.result?.outputLines || []);
      if (optionKey) {
        await runReplayCheck(phase, 'replay-decision-choose', `decision choose ${decisionId} ${optionKey}`);
      }
    }

    const snapshot = memoryStore.getSnapshot();
    trackSnapshotHealth(snapshot, phase.name);
    phaseEvent(phase, { type: 'snapshot', summary: summarizeSnapshotIds(snapshot) });
  });

  const finalSnapshot = memoryStore.getSnapshot();
  trackSnapshotHealth(finalSnapshot, 'final');
  recordUniqueIdViolations(finalSnapshot, 'final');
  recordTitleDuplicateViolations(finalSnapshot, 'final');

  const integrity = memoryStore.validateMemoryIntegrity();
  const runtimeMetrics = memoryStore.getRuntimeMetrics();
  const lockTimeouts = Number(runtimeMetrics?.lockTimeouts || 0);
  const integrityOk = !!integrity?.ok;
  const peakHeapMb = peakHeapBytes / (1024 * 1024);

  report.runtime.peakHeapMb = peakHeapMb;
  report.invariants = {
    LOCK_TIMEOUTS: lockTimeouts,
    INTEGRITY_OK: integrityOk,
    UNHANDLED_REJECTIONS: unhandledRejections,
    READONLY_HASH_VIOLATIONS: counters.READONLY_HASH_VIOLATIONS,
    IDEMPOTENCY_VIOLATIONS: counters.IDEMPOTENCY_VIOLATIONS,
    NEGATIVE_LEDGER_BALANCE_VIOLATIONS: counters.NEGATIVE_LEDGER_BALANCE_VIOLATIONS,
    FEED_CAP_VIOLATIONS: counters.FEED_CAP_VIOLATIONS,
    UNIQUE_ID_VIOLATIONS: counters.UNIQUE_ID_VIOLATIONS,
    TITLE_DUPLICATE_VIOLATIONS: counters.TITLE_DUPLICATE_VIOLATIONS
  };

  const overallPass = (
    lockTimeouts === 0
    && integrityOk === true
    && unhandledRejections === 0
    && counters.READONLY_HASH_VIOLATIONS === 0
    && counters.IDEMPOTENCY_VIOLATIONS === 0
    && counters.NEGATIVE_LEDGER_BALANCE_VIOLATIONS === 0
    && counters.FEED_CAP_VIOLATIONS === 0
    && counters.UNIQUE_ID_VIOLATIONS === 0
    && counters.TITLE_DUPLICATE_VIOLATIONS === 0
    && report.failures.length === 0
  );

  report.overall = overallPass ? 'PASS' : 'FAIL';
  report.endedAt = new Date().toISOString();
  report.durationMs = nowMs() - startedAt;

  fs.writeFileSync(reportPath, JSON.stringify(report, null, 2), 'utf8');

  process.stdout.write(`AGENTS: ${opts.agents}\n`);
  process.stdout.write(`ROUNDS: ${opts.rounds}\n`);
  process.stdout.write(`WORKERS: ${opts.workers}\n`);
  process.stdout.write(`PEAK_HEAP_MB: ${peakHeapMb.toFixed(2)}\n`);
  process.stdout.write(`LOCK_TIMEOUTS: ${lockTimeouts}\n`);
  process.stdout.write(`INTEGRITY_OK: ${integrityOk}\n`);
  process.stdout.write(`UNHANDLED_REJECTIONS: ${unhandledRejections}\n`);
  process.stdout.write(`READONLY_HASH_VIOLATIONS: ${counters.READONLY_HASH_VIOLATIONS}\n`);
  process.stdout.write(`IDEMPOTENCY_VIOLATIONS: ${counters.IDEMPOTENCY_VIOLATIONS}\n`);
  process.stdout.write(`NEGATIVE_LEDGER_BALANCE_VIOLATIONS: ${counters.NEGATIVE_LEDGER_BALANCE_VIOLATIONS}\n`);
  process.stdout.write(`FEED_CAP_VIOLATIONS: ${counters.FEED_CAP_VIOLATIONS}\n`);
  process.stdout.write(`UNIQUE_ID_VIOLATIONS: ${counters.UNIQUE_ID_VIOLATIONS}\n`);
  process.stdout.write(`TITLE_DUPLICATE_VIOLATIONS: ${counters.TITLE_DUPLICATE_VIOLATIONS}\n`);
  process.stdout.write(`OVERALL: ${overallPass ? 'PASS' : 'FAIL'}\n`);

  process.off('unhandledRejection', onUnhandledRejection);
  if (!overallPass) process.exitCode = 1;
}

main().catch((err) => {
  const message = err instanceof Error ? (err.stack || err.message) : String(err);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});

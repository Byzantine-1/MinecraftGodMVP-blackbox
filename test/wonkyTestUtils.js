'use strict';

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { publicRepoDir, findCliEntry } = require('../lib/bbHarness');

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function safeStamp() {
  return new Date().toISOString().replace(/[:.]/g, '-');
}

function createWonkyRunDir(testName) {
  const runDir = path.join(__dirname, '..', 'artifacts', safeStamp(), testName);
  fs.mkdirSync(runDir, { recursive: true });
  return runDir;
}

async function withPublicMemoryFile(runDir, fn) {
  const prior = process.env.PUBLIC_MEMORY_FILE;
  const memoryFile = path.join(runDir, 'memory.json');
  if (!fs.existsSync(memoryFile)) {
    fs.writeFileSync(memoryFile, '{}', 'utf8');
  }
  process.env.PUBLIC_MEMORY_FILE = memoryFile;
  try {
    return await fn(memoryFile);
  } finally {
    if (prior === undefined) delete process.env.PUBLIC_MEMORY_FILE;
    else process.env.PUBLIC_MEMORY_FILE = prior;
  }
}

function resolveEngineMemoryFile() {
  const repoDir = publicRepoDir();
  const entry = findCliEntry(repoDir);
  return path.join(path.dirname(entry), 'memory.json');
}

function unlinkIfExists(filePath) {
  try {
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
  } catch {}
}

async function withFreshEngineMemory(fn) {
  const memoryFile = resolveEngineMemoryFile();
  const lockFile = `${memoryFile}.lock`;
  const hadMemory = fs.existsSync(memoryFile);
  const backup = hadMemory ? fs.readFileSync(memoryFile, 'utf8') : null;

  unlinkIfExists(lockFile);
  if (!hadMemory) {
    fs.writeFileSync(memoryFile, '{}', 'utf8');
  } else {
    fs.writeFileSync(memoryFile, backup, 'utf8');
  }

  try {
    return await fn({ memoryFile, lockFile });
  } finally {
    unlinkIfExists(lockFile);
    if (hadMemory) fs.writeFileSync(memoryFile, backup, 'utf8');
    else unlinkIfExists(memoryFile);
    unlinkIfExists(lockFile);
  }
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
  return values[randInt(rng, 0, values.length - 1)];
}

function weightedPick(rng, weightedValues) {
  const total = weightedValues.reduce((sum, item) => sum + Number(item.weight || 0), 0);
  let roll = rng() * total;
  for (const item of weightedValues) {
    roll -= Number(item.weight || 0);
    if (roll <= 0) return item.value;
  }
  return weightedValues[weightedValues.length - 1].value;
}

function snapshotOffsets(cli) {
  const snap = cli.snapshot();
  return { stdout: snap.stdout.length, stderr: snap.stderr.length };
}

function deriveGodOperationId(command, sentAtMs, windowMs = 5000) {
  const bucket = Math.floor(Number(sentAtMs) / windowMs);
  const payload = JSON.stringify([bucket, 'cli', 'god', String(command || '')]);
  return crypto.createHash('sha256').update(payload).digest('hex').slice(0, 40);
}

function pushTranscript(transcript, entry) {
  transcript.commands.push({
    ts: new Date().toISOString(),
    ...entry
  });
}

function sendCommand(cli, transcript, command, meta = {}) {
  const sentAtMs = Date.now();
  const normalized = String(command || '').trim();
  const entry = {
    type: 'command',
    command: normalized,
    sentAtMs,
    ...meta
  };

  if (normalized.toLowerCase().startsWith('god ')) {
    const godCommand = normalized.slice(4).trim();
    entry.operationId = deriveGodOperationId(godCommand, sentAtMs);
    entry.operationBucket = Math.floor(sentAtMs / 5000);
  }

  pushTranscript(transcript, entry);
  try {
    cli.send(normalized);
  } catch (err) {
    entry.sendError = err instanceof Error ? err.message : String(err);
  }
  return entry;
}

async function waitForRegexSince(cli, regex, offsets, { timeoutMs = 8000, pollMs = 25 } = {}) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const snap = cli.snapshot();
    const stdoutDelta = snap.stdout.slice(offsets.stdout);
    const stderrDelta = snap.stderr.slice(offsets.stderr);
    const combined = `${stdoutDelta}\n${stderrDelta}`;
    const match = combined.match(regex);
    if (match) {
      return { match, stdoutDelta, stderrDelta, snapshot: snap };
    }
    await sleep(pollMs);
  }

  const final = cli.snapshot();
  throw new Error(
    `Timeout waiting for regex ${String(regex)}.\nstdout:\n${final.stdout}\n\nstderr:\n${final.stderr}`
  );
}

async function sendAndWait(cli, transcript, command, regex, options = {}) {
  const offsets = snapshotOffsets(cli);
  const sent = sendCommand(cli, transcript, command, options.meta || {});
  if (sent.sendError) {
    throw new Error(`Failed to send command "${command}": ${sent.sendError}`);
  }
  if (!regex) return null;
  return waitForRegexSince(cli, regex, offsets, options);
}

function escapeRegex(raw) {
  return String(raw || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function getLastMatch(text, regex) {
  const flags = regex.flags.includes('g') ? regex.flags : `${regex.flags}g`;
  const re = new RegExp(regex.source, flags);
  let last = null;
  for (const match of text.matchAll(re)) {
    last = match;
  }
  return last;
}

function parseLatestWorldMetrics(stdoutText) {
  const match = getLastMatch(
    stdoutText,
    /GOD INSPECT WORLD METRICS:\s*events=(\d+)\s+duplicates=(\d+)\s+committed=(\d+)\s+aborted=(\d+)\s+lock_retries=(\d+)\s+lock_timeouts=(\d+)/i
  );
  if (!match) return null;
  return {
    events: Number(match[1]),
    duplicates: Number(match[2]),
    committed: Number(match[3]),
    aborted: Number(match[4]),
    lockRetries: Number(match[5]),
    lockTimeouts: Number(match[6])
  };
}

function parseLatestStatusFlags(stdoutText) {
  const match = getLastMatch(stdoutText, /GOD STATUS FLAGS:\s*(.+)/i);
  return match ? String(match[1]).trim() : null;
}

function parseLatestBalance(stdoutText, agentName) {
  const target = String(agentName || '').toLowerCase();
  const re = /GOD BALANCE:\s*([A-Za-z0-9_]+)\s+balance=(-?\d+(?:\.\d+)?)\s+currency=([A-Za-z0-9_]+)/gi;
  let last = null;
  for (const match of stdoutText.matchAll(re)) {
    if (!target || String(match[1]).toLowerCase() === target) {
      last = {
        agent: match[1],
        balance: Number(match[2]),
        currency: match[3]
      };
    }
  }
  return last;
}

function parseOfferIds(stdoutText, marketName = null) {
  const ids = new Set();
  const marketFilter = marketName ? String(marketName).toLowerCase() : null;

  for (const match of stdoutText.matchAll(/GOD OFFER ADDED:\s*market=([^\s]+)\s+id=([^\s]+)\s+/gi)) {
    const market = String(match[1]).toLowerCase();
    if (!marketFilter || market === marketFilter) ids.add(match[2]);
  }

  const lineRegex = /GOD OFFER:\s*id=([^\s]+)\s+owner=([^\s]+)\s+side=([^\s]+)\s+amount=(\d+)\s+price=(\d+)\s+active=(true|false)/gi;
  for (const match of stdoutText.matchAll(lineRegex)) {
    ids.add(match[1]);
  }

  return Array.from(ids);
}

function countUnhandledRejections(text) {
  const matches = text.match(/unhandled_rejection|recoverable_unhandled_rejection|unhandled_promise_rejection/gi);
  return matches ? matches.length : 0;
}

function readMemoryJson(memoryFile) {
  const raw = fs.readFileSync(memoryFile, 'utf8');
  return JSON.parse(raw);
}

function countMarkersByName(memory, markerName) {
  const target = String(markerName || '').toLowerCase();
  const markers = Array.isArray(memory?.world?.markers) ? memory.world.markers : [];
  return markers.filter((marker) => String(marker?.name || '').toLowerCase() === target).length;
}

function countMarketsByName(memory, marketName) {
  const target = String(marketName || '').toLowerCase();
  const markets = Array.isArray(memory?.world?.markets) ? memory.world.markets : [];
  return markets.filter((market) => String(market?.name || '').toLowerCase() === target).length;
}

function findMarket(memory, marketName) {
  const target = String(marketName || '').toLowerCase();
  const markets = Array.isArray(memory?.world?.markets) ? memory.world.markets : [];
  return markets.find((market) => String(market?.name || '').toLowerCase() === target) || null;
}

function getLedgerBalance(memory, agentName) {
  const target = String(agentName || '').toLowerCase();
  const ledger = (memory?.world?.economy && typeof memory.world.economy.ledger === 'object')
    ? memory.world.economy.ledger
    : {};
  for (const [key, value] of Object.entries(ledger)) {
    if (String(key).toLowerCase() === target) return Number(value || 0);
  }
  return 0;
}

async function ensureWindowRoom(windowMs = 5000, minRoomMs = 300) {
  const remaining = windowMs - (Date.now() % windowMs);
  if (remaining < minRoomMs) {
    await sleep(remaining + 30);
  }
  return {
    bucket: Math.floor(Date.now() / windowMs),
    roomMs: windowMs - (Date.now() % windowMs)
  };
}

async function collectInvariantSnapshot(cli, transcript, phase) {
  await sendAndWait(
    cli,
    transcript,
    'god inspect world',
    /GOD INSPECT WORLD METRICS:\s*events=\d+\s+duplicates=\d+\s+committed=\d+\s+aborted=\d+\s+lock_retries=\d+\s+lock_timeouts=\d+/i,
    { timeoutMs: 10000, meta: { phase, category: 'invariant' } }
  );

  await sendAndWait(
    cli,
    transcript,
    'god status',
    /GOD STATUS FLAGS:\s*(.+)/i,
    { timeoutMs: 10000, meta: { phase, category: 'invariant' } }
  );

  const snap = cli.snapshot();
  const world = parseLatestWorldMetrics(snap.stdout);
  const flags = parseLatestStatusFlags(snap.stdout) || '';
  const unhandled = countUnhandledRejections(`${snap.stdout}\n${snap.stderr}`);

  return {
    INTEGRITY_OK: !/CRITICAL:integrity_failed/i.test(flags),
    LOCK_TIMEOUTS: Number(world?.lockTimeouts || 0),
    UNHANDLED_REJECTIONS: unhandled,
    worldMetrics: world,
    statusFlags: flags
  };
}

function writeArtifacts({ runDir, transcript, summary, snapshots }) {
  const snapList = Array.isArray(snapshots) && snapshots.length > 0
    ? snapshots
    : [{ name: 'run', snapshot: { stdout: '', stderr: '' } }];

  const stdout = snapList
    .map((entry) => `=== ${entry.name} ===\n${entry.snapshot.stdout || ''}`)
    .join('\n\n');
  const stderr = snapList
    .map((entry) => `=== ${entry.name} ===\n${entry.snapshot.stderr || ''}`)
    .join('\n\n');

  fs.writeFileSync(path.join(runDir, 'stdout.log'), stdout, 'utf8');
  fs.writeFileSync(path.join(runDir, 'stderr.log'), stderr, 'utf8');
  fs.writeFileSync(path.join(runDir, 'transcript.json'), JSON.stringify(transcript, null, 2), 'utf8');
  fs.writeFileSync(path.join(runDir, 'summary.json'), JSON.stringify(summary, null, 2), 'utf8');
}

module.exports = {
  sleep,
  createWonkyRunDir,
  withPublicMemoryFile,
  withFreshEngineMemory,
  mulberry32,
  randInt,
  pickOne,
  weightedPick,
  sendCommand,
  sendAndWait,
  waitForRegexSince,
  snapshotOffsets,
  parseLatestWorldMetrics,
  parseLatestStatusFlags,
  parseLatestBalance,
  parseOfferIds,
  countUnhandledRejections,
  readMemoryJson,
  countMarkersByName,
  countMarketsByName,
  findMarket,
  getLedgerBalance,
  ensureWindowRoom,
  collectInvariantSnapshot,
  writeArtifacts,
  getLastMatch,
  escapeRegex,
  deriveGodOperationId
};

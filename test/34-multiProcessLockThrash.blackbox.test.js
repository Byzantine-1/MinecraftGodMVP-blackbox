'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');

const { startCli, forceKill } = require('../lib/bbHarness');
const {
  sleep,
  createWonkyRunDir,
  withFreshEngineMemory,
  mulberry32,
  randInt,
  parseLatestWorldMetrics,
  countUnhandledRejections,
  readMemoryJson,
  writeArtifacts
} = require('./wonkyTestUtils');

function sendLogged(cli, transcript, command, meta = {}) {
  const entry = {
    ts: new Date().toISOString(),
    type: 'command',
    command,
    ...meta
  };
  transcript.commands.push(entry);
  try {
    cli.send(command);
  } catch (err) {
    entry.sendError = err instanceof Error ? err.message : String(err);
  }
}

function buildStormCommand(rng, label, idx) {
  const choice = randInt(rng, 0, 7);
  if (choice === 0) return 'god inspect world';
  if (choice === 1) return `god mark add ${label}_mk_${idx} ${randInt(rng, -80, 80)} 64 ${randInt(rng, -80, 80)} lock`;
  if (choice === 2) return `god mark remove ${label}_mk_${Math.max(0, idx - 1)}`;
  if (choice === 3) return 'god mint Mara 1';
  if (choice === 4) return 'god mint Eli 1';
  if (choice === 5) return 'god transfer Mara Eli 1';
  if (choice === 6) return 'god balance Mara';
  return 'god mark list';
}

async function stormWorker(cli, transcript, rng, label, count) {
  for (let idx = 0; idx < count; idx += 1) {
    const command = buildStormCommand(rng, label, idx);
    sendLogged(cli, transcript, command, { phase: 'storm', worker: label, index: idx });
    const jitter = randInt(rng, 0, 6);
    if (jitter > 0) await sleep(jitter);
  }
}

async function waitForLockGone(lockFile, timeoutMs = 5000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (!fs.existsSync(lockFile)) return true;
    await sleep(40);
  }
  return !fs.existsSync(lockFile);
}

function clearLockIfPresent(lockFile) {
  try {
    if (fs.existsSync(lockFile)) fs.unlinkSync(lockFile);
  } catch {}
}

async function ensureRecoverableLock(lockFile, runDir, transcript) {
  if (await waitForLockGone(lockFile, 5000)) return;
  if (!fs.existsSync(lockFile)) return;

  const staleAt = new Date(Date.now() - 20_000);
  fs.utimesSync(lockFile, staleAt, staleAt);

  const probe = startCli({ runDir });
  try {
    await probe.waitFor(/WORLD ONLINE/i, { timeoutMs: 20000 });
    transcript.commands.push({
      ts: new Date().toISOString(),
      type: 'event',
      phase: 'cleanup',
      action: 'probe_start_for_stale_lock_clear'
    });
  } finally {
    try {
      await probe.stopGraceful({ timeoutMs: 4000 });
    } catch (_) {}
  }

  assert.equal(await waitForLockGone(lockFile, 5000), true, 'residual lock should clear via stale-lock recovery');
}

test(
  'blackbox: multi-process lock thrash recovers after abrupt peer death',
  { timeout: 240000 },
  async () => {
    const testName = '34-multiProcessLockThrash';
    const seed = Number(process.env.BB_LOCK_THRASH_SEED || 340071);
    const rng = mulberry32(seed);
    const runDir = createWonkyRunDir(testName);

    const transcript = {
      testName,
      seed,
      startedAt: new Date().toISOString(),
      commands: []
    };
    const summary = {
      testName,
      seed,
      runDir,
      killedWorker: 'B',
      invariants: null
    };

    await withFreshEngineMemory(async ({ memoryFile }) => {
      const cliA = startCli({ runDir });
      const cliB = startCli({ runDir });
      const cliC = startCli({ runDir });
      let failure = null;

      try {
        const lockFile = `${memoryFile}.lock`;
        clearLockIfPresent(lockFile);

        await Promise.all([
          cliA.waitFor(/WORLD ONLINE/i, { timeoutMs: 20000 }),
          cliB.waitFor(/WORLD ONLINE/i, { timeoutMs: 20000 }),
          cliC.waitFor(/WORLD ONLINE/i, { timeoutMs: 20000 })
        ]);

        clearLockIfPresent(lockFile);

        sendLogged(cliA, transcript, 'god mark add __mp_init 0 64 0 lock', { phase: 'init', worker: 'A' });
        await cliA.waitFor(/GOD MARK ADDED:\s*__mp_init/i, { timeoutMs: 15000 });

        const firstWave = Promise.allSettled([
          stormWorker(cliA, transcript, rng, 'A', 95),
          (async () => {
            const killAt = 32;
            for (let idx = 0; idx < 95; idx += 1) {
              if (idx === killAt) {
                forceKill(cliB.child);
                transcript.commands.push({
                  ts: new Date().toISOString(),
                  type: 'event',
                  phase: 'storm',
                  worker: 'B',
                  action: 'force_kill',
                  index: idx
                });
                break;
              }
              const command = buildStormCommand(rng, 'B', idx);
              sendLogged(cliB, transcript, command, { phase: 'storm', worker: 'B', index: idx });
              const jitter = randInt(rng, 0, 6);
              if (jitter > 0) await sleep(jitter);
            }
          })(),
          stormWorker(cliC, transcript, rng, 'C', 95)
        ]);

        await firstWave;
        await Promise.all([
          stormWorker(cliA, transcript, rng, 'A', 40),
          stormWorker(cliC, transcript, rng, 'C', 40)
        ]);

        sendLogged(cliA, transcript, 'god inspect world', { phase: 'verify', worker: 'A' });
        sendLogged(cliC, transcript, 'god inspect world', { phase: 'verify', worker: 'C' });
        await Promise.all([
          cliA.waitFor(/GOD INSPECT WORLD METRICS:/i, { timeoutMs: 20000 }),
          cliC.waitFor(/GOD INSPECT WORLD METRICS:/i, { timeoutMs: 20000 })
        ]);

        const memory = readMemoryJson(memoryFile);
        const markers = Array.isArray(memory?.world?.markers) ? memory.world.markers : [];
        const markerNames = markers.map((entry) => String(entry?.name || '').toLowerCase());
        assert.equal(markerNames.some((name) => name.startsWith('a_mk_')), true, 'worker A markers should persist');
        assert.equal(markerNames.some((name) => name.startsWith('c_mk_')), true, 'worker C markers should persist');

        const aSnap = cliA.snapshot();
        const cSnap = cliC.snapshot();
        const combined = `${aSnap.stdout}\n${aSnap.stderr}\n${cSnap.stdout}\n${cSnap.stderr}`;

        const metricsA = parseLatestWorldMetrics(aSnap.stdout);
        const metricsC = parseLatestWorldMetrics(cSnap.stdout);
        assert.ok(metricsA, 'worker A should emit inspect world metrics');
        assert.ok(metricsC, 'worker C should emit inspect world metrics');
        assert.equal(Number(metricsA.lockTimeouts || 0) <= 2, true, 'worker A lock timeouts should remain bounded');
        assert.equal(Number(metricsC.lockTimeouts || 0) <= 2, true, 'worker C lock timeouts should remain bounded');
        assert.equal(countUnhandledRejections(combined), 0, 'no unhandled rejections expected');

        summary.invariants = {
          lockTimeoutsA: Number(metricsA.lockTimeouts || 0),
          lockTimeoutsC: Number(metricsC.lockTimeouts || 0),
          unhandledRejections: countUnhandledRejections(combined)
        };
      } catch (err) {
        failure = err;
        summary.error = err instanceof Error ? err.message : String(err);
      } finally {
        try {
          await cliA.stopGraceful({ timeoutMs: 5000 });
        } catch (_) {}
        try {
          await cliB.stopGraceful({ timeoutMs: 2000 });
        } catch (_) {}
        try {
          await cliC.stopGraceful({ timeoutMs: 5000 });
        } catch (_) {}

        const lockFile = `${memoryFile}.lock`;
        await ensureRecoverableLock(lockFile, runDir, transcript);

        writeArtifacts({
          runDir,
          transcript,
          summary,
          snapshots: [
            { name: 'worker-a', snapshot: cliA.snapshot() },
            { name: 'worker-b', snapshot: cliB.snapshot() },
            { name: 'worker-c', snapshot: cliC.snapshot() }
          ]
        });
      }

      if (failure) throw failure;
    });
  }
);

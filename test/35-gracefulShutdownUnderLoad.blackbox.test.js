'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');

const { startCli } = require('../lib/bbHarness');
const {
  sleep,
  createWonkyRunDir,
  withFreshEngineMemory,
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

async function waitForProcessExit(child, timeoutMs = 20000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (child.exitCode !== null) return true;
    await sleep(25);
  }
  return false;
}

function clearLockIfPresent(lockFile) {
  try {
    if (fs.existsSync(lockFile)) fs.unlinkSync(lockFile);
  } catch {}
}

async function waitForLockGone(lockFile, timeoutMs = 5000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (!fs.existsSync(lockFile)) return true;
    await sleep(40);
  }
  return !fs.existsSync(lockFile);
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
  'blackbox: graceful shutdown under queued write load preserves durable snapshot',
  { timeout: 240000 },
  async () => {
    const testName = '35-gracefulShutdownUnderLoad';
    const runDir = createWonkyRunDir(testName);
    const transcript = {
      testName,
      startedAt: new Date().toISOString(),
      commands: []
    };
    const summary = {
      testName,
      runDir,
      burstCommands: 0,
      exitedGracefully: false,
      invariants: null
    };

    await withFreshEngineMemory(async ({ memoryFile }) => {
      const cli = startCli({ runDir });
      let cli2 = null;
      let failure = null;

      try {
        const lockFile = `${memoryFile}.lock`;
        clearLockIfPresent(lockFile);
        await cli.waitFor(/WORLD ONLINE/i, { timeoutMs: 20000 });
        clearLockIfPresent(lockFile);

        sendLogged(cli, transcript, 'god mark add __shutdown_init 0 64 0 durable', { phase: 'init' });
        await cli.waitFor(/GOD MARK ADDED:\s*__shutdown_init/i, { timeoutMs: 15000 });

        const burst = 140;
        summary.burstCommands = burst;
        for (let idx = 0; idx < burst; idx += 1) {
          sendLogged(cli, transcript, `god mark add sd_${idx} ${idx} 64 ${-idx} load`, { phase: 'burst', index: idx });
          if (idx % 7 === 0) {
            sendLogged(cli, transcript, 'god mint Mara 1', { phase: 'burst' });
          }
        }

        sendLogged(cli, transcript, 'exit', { phase: 'shutdown' });
        summary.exitedGracefully = await waitForProcessExit(cli.child, 60000);
        assert.equal(summary.exitedGracefully, true, 'CLI should exit after queued shutdown');

        const memory = readMemoryJson(memoryFile);
        const markers = Array.isArray(memory?.world?.markers) ? memory.world.markers : [];
        const markerNames = markers.map((entry) => String(entry?.name || '').toLowerCase());
        assert.equal(markerNames.includes('__shutdown_init'), true, 'init marker must persist after shutdown');
        assert.equal(markerNames.some((name) => name.startsWith('sd_')), true, 'burst markers should persist');

        cli2 = startCli({ runDir });
        await cli2.waitFor(/WORLD ONLINE/i, { timeoutMs: 20000 });
        clearLockIfPresent(lockFile);
        sendLogged(cli2, transcript, 'god inspect world', { phase: 'resume' });
        await cli2.waitFor(/GOD INSPECT WORLD METRICS:/i, { timeoutMs: 20000 });
        sendLogged(cli2, transcript, 'god mark list', { phase: 'resume' });
        await cli2.waitFor(/GOD MARK LIST:/i, { timeoutMs: 20000 });

        const resumed = cli2.snapshot();
        const metrics = parseLatestWorldMetrics(resumed.stdout);
        assert.ok(metrics, 'resume inspect should include world metrics');
        assert.equal(Number(metrics.lockTimeouts || 0), 0, 'lock timeouts should stay zero after resume');

        const unhandled = countUnhandledRejections(`${resumed.stdout}\n${resumed.stderr}`);
        assert.equal(unhandled, 0, 'no unhandled rejections expected after resume');

        summary.invariants = {
          lockTimeouts: Number(metrics.lockTimeouts || 0),
          unhandledRejections: unhandled,
          markerCount: markerNames.length
        };
      } catch (err) {
        failure = err;
        summary.error = err instanceof Error ? err.message : String(err);
      } finally {
        try {
          if (cli2) await cli2.stopGraceful({ timeoutMs: 5000 });
        } catch (_) {}
        try {
          await cli.stopGraceful({ timeoutMs: 5000 });
        } catch (_) {}

        const lockFile = `${memoryFile}.lock`;
        await ensureRecoverableLock(lockFile, runDir, transcript);

        writeArtifacts({
          runDir,
          transcript,
          summary,
          snapshots: [
            { name: 'phase-one', snapshot: cli.snapshot() },
            { name: 'phase-two', snapshot: cli2 ? cli2.snapshot() : { stdout: '', stderr: '' } }
          ]
        });
      }

      if (failure) throw failure;
    });
  }
);

'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');

const { makeRunDir, startCli, waitForFileHash, writeText, forceKill } = require('../lib/bbHarness');

async function sleep(ms) {
  await new Promise((r) => setTimeout(r, ms));
}

test(
  'blackbox: crash window sweep markers (force-kill + restart) stays healthy',
  { timeout: 120000 },
  async () => {
    const runDir = makeRunDir('21-crashWindowSweepMarkers');

    // --- Run 1 ---
    const cli1 = startCli({ runDir });
    try {
      await cli1.waitFor(/WORLD ONLINE/i, { timeoutMs: 15000 });

      // Ensure durable file exists
      cli1.send('god mark add __bb_init 0 64 0 init');
      const h0 = await waitForFileHash(cli1.memFile, { timeoutMs: 15000 });
      assert.ok(h0, 'memory hash should exist after init marker write');
      assert.ok(fs.existsSync(cli1.memFile), 'memory.json should exist after init marker write');

      // Add a handful of markers
      for (let i = 0; i < 10; i++) {
        cli1.send(`god mark add crash_${i} ${i} 64 ${-i} tag`);
      }
      await sleep(250);

      const h1 = await waitForFileHash(cli1.memFile, { timeoutMs: 15000 });
      assert.ok(h1, 'memory hash should exist after marker writes');

      writeText(runDir, 'phase1.stdout.txt', cli1.snapshot().stdout);
      writeText(runDir, 'phase1.stderr.txt', cli1.snapshot().stderr);

      // Force-kill mid-activity (simulated abrupt crash)
      forceKill(cli1.child);
      await sleep(700);
    } catch (e) {
      try {
        const snap = cli1.snapshot?.();
        if (snap) {
          writeText(runDir, 'failure1.stdout.txt', snap.stdout || '');
          writeText(runDir, 'failure1.stderr.txt', snap.stderr || '');
        }
      } catch (_) {}
      throw e;
    }

    // --- Restart ---
    const cli2 = startCli({ runDir });
    try {
      await cli2.waitFor(/WORLD ONLINE/i, { timeoutMs: 15000 });

      const h2 = await waitForFileHash(cli2.memFile, { timeoutMs: 15000 });
      assert.ok(h2, 'memory hash should exist after restart');
      assert.ok(fs.existsSync(cli2.memFile), 'memory.json should exist after restart');

      const memText = fs.readFileSync(cli2.memFile, 'utf8');
      assert.ok(memText.includes('__bb_init'), '__bb_init should survive crash+restart');
      // We don't assert all crash_* markers survive because crash timing can land between commands.
      // We only assert: file integrity + engine boots + init marker remains.

      // Simple responsiveness check
      cli2.send('god mark list');
      await sleep(2000);

      writeText(runDir, 'phase2.stdout.txt', cli2.snapshot().stdout);
      writeText(runDir, 'phase2.stderr.txt', cli2.snapshot().stderr);
    } catch (e) {
      try {
        const snap = cli2.snapshot?.();
        if (snap) {
          writeText(runDir, 'failure2.stdout.txt', snap.stdout || '');
          writeText(runDir, 'failure2.stderr.txt', snap.stderr || '');
        }
      } catch (_) {}
      throw e;
    } finally {
      try {
        await cli2.stopGraceful({ timeoutMs: 5000 });
      } catch (_) {}
    }
  }
);


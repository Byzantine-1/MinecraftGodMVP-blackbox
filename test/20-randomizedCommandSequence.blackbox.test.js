// test/20-randomizedCommandSequence.blackbox.test.js
'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');

const {
  makeRunDir,
  startCli,
  waitForFileHash,
  writeText,
} = require('../lib/bbHarness');

// Deterministic RNG
function mulberry32(seed) {
  let a = seed >>> 0;
  return function () {
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

function pickOne(rng, arr) {
  return arr[randInt(rng, 0, arr.length - 1)];
}

async function sleep(ms) {
  await new Promise((r) => setTimeout(r, ms));
}

async function waitForFileContains(filePath, needle, timeoutMs = 15000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (fs.existsSync(filePath)) {
      const text = fs.readFileSync(filePath, 'utf8');
      if (text.includes(needle)) return text;
    }
    await sleep(40);
  }
  throw new Error(`Timeout waiting for "${needle}" in ${filePath}`);
}

test(
  'blackbox: randomized command sequence (seeded) maintains invariants across restarts',
  { timeout: 120000 },
  async () => {
    const seed = 1337;
    const rng = mulberry32(seed);

    const runDir = makeRunDir('20-randomizedCommandSequence');
    const cli1 = startCli({ runDir });

    try {
      // Wait for CLI to be ready (matches your public repo output)
      await cli1.waitFor(/WORLD ONLINE/i, { timeoutMs: 15000 });

      // Force one durable commit so memory.json exists deterministically.
      cli1.send('god mark add __bb_init 0 64 0 init');

      const h0 = await waitForFileHash(cli1.memFile, { timeoutMs: 15000 });
      assert.ok(h0, 'memory hash should exist after init marker write');
      assert.ok(fs.existsSync(cli1.memFile), 'memory.json should exist after init marker write');

      // Ensure marker text is actually present.
      const memText0 = await waitForFileContains(cli1.memFile, '__bb_init', 15000);
      assert.ok(memText0.includes('__bb_init'), 'memory should contain __bb_init marker');

      // Randomized command mix (mostly safe/read-only + marker churn)
      const commands = [
        () => `god inspect world`,
        () => `god mark list`,
        () => `god mark add m_${randInt(rng, 1, 12)} ${randInt(rng, -25, 25)} 64 ${randInt(rng, -25, 25)} tag_${randInt(rng, 1, 4)}`,
        () => `god mark remove m_${randInt(rng, 1, 12)}`,
        () => `god loop status`,
        () => `god freeze mara`,
        () => `god unfreeze mara`,
        () => `god leader set mara`,
        () => `god leader clear`,
        // These are fine even if agents do not exist; should not crash engine.
        () => `god inspect mara`,
        () => `god inspect eli`,
        () => `god inspect nox`,
      ];

      const N = 60;
      for (let i = 0; i < N; i++) {
        cli1.send(pickOne(rng, commands)());
      }

      await sleep(200);

      const h1 = await waitForFileHash(cli1.memFile, { timeoutMs: 15000 });
      assert.ok(h1, 'memory hash should exist after randomized spam');

      // Save a snapshot for debugging if needed
      const s1 = cli1.snapshot();
      writeText(runDir, 'phase1.stdout.txt', s1.stdout);
      writeText(runDir, 'phase1.stderr.txt', s1.stderr);

      // --- Restart ---
      await cli1.stopGraceful({ timeoutMs: 5000 });

      const cli2 = startCli({ runDir });
      try {
        await cli2.waitFor(/WORLD ONLINE/i, { timeoutMs: 15000 });

        // After restart, the memory file should still exist and include init marker
        const h2 = await waitForFileHash(cli2.memFile, { timeoutMs: 15000 });
        assert.ok(h2, 'memory hash should exist after restart');
        assert.ok(fs.existsSync(cli2.memFile), 'memory.json should still exist after restart');

        const memText2 = await waitForFileContains(cli2.memFile, '__bb_init', 15000);
        assert.ok(memText2.includes('__bb_init'), 'marker should survive restart');

        // Quick still-responsive poke.
        cli2.send('god mark list');
        await sleep(100);

        const s2 = cli2.snapshot();
        writeText(runDir, 'phase2.stdout.txt', s2.stdout);
        writeText(runDir, 'phase2.stderr.txt', s2.stderr);
      } finally {
        await cli2.stopGraceful({ timeoutMs: 5000 });
      }
    } catch (e) {
      // If it fails, capture whatever we have for fast diagnosis
      try {
        const snap = cli1.snapshot?.();
        if (snap) {
          writeText(runDir, 'failure.stdout.txt', snap.stdout || '');
          writeText(runDir, 'failure.stderr.txt', snap.stderr || '');
        }
      } catch (_) {}
      throw e;
    } finally {
      // Ensure process is not left hanging
      try {
        await cli1.stopGraceful({ timeoutMs: 2000 });
      } catch (_) {}
    }
  }
);


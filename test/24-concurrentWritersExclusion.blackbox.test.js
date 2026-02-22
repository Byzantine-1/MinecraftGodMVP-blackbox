'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');

const { makeRunDir, startCli, waitForFileHash, writeText } = require('../lib/bbHarness');

async function sleep(ms) {
  await new Promise((r) => setTimeout(r, ms));
}

test(
  'blackbox: concurrent writers exclusion (two processes) does not corrupt memory',
  { timeout: 120000 },
  async () => {
    const runDir = makeRunDir('24-concurrentWritersExclusion');

    // Start two instances pointing at the SAME memory file (same runDir)
    const cliA = startCli({ runDir });
    const cliB = startCli({ runDir });

    try {
      await cliA.waitFor(/WORLD ONLINE/i, { timeoutMs: 15000 });
      await cliB.waitFor(/WORLD ONLINE/i, { timeoutMs: 15000 });

      // Force a durable commit so file exists (from A)
      cliA.send('god mark add __bb_init 0 64 0 init');
      const h0 = await waitForFileHash(cliA.memFile, { timeoutMs: 15000 });
      assert.ok(fs.existsSync(cliA.memFile), 'memory.json should exist after init');
      assert.ok(h0, 'memory hash should exist after init');

      // Now both attempt writes; lock should serialize, not corrupt.
      for (let i = 0; i < 8; i++) {
        cliA.send(`god mark add a_${i} ${i} 64 0 tagA`);
        await sleep(30);
        cliB.send(`god mark add b_${i} ${-i} 64 0 tagB`);
        await sleep(30);
      }

      await sleep(1200);

      const h1 = await waitForFileHash(cliA.memFile, { timeoutMs: 15000 });
      assert.ok(h1, 'memory hash should exist after concurrent writes');

      // Basic integrity: file should be readable JSON
      const text = fs.readFileSync(cliA.memFile, 'utf8');
      assert.ok(text.includes('__bb_init'), 'init marker should exist');
      JSON.parse(text); // should not throw

      writeText(runDir, 'A.stdout.txt', cliA.snapshot().stdout);
      writeText(runDir, 'A.stderr.txt', cliA.snapshot().stderr);
      writeText(runDir, 'B.stdout.txt', cliB.snapshot().stdout);
      writeText(runDir, 'B.stderr.txt', cliB.snapshot().stderr);
    } catch (e) {
      try {
        writeText(runDir, 'failureA.stdout.txt', cliA.snapshot?.().stdout || '');
        writeText(runDir, 'failureA.stderr.txt', cliA.snapshot?.().stderr || '');
        writeText(runDir, 'failureB.stdout.txt', cliB.snapshot?.().stdout || '');
        writeText(runDir, 'failureB.stderr.txt', cliB.snapshot?.().stderr || '');
      } catch (_) {}
      throw e;
    } finally {
      try {
        await cliA.stopGraceful({ timeoutMs: 4000 });
      } catch (_) {}
      try {
        await cliB.stopGraceful({ timeoutMs: 4000 });
      } catch (_) {}
    }
  }
);

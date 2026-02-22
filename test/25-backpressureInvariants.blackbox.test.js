'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');

const { makeRunDir, startCli, waitForFileHash, writeText } = require('../lib/bbHarness');

async function sleep(ms) {
  await new Promise((r) => setTimeout(r, ms));
}

test(
  'blackbox: backpressure invariants (loop on + spam) does not deadlock/crash',
  { timeout: 90000 },
  async () => {
    const runDir = makeRunDir('25-backpressureInvariants');
    const cli = startCli({ runDir });

    try {
      await cli.waitFor(/WORLD ONLINE/i, { timeoutMs: 15000 });

      // Ensure durable file exists
      cli.send('god mark add __bb_init 0 64 0 init');
      assert.ok(await waitForFileHash(cli.memFile, { timeoutMs: 15000 }), 'memory hash should exist after init');
      assert.ok(fs.existsSync(cli.memFile), 'memory.json should exist after init');

      // Turn loop on if supported; if your CLI uses a different verb, adjust.
      cli.send('god loop start');
      await sleep(2000);

      // Spam a mix of read-only + durable marker churn
      for (let i = 0; i < 120; i++) {
        cli.send('god inspect world');
        cli.send(`god mark add bp_${i} ${i % 10} 64 ${-(i % 10)} bp`);
        if (i % 3 === 0) cli.send(`god mark remove bp_${i - 2}`);
      }

      await sleep(600);

      // Confirm engine still responds and memory remains valid
      cli.send('god loop status');
      cli.send('god mark list');
      await sleep(250);

      const h = await waitForFileHash(cli.memFile, { timeoutMs: 15000 });
      assert.ok(h, 'memory hash should exist after backpressure spam');

      const memText = fs.readFileSync(cli.memFile, 'utf8');
      JSON.parse(memText);

      const snap = cli.snapshot();
      writeText(runDir, 'stdout.txt', snap.stdout);
      writeText(runDir, 'stderr.txt', snap.stderr);
    } catch (e) {
      try {
        const snap = cli.snapshot?.();
        if (snap) {
          writeText(runDir, 'failure.stdout.txt', snap.stdout || '');
          writeText(runDir, 'failure.stderr.txt', snap.stderr || '');
        }
      } catch (_) {}
      throw e;
    } finally {
      try {
        await cli.stopGraceful({ timeoutMs: 5000 });
      } catch (_) {}
    }
  }
);

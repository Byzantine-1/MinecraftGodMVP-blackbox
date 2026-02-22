'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');

const { makeRunDir, startCli, waitForFileHash, writeText } = require('../lib/bbHarness');

async function sleep(ms) {
  await new Promise((r) => setTimeout(r, ms));
}

test(
  'blackbox: replay storm does not crash or explode',
  { timeout: 60000 },
  async () => {
    const runDir = makeRunDir('22-eventReplayStorm');
    const cli = startCli({ runDir });

    try {
      await cli.waitFor(/WORLD ONLINE/i, { timeoutMs: 15000 });

      // Ensure durable file exists
      cli.send('god mark add __bb_init 0 64 0 init');
      assert.ok(await waitForFileHash(cli.memFile, { timeoutMs: 15000 }), 'memory hash should exist after init');
      assert.ok(fs.existsSync(cli.memFile), 'memory.json should exist after init');

      // "Replay storm": repeat the same logical commands many times.
      // Blackbox intent: engine should not crash, and durability should hold.
      for (let i = 0; i < 40; i++) {
        cli.send('god inspect world');
        cli.send('god mark add replay 1 64 1 tag');
        cli.send('god mark add replay 1 64 1 tag');
        cli.send('god mark remove replay');
        cli.send('god mark remove replay');
        cli.send('god mark list');
      }

      await sleep(300);

      const h = await waitForFileHash(cli.memFile, { timeoutMs: 15000 });
      assert.ok(h, 'memory hash should exist after storm');

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

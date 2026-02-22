'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');

const { makeRunDir, startCli, sha256File, waitForFileHash, writeText } = require('../lib/bbHarness');

async function sleep(ms) {
  await new Promise((r) => setTimeout(r, ms));
}

test(
  'blackbox: invalid inputs are true no-ops (memory hash unchanged)',
  { timeout: 60000 },
  async () => {
    const runDir = makeRunDir('23-invalidInputsNoMutation');
    const cli = startCli({ runDir });
    const initMarker = `__bb_init_${Date.now().toString(36)}`;

    try {
      await cli.waitFor(/WORLD ONLINE/i, { timeoutMs: 15000 });

      // Ensure durable file exists
      cli.send(`god mark add ${initMarker} 0 64 0 init`);
      await cli.waitFor(new RegExp(`GOD MARK ADDED:\\s*${initMarker}\\b`, 'i'), { timeoutMs: 15000 });
      const h0 = await waitForFileHash(cli.memFile, { timeoutMs: 15000 });
      assert.ok(fs.existsSync(cli.memFile), 'memory.json should exist after init');
      assert.ok(h0, 'memory hash should exist after init');

      // Invalid marker coordinates: NaN / Infinity / overflow-ish
      // Expectation: engine should not crash; ideally these commands should be rejected/no-op.
      const invalid = [
        'god mark add bad_nan NaN 64 0 tag',
        'god mark add bad_inf Infinity 64 0 tag',
        'god mark add bad_ninf -Infinity 64 0 tag',
        'god mark add bad_big 1e309 64 0 tag',
        'god mark add bad_big2 0 64 1e309 tag',
        'god mark add bad_missing 0 64 tag',
      ];

      for (const cmd of invalid) cli.send(cmd);

      // Also invalid command forms should not crash.
      cli.send('god mark remove');
      cli.send('god mark add');
      cli.send('god mark wobble');

      // Drain pending command processing before sampling hash.
      cli.send('god mark list');
      await cli.waitFor(/GOD MARK LIST:/i, { timeoutMs: 15000 });
      await sleep(300);

      const h1 = sha256File(cli.memFile);
      assert.ok(h1, 'memory hash should exist after invalid inputs');

      // Assert durable state did not change due to invalid command spam.
      assert.equal(h1, h0, 'memory hash should remain unchanged after invalid input spam');

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

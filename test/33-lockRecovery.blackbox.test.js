'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');

const { startCli } = require('../lib/bbHarness');
const { sleep, createWonkyRunDir, withFreshEngineMemory } = require('./wonkyTestUtils');

test(
  'blackbox: stale lock + orphan temp file recover on startup',
  { timeout: 60000 },
  async () => {
    const runDir = createWonkyRunDir('33-lockRecovery');

    await withFreshEngineMemory(async ({ memoryFile }) => {
      const lockFile = `${memoryFile}.lock`;
      const orphanTmpFile = `${memoryFile}.${process.pid}.orphan.tmp`;

      fs.writeFileSync(memoryFile, '{}', 'utf8');
      fs.writeFileSync(lockFile, 'stale-lock', 'utf8');
      const staleAt = new Date(Date.now() - 15_000);
      fs.utimesSync(lockFile, staleAt, staleAt);
      fs.writeFileSync(orphanTmpFile, '{"partial":', 'utf8');

      const cli = startCli({ runDir });
      try {
        assert.equal(cli.lockInfo.existed, true, 'stale lock should be detected');
        assert.equal(cli.lockInfo.removed, true, 'stale lock should be removed before launch');

        await cli.waitFor(/WORLD ONLINE/i, { timeoutMs: 20000 });

        cli.send('god inspect world');
        await cli.waitFor(/GOD INSPECT WORLD METRICS:/i, { timeoutMs: 10000 });

        cli.send('god mark add __lock_probe 1 64 1 lock');
        await cli.waitFor(/GOD MARK ADDED:\s*__lock_probe/i, { timeoutMs: 10000 });

        await sleep(200);
        const parsed = JSON.parse(fs.readFileSync(memoryFile, 'utf8'));
        const markers = Array.isArray(parsed?.world?.markers) ? parsed.world.markers : [];
        assert.ok(markers.some((marker) => String(marker?.name || '').toLowerCase() === '__lock_probe'));
      } finally {
        try {
          await cli.stopGraceful({ timeoutMs: 5000 });
        } catch (_) {}
        try {
          if (fs.existsSync(orphanTmpFile)) fs.unlinkSync(orphanTmpFile);
        } catch (_) {}
      }
    });
  }
);

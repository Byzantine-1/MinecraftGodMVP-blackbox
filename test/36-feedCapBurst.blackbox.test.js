'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { publicRepoDir } = require('../lib/bbHarness');

function parseTailTotals(lines, regex) {
  for (const line of lines || []) {
    const match = String(line).match(regex);
    if (!match) continue;
    return { count: Number(match[1]), total: Number(match[2]) };
  }
  return null;
}

function loadEngineModules() {
  const repoDir = publicRepoDir();
  const srcDir = path.join(repoDir, 'src');
  return {
    createMemoryStore: require(path.join(srcDir, 'memory.js')).createMemoryStore,
    createGodCommandService: require(path.join(srcDir, 'godCommands.js')).createGodCommandService
  };
}

test(
  'blackbox: feed cap burst remains bounded in isolated external engine run',
  { timeout: 120000 },
  async () => {
    const { createMemoryStore, createGodCommandService } = loadEngineModules();

    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-bb-feed-cap-'));
    const memoryFile = path.join(tmpDir, 'memory.json');

    const memoryStore = createMemoryStore({ filePath: memoryFile });
    const service = createGodCommandService({ memoryStore });
    const agents = [
      { name: 'Mara', faction: 'Pilgrims', applyGodCommand: () => {} },
      { name: 'Eli', faction: 'Pilgrims', applyGodCommand: () => {} }
    ];

    const burstWrites = 260;
    for (let idx = 0; idx < burstWrites; idx += 1) {
      const result = await service.applyGodCommand({
        agents,
        command: `mark add feed_${idx} ${idx} 64 ${-idx} cap`,
        operationId: `bb-feed-cap-${idx}`
      });
      assert.equal(result.applied, true);
    }

    const snapshot = memoryStore.getSnapshot();
    const markers = Array.isArray(snapshot?.world?.markers) ? snapshot.world.markers : [];
    const markerNames = markers.map((entry) => String(entry?.name || '').toLowerCase());
    assert.equal(snapshot.world.news.length, 200, 'news feed should cap at 200');
    assert.equal(snapshot.world.chronicle.length, 200, 'chronicle feed should cap at 200');
    assert.equal(markerNames.includes(`feed_${burstWrites - 1}`), true, 'latest marker should persist');
    assert.equal(memoryStore.validateMemoryIntegrity().ok, true, 'integrity should remain valid');

    const newsTail = await service.applyGodCommand({
      agents,
      command: 'news tail 999',
      operationId: 'bb-feed-cap-news-tail'
    });
    const chronicleTail = await service.applyGodCommand({
      agents,
      command: 'chronicle tail 999',
      operationId: 'bb-feed-cap-chronicle-tail'
    });
    assert.equal(newsTail.applied, true);
    assert.equal(chronicleTail.applied, true);

    const newsTotals = parseTailTotals(newsTail.outputLines, /GOD NEWS TAIL:\s*count=(\d+)\s+total=(\d+)/i);
    const chronicleTotals = parseTailTotals(chronicleTail.outputLines, /GOD CHRONICLE TAIL:\s*count=(\d+)\s+total=(\d+)/i);
    assert.ok(newsTotals);
    assert.ok(chronicleTotals);
    assert.equal(newsTotals.count, 200);
    assert.equal(newsTotals.total, 200);
    assert.equal(chronicleTotals.count, 200);
    assert.equal(chronicleTotals.total, 200);

    const runtime = memoryStore.getRuntimeMetrics();
    assert.equal(Number(runtime?.lockTimeouts || 0), 0, 'lock timeouts should remain zero');
  }
);

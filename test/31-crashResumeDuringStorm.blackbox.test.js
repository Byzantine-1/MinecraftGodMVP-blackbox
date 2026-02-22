'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { startCli, forceKill, waitForFileHash } = require('../lib/bbHarness');
const {
  sleep,
  createWonkyRunDir,
  withFreshEngineMemory,
  mulberry32,
  randInt,
  pickOne,
  weightedPick,
  sendCommand,
  sendAndWait,
  collectInvariantSnapshot,
  countMarkersByName,
  countMarketsByName,
  findMarket,
  getLedgerBalance,
  readMemoryJson,
  ensureWindowRoom,
  writeArtifacts,
  escapeRegex
} = require('./wonkyTestUtils');

function setupEventSuffix(command, markerName, marketName) {
  const normalized = String(command || '').trim().toLowerCase();
  if (normalized === `god mark add ${markerName.toLowerCase()} 10 64 10 core`) return `mark_add:${markerName.toLowerCase()}`;
  if (/^god mint mara \d+$/i.test(normalized)) return 'mint:mara';
  if (/^god mint eli \d+$/i.test(normalized)) return 'mint:eli';
  if (normalized === `god market add ${marketName.toLowerCase()} ${markerName.toLowerCase()}`) return `market_add:${marketName.toLowerCase()}`;
  if (normalized === `god offer add ${marketName.toLowerCase()} mara sell 5 7`) return `offer_add:${marketName.toLowerCase()}:mara`;
  return null;
}

function buildCrashStormCommand(rng, input) {
  const category = weightedPick(rng, [
    { value: 'read_only', weight: 62 },
    { value: 'mutation', weight: 23 },
    { value: 'chaos', weight: 15 }
  ]);

  if (category === 'read_only') {
    const reads = [
      'god inspect world',
      'god inspect mara',
      'god inspect eli',
      'god market list',
      `god offer list ${input.marketName}`,
      'god economy',
      'god balance mara',
      'god balance eli'
    ];
    return { command: pickOne(rng, reads), category };
  }

  if (category === 'mutation') {
    const mutations = [
      () => `god mark add ctmp_${randInt(rng, 1, 30)} ${randInt(rng, -40, 40)} 64 ${randInt(rng, -40, 40)} storm`,
      () => `god mark remove ctmp_${randInt(rng, 1, 30)}`,
      () => `god job set ${pickOne(rng, ['mara', 'eli'])} ${pickOne(rng, ['scout', 'guard', 'builder'])} ${input.markerName}`,
      () => `god job clear ${pickOne(rng, ['mara', 'eli'])}`
    ];
    return { command: pickOne(rng, mutations)(), category };
  }

  const chaos = [
    'god mint mara 1.5',
    'god transfer mara eli 99999',
    `god offer add ghost_market mara sell 2 6`,
    `god trade ${input.marketName} ${input.offerId || 'offer:unknown'} eli 999`,
    'god market remove ghost_market',
    `god mark add ${input.markerName} NaN 64 10`
  ];
  return { command: pickOne(rng, chaos), category };
}

test(
  'blackbox: crash/resume during wonky storm preserves durable single-apply state',
  { timeout: 180000 },
  async () => {
    const testName = '31-crashResumeDuringStorm';
    const seed = Number(process.env.BB_CRASH_SEED || 310041);
    const rng = mulberry32(seed);
    const runDir = createWonkyRunDir(testName);

    const uniqueSuffix = `${Date.now().toString(36)}${Math.floor(rng() * 1e6).toString(36)}`
      .replace(/[^a-z0-9]/gi, '')
      .toLowerCase()
      .slice(0, 10);
    const markerName = `crashhub_${uniqueSuffix}`;
    const marketName = `crashbazaar_${uniqueSuffix}`;

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
      markerName,
      marketName,
      setup: {},
      preCrash: null,
      replay: {},
      postResume: null
    };

    await withFreshEngineMemory(async () => {
      const cli1 = startCli({ runDir });
      let cli2 = null;
      let failure = null;
      let phase1Snapshot = { stdout: '', stderr: '' };
      let phase2Snapshot = { stdout: '', stderr: '' };

      const maraMintAmount = randInt(rng, 91, 160);
      const eliMintAmount = randInt(rng, 41, 110);

      const setupCommands = [
        `god mark add ${markerName} 10 64 10 core`,
        `god mint mara ${maraMintAmount}`,
        `god mint eli ${eliMintAmount}`,
        `god market add ${marketName} ${markerName}`,
        `god offer add ${marketName} mara sell 5 7`
      ];

      try {
        await cli1.waitFor(/WORLD ONLINE/i, { timeoutMs: 20000 });
        await ensureWindowRoom(5000, 1200);

        const baselineMara = Number(
          (await sendAndWait(
            cli1,
            transcript,
            'god balance mara',
            /GOD BALANCE:\s*Mara\s+balance=(-?\d+(?:\.\d+)?)\s+currency=/i,
            { timeoutMs: 10000, meta: { phase: 'setup_baseline', category: 'read_only' } }
          )).match[1]
        );
        const baselineEli = Number(
          (await sendAndWait(
            cli1,
            transcript,
            'god balance eli',
            /GOD BALANCE:\s*Eli\s+balance=(-?\d+(?:\.\d+)?)\s+currency=/i,
            { timeoutMs: 10000, meta: { phase: 'setup_baseline', category: 'read_only' } }
          )).match[1]
        );

        const expectedMara = baselineMara + maraMintAmount;
        const expectedEli = baselineEli + eliMintAmount;

        await sendAndWait(cli1, transcript, setupCommands[0], new RegExp(`GOD MARK ADDED:\\s*${escapeRegex(markerName)}`, 'i'), {
          timeoutMs: 10000,
          meta: { phase: 'setup', category: 'mutation' }
        });
        await sendAndWait(cli1, transcript, setupCommands[1], new RegExp(`GOD MINT:\\s*Mara\\s+\\+${maraMintAmount}`, 'i'), {
          timeoutMs: 10000,
          meta: { phase: 'setup', category: 'mutation' }
        });
        await sendAndWait(cli1, transcript, setupCommands[2], new RegExp(`GOD MINT:\\s*Eli\\s+\\+${eliMintAmount}`, 'i'), {
          timeoutMs: 10000,
          meta: { phase: 'setup', category: 'mutation' }
        });
        await sendAndWait(
          cli1,
          transcript,
          setupCommands[3],
          new RegExp(`GOD MARKET ADDED:\\s*${escapeRegex(marketName)}`, 'i'),
          {
            timeoutMs: 10000,
            meta: { phase: 'setup', category: 'mutation' }
          }
        );
        const offerAdd = await sendAndWait(
          cli1,
          transcript,
          setupCommands[4],
          new RegExp(`GOD OFFER ADDED:\\s*market=${escapeRegex(marketName)}\\s+id=([^\\s]+)\\s+`, 'i'),
          {
            timeoutMs: 10000,
            meta: { phase: 'setup', category: 'mutation' }
          }
        );

        const setupEntries = transcript.commands.filter((entry) => entry.phase === 'setup' && entry.type === 'command');
        const setupEvents = setupEntries.map((entry) => {
          const suffix = setupEventSuffix(entry.command, markerName, marketName);
          return {
            command: entry.command,
            operationId: entry.operationId || null,
            operationBucket: entry.operationBucket || null,
            predictedEventId: suffix && entry.operationId ? `${entry.operationId}:${suffix}` : null
          };
        });

        const offerId = offerAdd && offerAdd.match ? offerAdd.match[1] : null;
        summary.setup = {
          commands: setupCommands,
          setupEvents,
          initialOfferId: offerId,
          baselineMara,
          baselineEli,
          expectedMara,
          expectedEli
        };

        await waitForFileHash(cli1.memFile, { timeoutMs: 15000 });
        const preStormMemory = readMemoryJson(cli1.memFile);
        summary.setup.preStormState = {
          markerCount: countMarkersByName(preStormMemory, markerName),
          marketCount: countMarketsByName(preStormMemory, marketName),
          maraBalance: getLedgerBalance(preStormMemory, 'Mara'),
          eliBalance: getLedgerBalance(preStormMemory, 'Eli')
        };

        const totalStormCommands = randInt(rng, 120, 170);
        const crashIndex = randInt(rng, Math.floor(totalStormCommands * 0.45), Math.floor(totalStormCommands * 0.7));
        let burstRemaining = 0;

        for (let i = 0; i < totalStormCommands; i += 1) {
          if (i === crashIndex - 2) {
            summary.preCrash = await collectInvariantSnapshot(cli1, transcript, 'pre-crash');
          }

          if (burstRemaining === 0 && rng() < 0.08) {
            burstRemaining = 6;
            transcript.commands.push({
              ts: new Date().toISOString(),
              type: 'storm_mode',
              phase: 'storm',
              mode: 'burst',
              count: 6
            });
          }

          const stormCmd = buildCrashStormCommand(rng, { offerId, marketName, markerName });
          sendCommand(cli1, transcript, stormCmd.command, {
            phase: 'storm',
            category: stormCmd.category,
            index: i
          });

          if (i >= crashIndex) {
            transcript.commands.push({
              ts: new Date().toISOString(),
              type: 'crash',
              phase: 'storm',
              index: i,
              reason: 'forced_kill_mid_storm'
            });
            forceKill(cli1.child);
            break;
          }

          if (burstRemaining > 0) {
            burstRemaining -= 1;
          } else {
            const jitter = randInt(rng, 0, 20);
            if (jitter > 0) await sleep(jitter);
          }
        }

        await sleep(800);
        phase1Snapshot = cli1.snapshot();

        cli2 = startCli({ runDir });
        await cli2.waitFor(/WORLD ONLINE/i, { timeoutMs: 20000 });

        const setupBuckets = new Set(
          setupEvents.map((entry) => entry.operationBucket).filter((value) => Number.isFinite(value))
        );
        const sameBucketReplay = setupBuckets.size === 1 && setupBuckets.has(Math.floor(Date.now() / 5000));
        summary.replay.mode = sameBucketReplay ? 'exact_setup_replay' : 'safe_partial_replay';

        if (sameBucketReplay) {
          for (const command of setupCommands) {
            sendCommand(cli2, transcript, command, {
              phase: 'resume_replay',
              category: 'replay',
              replayMode: 'exact'
            });
          }
          await sleep(300);
        } else {
          const safeReplay = [
            `god mark add ${markerName} 10 64 10 core`,
            `god market add ${marketName} ${markerName}`
          ];
          for (const command of safeReplay) {
            sendCommand(cli2, transcript, command, {
              phase: 'resume_replay',
              category: 'replay',
              replayMode: 'safe_partial'
            });
          }
          summary.replay.note = 'Exact setup eventId replay is not externally injectable once operation windows roll; replayed safe subset only.';
          await sleep(250);
        }

        const postBurstCommands = randInt(rng, 25, 45);
        for (let i = 0; i < postBurstCommands; i += 1) {
          const stormCmd = buildCrashStormCommand(rng, {
            offerId: summary.setup.initialOfferId,
            marketName,
            markerName
          });
          sendCommand(cli2, transcript, stormCmd.command, {
            phase: 'post_resume_burst',
            category: stormCmd.category,
            index: i
          });
          const jitter = randInt(rng, 0, 20);
          if (jitter > 0) await sleep(jitter);
        }

        await sleep(300);
        assert.equal(cli2.child.exitCode, null, 'process should be alive after restart burst');

        const postSummary = await collectInvariantSnapshot(cli2, transcript, 'post-resume');
        summary.postResume = postSummary;

        const resumedMemory = readMemoryJson(cli2.memFile);
        const markerCount = countMarkersByName(resumedMemory, markerName);
        const marketCount = countMarketsByName(resumedMemory, marketName);
        const maraBalance = getLedgerBalance(resumedMemory, 'Mara');
        const eliBalance = getLedgerBalance(resumedMemory, 'Eli');
        const crashMarket = findMarket(resumedMemory, marketName);
        const offers = Array.isArray(crashMarket && crashMarket.offers) ? crashMarket.offers : [];

        assert.equal(markerCount, 1, 'crash marker must exist exactly once');
        assert.equal(marketCount, 1, 'crash market must exist exactly once');
        assert.equal(maraBalance, summary.setup.expectedMara, 'mara balance should stay single-apply');
        assert.equal(eliBalance, summary.setup.expectedEli, 'eli balance should stay single-apply');
        assert.equal(offers.length, 1, 'setup offer should exist exactly once');
        assert.equal(Number(offers[0].amount), 5, 'setup offer remaining amount should be single-apply');

        assert.equal(postSummary.INTEGRITY_OK, true, 'integrity should remain OK');
        assert.equal(postSummary.LOCK_TIMEOUTS, 0, 'lock timeouts should remain zero');
        assert.equal(postSummary.UNHANDLED_REJECTIONS, 0, 'unhandled rejections should remain zero');

        summary.postResumeState = {
          markerCount,
          marketCount,
          maraBalance,
          eliBalance,
          offerCount: offers.length,
          offerRemaining: Number(offers[0].amount),
          offerId: offers[0].offer_id || null
        };
      } catch (err) {
        failure = err;
        summary.error = err instanceof Error ? err.message : String(err);
      } finally {
        try {
          await cli1.stopGraceful({ timeoutMs: 2000 });
        } catch (_) {}
        phase1Snapshot = cli1.snapshot();

        if (cli2) {
          try {
            await cli2.stopGraceful({ timeoutMs: 5000 });
          } catch (_) {}
          phase2Snapshot = cli2.snapshot();
        }

        writeArtifacts({
          runDir,
          transcript,
          summary,
          snapshots: [
            { name: 'pre-crash-run', snapshot: phase1Snapshot },
            { name: 'post-resume-run', snapshot: phase2Snapshot }
          ]
        });

        fs.writeFileSync(
          path.join(runDir, 'summary.preCrash.json'),
          JSON.stringify(summary.preCrash || {}, null, 2),
          'utf8'
        );
        fs.writeFileSync(
          path.join(runDir, 'summary.postResume.json'),
          JSON.stringify(summary.postResume || {}, null, 2),
          'utf8'
        );
      }

      if (failure) throw failure;
    });
  }
);

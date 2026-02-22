'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { startCli } = require('../lib/bbHarness');
const {
  sleep,
  createWonkyRunDir,
  withFreshEngineMemory,
  mulberry32,
  randInt,
  pickOne,
  sendCommand,
  sendAndWait,
  collectInvariantSnapshot,
  countMarketsByName,
  findMarket,
  getLedgerBalance,
  readMemoryJson,
  ensureWindowRoom,
  writeArtifacts,
  escapeRegex
} = require('./wonkyTestUtils');

test(
  'blackbox: replay storm focused keeps fixed mutation set single-apply',
  { timeout: 120000 },
  async () => {
    const testName = '32-replayStormFocused';
    const seed = Number(process.env.BB_REPLAY_SEED || 320051);
    const rng = mulberry32(seed);
    const runDir = createWonkyRunDir(testName);
    const uniqueSuffix = `${Date.now().toString(36)}${Math.floor(rng() * 1e6).toString(36)}`
      .replace(/[^a-z0-9]/gi, '')
      .toLowerCase()
      .slice(0, 10);
    const markerName = `hub_${uniqueSuffix}`;
    const marketName = `baz_${uniqueSuffix}`;

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
      setup: {},
      replayStorm: {},
      expectedSingleApply: {
        maraBalance: null,
        eliBalance: null,
        marketCount: 1,
        offerCount: 1,
        offerRemaining: 1
      },
      observed: {},
      invariants: {}
    };

    await withFreshEngineMemory(async () => {
      const cli = startCli({ runDir });
      let failure = null;
      let finalSnapshot = { stdout: '', stderr: '' };

      try {
        await cli.waitFor(/WORLD ONLINE/i, { timeoutMs: 20000 });
        const baselineMara = Number(
          (await sendAndWait(
            cli,
            transcript,
            'god balance mara',
            /GOD BALANCE:\s*Mara\s+balance=(-?\d+(?:\.\d+)?)\s+currency=/i,
            { timeoutMs: 10000, meta: { phase: 'setup_baseline', category: 'read_only' } }
          )).match[1]
        );
        const baselineEli = Number(
          (await sendAndWait(
            cli,
            transcript,
            'god balance eli',
            /GOD BALANCE:\s*Eli\s+balance=(-?\d+(?:\.\d+)?)\s+currency=/i,
            { timeoutMs: 10000, meta: { phase: 'setup_baseline', category: 'read_only' } }
          )).match[1]
        );
        summary.expectedSingleApply.maraBalance = baselineMara + 21;
        summary.expectedSingleApply.eliBalance = baselineEli + 4;

        await sendAndWait(cli, transcript, `god mark add ${markerName} 0 64 0 core`, new RegExp(`GOD MARK ADDED:\\s*${escapeRegex(markerName)}`, 'i'), {
          timeoutMs: 10000,
          meta: { phase: 'setup', category: 'mutation' }
        });

        await ensureWindowRoom(5000, 4200);

        const fixedCommands = {
          M1: 'god mint mara 25',
          T1: 'god transfer mara eli 10',
          K1: `god market add ${marketName} ${markerName}`,
          O1: `god offer add ${marketName} mara sell 2 6`,
          TR1: null
        };

        await sendAndWait(cli, transcript, fixedCommands.M1, /GOD MINT:\s*Mara\s+\+25/i, {
          timeoutMs: 10000,
          meta: { phase: 'single_apply', label: 'M1', category: 'mutation' }
        });
        await sendAndWait(cli, transcript, fixedCommands.T1, /GOD TRANSFER:\s*Mara->Eli\s+amount=10/i, {
          timeoutMs: 10000,
          meta: { phase: 'single_apply', label: 'T1', category: 'mutation' }
        });
        await sendAndWait(cli, transcript, fixedCommands.K1, new RegExp(`GOD MARKET ADDED:\\s*${escapeRegex(marketName)}\\s+marker=${escapeRegex(markerName)}`, 'i'), {
          timeoutMs: 10000,
          meta: { phase: 'single_apply', label: 'K1', category: 'mutation' }
        });
        const offerAdded = await sendAndWait(
          cli,
          transcript,
          fixedCommands.O1,
          new RegExp(`GOD OFFER ADDED:\\s*market=${escapeRegex(marketName)}\\s+id=([^\\s]+)\\s+`, 'i'),
          {
            timeoutMs: 10000,
            meta: { phase: 'single_apply', label: 'O1', category: 'mutation' }
          }
        );
        const offerId = offerAdded.match[1];
        fixedCommands.TR1 = `god trade ${marketName} ${offerId} eli 1`;

        await sendAndWait(
          cli,
          transcript,
          fixedCommands.TR1,
          new RegExp(`GOD TRADE:\\s*market=${escapeRegex(marketName)}\\s+offer_id=`, 'i'),
          {
          timeoutMs: 10000,
          meta: { phase: 'single_apply', label: 'TR1', category: 'mutation' }
          }
        );

        summary.setup.offerId = offerId;

        const replayKeys = ['M1', 'T1', 'K1', 'O1', 'TR1'];
        const replayCount = randInt(rng, 28, 44);
        const replayOrder = [];

        let burstRemaining = 0;
        for (let i = 0; i < replayCount; i += 1) {
          if (burstRemaining === 0 && rng() < 0.14) {
            burstRemaining = 4;
            transcript.commands.push({
              ts: new Date().toISOString(),
              type: 'storm_mode',
              phase: 'replay_storm',
              mode: 'burst',
              count: 4
            });
          }

          const key = pickOne(rng, replayKeys);
          replayOrder.push(key);
          sendCommand(cli, transcript, fixedCommands[key], {
            phase: 'replay_storm',
            label: key,
            category: 'replay'
          });

          if (burstRemaining > 0) {
            burstRemaining -= 1;
          } else {
            const jitter = randInt(rng, 0, 12);
            if (jitter > 0) await sleep(jitter);
          }
        }

        summary.replayStorm = {
          replayCount,
          replayOrder
        };

        await sleep(350);

        await sendAndWait(cli, transcript, 'god balance mara', /GOD BALANCE:\s*Mara\s+balance=\d+/i, {
          timeoutMs: 10000,
          meta: { phase: 'final_queries', category: 'read_only' }
        });
        await sendAndWait(cli, transcript, 'god balance eli', /GOD BALANCE:\s*Eli\s+balance=\d+/i, {
          timeoutMs: 10000,
          meta: { phase: 'final_queries', category: 'read_only' }
        });
        await sendAndWait(cli, transcript, 'god market list', /GOD MARKET LIST:\s*(count=\d+|\(none\))/i, {
          timeoutMs: 10000,
          meta: { phase: 'final_queries', category: 'read_only' }
        });
        await sendAndWait(cli, transcript, `god offer list ${marketName}`, new RegExp(`GOD OFFER LIST:\\s*market=${escapeRegex(marketName)}`, 'i'), {
          timeoutMs: 10000,
          meta: { phase: 'final_queries', category: 'read_only' }
        });

        const invariants = await collectInvariantSnapshot(cli, transcript, 'final');
        assert.equal(invariants.INTEGRITY_OK, true, 'integrity should remain OK');
        assert.equal(invariants.LOCK_TIMEOUTS, 0, 'lock timeouts should remain zero');
        assert.equal(invariants.UNHANDLED_REJECTIONS, 0, 'unhandled rejections should remain zero');

        const memory = readMemoryJson(cli.memFile);
        const maraBalance = getLedgerBalance(memory, 'Mara');
        const eliBalance = getLedgerBalance(memory, 'Eli');
        const marketCount = countMarketsByName(memory, marketName);
        const market = findMarket(memory, marketName);
        const offers = Array.isArray(market && market.offers) ? market.offers : [];
        const remaining = offers.length > 0 ? Number(offers[0].amount) : NaN;

        assert.equal(maraBalance, summary.expectedSingleApply.maraBalance, 'mint+transfer+trade should apply once');
        assert.equal(eliBalance, summary.expectedSingleApply.eliBalance, 'transfer+trade should apply once');
        assert.equal(marketCount, summary.expectedSingleApply.marketCount, 'market should exist exactly once');
        assert.equal(offers.length, summary.expectedSingleApply.offerCount, 'offer should exist exactly once');
        assert.equal(remaining, summary.expectedSingleApply.offerRemaining, 'offer remaining should decrement once');

        summary.observed = {
          maraBalance,
          eliBalance,
          marketCount,
          offerCount: offers.length,
          offerRemaining: remaining,
          offerId: offers[0] ? offers[0].offer_id : null
        };
        summary.invariants = invariants;
      } catch (err) {
        failure = err;
        summary.error = err instanceof Error ? err.message : String(err);
      } finally {
        try {
          await cli.stopGraceful({ timeoutMs: 5000 });
        } catch (_) {}
        finalSnapshot = cli.snapshot();

        writeArtifacts({
          runDir,
          transcript,
          summary,
          snapshots: [{ name: 'replay-storm', snapshot: finalSnapshot }]
        });
      }

      if (failure) throw failure;
    });
  }
);

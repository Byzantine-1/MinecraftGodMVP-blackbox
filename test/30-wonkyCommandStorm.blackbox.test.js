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
  weightedPick,
  sendCommand,
  sendAndWait,
  waitForRegexSince,
  snapshotOffsets,
  parseOfferIds,
  collectInvariantSnapshot,
  ensureWindowRoom,
  readMemoryJson,
  countMarkersByName,
  writeArtifacts
} = require('./wonkyTestUtils');

function buildStormCommand(rng, state) {
  const category = weightedPick(rng, [
    { value: 'read_only', weight: 56 },
    { value: 'mutation', weight: 34 },
    { value: 'chaos', weight: 10 }
  ]);

  if (category === 'read_only') {
    const readCommands = [
      () => 'god inspect world',
      () => `god inspect ${rng() < 0.1 ? pickOne(rng, state.unknownAgents) : pickOne(rng, state.agents)}`,
      () => 'god market list',
      () => `god offer list ${rng() < 0.1 ? pickOne(rng, state.unknownMarkets) : pickOne(rng, Array.from(state.markets))}`,
      () => 'god economy',
      () => `god balance ${rng() < 0.1 ? pickOne(rng, state.unknownAgents) : pickOne(rng, state.agents)}`
    ];
    return { command: pickOne(rng, readCommands)(), category, mutation: false };
  }

  if (category === 'mutation') {
    const mutationType = randInt(rng, 0, 10);

    if (mutationType === 0) {
      const duplicateName = state.markers.size > 0 && rng() < 0.25;
      const markerName = duplicateName
        ? pickOne(rng, Array.from(state.markers))
        : `mk_${randInt(rng, 1, 40)}`;
      const invalidCoords = rng() < 0.1;
      if (!invalidCoords) state.markers.add(markerName);
      return {
        command: invalidCoords
          ? `god mark add ${markerName} NaN 64 10`
          : `god mark add ${markerName} ${randInt(rng, -80, 80)} 64 ${randInt(rng, -80, 80)} ${pickOne(rng, ['core', 'route', 'mine'])}`,
        category,
        mutation: true
      };
    }

    if (mutationType === 1) {
      const existing = state.markers.size > 0 && rng() < 0.7;
      const markerName = existing ? pickOne(rng, Array.from(state.markers)) : `gone_${randInt(rng, 1, 25)}`;
      if (existing) state.markers.delete(markerName);
      return { command: `god mark remove ${markerName}`, category, mutation: true };
    }

    if (mutationType === 2) {
      const agent = rng() < 0.08 ? pickOne(rng, state.unknownAgents) : pickOne(rng, state.agents);
      const role = pickOne(rng, ['scout', 'guard', 'builder', 'farmer', 'hauler']);
      const useMarker = state.markers.size > 0 && rng() < 0.7;
      const marker = useMarker ? ` ${pickOne(rng, Array.from(state.markers))}` : '';
      return { command: `god job set ${agent} ${role}${marker}`, category, mutation: true };
    }

    if (mutationType === 3) {
      const agent = rng() < 0.08 ? pickOne(rng, state.unknownAgents) : pickOne(rng, state.agents);
      return { command: `god job clear ${agent}`, category, mutation: true };
    }

    if (mutationType === 4) {
      const agent = rng() < 0.08 ? pickOne(rng, state.unknownAgents) : pickOne(rng, state.agents);
      const amount = rng() < 0.1 ? '1.5' : String(randInt(rng, 1, 25));
      return { command: `god mint ${agent} ${amount}`, category, mutation: true };
    }

    if (mutationType === 5) {
      const from = rng() < 0.08 ? pickOne(rng, state.unknownAgents) : pickOne(rng, state.agents);
      let to = rng() < 0.08 ? pickOne(rng, state.unknownAgents) : pickOne(rng, state.agents);
      if (String(to).toLowerCase() === String(from).toLowerCase()) {
        const alternatives = [...state.agents, ...state.unknownAgents]
          .filter((name) => String(name).toLowerCase() !== String(from).toLowerCase());
        to = alternatives.length > 0 ? pickOne(rng, alternatives) : to;
      }
      const insufficient = rng() < 0.1;
      const amount = insufficient ? String(randInt(rng, 2000, 6000)) : (rng() < 0.08 ? '2.5' : String(randInt(rng, 1, 12)));
      return { command: `god transfer ${from} ${to} ${amount}`, category, mutation: true };
    }

    if (mutationType === 6) {
      const marketName = rng() < 0.25 && state.markets.size > 0
        ? pickOne(rng, Array.from(state.markets))
        : `mkt_${randInt(rng, 1, 24)}`;
      const attachMarker = state.markers.size > 0 && rng() < 0.7;
      const marker = attachMarker ? ` ${pickOne(rng, Array.from(state.markers))}` : '';
      state.markets.add(marketName);
      return { command: `god market add ${marketName}${marker}`, category, mutation: true };
    }

    if (mutationType === 7) {
      const existing = state.markets.size > 0 && rng() < 0.65;
      const marketName = existing ? pickOne(rng, Array.from(state.markets)) : pickOne(rng, state.unknownMarkets);
      if (existing && marketName !== 'bazaar') state.markets.delete(marketName);
      return { command: `god market remove ${marketName}`, category, mutation: true };
    }

    if (mutationType === 8) {
      const marketName = rng() < 0.08 ? pickOne(rng, state.unknownMarkets) : pickOne(rng, Array.from(state.markets));
      const owner = rng() < 0.08 ? pickOne(rng, state.unknownAgents) : pickOne(rng, state.agents);
      const side = pickOne(rng, ['buy', 'sell']);
      const amount = rng() < 0.08 ? '3.5' : String(randInt(rng, 1, 8));
      const price = rng() < 0.08 ? '2.5' : String(randInt(rng, 1, 12));
      return { command: `god offer add ${marketName} ${owner} ${side} ${amount} ${price}`, category, mutation: true };
    }

    if (mutationType === 9) {
      const marketName = rng() < 0.08 ? pickOne(rng, state.unknownMarkets) : pickOne(rng, Array.from(state.markets));
      const known = state.offerIds.length > 0 && rng() < 0.7;
      const offerId = known ? pickOne(rng, state.offerIds) : `offer:bogus:${randInt(rng, 1, 99)}`;
      return { command: `god offer cancel ${marketName} ${offerId}`, category, mutation: true };
    }

    const marketName = rng() < 0.08 ? pickOne(rng, state.unknownMarkets) : pickOne(rng, Array.from(state.markets));
    const known = state.offerIds.length > 0 && rng() < 0.75;
    const offerId = known ? pickOne(rng, state.offerIds) : `offer:bogus:${randInt(rng, 1, 99)}`;
    const buyer = rng() < 0.08 ? pickOne(rng, state.unknownAgents) : pickOne(rng, state.agents);
    const amount = rng() < 0.1 ? String(randInt(rng, 9, 20)) : String(randInt(rng, 1, 3));
    return { command: `god trade ${marketName} ${offerId} ${buyer} ${amount}`, category, mutation: true };
  }

  const chaos = [
    () => `god mint ${pickOne(rng, state.unknownAgents)} 10`,
    () => `god market add ${pickOne(rng, state.unknownMarkets)} ${pickOne(rng, state.unknownMarkers)}`,
    () => `god transfer mara eli 99999`,
    () => `god transfer mara eli 1.5`,
    () => `god offer add ${pickOne(rng, state.unknownMarkets)} mara sell 2 6`,
    () => `god trade bazaar ${state.offerIds[0] || 'offer:unknown'} eli 999`,
    () => `god mark add ${pickOne(rng, ['hub', 'mk_1', 'mk_2'])} NaN 64 10`
  ];

  return {
    command: pickOne(rng, chaos)(),
    category,
    mutation: false
  };
}

test(
  'blackbox: wonky command storm survives jitter, replay storms, and invalid mutations',
  { timeout: 180000 },
  async () => {
    const testName = '30-wonkyCommandStorm';
    const seed = Number(process.env.BB_WONKY_SEED || 300031);
    const rng = mulberry32(seed);
    const runDir = createWonkyRunDir(testName);

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
      storm: {},
      idempotencySpotCheck: {},
      invariants: {}
    };

    await withFreshEngineMemory(async () => {
      const cli = startCli({ runDir });
      let failure = null;
      let finalSnapshot = { stdout: '', stderr: '' };

      const state = {
        agents: ['mara', 'eli'],
        unknownAgents: ['nox', 'ghost', 'nobody'],
        markers: new Set(['hub']),
        unknownMarkers: ['unknown_hub', 'void_pin', 'lost_point'],
        markets: new Set(['bazaar']),
        unknownMarkets: ['void_market', 'ghost_market', 'null_exchange'],
        offerIds: []
      };

      try {
        await cli.waitFor(/WORLD ONLINE/i, { timeoutMs: 20000 });

        await sendAndWait(cli, transcript, 'god inspect world', /GOD INSPECT WORLD METRICS:/i, {
          timeoutMs: 10000,
          meta: { phase: 'warmup', category: 'read_only' }
        });
        await sendAndWait(cli, transcript, 'god mark add hub 0 64 0 core', /GOD MARK ADDED:\s*hub/i, {
          timeoutMs: 10000,
          meta: { phase: 'warmup', category: 'mutation' }
        });
        await sendAndWait(cli, transcript, 'god job set mara scout hub', /GOD JOB SET:\s*Mara\s+role=scout/i, {
          timeoutMs: 10000,
          meta: { phase: 'warmup', category: 'mutation' }
        });
        await sendAndWait(cli, transcript, 'god mint mara 50', /GOD MINT:\s*Mara\s+\+50/i, {
          timeoutMs: 10000,
          meta: { phase: 'warmup', category: 'mutation' }
        });
        await sendAndWait(cli, transcript, 'god market add bazaar hub', /GOD MARKET (ADDED|COMMAND IGNORED|MARKET LIST)/i, {
          timeoutMs: 10000,
          meta: { phase: 'warmup', category: 'mutation' }
        });
        const offerAdd = await sendAndWait(
          cli,
          transcript,
          'god offer add bazaar mara sell 3 5',
          /GOD OFFER ADDED:\s*market=bazaar\s+id=([^\s]+)\s+/i,
          {
            timeoutMs: 10000,
            meta: { phase: 'warmup', category: 'mutation' }
          }
        );
        if (offerAdd && offerAdd.match && offerAdd.match[1]) {
          state.offerIds.push(offerAdd.match[1]);
        }
        summary.setup.initialOfferId = state.offerIds[0] || null;

        const totalStormLoops = randInt(rng, 240, 340);
        const counts = {
          read_only: 0,
          mutation: 0,
          chaos: 0,
          replayMutations: 0,
          bursts: 0,
          pauses: 0,
          totalCommandsSent: 0
        };

        let burstRemaining = 0;
        for (let i = 0; i < totalStormLoops; i += 1) {
          if (burstRemaining === 0 && rng() < 0.09) {
            burstRemaining = 10;
            counts.bursts += 1;
            transcript.commands.push({
              ts: new Date().toISOString(),
              type: 'storm_mode',
              mode: 'burst',
              count: 10
            });
          }

          if (burstRemaining === 0 && rng() < 0.05) {
            const pauseMs = randInt(rng, 250, 750);
            counts.pauses += 1;
            transcript.commands.push({
              ts: new Date().toISOString(),
              type: 'sleep',
              reason: 'storm_pause',
              ms: pauseMs
            });
            await sleep(pauseMs);
          }

          const stormCmd = buildStormCommand(rng, state);
          sendCommand(cli, transcript, stormCmd.command, {
            phase: 'storm',
            category: stormCmd.category,
            index: i
          });
          counts[stormCmd.category] += 1;
          counts.totalCommandsSent += 1;

          if (stormCmd.category === 'mutation' && stormCmd.mutation && rng() < 0.2) {
            await ensureWindowRoom(5000, 180);
            sendCommand(cli, transcript, stormCmd.command, {
              phase: 'storm',
              category: 'replay_mutation',
              replayOfIndex: i
            });
            counts.replayMutations += 1;
            counts.totalCommandsSent += 1;
          }

          if (i % 20 === 0) {
            const known = parseOfferIds(cli.snapshot().stdout, 'bazaar');
            state.offerIds = Array.from(new Set([...state.offerIds, ...known]));
          }

          if (burstRemaining > 0) {
            burstRemaining -= 1;
          } else {
            const jitterMs = randInt(rng, 0, 50);
            if (jitterMs > 0) {
              transcript.commands.push({
                ts: new Date().toISOString(),
                type: 'sleep',
                reason: 'jitter',
                ms: jitterMs
              });
              await sleep(jitterMs);
            }
          }
        }

        await sleep(250);
        assert.equal(cli.child.exitCode, null, 'process exited during storm');
        await sendAndWait(cli, transcript, 'god inspect world', /GOD INSPECT WORLD METRICS:/i, {
          timeoutMs: 10000,
          meta: { phase: 'storm_flush', category: 'read_only' }
        });
        await sleep(2000);

        await ensureWindowRoom(5000, 320);
        const probeMarker = `replay_probe_${Date.now().toString(36)}`;
        const spotCommand = `god mark add ${probeMarker} 13 64 13 replay_probe`;

        const replayOffsets = snapshotOffsets(cli);
        sendCommand(cli, transcript, spotCommand, {
          phase: 'spotcheck',
          category: 'mutation',
          label: 'single_apply_probe'
        });
        sendCommand(cli, transcript, spotCommand, {
          phase: 'spotcheck',
          category: 'replay_mutation',
          label: 'single_apply_probe_replay'
        });

        const replayEvidence = await waitForRegexSince(
          cli,
          /DUPLICATE_EVENT_SKIPPED:\s*([^\s"]+:mark_add:[^\s"]+)|GOD COMMAND IGNORED:\s*mark add .*Duplicate operation ignored\./i,
          replayOffsets,
          { timeoutMs: 10000 }
        );
        await sleep(250);

        const memoryAfterReplay = readMemoryJson(cli.memFile);
        const probeCount = countMarkersByName(memoryAfterReplay, probeMarker);
        assert.equal(probeCount, 1, 'replayed marker add should persist exactly once');

        const duplicateIdMatch = replayEvidence.stderrDelta.match(/DUPLICATE_EVENT_SKIPPED:\s*([^\s"]+:mark_add:[^\s"]+)/i);

        const invariants = await collectInvariantSnapshot(cli, transcript, 'final');
        assert.equal(invariants.INTEGRITY_OK, true, 'integrity should remain OK');
        assert.equal(invariants.LOCK_TIMEOUTS, 0, 'lock timeouts should remain zero');
        assert.equal(invariants.UNHANDLED_REJECTIONS, 0, 'unhandled rejections should remain zero');

        summary.storm = counts;
        summary.idempotencySpotCheck = {
          command: spotCommand,
          probeMarker,
          replayEventId: duplicateIdMatch ? duplicateIdMatch[1] : null,
          markerCountAfterReplay: probeCount
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
          snapshots: [{ name: 'wonky-storm', snapshot: finalSnapshot }]
        });
      }

      if (failure) throw failure;
    });
  }
);

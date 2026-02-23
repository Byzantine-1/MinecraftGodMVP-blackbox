'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');

const { publicRepoDir } = require('../lib/bbHarness');

function loadEngineModules() {
  const repoDir = publicRepoDir();
  const srcDir = path.join(repoDir, 'src');
  return {
    createMemoryStore: require(path.join(srcDir, 'memory.js')).createMemoryStore,
    createGodCommandService: require(path.join(srcDir, 'godCommands.js')).createGodCommandService
  };
}

const { createMemoryStore, createGodCommandService } = loadEngineModules();

function createStore() {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-bb-trader-'));
  const filePath = path.join(tmpDir, 'memory.json');
  return {
    filePath,
    memoryStore: createMemoryStore({ filePath })
  };
}

function createStoreAt(filePath) {
  return createMemoryStore({ filePath });
}

function createAgents() {
  return [
    { name: 'Mara', faction: 'Pilgrims', applyGodCommand: () => {} },
    { name: 'Eli', faction: 'Pilgrims', applyGodCommand: () => {} }
  ];
}

function snapshotHash(snapshot) {
  return crypto.createHash('sha256').update(JSON.stringify(snapshot)).digest('hex');
}

function parseOfferId(result) {
  const line = (result.outputLines || []).find((entry) => String(entry).includes('GOD OFFER ADDED:')) || '';
  const match = String(line).match(/id=([^\s]+)/i);
  return match ? match[1] : '';
}

function assertNoDuplicateIds(rows, label) {
  const seen = new Set();
  let duplicates = 0;
  for (const row of rows || []) {
    const id = String(row && row.id ? row.id : '');
    if (!id) continue;
    if (seen.has(id)) duplicates += 1;
    seen.add(id);
  }
  assert.equal(duplicates, 0, `${label} should not contain duplicate ids`);
}

function getContractsForDay(snapshot, day) {
  return (Array.isArray(snapshot?.world?.quests) ? snapshot.world.quests : [])
    .filter((quest) => quest && quest.meta && quest.meta.contract === true && Number(quest.meta.contract_day || 0) === Number(day));
}

function getOfferedTradeContract(snapshot, townName) {
  const quests = Array.isArray(snapshot?.world?.quests) ? snapshot.world.quests : [];
  return quests.find((quest) => (
    quest
    && quest.meta
    && quest.meta.contract === true
    && quest.type === 'trade_n'
    && quest.state === 'offered'
    && String(quest.town || '').toLowerCase() === String(townName || '').toLowerCase()
  )) || null;
}

function countByTown(contracts) {
  const map = new Map();
  for (const contract of contracts || []) {
    const town = String(contract && contract.town ? contract.town : '').toLowerCase();
    if (!town) continue;
    map.set(town, Number(map.get(town) || 0) + 1);
  }
  return map;
}

async function apply(service, agents, command, operationId) {
  return service.applyGodCommand({ agents, command, operationId });
}

async function seedTown(service, agents, townName, markerName, operationId) {
  const result = await apply(
    service,
    agents,
    `mark add ${markerName} 0 64 0 town:${townName}`,
    operationId
  );
  assert.equal(result.applied, true);
}

function extractPulseTownOrder(outputLines) {
  return (outputLines || [])
    .filter((line) => String(line).startsWith('GOD MARKET PULSE TOWN:'))
    .map((line) => {
      const match = String(line).match(/town=([^\s]+)/i);
      return match ? match[1] : '';
    })
    .filter(Boolean);
}

function projectDeterministicWorld(snapshot) {
  const world = snapshot?.world || {};
  const economy = world.economy || {};
  const ledger = economy.ledger || {};
  const markets = Array.isArray(world.markets) ? world.markets : [];
  const markers = Array.isArray(world.markers) ? world.markers : [];
  const quests = Array.isArray(world.quests) ? world.quests : [];
  const moodsByTown = (world.moods && typeof world.moods.byTown === 'object' && world.moods.byTown)
    ? world.moods.byTown
    : {};
  const threatByTown = (world.threat && typeof world.threat.byTown === 'object' && world.threat.byTown)
    ? world.threat.byTown
    : {};

  return {
    clock: {
      day: Number(world.clock && world.clock.day || 0),
      phase: String(world.clock && world.clock.phase || ''),
      season: String(world.clock && world.clock.season || '')
    },
    ledger: Object.entries(ledger)
      .map(([name, value]) => ({ name, balance: Number(value || 0) }))
      .sort((a, b) => a.name.localeCompare(b.name)),
    markers: markers
      .map((marker) => ({
        name: String(marker && marker.name || ''),
        x: Number(marker && marker.x || 0),
        y: Number(marker && marker.y || 0),
        z: Number(marker && marker.z || 0),
        tag: String(marker && marker.tag || '')
      }))
      .sort((a, b) => a.name.localeCompare(b.name)),
    markets: markets
      .map((market) => ({
        name: String(market && market.name || ''),
        marker: String(market && market.marker || ''),
        offers: (Array.isArray(market && market.offers) ? market.offers : [])
          .map((offer) => ({
            owner: String(offer && offer.owner || ''),
            side: String(offer && offer.side || ''),
            amount: Number(offer && offer.amount || 0),
            price: Number(offer && offer.price || 0),
            active: Boolean(offer && offer.active)
          }))
          .sort((a, b) => {
            const ownerDiff = a.owner.localeCompare(b.owner);
            if (ownerDiff !== 0) return ownerDiff;
            const sideDiff = a.side.localeCompare(b.side);
            if (sideDiff !== 0) return sideDiff;
            const priceDiff = a.price - b.price;
            if (priceDiff !== 0) return priceDiff;
            return a.amount - b.amount;
          })
      }))
      .sort((a, b) => a.name.localeCompare(b.name)),
    quests: quests
      .map((quest) => ({
        type: String(quest && quest.type || ''),
        state: String(quest && quest.state || ''),
        town: String(quest && quest.town || ''),
        owner: String(quest && quest.owner || ''),
        reward: Number(quest && quest.reward || 0),
        title: String(quest && quest.title || ''),
        objective: {
          kind: String(quest && quest.objective && quest.objective.kind || ''),
          n: Number(quest && quest.objective && quest.objective.n || 0),
          market: String(quest && quest.objective && quest.objective.market || ''),
          town: String(quest && quest.objective && quest.objective.town || '')
        },
        progress: {
          done: Number(quest && quest.progress && quest.progress.done || 0),
          visited: Boolean(quest && quest.progress && quest.progress.visited)
        },
        contract: Boolean(quest && quest.meta && quest.meta.contract),
        contractDay: Number(quest && quest.meta && quest.meta.contract_day || 0),
        contractKind: String(quest && quest.meta && quest.meta.kind || '')
      }))
      .sort((a, b) => {
        const typeDiff = a.type.localeCompare(b.type);
        if (typeDiff !== 0) return typeDiff;
        const townDiff = a.town.localeCompare(b.town);
        if (townDiff !== 0) return townDiff;
        const stateDiff = a.state.localeCompare(b.state);
        if (stateDiff !== 0) return stateDiff;
        const ownerDiff = a.owner.localeCompare(b.owner);
        if (ownerDiff !== 0) return ownerDiff;
        const rewardDiff = a.reward - b.reward;
        if (rewardDiff !== 0) return rewardDiff;
        return a.title.localeCompare(b.title);
      }),
    moodsByTown: Object.entries(moodsByTown)
      .map(([town, mood]) => ({
        town,
        fear: Number(mood && mood.fear || 0),
        unrest: Number(mood && mood.unrest || 0),
        prosperity: Number(mood && mood.prosperity || 0)
      }))
      .sort((a, b) => a.town.localeCompare(b.town)),
    threatByTown: Object.entries(threatByTown)
      .map(([town, value]) => ({ town, level: Number(value || 0) }))
      .sort((a, b) => a.town.localeCompare(b.town)),
    feed: {
      news: Number(Array.isArray(world.news) ? world.news.length : 0),
      chronicle: Number(Array.isArray(world.chronicle) ? world.chronicle.length : 0)
    }
  };
}

test('blackbox trader: Nightfall Decision Tombstone', async () => {
  const { memoryStore } = createStore();
  const service = createGodCommandService({ memoryStore });
  const agents = createAgents();

  await seedTown(service, agents, 'alpha', 'alpha_hall', 'bb-nightfall-seed-town');
  await apply(service, agents, 'event seed 777', 'bb-nightfall-seed-event');
  await memoryStore.transact((memory) => {
    memory.world.clock = { day: 2, phase: 'day', season: 'dawn', updated_at: '2026-02-22T00:00:00.000Z' };
    memory.world.decisions = [
      {
        id: 'd_legacy_alpha',
        town: 'alpha',
        event_id: 'e_legacy_alpha',
        event_type: 'festival',
        prompt: 'Legacy decision from old save.',
        options: [
          { key: 'ration', label: 'Ration', effects: { mood: { unrest: 1 } } },
          { key: 'import', label: 'Import', effects: { mood: { prosperity: 1 } } }
        ],
        state: 'open',
        starts_day: 1,
        expires_day: 1,
        created_at: 1000
      }
    ];
  }, { eventId: 'bb-nightfall-seed-legacy-decision' });

  const advance = await apply(service, agents, 'clock advance 1', 'bb-nightfall-advance-a');
  assert.equal(advance.applied, true);

  const afterAdvance = memoryStore.getSnapshot();
  const decision = afterAdvance.world.decisions.find((entry) => entry.id === 'd_legacy_alpha');
  assert.ok(decision);
  assert.equal(decision.state, 'expired');
  assert.equal(afterAdvance.world.decisions.filter((entry) => entry.state === 'open').length, 0);

  const list = await apply(service, agents, 'decision list alpha', 'bb-nightfall-decision-list');
  assert.equal(list.applied, true);
  assert.ok(list.outputLines.some((line) => String(line).includes('GOD DECISION DEPRECATED:')));
  assert.equal(list.outputLines.some((line) => /\bstate=open\b/i.test(String(line))), false);

  const replay = await apply(service, agents, 'clock advance 1', 'bb-nightfall-advance-a');
  assert.equal(replay.applied, false);
  assert.deepEqual(memoryStore.getSnapshot(), afterAdvance);
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true);
});

test('blackbox trader: Pulse Hash Purity', async () => {
  const { memoryStore } = createStore();
  const seedService = createGodCommandService({ memoryStore });
  const agents = createAgents();

  await seedTown(seedService, agents, 'alpha', 'alpha_hall', 'bb-pulse-purity-seed-town');
  await apply(seedService, agents, 'threat set alpha 63', 'bb-pulse-purity-seed-threat');
  await apply(seedService, agents, 'clock season long_night', 'bb-pulse-purity-seed-season');
  await apply(seedService, agents, 'event seed 777', 'bb-pulse-purity-seed-event');
  await apply(seedService, agents, 'event draw alpha', 'bb-pulse-purity-seed-event-draw');

  let txCalls = 0;
  const originalTransact = memoryStore.transact.bind(memoryStore);
  memoryStore.transact = async (...args) => {
    txCalls += 1;
    return originalTransact(...args);
  };
  const service = createGodCommandService({ memoryStore });

  const before = memoryStore.getSnapshot();
  const beforeHash = snapshotHash(before);

  const pulseTownA = await apply(service, agents, 'market pulse alpha', 'bb-pulse-purity-town-a');
  const pulseTownB = await apply(service, agents, 'market pulse alpha', 'bb-pulse-purity-town-b');
  const pulseWorld = await apply(service, agents, 'market pulse world', 'bb-pulse-purity-world-a');

  const after = memoryStore.getSnapshot();
  const afterHash = snapshotHash(after);

  assert.equal(pulseTownA.applied, true);
  assert.equal(pulseTownB.applied, true);
  assert.equal(pulseWorld.applied, true);
  assert.equal(txCalls, 0);
  assert.equal(afterHash, beforeHash);
  assert.deepEqual(pulseTownA.outputLines, pulseTownB.outputLines);
  assert.ok(pulseTownA.outputLines.some((line) => String(line).includes('GOD MARKET PULSE HOT:')));
  assert.ok(pulseTownA.outputLines.some((line) => String(line).includes('GOD MARKET PULSE COLD:')));
  assert.ok(pulseTownA.outputLines.some((line) => String(line).includes('GOD MARKET PULSE RISK:')));
  assert.ok(pulseWorld.outputLines.some((line) => String(line).includes('GOD MARKET PULSE WORLD:')));
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true);
});

test('blackbox trader: Pulse Order Stability', async () => {
  const { memoryStore } = createStore();
  const seedService = createGodCommandService({ memoryStore });
  const agents = createAgents();

  await seedTown(seedService, agents, 'gamma', 'gamma_hall', 'bb-pulse-order-seed-gamma');
  await seedTown(seedService, agents, 'alpha', 'alpha_hall', 'bb-pulse-order-seed-alpha');
  await seedTown(seedService, agents, 'beta', 'beta_hall', 'bb-pulse-order-seed-beta');

  let txCalls = 0;
  const originalTransact = memoryStore.transact.bind(memoryStore);
  memoryStore.transact = async (...args) => {
    txCalls += 1;
    return originalTransact(...args);
  };
  const service = createGodCommandService({ memoryStore });

  const beforeHash = snapshotHash(memoryStore.getSnapshot());
  const pulseA = await apply(service, agents, 'market pulse world', 'bb-pulse-order-world-a');
  const pulseB = await apply(service, agents, 'market pulse world', 'bb-pulse-order-world-b');
  const pulseC = await apply(service, agents, 'market pulse world', 'bb-pulse-order-world-c');
  const afterHash = snapshotHash(memoryStore.getSnapshot());

  const orderA = extractPulseTownOrder(pulseA.outputLines);
  const orderB = extractPulseTownOrder(pulseB.outputLines);
  const orderC = extractPulseTownOrder(pulseC.outputLines);
  const sortedA = [...orderA].sort((a, b) => a.localeCompare(b));

  assert.equal(pulseA.applied, true);
  assert.equal(pulseB.applied, true);
  assert.equal(pulseC.applied, true);
  assert.equal(txCalls, 0);
  assert.deepEqual(orderA, sortedA);
  assert.deepEqual(orderB, orderA);
  assert.deepEqual(orderC, orderA);
  assert.deepEqual(orderA, ['alpha', 'beta', 'gamma']);
  assert.equal(afterHash, beforeHash);
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true);
});

test('blackbox trader: Contract Day Boundary', async () => {
  const { memoryStore } = createStore();
  const service = createGodCommandService({ memoryStore });
  const agents = createAgents();

  await seedTown(service, agents, 'alpha', 'alpha_hall', 'bb-contract-day-seed-alpha');
  await seedTown(service, agents, 'beta', 'beta_hall', 'bb-contract-day-seed-beta');

  const day2Advance = await apply(service, agents, 'clock advance 2', 'bb-contract-day2');
  assert.equal(day2Advance.applied, true);
  const day2Snapshot = memoryStore.getSnapshot();
  const day2Contracts = getContractsForDay(day2Snapshot, 2);
  assert.ok(day2Contracts.length >= 2);
  assert.ok(day2Contracts.length <= 4);
  const day2ByTown = countByTown(day2Contracts);
  assert.ok(day2ByTown.has('alpha'));
  assert.ok(day2ByTown.has('beta'));
  assert.ok(Number(day2ByTown.get('alpha')) >= 1 && Number(day2ByTown.get('alpha')) <= 2);
  assert.ok(Number(day2ByTown.get('beta')) >= 1 && Number(day2ByTown.get('beta')) <= 2);

  const day2Replay = await apply(service, agents, 'clock advance 2', 'bb-contract-day2');
  assert.equal(day2Replay.applied, false);
  assert.deepEqual(memoryStore.getSnapshot(), day2Snapshot);

  const toNight = await apply(service, agents, 'clock advance 1', 'bb-contract-night2');
  assert.equal(toNight.applied, true);
  const afterNight = memoryStore.getSnapshot();
  assert.equal(getContractsForDay(afterNight, 2).length, day2Contracts.length);
  assert.equal(getContractsForDay(afterNight, 3).length, 0);

  const toDay3 = await apply(service, agents, 'clock advance 1', 'bb-contract-day3');
  assert.equal(toDay3.applied, true);
  const day3Snapshot = memoryStore.getSnapshot();
  const day3Contracts = getContractsForDay(day3Snapshot, 3);
  assert.ok(day3Contracts.length >= 2);
  assert.ok(day3Contracts.length <= 4);
  const day3ByTown = countByTown(day3Contracts);
  assert.ok(day3ByTown.has('alpha'));
  assert.ok(day3ByTown.has('beta'));
  assert.ok(Number(day3ByTown.get('alpha')) >= 1 && Number(day3ByTown.get('alpha')) <= 2);
  assert.ok(Number(day3ByTown.get('beta')) >= 1 && Number(day3ByTown.get('beta')) <= 2);

  assert.equal(memoryStore.validateMemoryIntegrity().ok, true);
});

test('blackbox trader: Contract Replay Immunity', async () => {
  const { memoryStore } = createStore();
  const service = createGodCommandService({ memoryStore });
  const agents = createAgents();

  await seedTown(service, agents, 'alpha', 'alpha_hall', 'bb-contract-replay-seed-alpha');
  await apply(service, agents, 'clock advance 2', 'bb-contract-replay-generate');

  const offered = getOfferedTradeContract(memoryStore.getSnapshot(), 'alpha');
  assert.ok(offered);
  const contractId = offered.id;

  const acceptA = await apply(service, agents, `contract accept Mara ${contractId}`, 'bb-contract-replay-accept-a');
  const acceptReplay = await apply(service, agents, `contract accept Mara ${contractId}`, 'bb-contract-replay-accept-a');
  assert.equal(acceptA.applied, true);
  assert.equal(acceptReplay.applied, false);

  await memoryStore.transact((memory) => {
    const idx = memory.world.quests.findIndex((quest) => quest.id === contractId);
    assert.ok(idx >= 0);
    const quest = memory.world.quests[idx];
    quest.state = 'in_progress';
    if (quest.type === 'trade_n') {
      quest.progress = { done: Number(quest.objective && quest.objective.n || 1) };
    } else {
      quest.progress = { visited: true };
    }
    memory.world.quests[idx] = quest;
  }, { eventId: 'bb-contract-replay-seed-progress' });

  const maraBefore = Number(memoryStore.getSnapshot().world.economy.ledger.Mara || 0);
  const reward = Number((memoryStore.getSnapshot().world.quests.find((quest) => quest.id === contractId) || {}).reward || 0);

  const completeA = await apply(service, agents, `contract complete ${contractId}`, 'bb-contract-replay-complete-a');
  const completeReplay = await apply(service, agents, `contract complete ${contractId}`, 'bb-contract-replay-complete-a');
  const completeNewId = await apply(service, agents, `contract complete ${contractId}`, 'bb-contract-replay-complete-b');

  assert.equal(completeA.applied, true);
  assert.equal(completeReplay.applied, false);
  assert.equal(completeNewId.applied, false);

  const snapshot = memoryStore.getSnapshot();
  const maraAfter = Number(snapshot.world.economy.ledger.Mara || 0);
  assert.equal(maraAfter, maraBefore + reward);

  const questCompletionsChronicle = snapshot.world.chronicle
    .filter((entry) => entry.type === 'quest_complete' && entry.meta && entry.meta.quest_id === contractId);
  const questCompletionsNews = snapshot.world.news
    .filter((entry) => entry.topic === 'quest' && entry.meta && entry.meta.quest_id === contractId && String(entry.msg).includes('completed'));
  assert.equal(questCompletionsChronicle.length, 1);
  assert.equal(questCompletionsNews.length, 1);
  assertNoDuplicateIds(snapshot.world.news, 'news');
  assertNoDuplicateIds(snapshot.world.chronicle, 'chronicle');
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true);
});

test('blackbox trader: Accept Race (Two Agents)', async () => {
  const { memoryStore } = createStore();
  const service = createGodCommandService({ memoryStore });
  const agents = createAgents();

  await seedTown(service, agents, 'alpha', 'alpha_hall', 'bb-contract-race-seed-alpha');
  await apply(service, agents, 'clock advance 2', 'bb-contract-race-generate');

  const offered = getOfferedTradeContract(memoryStore.getSnapshot(), 'alpha');
  assert.ok(offered);
  const contractId = offered.id;

  const [maraAttempt, eliAttempt] = await Promise.all([
    apply(service, agents, `contract accept Mara ${contractId}`, 'bb-contract-race-mara'),
    apply(service, agents, `contract accept Eli ${contractId}`, 'bb-contract-race-eli')
  ]);

  const appliedCount = Number(maraAttempt.applied) + Number(eliAttempt.applied);
  assert.equal(appliedCount, 1);

  const snapshot = memoryStore.getSnapshot();
  const quest = snapshot.world.quests.find((entry) => entry.id === contractId);
  assert.ok(quest);
  assert.ok(quest.owner === 'Mara' || quest.owner === 'Eli');
  assert.ok(quest.state === 'accepted' || quest.state === 'in_progress');

  const acceptChronicle = snapshot.world.chronicle
    .filter((entry) => entry.type === 'quest_accept' && entry.meta && entry.meta.quest_id === contractId);
  const acceptNews = snapshot.world.news
    .filter((entry) => entry.topic === 'quest' && entry.meta && entry.meta.quest_id === contractId && String(entry.msg).includes('accepted'));
  assert.equal(acceptChronicle.length, 1);
  assert.equal(acceptNews.length, 1);
  assertNoDuplicateIds(snapshot.world.news, 'news');
  assertNoDuplicateIds(snapshot.world.chronicle, 'chronicle');
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true);
});

test('blackbox trader: Progress Ownership Integrity', async () => {
  const { memoryStore } = createStore();
  const service = createGodCommandService({ memoryStore });
  const agents = createAgents();

  await seedTown(service, agents, 'alpha', 'alpha_hall', 'bb-progress-owner-seed-town');
  await apply(service, agents, 'market add bazaar alpha_hall', 'bb-progress-owner-seed-market');
  await apply(service, agents, 'mint Mara 30', 'bb-progress-owner-seed-mara');

  await apply(service, agents, 'quest offer alpha trade_n 2 bazaar 5', 'bb-progress-owner-offer-buyer');
  const buyerQuestId = memoryStore.getSnapshot().world.quests[0].id;
  await apply(service, agents, `quest accept Mara ${buyerQuestId}`, 'bb-progress-owner-accept-buyer');

  await apply(service, agents, 'quest offer alpha trade_n 2 bazaar 9', 'bb-progress-owner-offer-seller');
  const sellerQuestId = memoryStore.getSnapshot().world.quests[1].id;
  await apply(service, agents, `quest accept Eli ${sellerQuestId}`, 'bb-progress-owner-accept-seller');

  await apply(service, agents, 'offer add bazaar Eli sell 5 2', 'bb-progress-owner-seed-offer');
  const offerId = memoryStore.getSnapshot().world.markets[0].offers[0].offer_id;

  const tradeA = await apply(service, agents, `trade bazaar ${offerId} Mara 1`, 'bb-progress-owner-trade-a');
  const tradeAReplay = await apply(service, agents, `trade bazaar ${offerId} Mara 1`, 'bb-progress-owner-trade-a');
  assert.equal(tradeA.applied, true);
  assert.equal(tradeAReplay.applied, false);

  const snapshotAfterA = memoryStore.getSnapshot();
  const buyerAfterA = snapshotAfterA.world.quests.find((entry) => entry.id === buyerQuestId);
  const sellerAfterA = snapshotAfterA.world.quests.find((entry) => entry.id === sellerQuestId);
  assert.equal(Number(buyerAfterA.progress.done || 0), 1);
  assert.equal(buyerAfterA.state, 'in_progress');
  assert.equal(Number(sellerAfterA.progress.done || 0), 0);
  assert.equal(sellerAfterA.state, 'accepted');

  const tradeB = await apply(service, agents, `trade bazaar ${offerId} Mara 1`, 'bb-progress-owner-trade-b');
  const tradeBReplay = await apply(service, agents, `trade bazaar ${offerId} Mara 1`, 'bb-progress-owner-trade-b');
  assert.equal(tradeB.applied, true);
  assert.equal(tradeBReplay.applied, false);

  const snapshot = memoryStore.getSnapshot();
  const buyerQuest = snapshot.world.quests.find((entry) => entry.id === buyerQuestId);
  const sellerQuest = snapshot.world.quests.find((entry) => entry.id === sellerQuestId);
  assert.equal(Number(buyerQuest.progress.done || 0), 2);
  assert.equal(buyerQuest.state, 'completed');
  assert.equal(Number(sellerQuest.progress.done || 0), 0);
  assert.equal(sellerQuest.state, 'accepted');
  assert.equal(Number(snapshot.world.economy.ledger.Mara || 0), 31);

  const buyerCompletions = snapshot.world.chronicle
    .filter((entry) => entry.type === 'quest_complete' && entry.meta && entry.meta.quest_id === buyerQuestId);
  assert.equal(buyerCompletions.length, 1);
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true);
});

test('blackbox trader: Wrong-Market Non-Progress', async () => {
  const { memoryStore } = createStore();
  const service = createGodCommandService({ memoryStore });
  const agents = createAgents();

  await seedTown(service, agents, 'alpha', 'alpha_hall', 'bb-wrong-market-seed-town');
  await apply(service, agents, 'market add bazaar alpha_hall', 'bb-wrong-market-seed-bazaar');
  await apply(service, agents, 'market add sidemarket alpha_hall', 'bb-wrong-market-seed-side');
  await apply(service, agents, 'mint Mara 20', 'bb-wrong-market-seed-mara');
  await apply(service, agents, 'quest offer alpha trade_n 1 bazaar 4', 'bb-wrong-market-quest-offer');

  const questId = memoryStore.getSnapshot().world.quests[0].id;
  await apply(service, agents, `quest accept Mara ${questId}`, 'bb-wrong-market-quest-accept');

  const offerResult = await apply(service, agents, 'offer add sidemarket Eli sell 2 1', 'bb-wrong-market-offer-side');
  assert.equal(offerResult.applied, true);
  const sideOfferId = parseOfferId(offerResult);
  assert.ok(sideOfferId);

  const wrongTrade = await apply(service, agents, `trade sidemarket ${sideOfferId} Mara 1`, 'bb-wrong-market-trade');
  assert.equal(wrongTrade.applied, true);

  const snapshotAfterWrong = memoryStore.getSnapshot();
  const questAfterWrong = snapshotAfterWrong.world.quests.find((entry) => entry.id === questId);
  assert.equal(Number(questAfterWrong.progress.done || 0), 0);
  assert.equal(questAfterWrong.state, 'accepted');
  assert.equal(Number(snapshotAfterWrong.world.economy.ledger.Mara || 0), 19);

  const manualComplete = await apply(service, agents, `quest complete ${questId}`, 'bb-wrong-market-manual-complete');
  assert.equal(manualComplete.applied, false);

  const completions = memoryStore.getSnapshot().world.chronicle
    .filter((entry) => entry.type === 'quest_complete' && entry.meta && entry.meta.quest_id === questId);
  assert.equal(completions.length, 0);
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true);
});

test('blackbox trader: Auto-Complete vs Manual Complete (pay once)', async () => {
  const { memoryStore } = createStore();
  const service = createGodCommandService({ memoryStore });
  const agents = createAgents();

  await seedTown(service, agents, 'alpha', 'alpha_hall', 'bb-auto-vs-manual-seed-town');
  await apply(service, agents, 'market add bazaar alpha_hall', 'bb-auto-vs-manual-seed-market');
  await apply(service, agents, 'mint Mara 40', 'bb-auto-vs-manual-seed-mara');
  await apply(service, agents, 'mint Eli 40', 'bb-auto-vs-manual-seed-eli');

  const offer = await apply(service, agents, 'offer add bazaar Eli sell 8 1', 'bb-auto-vs-manual-seed-offer');
  assert.equal(offer.applied, true);
  const offerId = parseOfferId(offer);
  assert.ok(offerId);

  await apply(service, agents, 'clock advance 2', 'bb-auto-vs-manual-generate-contracts');
  const contract = getOfferedTradeContract(memoryStore.getSnapshot(), 'alpha');
  assert.ok(contract);
  const contractId = contract.id;
  const tradeTarget = Math.max(1, Number(contract.objective && contract.objective.n || 1));

  const accepted = await apply(service, agents, `contract accept Mara ${contractId}`, 'bb-auto-vs-manual-accept');
  assert.equal(accepted.applied, true);
  const maraBeforeTrades = Number(memoryStore.getSnapshot().world.economy.ledger.Mara || 0);

  for (let idx = 0; idx < tradeTarget; idx += 1) {
    const trade = await apply(
      service,
      agents,
      `trade bazaar ${offerId} Mara 1`,
      `bb-auto-vs-manual-trade-${idx + 1}`
    );
    assert.equal(trade.applied, true);
  }

  const afterAutoComplete = memoryStore.getSnapshot();
  const completed = afterAutoComplete.world.quests.find((quest) => quest.id === contractId);
  assert.ok(completed);
  assert.equal(completed.state, 'completed');
  const maraAfterAutoComplete = Number(afterAutoComplete.world.economy.ledger.Mara || 0);
  const expectedBalance = maraBeforeTrades - tradeTarget + Number(completed.reward || 0);
  assert.equal(maraAfterAutoComplete, expectedBalance);

  const manualContractComplete = await apply(service, agents, `contract complete ${contractId}`, 'bb-auto-vs-manual-contract-complete');
  const manualQuestComplete = await apply(service, agents, `quest complete ${contractId}`, 'bb-auto-vs-manual-quest-complete');
  assert.equal(manualContractComplete.applied, false);
  assert.equal(manualQuestComplete.applied, false);

  const finalSnapshot = memoryStore.getSnapshot();
  assert.equal(Number(finalSnapshot.world.economy.ledger.Mara || 0), maraAfterAutoComplete);
  const completionNews = finalSnapshot.world.news
    .filter((entry) => entry.topic === 'quest' && entry.meta && entry.meta.quest_id === contractId && String(entry.msg).includes('completed'));
  assert.equal(completionNews.length, 1, 'no duplicate payouts/news should occur');
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true);
});

test('blackbox trader: News/Chronicle Duplicate Trap', async () => {
  const { memoryStore } = createStore();
  const service = createGodCommandService({ memoryStore });
  const agents = createAgents();

  const steps = [
    { command: 'mark add alpha_hall 0 64 0 town:alpha', operationId: 'bb-dup-trap-town' },
    { command: 'market add bazaar alpha_hall', operationId: 'bb-dup-trap-market' },
    { command: 'mint Mara 10', operationId: 'bb-dup-trap-mint-mara' },
    { command: 'mint Eli 10', operationId: 'bb-dup-trap-mint-eli' },
    { command: 'offer add bazaar Eli sell 2 2', operationId: 'bb-dup-trap-offer' }
  ];

  for (const step of steps) {
    const first = await apply(service, agents, step.command, step.operationId);
    const replay = await apply(service, agents, step.command, step.operationId);
    assert.equal(first.applied, true);
    assert.equal(replay.applied, false);
  }

  const offerId = memoryStore.getSnapshot().world.markets[0].offers[0].offer_id;
  const tradeA = await apply(service, agents, `trade bazaar ${offerId} Mara 1`, 'bb-dup-trap-trade');
  const tradeReplay = await apply(service, agents, `trade bazaar ${offerId} Mara 1`, 'bb-dup-trap-trade');
  assert.equal(tradeA.applied, true);
  assert.equal(tradeReplay.applied, false);

  const snapshot = memoryStore.getSnapshot();
  assertNoDuplicateIds(snapshot.world.news, 'news');
  assertNoDuplicateIds(snapshot.world.chronicle, 'chronicle');
  const tradeNews = snapshot.world.news.filter((entry) => String(entry.id || '').includes(':news:trade:'));
  const tradeChronicle = snapshot.world.chronicle.filter((entry) => String(entry.id || '').includes(':chronicle:trade:'));
  assert.equal(tradeNews.length, 1);
  assert.equal(tradeChronicle.length, 1);
  assert.ok(snapshot.world.news.length <= 200);
  assert.ok(snapshot.world.chronicle.length <= 200);
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true);
});

test('blackbox trader: Town Board Read-Only Hammer', async () => {
  const { memoryStore } = createStore();
  const seedService = createGodCommandService({ memoryStore });
  const agents = createAgents();

  await seedTown(seedService, agents, 'alpha', 'alpha_hall', 'bb-board-hammer-seed-town');
  await apply(seedService, agents, 'market add bazaar alpha_hall', 'bb-board-hammer-seed-market');
  await apply(seedService, agents, 'offer add bazaar Eli sell 2 2', 'bb-board-hammer-seed-offer');
  await apply(seedService, agents, 'clock advance 2', 'bb-board-hammer-generate-contracts');

  let txCalls = 0;
  const originalTransact = memoryStore.transact.bind(memoryStore);
  memoryStore.transact = async (...args) => {
    txCalls += 1;
    return originalTransact(...args);
  };
  const service = createGodCommandService({ memoryStore });

  const before = memoryStore.getSnapshot();
  const beforeHash = snapshotHash(before);
  let boardOutput = [];

  for (let idx = 0; idx < 20; idx += 1) {
    const townBoard = await apply(service, agents, 'town board alpha 20', `bb-board-hammer-town-${idx}`);
    const contractList = await apply(service, agents, 'contract list alpha', `bb-board-hammer-contract-${idx}`);
    const pulseTown = await apply(service, agents, 'market pulse alpha', `bb-board-hammer-pulse-${idx}`);
    const newsTail = await apply(service, agents, 'news tail 20', `bb-board-hammer-news-${idx}`);
    const chronicleTail = await apply(service, agents, 'chronicle tail 20', `bb-board-hammer-chronicle-${idx}`);
    const decisionList = await apply(service, agents, 'decision list alpha', `bb-board-hammer-decision-${idx}`);

    assert.equal(townBoard.applied, true);
    assert.equal(contractList.applied, true);
    assert.equal(pulseTown.applied, true);
    assert.equal(newsTail.applied, true);
    assert.equal(chronicleTail.applied, true);
    assert.equal(decisionList.applied, true);
    if (idx === 0) boardOutput = townBoard.outputLines || [];
  }

  const after = memoryStore.getSnapshot();
  const afterHash = snapshotHash(after);
  assert.equal(txCalls, 0, 'read-only hammer should not transact');
  assert.equal(afterHash, beforeHash, 'read-only hammer should not mutate state');
  assert.ok(boardOutput.some((line) => String(line).includes('GOD TOWN BOARD CONTRACTS AVAILABLE:')));
  assert.ok(boardOutput.some((line) => String(line).includes('GOD TOWN BOARD CONTRACTS ACTIVE:')));
  assert.ok(boardOutput.some((line) => String(line).includes('GOD TOWN BOARD MARKET PULSE HOT:')));
  assert.equal(boardOutput.some((line) => /OPEN DECISION/i.test(String(line))), false);
  assert.ok(after.world.news.length <= 200);
  assert.ok(after.world.chronicle.length <= 200);
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true);
});

test('blackbox trader: Parser Garbage Storm', async () => {
  const { memoryStore } = createStore();
  const service = createGodCommandService({ memoryStore });
  const agents = createAgents();

  await seedTown(service, agents, 'alpha', 'alpha_hall', 'bb-garbage-seed-town');
  await apply(service, agents, 'market add bazaar alpha_hall', 'bb-garbage-seed-market');

  const before = memoryStore.getSnapshot();
  const beforeHash = snapshotHash(before);

  const garbage = [
    '',
    '   ',
    '???',
    'market',
    'market pulse',
    'market pulse alpha extra',
    'contract',
    'contract accept',
    'contract accept Mara',
    'contract complete',
    'quest',
    'quest offer alpha trade_n nope',
    'quest offer',
    'quest complete',
    'offer add bazaar Mara sell two five',
    'offer cancel',
    'town board',
    'clock advance 0',
    'clock season winter',
    'trade',
    'trade bazaar',
    'rep add Mara iron_pact 1.5',
    'decision choose',
    'news tail 0',
    'chronicle tail -1',
    'god mode',
    'faction set',
    'threat set alpha nan'
  ];

  for (let idx = 0; idx < 140; idx += 1) {
    const raw = garbage[idx % garbage.length];
    const cmd = `${raw} __garbage_${idx}`.trim();
    try {
      const result = await apply(service, agents, cmd, `bb-garbage-${idx}`);
      assert.equal(result.applied, false);
    } catch (error) {
      assert.equal(error && error.code, 'INVALID_GOD_COMMAND');
    }
  }

  const after = memoryStore.getSnapshot();
  const afterHash = snapshotHash(after);
  assert.equal(afterHash, beforeHash);
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true);
});

test('blackbox trader: Soak with Duplicate IDs', async () => {
  const { memoryStore } = createStore();
  const service = createGodCommandService({ memoryStore });
  const agents = createAgents();

  await seedTown(service, agents, 'alpha', 'alpha_hall', 'bb-soak-dup-seed-town');

  const before = memoryStore.getSnapshot();
  const baselineMara = Number(before.world.economy.ledger.Mara || 0);

  const uniqueGroups = 48;
  for (let group = 0; group < uniqueGroups; group += 1) {
    const operationId = `bb-soak-dup-${group}`;
    const command = group % 2 === 0
      ? 'mint Mara 1'
      : `mark add soak_${group} ${group} 64 ${-group} soak`;

    for (let repeat = 0; repeat < 5; repeat += 1) {
      const result = await apply(service, agents, command, operationId);
      if (repeat === 0) assert.equal(result.applied, true);
      else assert.equal(result.applied, false);
    }
  }

  const snapshot = memoryStore.getSnapshot();
  const expectedMaraGain = uniqueGroups / 2;
  assert.equal(Number(snapshot.world.economy.ledger.Mara || 0), baselineMara + expectedMaraGain);

  const markers = Array.isArray(snapshot.world.markers) ? snapshot.world.markers : [];
  const soakMarkers = markers.filter((entry) => String(entry && entry.name || '').startsWith('soak_'));
  assert.equal(soakMarkers.length, uniqueGroups / 2);
  assert.equal(new Set(soakMarkers.map((entry) => entry.name)).size, uniqueGroups / 2);

  assertNoDuplicateIds(snapshot.world.news, 'news');
  assertNoDuplicateIds(snapshot.world.chronicle, 'chronicle');
  assert.ok(snapshot.world.news.length <= 200, 'feed bounds should be respected for news');
  assert.ok(snapshot.world.chronicle.length <= 200, 'feed bounds should be respected for chronicle');

  const runtime = memoryStore.getRuntimeMetrics();
  assert.equal(Number(runtime.lockTimeouts || 0), 0);
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true);
});

async function runRestartScenario(filePath, { restartAtStep = null } = {}) {
  const agents = createAgents();
  let memoryStore = createStoreAt(filePath);
  let service = createGodCommandService({ memoryStore });

  const state = {
    offerId: '',
    questId: ''
  };

  const steps = [
    { command: 'mark add alpha_hall 0 64 0 town:alpha', operationId: 'bb-restart-step-01' },
    { command: 'mark add beta_hall 20 64 0 town:beta', operationId: 'bb-restart-step-02' },
    { command: 'market add bazaar alpha_hall', operationId: 'bb-restart-step-03' },
    { command: 'mint Mara 50', operationId: 'bb-restart-step-04' },
    { command: 'mint Eli 50', operationId: 'bb-restart-step-05' },
    { command: 'offer add bazaar Eli sell 6 2', operationId: 'bb-restart-step-06' },
    { command: () => 'quest offer alpha trade_n 2 bazaar 4', operationId: 'bb-restart-step-07' },
    { command: () => `quest accept Mara ${state.questId}`, operationId: 'bb-restart-step-08' },
    { command: () => `trade bazaar ${state.offerId} Mara 1`, operationId: 'bb-restart-step-09' },
    { command: () => `trade bazaar ${state.offerId} Mara 1`, operationId: 'bb-restart-step-10' },
    { command: 'clock advance 2', operationId: 'bb-restart-step-11' },
    { command: 'contract list alpha', operationId: 'bb-restart-step-12' },
    { command: 'market pulse world', operationId: 'bb-restart-step-13' },
    { command: 'town board alpha 10', operationId: 'bb-restart-step-14' }
  ];

  for (let idx = 0; idx < steps.length; idx += 1) {
    const step = steps[idx];
    const command = typeof step.command === 'function' ? step.command() : step.command;
    const result = await apply(service, agents, command, step.operationId);
    assert.equal(result.applied, true);

    if (step.operationId === 'bb-restart-step-06') {
      state.offerId = parseOfferId(result);
      assert.ok(state.offerId);
    }
    if (step.operationId === 'bb-restart-step-07') {
      const quests = memoryStore.getSnapshot().world.quests;
      state.questId = quests[quests.length - 1].id;
      assert.ok(state.questId);
    }

    if (restartAtStep !== null && idx === restartAtStep) {
      memoryStore = createStoreAt(filePath);
      service = createGodCommandService({ memoryStore });
      if (step.operationId === 'bb-restart-step-06') {
        const replayAfterRestart = await apply(service, agents, command, step.operationId);
        assert.equal(replayAfterRestart.applied, false);
      }
    }
  }

  const snapshot = memoryStore.getSnapshot();
  return {
    snapshot,
    projected: projectDeterministicWorld(snapshot),
    integrity: memoryStore.validateMemoryIntegrity()
  };
}

test('blackbox trader: Restart Determinism', async () => {
  const baseDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-bb-restart-det-'));
  const baselineFile = path.join(baseDir, 'baseline-memory.json');
  const resumedFile = path.join(baseDir, 'resumed-memory.json');

  const baseline = await runRestartScenario(baselineFile, { restartAtStep: null });
  const resumed = await runRestartScenario(resumedFile, { restartAtStep: 5 });

  assert.deepEqual(resumed.projected, baseline.projected);
  assert.equal(baseline.integrity.ok, true);
  assert.equal(resumed.integrity.ok, true);
  assertNoDuplicateIds(baseline.snapshot.world.news, 'news');
  assertNoDuplicateIds(baseline.snapshot.world.chronicle, 'chronicle');
  assertNoDuplicateIds(resumed.snapshot.world.news, 'news');
  assertNoDuplicateIds(resumed.snapshot.world.chronicle, 'chronicle');
});

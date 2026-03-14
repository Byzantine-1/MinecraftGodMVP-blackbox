const fs = require('node:fs')
const assert = require('node:assert/strict')
const test = require('node:test')

const {
  buildExecutionHandoff,
  buildWorldMemoryRequest,
  createSeededTempState,
  spawnEngine,
} = require('../harness/engineHarness')

function createManagedFixture(t) {
  const state = createSeededTempState()
  const engines = []

  t.after(async () => {
    for (const engine of engines.reverse()) {
      try {
        await engine.kill({ cleanup: false })
      } catch {}
    }
    state.cleanup()
  })

  return {
    state,
    spawn(options = {}) {
      const engine = spawnEngine({
        state,
        backend: 'sqlite',
        ...options,
      })
      engines.push(engine)
      return engine
    },
  }
}

function assertCanonicalWorldMemoryContext(response, request) {
  assert.equal(response.type, 'world-memory-context.v1')
  assert.equal(response.schemaVersion, 1)
  assert.deepEqual(response.scope, request.scope)
  assert(Array.isArray(response.recentChronicle))
  assert(Array.isArray(response.recentHistory))
  assert(response.recentChronicle.length <= request.scope.chronicleLimit)
  assert(response.recentHistory.length <= request.scope.historyLimit)
}

function assertCanonicalExecutionResult(result, handoff) {
  assert.equal(result.type, 'execution-result.v1')
  assert.equal(result.schemaVersion, 1)
  assert.equal(result.handoffId, handoff.handoffId)
  assert.equal(result.proposalId, handoff.proposalId)
  assert.equal(result.idempotencyKey, handoff.idempotencyKey)
  assert.equal(result.command, handoff.command)
  assert.equal(result.snapshotHash, handoff.snapshotHash)
  assert.match(result.executionId, /^result_[0-9a-f]{64}$/)
  assert(Array.isArray(result.authorityCommands))
  assert.equal(result.authorityCommands.length, 1)
  assert.equal(result.authorityCommands[0], handoff.command)
  assert.match(result.worldState.postExecutionSnapshotHash, /^[0-9a-f]{64}$/)
  assert.match(result.evaluation.staleCheck.actualSnapshotHash, /^[0-9a-f]{64}$/)
}

function countHistoryEntries(response, predicate) {
  return response.recentHistory.filter(predicate).length
}

function pickComparableWorldView(response) {
  return {
    scope: response.scope,
    recentHistory: response.recentHistory,
    townSummary: response.townSummary,
    factionSummary: response.factionSummary,
    townIdentity: response.townIdentity,
    keyActors: response.keyActors,
  }
}

test('T1 golden retrieval (sqlite) returns canonical world-memory-context.v1', async (t) => {
  const fixture = createManagedFixture(t)
  const engine = fixture.spawn()
  const request = buildWorldMemoryRequest({
    townId: 'alpha',
    factionId: 'iron_pact',
    chronicleLimit: 2,
    historyLimit: 3,
  })

  await engine.waitForReady()
  engine.sendLine(request)

  const response = await engine.readNextJsonOfType('world-memory-context.v1')
  assertCanonicalWorldMemoryContext(response, request)
  assert.equal(response.townIdentity.townId, 'alpha')
  assert.equal(response.townIdentity.name, 'Alpha')
  assert.equal(response.factionSummary.factionId, 'iron_pact')
  assert.equal(response.townSummary.townId, 'alpha')
  assert.equal(response.townSummary.historyCount, 0)
  assert(response.recentChronicle.length <= 2)
  assert.equal(response.recentHistory.length, 0)
  assert(Array.isArray(response.keyActors))
  assert(response.keyActors.length >= 4)
  assert(response.keyActors.length <= 6)
  assert.deepEqual(
    response.keyActors.map((entry) => entry.actorId).sort(),
    ['alpha.captain', 'alpha.mayor', 'alpha.townsfolk', 'alpha.warden'],
  )

  await engine.shutdown({ cleanup: false })
})

test('T2 golden execution (sqlite) returns canonical execution-result.v1 and persists sqlite artifacts', async (t) => {
  const fixture = createManagedFixture(t)
  const engine = fixture.spawn()
  const handoff = buildExecutionHandoff({
    snapshotHash: fixture.state.snapshotHash,
    decisionEpoch: fixture.state.decisionEpoch,
    projectId: fixture.state.projectId,
  })

  await engine.waitForReady()
  engine.sendLine(handoff)

  const result = await engine.readNextJsonOfType('execution-result.v1')
  assertCanonicalExecutionResult(result, handoff)
  assert.equal(result.status, 'executed')
  assert.equal(result.accepted, true)
  assert.equal(result.executed, true)
  assert.equal(result.reasonCode, 'EXECUTED')

  await engine.shutdown({ cleanup: false })

  assert.equal(fs.existsSync(fixture.state.sqlitePath), true)
  assert(fs.statSync(fixture.state.sqlitePath).size > 0)
})

test('T3 mixed session coherence (sqlite) shows a stable retrieval -> execution -> retrieval delta', async (t) => {
  const fixture = createManagedFixture(t)
  const engine = fixture.spawn()
  const request = buildWorldMemoryRequest({
    townId: 'alpha',
    factionId: 'iron_pact',
    chronicleLimit: 3,
    historyLimit: 3,
  })
  const handoff = buildExecutionHandoff({
    snapshotHash: fixture.state.snapshotHash,
    decisionEpoch: fixture.state.decisionEpoch,
    projectId: fixture.state.projectId,
  })

  await engine.waitForReady()
  engine.sendLine(request)
  const pre = await engine.readNextJsonOfType('world-memory-context.v1')

  engine.sendLine(handoff)
  const result = await engine.readNextJsonOfType('execution-result.v1')

  engine.sendLine(request)
  const post = await engine.readNextJsonOfType('world-memory-context.v1')

  assertCanonicalWorldMemoryContext(pre, request)
  assertCanonicalWorldMemoryContext(post, request)
  assertCanonicalExecutionResult(result, handoff)
  assert.equal(pre.townSummary.historyCount, 0)
  assert.equal(pre.townSummary.executionCounts.executed, 0)
  assert(post.townSummary.historyCount > pre.townSummary.historyCount)
  assert(post.factionSummary.historyCount > pre.factionSummary.historyCount)
  assert(post.townSummary.executionCounts.executed > pre.townSummary.executionCounts.executed)
  assert(
    post.recentHistory.some((entry) => (
      entry.handoffId === handoff.handoffId
      && entry.proposalType === 'PROJECT_ADVANCE'
      && entry.status === 'executed'
    )),
    'post-execution recentHistory should include the canonical handoff result',
  )
  assert.notDeepEqual(
    pickComparableWorldView(post),
    pickComparableWorldView(pre),
    'post-execution world memory should reflect the durable world-state change',
  )

  await engine.shutdown({ cleanup: false })
})

test('T4 idempotency replay in the same process does not double-apply world mutations', async (t) => {
  const fixture = createManagedFixture(t)
  const engine = fixture.spawn()
  const request = buildWorldMemoryRequest({
    townId: 'alpha',
    factionId: 'iron_pact',
    chronicleLimit: 3,
    historyLimit: 3,
  })
  const handoff = buildExecutionHandoff({
    snapshotHash: fixture.state.snapshotHash,
    decisionEpoch: fixture.state.decisionEpoch,
    projectId: fixture.state.projectId,
  })

  await engine.waitForReady()
  engine.sendLine(handoff)
  const first = await engine.readNextJsonOfType('execution-result.v1')

  engine.sendLine(request)
  const afterFirst = await engine.readNextJsonOfType('world-memory-context.v1')

  engine.sendLine(handoff)
  const second = await engine.readNextJsonOfType('execution-result.v1')

  engine.sendLine(request)
  const afterSecond = await engine.readNextJsonOfType('world-memory-context.v1')

  assert.equal(first.status, 'executed')
  assert.equal(first.reasonCode, 'EXECUTED')
  assert.equal(second.handoffId, handoff.handoffId)
  assert.equal(second.idempotencyKey, handoff.idempotencyKey)
  assert.equal(second.status, 'duplicate')
  assert.equal(second.reasonCode, 'DUPLICATE_HANDOFF')
  assert.equal(afterFirst.townSummary.executionCounts.executed, 1)
  assert.equal(afterFirst.townSummary.executionCounts.executed, 1)
  assert.equal(afterSecond.townSummary.executionCounts.executed, 1)
  assert.equal(afterSecond.townSummary.activeProjectCount, afterFirst.townSummary.activeProjectCount)
  assert.equal(afterSecond.townSummary.historyCount, afterFirst.townSummary.historyCount + 1)
  assert.equal(
    countHistoryEntries(
      afterSecond,
      (entry) => entry.handoffId === handoff.handoffId && entry.kind === 'terminal_receipt',
    ),
    1,
  )
  assert.equal(
    countHistoryEntries(
      afterSecond,
      (entry) => entry.handoffId === handoff.handoffId && entry.kind === 'result:executed',
    ),
    1,
  )
  assert.equal(
    countHistoryEntries(
      afterSecond,
      (entry) => entry.handoffId === handoff.handoffId && entry.kind === 'duplicate_replayed',
    ),
    1,
  )

  await engine.shutdown({ cleanup: false })
})

test('T5 duplicate delivery across restart converges to a single terminal truth', async (t) => {
  const fixture = createManagedFixture(t)
  const handoff = buildExecutionHandoff({
    snapshotHash: fixture.state.snapshotHash,
    decisionEpoch: fixture.state.decisionEpoch,
    projectId: fixture.state.projectId,
  })
  const request = buildWorldMemoryRequest({
    townId: 'alpha',
    factionId: 'iron_pact',
    chronicleLimit: 3,
    historyLimit: 3,
  })

  const firstEngine = fixture.spawn()
  await firstEngine.waitForReady()
  firstEngine.sendLine(handoff)
  const firstResult = await firstEngine.readNextJsonOfType('execution-result.v1')
  assert.equal(firstResult.status, 'executed')
  await firstEngine.shutdown({ cleanup: false })

  const secondEngine = fixture.spawn()
  await secondEngine.waitForReady()
  secondEngine.sendLine(request)
  const beforeReplay = await secondEngine.readNextJsonOfType('world-memory-context.v1')

  secondEngine.sendLine(handoff)
  const replayResult = await secondEngine.readNextJsonOfType('execution-result.v1')

  secondEngine.sendLine(request)
  const afterReplay = await secondEngine.readNextJsonOfType('world-memory-context.v1')

  assert.equal(replayResult.status, 'duplicate')
  assert.equal(replayResult.reasonCode, 'DUPLICATE_HANDOFF')
  assert.equal(beforeReplay.townSummary.executionCounts.executed, 1)
  assert.equal(afterReplay.townSummary.executionCounts.executed, 1)
  assert.equal(afterReplay.townSummary.activeProjectCount, beforeReplay.townSummary.activeProjectCount)
  assert.equal(afterReplay.townSummary.historyCount, beforeReplay.townSummary.historyCount + 1)
  assert.equal(
    countHistoryEntries(
      afterReplay,
      (entry) => entry.handoffId === handoff.handoffId && entry.kind === 'terminal_receipt',
    ),
    1,
  )
  assert.equal(
    countHistoryEntries(
      afterReplay,
      (entry) => entry.handoffId === handoff.handoffId && entry.kind === 'result:executed',
    ),
    1,
  )
  assert.equal(
    countHistoryEntries(
      afterReplay,
      (entry) => entry.handoffId === handoff.handoffId && entry.kind === 'duplicate_replayed',
    ),
    1,
  )

  await secondEngine.shutdown({ cleanup: false })
})

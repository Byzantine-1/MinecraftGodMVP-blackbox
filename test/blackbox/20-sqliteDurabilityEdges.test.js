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

function countHistoryEntries(response, predicate) {
  return response.recentHistory.filter(predicate).length
}

async function runCrashRecoveryVariant(t, variant) {
  const fixture = createManagedFixture(t)
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

  const firstEngine = fixture.spawn()
  await firstEngine.waitForReady()
  firstEngine.sendLine(handoff)
  if (variant.delayMs > 0) {
    await new Promise((resolve) => setTimeout(resolve, variant.delayMs))
  }
  await firstEngine.kill({ cleanup: false })

  const secondEngine = fixture.spawn()
  await secondEngine.waitForReady()
  secondEngine.sendLine(request)
  const afterRestart = await secondEngine.readNextJsonOfType('world-memory-context.v1')

  secondEngine.sendLine(handoff)
  const firstReplay = await secondEngine.readNextJsonOfType('execution-result.v1')

  secondEngine.sendLine(request)
  const converged = await secondEngine.readNextJsonOfType('world-memory-context.v1')

  secondEngine.sendLine(handoff)
  const secondReplay = await secondEngine.readNextJsonOfType('execution-result.v1')

  secondEngine.sendLine(request)
  const stableReplay = await secondEngine.readNextJsonOfType('world-memory-context.v1')

  assert(afterRestart.townSummary.historyCount <= 1)
  assert(['executed', 'duplicate'].includes(firstReplay.status))
  assert(['EXECUTED', 'DUPLICATE_HANDOFF'].includes(firstReplay.reasonCode))
  assert(['duplicate', 'executed'].includes(secondReplay.status))
  assert.equal(
    countHistoryEntries(
      converged,
      (entry) => entry.handoffId === handoff.handoffId && entry.kind === 'terminal_receipt',
    ),
    1,
  )
  assert.equal(
    countHistoryEntries(
      stableReplay,
      (entry) => entry.handoffId === handoff.handoffId && entry.kind === 'terminal_receipt',
    ),
    1,
  )
  assert(
    countHistoryEntries(
      stableReplay,
      (entry) => entry.handoffId === handoff.handoffId && entry.kind === 'result:executed',
    ) <= 1,
  )
  assert(
    countHistoryEntries(
      stableReplay,
      (entry) => entry.handoffId === handoff.handoffId && entry.kind === 'duplicate_replayed',
    ) <= 1,
  )
  assert.equal(stableReplay.townSummary.activeProjectCount, converged.townSummary.activeProjectCount)
  assert.deepEqual(stableReplay.townIdentity, converged.townIdentity)
  assert.deepEqual(stableReplay.keyActors, converged.keyActors)
  assert.equal(stableReplay.townSummary.executionCounts.executed, converged.townSummary.executionCounts.executed)

  await secondEngine.shutdown({ cleanup: false })
}

test('T6 crash mid-flight + recovery convergence keeps sqlite state safe across restart windows', async (t) => {
  await runCrashRecoveryVariant(t, { label: 'immediate-kill', delayMs: 0 })
  await runCrashRecoveryVariant(t, { label: 'short-delay-kill', delayMs: 25 })
})

test('T7 noisy stdout tolerance still extracts canonical JSON seam messages', async (t) => {
  const fixture = createManagedFixture(t)
  const engine = fixture.spawn({
    logMinLevel: 'debug',
  })
  const request = buildWorldMemoryRequest({
    townId: 'alpha',
    factionId: 'iron_pact',
    chronicleLimit: 2,
    historyLimit: 2,
  })
  const handoff = buildExecutionHandoff({
    snapshotHash: fixture.state.snapshotHash,
    decisionEpoch: fixture.state.decisionEpoch,
    projectId: fixture.state.projectId,
  })

  await engine.waitForReady()
  engine.sendLine(request)
  const response = await engine.readNextJsonOfType('world-memory-context.v1')

  engine.sendLine(handoff)
  const result = await engine.readNextJsonOfType('execution-result.v1')

  assert.equal(response.type, 'world-memory-context.v1')
  assert.equal(result.type, 'execution-result.v1')
  assert.equal(result.status, 'executed')
  assert(engine.snapshot().stdout.includes('--- WORLD ONLINE ---'))

  await engine.shutdown({ cleanup: false })
})

test('T8 garbage or truncated input is rejected safely and the engine stays alive for valid requests', async (t) => {
  const fixture = createManagedFixture(t)
  const engine = fixture.spawn()
  const request = buildWorldMemoryRequest({
    townId: 'alpha',
    factionId: 'iron_pact',
    chronicleLimit: 2,
    historyLimit: 2,
  })

  await engine.waitForReady()
  engine.sendLine('{"type":"world-memory-request.v1"')
  engine.sendLine('{not-json')
  engine.sendLine('{"schemaVersion":"execution-handoff.v1","proposal":')

  await new Promise((resolve) => setTimeout(resolve, 75))
  assert.equal(engine.child.exitCode, null)

  engine.sendLine(request)
  const response = await engine.readNextJsonOfType('world-memory-context.v1')

  assert.equal(engine.child.exitCode, null)
  assert.equal(response.type, 'world-memory-context.v1')
  assert.equal(response.schemaVersion, 1)

  await engine.shutdown({ cleanup: false })
})

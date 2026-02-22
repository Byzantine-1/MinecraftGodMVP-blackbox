# MinecraftGodMVP Blackbox

External blackbox test harness and stress suite for `minecraft-god-mvp`.

This repository treats the engine as an external dependency and validates runtime behavior from the outside, including:

- deterministic replay and idempotency checks
- lock contention and crash-recovery scenarios
- feed bounds and integrity invariants
- command storm and edge-case durability tests
- chaos harness phases for rumors, decisions, quests, titles, and read-only guarantees

## Requirements

- Node.js 20+ (or a compatible Node version used by your engine)
- Access to a local engine checkout

## Configure Engine Path

Set the engine path in one of these ways:

1. Use `blackbox.config.json` (`publicRepoPath`)
2. Set `PUBLIC_REPO_DIR`
3. Pass `--engineDir` to the chaos harness

Example engine path:

`C:\Users\the10\Projects\minecraft-god-mvp`

## Install

```bash
npm install
```

## Run Tests

```bash
npm test
```

## Run Chaos Harness

```bash
node scripts/chaosHarness.js --engineDir="C:\Users\the10\Projects\minecraft-god-mvp" --agents=6 --rounds=250 --workers=10 --timers
```

## Report Output

The chaos harness writes a JSON report to:

- `artifacts/chaos-report.json` (default)

and prints stable invariant lines ending with:

- `OVERALL: PASS` or `OVERALL: FAIL`

## Notes

- `artifacts/` is intentionally ignored by git.
- This repository does not modify engine source files during blackbox runs.

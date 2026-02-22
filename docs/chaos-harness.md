# Chaos Harness (Blackbox)

`scripts/chaosHarness.js` is a stricter external blackbox chaos harness for `minecraft-god-mvp`.
It treats the engine repo as an external dependency and never edits engine files.

## What It Exercises

- Rumors: `spawn`, `list`, `show`, `clear`, `resolve`, `quest`
- Decisions: nightfall auto-open, `choose`, `expire`
- Traits/titles and rep-threshold title awards (`Pact Friend`, `Veil Initiate`, `Wanderer`)
- Side-quest flow and replay safety (visit and trade paths)
- Read-only non-mutation guarantees
- Concurrency stress with worker pools
- Replay/idempotency probes with intentional duplicate `operationId`
- Feed cap and durability invariants

## CLI

Required:

- `--engineDir="<absolute path to minecraft-god-mvp>"`

Optional:

- `--agents=6` (default)
- `--rounds=200` (default)
- `--workers=8` (default)
- `--seed=1337` (default)
- `--timers` (enables memory tx phase timing in engine store)
- `--report=artifacts/chaos-report.json` (default)

## Usage

```powershell
node scripts/chaosHarness.js --engineDir="C:\Users\the10\Projects\minecraft-god-mvp"
```

```powershell
node scripts/chaosHarness.js `
  --engineDir="C:\Users\the10\Projects\minecraft-god-mvp" `
  --agents=6 `
  --rounds=250 `
  --workers=10 `
  --seed=1337 `
  --timers `
  --report=artifacts/chaos-report.json
```

`package.json` includes:

```json
"test:chaos": "node scripts/chaosHarness.js"
```

Note: `npm run test:chaos` still requires `--engineDir` args to run successfully.

## Output Contract

At completion, the harness prints stable lines:

- `AGENTS:`
- `ROUNDS:`
- `WORKERS:`
- `PEAK_HEAP_MB:`
- `LOCK_TIMEOUTS:`
- `INTEGRITY_OK:`
- `UNHANDLED_REJECTIONS:`
- `READONLY_HASH_VIOLATIONS:`
- `IDEMPOTENCY_VIOLATIONS:`
- `NEGATIVE_LEDGER_BALANCE_VIOLATIONS:`
- `FEED_CAP_VIOLATIONS:`
- `UNIQUE_ID_VIOLATIONS:`
- `TITLE_DUPLICATE_VIOLATIONS:`
- `OVERALL: PASS|FAIL`

The harness exits non-zero if any invariant fails.

## Report File

The JSON report (`--report`) includes:

- resolved paths and runtime metadata
- per-phase stats and failures
- invariant counters
- overall `PASS`/`FAIL`

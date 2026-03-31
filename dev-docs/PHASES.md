# Implementation Phases

## Completed

- **Phase 0**: Kani prototype ✅ (52 proofs, compositional verification)
- **Phase 1a**: logfwd-arrow crate ✅ (PR #307)
- **Phase 1.5**: Framer + Aggregator ✅ (PR #311)
- **Phase 2**: StructuralIndex + wide SIMD ✅ (PR #360)
- **Phase 3**: Shadow parser decommissioned ✅ (PR #370)
- **Phase 4**: no_std + forbid(unsafe_code) ✅ (PR #375)
- **Phase 5**: Pipeline bypasses format.rs ✅ (PR #399)
- **Audit**: Dead code, test coverage, proofs, robustness ✅ (PR #416)

## Current: Pipeline Verification (#270, #272, #423)

Three-layer verification of the pipeline protocol.

### Layer 1: Pure State Machine (Kani) — #270
Extract `FlushMachine` into `logfwd-core/src/pipeline/state.rs`.
Pure function: `step(state, event) → (state, action)`.
Kani proves: no panic, no double-flush, done is terminal,
buffer cleared after flush, no data abandoned after shutdown.

The async driver calls `machine.step()` and performs the
requested action — it cannot make incorrect decisions.

### Layer 2: Multi-Process Model (TLA+) — #272
Model N inputs + bounded channel + consumer in TLA+.
TLC proves: NoDataAbandoned, ShutdownCompletes, InputProgress,
no deadlock under backpressure.

### Layer 3: Deterministic Simulation (Turmoil) — #423
Test the async IO layer with simulated network.
Chaos suite: partition recovery, slow consumer, crash recovery,
deterministic retry backoff.

Prerequisites: async sinks, async HTTP client (reqwest).

## Future

### Allocation-free kernel (#358)
Remove `extern crate alloc` from logfwd-core. Requires fully
sequential scanner (no stored bitmask Vecs).

### AsyncFanout (#319)
Independent sink tasks with retry, circuit breaker, per-sink
isolation. Depends on async sink migration.

### Async sink migration
Migrate OutputSink from sync to async. Replace ureq with reqwest.
Unblocks Turmoil and AsyncFanout.

## Parallel work

| Issue | What | Status |
|-------|------|--------|
| #404 | i64 overflow data corruption | Jules |
| #407 | Field suffix stripping bugs | Jules |
| #410 | Double-escaped unicode | Jules |
| #411-414 | Config validation bugs | Jules |
| #340 | Test coverage gaps | Detailed findings, needs impl |
| #341 | Missing doc comments (86 items) | Detailed findings, needs impl |
| #343 | Error handling consistency | Detailed findings, needs impl |
| #344 | Kani proof expansion (5 new proofs) | Detailed findings, needs impl |
| #346 | Pipeline robustness | Findings complete, one fix merged |
| #275 | CRI silent truncation | Open |
| #279 | Arrow version upgrade | Open |
| #308 | Rethink _raw column | Open |
| #415 | SQL rewriter missing arms | Open |

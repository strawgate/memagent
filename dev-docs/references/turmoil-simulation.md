# Turmoil Simulation Notes (Repo-Scoped)

Guidance for deterministic concurrency simulation tests.

## When To Use

- Concurrency/lifecycle bugs in runtime orchestration.
- Shutdown, cancellation, or ordering behavior that is flaky in realtime tests.
- Backpressure and reconnection behavior requiring repeatable schedules.

## Critical Rules

- Always capture and report `TURMOIL_SEED` for failures.
- Re-run failing seeds before changing code.
- Avoid relying on wall-clock timing; assert protocol/state outcomes.
- Keep one clear failure cause per test scenario.
- Prefer barrier-controlled seams over sleeps (`BeforeWorkerAckSend`, `BeforeCheckpointFlushAttempt`).
- Use directional faults (`partition_oneway`/`repair_oneway`) when a bug depends on asymmetric links.
- Validate replay equivalence using normalized transition history, not only aggregate counters.
- For filesystem durability semantics, pair `Sim::crash`/`Sim::bounce` with unstable-fs sync/no-sync assertions.

## Common Pitfalls

- Hidden runtime dependencies (`spawn_blocking`, OS-only calls) that bypass simulation assumptions.
- Tests that pass only for one schedule because assertions are too weak.
- Flake triage without seed replay, making regressions irreproducible.
- Tests that only assert final totals and miss link-level backlog or transition-order regressions.

## Local Debug Aids

- `TURMOIL_TRACE_PROGRESS=1`: emit periodic simulation progress lines.
- `TURMOIL_TRACE_PROGRESS_INTERVAL=250`: control progress print interval in steps.

## Minimal Workflow

```bash
TURMOIL_SEED=42 cargo test --features turmoil --test turmoil_sim -- --nocapture
for seed in {1..100}; do TURMOIL_SEED=$seed cargo test --features turmoil --test turmoil_sim; done
```

Replay and history-specific checks:

```bash
cargo test -p logfwd --features turmoil --test turmoil_sim fault_scenario_sim::replay_equivalence_same_seed_produces_same_normalized_contract_trace -- --exact
cargo test -p logfwd --features turmoil --test turmoil_sim fault_scenario_sim::replay_equivalence_seed_matrix_keeps_history_equivalent -- --exact
```

## Canonical Docs

- `dev-docs/VERIFICATION.md` (tool-selection policy)
- `dev-docs/research/turmoil-concurrency-analysis.md` (active findings)

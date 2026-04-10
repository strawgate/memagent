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

## Common Pitfalls

- Hidden runtime dependencies (`spawn_blocking`, OS-only calls) that bypass simulation assumptions.
- Tests that pass only for one schedule because assertions are too weak.
- Flake triage without seed replay, making regressions irreproducible.

## Minimal Workflow

```bash
TURMOIL_SEED=42 cargo test --features turmoil --test turmoil_sim -- --nocapture
for seed in {1..100}; do TURMOIL_SEED=$seed cargo test --features turmoil --test turmoil_sim; done
```

## Canonical Docs

- `dev-docs/VERIFICATION.md` (tool-selection policy)
- `dev-docs/research/turmoil-concurrency-bugs-report-2026-04.md` (active findings)


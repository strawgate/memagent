# Experiment Contract

> **Status:** Active
> **Date:** 2026-04-11
> **Context:** Required benchmark, correctness, and reporting contract for columnar builder experiments.

Every columnar-builder experiment must be comparable. Use this contract unless a
design note explicitly narrows the scope.

## Repository Assumptions

- Work from the branch that contains this file.
- Assume no hidden context from prior conversations.
- Read `AGENTS.md`, `DEVELOPING.md`, and `CONTRIBUTING.md` before making code
  changes.
- Do not put OTLP-specific semantics in `logfwd-arrow`.
- Prefer batch-local plans and caches. Do not introduce cross-batch caches unless
  the deliverable explicitly defends them.

## Required Correctness Checks

At minimum, run focused tests relevant to the changed area:

```bash
cargo fmt --check
cargo test -p logfwd-arrow streaming_builder -- --nocapture
cargo test -p logfwd-io otlp_receiver::projection::tests -- --nocapture
cargo clippy -p logfwd-arrow --lib -- -D warnings
cargo clippy -p logfwd-io --lib -- -D warnings
```

If output encoding changes, also run:

```bash
cargo test -p logfwd-output otlp_sink -- --nocapture
cargo clippy -p logfwd-output --lib -- -D warnings
```

If benchmark harnesses change, also run:

```bash
cargo clippy -p logfwd-bench --bench otlp_io -- -D warnings
cargo clippy -p logfwd-bench --bin otlp_io_profile -- -D warnings
cargo clippy -p logfwd-bench --features otlp-profile-alloc --bin otlp_io_profile -- -D warnings
```

Known caveat: some `logfwd-io` tests currently emit unrelated warnings from
`tail/tests.rs` when running targeted projection tests. Do not treat those as
new findings unless your change touches that area.

## Benchmark Commands

Use normal allocator timings for speed:

```bash
cargo run -p logfwd-bench --release --bin otlp_io_profile -- --case attrs-heavy --mode projected_view_decode --iterations 500
cargo run -p logfwd-bench --release --bin otlp_io_profile -- --case wide-10k --mode projected_view_decode --iterations 200
cargo run -p logfwd-bench --release --bin otlp_io_profile -- --case attrs-heavy --mode e2e_projected_view --iterations 300
cargo run -p logfwd-bench --release --bin otlp_io_profile -- --case wide-10k --mode e2e_projected_view --iterations 100
```

Use allocation instrumentation for memory pressure:

```bash
cargo run -p logfwd-bench --release --features otlp-profile-alloc --bin otlp_io_profile -- --case attrs-heavy --mode projected_view_decode --iterations 100
cargo run -p logfwd-bench --release --features otlp-profile-alloc --bin otlp_io_profile -- --case wide-10k --mode projected_view_decode --iterations 30
cargo run -p logfwd-bench --release --features otlp-profile-alloc --bin otlp_io_profile -- --case attrs-heavy --mode e2e_projected_view --iterations 100
cargo run -p logfwd-bench --release --features otlp-profile-alloc --bin otlp_io_profile -- --case wide-10k --mode e2e_projected_view --iterations 30
```

Use flamegraphs when a change is intended to move a CPU hotspot:

```bash
mkdir -p /tmp/logfwd-otlp-profiles/experiment
cargo run -p logfwd-bench --release --bin otlp_io_profile -- --case wide-10k --mode projected_view_decode --iterations 200 --flamegraph /tmp/logfwd-otlp-profiles/experiment/wide-10k-decode.svg
cargo run -p logfwd-bench --release --bin otlp_io_profile -- --case wide-10k --mode e2e_projected_view --iterations 200 --flamegraph /tmp/logfwd-otlp-profiles/experiment/wide-10k-e2e.svg
```

Criterion suite for final comparisons:

```bash
just bench-otlp-io-fast -- --warm-up-time 1 --measurement-time 2 --sample-size 10 otlp_input_decode_materialize
just bench-otlp-io
```

## Baseline To Report Against

Use foundation baseline commit
`a622416d8f54686dc31c4a75b5c6877e7d57471c` unless a design note provides a
newer pinned baseline. Representative baseline after the foundation
optimizations:

| Fixture | Mode | Time |
|---|---:|---:|
| attrs-heavy | projected-view decode | ~1734 ns/row |
| wide-10k | projected-view decode | ~1077 ns/row |
| attrs-heavy | projected-view decode->encode | ~2435 ns/row |
| wide-10k | projected-view decode->encode | ~1580 ns/row |

Allocation baseline:

| Fixture | Mode | Bytes/row | Allocs/row |
|---|---:|---:|---:|
| attrs-heavy | projected-view decode | ~2142 | ~0.443 |
| wide-10k | projected-view decode | ~1555 | ~0.055 |

Always include raw command output snippets in the deliverable so reviewers can
audit what was actually measured.

## Required Deliverable Format

Use these sections when reporting an experiment:

1. `Recommendation`: Adopt, reject, or investigate further.
2. `What I changed or prototyped`: include file paths and design shape.
3. `Correctness evidence`: tests, parity checks, fuzz/corpus work.
4. `Benchmark evidence`: before/after table with command lines.
5. `Flamegraph or memory findings`: hotspots that moved or remain.
6. `Maintainability assessment`: complexity, duplication, crate boundaries.
7. `Risks and unknowns`: what could invalidate the result.
8. `Next steps`: concrete follow-up tasks.

## Promotion Rules

- Promote a finding only when confidence is roughly `0.7` or higher.
- Put lower-confidence ideas in `Risks and unknowns` or `Watch items`.
- Do not average multiple weak signals into a strong recommendation.
- If a prototype regresses performance but improves architecture, say so plainly.
- If a prototype is faster but creates long-term semantic duplication, say so plainly.

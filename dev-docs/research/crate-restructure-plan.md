# Crate Restructure Plan

> **Status:** Active
> **Date:** 2026-04-03
> **Context:** Plan to split ffwd-core and ffwd-io for build time and blast radius improvements.

## Current State

10 workspace crates, 42,745 lines, 586 tests.

```
Layer 0 (leaves):   ffwd-core (8.5K, 4 deps, no_std)    ffwd-config (1.4K, 20 deps)
Layer 1:            ffwd-arrow (2.3K, 144 deps)          ffwd-io (9.1K, 363 deps)
Layer 2:            ffwd-transform (3.9K, 921 deps)      ffwd-output (6.5K, 551 deps)
Layer 3:            ffwd (4.8K, 1105 deps)
```

## Problems

### 1. ffwd-core blast radius = 8 crates

ffwd-core contains BOTH the SIMD scanner (changes often during optimization)
AND pipeline types (SourceId, BatchTicket, PipelineMachine — stable, everything
depends on these). Touching SIMD code rebuilds output sinks.

### 2. ffwd-io is a kitchen sink (9.1K lines, 19 ext deps)

Contains file tailing, TCP/UDP, OTLP receiver, checkpointing, compression,
enrichment, diagnostics server, metrics. These are unrelated concerns.

### 3. Undocumented cross-layer dependencies

ffwd-output depends on ffwd-io (tailer change → sink rebuild).
ffwd-transform depends on ffwd-io (tailer change → DataFusion rebuild).

### 4. 125s test in ffwd-transform

`edge_very_long_value` dominates CI. One test = 95% of total test wall time.

### 5. No default-members

`cargo build` compiles bench crates nobody needs during dev.

## Proposed Changes

### Phase A: default-members (1 line, zero risk)

```toml
default-members = [
    "crates/ffwd-config",
    "crates/ffwd-core",
    "crates/ffwd-arrow",
    "crates/ffwd-transform",
    "crates/ffwd-io",
    "crates/ffwd-output",
    "crates/ffwd",
]
```

Excludes ffwd-bench, ffwd-competitive-bench, ffwd-test-utils from
`cargo build` and `cargo check`. CI uses `--workspace` explicitly.

### Phase B: Extract ffwd-types (highest value)

Move from ffwd-core to new ffwd-types crate:
- `pipeline/mod.rs` (BatchTicket, PipelineMachine, SourceId, etc.)
- `pipeline/batch.rs`
- `pipeline/lifecycle.rs`
- `pipeline/registry.rs`
- Error types if any

ffwd-types is no_std, no deps beyond core. ffwd-core depends on
ffwd-types (re-exports for backward compat). Other crates that only
need pipeline types depend on ffwd-types directly, not ffwd-core.

**Blast radius change:**
- Touch SIMD in ffwd-core → rebuilds arrow, transform, bench (NOT output, io)
- Touch pipeline types → rebuilds everything (but types are stable)

### Phase C: Remove ffwd-io dep from output and transform

ffwd-output needs: conflict_schema (move to ffwd-arrow), checkpoint types
(move to ffwd-types). After moving, output depends on types + arrow + HTTP.
ffwd-transform needs: enrichment trait (move to ffwd-types). After moving,
transform depends on types + arrow + datafusion.

### Phase D: Split ffwd-io (optional, lower priority)

Split into:
- ffwd-input (file tailing, TCP, UDP, OTLP receiver)
- ffwd-checkpoint (checkpoint store, fingerprinting)
- ffwd-diagnostics (diagnostics server, metrics, spans)

Only do this if ffwd-io blast radius remains a problem after Phase C.

## Research References

- **DataFusion**: `-common` crates for types/traits, logic crates depend on them
- **Rustls**: provable core with zero implementation deps, providers as separate crates
- **s2n-quic**: `no_std` kernel, `testing` feature gates property test generators
- **Vector**: feature flags within crates, `vector-lib` facade for re-exports
- **Tokio**: feature flags to avoid compiling unused subsystems

## What NOT to change

- Don't split output sinks into separate crates (Vector tried, retreated)
- Don't merge arrow into core (144 deps would bloat proven kernel)
- Don't split transform (DataFusion is one crate, nothing to split)
- Don't add feature flags yet (premature optimization, adds complexity)

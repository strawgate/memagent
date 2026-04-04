# Crate Restructure Plan

> **Status:** Active
> **Date:** 2026-04-03
> **Context:** Plan to split logfwd-core and logfwd-io for build time and blast radius improvements.

## Current State

10 workspace crates, 42,745 lines, 586 tests.

```
Layer 0 (leaves):   logfwd-core (8.5K, 4 deps, no_std)    logfwd-config (1.4K, 20 deps)
Layer 1:            logfwd-arrow (2.3K, 144 deps)          logfwd-io (9.1K, 363 deps)
Layer 2:            logfwd-transform (3.9K, 921 deps)      logfwd-output (6.5K, 551 deps)
Layer 3:            logfwd (4.8K, 1105 deps)
```

## Problems

### 1. logfwd-core blast radius = 8 crates

logfwd-core contains BOTH the SIMD scanner (changes often during optimization)
AND pipeline types (SourceId, BatchTicket, PipelineMachine — stable, everything
depends on these). Touching SIMD code rebuilds output sinks.

### 2. logfwd-io is a kitchen sink (9.1K lines, 19 ext deps)

Contains file tailing, TCP/UDP, OTLP receiver, checkpointing, compression,
enrichment, diagnostics server, metrics. These are unrelated concerns.

### 3. Undocumented cross-layer dependencies

logfwd-output depends on logfwd-io (tailer change → sink rebuild).
logfwd-transform depends on logfwd-io (tailer change → DataFusion rebuild).

### 4. 125s test in logfwd-transform

`edge_very_long_value` dominates CI. One test = 95% of total test wall time.

### 5. No default-members

`cargo build` compiles bench crates nobody needs during dev.

## Proposed Changes

### Phase A: default-members (1 line, zero risk)

```toml
default-members = [
    "crates/logfwd-config",
    "crates/logfwd-core",
    "crates/logfwd-arrow",
    "crates/logfwd-transform",
    "crates/logfwd-io",
    "crates/logfwd-output",
    "crates/logfwd",
]
```

Excludes logfwd-bench, logfwd-competitive-bench, logfwd-test-utils from
`cargo build` and `cargo check`. CI uses `--workspace` explicitly.

### Phase B: Extract logfwd-types (highest value)

Move from logfwd-core to new logfwd-types crate:
- `pipeline/mod.rs` (BatchTicket, PipelineMachine, SourceId, etc.)
- `pipeline/batch.rs`
- `pipeline/lifecycle.rs`
- `pipeline/registry.rs`
- Error types if any

logfwd-types is no_std, no deps beyond core. logfwd-core depends on
logfwd-types (re-exports for backward compat). Other crates that only
need pipeline types depend on logfwd-types directly, not logfwd-core.

**Blast radius change:**
- Touch SIMD in logfwd-core → rebuilds arrow, transform, bench (NOT output, io)
- Touch pipeline types → rebuilds everything (but types are stable)

### Phase C: Remove logfwd-io dep from output and transform

logfwd-output needs: conflict_schema (move to logfwd-arrow), checkpoint types
(move to logfwd-types). After moving, output depends on types + arrow + HTTP.
logfwd-transform needs: enrichment trait (move to logfwd-types). After moving,
transform depends on types + arrow + datafusion.

### Phase D: Split logfwd-io (optional, lower priority)

Split into:
- logfwd-input (file tailing, TCP, UDP, OTLP receiver)
- logfwd-checkpoint (checkpoint store, fingerprinting)
- logfwd-diagnostics (diagnostics server, metrics, spans)

Only do this if logfwd-io blast radius remains a problem after Phase C.

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

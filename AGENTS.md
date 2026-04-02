# Agent Guide

## Before You Start

Read these project docs first:
- `README.md` — what logfwd does, performance targets
- `DEVELOPING.md` — build/test/bench commands, codebase structure
- `dev-docs/ARCHITECTURE.md` — pipeline data flow, scanner stages

## Crate Structure

```text
logfwd-core         Parsing, structural SIMD detection, pipeline logic
  structural.rs     Streaming 10-char SIMD detection via `wide` crate
  scanner.rs        JSON field extraction using StructuralIndex bitmasks
  framer.rs         Newline framing (Kani-proven)
  aggregator.rs     CRI P/F reassembly (Kani-proven)
  otlp.rs           Protobuf encoding

logfwd-arrow        Arrow builders (StreamingBuilder, StorageBuilder)
logfwd-input        File tailer, IO
logfwd-transform    SQL transforms via DataFusion
logfwd-output       Sinks (OTLP, Elasticsearch, Loki, Parquet, JSON lines)
logfwd              Async pipeline shell, CLI, config
```

## Reference Docs

Library-specific guides — read the relevant ones before working on related code.

| Doc | When to read |
|-----|-------------|
| [`dev-docs/references/arrow-v54.md`](dev-docs/references/arrow-v54.md) | RecordBatch construction, StringViewArray zero-copy, schema evolution |
| [`dev-docs/references/datafusion-v45.md`](dev-docs/references/datafusion-v45.md) | SessionContext, MemTable, UDF creation, SQL execution |
| [`dev-docs/references/tokio-async-patterns.md`](dev-docs/references/tokio-async-patterns.md) | Runtime, bounded channels, CancellationToken, select! safety |
| [`dev-docs/references/opentelemetry-otlp.md`](dev-docs/references/opentelemetry-otlp.md) | MeterProvider, OTLP protobuf nesting, HTTP vs gRPC |
| [`dev-docs/references/notify-memchr-zstd.md`](dev-docs/references/notify-memchr-zstd.md) | File watcher events, SIMD search, zstd compression |
| [`dev-docs/references/kani-verification.md`](dev-docs/references/kani-verification.md) | Kani proofs, function contracts, solver selection, Bolero |

## Architecture Decisions

`dev-docs/DECISIONS.md` — settled architecture decisions with reasoning. Read before reopening a settled question.

Key decisions:
- Two-stage architecture: SIMD detects (parallel), scanner consumes (sequential)
- Streaming per-block: quote/in_string bitmasks stored, others consumed immediately (moving toward zero stored)
- Portable SIMD via `wide` crate: no hand-rolled platform backends
- Three-layer verification: Kani (scalar) → proptest (SIMD ≡ scalar) → Kani (consumers)
- Pipeline state machine over linear BatchToken (async cancellation prevents linear types)

## Research

`dev-docs/research/` — research results that informed decisions.

| Doc | Topic |
|-----|-------|
| `structural-index-research.md` | Kani SIMD, memory layout, block sizes, simdjson architecture |
| `wide-crate-evaluation.md` | Portable SIMD library evaluation |
| `KANI_LIMITS.md` | Kani bounded model checker practical limits |
| `BOUNDARY_PATTERNS.md` | Crate boundary design patterns |

## Issue Labels

| Label | Meaning |
|-------|---------|
| `bug` | Broken behavior |
| `enhancement` | New feature or improvement |
| `performance` | Performance optimization |
| `production` | Required for production readiness |
| `research` | Needs investigation before implementation |

## Code Quality

- `just ci` must pass before submitting
- No per-line heap allocations in the hot path (scanner, format parser, CRI parser, builders, OTLP encoder, compress)
- No `.unwrap()` in production code paths — use `?` or `.expect("reason")`
- No new dependencies without justification
- Public functions need doc comments
- Kani proofs for pure logic in logfwd-core

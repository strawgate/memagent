# Agent Guide

## Before You Start

Read these project docs first:
- `README.md` — what logfwd does, performance targets
- `DEVELOPING.md` — build/test/bench commands, codebase structure
- `docs/ARCHITECTURE.md` — v2 Arrow pipeline design

## Reference Docs

Library-specific guides covering non-obvious APIs, gotchas, and patterns. Read the relevant ones before working on related code.

| Doc | When to read |
|-----|-------------|
| [`docs/references/arrow-v54.md`](docs/references/arrow-v54.md) | RecordBatch construction, StringViewArray zero-copy, IPC read/write with compression, schema evolution, null handling, builder reuse |
| [`docs/references/datafusion-v45.md`](docs/references/datafusion-v45.md) | SessionContext lifecycle and reuse, MemTable registration, UDF creation, SQL execution patterns, the nested-runtime panic |
| [`docs/references/tokio-async-patterns.md`](docs/references/tokio-async-patterns.md) | Runtime vs Handle, bounded mpsc channels for backpressure, CancellationToken + ordered shutdown, select! cancellation safety, sync↔async migration |
| [`docs/references/opentelemetry-otlp.md`](docs/references/opentelemetry-otlp.md) | MeterProvider + PeriodicReader setup, OTLP protobuf message nesting and field numbers, dual-write metrics pattern, HTTP vs gRPC conventions |
| [`docs/references/notify-memchr-zstd.md`](docs/references/notify-memchr-zstd.md) | File watcher event model and debouncing, SIMD string search with memchr/memmem, zstd streaming compression and dictionary training |

## Architecture Spec

[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — formal component spec with traits, data contracts, and migration plan. Read before working on:
- Reader/Scanner/DiskQueue/Transform/Sink/Pipeline/Checkpoint components
- Backpressure, retry, or shutdown behavior
- Any issue in the Phase 1–4 implementation plan

## Investigation Findings

[`docs/investigation/`](docs/investigation/) — research results that informed design decisions. Consult before revisiting a settled question.

| Doc | Decision |
|-----|----------|
| [`arrow-ipc-feasibility.md`](docs/investigation/arrow-ipc-feasibility.md) | DiskQueue uses Arrow IPC FileWriter + zstd + atomic rename |

## Issue Labels

| Label | Meaning |
|-------|---------|
| `bug` | Broken behavior |
| `enhancement` | New feature or improvement |
| `performance` | Performance optimization |
| `production` | Required for production readiness |
| `research` | Needs investigation before implementation |
| `documentation` | Docs only |

## Code Quality

- `cargo test` must pass before submitting
- `cargo clippy -- -D warnings` must be clean
- `cargo fmt --check` must pass
- No per-line heap allocations in the hot path (scanner, format parser)
- No `.unwrap()` in production code paths — use `?` or `.expect("reason")`
- No new dependencies without justification

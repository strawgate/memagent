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
| [`docs/references/arrow-v54.md`](docs/references/arrow-v54.md) | RecordBatch construction, StringViewArray zero-copy, schema evolution |
| [`docs/references/datafusion-v45.md`](docs/references/datafusion-v45.md) | SessionContext, MemTable, UDF creation, SQL execution |
| [`docs/references/tokio-async-patterns.md`](docs/references/tokio-async-patterns.md) | Runtime, bounded channels, CancellationToken, select! safety |
| [`docs/references/opentelemetry-otlp.md`](docs/references/opentelemetry-otlp.md) | MeterProvider, OTLP protobuf nesting, HTTP vs gRPC |
| [`docs/references/notify-memchr-zstd.md`](docs/references/notify-memchr-zstd.md) | File watcher events, SIMD search, zstd compression |
| [`docs/references/kani-verification.md`](docs/references/kani-verification.md) | Kani proofs, function contracts, solver selection, Bolero |
| [`docs/VERIFICATION_GUIDE.md`](docs/VERIFICATION_GUIDE.md) | When to add proofs, proof quality requirements, practical guidelines |
| [`docs/KANI_PATTERNS.md`](docs/KANI_PATTERNS.md) | Reusable proof templates from codebase, anti-patterns, checklist |

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

## Kani Verification Requirements

### When Kani Proofs are REQUIRED

Add Kani proofs when implementing or modifying:

1. **Parsers** — Any code handling raw bytes (`framer.rs`, `cri.rs`, `json_scanner.rs`)
   - MUST have `verify_<function>_no_panic` proof
   - SHOULD have correctness proof against oracle if feasible

2. **Wire Formats** — Protobuf encoding, varint encoding, size calculations
   - MUST prove no panic on all inputs within bounded size
   - MUST verify size limits are respected

3. **Bitmask Operations** — SIMD structural character detection (`structural.rs`)
   - MUST have oracle-based correctness proof
   - Example: `verify_compute_real_quotes` with byte-by-byte oracle

4. **Byte Search Primitives** — Low-level search operations (`byte_search.rs`)
   - MUST prove correctness matches naive implementation

5. **State Machines** — Protocol state transitions (P/F flags, aggregation)
   - MUST prove state invariants hold across all transitions
   - Test all valid state sequences (P+F, P+P+F, etc.)

### When Kani Proofs are NOT Required

Do NOT add Kani proofs for:

- I/O operations (file/network — not pure)
- Async runtime logic (not supported by Kani)
- Complex state machines > 8-10 transitions (use proptest)
- Heap-heavy Vec/HashMap code (use proptest + Miri)
- Simple getters/setters
- Integration tests across crates

### Proof Quality Requirements

Every Kani proof MUST:

1. Use `#[cfg(kani)]` to isolate from regular builds
2. Follow naming: `verify_<function>_<property>`
3. Add `#[kani::unwind(N)]` for any loops (N = iterations + 2)
4. Use appropriate input sizes (8-32 bytes for parsing, full range for bitmasks)
5. Add `kani::cover!()` if using `kani::assume()` to guard against vacuity

### Quick Reference

```rust
// Pattern 1: No-panic proof
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(34)]
fn verify_my_parser_no_panic() {
    let input: [u8; 32] = kani::any();
    let _ = my_parser(&input);
}

// Pattern 2: Oracle-based correctness
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(N)]
fn verify_my_function_correct() {
    let input = symbolic_input();
    let result = optimized_impl(input);
    let expected = naive_oracle(input);
    assert_eq!(result, expected);
}

// Pattern 3: Property proof with covers
#[cfg(kani)]
#[kani::proof]
fn verify_size_limit() {
    let max: usize = kani::any();
    kani::assume(max >= 1 && max <= 32);
    let input: [u8; 16] = kani::any();

    let result = process(&input, max);
    assert!(result.len() <= max);

    // Guard vacuity
    kani::cover!(result.len() > 0, "non-empty output");
    kani::cover!(result.len() == max, "max size reached");
}
```

See `docs/KANI_PATTERNS.md` for complete pattern library and `docs/VERIFICATION_GUIDE.md` for detailed guidelines.


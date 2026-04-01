# Implementation Phases

Current work tracked in GitHub epic #262.

## Phase 0: Kani prototype ✅ DONE

Kani added to workspace. `prefix_xor` and `compute_real_quotes` proved
exhaustively. Tooling validated, CI working, 31 proofs total.

## Phase 1: Crate restructuring (partial)

Split logfwd-core into proven core + satellite crates.

```text
1a: Create logfwd-arrow     ✅ DONE (PR #307)
1b: Create logfwd-input     → Copilot (#265)
1c: Move remaining impure   → Copilot (#266)
1d: Tighten core to no_std  (#267) — after 1b+1c
```

## Phase 1.5: Framer + Aggregator + Proofs ✅ DONE (PR #311)

- NewlineFramer: fixed-size output, no heap, 4 Kani proofs (oracle)
- CriAggregator: zero-copy F path, max_message_size, 3 Kani proofs
- byte_search module: proven alternatives to memchr, 2 Kani proofs
- 31 Kani proofs total across core modules

## Phase 2: Unified StructuralIndex (#313) ← NEXT

Extend ChunkIndex into StructuralIndex: one SIMD pass detecting 9
structural characters (\n, space, ", \, comma, colon, {, }, [).

Benchmark proved viability (2026-03-30):
- 9 chars at 3 GiB/s (NEON) — 11x headroom over target
- Scaling linear at ~28µs per character per 760KB

```text
2a: Extend ChunkIndex → StructuralIndex (NEON + AVX2 + SSE2 + scalar)
2b: NewlineFramer consumes newline bitmask (replaces byte loop)
2c: CRI field extraction from space bitmask (replaces per-line parsing)
2d: Scanner consumes full bitmask set
2e: Kani proofs for scalar fallback
```

## Phase 3: Zero-copy Bytes pipeline (#303)

```text
3a: Bytes-based Reader (BytesMut → freeze → Bytes)
3b: Connect StructuralIndex to Bytes pipeline
3c: StreamingBuilder receives Bytes directly
```

## Phase 4: Scanner restructure + FieldSink (#269)

Rename ScanBuilder → FieldSink. Ensure scan loop works in no_std.
Add bounded Kani proof for scan_line. proptest oracle vs serde_json.

## Phase 5: Pipeline state machine + BatchTicket (#270) ← IN PROGRESS

Extract run_async decisions into pure state machine in core.

```text
5a: Typestate BatchTicket<S, C>         ✅ DONE
    Generic over checkpoint type C (opaque to pipeline)
    Queued → Sending → Acked/Rejected
    #[must_use], private fields, 5 Kani proofs, 5 unit tests

5b: PipelineMachine<S, C> lifecycle    ✅ DONE
    Starting → Running → Draining → Stopped
    Ordered ACK via BatchId (not checkpoint values)
    5 Kani proofs, 15 unit tests

5c: Wire into pipeline.rs              → TODO
    PipelineMachine<_, u64> for file byte offsets
    Checkpoint persistence (fingerprint → offset map)

5d: proptest random event sequences    ✅ DONE
    4 property tests: in-flight consistency, final checkpoint,
    drain completion, multi-source simulation

5e: Research: offset semantics          ✅ DONE
    dev-docs/research/offset-checkpoint-research.md
    Filebeat, Vector, Fluent Bit, OTel Collector analysis
    Decision: checkpoint is opaque to pipeline
```

## Phase 6: proptest state machines + CI hardening (#271)

proptest-state-machine for stateful components.
Proof coverage enforcement. cargo-mutants weekly. cargo-vet.

## Phase 7: TLA+ pipeline specification (#272)

Model batching/timeout/shutdown protocol. Prove liveness (data is
never abandoned) and fairness (no input starved). Requires Phase 5
completion (pure state machine extraction).

```text
7a: PipelineBatch.tla — N inputs, bounded channel, consumer
7b: Prove NoDataAbandoned (every batch eventually acked or rejected)
7c: Prove ShutdownCompletes (drain terminates)
7d: Turmoil simulation tests (requires async sink migration)
```

## Phase 8: Tighten logfwd-core (#267)

- `#![no_std]` + `#![forbid(unsafe_code)]`
- SIMD via `wide` crate (safe), scalar fallback for Kani
- CI: `cargo build --target thumbv6m-none-eabi`

## Phase 9: Allocation-free kernel (#358)

Remove `extern crate alloc` entirely. No Vec, String, Box in core.
All buffers stack-local or caller-provided. Mathematically impossible
to OOM. Requires fully sequential scanner (no stored bitmask vecs).

## Phase 10: Type suffix redesign (#445) ← NEXT

Eliminate `_str`/`_int`/`_float` column name suffixes. Root cause of
11 bugs. One JSON key = one Arrow column. DataType-based dispatch
everywhere.

```text
10a: Type promotion in builders          → TODO
     ResolvedType enum, single column per field
     StreamingBuilder + StorageBuilder

10b: DataType dispatch in output sinks   → TODO
     write_row_json dispatches on DataType
     Delete parse_column_name

10c: Delete SQL rewriter                 → TODO
     Delete rewriter.rs (772 lines)
     Delete strip_type_suffix

10d: Update all tests                    → TODO
     "status_int" → "status" everywhere
```

Research: `dev-docs/research/type-suffix-redesign.md`

## Parallel work

| Issue | What | Status |
|-------|------|--------|
| #491 | Transform/output errors kill pipeline | Open (production) |
| #318 | Retry with backoff | Open (production) |
| #319 | AsyncFanout with circuit breaker | Open (production) |
| #357 | Decommission shadow JSON parser in otlp.rs | Open |
| #275 | Fix CRI silent truncation | Open |
| #337-346 | Codebase audit (10 issues) | Jules assigned |

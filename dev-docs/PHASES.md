# Implementation Phases

Current work tracked in GitHub epic #262.

## Phase 0: Kani prototype ✅ DONE

Kani added to workspace. `prefix_xor` and `compute_real_quotes` proved
exhaustively. Tooling validated, CI working, 31 proofs total.

## Phase 1: Crate restructuring (partial)

Split logfwd-core into proven core + satellite crates.

```
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

```
2a: Extend ChunkIndex → StructuralIndex (NEON + AVX2 + SSE2 + scalar)
2b: NewlineFramer consumes newline bitmask (replaces byte loop)
2c: CRI field extraction from space bitmask (replaces per-line parsing)
2d: Scanner consumes full bitmask set
2e: Kani proofs for scalar fallback
```

## Phase 3: Zero-copy Bytes pipeline (#303)

```
3a: Bytes-based Reader (BytesMut → freeze → Bytes)
3b: Connect StructuralIndex to Bytes pipeline
3c: StreamingBuilder receives Bytes directly
```

## Phase 4: Scanner restructure + FieldSink (#269)

Rename ScanBuilder → FieldSink. Ensure scan loop works in no_std.
Add bounded Kani proof for scan_line. proptest oracle vs serde_json.

## Phase 5: Pipeline state machine + BatchToken (#270)

Extract run_async decisions into pure state machine in core.
Kani exhaustive single-step. proptest random event sequences.
BatchToken #[must_use] linear type. Wire into run_async.

## Phase 6: proptest state machines + CI hardening (#271)

proptest-state-machine for stateful components.
Proof coverage enforcement. cargo-mutants weekly. cargo-vet.

## Phase 7: TLA+ pipeline specification (#272)

Model batching/timeout/shutdown protocol. Prove liveness (data is
never abandoned) and fairness (no input starved).

## Phase 8: Tighten logfwd-core (#267)

- `#![no_std]` + `#![forbid(unsafe_code)]`
- SIMD via `wide` crate (safe), scalar fallback for Kani
- CI: `cargo build --target thumbv6m-none-eabi`

## Phase 9: Allocation-free kernel (#358)

Remove `extern crate alloc` entirely. No Vec, String, Box in core.
All buffers stack-local or caller-provided. Mathematically impossible
to OOM. Requires fully sequential scanner (no stored bitmask vecs).

## Parallel work

| Issue | What | Status |
|-------|------|--------|
| #357 | Decommission shadow JSON parser in otlp.rs | Open |
| #359 | proptest: SQL pushdown integrity | Open |
| #275 | Fix CRI silent truncation | Open |
| #279 | Arrow version upgrade | Open |
| #308 | Rethink _raw column | Open |
| #337-346 | Codebase audit (10 issues) | Copilot assigned |

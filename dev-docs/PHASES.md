# Implementation Phases

Current work tracked in GitHub epic #262.

## Phase 0: Kani prototype ✅ DONE

Kani validated. 31 proofs total. `prefix_xor`, `compute_real_quotes`
proved exhaustively. CI working.

## Phase 1: Crate restructuring (partial)

```
1a: Create logfwd-arrow     ✅ DONE (PR #307)
1b: Create logfwd-input     → Copilot (#265)
1c: Move remaining impure   → Copilot (#266)
1d: Tighten core to no_std  (#267) — after all phases complete
```

## Phase 1.5: Framer + Aggregator + Proofs ✅ DONE (PR #311)

NewlineFramer, CriAggregator, byte_search module. 31 Kani proofs.

## Phase 2: Streaming StructuralIndex (#313) ← NEXT

Extend ChunkIndex into a streaming per-block structural scanner.
One SIMD pass detects 10 chars, bitmasks consumed immediately
(zero heap allocation). simdjson/simd-json proven architecture.

Benchmark: 2.7x faster than separate passes. 2.7 GiB/s full pipeline.

```
2a: RawBlockMasks + StreamingStructural in core (pure u64 logic)
2b: find_char_mask_scalar + Kani proof (exhaustive)
2c: SIMD backends (NEON/AVX2/SSE2) in arrow via StructuralDetector trait
2d: proptest: SIMD ≡ scalar for all backends
2e: Kani: process_block + bitmask consumers
```

Key insight: don't store bitmask vectors (simdjson never does).
Process per-block, carry only 2 u64s between blocks.

## Phase 3: Sequential scanner restructure (#269, #313)

Replace `ChunkIndex::next_quote(pos)` random access with sequential
bitmask iteration (`trailing_zeros` + clear-lowest-bit). The scanner
processes one 64-byte block at a time, consuming ProcessedBlock
bitmasks directly. This is simdjson's Stage 2 pattern.

```
3a: Scanner consumes ProcessedBlock instead of ChunkIndex
3b: Remove ChunkIndex stored vectors
3c: Rename ScanBuilder → FieldSink
3d: Kani bounded proof for scan_line
3e: proptest oracle: scanner vs serde_json
```

## Phase 4: Zero-copy Bytes pipeline (#303)

```
4a: Bytes-based Reader (BytesMut → freeze → Bytes)
4b: Connect streaming scanner to Bytes pipeline
4c: StreamingBuilder receives Bytes directly → StringViewArray
4d: Delete format.rs, json_buf accumulation
```

## Phase 5: Pipeline state machine + BatchToken (#270)

Extract run_async decisions into pure state machine in core.
Kani exhaustive single-step. BatchToken #[must_use] linear type.

## Phase 6: proptest state machines + CI hardening (#271)

proptest-state-machine for stateful components.
Proof coverage enforcement. cargo-mutants. cargo-vet.

## Phase 7: TLA+ pipeline specification (#272)

Model batching/timeout/shutdown protocol. Prove liveness and fairness.

## Phase 8: Tighten logfwd-core (#267)

```
#![no_std] + extern crate alloc
#![forbid(unsafe_code)]
SIMD backends in logfwd-arrow, scalar fallback in core
cargo build --target thumbv6m-none-eabi
```

## Parallel work

| Issue | What | Status |
|-------|------|--------|
| #273 | Fix offset_of u32 truncation | Copilot |
| #274 | Fix row_count u32 overflow | Copilot |
| #275 | Fix CRI silent truncation | Open |
| #285 | Fix OTLP type suffix assumption | Copilot |
| #287 | Fix dup key detection >64 fields | Copilot |
| #288 | Fix RawParser non-UTF-8 | Copilot |
| #304 | Dead code cleanup | Copilot |
| #305 | Multi-file SIMD benchmark | After #313 |
| #278 | Safe indexing benchmark | After #313 |
| #279 | Arrow version upgrade | Open |
| #308 | Rethink _raw column | Open |

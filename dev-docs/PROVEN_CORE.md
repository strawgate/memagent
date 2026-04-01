# Proven Core Design

logfwd-core is being restructured into a formally verified crate.
This document covers what's in scope, how verification works, and
how to contribute.

## Why

We found 4 CRITICAL silent data corruption bugs during an implicit
contracts analysis. We've had 3 concurrency bugs in the async pipeline.
The codebase already has a natural split: pure logic vs IO/async/Arrow.
We're formalizing it.

A bug in the proven core means silent data corruption downstream.
A bug outside the core means a crash, error message, or wrong
dashboard — things you notice.

## What's in logfwd-core

| Module | What it does | Verification |
|--------|-------------|-------------|
| structural.rs | Escape detection, quote classification, SIMD structural detection (u64 bitmask ops) | Kani exhaustive (12 proofs) + proptest (SIMD ≡ scalar) |
| structural_iter.rs | Streaming structural position iterator (block iteration, classify) | Kani exhaustive (2 proofs) |
| framer.rs | Newline framing, line boundary detection | Kani exhaustive + oracle (4 proofs) |
| aggregator.rs | CRI partial line reassembly (P/F merging, zero-copy) | Kani exhaustive (5 proofs) |
| byte_search.rs | Proven byte search (find_byte, rfind_byte) | Kani exhaustive + oracle (2 proofs) |
| scanner.rs | JSON field extraction (generic over ScanBuilder trait) | Kani bounded (skip_ws) + proptest oracle |
| json_scanner.rs | Streaming JSON field scanner using bitmask iteration | Kani bounded (5 proofs) + proptest oracle |
| scan_config.rs | parse_int_fast, parse_float_fast, ScanConfig | Kani exhaustive (2 proofs) |
| cri.rs | CRI log parsing + partial line reassembly | Kani exhaustive (8 proofs) |
| otlp.rs | Protobuf wire format + OTLP encoding + timestamp parsing | Kani exhaustive (20 proofs) + 3 contracts |
| pipeline/lifecycle.rs | Pipeline state machine (ordered ACK, drain, shutdown) | Kani exhaustive (5 proofs) + proptest |
| pipeline/batch.rs | BatchTicket typestate (ack/nack/fail/reject) | Kani exhaustive (5 proofs) + compile-time |

## Rules

1. **`#![no_std]`** — compiler prevents IO. No filesystem, network,
   threads, or async. Use `core::` and `alloc::` only.

2. **`#![forbid(unsafe_code)]`** — no unsafe anywhere. SIMD lives in
   logfwd-arrow behind a safe `CharDetector` trait.

3. **Only dependency: memchr** (with `default-features = false`).
   Adding any dependency requires justification and audit.

4. **Every public function needs a proof or test.** CI checks this.
   Kani proofs for functions where exhaustive verification is feasible.
   proptest for everything else.

5. **No panics in release.** `clippy::unwrap_used`, `clippy::panic`,
   `clippy::indexing_slicing` are all denied.

## Verification tiers

**Tier 1 — Exhaustive (Kani proves for ALL inputs):**
- u64 bitmask operations (compute_real_quotes, prefix_xor, find_char_mask)
- Integer parsing (parse_int_fast, all ≤20 byte inputs)
- Varint encode/decode roundtrip (all u64 values)
- Pipeline state machine (all State×Event pairs, all ack orderings)
- BatchTicket transitions (all state transitions preserve fields)
- classify_bit (structural iterator, all bit positions)
- is_json_delimiter (all 256 byte values)
- Severity parsing (no false positives for any 8-byte input)

**Tier 2 — Bounded (Kani proves for inputs up to size N):**
- parse_cri_line (all CRI lines ≤32 bytes)
- days_from_civil (all valid dates in [1970, 2100])
- NewlineFramer (all 32-byte inputs)
- skip_nested (all 16-byte inputs, both scanner implementations)
- scan_string (all 16-byte inputs)
- CriAggregator P+P+F (all 8-byte messages, max_size ≤32)
- write_json_line (all 8-byte messages with 4-byte prefix)
- parse_timestamp_nanos compositional (all 24-byte inputs)

**Tier 3 — Statistical (proptest, high confidence):**
- SIMD ≡ scalar structural detection (random 64-byte blocks)
- scan_line / scan_streaming for large inputs (oracle vs sonic-rs)
- Scanner consistency (SimdScanner vs StreamingSimdScanner)
- Pipeline event sequences (random events, in_flight_consistent)
- Pipeline ack ordering (shuffled ack orders reach final checkpoint)

**Tier 4 — Compile-time (Rust type system):**
- BatchTicket #[must_use] + consume-on-transition (silent data loss is loud)
- AckReceipt #[must_use] (must pass to apply_ack)

## How to add a function to core

1. Write the function. Pure logic only — no IO, no allocation-heavy
   patterns, no panics.

2. Choose verification tier:
   - Can Kani verify ALL inputs? (small fixed-width types) → Tier 1
   - Can Kani verify bounded inputs? (byte slices ≤256) → Tier 2
   - Neither? → Tier 3 (proptest)

3. Write the proof or test:
   ```rust
   // Tier 1: Kani exhaustive
   #[cfg(kani)]
   mod verification {
       use super::*;
       #[kani::proof]
       #[kani::solver(kissat)]
       fn verify_my_function() {
           let input: u64 = kani::any();
           let result = my_function(input);
           assert!(/* postcondition */);
       }
   }

   // Tier 3: proptest
   #[cfg(test)]
   mod tests {
       use proptest::prelude::*;
       proptest! {
           fn my_function_never_panics(input in any::<Vec<u8>>()) {
               let _ = my_function(&input);
           }
       }
   }
   ```

4. Verify locally:
   ```bash
   cargo kani -p logfwd-core --tests       # Kani proofs
   cargo test -p logfwd-core               # Unit + proptest
   cargo build -p logfwd-core --target thumbv6m-none-eabi  # no_std check
   ```

## The ScanBuilder boundary

The core doesn't know about Arrow. It parses JSON fields and calls
methods on a `ScanBuilder` trait (defined in scanner.rs):

```rust
pub trait ScanBuilder {
    fn begin_batch(&mut self);
    fn begin_row(&mut self);
    fn end_row(&mut self);
    fn resolve_field(&mut self, key: &[u8]) -> usize;
    fn append_str_by_idx(&mut self, idx: usize, value: &[u8]);
    fn append_int_by_idx(&mut self, idx: usize, value: &[u8]);
    fn append_float_by_idx(&mut self, idx: usize, value: &[u8]);
    fn append_null_by_idx(&mut self, idx: usize);
    fn append_raw(&mut self, line: &[u8]);
}
```

logfwd-arrow implements this with StreamingBuilder/StorageBuilder,
producing Arrow RecordBatches. The core is verified independently
of Arrow — proptest uses a mock ScanBuilder that records calls.

## SIMD split

SIMD structural detection lives in structural.rs (`find_structural_chars`
via `wide` crate). The scalar fallback (`find_structural_chars_scalar`)
is Kani-provable. proptest verifies SIMD matches scalar for random inputs.

## Kani mechanics

- Proofs use `#[cfg(kani)]` — invisible to normal builds
- bolero unified harnesses run as test + fuzz + Kani proof
- Default solver: kissat (up to 265x faster than MiniSat)
- Function contracts (`kani::requires`/`kani::ensures`) enable
  compositional verification via `stub_verified`
- memchr stubbed with naive loop for Kani, real impl at runtime

## Implementation status

See GitHub epic #262 for current progress across 6 phases.
Phase 0 (Kani prototype) is the go/no-go gate.

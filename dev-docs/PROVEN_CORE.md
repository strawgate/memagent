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
| classify.rs | Escape detection, quote classification (u64 bitmask ops) | Kani exhaustive |
| scan.rs | JSON field extraction (generic over FieldSink trait) | Kani bounded + proptest oracle |
| scan_config.rs | parse_int_fast, parse_float_fast, ScanConfig | Kani exhaustive |
| format.rs | Line boundary detection (JsonParser, RawParser, CriParser) | proptest state-machine |
| cri.rs | CRI log parsing + partial line reassembly | proptest state-machine |
| otlp.rs | Protobuf wire format + OTLP encoding | Kani exhaustive (wire) + proptest |
| pipeline/state.rs | Pipeline state machine (flush, drain, shutdown) | Kani exhaustive + TLA+ |
| pipeline/token.rs | BatchToken linear type (ack/nack) | compile-time |
| sink.rs | FieldSink trait definition | trait contract |
| error.rs | ParseError enum | trivial |

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
- u64 bitmask operations (compute_real_quotes, prefix_xor)
- Integer parsing (parse_int_fast, all ≤20 byte inputs)
- Varint encode/decode roundtrip (all u64 values)
- Pipeline state machine (all State×Event pairs)

**Tier 2 — Bounded (Kani proves for inputs up to size N):**
- scan_line (all JSON lines ≤128 bytes with ≤8 fields)
- parse_cri_line (all CRI lines ≤256 bytes)
- days_from_civil (all valid dates in [1970, 2100])

**Tier 3 — Statistical (proptest, high confidence):**
- FormatParser state machines (random chunk sequences)
- CriReassembler (random feed/reset sequences)
- scan_line for large inputs (oracle vs serde_json)
- Pipeline event sequences (random events, no-data-abandoned)

**Tier 4 — Compile-time (Rust type system):**
- BatchToken #[must_use] + Drop (silent data loss is loud)
- ParseError #[non_exhaustive] (adding variants is non-breaking)
- FieldSink sealed trait (controlled implementations)

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

## The FieldSink boundary

The core doesn't know about Arrow. It parses JSON fields and calls
methods on a `FieldSink` trait:

```rust
pub trait FieldSink {
    fn begin_row(&mut self);
    fn end_row(&mut self);
    fn resolve_field(&mut self, key: &[u8]) -> usize;
    fn field_str(&mut self, idx: usize, value: &[u8]);
    fn field_int(&mut self, idx: usize, value: &[u8]);
    fn field_float(&mut self, idx: usize, value: &[u8]);
    fn field_null(&mut self, idx: usize);
}
```

logfwd-arrow implements this with StreamingBuilder/StorageBuilder,
producing Arrow RecordBatches. The core is verified independently
of Arrow — Kani uses a mock FieldSink that records calls.

## SIMD split

Core defines `CharDetector` trait (safe). logfwd-arrow implements
it with SIMD (unsafe). Core provides a scalar fallback that Kani
proves correct. proptest verifies SIMD matches scalar.

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

# Formal Verification Guide (Kani)

This project uses [Kani](https://model-checking.github.io/kani/) for formal verification of its high-performance data processing kernel.

## Verification Strategy: Tier 1

We follow a "Tier 1" strategy: verify small, pure functions with clear specifications rather than complex, stateful systems.

### 1. Compositional Proofs

Avoid monolithic proofs. If a function `A` calls `B`, and `B` is already verified:

1. Define a **Contract** for `B` using `kani::assume`.
2. Use `kani::stub` to replace `B` with its contract when verifying `A`.
3. This prevents state-space explosion and keeps proof times low.

### 2. No-Panic Safety

Every parser and wire-helper MUST have a no-panic proof:

```rust
#[cfg(kani)]
#[kani::proof]
fn verify_my_parser_no_panic() {
    let buf: [u8; 16] = kani::any();
    let _ = my_parser(&buf); // Should never panic
}
```

### 3. Oracle Testing

For complex logic where a "naive" implementation is easy but slow (like calendar math or string searching), use the naive version as a **Reference Oracle** in your Kani proof.

```rust
let result = fast_impl(input);
let expected = naive_oracle(input);
assert_eq!(result, expected);
```

### 4. Memory Safety & Overflows

Always verify that arithmetic operations (especially for timestamps and offsets) use checked math or have `kani::assume` guards that reflect real-world constraints.

## Running Proofs

```bash
# Run all proofs in a crate
cargo kani

# Run a specific harness
cargo kani --harness verify_my_function
```

## When to add a proof?

### MUST add Kani proofs for:

- **Parsers**: Any logic handling raw bytes (`framer.rs`, `cri.rs`, `json_scanner.rs`)
  - Example: `verify_newline_framer_no_panic` in `framer.rs:210`
- **Wire Formats**: Protobuf/Varint encoding and size calculations (`otlp.rs`)
  - Must prove no panic on all valid inputs within bounded size
- **Bitmask Logic**: SIMD-adjacent bit manipulation (`structural.rs`)
  - Example: `verify_prefix_xor` for escape sequence detection
- **Structural Operations**: JSON structural character detection
  - Example: `verify_compute_real_quotes` with oracle in `structural.rs:350`
- **Byte Search**: Low-level string/byte search primitives (`byte_search.rs`)
  - Must prove correctness matches naive oracle
- **State Machines**: Protocol state transitions with P/F flags (`aggregator.rs`)
  - Must prove state invariants hold across all transitions

### SHOULD add Kani proofs for:

- **Arithmetic**: Calendar math, unit conversions, division/remainder operations
- **Iterator Logic**: Complex iteration with early exit conditions
- **Buffer Management**: Index calculations, range validation

### DO NOT add Kani proofs for:

- **I/O Operations**: File/network operations (not pure, not tractable)
- **Async Logic**: Runtime scheduling, futures (not supported by Kani)
- **Large State Machines**: > 8-10 state transitions (use proptest instead)
- **Heap-Heavy Code**: Extensive Vec/HashMap operations (use proptest + Miri)
- **Trivial Getters/Setters**: Simple field accessors without logic
- **Integration Tests**: Multi-crate interactions (use integration tests)

## Best Practices

### Guard against vacuous proofs with `kani::cover!()`

Every proof with `kani::assume()` or constrained inputs should include cover
statements verifying that interesting paths are reachable. If a cover reports
UNSATISFIABLE, the proof may be vacuously true.

```rust
// After assertions, add covers for boundary cases
kani::cover!(count > 0, "at least one match found");
kani::cover!(count == 0, "no matches (empty case exercised)");
```

Do NOT blanket-add covers to every proof. Add them selectively when:
- The proof uses `kani::assume()`
- The proof has complex branching where vacuity is non-obvious
- You want to confirm specific edge cases are exercised

### Use symbolic exploration, not hardcoded orderings

When verifying order-independent properties (e.g., ack permutations), use
`kani::any_where()` to let Kani explore all orderings symbolically:

```rust
let order: u8 = kani::any_where(|&o: &u8| o < 6);
let (first, second, third) = match order { /* all 6 permutations */ };
```

### Build independent oracles

Don't trust internal state as the correctness oracle. Compute expected values
independently from the raw input. For iterators, count expected items by
scanning the input buffer directly rather than trusting the iterator's internal
bitmask.

### Select solvers for complex proofs

Add `#[kani::solver(kissat)]` to proofs that take > 10 seconds. Published
benchmarks from large-scale QUIC protocol verification show kissat is fastest
for ~47% of slow harnesses, with speedups up to 265x over minisat. For
arithmetic-heavy proofs, try `z3` or `bitwuzla`.
See: https://model-checking.github.io/kani-verifier-blog/2023/08/03/turbocharging-rust-code-verification.html

### Verify complete partitions

When a function splits input into ranges (line extraction, tokenization),
prove that bytes outside all ranges are delimiters, and that the empty-range
case means all bytes are delimiters. This catches off-by-one errors at
boundaries that range-only checks miss.

### Prefer `kani::any_where()` over separate `any()` + `assume()`

Keep constraints co-located with value generation. Use `assume()` only when
constraints span multiple variables or depend on computed state.

### Compositionality scales; monolithic proofs don't

For deep call chains, add `#[kani::requires]` / `#[kani::ensures]` contracts
to leaf functions, verify them with `proof_for_contract`, then use
`stub_verified` in higher-level proofs. This prevents exponential state-space
explosion by decomposing verification into N independent proofs, each
verifying a bounded slice of the call graph.

**Current usage:** Only 1 function contract in the codebase. Opportunity to
expand to parser sub-components (e.g., `parse_timestamp`, `parse_stream_flag`).

## Proof Naming Convention

Use descriptive names that indicate what property is being verified:

- `verify_<function>_no_panic` — Proves function never panics for any valid input
- `verify_<function>_correct` — Proves correctness against oracle or specification
- `verify_<function>_<property>` — Proves specific property (e.g., `_max_size`, `_ranges_valid`)
- `verify_<function>_rejects_<case>` — Proves function correctly rejects invalid inputs

**Example from codebase:**
```rust
verify_newline_framer_no_panic          // framer.rs:210
verify_newline_framer_ranges_valid      // framer.rs:217
verify_compute_real_quotes              // structural.rs:350
verify_find_byte_correct                // byte_search.rs:45
```

## Input Size Guidelines

Different operations have different practical bounds in Kani:

| Operation Type | Practical Max | Example |
|----------------|---------------|---------|
| Loop-free bitmask ops | Full 64-bit range | `prefix_xor` with u64 |
| Simple byte scanning | ~100 bytes (3-4s) | Newline framing |
| Complex parsing | ~16-32 bytes | CRI line parsing, JSON |
| Vec operations | ~8-16 elements | Use fixed arrays or InlineVec |
| State transitions | ~8-10 steps | P+P+P+F sequences |

**From the codebase:**
- `framer.rs`: Uses `[u8; 32]` for framing proofs
- `aggregator.rs`: Max message size bounded to 32 bytes in proofs
- `structural.rs`: Full 64-bit range for bitmask operations

When choosing input sizes, balance thoroughness with proof time. Start small
(8-16 bytes), measure proof time, then increase if tractable.

## Cover Statement Guidelines

Add `kani::cover!()` statements **only when needed** to guard against vacuity:

### When to add covers:

1. **After kani::assume() constraints**
   ```rust
   let x: usize = kani::any();
   kani::assume(x < max_size && x % 2 == 0);
   // MUST add cover to verify constraint isn't too restrictive
   kani::cover!(x > 0, "non-zero values reachable");
   ```

2. **Complex branching with multiple paths**
   ```rust
   // Prove both success and error paths are exercised
   kani::cover!(result.is_ok(), "success path reachable");
   kani::cover!(result.is_err(), "error path reachable");
   ```

3. **Edge cases you want to confirm are tested**
   ```rust
   kani::cover!(count == 0, "empty case verified");
   kani::cover!(count > 0, "non-empty case verified");
   kani::cover!(count == MAX, "maximum case verified");
   ```

### When NOT to add covers:

- Fully unconstrained symbolic inputs (`let x: u32 = kani::any()` with no assume)
- Simple no-panic proofs without branching
- Trivial assertions that obviously execute

**Current coverage:** 11 cover statements across 83 proofs. Could expand to
proofs with `kani::assume()` constraints (found in `structural.rs`, `aggregator.rs`).

## Kani vs Proptest vs Miri

| Technique | Best for | Limitation |
|-----------|----------|------------|
| **Kani** | Exhaustive bounded inputs; unsafe code; pure functions | Slow on wide types, no concurrency |
| **Proptest** | End-to-end integration; heap-intensive code | Incomplete coverage |
| **Miri** | UB detection; concurrency; allocator behavior | Not exhaustive |

If the function is pure, bounded, and critical -- prove it with Kani.
If it's stateful, heap-heavy, or async -- test with proptest. For unsafe
code, do both.

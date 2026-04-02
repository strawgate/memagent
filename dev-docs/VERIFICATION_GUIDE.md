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

- **Parsers**: Any logic handling raw bytes.
- **Wire Formats**: Protobuf/Varint encoding and size calculations.
- **Bitmask Logic**: SIMD-adjacent bit manipulation (e.g., `ChunkIndex`).
- **Math**: Calendar math, unit conversions, or anything involving `div` or `rem`.

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

## Kani vs Proptest vs Miri

| Technique | Best for | Limitation |
|-----------|----------|------------|
| **Kani** | Exhaustive bounded inputs; unsafe code; pure functions | Slow on wide types, no concurrency |
| **Proptest** | End-to-end integration; heap-intensive code | Incomplete coverage |
| **Miri** | UB detection; concurrency; allocator behavior | Not exhaustive |

If the function is pure, bounded, and critical -- prove it with Kani.
If it's stateful, heap-heavy, or async -- test with proptest. For unsafe
code, do both.

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

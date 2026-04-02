# Copilot Instructions for logfwd

Read `AGENTS.md` in the repository root for full project context, crate structure, reference docs, and code quality requirements.

## Quick Reference

- **Language:** Rust (stable toolchain, no hand-written unsafe — SIMD via wide crate)
- **Performance target:** 1.7M lines/sec CRI parsing + OTLP encoding, single-core ARM64
- **Build:** `just ci` (lint + test — run before any PR)
- **Test:** `just test`
- **Lint:** `just lint` (clippy + rustfmt + taplo + cargo-deny)

## What NOT to Do

- Do not add dependencies without justification
- Do not refactor code unrelated to your task
- Do not skip `just ci` before committing
- Do not introduce allocations in hot paths without benchmarking
- Do not use `.unwrap()` in production code paths
- Do not skip Kani proofs for parsers, wire formats, or bitmask operations

## Kani Verification Rules

When modifying code in `logfwd-core`, follow these Kani verification rules:

### MUST Add Kani Proof When:

1. Implementing or modifying parsers (framer.rs, cri.rs, json_scanner.rs)
2. Adding wire format encoding/decoding logic (otlp.rs)
3. Implementing bitmask operations (structural.rs)
4. Adding byte search primitives (byte_search.rs)
5. Modifying state machine logic (aggregator.rs)

### Proof Requirements:

- Use `#[cfg(kani)]` guard
- Name proofs: `verify_<function>_<property>`
- Add `#[kani::unwind(N)]` for loops (N = max_iterations + 2)
- Use bounded input sizes: 8-32 bytes for parsing, full u64 for bitmasks
- Add `kani::cover!()` after any `kani::assume()` to guard vacuity

### Example Template:

```rust
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(34)]
fn verify_my_parser_no_panic() {
    let input: [u8; 32] = kani::any();
    let _ = my_parser(&input);
}
```

See `docs/KANI_PATTERNS.md` for complete templates and `docs/VERIFICATION_GUIDE.md` for guidelines.


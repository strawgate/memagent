# logfwd-core Agent Guide

Read `README.md` in this directory for crate constraints and module map.

Key rules:
- `#![no_std]` + `#![forbid(unsafe_code)]` — the compiler enforces these, not lints
- Only deps: memchr + wide
- Every new public function requires a Kani proof (proptest only for async/heap-heavy code Kani can't reach)
- No `.unwrap()`, no panics, no indexing — use `?` or `.get()`
- See `../../dev-docs/VERIFICATION.md` for proof requirements and exemptions

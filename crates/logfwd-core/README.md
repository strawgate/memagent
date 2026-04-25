# logfwd-core

Proven pure-logic kernel. Scanner, parsers, pipeline state machine, OTLP encoding.

## Constraints

| Rule | Enforcement |
|------|-------------|
| `#![no_std]` + alloc | Compiler. CI: `cargo build --target thumbv6m-none-eabi` |
| `#![forbid(unsafe_code)]` | Compiler. Cannot be overridden with `#[allow]`. |
| Only deps: memchr + wide + logfwd-kani + logfwd-lint-attrs | CI dependency allowlist check |
| No panics | `clippy::unwrap_used`, `clippy::panic`, `clippy::indexing_slicing` = deny |
| Proof-bearing core modules stay Kani-covered | CI Kani job + `dev-docs/VERIFICATION.md` inventory |
| No IO, no threads, no async | Structural (`no_std` removes the APIs) |

## Key modules

- `json_scanner.rs` — streaming JSON field extraction via ScanBuilder callbacks
- `scanner.rs` — scanner-to-builder protocol (`ScanBuilder`, `BuilderState`)
- `structural.rs` — SIMD structural character detection (quote, backslash, bitmasks)
- `cri.rs` — CRI log format parser + reassembler
- `framer.rs` — NewlineFramer (Kani-proven, fixed-size, no heap)
- `reassembler.rs` — CriReassembler (zero-copy F path, Kani-proven)
- `pipeline/` — BatchTicket, PipelineMachine (typestate, Kani-proven)
- `otlp.rs` — OTLP protobuf encoding

## Verification

Critical proof-bearing public APIs must stay covered by Kani or proptest. See `dev-docs/VERIFICATION.md`.
Kani proofs live in `#[cfg(kani)] mod verification {}` at the bottom of each file.

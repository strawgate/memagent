# logfwd-core

Proven pure-logic kernel. Scanner, parsers, pipeline state machine, OTLP encoding.

## Constraints

| Rule | Enforcement |
|------|-------------|
| `#![no_std]` + alloc | Compiler. CI: `cargo build --target thumbv6m-none-eabi` |
| `#![forbid(unsafe_code)]` | Compiler. Cannot be overridden with `#[allow]`. |
| Only deps: memchr + wide | CI dependency allowlist check |
| No panics | `clippy::unwrap_used`, `clippy::panic`, `clippy::indexing_slicing` = deny |
| Every public fn has a proof | CI proof coverage script |
| No IO, no threads, no async | Structural (`no_std` removes the APIs) |

## Key modules

- `scanner.rs` — JSON field extraction via ScanBuilder trait callbacks
- `structural.rs` — SIMD structural character detection (quote, backslash, bitmasks)
- `cri.rs` — CRI log format parser + reassembler
- `framer.rs` — NewlineFramer (Kani-proven, fixed-size, no heap)
- `aggregator.rs` — CriReassembler (zero-copy F path, Kani-proven)
- `pipeline/` — BatchTicket, PipelineMachine (typestate, Kani-proven)
- `otlp.rs` — OTLP protobuf encoding

## Verification

Every public function must have a Kani proof or proptest. See `dev-docs/VERIFICATION.md`.
Kani proofs live in `#[cfg(kani)] mod verification {}` at the bottom of each file.

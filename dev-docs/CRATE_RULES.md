# Crate Rules

Rules and constraints for each crate. Enforced by CI, not just convention.

## logfwd-core (proven)

| Rule | Enforcement |
|------|-------------|
| `#![no_std]` + alloc | Compiler. CI: `cargo build --target thumbv6m-none-eabi` |
| `#![forbid(unsafe_code)]` | Compiler. Cannot be overridden with `#[allow]`. |
| Only deps: memchr + wide | CI dependency allowlist check |
| No panics | `clippy::unwrap_used`, `clippy::panic`, `clippy::indexing_slicing` = deny |
| Every public fn has a proof | CI proof coverage script |
| No IO, no threads, no async | Structural (no_std removes the APIs) |

## logfwd-arrow

| Rule | Enforcement |
|------|-------------|
| unsafe allowed (SIMD only) | Code review. SIMD impls only. |
| Implements core's ScanBuilder + CharDetector traits | Compilation |
| proptest: SIMD output == scalar output | CI test suite |
| Deps: core + arrow + bytes | Cargo.toml |

## logfwd-io

| Rule | Enforcement |
|------|-------------|
| IO is expected here | — |
| Tests use tempfiles, not real filesystems | Convention |
| Deps: core + arrow + notify + serde | Cargo.toml |

## logfwd-transform

| Rule | Enforcement |
|------|-------------|
| DataFusion is the SQL engine | Architecture decision |
| Enrichment tables implement Arrow's RecordBatchReader | Convention |
| Deps: core + arrow + datafusion | Cargo.toml |

## logfwd-output

| Rule | Enforcement |
|------|-------------|
| Uses core for encoding (OTLP protobuf) | Architecture |
| Transport is separate from serialization | Convention |
| Deps: core + arrow + ureq/reqwest | Cargo.toml |

## logfwd (binary)

| Rule | Enforcement |
|------|-------------|
| Async orchestration only — no business logic | Code review |
| Pipeline decisions go through core state machine | Architecture |
| Deps: everything + tokio + bytes | Cargo.toml |
| BytesMut/Bytes used for pipeline buffer accumulation | Architecture |

## Adding a new crate

1. Define its purpose in one sentence
2. List its allowed dependencies
3. Add rules to this file
4. Create per-crate AGENTS.md with the rules
5. Add CI checks for any structural rules

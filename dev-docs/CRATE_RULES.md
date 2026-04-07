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
| Pure seam Kani boundary status tracked in `dev-docs/verification/kani-boundary-contract.toml` | CI script: `python3 scripts/verify_kani_boundary_contract.py` |

## logfwd-types

| Rule | Enforcement |
|------|-------------|
| Shared pure state-machine and diagnostics value semantics live here | Architecture |
| Lock-free stats storage may live alongside pure value modules, but not inside them | Code review |
| Pure seam Kani boundary status tracked in `dev-docs/verification/kani-boundary-contract.toml` | CI script: `python3 scripts/verify_kani_boundary_contract.py` |

## logfwd-io

| Rule | Enforcement |
|------|-------------|
| IO is expected here | — |
| Tests use tempfiles, not real filesystems | Convention |
| Deps: core + arrow + notify + serde | Cargo.toml |
| `InputSource` implementations must define `health()` explicitly; no optimistic trait default | Compilation + code review |
| Pure seam Kani boundary status tracked in `dev-docs/verification/kani-boundary-contract.toml` | CI script: `python3 scripts/verify_kani_boundary_contract.py` |

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
| Pure seam Kani boundary status tracked in `dev-docs/verification/kani-boundary-contract.toml` | CI script: `python3 scripts/verify_kani_boundary_contract.py` |

## logfwd (binary)

| Rule | Enforcement |
|------|-------------|
| Async orchestration only — no business logic | Code review |
| Pipeline decisions go through core state machine | Architecture |
| Deps: everything + tokio + bytes | Cargo.toml |
| BytesMut/Bytes used for pipeline buffer accumulation | Architecture |
| Pure seam Kani boundary status tracked in `dev-docs/verification/kani-boundary-contract.toml` | CI script: `python3 scripts/verify_kani_boundary_contract.py` |

## Adding a new crate

1. Define its purpose in one sentence
2. List its allowed dependencies
3. Add rules to this file
4. Create per-crate AGENTS.md with the rules
5. Add CI checks for any structural rules

## Crate boundary examples (good vs bad)

### Good: core logic stays in `logfwd-core`

- Parsing primitives, framing logic, and deterministic state transitions live in `logfwd-core`.
- Platform or transport integration layers call core APIs without reimplementing core semantics.

### Bad: business logic in the binary crate

- Avoid adding transform semantics, framing behavior, or output encoding rules directly in `logfwd`.
- The binary should orchestrate startup/wiring, not own domain logic.

### Good: IO and transport concerns stay out of core

- File watching, sockets, retries, and collector-specific transport behavior belong in IO/output layers.
- `logfwd-core` remains pure and portable with no OS dependencies.

### Bad: leaking heavy dependencies across boundaries

- Do not introduce `datafusion` into crates that should remain lightweight parsing/runtime layers.
- Do not add IO/network crates to proof-oriented core crates.

When uncertain, prefer the narrower dependency surface and document the decision in this file.

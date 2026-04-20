# Crate Rules

Rules and constraints for each crate. Enforced by CI, not just convention.

## logfwd-core (proven)

| Rule | Enforcement |
|------|-------------|
| `#![no_std]` + alloc | Compiler. CI: `cargo build --target thumbv6m-none-eabi` |
| `#![forbid(unsafe_code)]` | Compiler. Cannot be overridden with `#[allow]`. |
| Only deps: memchr + wide | CI dependency allowlist check |
| No panics | `clippy::unwrap_used`, `clippy::panic`, `clippy::indexing_slicing` = deny |
| Every public item documented | `#![warn(missing_docs)]` at crate root |
| No stdout/stderr writes | `clippy::print_stdout`, `clippy::print_stderr` = warn workspace-wide; no `#![allow]` opt-out |
| Proof-bearing core modules stay Kani-covered | CI Kani job + `dev-docs/VERIFICATION.md` inventory |
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
| Every public item documented | `#![warn(missing_docs)]` at crate root |
| No stdout/stderr writes | `clippy::print_stdout`, `clippy::print_stderr` = warn workspace-wide; no `#![allow]` opt-out |
| Pure seam Kani boundary status tracked in `dev-docs/verification/kani-boundary-contract.toml` | CI script: `python3 scripts/verify_kani_boundary_contract.py` |

## logfwd-config

| Rule | Enforcement |
|------|-------------|
| YAML schema parsing and validation only — no I/O, no transforms | Architecture |
| Parse-don't-validate: `Result<TypedConfig, ConfigError>`, never `bool` validators | Code review |
| Every public enum carries `#[non_exhaustive]` unless the closed set is genuinely stable | Code review |
| No stdout/stderr writes | `clippy::print_stdout`, `clippy::print_stderr` = warn workspace-wide; no `#![allow]` opt-out |
| **TODO:** enable `#![warn(missing_docs)]` once the ~280 existing schema gaps are documented (tracked separately; do not regress new public items) | Code review for now |

## logfwd-io

| Rule | Enforcement |
|------|-------------|
| IO is expected here | — |
| Tests use tempfiles, not real filesystems | Convention |
| Deps: core + arrow + notify + serde + logfwd-diagnostics | Cargo.toml |
| `InputSource` implementations must define `health()` explicitly; no optimistic trait default | Compilation + code review |
| `InputSource` trait-shape changes must pass cross-workspace compile checks for turmoil tests and bench binaries (`cargo test -p logfwd --features turmoil --test turmoil_sim --no-run`, `cargo build -p logfwd-bench --bin framed_input_profile`) | CI/local verification checklist |
| Pure seam Kani boundary status tracked in `dev-docs/verification/kani-boundary-contract.toml` | CI script: `python3 scripts/verify_kani_boundary_contract.py` |
| **No raw payload injection.** Source metadata must never be written into raw input bytes. `_source_path`, `_source_id`, `_input`, UDP sender identity, and future source descriptors must be attached from sidecar metadata after scan, before SQL, or exposed through cold-path enrichment tables. The CI guard scans Rust crate code for legacy injection helpers/toggles and raw byte writes of guarded source metadata fields. | CI guard script (`python3 scripts/check_no_raw_payload_injection.py`) + code review |

## logfwd-diagnostics

| Rule | Enforcement |
|------|-------------|
| Owns diagnostics server, telemetry shaping, and stderr/span buffering | Architecture |
| Allowed deps: `logfwd-types`, `tiny_http`, `tracing`, `serde(_yaml_ng/_json)`, `opentelemetry(_sdk)`, `url`, `libc` | Cargo.toml + review |
| Owns diagnostics dashboard asset `crates/logfwd-diagnostics/src/dashboard.html` (generated from `dashboard/`) | Dashboard build + code review |
| Must not depend on runtime orchestration crates (`logfwd-runtime`, `logfwd-output`) | Cargo dependency graph + review |
| Kani seams for readiness policy and stderr escaping stay tracked in `dev-docs/verification/kani-boundary-contract.toml` | CI script: `python3 scripts/verify_kani_boundary_contract.py` |

## logfwd-config-wasm

| Rule | Enforcement |
|------|-------------|
| WASM bindings for the config validator; must expose the same parsing/validation surface as `logfwd-config` so browser and Node.js hosts reject invalid YAML with identical diagnostics | Architecture |
| Allowed deps: `logfwd-config`, `wasm-bindgen`, `serde`, `serde_json`, `js-sys` | Cargo.toml + review |
| Must not depend on runtime, I/O, or transform crates — the validator is pure-config parsing | Cargo dependency graph + review |

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
| User-facing sinks drop only known FastForward internal columns such as `__source_id`; user payload fields that merely start with `__` must be preserved | Unit/integration tests + code review |
| No `panic!`/`todo!`/`unimplemented!` in production paths | CI script: `python3 scripts/check_no_panic_in_production.py` (test modules and `// ALLOW-PANIC: <reason>` lines are exempt) |
| Public errors are `thiserror` enums, not `Box<dyn Error>` | CI script: `python3 scripts/check_no_box_dyn_error.py` |
| Pure seam Kani boundary status tracked in `dev-docs/verification/kani-boundary-contract.toml` | CI script: `python3 scripts/verify_kani_boundary_contract.py` |

## logfwd-bench

| Rule | Enforcement |
|------|-------------|
| Owns Criterion benchmarks and standalone profiling binaries for scanner, pipeline, output, and source metadata performance | Cargo.toml + just recipes |
| Benchmark-only dependencies stay isolated here unless also used by production or test crates | Cargo.toml + dependency review |
| Source metadata benchmarks must keep raw JSON metadata injection only as a historical comparison baseline, never as production guidance | Code review |

## logfwd-runtime

| Rule | Enforcement |
|------|-------------|
| Owns async pipeline orchestration, worker pool, and processor chain | Architecture |
| Domain logic stays in lower crates when a pure seam exists | Code review |
| Feature forwarding must preserve `datafusion` and `turmoil` behavior for downstream `logfwd` users | Compilation |
| No `panic!`/`todo!`/`unimplemented!` in production paths | CI script: `python3 scripts/check_no_panic_in_production.py` (test modules and `// ALLOW-PANIC: <reason>` lines are exempt) |
| Public errors are `thiserror` enums, not `Box<dyn Error>` | CI script: `python3 scripts/check_no_box_dyn_error.py` |
| Pure seam Kani boundary status tracked in `dev-docs/verification/kani-boundary-contract.toml` | CI script: `python3 scripts/verify_kani_boundary_contract.py` |

## logfwd (binary / facade)

| Rule | Enforcement |
|------|-------------|
| CLI/bootstrap only — no long-lived runtime orchestration | Code review |
| Public re-exports must stay compatibility-only and not fork runtime behavior | Code review |
| Pipeline decisions still go through core state machine via `logfwd-runtime` | Architecture |
| Pure seam Kani boundary status tracked in `dev-docs/verification/kani-boundary-contract.toml` | CI script: `python3 scripts/verify_kani_boundary_contract.py` |

## sensor-ebpf (prototype)

| Rule | Enforcement |
|------|-------------|
| Standalone eBPF sensor binary for manual testing. Linux-only. Not part of pipeline. | Convention |

## sensor-ebpf-common

| Rule | Enforcement |
|------|-------------|
| Shared `repr(C)` event types for eBPF kernel↔userspace boundary. `no_std` compatible. | Compilation |

## sensor-ebpf-kern (excluded)

| Rule | Enforcement |
|------|-------------|
| eBPF kernel programs. Requires nightly + `bpfel-unknown-none` target. Excluded from workspace. | Cargo.toml `exclude` |

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

# Crate Rules

Rules and constraints for each crate. Enforced by CI, not just convention.

## ffwd-core (proven)

| Rule | Enforcement |
|------|-------------|
| `#![no_std]` + alloc | Compiler. CI: `cargo build --target thumbv6m-none-eabi` |
| `#![forbid(unsafe_code)]` | Compiler. Cannot be overridden with `#[allow]`. |
| Only deps: memchr + wide + ffwd-kani | CI dependency allowlist check |
| No panics | `clippy::unwrap_used`, `clippy::panic`, `clippy::indexing_slicing` = deny |
| Every public item documented | `#![warn(missing_docs)]` at crate root |
| No stdout/stderr writes | `clippy::print_stdout`, `clippy::print_stderr` = warn workspace-wide; no `#![allow]` opt-out |
| Proof-bearing core modules stay Kani-covered | CI Kani job + `dev-docs/VERIFICATION.md` inventory |
| No IO, no threads, no async | Structural (no_std removes the APIs) |

## ffwd-kani (verification oracles)

| Rule | Enforcement |
|------|-------------|
| `#![no_std]` | Compiler |
| Zero external dependencies | Cargo.toml |
| Oracle functions are reference implementations only — never called in production paths | Code review |
| Every oracle documented at its definition site: what it computes, what it verifies (if any), known limitations | Code review |
| Every public item documented | `#![warn(missing_docs)]` at crate root |
| Internal Kani proofs verify oracle correctness | CI Kani job |

## ffwd-arrow

| Rule | Enforcement |
|------|-------------|
| unsafe allowed (SIMD only) | Code review. SIMD impls only. |
| `#![deny(clippy::indexing_slicing)]` at crate root | `#![deny]` in lib.rs. Hot-path SIMD and batch builder functions use `#[allow]` for internally-controlled indexing. Non-SIMD external data paths should use `.get()` where feasible. |
| Implements core's ScanBuilder + CharDetector traits | Compilation |
| proptest: SIMD output == scalar output | CI test suite |
| Deps: core + arrow + bytes | Cargo.toml |
| Pure seam Kani boundary status tracked in `dev-docs/verification/kani-boundary-contract.toml` | CI script: `python3 scripts/verify_kani_boundary_contract.py` |

## ffwd-types

| Rule | Enforcement |
|------|-------------|
| Shared pure state-machine and diagnostics value semantics live here | Architecture |
| Lock-free stats storage may live alongside pure value modules, but not inside them | Code review |
| Every public item documented | `#![warn(missing_docs)]` at crate root |
| No stdout/stderr writes | `clippy::print_stdout`, `clippy::print_stderr` = warn workspace-wide; no `#![allow]` opt-out |
| Pure seam Kani boundary status tracked in `dev-docs/verification/kani-boundary-contract.toml` | CI script: `python3 scripts/verify_kani_boundary_contract.py` |

## ffwd-config

| Rule | Enforcement |
|------|-------------|
| YAML schema parsing and validation only — no I/O, no transforms | Architecture |
| Parse-don't-validate: `Result<TypedConfig, ConfigError>`, never `bool` validators | Code review |
| Every public enum carries `#[non_exhaustive]` unless the closed set is genuinely stable | Code review |
| No stdout/stderr writes | `clippy::print_stdout`, `clippy::print_stderr` = warn workspace-wide; no `#![allow]` opt-out |
| **TODO:** enable `#![warn(missing_docs)]` once the ~280 existing schema gaps are documented (tracked separately; do not regress new public items) | Code review for now |

## ffwd-io

| Rule | Enforcement |
|------|-------------|
| IO is expected here | — |
| Tests use tempfiles, not real filesystems | Convention |
| Deps: core + arrow + notify + serde + ffwd-diagnostics | Cargo.toml |
| `InputSource` implementations must define `health()` explicitly; no optimistic trait default | Compilation + code review |
| `InputSource` trait-shape changes must pass cross-workspace compile checks for turmoil tests and bench binaries (`cargo test -p ffwd --features turmoil --test turmoil_sim --no-run`, `cargo build -p ffwd-bench --features bench-tools --bin framed_input_profile`) | CI/local verification checklist |
| Pure seam Kani boundary status tracked in `dev-docs/verification/kani-boundary-contract.toml` | CI script: `python3 scripts/verify_kani_boundary_contract.py` |
| **No raw payload injection.** Source metadata must never be written into raw input bytes. `_source_path`, `_source_id`, `_input`, UDP sender identity, and future source descriptors must be attached from sidecar metadata after scan, before SQL, or exposed through cold-path enrichment tables. The CI guard scans Rust crate code for legacy injection helpers/toggles and raw byte writes of guarded source metadata fields. | CI guard script (`python3 scripts/check_no_raw_payload_injection.py`) + code review |

## ffwd-diagnostics

| Rule | Enforcement |
|------|-------------|
| Owns diagnostics server, telemetry shaping, and stderr/span buffering | Architecture |
| Allowed deps: `ffwd-types`, `tiny_http`, `tracing`, `serde(_yaml_ng/_json)`, `opentelemetry(_sdk)`, `url`, `libc` | Cargo.toml + review |
| Owns diagnostics dashboard asset `crates/ffwd-diagnostics/src/dashboard.html` (generated from `dashboard/`) | Dashboard build + code review |
| Must not depend on runtime orchestration crates (`ffwd-runtime`, `ffwd-output`) | Cargo dependency graph + review |
| Kani seams for readiness policy and stderr escaping stay tracked in `dev-docs/verification/kani-boundary-contract.toml` | CI script: `python3 scripts/verify_kani_boundary_contract.py` |

## ffwd-config-wasm

| Rule | Enforcement |
|------|-------------|
| WASM bindings for the config validator; must expose the same parsing/validation surface as `ffwd-config` so browser and Node.js hosts reject invalid YAML with identical diagnostics | Architecture |
| Allowed deps: `ffwd-config`, `wasm-bindgen`, `serde`, `serde_json`, `js-sys` | Cargo.toml + review |
| Must not depend on runtime, I/O, or transform crates — the validator is pure-config parsing | Cargo dependency graph + review |

## ffwd-transform

| Rule | Enforcement |
|------|-------------|
| DataFusion is the SQL engine | Architecture decision |
| Enrichment tables implement Arrow's RecordBatchReader | Convention |
| Deps: core + arrow + datafusion | Cargo.toml |

## ffwd-output

| Rule | Enforcement |
|------|-------------|
| Uses core for encoding (OTLP protobuf) | Architecture |
| Transport is separate from serialization | Convention |
| Deps: core + arrow + ureq/reqwest | Cargo.toml |
| User-facing sinks drop only known FastForward internal columns such as `__source_id`; user payload fields that merely start with `__` must be preserved | Unit/integration tests + code review |
| No `panic!`/`todo!`/`unimplemented!` in production paths | CI script: `python3 scripts/check_no_panic_in_production.py` (test modules and `// ALLOW-PANIC: <reason>` lines are exempt) |
| Public errors are `thiserror` enums, not `Box<dyn Error>` | CI script: `python3 scripts/check_no_box_dyn_error.py` |
| Pure seam Kani boundary status tracked in `dev-docs/verification/kani-boundary-contract.toml` | CI script: `python3 scripts/verify_kani_boundary_contract.py` |

## ffwd-bench

| Rule | Enforcement |
|------|-------------|
| Owns Criterion benchmarks and standalone profiling binaries for scanner, pipeline, output, and source metadata performance | Cargo.toml + just recipes |
| Benchmark-only dependencies stay isolated here unless also used by production or test crates | Cargo.toml + dependency review |
| Source metadata benchmarks must keep raw JSON metadata injection only as a historical comparison baseline, never as production guidance | Code review |

## ffwd-lint-attrs

| Rule | Enforcement |
|------|-------------|
| Proc-macro crate only. Exports attribute markers (`#[hot_path]` today, `#[cancel_safe]` / `#[no_panic]` / `#[checkpoint_ordered]` planned) consumed by the dylint lint library | Architecture |
| All attributes must be compile-time no-ops: the function/item behaves identically with or without the attribute | Code review |
| May not depend on any other ffwd crate | Cargo.toml |

## ffwd-lints

| Rule | Enforcement |
|------|-------------|
| Excluded from the main workspace (`Cargo.toml` `exclude` list). Pins its own rustc nightly via `rust-toolchain` and links against `rustc-private` crates | Workspace config |
| Dylint library — compiled as `cdylib`, invoked via `cargo dylint` (wrapped by `just dylint`) | Architecture |
| Every lint that expects a source-level marker looks for the marker the corresponding attribute in `ffwd-lint-attrs` emits (e.g., `#[doc = "__ffwd_hot_path__"]`), because proc-macro attributes are stripped before HIR | Code review |
| Every new lint adds an entry to the lint table in `dev-docs/CODE_STYLE.md` → *Semantic lints (dylint)* and to `crates/ffwd-lints/README.md` | Code review |
| CI integration via `just dylint` (not yet on mandatory path — see roadmap) | `justfile` |

## ffwd-runtime

| Rule | Enforcement |
|------|-------------|
| Owns async pipeline orchestration, worker pool, and processor chain | Architecture |
| Domain logic stays in lower crates when a pure seam exists | Code review |
| Feature forwarding must preserve `datafusion` and `turmoil` behavior for downstream `ffwd` users | Compilation |
| No `panic!`/`todo!`/`unimplemented!` in production paths | CI script: `python3 scripts/check_no_panic_in_production.py` (test modules and `// ALLOW-PANIC: <reason>` lines are exempt) |
| Public errors are `thiserror` enums, not `Box<dyn Error>` | CI script: `python3 scripts/check_no_box_dyn_error.py` |
| Pure seam Kani boundary status tracked in `dev-docs/verification/kani-boundary-contract.toml` | CI script: `python3 scripts/verify_kani_boundary_contract.py` |

## ffwd (binary / facade)

| Rule | Enforcement |
|------|-------------|
| CLI/bootstrap only — no long-lived runtime orchestration | Code review |
| Public re-exports must stay compatibility-only and not fork runtime behavior | Code review |
| Pipeline decisions still go through core state machine via `ffwd-runtime` | Architecture |
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

### Good: core logic stays in `ffwd-core`

- Parsing primitives, framing logic, and deterministic state transitions live in `ffwd-core`.
- Platform or transport integration layers call core APIs without reimplementing core semantics.

### Bad: business logic in the binary crate

- Avoid adding transform semantics, framing behavior, or output encoding rules directly in `ffwd`.
- The binary should orchestrate startup/wiring, not own domain logic.

### Good: IO and transport concerns stay out of core

- File watching, sockets, retries, and collector-specific transport behavior belong in IO/output layers.
- `ffwd-core` remains pure and portable with no OS dependencies.

### Bad: leaking heavy dependencies across boundaries

- Do not introduce `datafusion` into crates that should remain lightweight parsing/runtime layers.
- Do not add IO/network crates to proof-oriented core crates.

When uncertain, prefer the narrower dependency surface and document the decision in this file.

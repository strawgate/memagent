Repository: fastforward | Primary language: Rust

## Architecture Overview
`ffwd` is a high-performance Rust log forwarder designed for research and learning, focusing on modern Rust tooling. It processes log data through an Arrow-native pipeline: tailing files, parsing JSON/CRI logs with portable SIMD, transforming data using Apache DataFusion SQL, and shipping to OTLP collectors. The project emphasizes a single static binary (~15 MB) with zero-copy data handling. Key architectural layers include `ffwd-core` (proven, `no_std` logic), `ffwd-arrow` (Arrow/SIMD integration), `ffwd-io` (input sources), `ffwd-transform` (SQL processing), `ffwd-output` (output sinks), and `ffwd-runtime` (async orchestration, worker pools) [dev-docs/ARCHITECTURE.md:L10-L39].

## Code Style & Conventions
- **Rust Formatting:** Enforced by `rustfmt` via `just fmt` or `cargo fmt --all -- --check` [AGENTS.md:L34, .pre-commit-config.yaml:L11-L17].
- **Rust Linting:** Enforced by `clippy` via `just clippy` or `cargo clippy --all-targets --all-features -- -D warnings`. CI runs with `-D warnings`, so any clippy warning is a build failure [AGENTS.md:L18, AGENTS.md:L35, .pre-commit-config.yaml:L19-L25].
- **TOML Formatting:** Enforced by `taplo` via `just toml-fmt` or `just toml-check`. Configuration in `taplo.toml` specifies `array_trailing_comma = true`, `compact_arrays = true`, `indent_string = "    "`, and reorders keys for `dependencies`/`dev-dependencies`/`build-dependencies` [AGENTS.md:L36-L37, taplo.toml:L5-L19].
- **Workspace Lint Policy:** Workspace-level lints are defined in `Cargo.toml:[workspace.lints]` and inherited by all crates. Specific rules include `clippy::pedantic` (warn with selective allows), `clippy::unwrap_used = deny`, `clippy::panic = deny`, `clippy::dbg_macro = deny`, `unsafe_code = forbid` in `ffwd-core` (with `// SAFETY:` comments required elsewhere), and `clippy::print_stdout`/`clippy::print_stderr = warn` (with explicit `#[allow]` for CLI/benchmarks) [dev-docs/CODE_STYLE.md:L40-L62, dev-docs/CRATE_RULES.md:L8-L13].
- **Public API Shape:** New `pub` items require `///` doc comments with examples, accept general types (`&str`, `&[T]`), return concrete types, use `thiserror` enums for public errors, mark new public enums `#[non_exhaustive]` unless genuinely stable, derive standard traits where valid, and use builder patterns for types with >3 config knobs [CONTRIBUTING.md:L60-L79].

## Key Directories & Entry Points
| Directory | Why it matters |
|---|---|
| `crates/ffwd/` | Binary crate; contains CLI entry points (`src/main.rs`, `src/cli.rs`) and compatibility re-exports. |
| `crates/ffwd-runtime/` | Core runtime orchestration, pipeline loop, worker pool, and processors. |
| `crates/ffwd-core/` | Proven kernel with `no_std` pure logic: scanner, parsers, OTLP encoding, pipeline state machine. |
| `crates/ffwd-io/` | I/O layer: file tailing, TCP/UDP/OTLP inputs, checkpointing, diagnostics. |
| `crates/ffwd-transform/` | DataFusion SQL transforms and User-Defined Functions (UDFs). |
| `crates/ffwd-output/` | Output sinks: OTLP, Elasticsearch, Loki, JSON lines, stdout. |
| `crates/ffwd-config/` | YAML configuration parsing and validation. |
| `dev-docs/` | Canonical developer documentation, architecture, code style, and verification rules. |

## Quick Recipes
| Command | Description | Source |
|---|---|---|
| `just setup` | Install tools, fetch dependencies, set up git hooks, build dashboard. | [DEVELOPING.md:L45] |
| `just doctor` | Verify all required and optional dev tools are installed. | [justfile:L61] |
| `just build` | Build release binary with DataFusion SQL. | [AGENTS.md:L22] |
| `just build-dev-lite` | Build fast dev binary (no DataFusion SQL). | [AGENTS.md:L23] |
| `just test` | Run default-members tests (fast, skips datafusion). | [AGENTS.md:L27] |
| `just test-all` | Run full workspace tests (includes datafusion). | [AGENTS.md:L28] |
| `cargo test -p ffwd-core` | Run tests for a single crate (fastest iteration). | [AGENTS.md:L29] |
| `just lint` | Run fast lint checks: `fmt-check`, `clippy`, `toml-check`. | [AGENTS.md:L32] |
| `just lint-all` | Run full lint checks: `fmt-check`, `kani-boundary`, `clippy-all`, `toml-check`, `deny`. | [AGENTS.md:L33] |
| `just fmt` | Auto-format Rust code. | [AGENTS.md:L34] |
| `just clippy` | Run Clippy with `-D warnings` (matches CI). | [AGENTS.md:L35] |
| `just ci` | Run fast CI tier (lint + test default-members). | [AGENTS.md:L40] |
| `just ci-all` | Run full CI tier (lint-all + test-all, CI parity). | [AGENTS.md:L41] |
| `just bench` | Run Tier 1 Criterion benchmarks. | [AGENTS.md:L48] |
| `ff run --config config.yaml` | Run the log forwarder with a specified config. | [README.md:L53] |
| `ff devour` | Start a local OTLP blackhole receiver on `127.0.0.1:4318`. | [DEVELOPING.md:L64] |

## Dependencies & Compatibility
- **Critical Runtime Dependencies:** Apache Arrow (v58, `ipc_compression` feature) for columnar data processing; Apache DataFusion (v53, `sql`, `string_expressions`, `unicode_expressions`, `regex_expressions`, `recursive_protection` features) for SQL transforms; `tokio` (v1) for async runtime; `reqwest` for HTTP clients; `serde` (v1, `derive` feature) for configuration (de)serialization [Cargo.toml:L56-L99].
- **Toolchain & Versions:** Rust stable toolchain (1.89+) managed by `rust-toolchain.toml`. Development tools include `just` (1.40+), `cargo-nextest` (0.9+), `taplo`, `cargo-deny`, `sccache`, `Node.js` (22+) for dashboard, `Docker` for Elasticsearch, `OpenJDK` for TLC model checks, and `Miri` nightly components for UB checks [rust-toolchain.toml:L1-L2, DEVELOPING.md:L21-L37].
- **Observability:** `tracing` for structured logging (used throughout the codebase, e.g., [crates/ffwd-io/src/checkpoint.rs:L91]); `opentelemetry` (v0.31, `trace`, `metrics` features) and `opentelemetry-otlp` (v0.31, `http-proto`, `reqwest-client`, `trace`, `metrics` features) for metrics and tracing, integrated via `ffwd-diagnostics` [Cargo.toml:L81-L95, crates/ffwd-diagnostics/src/lib.rs:L1-L6].

## Unique Workflows
- **Formal Verification:** `ffwd-core` uses Kani (bounded model checking) for proofs, state machines use TLA+, and SIMD conformance uses proptest. CI includes `just kani` and `just kani-boundary` for verification [AGENTS.md:L44-L45, dev-docs/CRATE_RULES.md:L14, dev-docs/DESIGN.md:L15].
- **Code Generation:** Protobuf build scripts (`ffwd-proto-build`) generate Rust code from `.proto` files (e.g., `crates/ffwd-otap-proto/build.rs`).
- **Monorepo Tooling:** Uses `just` as a task runner for consistent commands across the workspace, and `cargo-deny` for license and advisory audits [AGENTS.md:L9, deny.toml:L0-L44].
- **SIMD Parsing:** Employs portable SIMD for high-performance JSON/CRI parsing, with zero-copy Arrow pipeline integration [README.md:L77-L78, dev-docs/DESIGN.md:L91-L98].

## API Surface Map
- **CLI:** The primary external interface is the `ff` CLI binary, built from `crates/ffwd/src/main.rs` and `crates/ffwd/src/cli.rs`. Commands include `run`, `send`, `validate`, `dry-run`, `effective-config`, `blackhole`, `blast`, `devour`, `generate-json`, and `wizard` [crates/ffwd/src/main.rs:L40-L52].
- **Configuration:** YAML-based configuration is loaded and validated by `ffwd-config` (see `crates/ffwd-config/src/types/root.rs` for schema definition and `crates/ffwd-config/src/load.rs` for loading logic).
- **Input Sources:** Various input types are defined in `crates/ffwd-config/src/types/input.rs`, including `file` (tailing, `crates/ffwd-io/src/tail/mod.rs`), `tcp` (`crates/ffwd-io/src/tcp_input/listener.rs`), `udp` (`crates/ffwd-io/src/udp_input.rs`), `http` (`crates/ffwd-io/src/http_input.rs`), `otlp` (`crates/ffwd-io/src/otlp_receiver/server.rs`), and `s3` (`crates/ffwd-io/src/s3_input/mod.rs`).
- **Output Sinks:** Output types are defined in `crates/ffwd-config/src/types/output.rs`, including `stdout` (`crates/ffwd-output/src/stdout.rs`), `json_lines` (`crates/ffwd-output/src/json_lines.rs`), `otlp` (`crates/ffwd-output/src/otlp_sink/mod.rs`), `elasticsearch` (`crates/ffwd-output/src/elasticsearch/mod.rs`), and `loki` (`crates/ffwd-output/src/loki.rs`).
- **Diagnostics:** An HTTP diagnostics server is provided by `ffwd-diagnostics` (see `crates/ffwd-diagnostics/src/diagnostics/server.rs`) for status, metrics, and live tracing.

## Onboarding Steps
- **Start with Developer Docs:** Begin with `dev-docs/README.md` to understand the project's ethos, architecture, crate rules, and verification requirements.
- **Run Fast CI:** Execute `just ci` to ensure a clean, fast baseline and verify local setup [CONTRIBUTING.md:L5].
- **Understand Crate Layout:** Review `DEVELOPING.md:L72` for a detailed breakdown of each crate's purpose and boundaries.
- **Explore Configuration:** Refer to `book/src/content/docs/quick-start.mdx` for practical examples and `dev-docs/CHANGE_MAP.md` for config schema changes.
- **Gotcha: `cargo clippy` vs `just clippy`:** Always use `just clippy` (or `cargo clippy -- -D warnings`) as bare `cargo clippy` will not catch CI failures due to different warning levels [AGENTS.md:L18].
- **Gotcha: `default-members`:** Bare `cargo check`/`clippy`/`test` commands operate only on `default-members` (excluding `ffwd-transform` and `ffwd` binary) for faster iteration. Use `--workspace` or `-all` `just` targets for full coverage [AGENTS.md:L57-L59, Cargo.toml:L34-L49].

## Getting Unstuck
- **General Development Issues:** Consult `DEVELOPING.md` for sections like "Things that will bite you" for historical gotchas and "Local CPU profiling (macOS)" for performance debugging.
- **File Tailing Issues:** Refer to `dev-docs/references/file-discovery.md` for detailed notes on file-discovery reliability.
- **Verification Failures:** Check `dev-docs/VERIFICATION.md` for Kani/TLA+/proptest requirements and `dev-docs/verification/kani-boundary-contract.toml` for boundary status.
- **CI Failures:** The `CONTRIBUTING.md` provides a "Pre-Commit Checklist" and "Pre-Push Checklist" with commands to run locally to match CI behavior.
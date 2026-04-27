# Agent Guide — ffwd

> A Rust log forwarder: tails files, parses JSON/CRI with portable SIMD, transforms with SQL (DataFusion), ships to OTLP collectors. Single static binary, ~15 MB.
> Humans: start with `dev-docs/README.md` for the fastest contributor path. This file is optimized for coding agents.

## Prerequisites

- **Rust 1.89+** (`rustup` recommended)
- **[just](https://github.com/casey/just)** task runner (`cargo install just`)
- **[taplo](https://taplo.tamasfe.dev/)** for TOML formatting (`cargo install taplo-cli`)
- **[cargo-nextest](https://nexte.st/)** for test execution (`cargo install cargo-nextest`)
- **[sccache](https://github.com/mozilla/sccache)** for compile caching (`cargo install sccache --locked`) — pre-configured in `.cargo/config.toml`
- **Optional:** [pre-commit](https://pre-commit.com/) (`pip install pre-commit && pre-commit install`)
- **Optional:** [Kani](https://github.com/model-checking/kani) for formal verification (`cargo install --locked kani-verifier && cargo kani setup`)

## Primary commands

All commands use `just` recipes. **Never use bare `cargo clippy`** — it won't catch CI failures because CI runs `cargo clippy -- -D warnings`.

```bash
# ── Build ───────────────────────────────────────────────
just build                   # Release binary with DataFusion SQL
just build-dev-lite          # Fast dev binary (no DataFusion SQL)
cargo build --release -p ffwd  # Equivalent to `just build`

# ── Test ────────────────────────────────────────────────
just test                    # Default-members only (~30s, skips datafusion)
just test-all                # Full workspace (~3min, includes datafusion)
cargo test -p ffwd-core    # Single-crate iteration (fastest)

# ── Lint & Format ──────────────────────────────────────
just lint                    # fmt-check + clippy + toml-check (fast)
just lint-all                # fmt-check + kani-boundary + clippy-all + toml-check + deny
just fmt                     # Auto-format Rust code
just clippy                  # Clippy with -D warnings (matches CI)
just toml-check              # Validate TOML formatting
just toml-fmt                # Auto-format TOML files

# ── CI (run before pushing) ────────────────────────────
just ci                      # lint + test (default-members, fast)
just ci-all                  # lint-all + test-all (full workspace, CI parity)

# ── Formal Verification ────────────────────────────────
just kani                    # Kani proofs for production crates
just kani-boundary           # Validate non-core Kani boundary contract

# ── Benchmarks ──────────────────────────────────────────
just bench                   # Tier 1 Criterion benchmarks (~30s: pipeline, output_encode, full_chain)

# ── Fuzz ────────────────────────────────────────────────
just fuzz scanner 300        # Fuzz a target for 300s (requires nightly)

# ── Profiling ───────────────────────────────────────────
just profile-otlp-local      # CPU profile: file → OTLP with flamegraph
```

### Why two tiers?

The workspace `default-members` excludes `ffwd-transform` (datafusion) and `ffwd` (binary). Bare `cargo check` / `just clippy` skip them (~30s vs ~3min). Use `--workspace`, `-p ffwd`, or the `-all` just targets when you need the full build. CI always uses `--workspace`.

## Working Efficiently

**TL;DR:** maximize useful context per turn, minimize filler, and keep outputs dense enough that the next action is obvious.

- Make many independent tool calls in the same round. Batch file reads, searches, `git` queries, and metadata checks instead of serializing them one at a time.
- Read broadly before editing. When a change crosses boundaries, inspect the caller, callee, tests, docs, and config schema together so the first patch is informed.
- Prefer high-signal commands: `rg`, `rg --files`, targeted `sed -n`, `git diff -- <paths>`, and focused test filters. Avoid noisy recursive dumps.
- Keep context compact. Summarize large files or command output; quote only the lines needed to justify the next step.
- Start responses with the answer or status, then the key evidence. Use a short TL;DR for multi-step updates and final handoffs.
- Use dense bullets over long prose when reporting findings, decisions, changed files, or verification results.
- Say what changed, where, and how it was verified. Omit generic narration, apologies, and restating the user's request.
- Ask only when blocked by missing intent or unsafe ambiguity. Otherwise, make the conservative repo-aligned choice and continue.
- Keep tool output and final responses proportional to risk: concise for simple edits, more detailed for architecture, state-machine, or CI-impacting changes.

## Repository map

```
.
├── AGENTS.md                    ← You are here
├── CLAUDE.md                    ← Identical (for Claude Code compatibility)
├── DEVELOPING.md                ← Build/test/bench commands, hard-won lessons
├── CONTRIBUTING.md              ← PR process, issue labels, pre-commit checklist
├── README.md                    ← Product overview, quick start, CLI reference
├── justfile                     ← All task-runner recipes
├── Cargo.toml                   ← Workspace root, workspace-level lints
├── .cargo/config.toml           ← sccache wrapper, build settings
│
├── crates/
│   ├── ffwd/                  ← Binary crate: CLI, startup, signal handling
│   ├── ffwd-runtime/          ← Async orchestration: pipeline loop, worker pool
│   ├── ffwd-core/             ← Proven kernel (no_std, forbid(unsafe)): scanner, parsers, framer, OTLP encoding
│   ├── ffwd-arrow/            ← Arrow integration: ScanBuilder impls, SIMD backends, RecordBatch
│   ├── ffwd-config/           ← YAML config parsing and validation
│   ├── ffwd-io/               ← I/O layer: file tailing, TCP/UDP/OTLP inputs, checkpointing
│   ├── ffwd-transform/        ← DataFusion SQL transforms, UDFs (grok, regexp_extract, geo_lookup)
│   ├── ffwd-output/           ← Output sinks: OTLP, Elasticsearch, Loki, JSON lines, stdout
│   ├── ffwd-types/            ← Shared value types, state-machine semantics, diagnostics
│   ├── ffwd-bench/            ← Criterion benchmarks for the scanner pipeline
│   ├── ffwd-test-utils/       ← Shared test utilities
│   ├── ffwd-diagnostics/      ← Diagnostics control plane: HTTP endpoints, dashboard, readiness
│   ├── ffwd-config-wasm/      ← WASM bindings for the config validator (browser/Node.js)
│   ├── ffwd-ebpf-proto/       ← eBPF log capture protocol definitions (experimental)
│   ├── ffwd-otap-proto/       ← OTAP protocol definitions
│   └── ffwd-proto-build/      ← Protobuf build scripts
│
├── dev-docs/                    ← Developer documentation
│   ├── README.md                ← Developer-doc start page and task routing
│   ├── ARCHITECTURE.md          ← Pipeline data flow, scanner stages, crate map, buffer lifecycle
│   ├── DESIGN.md                ← Vision, target architecture, architecture decision records
│   ├── VERIFICATION.md          ← TLA+, Kani, proptest: when to use each, proof requirements
│   ├── CRATE_RULES.md           ← Per-crate rules enforced by CI
│   ├── CODE_STYLE.md            ← Naming, error handling, hot path rules
│   ├── ADAPTER_CONTRACT.md      ← Receiver/pipeline/sink contracts, checkpoint rules
│   ├── SCANNER_CONTRACT.md      ← Scanner input requirements, output guarantees
│   ├── CHANGE_MAP.md            ← What must change together (config, pipeline, verification, crates)
│   ├── PR_PROCESS.md            ← Copilot assignment, PR triage, review criteria
│   ├── DOCS_STANDARDS.md        ← Doc taxonomy, lifecycle rules
│   ├── verification/            ← Kani boundary contract TOML + scripts
│   └── references/              ← Library-specific guides (Arrow, DataFusion, Tokio, OTLP, Kani)
│
├── book/src/content/docs/       ← User-facing documentation (Astro Starlight)
│   └── (sidebar in book/astro.config.mjs)
│
├── tla/                         ← TLA+ specs: PipelineMachine, ShutdownProtocol, PipelineBatch
│   └── README.md                ← TLA+ documentation and model config
│
├── examples/use-cases/          ← 20 ready-made config starters for common patterns
├── deploy/                      ← Kubernetes DaemonSet manifests
└── scripts/                     ← CI and verification helper scripts
```

## Architecture overview

```
log files → SIMD parse → Arrow RecordBatch → DataFusion SQL → OTLP → collector
```

Data flows through four layers (dependencies flow downward only):

```
ffwd (binary)           CLI entrypoints, config loading, signal handling
       ↓
ffwd-runtime            Async orchestration, pipeline loop, worker pool
       ↓
ffwd-arrow              ScanBuilder impls, SIMD backends, RecordBatch builders
       ↓
ffwd-core               Pure logic, proven, no_std, forbid(unsafe)
```

**Pipeline loop** (`ffwd-runtime/src/pipeline.rs`): input threads (OS threads, blocking IO) feed a bounded `tokio::sync::mpsc` channel → async pipeline loop batches and flushes → scanner → SQL transform → output sinks. See `dev-docs/ARCHITECTURE.md` for the full data flow.

**Key abstraction boundary**: `ScanBuilder` trait (defined in `ffwd-core`, implemented in `ffwd-arrow`). The scanner knows nothing about Arrow — it calls trait methods.

## Crate rules (CI-enforced)

| Crate | Key constraints |
|-------|----------------|
| `ffwd-core` | `no_std` + `forbid(unsafe_code)`. Only deps: memchr + wide + ffwd-kani + ffwd-lint-attrs. Every public fn needs a Kani proof. No panics, no unwrap, no indexing. |
| `ffwd-arrow` | Implements core's ScanBuilder. unsafe allowed for SIMD only. proptest: SIMD ≡ scalar. |
| `ffwd-io` | IO lives here. Tests use tempfiles. No raw payload injection (see #1615). |
| `ffwd-transform` | DataFusion is the SQL engine. Enrichment tables implement Arrow RecordBatchReader. |
| `ffwd-output` | Uses core for OTLP encoding. Transport separated from serialization. |
| `ffwd-runtime` | Async orchestration only. Extract pure reducers instead of growing async shells. |
| `ffwd` (binary) | CLI/bootstrap only. No long-lived runtime orchestration logic. |

Full details: `dev-docs/CRATE_RULES.md`.

## Code style essentials

- **No `.unwrap()` in production paths.** Use `?`, `.expect("reason")`, or `unwrap_or`.
- **Hot path** (reader → framer → scanner → builders → OTLP → compress): no per-record allocations, no `format!()` in loops, no `Vec::push` in per-line loops, prefer `&[u8]` over `&str`.
- **Naming:** functions are `verb_noun`, booleans are `is_`/`has_`/`should_` prefixed.
- **Comments:** doc comments (`///`) on all public items. No restating the code. TODOs: `// TODO(#123): description`.
- **Tests:** one test per behavior. Names describe the scenario: `empty_input_returns_none`.
- **Workspace lints** in root `Cargo.toml` under `[workspace.lints]` — all crates inherit. No per-crate overrides.

Full details: `dev-docs/CODE_STYLE.md`.

## Verification requirements

| Technique | Use when |
|-----------|----------|
| **TLA+** (`tla/`) | Temporal properties: liveness, drain, protocol ordering |
| **Kani** | Pure functions, bounded inputs, unsafe code — exhaustive bounded proofs |
| **proptest** | Stateful, heap-heavy, or async code; SIMD conformance |
| **Miri** | UB detection, allocator behavior |

- Every new public function in `ffwd-core` **requires** a Kani proof.
- SIMD backends **require** proptest (SIMD output ≡ scalar output).
- State-machine changes **require** updating TLA+ specs + Kani/proptest.

Kani quick path:
1. Add/adjust proof harness in `#[cfg(kani)] mod verification`.
2. Use `verify_<function>_<property>` naming and add `kani::cover!`.
3. Run targeted harness.
4. Run `just kani`.
5. Run `just kani-boundary` when touching non-core seam contract areas.
6. Update `dev-docs/VERIFICATION.md` when requirements/status changed.

Full details: `dev-docs/VERIFICATION.md`.

## Change map — what must change together

Before editing code, check `dev-docs/CHANGE_MAP.md`:

- **Config fields changed** → update parsing code, `book/src/content/docs/configuration/reference.mdx`, example configs, validation tests.
- **Pipeline behavior changed** → update `dev-docs/ARCHITECTURE.md`, troubleshooting, performance docs.
- **Invariants changed** → update `dev-docs/VERIFICATION.md`, Kani/proptest harnesses, TLA+ specs.
- **Crate boundaries changed** → update `dev-docs/CRATE_RULES.md`, crate-level `AGENTS.md` files.

## Git and PR conventions

- **Commit messages:** `type: concise description` — `fix:`, `feat:`, `refactor:`, `docs:`, `bench:`, `test:`
- **One concern per commit.** Don't mix fixes with features.
- **PR titles:** same format as commits, under 70 chars.
- **PR body:** explain what and why. Include benchmark results if relevant.
- **Tests required** in every PR.
- **`just ci` must pass** before pushing.

Full details: `CONTRIBUTING.md`, `dev-docs/PR_PROCESS.md`.

## Issue labels (quick reference)

Every issue needs **type + priority + component(s)**:

| Type | Priority |
|------|----------|
| `bug`, `enhancement`, `performance`, `architecture`, `research`, `documentation`, `work-unit` | `P0` critical, `P1` high, `P2` medium, `P3` low |

Component labels use `component:` prefix (e.g., `component:processor/scanner`). Full taxonomy: `CONTRIBUTING.md`.

## Navigation tips

- **Start developer docs quickly:** `dev-docs/README.md` — canonical map and task routing.
- **Find a crate's purpose:** `crates/<name>/` — each has a `Cargo.toml` and many have an `AGENTS.md` with crate-specific rules.
- **Find how data flows:** `dev-docs/ARCHITECTURE.md` — full pipeline diagram and buffer lifecycle.
- **Find what SQL UDFs exist:** `crates/ffwd-transform/src/udf/` for `json()`, `json_int()`, `json_float()`, `regexp_extract()`, `grok()`, `geo_lookup()`, `hash()` (internal), and `crates/ffwd-transform/src/cast_udf.rs` for `int()` / `float()` — all are custom `ScalarUDFImpl` implementations registered on the DataFusion context.
- **Find config schema:** `book/src/content/docs/configuration/reference.mdx` — all YAML fields, input/output types.
- **Find example configs:** `examples/use-cases/` — 20 common patterns.
- **Review active work/priorities:** open GitHub issues and PRs (`gh issue list --state open`, `gh pr list --state open`).
- **Find library-specific patterns:** `dev-docs/references/` — Arrow, DataFusion, Tokio, OTLP, Kani.
- **Find CLI reference:** `book/src/content/docs/cli/reference.mdx` — all `ff` subcommands with options.
- **Start quickly:** `ff init` creates a starter `ffwd.yaml`; `ff wizard` is an interactive alternative.
- **Find verification status:** `dev-docs/VERIFICATION.md` — per-module proof status.
- **Find what breaks together:** `dev-docs/CHANGE_MAP.md` — co-change requirements.

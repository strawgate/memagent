# Copilot Instructions for logfwd

## Project Overview

logfwd is a high-performance log forwarder written in Rust. It reads logs from multiple sources (files, UDP, TCP, OTLP), optionally parses container formats (CRI), transforms them with SQL (Apache DataFusion on Arrow RecordBatches), and forwards to multiple destinations (OTLP, Elasticsearch, Loki, Parquet, JSON lines, stdout).

**Performance target:** 1.7M lines/sec with CRI parsing and OTLP encoding on single-core ARM64.

## Tech Stack

- **Language:** Rust (stable toolchain, no unsafe, no async runtime)
- **Data format:** Apache Arrow RecordBatches
- **SQL engine:** Apache DataFusion
- **Serialization:** Hand-rolled protobuf (OTLP), serde (JSON/YAML)
- **Compression:** zstd level 1
- **Build:** Cargo workspace (7 crates), `just` task runner
- **CI:** GitHub Actions (lint, test on Linux/macOS, release check, nightly benchmarks)
- **Linting:** clippy (warnings as errors), rustfmt, taplo (TOML), cargo-deny

## Required Reading

Before making any code changes, read these files in full:

- `README.md` — project overview and performance benchmarks
- `DEVELOPING.md` — codebase structure, design decisions, build/test/bench commands
- `docs/ARCHITECTURE.md` — v2 Arrow pipeline design
- `docs/PREDICATE_PUSHDOWN.md` — query optimization
- `docs/research/SCANNER_DESIGN_V1.md` — scanner design history
- `docs/research/BENCHMARKS_V1.md` — benchmark methodology

## Key Commands

```bash
just ci        # Full CI: lint + test (run this before any PR)
just test      # Run all tests
just lint      # Format check + clippy + TOML check + deny
just clippy    # Clippy lints only
just fmt       # Format code
just bench     # Criterion micro-benchmarks
```

## Code Style

- No async runtime — purely blocking, high-throughput design
- No per-line heap allocations in hot paths (scanner, CRI parser, OTLP encoder, compress)
- No unnecessary abstractions or trait indirection
- Follow existing patterns in surrounding code exactly
- Prefer standard library or existing dependencies over new crates
- All public APIs must have doc comments
- All changes must have tests

## Project Structure

```
crates/logfwd/           # Binary: CLI entry point
crates/logfwd-core/      # Core: scanner, compress, CRI, OTLP, tail, diagnostics
crates/logfwd-config/    # Configuration: YAML parser
crates/logfwd-output/    # Sinks: OTLP, JSON lines, Elasticsearch, Loki, Parquet, stdout
crates/logfwd-transform/ # SQL transforms via DataFusion
crates/logfwd-bench/     # Criterion benchmarks
```

## What NOT to Do

- Do not add async/await or any async runtime
- Do not add dependencies without justification
- Do not refactor code that isn't related to the task at hand
- Do not skip running `just ci` before committing
- Do not introduce allocations in hot paths without benchmarking

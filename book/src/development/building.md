# Building & Testing

## Prerequisites

```bash
# Install Rust stable
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install task runner
cargo install just

# Install development tools
just install-tools
```

## Common commands

```bash
just ci          # Run full CI suite (lint + test)
just fmt         # Format code
just clippy      # Run lints
just test        # Run all tests
just bench       # Run Criterion microbenchmarks
just build       # Build release binary
```

## Project structure

```
crates/logfwd/           # Binary entry point, CLI, pipeline orchestrator
crates/logfwd-core/      # SIMD scanner, file tailer, CRI parser, diagnostics
crates/logfwd-config/    # YAML config parser
crates/logfwd-output/    # Output sinks (OTLP, HTTP, stdout)
crates/logfwd-transform/ # SQL transforms via DataFusion, UDFs
crates/logfwd-bench/     # Benchmarks (Criterion + exploratory)
```

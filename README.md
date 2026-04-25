<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/brand/lockup-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="assets/brand/lockup-light.svg">
  <img width="200" alt="FastForward" src="assets/brand/lockup-dark.svg">
</picture>

**a learning-oriented log forwarder built with Rust**

[![Docs](https://img.shields.io/badge/docs-fastforward-D85A30)](https://strawgate.github.io/fastforward/)
[![CI](https://github.com/strawgate/fastforward/actions/workflows/ci.yml/badge.svg)](https://github.com/strawgate/fastforward/actions/workflows/ci.yml)

</div>

---

FastForward is a research and learning project exploring how far you can push a log forwarding pipeline with modern Rust tooling. It tails files, parses JSON and CRI logs with portable SIMD, transforms with SQL, and ships to OTLP collectors.

The [documentation site](https://strawgate.github.io/fastforward/) has interactive guides that explain how each piece works — from SIMD parsing to backpressure to checkpoint ordering — with live simulations you can play with.

> **Note:** The Cargo package is named `ffwd`; the installed CLI binary is `ff`.

## Try it

```bash
# Build from source (Rust 1.89+)
git clone https://github.com/strawgate/fastforward.git && cd fastforward
cargo build --release -p ffwd

# Generate some test data
./target/release/ff generate-json 100000 logs.json
```

```yaml
# config.yaml
input:
  type: file
  path: logs.json
  format: json

transform: |
  SELECT level, message, status, duration_ms
  FROM logs
  WHERE level = 'ERROR' AND duration_ms > 50

output:
  type: stdout
  format: console
```

```bash
./target/release/ff run --config config.yaml
```

Only error records with slow durations make it through — everything else is filtered by the SQL transform.

For one-off command-line sends, keep a destination-only config and pipe data into `ff`:

```yaml
# destination.yaml
output:
  type: otlp
  endpoint: http://127.0.0.1:4318/v1/logs
```

```bash
cat logs.json | ./target/release/ff send --config destination.yaml --format json --service checkout
```

## What makes it interesting

```text
log files → SIMD parse → Arrow RecordBatch → DataFusion SQL → OTLP → your collector
```

- **Portable SIMD parsing** — 10 broadcast-compare ops per 64-byte block, one pass per buffer, runs on x86_64 and ARM64
- **Zero-copy Arrow pipeline** — `StringViewArray` stores views into the read buffer; string data isn't copied from scanner to RecordBatch
- **SQL transforms** — every batch runs through a [DataFusion](https://datafusion.apache.org/) SQL query with built-in UDFs like `regexp_extract()`, `grok()`, and `geo_lookup()`
- **Formal verification** — the parsing core is verified with [Kani](https://github.com/model-checking/kani) (bounded model checking), state machines with TLA+, and SIMD conformance with proptest
- **Single static binary** — ~15 MB, no JVM, no Python, no runtime dependencies

## Documentation

The docs are the best way to understand FastForward:

- **[Quick Start](https://strawgate.github.io/fastforward/quick-start/)** — install, run your first pipeline, ship logs
- **[SQL Transforms](https://strawgate.github.io/fastforward/configuration/sql-transforms/)** — filter, reshape, extract, join
- **[Configuration Reference](https://strawgate.github.io/fastforward/configuration/reference/)** — every YAML field, input/output type, UDF

The "Understand It" section of the docs has interactive guides with live simulations:

- **[Tailing](https://strawgate.github.io/fastforward/learn/tailing/)** — watch file rotation and truncation handling live
- **[SIMD Scanner](https://strawgate.github.io/fastforward/learn/scanner/)** — step through JSON parsing, toggle field pushdown
- **[Backpressure](https://strawgate.github.io/fastforward/learn/backpressure/)** — slow the output and watch pressure cascade back
- **[Columnar Storage](https://strawgate.github.io/fastforward/learn/columnar/)** — row vs column layout, why Arrow makes SQL fast
- **[Checkpoint Ordering](https://strawgate.github.io/fastforward/learn/checkpoints/)** — out-of-order ACKs and the committed watermark

## Contributing

This is a learning project — contributions, questions, and experiments are welcome.

- **[Quick Start for contributors](dev-docs/README.md)** — task routing and developer docs
- **[CONTRIBUTING.md](CONTRIBUTING.md)** — PR process and pre-commit checks
- **[DEVELOPING.md](DEVELOPING.md)** — build, test, lint, bench commands

```bash
just build    # release binary
just test     # run tests
just lint     # fmt + clippy + toml check
just ci       # lint + test (run before pushing)
```

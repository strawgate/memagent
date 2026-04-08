# logfwd

A Rust log forwarder that tails files, parses JSON and Kubernetes CRI logs with portable SIMD, transforms with SQL, and ships to any OTLP-compatible collector — at 1.7 million lines/second on a single ARM64 core.

```
log files → SIMD parse → Arrow RecordBatch → DataFusion SQL → OTLP → your collector
```

---

## Quick Start

Try logfwd in 60 seconds — no collector, no infrastructure, just a terminal.

**1. Install**

```bash
# Download the latest release (macOS Apple Silicon shown — see Install section for all platforms)
curl -fsSL https://github.com/strawgate/memagent/releases/latest/download/logfwd-darwin-arm64 -o logfwd
chmod +x logfwd

# Or build from source (Rust 1.85+)
cargo build --release -p logfwd && cp target/release/logfwd .
```

**2. Generate test data**

```bash
./logfwd generate-json 100000 logs.json
```

**3. Create a pipeline**

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

**4. Run it**

```bash
./logfwd run --config config.yaml
```

You'll see only the records that match your SQL filter:

```
ERROR  request handled GET /api/v2/products/10049  status=500 duration_ms=92
ERROR  request handled POST /api/v1/orders/10121  status=503 duration_ms=78
...
```

Only error records with slow durations made it through — everything else was filtered by the SQL transform. See the [Quick Start guide](book/src/getting-started/quickstart.md) to keep going.

---

## Why logfwd?

| What | How |
|------|-----|
| **Fast SIMD parsing** | One pass per buffer with portable SIMD — 10 broadcast-compare ops per 64-byte block, runs on x86_64 and ARM64 |
| **Low-copy pipeline** | Apache Arrow `StringViewArray` stores views into the read buffer — string data isn't copied from scanner to RecordBatch |
| **SQL transforms** | Every batch runs through a DataFusion SQL query. Filter, reshape, regex-extract, join enrichment tables — standard SQL |
| **OTLP native** | Encodes directly to OTLP protobuf. Works with any OpenTelemetry Collector, Grafana Alloy, or OTLP-speaking backend |
| **Kani-verified core** | The framer, aggregator, and OTLP wire-format code are verified with the [Kani model checker](https://github.com/model-checking/kani) — exhaustive bounded proofs that these paths cannot panic, overflow, or produce invalid output |
| **Single static binary** | One statically-linked file (~15 MB). No JVM, no Python, no Lua, no runtime dependencies |

---

## SQL Transforms

The SQL transform is why you'd pick logfwd over a plain forwarder. Every parsed batch becomes a DataFusion table named `logs`.

```sql
-- Forward only errors and slow requests
SELECT level, message, duration_ms, status
FROM logs
WHERE level = 'ERROR' OR duration_ms > 1000
```

```sql
-- Extract fields with regex, rename columns
SELECT
  level,
  regexp_extract(message, 'request_id=([a-f0-9-]+)', 1) AS request_id,
  status
FROM logs
WHERE status >= 400
```

Built-in UDFs: `int()`, `float()`, `regexp_extract()`, `grok()`, `json()`, `json_int()`, `json_float()`. The `geo_lookup()` UDF is also available when a geo-IP database is configured. See the [SQL Transforms guide](book/src/config/sql-transforms.md).

---

## Install

```bash
# Binary (pick your platform)
#   logfwd-linux-amd64, logfwd-linux-arm64, logfwd-darwin-amd64, logfwd-darwin-arm64
curl -fsSL https://github.com/strawgate/memagent/releases/latest/download/logfwd-linux-amd64 -o logfwd
chmod +x logfwd
sudo mv logfwd /usr/local/bin/

# Docker
docker run --rm -v $(pwd)/config.yaml:/etc/logfwd/config.yaml:ro \
  ghcr.io/strawgate/memagent:latest run --config /etc/logfwd/config.yaml

# From source (requires Rust 1.85+)
cargo build --release -p logfwd

# Dev-only faster local build (no DataFusion SQL support)
cargo build --release -p logfwd --no-default-features
```

See [Installation](book/src/getting-started/installation.md) for all platforms and options.

---

## Configuration

### Simple — one pipeline

```yaml
input:
  type: file
  path: /var/log/app/*.log
  format: json

transform: SELECT level, message, status FROM logs WHERE status >= 400

output:
  type: otlp
  endpoint: https://otel-collector:4318/v1/logs
  compression: zstd
```

### Advanced — multiple named pipelines

```yaml
pipelines:
  errors:
    inputs:
      - type: file
        path: /var/log/pods/**/*.log
        format: cri
    transform: SELECT * FROM logs WHERE level = 'ERROR'
    outputs:
      - type: otlp
        endpoint: https://otel-collector:4318/v1/logs

  debug:
    inputs:
      - type: file
        path: /var/log/pods/**/*.log
        format: cri
    outputs:
      - type: stdout
        format: console
```

See the [Configuration Reference](book/src/config/reference.md) for all YAML fields, input/output types, and enrichment tables.

---

## Kubernetes

```bash
kubectl apply -f deploy/daemonset.yml
kubectl -n collectors rollout status daemonset/logfwd
```

Runs one logfwd pod per node, reads all container logs from `/var/log`. Typical resource use: ~128 MiB memory, 250m CPU at moderate log volume.

See the [Deployment Guide](book/src/deployment/kubernetes.md) for resource sizing, OTLP collector integration, and CRI log format details.

---

## Output Destinations

For current output support status, see the canonical tables in the
[Configuration Reference](book/src/config/reference.md#output-types). The
README and task-oriented guides intentionally avoid duplicating status claims.

---

## CLI Reference

```
logfwd run --config <file>               Run the pipeline
logfwd validate --config <file>          Validate config and environment-dependent pipeline requirements
logfwd dry-run --config <file>           Build and validate the pipeline without starting the runtime
logfwd init                              Generate a starter config from built-in templates
logfwd blackhole [bind_addr]             Start OTLP blackhole receiver for testing
logfwd generate-json <n> <file>          Generate synthetic JSON log data
logfwd effective-config [--config file]  Validate and print effective runnable config
logfwd wizard                            Interactive config wizard
logfwd completions <shell>               Generate shell completions (bash, zsh, fish, nushell, powershell, elvish)
logfwd --version                         Print version
```

For ready-made starters, see [`examples/use-cases/`](examples/use-cases/README.md) (20 common app/source patterns).

---

## Documentation

**Start here by goal**

- Not sure where to begin: [Choose the Right Guide](book/src/getting-started/which-guide.md)
- Run logfwd quickly: [Quick Start](book/src/getting-started/quickstart.md)
- Build a safer production baseline: [Your First Pipeline](book/src/getting-started/first-pipeline.md)
- Debug failures: [Troubleshooting](book/src/troubleshooting.md)

**User guides** — [book/src/](book/src/)

| Guide | Description |
|-------|-------------|
| [Choose the Right Guide](book/src/getting-started/which-guide.md) | Goal-based chooser for operators, contributors, and evaluators |
| [Quick Start](book/src/getting-started/quickstart.md) | Working pipeline in 10 minutes with copy/paste commands |
| [Your First Pipeline](book/src/getting-started/first-pipeline.md) | Production config with monitoring and validation |
| [Configuration Reference](book/src/config/reference.md) | All YAML fields, input/output types, SQL transforms, UDFs, enrichment |
| [SQL Transforms](book/src/config/sql-transforms.md) | DataFusion SQL examples, column naming, UDFs |
| [Deployment](book/src/deployment/kubernetes.md) | Kubernetes DaemonSet, Docker, resource sizing |
| [Troubleshooting](book/src/troubleshooting.md) | Common errors, debug mode, diagnostics API |

**Developer guides**

| Guide | Description |
|-------|-------------|
| [DEVELOPING.md](DEVELOPING.md) | Build, test, lint, bench commands |
| [CONTRIBUTING.md](CONTRIBUTING.md) | How to contribute — PR process, pre-commit checks |
| [Architecture](dev-docs/ARCHITECTURE.md) | Pipeline data flow, SIMD stages, crate map |
| [Design](dev-docs/DESIGN.md) | Vision, target architecture, decision records |
| [Verification](dev-docs/VERIFICATION.md) | TLA+, Kani, proptest — tool selection, proof status |

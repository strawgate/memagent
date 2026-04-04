# logfwd

A Rust log forwarder that tails files, parses JSON and Kubernetes CRI logs with portable SIMD, transforms with SQL, and ships to any OTLP-compatible collector — at 1.7 million lines/second on a single ARM64 core.

---

## What it does

logfwd reads log files (including Kubernetes container logs in CRI format), applies a SQL filter/transform, and forwards records as OTLP protobuf to an OpenTelemetry collector or compatible backend.

```
log files → SIMD parse → Arrow RecordBatch → DataFusion SQL → OTLP → your collector
```

logfwd needs an OTLP receiver to send to. Options: [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/), [Grafana Alloy](https://grafana.com/oss/alloy/), or logfwd's own built-in blackhole sink for testing (ships with the binary).

---

## Why logfwd?

| What | How |
|------|-----|
| **Fast SIMD parsing** | One pass per buffer using the [`wide`](https://crates.io/crates/wide) crate for portable SIMD — 10 sequential broadcast-compare operations per 64-byte block, no hand-rolled intrinsics, runs on x86_64, ARM64, or any LLVM-supported target |
| **Low-copy pipeline** | Apache Arrow `StringViewArray` stores views into the read buffer — string data isn't copied from scanner to RecordBatch |
| **SQL transforms** | Every parsed batch runs through a DataFusion SQL query before anything hits the wire. Filter, reshape, regex-extract, join enrichment tables — all in standard SQL |
| **OTLP native** | Encodes directly to OTLP protobuf. Any OpenTelemetry Collector, Grafana Alloy, or OTLP-speaking backend works out of the box |
| **Kani-verified core** | `framer.rs`, `aggregator.rs`, and wire-format primitives in `otlp.rs` are verified with the [Kani bounded model checker](https://github.com/model-checking/kani) — exhaustive bounded verification, not just fuzzing |
| **Single static binary** | One file. No JVM, no Python, no Lua, no runtime dependencies |

---

## Install

Download the latest release from [GitHub Releases](https://github.com/strawgate/memagent/releases) or build from source:

```bash
# From source (requires Rust toolchain)
cargo build --release -p logfwd
cp target/release/logfwd /usr/local/bin/
```

---

## Quick Start

### Try it in 60 seconds — no collector needed

logfwd ships a test-data generator and a built-in OTLP "blackhole" — accepts data and discards it immediately, a perfectly reliable drain for your benchmarks.

```bash
# 1. Generate 100,000 synthetic JSON log lines
logfwd --generate-json 100000 logs.json

# 2. Start the blackhole receiver (listens on :4318)
logfwd --blackhole &

# 3. Create config.yaml
cat > config.yaml << 'EOF'
input:
  type: file
  path: logs.json
  format: json

transform: |
  SELECT * FROM logs WHERE duration_ms > 50

output:
  type: otlp
  endpoint: http://127.0.0.1:4318
  compression: zstd
EOF

# 4. Run
logfwd --config config.yaml
```

### Or just print to stdout

No collector, no config ceremony:

```yaml
input:
  type: file
  path: /var/log/app/app.log
  format: json

output:
  type: stdout
  format: console
```

```bash
logfwd --config config.yaml
```

---

## SQL Transforms

The transform is the main reason to use logfwd over a plain forwarder. Every batch of parsed log records is a DataFusion SQL table named `logs`.

> **Column names:** `--generate-json` produces JSON with bare field names (`level`, `message`, `status`, `duration_ms`, `request_id`, `service`, `timestamp`). For JSON sources where every row has a consistent field type, logfwd uses the bare field name as the SQL column name — no type suffix is added. See [Column naming](#column-naming) below.

```sql
-- Forward only errors and slow requests
SELECT level, message, duration_ms, status
FROM logs
WHERE level = 'ERROR'
   OR duration_ms > 1000
```

```sql
-- Extract a field with regex, rename columns
SELECT
  level,
  message,
  regexp_extract(message, 'request_id=([a-f0-9-]+)', 1) AS request_id,
  status
FROM logs
WHERE level IN ('ERROR', 'WARN')
  AND status >= 400
```

**Column naming:** logfwd always uses the bare JSON field name as the SQL column name — no type suffix is appended.

- **Consistent-type field** (same type in every row of a batch): one Arrow column with the native type — e.g. `level` (Utf8View), `status` (Int64).
- **Mixed-type field** (different types across rows in the same batch): a conflict-struct column is produced and automatically normalized to a bare `Utf8` column. Use `int()` / `float()` UDFs for typed access.

| JSON type      | Column name   | Example                                         |
|----------------|---------------|-------------------------------------------------|
| String         | bare name     | `level` (Utf8View)                              |
| Integer        | bare name     | `status` (Int64)                                |
| Float          | bare name     | `latency_ms` (Float64)                          |
| Boolean        | bare name     | `enabled` (stored as `"true"` / `"false"`)      |
| Object / Array | bare name     | `metadata` — raw JSON string                    |

> Nested objects are stored as raw JSON strings, not expanded into sub-fields.

**Built-in UDFs:** `int()`, `float()`, `grok()`, `regexp_extract()` — see [docs/CONFIG_REFERENCE.md](docs/CONFIG_REFERENCE.md).

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
  endpoint: http://otel-collector:4318
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
        endpoint: http://otel-collector:4318

  debug:
    inputs:
      - type: file
        path: /var/log/pods/**/*.log
        format: cri
    outputs:
      - type: stdout
        format: console
```

See [docs/CONFIG_REFERENCE.md](docs/CONFIG_REFERENCE.md) for all YAML fields, input/output types, and enrichment tables.

---

## Kubernetes (CRI) Support

Point logfwd at `/var/log/pods/**/*.log` with `format: cri` and it handles timestamp parsing, stdout/stderr routing, and multi-line log reassembly (CRI partial-log flags).

Every CRI record gets these extra columns:

| Column | Description |
|--------|-------------|
| `_timestamp` | CRI timestamp as an RFC 3339 string |
| `_stream` | `stdout` or `stderr` |

Use `_stream` to filter by stream:

```sql
SELECT _timestamp, _stream, level, message
FROM logs
WHERE _stream = 'stderr'
  AND level = 'ERROR'
```

> **Coming soon:** Kubernetes namespace/pod/container metadata enrichment (`k8s_path`) is implemented in the pipeline but not yet wired into the YAML config schema.

---

## Output destinations

| Output          | Status         | Description |
|-----------------|----------------|-------------|
| `otlp`          | ✅ Implemented  | OTLP protobuf over HTTP or gRPC — works with any OpenTelemetry-compatible receiver |
| `http`          | ✅ Implemented  | JSON lines over HTTP POST, optional zstd compression |
| `stdout`        | ✅ Implemented  | JSON or colored console output — great for local debugging |
| `elasticsearch` | ✅ Implemented   | Elasticsearch bulk API with retry logic, per-document error handling |
| `loki`          | 🚧 Stub         | Struct exists; Loki push API not yet implemented |
| `parquet`       | 🚧 Stub         | Struct exists; Parquet file writing not yet implemented |

---

## CLI Reference

```
logfwd --config <config.yaml>             Run the pipeline
logfwd --config <config.yaml> --validate  Parse and validate config only
logfwd --config <config.yaml> --dry-run   Build pipeline objects, check SQL syntax
logfwd --blackhole [bind_addr]            Start OTLP blackhole for testing
logfwd --generate-json <n> <file>         Generate synthetic test data
logfwd --version                          Print version
```

---

## Deployment

### Docker

```bash
docker run -d \
  --name logfwd \
  -v /var/log:/var/log:ro \
  -v $(pwd)/config.yaml:/etc/logfwd/config.yaml:ro \
  logfwd:latest --config /etc/logfwd/config.yaml
```

### Kubernetes DaemonSet

A ready-to-apply manifest ships at `deploy/daemonset.yml`. It runs one logfwd pod per node and reads all container logs from `/var/log`.

```bash
kubectl apply -f deploy/daemonset.yml
kubectl -n collectors rollout status daemonset/logfwd
```

Typical resource use: ~128 MiB memory, 250m CPU at moderate log volume.

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for full details, resource sizing, and OTLP collector integration examples.

---

## Documentation

### User guides (`docs/`)

| Guide | Description |
|-------|-------------|
| [docs/CONFIG_REFERENCE.md](docs/CONFIG_REFERENCE.md) | All YAML fields, input/output types, SQL transforms, UDFs, enrichment |
| [docs/COLUMN_NAMING.md](docs/COLUMN_NAMING.md) | How JSON fields map to SQL column names |
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | Docker, Kubernetes DaemonSet, resource sizing, OTLP integration |
| [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Common errors, debug mode, diagnostics API |

### Developer guides (`dev-docs/`)

| Guide | Description |
|-------|-------------|
| [dev-docs/ARCHITECTURE.md](dev-docs/ARCHITECTURE.md) | Pipeline data flow, SIMD stages, crate map |
| [dev-docs/DESIGN.md](dev-docs/DESIGN.md) | Vision, target architecture, architecture decision records |
| [dev-docs/VERIFICATION.md](dev-docs/VERIFICATION.md) | TLA+, Kani, proptest — tool selection, tiers, per-module status |
| [dev-docs/CODE_STYLE.md](dev-docs/CODE_STYLE.md) | Code style conventions enforced during review |
| [DEVELOPING.md](DEVELOPING.md) | Build, test, lint, bench commands |
| [CONTRIBUTING.md](CONTRIBUTING.md) | How to contribute — PR process, pre-commit checks |

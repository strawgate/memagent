# logfwd

A high-performance log forwarder. Tails log files, parses JSON and CRI container format, transforms with SQL, and ships to OTLP, HTTP, or stdout.

## Quick start

```bash
cargo build --release -p logfwd
```

Create `config.yaml`:

```yaml
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri

output:
  type: stdout
  format: json
```

Run:

```bash
./target/release/logfwd --config config.yaml
```

## Configuration

logfwd uses YAML configuration with two modes:

**Simple** (single pipeline):

```yaml
input:
  type: file
  path: /var/log/app/*.log
  format: json

transform: SELECT level_str, msg_str, status_int FROM logs WHERE status_int >= 400

output:
  type: otlp
  endpoint: otel-collector:4317
```

**Advanced** (multiple pipelines):

```yaml
pipelines:
  errors:
    input:
      type: file
      path: /var/log/pods/**/*.log
      format: cri
    transform: SELECT * FROM logs WHERE level_str = 'ERROR'
    output:
      type: otlp
      endpoint: otel-collector:4317

  all-logs:
    input:
      type: file
      path: /var/log/pods/**/*.log
      format: cri
    output:
      type: stdout
      format: json
```

### Input types

| Type | Description | Status |
|------|-------------|--------|
| `file` | Tail log files by glob pattern | Implemented |
| `tcp` | TCP listener | Not yet |
| `udp` | UDP listener | Not yet |
| `otlp` | Receive OTLP logs | Not yet |

### Output types

| Type | Description | Status |
|------|-------------|--------|
| `otlp` | OTLP protobuf over HTTP/gRPC | Implemented |
| `http` | JSON lines over HTTP | Implemented |
| `stdout` | Print to stdout (json or text) | Implemented |
| `elasticsearch` | Elasticsearch bulk API | Stub |
| `loki` | Grafana Loki push API | Stub |
| `parquet` | Parquet files | Stub |

### SQL transforms

Transforms use DataFusion SQL. Column names follow the pattern `{field}_{type}`:

```sql
-- Filter by level
SELECT * FROM logs WHERE level_str = 'ERROR'

-- Extract fields
SELECT level_str, status_int, duration_ms_float FROM logs

-- Type casting
SELECT int(status_str) AS status_int FROM logs

-- Pattern matching
SELECT grok('%{IP:client_ip} %{WORD:method}', msg_str) FROM logs
```

Available UDFs: `int()`, `float()`, `grok()`, `regexp_extract()`.

### Enrichment

Enrichment tables are available as DataFusion tables for SQL JOINs:

```yaml
enrichment:
  static_labels:
    type: static
    fields:
      environment: production
      cluster: us-east-1

  k8s:
    type: k8s_path
```

```sql
SELECT l.*, k.namespace, k.pod_name
FROM logs l JOIN k8s k ON l._file_str = k.log_path_prefix
```

## CLI

```
logfwd --config <config.yaml>        Run pipeline
logfwd --config <config.yaml> --validate   Validate config without running
logfwd --config <config.yaml> --dry-run    Build pipelines, don't start
logfwd --blackhole [bind_addr]       OTLP collector for benchmarks
logfwd --generate-json <n> <file>    Generate synthetic test data
logfwd --version                     Print version
```

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for internal design.

See [DEVELOPING.md](DEVELOPING.md) for development guide.

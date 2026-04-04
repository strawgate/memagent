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

transform: SELECT level, message, status FROM logs WHERE status >= 400

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
    transform: SELECT * FROM logs WHERE level = 'ERROR'
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
| `generator` | Synthetic data for benchmarking | Implemented |
| `tcp` | TCP listener | Implemented |
| `udp` | UDP listener | Implemented |
| `otlp` | Receive OTLP logs | Implemented |

### Output types

| Type | Description | Status |
|------|-------------|--------|
| `otlp` | OTLP protobuf over HTTP/gRPC | Implemented |
| `http` | JSON lines over HTTP | Implemented |
| `stdout` | Print to stdout (json or text) | Implemented |
| `elasticsearch` | Elasticsearch bulk API | Implemented |
| `loki` | Grafana Loki push API | Implemented |
| `parquet` | Parquet files | Stub |

### SQL transforms

Transforms use DataFusion SQL. Column names use bare field names by default:

```sql
-- Filter by level
SELECT * FROM logs WHERE level = 'ERROR'

-- Extract fields
SELECT level, status, duration_ms FROM logs

-- Type casting
SELECT int(status) AS status_code FROM logs

-- Pattern matching
SELECT grok('%{IP:client_ip} %{WORD:method}', message) FROM logs
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
FROM logs l JOIN k8s k ON l._source_path = k.log_path_prefix
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

## Documentation

| Guide | Description |
|-------|-------------|
| [Configuration Reference](./config/reference.md) | All YAML fields, input/output types, SQL transforms, UDFs, enrichment |
| [Deployment](./deployment/kubernetes.md) | Kubernetes DaemonSet, Docker, resource sizing, OTLP integration |
| [Troubleshooting](./troubleshooting.md) | Common errors, diagnosing dropped data, diagnostics API, debug mode |
| [SQL Transforms](./config/sql-transforms.md) | DataFusion SQL examples, column naming, UDFs |

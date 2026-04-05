# Configuration Reference

logfwd is configured with a YAML file passed via `--config <path>`.

## Overview

logfwd supports two layout styles:

- **Simple** — single pipeline with top-level `input`, `transform`, and `output` keys.
- **Advanced** — multiple named pipelines under a `pipelines` map.

Environment variables are expanded using `${VAR}` syntax anywhere in the file. If a
variable is not set the placeholder is left as-is.

---

## Simple layout

```yaml
input:
  type: file
  path: /var/log/app/*.log
  format: json

transform: SELECT level, message, status FROM logs WHERE status >= 400

output:
  type: otlp
  endpoint: otel-collector:4317
  compression: zstd

server:
  diagnostics: 0.0.0.0:9090
  log_level: info
```

## Advanced layout

```yaml
pipelines:
  errors:
    inputs:
      - name: pod_logs
        type: file
        path: /var/log/pods/**/*.log
        format: cri
    transform: SELECT * FROM logs WHERE level = 'ERROR'
    outputs:
      - type: otlp
        endpoint: otel-collector:4317

  debug:
    inputs:
      - type: file
        path: /var/log/pods/**/*.log
        format: cri
    outputs:
      - type: stdout
        format: json

server:
  diagnostics: 0.0.0.0:9090
```

The two layouts cannot be mixed: specifying both `input`/`output` at the top level and
a `pipelines` map is a validation error.

---

## Input configuration

Each pipeline requires at least one input. Use a single mapping for one input or a
YAML sequence for multiple inputs.

### Common fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Input type. See [Input types](#input-types). |
| `name` | string | No | Friendly name shown in diagnostics. |
| `format` | string | No | Log format. See [Formats](#formats). Defaults to `auto`. |

### `file` input

Tail one or more log files that match a glob pattern.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | Yes | Glob pattern, e.g. `/var/log/pods/**/*.log`. |

```yaml
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri
```

### `udp` input *(not yet implemented)*

Listen for log lines on a UDP socket.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `listen` | string | Yes | `host:port`, e.g. `0.0.0.0:514`. |

```yaml
input:
  type: udp
  listen: 0.0.0.0:514
  format: syslog
```

### `tcp` input *(not yet implemented)*

Accept log lines on a TCP socket.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `listen` | string | Yes | `host:port`, e.g. `0.0.0.0:5140`. |

```yaml
input:
  type: tcp
  listen: 0.0.0.0:5140
  format: json
```

### `otlp` input *(not yet implemented)*

Receive OTLP log records from another agent or SDK.

No extra fields required; the listen address will be configurable in a future release.

---

## Input types

| Value | Status | Description |
|-------|--------|-------------|
| `file` | Implemented | Tail files matching a glob pattern. |
| `udp` | Planned | Receive log lines over UDP. |
| `tcp` | Planned | Accept log lines over TCP. |
| `otlp` | Planned | Receive OTLP logs. |

---

## Formats

The `format` field controls how raw bytes from the input are parsed into log records.

| Value | Description |
|-------|-------------|
| `auto` | Auto-detect (default). Tries CRI first, then JSON, then raw. |
| `cri` | CRI container log format (`<timestamp> <stream> <flags> <message>`). Multi-line log reassembly via the `P` partial flag is supported. |
| `json` | Newline-delimited JSON. Each line must be a single JSON object. |
| `raw` | Treat each line as an opaque string stored in `_raw_str`. |
| `logfmt` | Key=value pairs (e.g. `level=info msg="hello"`). *Not yet implemented.* |
| `syslog` | RFC 5424 syslog. *Not yet implemented.* |
| `console` | Human-readable coloured output for interactive debugging. Output mode only. |

---

## Output configuration

Each pipeline requires at least one output.

### Common fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Output type. See [Output types](#output-types). |
| `name` | string | No | Friendly name shown in diagnostics. |

### `otlp` output

Send log records as OTLP protobuf to an OpenTelemetry collector.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `endpoint` | string | Yes | — | Collector address, e.g. `otel-collector:4317` (gRPC) or `http://otel-collector:4318` (HTTP). |
| `protocol` | string | No | `http` | `http` or `grpc`. |
| `compression` | string | No | none | `zstd` to compress the request body. |

```yaml
output:
  type: otlp
  endpoint: otel-collector:4317
  protocol: grpc
  compression: zstd
```

### `http` output

POST log records as newline-delimited JSON to an HTTP endpoint.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `endpoint` | string | Yes | Full URL, e.g. `http://ingest.example.com/logs`. |
| `compression` | string | No | `zstd` to compress the request body. |

```yaml
output:
  type: http
  endpoint: http://ingest.example.com/logs
  compression: zstd
```

### `stdout` output

Print records to standard output for local debugging.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `format` | string | No | `json` | `json` (newline-delimited JSON) or `console` (coloured text). |

```yaml
output:
  type: stdout
  format: console
```

### `elasticsearch` output *(stub)*

Ship to Elasticsearch via the bulk API. Not yet functional.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `endpoint` | string | Yes | Elasticsearch base URL. |

### `loki` output *(stub)*

Push to Grafana Loki. Not yet functional.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `endpoint` | string | Yes | Loki push URL. |

### `file_out` output *(partial)*

Write records to a file.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | Yes | Destination file path. |

### `parquet` output *(stub)*

Write records to Parquet files. Not yet functional.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | Yes | Destination file path. |

---

## Output types

| Value | Status | Description |
|-------|--------|-------------|
| `otlp` | Implemented | OTLP protobuf over HTTP or gRPC. |
| `http` | Implemented | JSON lines over HTTP POST. |
| `stdout` | Implemented | Print to stdout (JSON or coloured text). |
| `elasticsearch` | Stub | Elasticsearch bulk API. |
| `loki` | Stub | Grafana Loki push API. |
| `file_out` | Partial | Write to a file. |
| `parquet` | Stub | Write Parquet files. |

---

## SQL transform

The optional `transform` field contains a DataFusion SQL query that is applied to every
Arrow `RecordBatch` produced by the scanner. The source table is always named `logs`.

```yaml
transform: SELECT level, message, status FROM logs WHERE status >= 400
```

Multi-line SQL is supported with YAML block scalars:

```yaml
transform: |
  SELECT
    level,
    message,
    regexp_extract(message, 'request_id=([a-f0-9-]+)', 1) AS request_id,
    status
  FROM logs
  WHERE level IN ('ERROR', 'WARN')
    AND status >= 400
```

### Column naming convention

The scanner maps each JSON field to a typed Arrow column using the field's base
name (no type suffix):

| JSON value type | Arrow column type | Column name | Example |
|-----------------|-------------------|-------------|---------|
| String | StringArray | `{field}` | `level` |
| Integer | Int64Array | `{field}` | `status` |
| Float | Float64Array | `{field}` | `latency_ms` |
| Boolean | StringArray (`"true"`/`"false"`) | `{field}` | `enabled` |
| Null | null in column | `{field}` | — |
| Object / Array | StringArray (raw JSON) | `{field}` | `metadata` |

When a field contains mixed types across rows, the scanner emits a single
**Struct column** under the field's base name containing one child per observed
type (e.g., a `status` Struct with `int` and `str` children). Legacy
single-underscore suffixed columns (`status_int`, `level_str`) are not emitted.

Special columns added by the scanner:

| Column | Type | Description |
|--------|------|-------------|
| `_file_str` | string | Absolute path of the source file (file inputs only). |
| `_raw_str` | string | Original JSON line (only when `keep_raw: true`). |
| `_time_ns_int` | int64 | Timestamp from CRI header in nanoseconds (CRI inputs only). |
| `_stream_str` | string | CRI stream name (`stdout`/`stderr`). |

### Built-in UDFs

| Function | Signature | Description |
|----------|-----------|-------------|
| `int(expr)` | `int(any) → int64` | Cast any value to int64. Returns NULL on failure. |
| `float(expr)` | `float(any) → float64` | Cast any value to float64. Returns NULL on failure. |
| `grok(pattern, input)` | `grok(utf8, utf8) → utf8` | Apply a Grok pattern to `input` and return the first capture as JSON. |
| `regexp_extract(input, pattern, group)` | `regexp_extract(utf8, utf8, int64) → utf8` | Return capture group `group` from a regex match. |

Examples:

```sql
-- Cast a string column to int
SELECT int(status) AS status FROM logs

-- Extract a field with Grok
SELECT grok('%{IP:client} %{WORD:method} %{URIPATHPARAM:path}', message) AS parsed FROM logs

-- Extract a named group with regex
SELECT regexp_extract(message, 'user=([a-z]+)', 1) AS user FROM logs

-- Type-cast from environment-injected string
SELECT float(duration) AS duration_ms FROM logs
```

---

## Enrichment tables

Enrichment tables are made available as SQL tables that can be joined in the transform
query. They are declared under the top-level `enrichment` key.

```yaml
enrichment:
  k8s:
    type: k8s_path

  host:
    type: host_info

  labels:
    type: static
    fields:
      environment: production
      region: us-east-1
```

### `k8s_path` enrichment

Parses Kubernetes pod log paths (e.g.
`/var/log/pods/<namespace>_<pod>_<uid>/<container>/`) to extract metadata.

```sql
SELECT l.level, l.message, k.namespace, k.pod_name, k.container_name
FROM logs l
JOIN k8s k ON l._file_str = k.log_path_prefix
```

Columns exposed by `k8s`:

| Column | Description |
|--------|-------------|
| `log_path_prefix` | Directory prefix used as join key. |
| `namespace` | Kubernetes namespace. |
| `pod_name` | Pod name. |
| `pod_uid` | Pod UID. |
| `container_name` | Container name. |

### `host_info` enrichment

Exposes the hostname of the machine running logfwd.

| Column | Description |
|--------|-------------|
| `hostname` | System hostname. |

### `static` enrichment

A table with one row containing user-defined label columns.

```yaml
enrichment:
  labels:
    type: static
    fields:
      environment: production
      cluster: us-east-1
      tier: backend
```

```sql
SELECT l.*, lbl.environment, lbl.cluster
FROM logs l CROSS JOIN labels lbl
```

---

## Server configuration

The optional `server` block controls the diagnostics server and observability settings.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `diagnostics` | string | none | `host:port` to listen for HTTP diagnostics. See [Diagnostics API](#diagnostics-api). |
| `log_level` | string | `info` | Log verbosity. One of `error`, `warn`, `info`, `debug`, `trace`. |
| `metrics_endpoint` | string | none | OTLP endpoint for periodic metrics push, e.g. `http://otel-collector:4318`. |
| `metrics_interval_secs` | integer | `60` | Push interval for OTLP metrics in seconds. |

```yaml
server:
  diagnostics: 0.0.0.0:9090
  log_level: info
  metrics_endpoint: http://otel-collector:4318
  metrics_interval_secs: 30
```

---

## Diagnostics API

When `server.diagnostics` is configured, logfwd exposes an HTTP API for monitoring and troubleshooting.

| Route | Method | Description |
|-------|--------|-------------|
| `/` | GET | Dashboard HTML (visual explorer for metrics and traces). |
| `/health` | GET | Liveness probe. Returns 200 OK if the server is running. |
| `/ready` | GET | Readiness probe. Returns 200 OK once pipelines are initialized. |
| `/api/pipelines` | GET | Per-pipeline counters (lines, bytes, errors, batches, stage timing). |
| `/api/stats` | GET | Aggregate process stats (uptime, RSS, CPU, aggregate line counts). |
| `/api/config` | GET | Currently loaded YAML configuration and its file path. |
| `/api/logs` | GET | Recent log lines from logfwd's own stderr (ring buffer). |
| `/api/history` | GET | Time-series data (1-hour window) for dashboard charts. |
| `/api/traces` | GET | Recent batch processing spans for detailed latency analysis. |

Note: The `/metrics` (Prometheus) endpoint was removed in favor of `/api/pipelines`. It returns `410 Gone`. The `/api/system` route mentioned in some older documentation does not exist.

---

## Storage configuration

The optional `storage` block controls where logfwd persists state (checkpoints, disk
queue).

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `data_dir` | string | none | Directory for state files. Created if it does not exist. |

```yaml
storage:
  data_dir: /var/lib/logfwd
```

---

## Environment variable substitution

Any value in the config file can reference an environment variable with `${VAR}`:

```yaml
output:
  type: otlp
  endpoint: ${OTEL_COLLECTOR_ADDR}

server:
  metrics_endpoint: ${METRICS_PUSH_URL}
```

If the variable is not set, the placeholder is left as-is (no error).

---

## Complete example

```yaml
pipelines:
  app:
    inputs:
      - name: pod_logs
        type: file
        path: /var/log/pods/**/*.log
        format: cri
    transform: |
      SELECT
        l.level,
        l.message,
        l.status,
        k.namespace,
        k.pod_name,
        k.container_name,
        lbl.environment
      FROM logs l
      LEFT JOIN k8s k ON l._file_str = k.log_path_prefix
      CROSS JOIN labels lbl
      WHERE l.level IN ('ERROR', 'WARN')
        OR l.status >= 500
    outputs:
      - name: collector
        type: otlp
        endpoint: ${OTEL_ENDPOINT}
        protocol: grpc
        compression: zstd
      - name: debug
        type: stdout
        format: console

enrichment:
  k8s:
    type: k8s_path
  labels:
    type: static
    fields:
      environment: ${ENVIRONMENT}
      cluster: ${CLUSTER_NAME}

server:
  diagnostics: 0.0.0.0:9090
  log_level: info
  metrics_endpoint: ${OTEL_ENDPOINT}
  metrics_interval_secs: 60

storage:
  data_dir: /var/lib/logfwd
```

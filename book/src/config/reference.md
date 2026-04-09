# Configuration Reference

logfwd commands that operate on pipeline config (for example `run`, `validate`, `dry-run`, and `effective-config`) accept a YAML file via `--config <config.yaml>`.

## Overview

logfwd supports two layout styles:

- **Simple** — single pipeline with top-level `input`, `transform`, and `output` keys.
- **Advanced** — multiple named pipelines under a `pipelines` map.

Environment variables are expanded using `${VAR}` syntax anywhere in the file.
If a variable is not set, config loading fails fast with a validation error.

This page is the canonical source for config support and status. Other docs may
show examples or task-oriented guidance, but support-status claims for inputs,
outputs, and config-only surfaces should link back here.

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
  endpoint: https://otel-collector:4318/v1/logs
  compression: zstd

server:
  diagnostics: 127.0.0.1:9090
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
        endpoint: https://otel-collector:4318/v1/logs

  debug:
    inputs:
      - type: file
        path: /var/log/pods/**/*.log
        format: cri
    outputs:
      - type: stdout
        format: json

server:
  diagnostics: 127.0.0.1:9090
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
| `poll_interval_ms` | integer | No | How often to poll the file when tailing (default: 50). |
| `read_buf_size` | integer | No | Buffer size for file reads in bytes (default: 262144). |
| `per_file_read_budget_bytes` | integer | No | Maximum bytes read per file per poll (default: 262144). |

```yaml
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri
```

### `generator` input

Emit synthetic records for benchmarks, demos, and pipeline tests.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `generator` | object | No | Generator settings. If omitted, runtime defaults are used, including `batch_size: 1000` and `events_per_sec: 0`. See [Input Types](inputs.md#generator) for the detailed nested fields. |

```yaml
input:
  type: generator
  generator:
    events_per_sec: 50000
    batch_size: 4096
```

### `udp` input

Listen for log lines on a UDP socket.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `listen` | string | Yes | `host:port`, e.g. `0.0.0.0:514`. |

`udp` treats each datagram as one or more newline-delimited records. TLS is
not supported for UDP inputs (DTLS is not implemented).

```yaml
input:
  type: udp
  listen: 0.0.0.0:514
  format: json
```

### `tcp` input

Accept log lines on a TCP socket.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `listen` | string | Yes | `host:port`, e.g. `0.0.0.0:5140`. |

TCP framing uses RFC 6587 octet-counting when a valid `<len><space>` prefix is
present, and falls back to legacy newline framing otherwise. Oversized frames
(`> 1 MiB`) are discarded.

TLS is not supported for TCP inputs (mTLS is not implemented); any `tls:`
block under a `tcp` input is rejected at config validation time.

```yaml
input:
  type: tcp
  listen: 0.0.0.0:5140
  format: json
```

### `otlp` input

Receive OTLP log records from another agent or SDK.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `listen` | string | Yes | `host:port`, e.g. `0.0.0.0:4318`. |

```yaml
input:
  type: otlp
  listen: 0.0.0.0:4318
```

### `http` input

Receive newline-delimited payloads over HTTP `POST`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `listen` | string | Yes | `host:port`, e.g. `0.0.0.0:8081`. |
| `http.path` | string | No | Route path. Must start with `/`. Defaults to `/`. |
| `http.strict_path` | boolean | No | When `true` (default), require exact path match. |
| `http.method` | string | No | Accepted method. Defaults to `POST`. |
| `http.max_request_body_size` | integer | No | Maximum request body size in bytes. Defaults to 20 MiB. |
| `http.response_code` | integer | No | Success code. One of `200`, `201`, `202`, `204` (default `200`). |

```yaml
input:
  type: http
  listen: 0.0.0.0:8081
  format: json
  http:
    path: /ingest
    strict_path: true
    method: POST
    max_request_body_size: 20971520
    response_code: 200
```

### `linux_sensor_beta` input

Linux beta sensor lane for early platform-native ingestion development.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sensor_beta.poll_interval_ms` | integer | No | Heartbeat cadence in milliseconds. Defaults to `10000`. |
| `sensor_beta.emit_heartbeat` | boolean | No | Emit periodic heartbeat rows while idle. Defaults to `true`. |

```yaml
input:
  type: linux_sensor_beta
  sensor_beta:
    poll_interval_ms: 2000
```

### `macos_sensor_beta` input

macOS beta sensor lane for EndpointSecurity-oriented adapter bring-up.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sensor_beta.poll_interval_ms` | integer | No | Heartbeat cadence in milliseconds. Defaults to `10000`. |
| `sensor_beta.emit_heartbeat` | boolean | No | Emit periodic heartbeat rows while idle. Defaults to `true`. |

```yaml
input:
  type: macos_sensor_beta
```

### `windows_sensor_beta` input

Windows beta sensor lane for eBPF/ETW hybrid adapter bring-up.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sensor_beta.poll_interval_ms` | integer | No | Heartbeat cadence in milliseconds. Defaults to `10000`. |
| `sensor_beta.emit_heartbeat` | boolean | No | Emit periodic heartbeat rows while idle. Defaults to `true`. |

```yaml
input:
  type: windows_sensor_beta
```

### `arrow_ipc` input *(not yet supported)*

Reserved for future Arrow IPC ingest. Config parsing recognizes the type, but
config validation currently rejects it.

---

## Input types

| Value | Status | Description |
|-------|--------|-------------|
| `file` | Implemented | Tail files matching a glob pattern. |
| `generator` | Implemented | Emit synthetic JSON-like records from an in-process source. |
| `udp` | Implemented | Receive log lines over UDP. |
| `tcp` | Implemented | Accept log lines over TCP. |
| `otlp` | Implemented | Receive OTLP logs over a bound listen address. |
| `http` | Implemented | Receive newline-delimited payloads via HTTP `POST`. |
| `linux_sensor_beta` | Beta | Linux platform sensor beta lane (heartbeat/control events while integration is in progress). |
| `macos_sensor_beta` | Beta | macOS platform sensor beta lane (heartbeat/control events while integration is in progress). |
| `windows_sensor_beta` | Beta | Windows platform sensor beta lane (heartbeat/control events while integration is in progress). |
| `arrow_ipc` | Not yet supported | Reserved for future Arrow IPC ingest. |

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
| `console` | Human-readable coloured output for interactive debugging. Output mode only. |

---

## Output configuration

Each pipeline requires at least one output.

### Common fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Output type. See [Output types](#output-types). |
| `name` | string | No | Friendly name shown in diagnostics. |

For URL-based outputs (`otlp`, `elasticsearch`, `loki`, `arrow_ipc`, and `http`),
endpoints must be valid `http://` or `https://` URLs. Embedded credentials
(`https://user:pass@host/...`) are rejected; use `output.auth` instead. For
transport safety, non-loopback endpoints must use `https://`. Plain `http://`
is only accepted for loopback targets (`localhost`, `127.0.0.1`, `[::1]`) for
local development.

### `otlp` output

Send log records as OTLP protobuf to an OpenTelemetry collector.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `endpoint` | string | Yes | — | Full collector URL, e.g. `https://otel-collector:4317` (gRPC) or `https://otel-collector:4318/v1/logs` (HTTP). |
| `protocol` | string | No | `http` | `http` or `grpc`. |
| `compression` | string | No | none | `zstd`, `gzip`, or `none` for the request body. |

```yaml
output:
  type: otlp
  endpoint: https://otel-collector:4317
  protocol: grpc
  compression: zstd
```

### `http` output *(not yet supported)*

Reserved for newline-delimited JSON over HTTP POST. Config parsing recognizes
the type, but config validation currently rejects it until runtime support
lands.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `endpoint` | string | Yes | Full URL, e.g. `https://ingest.example.com/logs`. |
| `compression` | string | No | Reserved for future use. |

```yaml
output:
  type: http
  endpoint: https://ingest.example.com/logs
```

### `stdout` output

Print records to standard output for local debugging.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `format` | string | No | `console` | `json` (newline-delimited JSON), `console` (coloured text), or `text` (raw text). |

```yaml
output:
  type: stdout
  format: console
```

### `elasticsearch` output

Ship to Elasticsearch via the Bulk API.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `endpoint` | string | Yes | Elasticsearch base URL. |

### `loki` output

Push to Grafana Loki.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `endpoint` | string | Yes | Loki push URL. |

### `file` output

Write records to a file.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | Yes | Destination file path. |
| `format` | string | No | `json` for NDJSON output, or `text` to write raw lines. |

```yaml
output:
  type: file
  path: /var/log/logfwd/capture.ndjson
  format: json
```

### `parquet` output *(not yet supported)*

Write records to Parquet files.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | Yes | Destination file path. |

---

## Output types

| Value | Status | Description |
|-------|--------|-------------|
| `otlp` | Implemented | OTLP protobuf over HTTP or gRPC. |
| `http` | Not yet supported | Reserved for newline-delimited JSON over HTTP POST. |
| `stdout` | Implemented | Print to stdout (JSON, console, or text). |
| `elasticsearch` | Implemented | Elasticsearch Bulk API with retry and request-mode controls. |
| `loki` | Implemented | Grafana Loki push API with label grouping. |
| `file` | Implemented | Write NDJSON or text to a local file. |
| `null` | Implemented | Drop records intentionally for tests and benchmark baselines. |
| `tcp` | Implemented | Send records to a TCP endpoint. |
| `udp` | Implemented | Send records to a UDP endpoint. |
| `arrow_ipc` | Implemented | Send Arrow IPC payloads to an HTTP endpoint. |
| `parquet` | Not yet supported | Reserved for Parquet file output. |

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

Special columns added by the scanner / input format layer:

| Column | Type | Description |
|--------|------|-------------|
| `_raw` | string | Original input line (only when `keep_raw: true`, or when a non-JSON CRI line is wrapped for scanner safety). |
| `_timestamp` | string | Timestamp from the CRI header as an RFC 3339 string (CRI inputs only). |
| `_stream` | string | CRI stream name (`stdout` / `stderr`). |
| `_source_path` | string | Canonical file path for file-backed rows (JSON and CRI file inputs). |

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
  - type: k8s_path
  - type: host_info
  - type: static
    table_name: labels
    labels:
      environment: production
      region: us-east-1
```

### `k8s_path` enrichment

Parses Kubernetes pod log paths (e.g.
`/var/log/pods/<namespace>_<pod>_<uid>/<container>/`) to extract metadata.

Join file-backed logs on `_source_path`:

```sql
SELECT l.*, k.namespace, k.pod_name, k.container_name
FROM logs l
LEFT JOIN k8s k ON starts_with(l._source_path, k.log_path_prefix)
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
  - type: static
    table_name: labels
    labels:
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
| `metrics_endpoint` | string | none | OTLP endpoint for periodic metrics push, e.g. `https://otel-collector:4318`. |
| `metrics_interval_secs` | integer | `60` | Push interval for OTLP metrics in seconds. |

```yaml
server:
  diagnostics: 127.0.0.1:9090
  log_level: info
  metrics_endpoint: https://otel-collector:4318
  metrics_interval_secs: 30
```

---

## Diagnostics API

When `server.diagnostics` is configured, logfwd exposes an HTTP API for monitoring and troubleshooting.

| Route | Method | Description |
|-------|--------|-------------|
| `/` | GET | Dashboard HTML (visual explorer for metrics and traces). |
| `/live` | GET | Liveness probe. Returns 200 OK if the process and control plane are running. |
| `/ready` | GET | Readiness probe. Returns 200 OK when required components are initialized and in a ready health state; returns 503 while components are still starting, stopping, stopped, failed, or otherwise not ready. |
| `/admin/v1/status` | GET | Canonical rich status payload with live/ready state, component health, and per-pipeline counters. |
| `/admin/v1/stats` | GET | Aggregate process stats (uptime, RSS, CPU, aggregate line counts). |
| `/admin/v1/config` | GET | Currently loaded YAML configuration and its file path (disabled by default; see note below). |
| `/admin/v1/logs` | GET | Recent log lines from logfwd's own stderr (ring buffer). |
| `/admin/v1/history` | GET | Time-series data (1-hour window) for dashboard charts. |
| `/admin/v1/traces` | GET | Recent batch processing spans for detailed latency analysis. |

For input diagnostics, `bytes_total` reflects source payload bytes accepted at
the input boundary. For structured receivers such as OTLP, this is the
accepted request-body size as received on the wire, not the in-memory Arrow
batch footprint or the post-decompression payload size.

Security note:

- Bind diagnostics to loopback unless you intentionally need remote access (for
  example: `127.0.0.1:9090`).
- `GET /admin/v1/config` is disabled by default because configs may contain
  secrets. To enable it for short-lived local debugging, set
  `LOGFWD_UNSAFE_EXPOSE_CONFIG=1` (or `true`) before starting logfwd.

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

If the variable is not set, config loading fails fast with a validation error.

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
        lbl.environment
      FROM logs l
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
  - type: static
    table_name: labels
    labels:
      environment: ${ENVIRONMENT}
      cluster: ${CLUSTER_NAME}

server:
  diagnostics: 127.0.0.1:9090
  log_level: info
  metrics_endpoint: ${OTEL_ENDPOINT}
  metrics_interval_secs: 60

storage:
  data_dir: /var/lib/logfwd
```

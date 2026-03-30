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

transform: SELECT level_str, msg_str, status_int FROM logs WHERE status_int >= 400

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
    transform: SELECT * FROM logs WHERE level_str = 'ERROR'
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
transform: SELECT level_str, msg_str, status_int FROM logs WHERE status_int >= 400
```

Multi-line SQL is supported with YAML block scalars:

```yaml
transform: |
  SELECT
    level_str,
    msg_str,
    regexp_extract(msg_str, 'request_id=([a-f0-9-]+)', 1) AS request_id_str,
    status_int
  FROM logs
  WHERE level_str IN ('ERROR', 'WARN')
    AND status_int >= 400
```

### Column naming convention

The scanner maps each JSON field to one or more typed Arrow columns following the
`{field}_{type}` naming convention:

| JSON value type | Arrow column type | Column name pattern | Example |
|-----------------|-------------------|---------------------|---------|
| String | StringArray | `{field}_str` | `level_str` |
| Integer | Int64Array | `{field}_int` | `status_int` |
| Float | Float64Array | `{field}_float` | `latency_ms_float` |
| Boolean | StringArray (`"true"`/`"false"`) | `{field}_str` | `enabled_str` |
| Null | null in all type columns | — | — |
| Object / Array | StringArray (raw JSON) | `{field}_str` | `metadata_str` |

When a field contains mixed types across rows, separate columns are emitted:
`status_int` and `status_str` can coexist in the same batch.

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
SELECT int(status_str) AS status_int FROM logs

-- Extract a field with Grok
SELECT grok('%{IP:client} %{WORD:method} %{URIPATHPARAM:path}', msg_str) AS parsed_str FROM logs

-- Extract a named group with regex
SELECT regexp_extract(msg_str, 'user=([a-z]+)', 1) AS user_str FROM logs

-- Type-cast from environment-injected string
SELECT float(duration_str) AS duration_ms_float FROM logs
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
SELECT l.level_str, l.msg_str, k.namespace, k.pod_name, k.container_name
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
| `diagnostics` | string | none | `host:port` to listen for HTTP diagnostics. Exposes `/metrics` and `/api/pipelines`. |
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
        l.level_str,
        l.msg_str,
        l.status_int,
        k.namespace,
        k.pod_name,
        k.container_name,
        lbl.environment
      FROM logs l
      LEFT JOIN k8s k ON l._file_str = k.log_path_prefix
      CROSS JOIN labels lbl
      WHERE l.level_str IN ('ERROR', 'WARN')
        OR l.status_int >= 500
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

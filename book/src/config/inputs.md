# Input Types

Support status lives in the [Configuration Reference](reference.md#input-types).
This page focuses on usage notes and examples.

## File

Tail one or more log files with glob pattern support.

```yaml
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri      # cri | json | raw | auto
```

- **Glob re-scanning**: New files matching the pattern are discovered automatically (every 5s).
- **Rotation handling**: Detects file rotation (rename + create) and switches to the new file. Drains remaining data from the old file before switching.
- **Formats**: CRI (Kubernetes container runtime), JSON (newline-delimited), raw (plain text, each line becomes `{"_raw": "..."}`)

## Generator

Emit synthetic JSON log lines for benchmarking and pipeline testing. No external
data source is required.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `generator.events_per_sec` | integer | No | `0` | Target events per second. `0` means unlimited. |
| `generator.batch_size` | integer | No | `1000` | Events emitted per poll/batch. |
| `generator.total_events` | integer | No | `0` | Total events to emit before stopping. `0` means infinite. |
| `generator.profile` | string | No | `logs` | `logs` for generic synthetic request logs, `record` for flat JSON rows assembled from attributes and generated fields. |
| `generator.complexity` | string | No | `simple` | Size/shape for the `logs` profile: `simple` or `complex`. Ignored by the `record` profile. |
| `generator.attributes` | object | No | `{}` | Static scalar JSON fields written into every `record` row (`string`, `number`, `boolean`, or `null`). |
| `generator.sequence.field` | string | No | unset | Output field name for a monotonic generated sequence in `record` rows. |
| `generator.sequence.start` | integer | No | `1` | Initial value for the generated sequence. |
| `generator.event_created_unix_nano_field` | string | No | unset | Adds a source-created nanosecond timestamp field to each `record` row. |

```yaml
input:
  type: generator
  generator:
    events_per_sec: 50000
    batch_size: 4096
    profile: record
    attributes:
      benchmark_id: ${BENCHMARK_ID}
      pod_name: ${POD_NAME}
      stream_id: ${POD_NAME}
      service: bench-emitter
      status: 200
      sampled: true
      deleted_at: null
    sequence:
      field: seq
    event_created_unix_nano_field: event_created_unix_nano
```

Use `generate-json <num_lines> <output_file>` on the CLI to write a fixed number of lines to a file instead.

## UDP

Receive log lines on a UDP socket.

```yaml
input:
  type: udp
  listen: 0.0.0.0:514
  format: json
```

TLS is not supported for UDP inputs.

## TCP

Accept log lines on a TCP socket.

```yaml
input:
  type: tcp
  listen: 0.0.0.0:5140
  format: json
```

Framing behavior:

- Prefer RFC 6587 octet counting (`<len><space><payload>`).
- Fall back to newline-delimited records for legacy senders.
- Records larger than 1 MiB are discarded.

Optional TLS/mTLS config:

```yaml
input:
  type: tcp
  listen: 0.0.0.0:5140
  tls:
    cert_file: /etc/logfwd/tls/server.pem
    key_file: /etc/logfwd/tls/server.key
    require_client_auth: true
    client_ca_file: /etc/logfwd/tls/clients-ca.pem
```

## OTLP

Receive OTLP log records from another agent or SDK.

```yaml
input:
  type: otlp
  listen: 0.0.0.0:4318
```

## HTTP

Receive newline-delimited payloads over HTTP `POST` (NDJSON or raw lines).

```yaml
input:
  type: http
  listen: 0.0.0.0:8081
  format: json     # json | raw
  http:
    path: /ingest  # optional, defaults to /
    strict_path: true
    method: POST
    max_request_body_size: 20971520
    response_code: 200
```

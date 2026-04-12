---
title: "Input Types"
description: "Usage notes, data flow, and examples for each input type"
---

Support status lives in the [Configuration Reference](/configuration/reference/#input-types).
This page covers usage notes, data flow, and configuration examples for every input type.

## File

Tail one or more log files with glob pattern support. New files are discovered automatically and rotation is handled transparently.

### Data flow

```
Glob Pattern (/var/log/**/*.log)
  → Glob Discovery (every 5s)
    → File Watcher (inotify / kqueue)
      → Read Loop (poll_interval_ms)
        → Line Framing (newline split)
          → Format Parse (cri | json | raw)
            → RecordBatch
```

### Configuration

```yaml
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri      # cri | json | raw | auto
```

### Key behaviors

- **Glob re-scanning**: New files matching the pattern are discovered automatically (every 5s).
- **Rotation handling**: Detects file rotation (rename + create) and switches to the new file. Drains remaining data from the old file before switching.
- **Formats**: CRI (Kubernetes container runtime), JSON (newline-delimited), raw (plain text passthrough; line capture typically writes each line to `body` via `line_field_name`).

### Tuning knobs (optional)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `poll_interval_ms` | `50` | How often (ms) the tailer checks for new data when at EOF. |
| `read_buf_size` | `262144` (max `4194304`) | Buffer size for reading file chunks. |
| `per_file_read_budget_bytes` | `262144` | Max bytes from one file per poll iteration before yielding. |
| `adaptive_fast_polls_max` | `8` | Immediate repoll budget after a read-budget hit; `0` disables. |

---

## Generator

Emit synthetic JSON log lines for benchmarking and pipeline testing. No external data source is required.

### Data flow

```
Config (profile + rate + batch_size)
  → Event Template (logs | record)
    → Rate Limiter (events_per_sec)
      → Batch Assembly (batch_size records)
        → Format Serialize (JSON)
          → RecordBatch
```

### Configuration

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
| `generator.timestamp.start` | string | No | `2024-01-15T00:00:00Z` | Start timestamp for the `logs` profile. ISO8601 (`YYYY-MM-DDTHH:MM:SSZ`) or `"now"` for wall-clock time at startup. |
| `generator.timestamp.step_ms` | integer | No | `1` | Milliseconds between events. Positive = forward, negative = backward. |

```yaml
# Record profile (benchmarking)
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

### Timestamp examples (logs profile)

```yaml
# Real-time simulation (timestamps start at now, +1ms per event)
input:
  type: generator
  generator:
    timestamp:
      start: "now"

# Backfill from now (timestamps go backward, 100ms apart)
input:
  type: generator
  generator:
    timestamp:
      start: "now"
      step_ms: -100

# Fixed historical window (5-second intervals)
input:
  type: generator
  generator:
    timestamp:
      start: "2023-06-01T12:00:00Z"
      step_ms: 5000
```

Use `generate-json <num_lines> <output_file>` on the CLI to write a fixed number of lines to a file instead.

---

## UDP

Receive log lines on a UDP socket. Each datagram is treated as a single log record.

### Data flow

```
Remote Sender
  → UDP Socket (bind listen addr)
    → Datagram Receive
      → Line Extract (one record per datagram)
        → Format Parse (json | raw)
          → RecordBatch
```

### Configuration

```yaml
input:
  type: udp
  listen: 0.0.0.0:514
  format: json
```

### Key behaviors

- Each UDP datagram produces one log record.
- TLS is **not** supported for UDP inputs.
- No connection state -- dropped datagrams are silently lost (inherent to UDP).

---

## TCP

Accept log lines on a TCP socket with automatic framing detection.

### Data flow

```
Remote Client
  → TCP Accept (per-connection task)
    → TLS Handshake (optional, mTLS supported)
      → Keepalive
        → Frame Detect (octet-counting or newline)
          → Line Extract
            → Format Parse (json | raw)
              → RecordBatch
```

### Configuration

```yaml
input:
  type: tcp
  listen: 0.0.0.0:5140
  format: json
```

### Framing behavior

- Prefers RFC 6587 octet counting (`<len><space><payload>`).
- Falls back to newline-delimited records for legacy senders.
- Records larger than 1 MiB are discarded.

### Optional TLS / mTLS

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

---

## OTLP

Receive OTLP log records from another agent or OpenTelemetry SDK over HTTP.

### Data flow

```
OTLP Client (agent / SDK)
  → HTTP POST /v1/logs
    → Protobuf Decode (ExportLogsServiceRequest)
      → Resource + Scope Flatten
        → Arrow Convert (typed columns)
          → RecordBatch
```

### Configuration

```yaml
input:
  type: otlp
  listen: 0.0.0.0:4318
```

### Key behaviors

- Accepts the standard OTLP/HTTP log export endpoint.
- Resource and scope attributes are flattened into per-record columns.
- Protobuf wire format (not JSON) is expected from clients.

---

## HTTP

Receive newline-delimited payloads over HTTP POST (NDJSON or raw lines).

### Data flow

```
HTTP Client
  → HTTP POST (path + method filter)
    → Body Read (up to max_request_body_size)
      → Newline Split (NDJSON lines)
        → Format Parse (json | raw)
          → RecordBatch
```

### Configuration

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

### Key behaviors

- When `strict_path` is true, requests to paths other than the configured `path` receive a 404.
- `max_request_body_size` defaults to 20 MiB. Requests exceeding this are rejected.
- The configured `response_code` is returned on successful ingestion.

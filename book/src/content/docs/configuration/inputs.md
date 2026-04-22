---
title: "Input Types"
description: "Usage notes and examples for each input type"
---

Support status lives in the [YAML Reference](/configuration/reference/#input-types).
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
- **Formats**: CRI (Kubernetes container runtime), JSON (newline-delimited), raw (plain text passthrough; line capture typically writes each line to `body` via `line_field_name`)

**Tuning knobs (optional):**

- `poll_interval_ms` (default: 50): How often, in milliseconds, the tailer checks the file for new data when at the end of the file.
- `read_buf_size` (default: 262144, max: 4194304): The buffer size used when reading chunks of the file.
- `per_file_read_budget_bytes` (default: 262144): The maximum bytes to read from a single file during one polling iteration before yielding to other files.
- `adaptive_fast_polls_max` (default: 8): Immediate repoll budget after a read-budget hit; set to `0` to disable adaptive fast repolls.

## Stdin

Read data from standard input, drain outputs, and exit when stdin closes.
This input is intended for `ff send` and piped shell workflows.

```yaml
input:
  type: stdin
  format: auto     # auto | cri | json | raw
```

```bash
cat app.log | ff send --config destination.yaml --format json
kubectl logs pod/app | LOGFWD_CONFIG=destination.yaml ff send --format cri
```

Use `file` input for daemon-style tailing. `stdin` is finite command input; it does not watch paths or discover rotated files.

## Generator

Emit synthetic JSON log lines for benchmarking and pipeline testing. No external
data source is required.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `generator.events_per_sec` | integer | No | `0` | Target events per second. `0` means unlimited. |
| `generator.batch_size` | integer | No | `1000` | Events emitted per poll/batch. |
| `generator.total_events` | integer | No | `0` | Total events to emit before stopping. `0` means infinite. |
| `generator.profile` | string | No | `logs` | Generator profile — see [profiles](#generator-profiles) below. |
| `generator.complexity` | string | No | `simple` | Size/shape for the `logs` profile: `simple` or `complex`. Ignored by other profiles. |
| `generator.attributes` | object | No | `{}` | Static scalar JSON fields written into every `record` row (`string`, `number`, `boolean`, or `null`). |
| `generator.sequence.field` | string | No | unset | Output field name for a monotonic generated sequence in `record` rows. |
| `generator.sequence.start` | integer | No | `1` | Initial value for the generated sequence. |
| `generator.event_created_unix_nano_field` | string | No | unset | Adds a source-created nanosecond timestamp field to each `record` row. |
| `generator.timestamp.start` | string | No | `2024-01-15T00:00:00Z` | Start timestamp for the `logs` profile. ISO8601 (`YYYY-MM-DDTHH:MM:SSZ`) or `"now"` for wall-clock time at startup. |
| `generator.timestamp.step_ms` | integer | No | `1` | Milliseconds between events. Positive = forward, negative = backward. |

### Generator profiles

| Profile | Description | Format | ~Line Size |
|---------|-------------|--------|------------|
| `logs` (default) | Generic JSON logs with timestamp, level, message | JSON | ~200–800B |
| `record` | Custom attribute-based records | JSON | varies |
| `envoy` | Envoy proxy access logs with realistic traffic patterns | JSON | ~420B |
| `cri_k8s` | CRI-format Kubernetes container logs | CRI | ~350B |
| `wide` | Wide structured logs with 20+ fields | JSON | ~600B |
| `narrow` | Narrow JSON logs with 5 fields | JSON | ~120B |
| `cloud_trail` | CloudTrail-like AWS audit events | JSON | ~1400B |

> **Note:** The `cri_k8s` profile produces CRI-format output. FastForward auto-defaults `format: cri` when this profile is selected, so you do not need to set it explicitly.
>
> **Note:** The `generator.timestamp` configuration only applies to the `logs` profile.

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

```yaml
# Envoy access log traffic
input:
  type: generator
  generator:
    profile: envoy
    events_per_sec: 10000
```

```yaml
# CRI Kubernetes logs (note: format auto-defaults to cri)
input:
  type: generator
  format: cri
  generator:
    profile: cri_k8s
    events_per_sec: 5000
```

```yaml
# CloudTrail audit events
input:
  type: generator
  generator:
    profile: cloud_trail
    events_per_sec: 1000
```

### Timestamp examples (`logs` profile)

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

`client_ca_file` enables client certificate verification and is accepted only
when `require_client_auth: true` is set. Supplying a client CA without requiring
client authentication is rejected at startup.

## OTLP

Receive OTLP log records from another agent or SDK.

```yaml
input:
  type: otlp
  listen: 0.0.0.0:4318
  # Experimental: projected_fallback/projected_only require otlp-research builds.
  protobuf_decode_mode: prost
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

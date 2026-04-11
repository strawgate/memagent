---
title: "Monitoring & Diagnostics"
description: "Diagnostics API endpoints and metrics reference"
---

Enable the diagnostics server in your config:

```yaml
server:
  diagnostics: 127.0.0.1:9090
```

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /live` | Liveness probe (process/control-plane only) |
| `GET /ready` | Readiness probe (200 once initialized) |
| `GET /admin/v1/status` | Canonical rich status JSON (live, ready, component health, per-pipeline detail) |
| `GET /admin/v1/stats` | Flattened JSON for polling/benchmarks |
| `GET /admin/v1/config` | View active YAML configuration (disabled by default; enable with `LOGFWD_UNSAFE_EXPOSE_CONFIG=1`) |
| `GET /admin/v1/logs` | View recent log lines from stderr |
| `GET /admin/v1/history` | Time-series data for dashboard charts |
| `GET /admin/v1/traces` | Detailed latency spans for recent batches |
| `GET /` | HTML dashboard |

## Key metrics

| Metric | Description |
|--------|-------------|
| `logfwd_input_lines_total` | Lines read per input |
| `logfwd_transform_lines_in` | Lines entering SQL transform |
| `logfwd_transform_lines_out` | Lines after filtering |
| `logfwd_stage_seconds_total` | Time per stage (scan, transform, output) |
| `logfwd_flush_reason_total` | Flush triggers (size vs timeout) |

## Transport Observability

The `/admin/v1/status` endpoint includes a `transport` object inside each input's JSON representation containing specific metrics for that transport type:

* **File:** exposes `consecutive_error_polls`, representing the current file-tail pressure and backoff state.
* **TCP:** exposes `accepted_connections` (total accepted) and `active_connections` (currently connected clients) indicators.
* **UDP:** exposes `drops_detected` (datagrams dropped due to kernel buffer overflows) and `recv_buffer_size` (actual kernel receive buffer size applied) indicators.

## OTLP metrics push

```yaml
server:
  metrics_endpoint: https://otel-collector:4318
  metrics_interval_secs: 60
```

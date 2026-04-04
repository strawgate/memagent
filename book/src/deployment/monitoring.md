# Monitoring & Diagnostics

Enable the diagnostics server in your config:

```yaml
server:
  diagnostics: 0.0.0.0:9090
```

## Endpoints

| Route | Status | Description |
|-------|--------|-------------|
| `/` | 200 | Interactive dashboard (HTML) |
| `/health` | 200 | Liveness probe |
| `/ready` | 200/503 | Readiness probe (200 once pipelines initialized) |
| `/api/pipelines` | 200 | Per-pipeline metrics |
| `/api/stats` | 200 | Aggregate statistics (uptime, CPU/RSS, total lines) |
| `/api/config` | 200 | Active YAML configuration |
| `/api/logs` | 200 | Recent stderr log lines (up to 1 MiB) |
| `/api/history` | 200 | Time-series metrics history for dashboard |
| `/api/traces` | 200 | Recent batch processing spans (up to 500) |
| `/metrics` | 410 | Removed — returns pointer to `/api/pipelines` |

> **Security note:** The diagnostics server has no authentication.
> Endpoints like `/api/config` and `/api/logs` expose operational details.
> Bind to `127.0.0.1` or a pod-internal address — do not expose to the public internet.

## Key metrics

| Metric | Description |
|--------|-------------|
| `logfwd_input_lines_total` | Lines read per input |
| `logfwd_transform_lines_in` | Lines entering SQL transform |
| `logfwd_transform_lines_out` | Lines after filtering |
| `logfwd_stage_seconds_total` | Time per stage (scan, transform, output) |
| `logfwd_flush_reason_total` | Flush triggers (size vs timeout) |

## OTLP metrics push

```yaml
server:
  metrics_endpoint: http://otel-collector:4318
  metrics_interval_secs: 60
```

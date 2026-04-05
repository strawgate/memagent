# Monitoring & Diagnostics

Enable the diagnostics server in your config:

```yaml
server:
  diagnostics: 0.0.0.0:9090
```

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Liveness probe (uptime, version) |
| `GET /ready` | Readiness probe (200 once initialized) |
| `GET /api/pipelines` | Detailed JSON with per-stage metrics |
| `GET /api/stats` | Flattened JSON for polling/benchmarks |
| `GET /api/config` | View active YAML configuration |
| `GET /api/logs` | View recent log lines from stderr |
| `GET /api/history` | Time-series data for dashboard charts |
| `GET /api/traces` | Detailed latency spans for recent batches |
| `GET /` | HTML dashboard |

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

---
title: "Monitoring & Diagnostics"
description: "Diagnostics API endpoints and metrics reference"
---

:::tip
Enable the diagnostics server to get health probes, pipeline metrics, and a built-in HTML dashboard.
:::

:::caution
The diagnostics API has no authentication. Bind to `127.0.0.1` (localhost only) unless you need external access. In Kubernetes, use `0.0.0.0` with a `ClusterIP` Service so only in-cluster traffic can reach it.
:::

```yaml
server:
  diagnostics: 127.0.0.1:9090
  log_level: info
```

### Log level

The `log_level` field controls the verbosity of FastForward's own stderr output.
Supported values, from most verbose to least:

| Value | When to use |
|-------|-------------|
| `trace` | Deep debugging of I/O loops and buffer internals (very noisy) |
| `debug` | Investigating specific pipeline behavior or connection issues |
| `info` | **Default.** Startup, shutdown, config reload, and periodic summary lines |
| `warn` | Only warnings and errors (recommended for high-throughput production) |
| `error` | Only unrecoverable or action-required errors |

You can change the level at runtime by sending a PUT request:

```bash
curl -X PUT http://localhost:9090/admin/v1/log_level -H 'Content-Type: application/json' -d '"debug"'
```

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /live` | Liveness probe (process/control-plane only) |
| `GET /ready` | Readiness probe (200 once initialized) |
| `GET /admin/v1/status` | Canonical rich status JSON (live, ready, component health, per-pipeline detail) |
| `GET /admin/v1/stats` | Flattened JSON for polling/benchmarks |
| `GET /admin/v1/config` | View active YAML configuration (disabled by default; enable with `FFWD_UNSAFE_EXPOSE_CONFIG=1`) |
| `GET /admin/v1/logs` | View recent log lines from stderr |
| `GET /admin/v1/history` | Time-series data for dashboard charts |
| `GET /admin/v1/traces` | Detailed latency spans for recent batches |
| `GET /` | HTML dashboard |

## HTML dashboard

Visiting `http://<host>:9090/` in a browser opens a built-in, single-page HTML
dashboard. It provides a live view of:

- **Pipeline throughput** -- lines/sec per input and output, with sparkline charts.
- **Flush behavior** -- ratio of size-triggered vs. timeout-triggered flushes.
- **Stage latency** -- time spent in scan, transform, and output stages.
- **Error counts** -- recent transport errors or dropped batches.

The dashboard pulls data from `/admin/v1/history` and refreshes automatically.
No external dependencies or authentication are required. It is a useful
first-look tool before diving into the JSON API or Grafana dashboards.

## Status endpoint

`GET /admin/v1/status` returns the canonical health payload. Here is an
annotated example:

```json
{
  "live": { "status": "live" },
  "ready": { "status": "ready" },
  "pipelines": [
    {
      "name": "host-logs",
      "inputs": [
        {
          "type": "file",
          "lines_total": 184200,
          "bytes_total": 52428800,
          "errors_total": 0
        }
      ],
      "transform": {
        "lines_in": 184200,
        "lines_out": 91040,
        "filter_drop_rate": 0.506
      },
      "outputs": [
        {
          "type": "otlp",
          "lines_total": 91040,
          "bytes_total": 11206400,
          "errors_total": 0
        }
      ]
    }
  ],
  "system": {
    "uptime_seconds": 3621,
    "version": "0.14.0"
  }
}
```

Pipelines are returned as an array. Use `jq '.pipelines[0]'` to access the first pipeline, or filter by name with `jq '.pipelines[] | select(.name == "host-logs")'`.

| Field | Description |
|-------|-------------|
| `live` | `true` when the process is running and the control plane is healthy |
| `ready` | `true` once all pipelines have completed initialization |
| `uptime_secs` | Seconds since the process started |
| `version` | FastForward binary version |
| `pipelines.<name>.input.lines_total` | Total lines read by this input since startup |
| `pipelines.<name>.input.bytes_total` | Total bytes read by this input |
| `pipelines.<name>.input.errors_total` | Cumulative read errors (file permission, connection reset, etc.) |
| `pipelines.<name>.input.transport` | Transport-specific metrics (see Transport Observability below) |
| `pipelines.<name>.transform.lines_in` | Lines entering the SQL transform stage |
| `pipelines.<name>.transform.lines_out` | Lines emitted after filtering |
| `pipelines.<name>.transform.filter_drop_rate` | Fraction of input lines dropped by the filter (higher = more aggressive filter) |
| `pipelines.<name>.output.lines_total` | Lines successfully delivered to the destination |
| `pipelines.<name>.output.errors_total` | Delivery failures (connection errors, timeouts, HTTP 5xx) |
| `pipelines.<name>.output.last_flush_age_secs` | Seconds since the last successful flush; useful for staleness alerts |

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

## Alerting guidance

Use the status endpoint and OTLP metrics to build alerts for the conditions
that matter most. The table below lists recommended thresholds as starting
points -- adjust them based on your traffic patterns and SLOs.

:::tip
Start with a small number of high-signal alerts. It is better to have three
alerts you always act on than twenty you learn to ignore.
:::

| Condition | Metric / check | Suggested threshold | Severity |
|-----------|---------------|---------------------|----------|
| Process down | `/live` returns non-200 | 2 consecutive failures (30 s apart) | Critical |
| Not ready | `/ready` returns non-200 | > 60 s after container start | Warning |
| Output stale | `last_flush_age_secs` | > 120 s | Critical |
| Delivery errors | `output.errors_total` rate | > 0 sustained for 5 min | Warning |
| Input errors | `input.errors_total` rate | > 0 sustained for 5 min | Warning |
| High drop rate | `transform.filter_drop_rate` | > 0.99 (dropping >99% of lines) | Info |
| Memory pressure | Container memory usage | > 85 % of limit | Warning |
| CPU saturation | `logfwd_stage_seconds_total` rate | Approaching `--cpus` limit | Warning |
| UDP drops | `transport.drops_detected` rate | > 0 sustained for 2 min | Warning |

:::caution
An `output.errors_total` that keeps climbing usually means the downstream
collector or storage is unreachable. Check network connectivity and the
destination's health before adjusting FastForward configuration.
:::

## OTLP metrics push

In addition to the pull-based diagnostics API, FastForward can push its own internal
metrics to an OpenTelemetry Collector over OTLP/HTTP.

```yaml
server:
  metrics_endpoint: https://otel-collector:4318
  metrics_interval_secs: 60
```

| Field | Description |
|-------|-------------|
| `metrics_endpoint` | URL of the OTLP HTTP receiver (typically port 4318) |
| `metrics_interval_secs` | How often FastForward pushes a metrics batch (default: 60) |

### What gets pushed

All of the counters and histograms listed in the Key metrics table above are
exported as OTLP metrics, using the `logfwd_` prefix (metric prefix will change to `ffwd_` in a future release). Each metric includes
resource attributes identifying the host and FastForward instance. The payload uses
OTLP protobuf encoding over HTTP.

### Verifying the push path

```bash
# 1. Confirm FastForward is sending metrics (look for export lines in debug logs)
docker logs ffwd 2>&1 | grep -i "metrics export"

# 2. Query the collector's own metrics to see ingest counts
curl -s http://otel-collector:8888/metrics | grep otelcol_receiver_accepted_metric_points

# 3. Check the status endpoint for push errors
curl -s http://localhost:9090/admin/v1/status | jq '.metrics_push'
```

:::tip
If `metrics_endpoint` is not set, FastForward does not attempt to push metrics.
The pull-based `/admin/v1/status` and `/admin/v1/stats` endpoints remain
available regardless.
:::

## What's next

- [Docker Deployment](/deployment/docker/) -- container setup, volumes, and resource constraints.
- [Kubernetes Deployment](/deployment/kubernetes/) -- DaemonSet manifests and production defaults.
- [Output Types](/configuration/outputs/) -- configure OTLP and other destinations.
- [Troubleshooting](/troubleshooting/) -- common issues and diagnostic steps.

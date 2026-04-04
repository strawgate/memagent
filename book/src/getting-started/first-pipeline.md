# Your First Pipeline

This guide walks through a production-ready logfwd setup: tail application logs, filter by severity, ship to an OpenTelemetry Collector, and monitor the pipeline.

If you haven't run logfwd yet, start with the [Quick Start](./quickstart.md).

---

## The scenario

You have JSON application logs at `/var/log/app/*.log` and an OpenTelemetry Collector (or Grafana Alloy) accepting OTLP on port 4318. You want to forward only warnings and errors.

## Config

```yaml
# pipeline.yaml
input:
  type: file
  path: /var/log/app/*.log
  format: json

transform: |
  SELECT level, message, status, duration_ms, service
  FROM logs
  WHERE level IN ('ERROR', 'WARN')

output:
  type: otlp
  endpoint: http://otel-collector:4318/v1/logs
  compression: zstd

server:
  diagnostics: 0.0.0.0:9090
```

**What each section does:**

- **input** — Tails all `.log` files matching the glob. `format: json` tells the scanner to parse JSON. New files matching the glob are picked up automatically.
- **transform** — SQL filter applied to every batch. Only `ERROR` and `WARN` records pass through. The `SELECT` also controls which columns are forwarded — here we keep five fields and drop the rest.
- **output** — Encodes to OTLP protobuf, compresses with zstd, and sends over HTTP. Replace `otel-collector` with your actual collector hostname.
- **server.diagnostics** — Starts a diagnostics HTTP server for health checks and metrics.

## Validate before running

Always validate config before deploying. (`--validate` checks YAML; `--dry-run` also compiles the SQL — see [Quick Start](./quickstart.md#validate-before-deploying) for details.)

```bash
logfwd --config pipeline.yaml --validate
logfwd --config pipeline.yaml --dry-run
```

## Run it

```bash
logfwd --config pipeline.yaml
```

## Monitor it

The diagnostics server gives you three endpoints:

```bash
# Health check — returns 200 when the pipeline is running
curl http://localhost:9090/health

# Prometheus-format metrics — lines processed, bytes sent, errors
curl http://localhost:9090/metrics

# Pipeline details — JSON with per-pipeline state, input/output stats
curl http://localhost:9090/api/pipelines | jq .
```

---

## Multiple pipelines

When you need different filters or destinations for the same logs, use named pipelines:

```yaml
# pipeline.yaml
pipelines:
  errors-to-collector:
    inputs:
      - type: file
        path: /var/log/app/*.log
        format: json
    transform: SELECT * FROM logs WHERE level = 'ERROR'
    outputs:
      - type: otlp
        endpoint: http://otel-collector:4318/v1/logs
        compression: zstd

  all-to-console:
    inputs:
      - type: file
        path: /var/log/app/*.log
        format: json
    outputs:
      - type: stdout
        format: console

server:
  diagnostics: 0.0.0.0:9090
```

Each pipeline runs independently. The `errors-to-collector` pipeline ships only errors to your collector; the `all-to-console` pipeline prints everything to stdout for local debugging. Both read from the same files.

---

## Kubernetes CRI logs

On Kubernetes, container logs use CRI format instead of plain JSON. Point logfwd at the pod log directory with `format: cri`:

```yaml
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri
```

CRI records get two extra columns: `_timestamp` (RFC 3339) and `_stream` (`stdout` or `stderr`). Use them in your SQL:

```sql
SELECT _timestamp, _stream, level, message
FROM logs
WHERE _stream = 'stderr' AND level = 'ERROR'
```

See the [Kubernetes deployment guide](../deployment/kubernetes.md) for a ready-to-apply DaemonSet manifest and resource sizing.

---

## What's next

| Guide | What you'll learn |
|-------|-------------------|
| [SQL Transforms](../config/sql-transforms.md) | JOINs, UDFs, enrichment tables, column naming rules |
| [Configuration Reference](../config/reference.md) | Every YAML field and option |
| [Kubernetes Deployment](../deployment/kubernetes.md) | DaemonSet, Helm, resource sizing |
| [Troubleshooting](../troubleshooting.md) | Common errors, debug mode, diagnostics API |

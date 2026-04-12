---
title: "Your First Pipeline"
description: "Production-ready logfwd config with monitoring, CRI format, and multi-pipeline setup"
---

This guide builds on the [Quick Start](/getting-started/quickstart/) to set up a production-ready pipeline. You'll configure CRI log parsing for Kubernetes, enable monitoring, and set up multiple pipelines.

:::tip[Prerequisites]
You need `logfwd` installed and working. If you haven't run through the [Quick Start](/getting-started/quickstart/) yet, do that first — it takes about 10 minutes and covers the basics.
:::

## A production config

This config tails Kubernetes pod logs in CRI format, filters by severity, and forwards to an OTLP collector with compression and monitoring enabled.

```yaml
# pipeline.yaml
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri

transform: |
  SELECT * FROM logs WHERE level IN ('ERROR', 'WARN')

output:
  type: otlp
  endpoint: https://otel-collector:4318/v1/logs
  compression: zstd

server:
  diagnostics: 0.0.0.0:9090
  log_level: info
```

**What's different from the Quick Start config:**

- **`format: cri`** — Kubernetes container runtime format (`<timestamp> <stream> <flags> <message>`). logfwd strips the CRI prefix, reassembles multi-line logs via the `P` partial flag, and exposes `_timestamp` and `_stream` as SQL columns.
- **`path: /var/log/pods/**/*.log`** — Recursive glob matching all pod log files. logfwd discovers new files automatically (every 5 seconds) and handles rotation.
- **`compression: zstd`** — Compresses OTLP payloads before sending. Reduces network egress significantly.
- **`server.diagnostics`** — Exposes an HTTP API for health checks, metrics, and pipeline status.

## Validate before running

Always validate before deploying. `validate` catches YAML errors; `dry-run` goes further and compiles the SQL against the Arrow schema.

```bash
logfwd validate --config pipeline.yaml
# config ok: 1 pipeline(s)

logfwd dry-run --config pipeline.yaml
#   ready: default
# dry run ok: 1 pipeline(s)
```

:::caution
`dry-run` catches column name typos and type mismatches that `validate` cannot detect. Always run both before deploying a new config.
:::

## Run and monitor

```bash
logfwd run --config pipeline.yaml
```

With `diagnostics` enabled, you can check pipeline health:

```bash
# Liveness probe (is the process running?)
curl -s http://localhost:9090/live
# {"status":"ok"}

# Readiness probe (are all components initialized?)
curl -s http://localhost:9090/ready
# {"status":"ok"}

# Full pipeline status with counters
curl -s http://localhost:9090/admin/v1/status | jq .
```

**Key counters to watch in `/admin/v1/status`:**

| Counter | What it tells you |
|---------|-------------------|
| `inputs[].lines_total` | Lines read from source — should increase steadily |
| `transform.lines_in` / `lines_out` | Lines entering vs. leaving the SQL filter |
| `outputs[].lines_total` | Lines successfully sent to the destination |
| `outputs[].errors` | Send failures — should stay at 0 |

If `lines_in` increases but `lines_out` stays at 0, your SQL `WHERE` clause is filtering everything out. Try removing the filter temporarily to confirm data flows end-to-end.

## Multiple pipelines

For more complex setups, use the advanced layout to run multiple pipelines in a single logfwd instance. Each pipeline has its own inputs, transform, and outputs.

```yaml
# multi-pipeline.yaml
pipelines:
  errors:
    inputs:
      - type: file
        path: /var/log/pods/**/*.log
        format: cri
    transform: |
      SELECT level, message, _timestamp, _stream
      FROM logs
      WHERE level IN ('ERROR', 'WARN')
    outputs:
      - type: otlp
        endpoint: https://otel-collector:4318/v1/logs
        compression: zstd

  all-logs:
    inputs:
      - type: file
        path: /var/log/pods/**/*.log
        format: cri
    outputs:
      - type: stdout
        format: json

server:
  diagnostics: 0.0.0.0:9090
  log_level: info
```

The `errors` pipeline filters and ships to your collector. The `all-logs` pipeline streams everything to stdout for debugging. Each pipeline runs independently with its own thread.

:::note
The simple layout (`input`/`output` at the top level) and advanced layout (`pipelines` map) cannot be mixed in the same config file.
:::

## Enrichment with Kubernetes metadata

Use enrichment tables to attach pod metadata to every log record without calling the Kubernetes API:

```yaml
enrichment:
  - type: static
    table_name: labels
    labels:
      environment: production
      cluster: us-east-1

transform: |
  SELECT
    l.*,
    lbl.environment,
    lbl.cluster
  FROM logs l
  CROSS JOIN labels lbl
  WHERE l.level IN ('ERROR', 'WARN')
```

The `static` enrichment creates a one-row table with your labels. `CROSS JOIN` attaches them to every log record. See the [Configuration Reference](/configuration/reference/#enrichment-tables) for other enrichment types including `k8s_path` and `host_info`.

## What's next

| Guide | What you'll learn |
|-------|-------------------|
| [Configuration Reference](/configuration/reference/) | Every YAML field, input/output type, and option |
| [SQL Transforms](/configuration/sql-transforms/) | Full SQL reference — UDFs, enrichment tables, column naming |
| [Kubernetes Deployment](/deployment/kubernetes/) | DaemonSet manifest, resource sizing, OTLP collector integration |
| [Monitoring & Diagnostics](/deployment/monitoring/) | Diagnostics API endpoints and metrics |
| [Troubleshooting](/troubleshooting/) | Symptom-based triage for common issues |

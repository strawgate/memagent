---
title: "Quick Start"
description: "Get a working logfwd pipeline in about 10 minutes"
---

Get a working logfwd pipeline in about 10 minutes, then build on it.

## Prerequisites

You need the `logfwd` binary. See [Installation](/memagent/getting-started/installation/) for all options, or grab it quickly:

```bash
# Download the latest release (macOS Apple Silicon shown)
curl -fsSL https://github.com/strawgate/memagent/releases/latest/download/logfwd-darwin-arm64 -o logfwd
chmod +x logfwd

# Or build from source (Rust 1.85+)
cargo build --release --bin logfwd && cp target/release/logfwd .
```

Verify it works:

```bash
./logfwd --version
```

---

## Stage 1: See log output

Generate synthetic JSON log lines and print them to your terminal.

**Generate test data:**

```bash
./logfwd generate-json 10000 logs.json
```

This creates 10,000 JSON log lines with fields like `level`, `message`, `status`, `duration_ms`, and `service`.

**Create a config file:**

```yaml
# config.yaml
input:
  type: file
  path: logs.json
  format: json

output:
  type: stdout
  format: console
```

**Run it:**

```bash
./logfwd run --config config.yaml
```

You'll see a startup banner followed by colored output for every log line:

```
logfwd v0.1.0

  ✓  default
     in   file  logs.json
     out  stdout

ready · 1 pipeline

10:30:00.000Z  INFO   request handled GET /api/v1/users/10000  duration_ms=1 request_id=... service=myapp status=200
10:30:00.000Z  WARN   request handled POST /api/v2/orders/10015  duration_ms=87 request_id=... service=myapp status=429
10:30:00.000Z  ERROR  request handled GET /health/10021  duration_ms=40 request_id=... service=myapp status=503
10:30:00.000Z  DEBUG  request handled GET /api/v1/users/10033  duration_ms=3 request_id=... service=myapp status=200
...
```

logfwd parsed every JSON line, detected field types automatically (strings, integers, floats), built Arrow RecordBatches, and printed them in a human-readable format. All 10,000 lines stream through in under a second.

:::note
logfwd exits when it reaches the end of a finite file. In production you'd point it at a log file that's actively being appended to, and logfwd will tail it continuously — like `tail -f`, but with parsing and SQL.
:::

---

## Stage 2: Filter with SQL

Now add a SQL transform to keep only what matters. This is the core reason to use logfwd — every batch of parsed records becomes a DataFusion SQL table named `logs`.

First, regenerate the test data — logfwd tracks file positions between runs so it doesn't reprocess data it has already seen:

```bash
./logfwd generate-json 10000 logs.json
```

**Update your config:**

```yaml
# config.yaml
input:
  type: file
  path: logs.json
  format: json

transform: |
  SELECT level, message, status, duration_ms
  FROM logs
  WHERE level = 'ERROR' AND duration_ms > 50

output:
  type: stdout
  format: console
```

**Run it:**

```bash
./logfwd run --config config.yaml
```

Now you see far fewer lines — only errors with slow durations:

```
ERROR  request handled GET /api/v2/products/10049  status=500 duration_ms=92
ERROR  request handled POST /api/v1/orders/10121  status=503 duration_ms=78
...
```

Only the four columns from the `SELECT` appear — `level`, `message`, `status`, `duration_ms`. Everything else was filtered out before it reached the output. In production, this means you only ship the logs you care about — saving bandwidth, storage, and money.

**Try a more advanced transform:**

```yaml
transform: |
  SELECT
    level,
    regexp_extract(message, '(GET|POST|PUT|DELETE) (\S+)', 2) AS path,
    status,
    duration_ms
  FROM logs
  WHERE status >= 400
```

This extracts the URL path from the message with a regex and keeps only 4xx/5xx responses. Full SQL — `JOIN`, `GROUP BY`, `HAVING`, subqueries — all works.

:::caution
`ORDER BY` is valid SQL and works correctly within each batch. However, logfwd processes data in streaming batches, so ordering only applies within a single batch, not globally across all data.
:::

Built-in UDFs: `int()`, `float()`, `regexp_extract()`, `grok()`, `json()`, `json_int()`, `json_float()`. The `geo_lookup()` UDF is also available when a geo-IP database is configured. See the [SQL Transforms guide](/memagent/configuration/sql-transforms/) for the complete reference.

---

## Stage 3: Ship to a collector

In production, logfwd sends OTLP protobuf to an OpenTelemetry Collector, Grafana Alloy, or any OTLP-compatible backend. Let's try it with logfwd's built-in blackhole receiver — it accepts OTLP data and discards it, so you can test the full pipeline without external infrastructure.

**Start the blackhole receiver:**

```bash
./logfwd blackhole &
# logfwd blackhole starting on 127.0.0.1:4318
```

**Regenerate test data and update your config to send OTLP:**

```bash
./logfwd generate-json 10000 logs.json
```

```yaml
# config.yaml
input:
  type: file
  path: logs.json
  format: json

transform: |
  SELECT level, message, status, duration_ms
  FROM logs
  WHERE level IN ('ERROR', 'WARN')

output:
  type: otlp
  endpoint: http://127.0.0.1:4318/v1/logs
  compression: zstd
```

**Run it:**

```bash
./logfwd run --config config.yaml
```

logfwd parses the JSON, runs the SQL filter, encodes matching records as OTLP protobuf with zstd compression, and ships them over HTTP. The blackhole receiver accepts everything.

To ship to a real collector, replace the endpoint:

```yaml
output:
  type: otlp
  endpoint: https://otel-collector:4318/v1/logs   # your real collector
  compression: zstd
```

logfwd works out of the box with:
- [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/)
- [Grafana Alloy](https://grafana.com/oss/alloy/)
- Any backend that speaks OTLP over HTTP or gRPC

---

## Validate before deploying

:::tip
Always validate before deploying. `validate` catches YAML errors; `dry-run` goes further and compiles the SQL against the Arrow schema, catching column name typos and type mismatches before any data flows.
:::

```bash
./logfwd validate --config config.yaml
#   ready: default
# config ok: 1 pipeline(s)

./logfwd dry-run --config config.yaml
#   ready: default
# dry run ok: 1 pipeline(s)
```

---

## What's next

| Guide | What you'll learn |
|-------|-------------------|
| [Your First Pipeline](/memagent/getting-started/first-pipeline/) | Production config with monitoring, multi-pipeline setup, CRI format |
| [SQL Transforms](/memagent/configuration/sql-transforms/) | Full SQL reference — JOINs, UDFs, enrichment tables, column naming |
| [Configuration Reference](/memagent/configuration/reference/) | Every YAML field, input/output type, and option |
| [Kubernetes Deployment](/memagent/deployment/kubernetes/) | DaemonSet, resource sizing, OTLP collector integration |
| [Troubleshooting](/memagent/troubleshooting/) | Common errors, debug mode, diagnostics API |

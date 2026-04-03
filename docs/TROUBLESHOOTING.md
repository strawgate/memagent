# Troubleshooting

This guide helps you diagnose and fix common problems with logfwd.

---

## Common error messages

### `config validation error: pipeline '...' has no inputs`

The named pipeline has an empty `inputs` list (or no `input` key in simple layout).

```yaml
# Wrong — no inputs
pipelines:
  app:
    outputs:
      - type: stdout

# Fixed
pipelines:
  app:
    inputs:
      - type: file
        path: /var/log/app/*.log
    outputs:
      - type: stdout
```

### `config validation error: file input requires 'path'`

A `file` input is missing the required `path` glob.

```yaml
# Wrong
input:
  type: file

# Fixed
input:
  type: file
  path: /var/log/app/*.log
```

### `config validation error: otlp output requires 'endpoint'`

An `otlp`, `http`, `elasticsearch`, or `loki` output is missing the required
`endpoint` field.

```yaml
# Wrong
output:
  type: otlp

# Fixed
output:
  type: otlp
  endpoint: otel-collector:4317
```

### `config validation error: cannot mix top-level input/output with pipelines`

You used both the simple layout (top-level `input`/`output`) and the advanced layout
(`pipelines` map) in the same file. Choose one.

### `config YAML error: ...`

The YAML is malformed. Common causes:

- Indentation errors (YAML requires consistent spaces, not tabs).
- Missing quotes around values that contain special characters like `:`.
- Multi-line SQL not using a block scalar (`|`):

```yaml
# Wrong — colon in SQL breaks YAML
transform: SELECT level_str FROM logs WHERE level_str = 'ERROR'

# Fixed — use block scalar
transform: |
  SELECT level_str FROM logs WHERE level_str = 'ERROR'
```

### `error sending to OTLP endpoint: connection refused`

logfwd cannot reach the configured OTLP collector.

1. Verify the endpoint address and port are correct.
2. Check that the collector is running:
   ```bash
   curl -v http://otel-collector:4318/v1/logs
   ```
3. If using gRPC (`protocol: grpc`), ensure port 4317 is open; for HTTP use 4318.
4. In Kubernetes, verify the service name resolves from within the pod:
   ```bash
   kubectl exec -n collectors <pod> -- nslookup otel-collector
   ```

### `error watching path: No such file or directory`

The glob pattern in a `file` input matched no files and the base directory does not
exist. Ensure the directory is mounted and the pattern is correct.

### `error watching path: permission denied`

logfwd cannot read the log directory. In Kubernetes, confirm the `varlog` volume
is mounted with `readOnly: true` at `/var/log` and that the container user has read
access.

---

## Diagnosing dropped or missing data

### Step 1 — Check transform filter_drop_rate

Call the `/api/pipelines` endpoint (see [Reading /api/pipelines](#reading-apipipelines)
below). Look at the `transform.filter_drop_rate` field. A value close to `1.0` means
almost all records are being dropped by your `WHERE` clause.

Example: you intended to keep errors but accidentally wrote a filter that matches
nothing:

```sql
-- Typo — 'error' vs 'ERROR'
WHERE level_str = 'error'
```

Fix: adjust the WHERE clause or remove it temporarily to confirm records are flowing.

### Step 2 — Check input and output line counts

In `/api/pipelines`, compare:

- `inputs[*].lines_total` — lines read from source
- `transform.lines_in` — lines entering the transform stage
- `transform.lines_out` — lines leaving the transform stage
- `outputs[*].lines_total` — lines successfully sent

If `inputs[*].lines_total` is zero, logfwd is not reading any files. Check the
glob pattern and confirm new lines are being appended to the files.

If `outputs[*].errors` is non-zero, there are delivery failures. Check the logs
for `error sending` messages.

### Step 3 — Verify file tailing

Use `--dry-run` to confirm the file input starts without error:

```bash
logfwd --config config.yaml --dry-run
```

Enable debug logging to see file discovery and tail events:

```yaml
server:
  log_level: debug
```

Look for log lines like:

```
[DEBUG] tailing /var/log/pods/app_pod-xyz/app/0.log
[DEBUG] read 4096 bytes from /var/log/pods/app_pod-xyz/app/0.log
```

If files appear in the log but no records reach the output, the format parser may
be discarding lines. Switch to `format: raw` temporarily to confirm raw lines are
flowing:

```yaml
input:
  type: file
  path: /var/log/app/*.log
  format: raw
```

### Step 4 — Check for output errors

If `outputs[*].errors` is increasing, look at stderr for the specific error. Common
causes:

| Symptom | Cause | Fix |
|---------|-------|-----|
| `connection refused` | Collector is down or unreachable | Check network connectivity and collector health |
| `413 Request Entity Too Large` | Batch too large | Reduce batch size (future config option) |
| `401 Unauthorized` | Missing auth token | Add `Authorization` header support (not yet implemented) |
| Slow `output_s` in stage_seconds | Network latency to collector | Use compression (`compression: zstd`) or move collector closer |

---

## Reading /api/pipelines

Enable the diagnostics server:

```yaml
server:
  diagnostics: 0.0.0.0:9090
```

Then query it:

```bash
curl -s http://localhost:9090/api/pipelines | jq .
```

### Response schema

```json
{
  "pipelines": [
    {
      "name": "default",
      "inputs": [
        {
          "name": "pod_logs",
          "type": "file",
          "lines_total": 1024000,
          "bytes_total": 204800000,
          "errors": 0
        }
      ],
      "transform": {
        "sql": "SELECT * FROM logs WHERE level_str = 'ERROR'",
        "lines_in": 1024000,
        "lines_out": 2048,
        "errors": 0,
        "filter_drop_rate": 0.998
      },
      "outputs": [
        {
          "name": "collector",
          "type": "otlp",
          "lines_total": 2048,
          "bytes_total": 512000,
          "errors": 0
        }
      ],
      "batches": {
        "total": 512,
        "avg_rows": 4.0,
        "flush_by_size": 500,
        "flush_by_timeout": 12
      },
      "stage_seconds": {
        "scan": 1.234567,
        "transform": 0.012345,
        "output": 0.456789
      }
    }
  ],
  "system": {
    "uptime_seconds": 3600,
    "version": "0.1.0"
  }
}
```

### Key fields

| Field | Description |
|-------|-------------|
| `inputs[*].lines_total` | Total log lines read from this input since start. |
| `inputs[*].bytes_total` | Total bytes read from this input since start. |
| `inputs[*].errors` | Total read errors (file open failures, etc.). |
| `transform.lines_in` | Lines entering the SQL transform. |
| `transform.lines_out` | Lines produced by the SQL transform after filtering. |
| `transform.filter_drop_rate` | Fraction of input lines dropped: `1 - (lines_out / lines_in)`. |
| `outputs[*].lines_total` | Lines successfully delivered to this output. |
| `outputs[*].errors` | Delivery errors (network failures, HTTP errors, etc.). |
| `batches.total` | Total Arrow batches processed. |
| `batches.avg_rows` | Average rows per batch. |
| `batches.flush_by_size` | Batches flushed because they reached the row limit. |
| `batches.flush_by_timeout` | Batches flushed because the timeout expired. |
| `stage_seconds.scan` | Total CPU time spent in the SIMD scanner. |
| `stage_seconds.transform` | Total CPU time spent in DataFusion SQL. |
| `stage_seconds.output` | Total CPU time spent in the output sink (includes network). |
| `system.uptime_seconds` | Seconds since logfwd started. |

---

## Debug mode / increasing log verbosity

Set `log_level` in the `server` block:

```yaml
server:
  log_level: debug
```

Available levels (least to most verbose): `error`, `warn`, `info`, `debug`, `trace`.

logfwd writes all log output to **stderr**. Redirect it to a file if needed:

```bash
logfwd --config config.yaml 2>logfwd.log
```

In Kubernetes:

```bash
kubectl -n collectors logs daemonset/logfwd
```

### What each level shows

| Level | What you see |
|-------|-------------|
| `error` | Fatal errors only. |
| `warn` | Recoverable problems (e.g. failed delivery, retrying). |
| `info` | Pipeline start/stop, file discovery, batch flush summaries. |
| `debug` | Per-file tail events, batch sizes, SQL plan details. |
| `trace` | Per-record scanner output, individual JSON field extraction. |

> **Warning:** `trace` level generates very large output on busy nodes. Use only for
> short debugging sessions.

---

## Validating configuration

```bash
# Check syntax and field types only (fast)
logfwd --config config.yaml --validate

# Build full pipeline objects including SQL parsing (slower, catches SQL errors)
logfwd --config config.yaml --dry-run
```

Both commands print a success message or a detailed error to stderr and exit without
starting any pipelines.

---

## Checking the SQL transform

Use `--dry-run` to catch SQL syntax errors:

```bash
logfwd --config config.yaml --dry-run
# error: SQL error: Execution error: column "leve_str" not found
```

Test your SQL against a sample file using the `stdout` output:

```yaml
input:
  type: file
  path: /path/to/sample.log
  format: json

transform: |
  SELECT level_str, message_str FROM logs WHERE status_int >= 500

output:
  type: stdout
  format: console
```

Run once and inspect the output:

```bash
logfwd --config test.yaml
```

---

## Performance issues

### High CPU usage

1. Check `stage_seconds` in `/api/pipelines`. If `transform` dominates, simplify the
   SQL query or add indexes via WHERE-clause pushdown.
2. If `scan` dominates, check the input volume with `inputs[*].bytes_total /
   system.uptime_seconds`. logfwd processes ~1.7 GB/s on a single core; sustained
   CPU near 100 % on a fast input is expected.

### High memory usage

1. Confirm `keep_raw: false` (the default). Setting `keep_raw: true` stores the full
   JSON line and can double or triple memory consumption.
2. Check the number of unique field names across your log lines. Each unique field
   produces at least one Arrow column; very wide schemas use more memory per batch.

### Records accumulating / not being shipped

Check `outputs[*].errors` — if non-zero, delivery is failing and records are being
discarded. Enable `log_level: warn` to see delivery error messages.

Check `batches.flush_by_timeout` vs `batches.flush_by_size`. If nearly all flushes
are timeout-driven, the input rate is low and latency is bounded by the flush timeout
(expected behaviour).

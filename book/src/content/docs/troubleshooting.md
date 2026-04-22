---
title: "Troubleshooting"
description: "Symptom-based triage for common FastForward issues"
---

Use this page when FastForward is running but results are wrong, incomplete, or unstable.
Start with the symptom table, run the exact checks, and compare expected output before changing config.

:::tip[Before you start]
- Use a config that passes validation: `ff validate --config config.yaml`.
- Enable diagnostics while debugging:

```yaml
server:
  diagnostics: 127.0.0.1:9090
  log_level: info
```

- Keep one terminal tailing logs:

```bash
kubectl -n collectors logs -f daemonset/logfwd
# or local/docker: docker logs -f logfwd
```
:::

## Symptom-first triage

| Symptom | First check | Expected output | If not expected |
|---|---|---|---|
| No logs arrive at destination | `curl -s http://localhost:9090/admin/v1/status | jq '.pipelines[0].inputs'` | `lines_total` increasing | Fix file path/mount permissions |
| Logs read, but nothing forwarded | `curl -s http://localhost:9090/admin/v1/status | jq '.pipelines[0].transform'` | `lines_in > 0` and `lines_out > 0` | Transform filter dropping all rows |
| Frequent OTLP send errors | Check runtime logs for `error sending` | No repeated connection/auth errors | Fix endpoint/protocol/connectivity |
| Startup/config errors | `ff validate --config config.yaml` | Output contains `config ok:` | Fix YAML, path, output, or SQL errors |
| Throughput unexpectedly low | `curl -s http://localhost:9090/admin/v1/status | jq '.pipelines[0].stage_seconds'` | `output` not dominating total | Network/collector bottleneck |

## Scenario 1: No logs arrive at destination

### Checks

```bash
# 1) Are inputs being read?
curl -s http://localhost:9090/admin/v1/status | jq '.pipelines[0].inputs'

# 2) Are output counters increasing?
curl -s http://localhost:9090/admin/v1/status | jq '.pipelines[0].outputs'
```

### Expected

- `inputs[*].lines_total` increases over time.
- `outputs[*].lines_total` increases over time.

### Common causes and fixes

1. Wrong file glob.
   - Fix `input.path` and validate the directory exists inside the container/pod.
2. Missing host mount.
   - Ensure `/var/log` is mounted read-only in Docker/Kubernetes.
3. Permission denied on log files.
   - Run with access to host log files and verify container security context.

### Verify fix

Run the two checks again and confirm both input and output counters increase.

## Scenario 2: Inputs increase, but outputs remain zero

### Checks

```bash
curl -s http://localhost:9090/admin/v1/status | jq '.pipelines[0].transform'
```

### Expected

- `lines_in` and `lines_out` are both greater than zero.
- `filter_drop_rate` is not near `1.0`.

### Common causes and fixes

1. SQL `WHERE` clause filters everything.
   - Temporarily remove `WHERE` to confirm data flow.
2. Case mismatch in filters.
   - Example: `level = 'error'` vs actual `ERROR`.
3. Wrong field names.
   - Verify selected columns exist in parsed records.

### Verify fix

`lines_out` should increase within a few seconds under load.

## Scenario 3: OTLP send failures (`connection refused`, timeouts, auth)

### Checks

```bash
# HTTP OTLP health check (4318)
curl -v http://otel-collector:4318/v1/logs

# Kubernetes DNS resolution check
POD=$(kubectl -n collectors get pods -l app=logfwd -o jsonpath='{.items[0].metadata.name}')
kubectl -n collectors exec "$POD" -- nslookup otel-collector
```

### Expected

- DNS resolves collector service name.
- Endpoint responds (even non-200 can prove reachability).
- Runtime logs stop repeating send errors.

### Common causes and fixes

1. Protocol/port mismatch.
   - gRPC uses `4317`, HTTP uses `4318`.
2. Wrong namespace-qualified service name.
   - Use full in-cluster DNS name when needed.
3. Network policy blocking egress.
   - Allow traffic from the `collectors` namespace to the collector service.

### Verify fix

`outputs[*].errors` stops increasing and `outputs[*].lines_total` resumes growing.

## Scenario 4: Startup fails with config validation errors

### Checks

```bash
ff validate --config config.yaml
```

### Expected

Validation succeeds and prints `config ok: <n> pipeline(s)`.

### Common causes and fixes

- Missing `input.path` for `file` input.
- Missing `endpoint` for `otlp`/`http`/`elasticsearch`/`loki` outputs.
- Mixing simple layout (`input`/`output`) with `pipelines` map in one file.
- YAML scalar mistakes in SQL.

Use block scalar syntax for SQL:

```yaml
transform: |
  SELECT level, message
  FROM logs
  WHERE level = 'ERROR'
```

### Verify fix

Validation passes, then `dry-run` succeeds:

```bash
ff dry-run --config config.yaml
```

Both commands are read-only checks, so they are safe to use before deploys and in CI.

## Scenario 5: Throughput drops or latency spikes

### Checks

```bash
curl -s http://localhost:9090/admin/v1/status | jq '.pipelines[0].stage_seconds'
```

### Expected

Stage times should be stable, with no sudden sustained growth in output time.

### Common causes and fixes

1. Output stage dominates.
   - Move collector closer, enable compression (`compression: zstd`), or scale collector.
2. Excessive transform complexity.
   - Simplify query or split into named pipelines.
3. Node resource pressure.
   - Increase CPU/memory requests for the `logfwd` DaemonSet.

### Verify fix

Observe reduced output stage time and stable forwarding counters.

## Recovery fallback (safe mode)

If you need immediate stabilization while investigating:

1. Remove complex transform filters temporarily.
2. Send to `stdout` in a non-production environment to confirm parse path.
3. Re-enable OTLP once counters and errors look healthy.

This narrows failure scope without changing input collection semantics.

## Helpful diagnostics commands

```bash
# Health and readiness
curl -s http://localhost:9090/live | jq .
curl -s http://localhost:9090/ready | jq .

# End-to-end pipeline stats
curl -s http://localhost:9090/admin/v1/status | jq .

# Flattened stats snapshot
curl -s http://localhost:9090/admin/v1/stats | jq .
```

## What's next

| Topic | Where to go |
|-------|-------------|
| Check pipeline metrics | [Monitoring & Diagnostics](/deployment/monitoring/) |
| Review your config | [YAML Reference](/configuration/reference/) |
| Understand how FastForward works | [Learn](/learn/) (interactive) |

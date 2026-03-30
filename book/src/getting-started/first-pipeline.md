# Your First Pipeline

This guide walks through setting up logfwd to tail application logs, filter
by severity, and forward to an OpenTelemetry collector.

## The config

```yaml
input:
  type: file
  path: /var/log/app/*.log
  format: json

transform: |
  SELECT * FROM logs WHERE level IN ('ERROR', 'WARN')

output:
  type: otlp
  endpoint: https://otel-collector:4318
  protocol: http
  compression: zstd

server:
  diagnostics: 0.0.0.0:9090
```

## Run it

```bash
logfwd --config pipeline.yaml
```

## Monitor it

```bash
# Health check
curl http://localhost:9090/health

# Live metrics
curl http://localhost:9090/metrics

# Pipeline details (JSON)
curl http://localhost:9090/api/pipelines | jq .
```

## Validate config without running

```bash
logfwd --config pipeline.yaml --validate
# config ok: 1 pipeline(s)

logfwd --config pipeline.yaml --dry-run
# logfwd v0.1.0
#   pipeline default: 1 input(s) -> SELECT * FROM logs WHERE ... -> 1 output(s)
#   ready: default
# dry run ok: 1 pipeline(s) constructed
```

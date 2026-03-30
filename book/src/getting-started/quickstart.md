# Quick Start

## Generate test data

```bash
logfwd --generate-json 10000 /tmp/test.jsonl
```

## Process with SQL and output to console

```yaml
# pipeline.yaml
input:
  type: file
  path: /tmp/test.jsonl
  format: json
transform: "SELECT * FROM logs WHERE level = 'ERROR'"
output:
  type: stdout
  format: console
```

```bash
logfwd --config pipeline.yaml
```

You'll see colored output:

```
10:30:00.003Z  ERROR  request handled GET /health/10021  duration_ms=40 service=myapp
10:30:00.007Z  ERROR  request handled GET /api/v2/products/10049  duration_ms=92 ...
```

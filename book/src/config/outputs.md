# Output Types

## OTLP

Send logs as OpenTelemetry protobuf over HTTP or gRPC.

```yaml
output:
  type: otlp
  endpoint: https://collector:4318
  protocol: http       # http | grpc
  compression: zstd    # zstd | gzip | none
  auth:
    bearer_token: "${OTEL_TOKEN}"
```

## HTTP (JSON Lines)

POST newline-delimited JSON to any HTTP endpoint.

```yaml
output:
  type: http
  endpoint: https://logging-service:9200
  auth:
    headers:
      X-API-Key: "${API_KEY}"
```

## Stdout

Print to stdout for debugging/testing.

```yaml
output:
  type: stdout
  format: console    # console | json | text
```

- **console**: Colored, human-readable (timestamp, level, message, key=value pairs)
- **json**: One JSON object per line (machine-parseable)
- **text**: Raw text (uses `_raw` column if available)

## Multiple outputs (fan-out)

```yaml
outputs:
  - name: collector
    type: otlp
    endpoint: https://collector:4318
  - name: debug
    type: stdout
    format: console
```

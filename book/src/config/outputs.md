# Output Types

Support status lives in the [Configuration Reference](reference.md#output-types).
This page focuses on usage notes and examples.

## OTLP

Send logs as OpenTelemetry protobuf over HTTP or gRPC.

```yaml
output:
  type: otlp
  endpoint: https://collector:4318
  protocol: http       # http | grpc
  compression: zstd    # zstd | none (gzip not yet supported)
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

## Elasticsearch

Ship logs to Elasticsearch via the Bulk API.

```yaml
output:
  type: elasticsearch
  endpoint: https://es-cluster:9200
  index: logs             # default: "logs"
  compression: gzip       # gzip | none (optional)
  request_mode: buffered  # buffered | streaming (optional, default buffered)
```

- **Bulk API**: Per-document error handling with automatic retries.
- **Batch splitting**: Large payloads are split automatically to stay within Elasticsearch limits.
- **Compression**: Optional gzip compression for reduced network usage.
- **Streaming mode**: Experimental chunked HTTP request bodies for benchmarking against hosted/serverless clusters.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `endpoint` | string | Yes | — | Elasticsearch URL |
| `index` | string | No | `logs` | Target index name |
| `compression` | string | No | none | `gzip` or `none` |
| `request_mode` | string | No | `buffered` | `buffered` or experimental `streaming` (streaming currently requires `compression: none`) |

## Loki

Push logs to Grafana Loki with automatic label grouping.

```yaml
output:
  type: loki
  endpoint: http://loki:3100
```

When `label_columns` or `static_labels` are used, logfwd sanitizes label keys
into Loki-compatible names. Configurations are rejected if two source columns,
two `static_labels` entries, or a source column and a `static_labels` entry
would collapse to the same sanitized Loki label key.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `endpoint` | string | Yes | Loki push URL |

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

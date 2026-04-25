# Elasticsearch Output Examples

This directory contains example configurations for using ffwd with Elasticsearch.

## Quick Start

### 1. Start Elasticsearch

```bash
cd examples/elasticsearch
docker-compose up -d
```

### 2. Verify Elasticsearch is running

```bash
curl http://localhost:9200/_cluster/health
```

## Features

- **Bulk API**: Efficient batch ingestion using Elasticsearch bulk API
- **ES|QL with Arrow IPC**: Query Elasticsearch and receive results in high-performance Arrow IPC format (see [ARROW_IPC.md](ARROW_IPC.md))
- **Retry logic**: Automatic retry with exponential backoff for transient failures
- **Authentication**: Support for bearer tokens and custom headers

## Basic Elasticsearch Output

Forward JSON logs to Elasticsearch with minimal configuration:

```yaml
input:
  type: file
  path: /var/log/app/*.log
  format: json

output:
  type: elasticsearch
  endpoint: http://elasticsearch:9200
  index: logs
```

## With Authentication

Use bearer token or API key authentication:

```yaml
input:
  type: file
  path: /var/log/app/*.log
  format: json

output:
  type: elasticsearch
  endpoint: https://elasticsearch.example.com:9200
  index: application-logs
  auth:
    bearer_token: ${ELASTICSEARCH_API_KEY}
    headers:
      X-Tenant: production
```

## With SQL Transform

Filter and enrich logs before sending to Elasticsearch:

```yaml
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri
  source_metadata: ecs

transform: |
  SELECT
    level,
    message,
    status,
    duration_ms,
    regexp_extract("file.path", '/([^/]+)/[^/]+\\.log$', 1) AS pod_name
  FROM logs
  WHERE level IN ('ERROR', 'WARN')
    AND status >= 400

output:
  type: elasticsearch
  endpoint: http://elasticsearch:9200
  index: kubernetes-errors
```

## Multi-Index Pattern

Route different log types to different indices:

```yaml
pipelines:
  errors:
    inputs:
      - type: file
        path: /var/log/app/*.log
        format: json
    transform: SELECT * FROM logs WHERE level = 'ERROR'
    outputs:
      - type: elasticsearch
        endpoint: http://elasticsearch:9200
        index: app-errors

  access_logs:
    inputs:
      - type: file
        path: /var/log/nginx/access.log
        format: json
    transform: SELECT * FROM logs WHERE status >= 200
    outputs:
      - type: elasticsearch
        endpoint: http://elasticsearch:9200
        index: nginx-access
```

## Performance Tuning

For high-volume logging (1M+ lines/sec), use these settings:

```yaml
input:
  type: file
  path: /var/log/high-volume/*.log
  format: json
  max_open_files: 2048  # Increase file descriptor limit

transform: |
  -- Minimal transform for best performance
  SELECT * FROM logs

output:
  type: elasticsearch
  endpoint: http://elasticsearch:9200
  index: high-volume-logs
  # Elasticsearch will batch records internally via bulk API
  # Each batch is 2 lines per record (action + document)
```

## Kubernetes DaemonSet

Deploy as a DaemonSet to collect logs from all pods:

```yaml
# config.yaml
input:
  type: file
  path: /var/log/pods/**/*.log
  format: cri
  source_metadata: ecs

transform: |
  SELECT
    level,
    message,
    "file.path" AS source_file
  FROM logs

output:
  type: elasticsearch
  endpoint: http://elasticsearch.elasticsearch.svc.cluster.local:9200
  index: kubernetes-logs
  auth:
    bearer_token: ${ELASTICSEARCH_TOKEN}
```

## Configuration Fields

### Required
- `endpoint`: Elasticsearch HTTP endpoint (e.g., `http://localhost:9200`)
- `type`: Must be `elasticsearch`

### Optional
- `index`: Target index name (default: `logs`)
- `auth`: Authentication configuration
  - `bearer_token`: Bearer token or API key
  - `headers`: Additional HTTP headers (e.g., `X-Tenant`, `X-API-Key`)

## Bulk API Behavior

The Elasticsearch sink uses the bulk API format:
```json
{"index":{"_index":"logs"}}
{"level":"ERROR","msg":"failed","status":500}
```

Each log record produces 2 lines: an action line and a document line. The bulk API endpoint automatically batches these for efficient ingestion.

## Performance Notes

- The sink reuses buffers to minimize allocations (hot path optimization)
- Bulk payloads are split before they exceed `5242880` bytes (5 MiB)
- HTTP requests include retry logic with exponential backoff (100ms → 200ms → 400ms)
- Transient errors (429, 5xx, network failures) are automatically retried up to 3 times
- Response parsing checks for per-document errors and fails the entire batch if any document fails
- For maximum throughput, keep transforms minimal and avoid complex SQL operations

## Error Handling

If Elasticsearch returns a bulk error response, the entire batch fails and can be retried. Example error:

```
ES bulk error: mapper_parsing_exception: failed to parse field [status]
```

Check your index mappings if you see field parsing errors. You may need to update the mapping or adjust your transform query to match the expected types.

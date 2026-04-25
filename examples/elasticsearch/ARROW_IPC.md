# ES|QL with Arrow IPC Format

This directory demonstrates querying Elasticsearch using ES|QL and receiving results in Apache Arrow IPC format for high-performance data transfer.

## What is ES|QL?

ES|QL (Elasticsearch Query Language) is a piped query language introduced in Elasticsearch 8.11+. It provides a powerful way to query, transform, and aggregate data with a syntax similar to SQL and Unix pipes.

Example ES|QL query:
```
FROM logs
| WHERE level == "ERROR"
| STATS count() BY service
| LIMIT 100
```

## Arrow IPC Format Support

ES|QL can return query results in Apache Arrow IPC format, which is significantly more efficient than JSON for large result sets:

- **Binary format**: No JSON parsing overhead
- **Zero-copy**: Direct memory access to columnar data
- **Compression**: Built-in compression support
- **Schema preservation**: Type information preserved across the wire

## Setup

### 1. Start Elasticsearch with Docker

```bash
cd examples/elasticsearch
docker-compose up -d
```

Wait for Elasticsearch to be healthy:
```bash
curl http://localhost:9200/_cluster/health
```

### 2. Index Some Test Data

Use the ffwd Elasticsearch sink to index data:

```yaml
# config.yaml
input:
  type: file
  path: /var/log/app/*.log
  format: json

output:
  type: elasticsearch
  endpoint: http://localhost:9200
  index: logs
```

Run ffwd:
```bash
cargo run -p ffwd -- run --config config.yaml
```

## Querying with Arrow IPC

### Using the ElasticsearchSinkFactory API

```rust
use std::sync::Arc;
use ffwd_output::ElasticsearchSinkFactory;
use ffwd_output::sink::SinkFactory;
use ffwd_types::diagnostics::ComponentStats;

let factory = ElasticsearchSinkFactory::new(
    "query".into(),
    "http://localhost:9200".into(),
    "logs".into(),
    vec![],
    false,
    Arc::new(ComponentStats::default()),
)?;
let sink = factory.create()?;

// Query with ES|QL uses the async sink's query_arrow method.
// See ElasticsearchSink::query_arrow for the Arrow IPC query API.
```

### ES|QL Query Examples

**Filter by field:**
```
FROM logs | WHERE status >= 500 | LIMIT 100
```

**Projection (select specific fields):**
```
FROM logs | KEEP timestamp, level, message | LIMIT 1000
```

**Aggregation:**
```
FROM logs
| STATS
    count() AS total,
    avg(duration_ms) AS avg_duration
  BY level
```

**Complex pipeline:**
```
FROM logs
| WHERE timestamp > "2024-01-01"
| EVAL is_error = status >= 400
| STATS error_count = count(is_error) BY service
| SORT error_count DESC
| LIMIT 10
```

## Running Tests

Integration tests require a running Elasticsearch instance:

```bash
# Start Elasticsearch
cd examples/elasticsearch && docker-compose up -d

# Run integration tests (note: they're marked as #[ignore] by default)
cargo test --package ffwd-output --test elasticsearch_arrow_ipc -- --ignored

# Run specific test
cargo test --package ffwd-output --test elasticsearch_arrow_ipc test_query_arrow_all_documents -- --ignored
```

## Running Benchmarks

Benchmarks measure query performance and compare Arrow IPC vs JSON format:

```bash
# Start Elasticsearch
cd examples/elasticsearch && docker-compose up -d

# Run benchmarks (note: this is a bench target, not a bin target)
# The benchmark will run as a standalone executable with `main()`
cargo bench --package ffwd-bench --bench elasticsearch_arrow --no-run
./target/release/deps/elasticsearch_arrow-*

# Or build and run in one step (release mode)
cargo build --release -p ffwd-bench --benches
./target/release/deps/elasticsearch_arrow-*

# Expected output:
# - Query latency (ms)
# - Throughput (rows/sec)
# - Arrow IPC vs JSON speedup comparison
```

Benchmark scenarios:
- **Full scan**: Query all documents with LIMIT
- **Filtered query**: WHERE clause filtering
- **Projection**: KEEP specific columns
- **Aggregation**: STATS grouping

## Performance Benefits

Arrow IPC format provides significant performance improvements over JSON:

| Metric | JSON | Arrow IPC | Improvement |
|--------|------|-----------|-------------|
| Parse overhead | High (JSON parsing) | Low (binary format) | ~3-5x faster |
| Memory copies | Multiple | Zero-copy | ~2x less memory |
| Network size | Large (text) | Compressed binary | ~2-4x smaller |
| Type safety | Runtime parsing | Schema preserved | Compile-time checks |

## API Reference

### `ElasticsearchSink::query_arrow(query: &str) -> io::Result<Vec<RecordBatch>>`

Query Elasticsearch using ES|QL and receive Arrow IPC response.

**Parameters:**
- `query`: ES|QL query string

**Returns:**
- `Vec<RecordBatch>`: Query results as Arrow RecordBatches

**Errors:**
- Returns `io::Error` if:
  - Elasticsearch is not reachable
  - Query syntax is invalid
  - ES|QL is not enabled (requires ES 8.11+)
  - Arrow IPC parsing fails

**Example:**
```rust
let batches = sink.query_arrow("FROM logs | LIMIT 100")?;
let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
println!("Retrieved {} rows", total_rows);
```

## Troubleshooting

### ES|QL not available

If you get errors like "unknown endpoint /_query", ensure you're using Elasticsearch 8.11 or later:

```bash
curl http://localhost:9200 | jq '.version.number'
```

### Arrow IPC parsing errors

If Arrow IPC parsing fails, verify the Accept header is correctly set:
- `Accept: application/vnd.apache.arrow.stream`

The implementation automatically sets this header in `query_arrow()`.

### Timeout errors

For large queries, increase the HTTP timeout:
- Default: 30 seconds
- Adjust in `ElasticsearchSink::new()` configuration

## References

- [ES|QL Documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/esql.html)
- [Arrow IPC Format Specification](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc)
- [Elasticsearch REST API](https://www.elastic.co/guide/en/elasticsearch/reference/current/rest-apis.html)

## Next Steps

1. **Production use**: Add authentication headers for secure Elasticsearch clusters
2. **Streaming**: Implement streaming Arrow IPC for large result sets
3. **Query builder**: Create a type-safe ES|QL query builder
4. **Pushdown**: Push DataFusion query plans to ES|QL for server-side processing

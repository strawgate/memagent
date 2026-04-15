# Arrow Ecosystem: Receivers and Exporters

Reference for systems that speak Arrow natively and can integrate with
logfwd as receivers (produce RecordBatch) or exporters (consume RecordBatch).

## Tier 1 — Production-ready, excellent Rust support

| System | Receive | Export | Protocol | Rust crate |
|--------|:-------:|:------:|----------|------------|
| Arrow IPC files (local/S3) | ✓ | ✓ | File read/write | `arrow-ipc` + `object_store` |
| Parquet files (local/S3) | ✓ | ✓ | File read/write | `parquet` + `object_store` |
| ClickHouse | ✓ | ✓ | HTTP `FORMAT ArrowStream` | `reqwest` + `arrow-ipc` |
| Arrow Flight | ✓ | ✓ | gRPC streaming | `arrow-flight` (tonic) |
| InfluxDB 3 | ✓ | — | Arrow Flight SQL | `arrow-flight` |
| DuckDB (in-process) | ✓ | — | C API | `duckdb` with `arrow` feature |
| CSV/JSON/Avro files | ✓ | ✓ | File read/write | `arrow-csv`, `arrow-json`, `arrow-avro` |

## Tier 2 — Works with some glue

| System | Receive | Export | Protocol | Notes |
|--------|:-------:|:------:|----------|-------|
| Elasticsearch ES\|QL | ✓ | — | HTTP `?format=arrow` | Experimental (ES 8.15+). We can receive (query) Arrow from ES, but ES cannot ingest Arrow — writing to ES requires NDJSON bulk. |
| GreptimeDB | ✓ | ✓ | Arrow Flight DoPut + OTel-Arrow | Production for metrics signal. |
| Kafka | ✓ | ✓ | Arrow IPC as message bytes | DIY via `rdkafka` + `arrow-ipc`. No standard schema registry support. |
| PostgreSQL | ✓ | ✓ | Flight SQL adapter (alpha) or ADBC (FFI) | Flight SQL adapter is 0.1.0. ADBC requires FFI to C driver. |

## Tier 3 — Significant effort

| System | Notes |
|--------|-------|
| BigQuery | Storage API returns Arrow over gRPC. No Rust SDK; need proto codegen. |
| Snowflake | ADBC driver (Go-based, C API). Rust FFI overhead. |
| Databricks | Flight SQL for queries. Write via Parquet on cloud storage. |

## Key implementation notes

### ClickHouse ArrowStream

```http
# Export (receive Arrow from ClickHouse):
POST http://host:8123/?query=SELECT+*+FROM+table+FORMAT+ArrowStream
→ response body is Arrow IPC stream bytes

# Import (export Arrow to ClickHouse):
POST http://host:8123/?query=INSERT+INTO+table+FORMAT+ArrowStream
← request body is Arrow IPC stream bytes
```

Compression: `output_format_arrow_compression_method` (default lz4_frame).
ArrowStream stable since ~2020.

### Elasticsearch ES|QL

```http
POST /_query?format=arrow
{ "query": "FROM logs | WHERE level == 'ERROR' | LIMIT 100" }
→ response body is Arrow IPC stream bytes
```

Marked experimental. Response is raw IPC stream; deserialize with
`arrow_ipc::reader::StreamReader`.

### Arrow IPC file conventions

- `.arrows` = Arrow IPC stream (sequential read)
- `.arrow` = Arrow IPC file (random access via footer)
- DuckDB auto-detects both: `SELECT * FROM 'logs.arrows'`
- Polars: `pl.scan_ipc("logs.arrows")` with predicate/projection pushdown
- ZSTD compression: DuckDB reads it; Polars has edge-case issues (prefer uncompressed for Polars compat)

### Arrow Flight vs Arrow IPC over HTTP

Wire format is identical (Arrow IPC flatbuffers). Flight wraps IPC in
gRPC framing (protobuf envelope, ~20 bytes overhead per message).

Flight adds: multiplexing (HTTP/2), per-stream backpressure, service
discovery (`GetFlightInfo`), schema negotiation (`GetSchema`), auth.

Arrow IPC over HTTP is simpler: POST IPC bytes, read IPC bytes. No
new dependencies. Sufficient for point-to-point pipeline transport.

**Recommendation:** Start with IPC over HTTP. Add Flight when we need
multiplexed streaming or external queryability. Migration is smooth —
just wrap existing IPC encoding in `FlightDataEncoderBuilder`.

### Dependency cost of Arrow Flight

Adding `arrow-flight` brings: tonic, prost, h2, tower, tower-http (~30-40 new crates).
~2-4 seconds added to clean compile. Only add when actually needed.

## OTel Arrow (OTAP) wire protocol

The OTel Arrow protocol uses a normalized multi-table schema with
foreign keys between attribute tables and signal tables. It sends
Arrow IPC inside bidirectional gRPC streams with dictionary delta
encoding across batches for 50-70% bandwidth savings over OTLP protobuf.

An OTAP receiver would accept this wire format and convert to our flat
schema. An OTAP exporter would convert our flat RecordBatches to the
normalized multi-table form. Both are optional — the core pipeline
doesn't assume OTel semantics.

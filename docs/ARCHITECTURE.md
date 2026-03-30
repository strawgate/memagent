# Architecture

## Data flow

```
Sources (file / TCP / UDP / OTLP receiver)
    │
    │  each source produces Bytes independently
    ▼
Format Parser (CRI / JSON / Raw)  ─per source─
    │  strips CRI timestamp/stream prefix, accumulates NDJSON
    ▼
SIMD Scanner  ─per source─
    │  one pass classifies entire buffer via ChunkIndex
    │  walks structural positions directly into Arrow columns
    │  injects _resource_* columns from source metadata
    ▼
RecordBatch per source (partitions)
    │
    ├─→ registered as partitions of `logs` MemTable
    │   (DataFusion concatenates partitions during query)
    ▼
SQL Transform (DataFusion)
    │  user SQL: SELECT, WHERE, GROUP BY + UDFs
    │  enrichment tables available via JOIN
    │  _resource_* columns flow through like any other column
    ▼
Post-transform RecordBatch
    │
    ▼
Disk Queue (Arrow IPC Stream segments, one queue per pipeline)
    │  all outputs share one queue with independent cursors
    ▼
Output Sinks (OTLP / JSON lines / HTTP / stdout)
    │  group rows by _resource_* columns for OTLP ResourceLogs
    │  independent cursor per output, at-least-once delivery
```

## Multi-source pipeline

Each source scans independently and contributes its RecordBatch as a
separate partition of the `logs` MemTable. This means:

- Each source can have a different `ScanConfig` (different wanted_fields
  from predicate pushdown)
- Schema differences between sources are handled by DataFusion's schema
  merging (missing columns are null)
- The StreamingBuilder's `Bytes` lifetime is per-source
- A source with no data in a cycle contributes no partition

The SQL transform sees all rows from all sources in one `logs` table.
The result is a single RecordBatch that flows to the disk queue and all
outputs.

## Resource metadata as columns

Source identity and resource attributes are carried as `_resource_*`
prefixed columns (e.g., `_resource_k8s_pod_name`,
`_resource_k8s_namespace`, `_resource_service_name`). These are injected
during scanning based on the source's configuration.

This design:

- Survives SQL transforms naturally (they're just columns)
- Persists in the disk queue (columns in Arrow IPC segments)
- Enables OTLP output to group rows by resource (group-by on
  `_resource_*` columns, one `ResourceLogs` per distinct combination)
- Uses dictionary encoding for efficiency (same pod name on every row
  from one source costs ~one entry)
- Output sinks exclude `_resource_*` columns from the payload (same
  pattern as `_raw`)

## Column naming conventions

| Prefix | Purpose | Example |
|--------|---------|---------|
| `{field}_str` | String value from JSON | `message_str` |
| `{field}_int` | Integer value from JSON | `status_int` |
| `{field}_float` | Float value from JSON | `latency_float` |
| `_raw` | Raw input line (optional) | `_raw` |
| `_resource_*` | Source/resource metadata | `_resource_k8s_pod_name` |

Type conflicts produce separate columns: `status_int` and `status_str`
can coexist.

## Scanner architecture

The scanner is the performance-critical path. It has two stages:

**Stage 1 — Chunk classification** (`chunk_classify.rs`): Process the
entire NDJSON buffer in 64-byte blocks. For each block, find all quote
and backslash positions, compute an escape-aware real-quote bitmask, and
build a string-interior mask. Output: `ChunkIndex` with pre-computed
bitmasks.

**Stage 2 — Field extraction** (`scanner.rs`): A scalar state machine
walks top-level JSON objects. For each field, it resolves the key to an
index (HashMap, once per field per batch) and routes the value to the
builder via `append_*_by_idx`. String scanning uses the pre-computed
`ChunkIndex` for O(1) closing-quote lookup.

The scan loop is generic over the `ScanBuilder` trait:

- **`StorageBuilder`**: collects `(row, value)` records. Builds columns
  independently at `finish_batch`. Correct by construction. For
  persistence.
- **`StreamingBuilder`**: stores `(row, offset, len)` views into a
  `bytes::Bytes` buffer. Builds `StringViewArray` columns with zero
  copies. 20% faster. For real-time hot path. Default in pipeline.

## Disk queue

Post-transform Arrow IPC Stream segments. One queue per pipeline, shared
by all outputs with independent read cursors.

**Format:** Arrow IPC Stream (not File). Stream format is crash-safe
(`StreamReader` recovers all complete batches from a truncated segment),
append-friendly, and supports dictionary replacement across batches.

**Segments:** Each segment is a complete IPC stream (schema + N batches +
EOS). Sealed by size threshold or time threshold, then fsync'd. New
segment started after sealing. Readers only open sealed segments.

**Retention:** TTL (`max_segment_age`) + size (`max_disk_bytes`). When
either limit is hit, oldest segments are evicted regardless of cursor
positions. Cursors pointing at deleted segments advance to the oldest
surviving segment. Gaps are tracked in metrics (`segments_dropped`).

**Delivery:** At-least-once. Crash-before-ack means re-delivery on
restart. Monotonic `batch_id` in segment metadata enables optional
downstream deduplication.

**New output cursors:** Default to tail (skip history). Configurable
`replay_from: oldest` per output for archival use cases.

## Async pipeline

The pipeline runs on a tokio multi-thread runtime. Key components:

- **Sources** implement `async fn run(&mut self, ctx: &mut SourceContext)`
  (Arroyo-style source-owns-loop). File sources wrap FileTailer via
  `spawn_blocking`.
- **Scanner** runs on `spawn_blocking` (pure CPU, ~4MB per call).
- **Transform** is `async fn execute()` (DataFusion is natively async).
- **Sinks** implement `async fn send_batch()`. HTTP-based sinks use
  async `reqwest` for connection pooling and timeouts.
- **Shutdown** via `CancellationToken` (already implemented).

## Crate map

| Crate | Purpose |
|-------|---------|
| `logfwd` | Binary. CLI, pipeline orchestration, OTel metrics. |
| `logfwd-core` | Scanner, builders, parsers, diagnostics, enrichment, OTLP encoder. |
| `logfwd-config` | YAML config deserialization and validation. |
| `logfwd-transform` | DataFusion SQL. UDFs: `grok()`, `regexp_extract()`, `int()`, `float()`. |
| `logfwd-output` | Output sinks + async Sink trait. OTLP, JSON lines, HTTP, stdout. |
| `logfwd-bench` | Criterion benchmarks. |

## What's implemented vs not yet

**Implemented:** file input, CRI/JSON/Raw parsing, SIMD scanner, two
builder backends (StreamingBuilder default), DataFusion SQL transforms
(async), custom UDFs (grok, regexp_extract, int, float), enrichment
(K8s path, host info, static labels), OTLP output, JSON lines output,
stdout output, diagnostics server, OTel metrics, signal handling
(SIGINT/SIGTERM via CancellationToken), graceful shutdown, async Sink
trait.

**Not yet:** async pipeline runtime, async Source trait, disk queue,
`_resource_*` column injection, OTLP resource grouping, TCP/UDP/OTLP
input, Elasticsearch/Loki/Parquet output, file offset checkpointing,
SQL rewriter.

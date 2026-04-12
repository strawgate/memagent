---
title: "Pipeline Design"
description: "How data flows from input through transform to output"
---

## Data flow

```
Sources (file / TCP / UDP / OTLP receiver)
    │
    │  each source produces Bytes independently
    ▼
Format Parser (CRI / JSON / Raw)  ─per source─
    │  strips CRI timestamp/stream prefix, accumulates NDJSON
    ▼
Scanner  ─per source─
    │  one pass classifies entire buffer via ChunkIndex
    │  walks structural positions directly into Arrow columns
    │  injects _resource_* columns from source metadata
    ▼
RecordBatch per source
    │
    ├─→ [if queue.mode: pre-transform] write Arrow IPC segment → ack source
    │
    ├─→ register as partitions of `logs` MemTable
    │   (DataFusion concatenates partitions during query)
    ▼
SQL Transform (DataFusion)
    │  user SQL: SELECT, WHERE, GROUP BY + UDFs
    │  enrichment tables available via JOIN
    │  _resource_* columns flow through like any other column
    ▼
Post-transform RecordBatch
    │
    ├─→ [if queue.mode: post-transform] write Arrow IPC segment → ack source
    │
    ▼
Output Sinks (OTLP / JSON lines / HTTP / stdout)
    │  in-memory fan-out via bounded channels
    │  group rows by _resource_* columns for OTLP ResourceLogs
```

## Persistence

Arrow IPC is the universal segment format. Every persistence point
writes Arrow IPC File format with atomic seal, and every persistence
point can target local disk or object storage (S3/GCS/Azure Blob).

**Queue mode is configurable per pipeline:**

```yaml
pipeline:
  queue:
    mode: pre-transform   # or post-transform, or none
    storage: s3://my-bucket/logfwd/pipeline-a/
    max_bytes: 10GB
    max_age: 24h
```

**`pre-transform`:** Source segments are the queue. Each source writes
its own Arrow IPC segments after scanning. Outputs replay from source
segments and re-run the transform. Raw data is re-queryable with
different SQL. One copy of data. Enrichment data may be stale on replay
(`_resource_*` columns are always correct; enrichment table data
reflects query-time state, not ingest-time).

**`post-transform`:** Shared post-transform queue per pipeline.
Transform runs once, result is persisted. All outputs share one queue
with independent cursors. Enrichment baked in at ingest time — correct
even on replay days later. Can't re-query with different SQL. Second
copy of data (but usually smaller due to filtering).

**`none` (default):** Pure in-memory fan-out. Transform runs once,
RecordBatches passed to outputs via bounded channels. Batches drop on
output failure. Lowest resource usage.

## End-to-end ack

Sources must NOT acknowledge to their upstream until persistence
confirms durability. The ack chain:

```
Scanner produces RecordBatch
  → FileWriter writes to .tmp
  → fsync + rename (segment sealed)
  → Ack source: "data through offset X is durable"
  → Source advances checkpoint
```

Ack latency is scanner + IPC write + fsync, typically 5-15ms. Comparable
to Kafka's acks=all (3-15ms).

**File sources** don't need the ack latency to be fast — the log file is
already durable. The checkpoint just tracks the file offset.

**Network sources** see the full ack latency as their response time.

**When `queue.mode: none`:** No persistence, no ack-after-write. Source
checkpoints advance after in-memory fan-out. Data can be lost on crash.

## Output delivery

**Real-time path (all modes):** Transform runs once per batch cycle,
result fans out to all outputs via bounded channels.

**Replay (when queue is enabled):** If an output falls behind or
restarts, it replays from the queue. With `pre-transform` mode, replay
re-runs the transform. With `post-transform` mode, replay reads
transformed segments directly.

**Cursor tracking:** Each output tracks `{ segment_id, batch_offset }`.
Persisted on each ack. On restart, resume from last position.

**Retention:** TTL (`max_segment_age`) + size (`max_disk_bytes`). When
either limit is hit, oldest segments are evicted regardless of cursor
positions. Outputs pointing at evicted segments see a gap (tracked in
`segments_dropped` metric).

**Delivery semantics:** At-least-once when queue is enabled.
Best-effort when queue is `none`.

## Multi-source pipeline

Each source scans independently and writes its own Arrow IPC segments.
At transform time, the pipeline reads the latest segments from each
source and registers them as partitions of the `logs` MemTable.
DataFusion concatenates partitions during query execution.

This means:

- Each source can have a different `ScanConfig` (different wanted_fields
  from predicate pushdown)
- Schema differences between sources are handled by DataFusion's schema
  merging (missing columns are null)
- The StreamingBuilder's `Bytes` lifetime is per-source
- A source with no data in a cycle contributes no partition

## Resource metadata as columns

Source identity and resource attributes are carried as `_resource_*`
prefixed columns (e.g., `_resource_k8s_pod_name`,
`_resource_k8s_namespace`, `_resource_service_name`). These are injected
during scanning based on the source's configuration.

This design:

- Survives SQL transforms naturally (they're just columns)
- Persists in the Arrow IPC segments (always correct, unlike enrichment)
- Enables OTLP output to group rows by resource (group-by on
  `_resource_*` columns, one `ResourceLogs` per distinct combination)
- Uses dictionary encoding for efficiency (same pod name on every row
  from one source costs ~one entry)
- Output sinks exclude `_resource_*` columns from the payload (same
  pattern as `body`)

## Column naming conventions

| Column | Purpose | Example |
|--------|---------|---------|
| `{field}` | Bare name, native Arrow type | `body` (Utf8View), `status` (Int64) |
| `{field}` (conflict) | StructArray with typed children | `status: Struct { int: Int64, str: Utf8View }` |
| `body` | Original input line (optional) | `body` |
| `_timestamp` | CRI timestamp (RFC 3339 string) | `_timestamp` |
| `_stream` | CRI stream name | `_stream` |
| `_resource_*` | Source/resource metadata | `_resource_k8s_pod_name` |

Type conflicts produce a single `StructArray` column with typed children,
not separate flat columns.

## Arrow IPC segment format

**Format:** Arrow IPC File format with atomic seal.

**Write path:** `FileWriter` writes to `.tmp` file. On seal:
`finish()` (writes footer) → `fsync` → atomic `rename` to final path.
Readers never see incomplete files.

**Segment lifecycle:**
1. Pipeline writes batch(es) to `.tmp` file via `FileWriter`
2. At size threshold (64 MB) or time threshold: seal segment
3. On startup: delete orphaned `.tmp` files

**Storage abstraction:** All persistence writes go through a
`SegmentStore` trait with implementations for local filesystem and
object storage (S3/GCS/Azure Blob). The same segment format and code
path is used regardless of storage backend.

## Deployment model

logfwd scales by running more instances. One binary, one pipeline per
instance. S3 is the coordination layer between instances.

**Single instance:** Source → Scanner → Transform → Output, all in one
process. Predicate pushdown works. In-memory fan-out. Simple.

**Scaled out:** Multiple instances with different configs, sharing S3
paths. Each instance is a full logfwd binary.

```
Instance A (edge collector):
  file source → scanner → write Arrow IPC to s3://bucket/ingest/

Instance B (central processor):
  s3 source (reads s3://bucket/ingest/) → transform → output sinks
  # or: → write to s3://bucket/transformed/ for another instance

Instance C (dedicated sender):
  s3 source (reads s3://bucket/transformed/) → output sinks
```

No custom RPC, no cluster coordination. Arrow IPC on S3 is the
universal interface. Any instance can read any other's segments.

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

- **`StreamingBuilder`** (via `Scanner::scan_detached`): builds detached
  `StringArray` columns. For the persistence path (Arrow IPC segments).
- **`StreamingBuilder`** (via `Scanner::scan`): stores `(row, offset, len)` views into a
  `bytes::Bytes` buffer. Builds `StringViewArray` columns with zero
  copies. 20% faster. For real-time hot path when persistence is
  disabled.

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

## Current Support Status

For current input/output support status, see the canonical tables in the
[Configuration Reference](/memagent/configuration/reference/#input-types) and
[Configuration Reference](/memagent/configuration/reference/#output-types). This
architecture guide focuses on data flow and system shape rather than
duplicating per-surface availability claims.

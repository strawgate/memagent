# Arrow Pipeline Engineering Patterns

Patterns observed in production Arrow-native pipeline systems that
inform logfwd's architecture. These are ideas, not endorsements.

## Dual data representation

Pipeline data can flow as either raw bytes OR Arrow RecordBatches.
Conversion is lazy — a receiver that accepts protobuf or JSON hands off
the raw bytes, and conversion to Arrow only happens when a processor
needs columnar access.

```rust
enum PipelineData {
    RawBytes(Bytes),           // cheap to clone, no deserialization yet
    Arrow(RecordBatch),        // columnar, ready for SQL
}
```

This avoids wasting CPU on serialization when a pipeline stage just
forwards data without inspection (e.g., a routing processor that only
checks metadata, not column contents).

## Thread-per-core with !Send local execution

The hot path uses `tokio::task::LocalSet` with `RefCell`-based channels.
No atomics, no `Arc`, no `Mutex` on the data path. Each CPU core runs
an independent pipeline instance.

- **Local channels** (`!Send`): `RefCell`-based state, waker-based
  async. Zero synchronization cost.
- **Shared channels** (`Send + Sync`): `Arc`-based, for cross-thread
  coordination only (config reload, shutdown).

This matters because Arrow RecordBatches contain `Arc<dyn Array>` —
passing them across threads forces atomic refcount operations. Keeping
the hot path single-threaded avoids this.

## Ack/Nack context stack

Every message carries a stack of frames. Each pipeline node pushes a
frame when data passes through it. When an exporter succeeds or fails,
it calls ack or nack, and the stack unwinds back through the pipeline,
notifying each interested node.

```rust
struct Frame {
    interests: Interests,   // bitflags: ACKS | NACKS
    route: RouteData,       // callback data, timestamps, output port
}
```

This enables:
- End-to-end delivery confirmation (source doesn't ack upstream until
  exporter confirms)
- Per-node error tracking (which stage failed?)
- Fan-out delivery tracking (ack when ALL outputs succeed, nack when
  ANY fails)

## Adaptive dictionary encoding

Dictionary builders start with the smallest possible key type and
upgrade on overflow:

1. Start with no builder (all-null optimization)
2. First non-null value → create dictionary with UInt8 keys (≤255 unique)
3. Overflow → upgrade to UInt16 keys (≤65535 unique)
4. Overflow → fall back to native (non-dictionary) encoding

Per-column strategy: fields like `severity_text` (very low cardinality)
always get dictionary encoding. Fields like timestamps never do. The
schema declares the strategy per column.

For wire protocols that maintain dictionary state across batches, only
deltas are sent — new entries since the last batch. Combined with zstd
compression, this yields 15-30x compression of the uncompressed size.

## Normalized vs flat schema

Two valid approaches for observability data:

**Normalized (multi-table):** Main signal table + separate attribute
tables linked by `parent_id` foreign keys. Resource attributes, scope
attributes, and record attributes each get their own table. Keys repeat
heavily, so dictionary encoding on the key column compresses extremely
well.

- Pro: Wire-efficient, semantically faithful to OTel data model
- Pro: Attribute filtering propagates through foreign key chains
- Con: ~20 RecordBatch types per signal. Complex to implement and query.

**Flat (single-table):** All fields as columns in one RecordBatch.
Resource metadata as `_resource_*` prefixed columns. Type conflicts
produce separate columns (`status_int`, `status_str`).

- Pro: Directly queryable by SQL engines (DuckDB, DataFusion, Polars)
- Pro: Simple to implement, simple to reason about
- Con: Less wire-efficient for high-cardinality attribute sets

**Our choice:** Flat schema as the core representation. Normalized form
only at the edges — an OTel receiver converts normalized → flat on
ingest, an OTel exporter converts flat → normalized on output.

## Backpressure at every layer

Backpressure must flow end-to-end, not just at channel boundaries:

1. **Channel level:** Bounded async channels. `send()` awaits when full.
2. **Processor level:** `accept_data()` gate lets processors pause
   admission (e.g., during compaction or flush).
3. **Receiver level:** Semaphore gates request admission. When
   downstream is saturated, receivers return "unavailable" / HTTP 503.
4. **Durable buffer level:** Disk budget with soft/hard caps. Soft cap
   triggers backpressure. Hard cap drops oldest segments.

The key insight: gRPC/HTTP receiver concurrency should be tied to
downstream channel capacity. Don't accept requests you can't process.

## Durable buffering with Arrow IPC segments

WAL (Write-Ahead Log) + immutable Arrow IPC segment files:

- WAL for crash recovery of in-flight data
- Segments are sealed (fsync + rename) and immutable once written
- Multiple independent subscribers with at-least-once delivery
- Each subscriber tracks progress independently
- Disk budget management: soft cap triggers backpressure, hard cap
  evicts oldest segments
- Optional mmap reads for segment replay

## Component registration via linker sections

Use `linkme::distributed_slice` for zero-cost plugin registration:

```rust
#[distributed_slice(RECEIVER_FACTORIES)]
static FILE_RECEIVER: ReceiverFactory = ReceiverFactory {
    name: "file",
    create: |config| { ... },
};
```

Components self-register at link time. No runtime HashMap, no manual
factory function wiring. New components are discovered automatically.

## Zero-copy view traits

GAT-heavy trait hierarchy for accessing data without copying. A
processor that only reads `timestamp` and `level` columns can work on
both the raw bytes representation and the Arrow representation without
materializing the full RecordBatch.

This is an optimization for later — start with full materialization,
add views when profiling shows it matters.

## Schema conventions for observability

Based on the OTel Arrow specification and broader Arrow ecosystem:

| Field | Arrow type | Notes |
|-------|-----------|-------|
| Timestamps | `Timestamp(Nanosecond, Some("UTC"))` | Not strings, not i64. Enables query engine optimizations. |
| Trace ID | `FixedSizeBinary(16)` | Not strings. Dictionary-encoded (many logs share a trace). |
| Span ID | `FixedSizeBinary(8)` | Not strings. Dictionary-encoded. |
| Severity number | `Int32` | OTel enum 1-24. |
| Severity text | `Utf8` (dictionary) | "INFO", "ERROR", etc. Very low cardinality. |
| Log body | `Utf8` or typed union | Single string column for flat schema. |
| Resource attrs | `_resource_*` columns | Flat schema. Dictionary-encoded per column. |

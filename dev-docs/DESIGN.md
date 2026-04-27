# Design

ffwd is an Arrow-native data processing pipeline. The core moves `RecordBatch` between
receivers, processors, and exporters. OTel semantics are optional adapters at the edges.

**RecordBatch in, RecordBatch out.** The pipeline doesn't care whether the data is logs,
metrics, traces, or CSV files.

## Target architecture

```
ffwd-core         Proven pure logic. #![no_std], #![forbid(unsafe_code)]
                    Parsing, encoding, pipeline state machine.
                    Streaming structural classification, framer, CRI, scanner.
                    Dependencies: memchr, wide (portable SIMD).
                    Every public function has a Kani proof or proptest.

ffwd-arrow        Arrow integration. Implements core's ScanBuilder trait.
                    StreamingBuilder, Scanner.
                    Bridge between parsed fields and RecordBatch.

ffwd-io           Produces RecordBatch from external sources.
                    File tailer, OTLP/TCP/UDP receivers, Arrow IPC reader.
                    Raw data goes through core→arrow→RecordBatch.
                    Arrow-native sources skip straight to RecordBatch.

ffwd-transform    RecordBatch → RecordBatch via DataFusion SQL.
                    UDFs, enrichment tables, JOINs.
                    CSV enrichment tables produce Utf8View columns via the
                    shared columnar builder.

ffwd-output       Consumes RecordBatch, sends externally.
                    OTLP, Arrow IPC, JSON lines. Parquet/ClickHouse are planned.

ffwd-runtime      Async runtime shell. Pipeline orchestration,
                    worker pool, processor chain, diagnostics wiring.

ffwd              Binary shell. CLI, config loading, signal handling,
                    startup/shutdown bootstrap, compatibility re-exports.
```

## Ecosystem interop

| System | Receive | Export |
|--------|:-------:|:------:|
| OTLP protobuf | ✓ | ✓ |
| Arrow IPC files | ✓ | ✓ |
| Parquet files | — | planned |
| ClickHouse (HTTP ArrowStream) | — | planned |
| Arrow Flight (gRPC) | planned | planned |
| OTel Arrow (OTAP) | planned | planned |

OTAP: stay independent of the official otap-dataflow crates (unpublished, heavy deps,
unstable protocol). When we implement OTAP, use the proto file directly and implement
star-to-flat / flat-to-star conversion at the boundary.

## Performance target

1M+ events/sec end-to-end. Key bottleneck is output transport throughput — the async pipeline overlaps IO
with CPU. SIMD structural scanning runs at 3.0 GiB/s (9 structural characters, NEON,
~760KB NDJSON, 2026-03-30 benchmark).

## What we're not building

- A general-purpose OTel Collector
- A database or query engine (we use DataFusion)
- A distributed system (scale by running more instances)

---

## Architecture decisions

### no_std structural enforcement for ffwd-core

`#![no_std]` with `extern crate alloc`. The compiler blocks IO — not lints that can be
`#[allow]`'d. This is the rustls/s2n-quic pattern. Four independent research studies
unanimously recommended this over std + clippy.

**Cost:** `core::`/`alloc::` import paths, hashbrown for HashMap if needed, memchr with
`default-features = false`.  
**Benefit:** Impossible to accidentally add IO to the proven core.

### ScanBuilder trait boundary (zero-allocation parsing)

Generic trait (serde Visitor pattern), not `Vec<ParsedField>`. Data flows directly from
parser into builder via monomorphization — no intermediate allocation. This is how serde
achieves zero-cost serialization.

**Alternative considered:** `SmallVec<[ParsedField; 16]>` return type. Vec is often
faster than SmallVec in benchmarks, and the callback pattern eliminates allocation entirely.

### Two-stage architecture: SIMD detects, scalar consumes

Stage 1 (SIMD character detection) and Stage 2 (parsing/field extraction) are separate.
SIMD produces bitmasks; a sequential scanner consumes them.

SIMD is embarrassingly parallel within a 64-byte block. Parsing is inherently sequential —
the meaning of a comma depends on whether we're inside a string. These are fundamentally
different parallelism profiles.

Stage 1 runs one SIMD pass over the entire buffer (via `wide` crate), detecting structural
characters and producing `u64` bitmasks. Cross-block state: only 2 `u64` values (escape
carry, string interior carry). Stage 2 walks structural positions using `trailing_zeros` +
clear-lowest-bit. This is the proven simdjson architecture.

### Scalar SIMD fallback in ffwd-core

`#![forbid(unsafe_code)]` is in core. Portable SIMD is provided by the `wide` crate,
which exposes a safe API for vectorized operations. Core has a safe scalar
`find_structural_chars_scalar` that is the Kani-provable specification.
`find_structural_chars` uses `wide` to provide portable SIMD across NEON,
AVX2, and SSE2 within the safe-code boundaries of core. proptest verifies
SIMD matches scalar for random inputs.

### Pipeline state machine over linear BatchToken

Prove pipeline correctness via a pure state machine in core (Kani single-step + TLA+
liveness), not a linear type.

Strictly linear `BatchToken` is impossible with async cancellation: Tokio can cancel a
future at any `.await` point, so a `#[must_use]` token can be dropped without being
consumed. The type system cannot prevent this.

The state machine in core is proven correct: no state exists where a batch is "forgotten."
The async shell handles best-effort delivery (at-least-once) using ordered ACK via BatchId.

**Related:** #270, #272.

### Opaque checkpoints (`C` type parameter)

`BatchTicket<S, C>` and `PipelineMachine<S, C>` are generic over checkpoint type `C`. The
pipeline stores and forwards checkpoints without interpreting them.

"Offset" means different things per input: file tail is `u64` byte position, Kafka is `i64`
partition offset (gaps from compaction), Journald is an opaque cursor string, push sources
(OTLP, syslog) have no checkpoint at all. A `u64` with contiguity enforcement would only
work for file inputs.

**Cost:** `C: Clone` bound on methods; generic propagates to `AckReceipt<C>`,
`CheckpointAdvance<C>`.  
**Research:** `dev-docs/research/checkpoint-snapshot-design.md`. **Related:** #270.

### Kani checkpoint proof domain

The `pipeline/lifecycle.rs` Kani harnesses may narrow symbolic checkpoint values
to a small integer domain when the proof only checks lifecycle behavior. This is
valid because `PipelineMachine<S, C>` stores, orders, and returns checkpoints
without interpreting their arithmetic meaning. TLA+ coverage is unchanged: the
`PipelineMachine.tla` model already treats checkpoint values as opaque tokens
while checking batch sequencing, ACK/reject advance, drain, and ordering
invariants.

### Rejected batches advance the checkpoint

`RejectBatch` has the same state transition as `AckBatch` — permanently-undeliverable data
must not block checkpoint progress indefinitely, which would stall drain forever. At-least-
once delivery is weakened to at-most-once only for rejected batches.

This matches Filebeat's behavior (advance past malformed records) and differs from Fluent
Bit (drops the route, retries via a backlog). If a batch is rejected, the data in that
batch is lost — the correct behavior for a forwarder where corrupted or oversized data
cannot be retried, but it must be metered.

### Per-source independent checkpoints

Each source has its own `committed[s]`. A slow source doesn't block a fast source from
committing. Equivalent to per-partition independent offsets in Kafka. Differs from Flink's
global checkpoint barrier, which is required for stateful stream processing but adds
blocking a stateless forwarder should not need.

### `fail()` is invisible to the pipeline machine

A `fail()`'d batch returns its ticket to Queued state, but the machine still tracks it as
`in_flight` — it was already `begin_send`'d. `fail()` changes no machine state. The
BTreeMap entry in `in_flight[source]` is not removed until `apply_ack` is called.

This models the Rust code exactly: `fail()` returns `BatchTicket<Queued, C>` but does not
touch the BTreeMap.

Current implication at the worker/checkpoint seam: a non-advancing failure
cannot be resolved by calling `fail()` alone unless the runtime still holds the
batch payload and can resubmit it. Until that richer retry path exists, held
worker outcomes remain unresolved and are replayed after restart if shutdown
force-stops with tickets still in flight.

### Suffix only on type conflict

Bare column names by default. Suffixed columns (`_int`, `_str`, `_float`) only when a
field has multiple types within a batch.

**Core requirement — round-trip type fidelity.** A field that enters as `int 200` must
exit as `int 200`. No type promotion across documents.  
**Core requirement — clean schemas stay clean.** OTLP, Arrow IPC, CSV, and consistent JSON
produce bare column names with native types. CSV enrichment columns are bare nullable
`Utf8View` string columns. The suffix machinery only activates when there is an actual
per-row type conflict.

Output always serializes with the bare JSON key, dispatching on DataType. A view layer
provides bare-name access for suffixed columns.

**Research:** `dev-docs/research/type-suffix-redesign.md`. **Related:** #445.

### Flat schema (not OTAP star schema)

Single `RecordBatch` with all fields as columns. `resource.attributes.*` prefix
for resource attributes. Directly queryable by DuckDB, Polars, DataFusion with
zero schema knowledge.
OTAP's star schema (4+ tables with foreign keys) is optimized for wire efficiency, not
queryability. Convert at the boundary when needed.

### bytes::Bytes for pipeline buffer ownership

The ingest path uses `BytesMut` while a component still owns mutable fill state,
then hands off immutable `Bytes` once ownership crosses a boundary. This is the
standard Rust pattern for I/O pipelines where buffers cross thread/async
boundaries and may need to outlive the original producer.

Why `Bytes` instead of `Vec<u8>`: The Arrow `StreamingBuilder` needs the input
buffer to outlive the scan phase — `StringViewArray` stores `(offset, len)` views
into the buffer, and the `RecordBatch` carries these through SQL transform and
output serialization. `Bytes` provides this lifetime extension via refcounting.
`Vec<u8>` cannot be shared without cloning.

Why not `Arc<Vec<u8>>`: `Bytes` supports zero-copy `slice()` and `split()` that
`Arc<Vec<u8>>` does not. That matters both for the zero-copy passthrough framing
path already on `main` and for the shared-buffer framing path now landing in
`FramedInput`, which still needs to preserve one contiguous scanner input.

Current boundary:

- `ffwd-io` tailing uses per-source `BytesMut` read buffers.
- `SourceEvent::Data` already carries `Bytes`.
- `FramedInput` may still keep small `Vec<u8>` remainders where a line is
  incomplete or overflow-tainted.
- `ffwd-runtime` currently performs the remaining pre-scan accumulation into
  `IngestState.buf: BytesMut`.
- `ffwd-core` is untouched by this ownership shift; the scanner still takes
  contiguous `Bytes` via `Bytes::Deref`.

Current implication: the remaining hot-path copy on `main` is not “tailer to
event” anymore. It is the runtime reassembly step that appends scanner-ready
`Bytes` into `IngestState.buf` before scan on the legacy event route. The next
architecture slice should widen the shared-buffer path so more polls append
directly into that final batch buffer before scan, targeting that seam
directly rather than reintroducing divergent source-side batching paths.

### Verification strategy

Three tools, each at a different layer. See `dev-docs/VERIFICATION.md` for mechanics.

| Layer | Tool | What it proves |
|-------|------|----------------|
| Design | TLA+ | Temporal properties: liveness, drain guarantee, checkpoint ordering |
| Implementation | Kani | Memory safety, arithmetic, no-panic, state transitions |
| Property-based | proptest | End-to-end correctness under arbitrary inputs |
| Compile-time | Rust type system | Illegal state transitions, silent data loss |

TLA+ proves the design is correct — no race conditions, correct ordering, drain eventually
completes. Kani proves the Rust implementation doesn't panic or overflow. A design bug is
caught by TLA+; an implementation bug is caught by Kani.

### Checkpoint as a pipeline step

Checkpoint is a first-class pipeline step that persists RecordBatches to disk between
transforms. Users control placement — the framework validates but doesn't dictate.

```yaml
pipelines:
  web_errors:
    inputs:
      - type: file
        path: /var/log/app/*.log
    steps:
      - type: transform
        sql: SELECT * FROM logs WHERE level IN ('ERROR', 'WARN')
      - type: checkpoint
        buffer_size: 256MB
        retention: 1h
      - type: transform
        sql: |
          SELECT level, message,
                 regexp_extract(message, 'request_id=([a-f0-9]+)', 1) AS req_id
          FROM logs
    outputs:
      - type: otlp
        endpoint: https://collector:4318
```

**Semantics:** The checkpoint step receives a RecordBatch, writes it to an Arrow IPC + zstd
segment on disk, and passes it through unchanged. On crash recovery, the pipeline replays
from the checkpoint's buffer instead of re-reading from the source.

**Config validation rules:**
- Checkpoint must come after an input source (not floating)
- Checkpoint must come before at least one output sink
- At most one checkpoint per input-to-output path
- Transforms downstream of checkpoint must be idempotent (no NOW(), RANDOM())

**Checkpoint coordination with input sources:**
- When the checkpoint buffer is durably flushed to disk (fsync), the input source's
  checkpoint (file offset or network sequence) advances atomically in the same
  `CheckpointStore::flush()` call
- On crash: replay from the input source checkpoint, which is guaranteed to be ≤ the
  checkpoint buffer's starting point
- File inputs: the file is the source-of-truth persistence; checkpoint step stores filtered output
- Network inputs: checkpoint step IS the only persistence

**Segment format:**
```
[4B magic: LCHK]
[4B version: 1]
[8B segment_id]
[8B sql_hash]          -- hash of upstream SQL, for schema change detection
[Arrow schema]         -- embedded for self-describing segments
... Arrow IPC stream batches (length-prefixed, zstd compressed) ...
[8B record_count]      -- footer
[4B CRC-32]
[4B magic: LCHK]       -- end marker
```

On recovery: read footer first. Missing/corrupt footer → discard segment. Schema hash
mismatch with current config → error with remediation ("clear buffer or revert SQL").

**Memory model:**
- RecordBatch wrapped in `Arc<RecordBatch>` — one ref for downstream, one for async writer
- Async writer runs in background tokio task with bounded channel (default 10K batches)
- When channel full → backpressure propagates upstream
- LRU eviction of in-memory segment cache when over memory budget

**Fan-out GC:**
- Per-output-sink ack watermark tracks which segments each sink has processed
- Segment GC when ALL sinks have acked (min watermark)
- Dead sink timeout: if no ack in 5 min, log error and force-advance that sink's watermark
- Prevents unbounded buffer growth from permanently dead sinks

**Alternative considered:** Framework-injected barriers (Flink model). Rejected because it
requires global coordination across sources, which conflicts with our per-source independent
checkpoint architecture. The user-placed checkpoint step gives control without barrier
complexity. If we add cross-source JOINs later, we can layer barriers on top.

### Pipeline Topology Compiler (#1363)

The pipeline topology compiler transforms a parsed configuration into a typed, verified execution DAG without starting any long-running workers.

**Phase 1: Dry-run scaffold**
- Introduce `PipelineSpec` and `CompiledTopology` models.
- Provide `compile_topology(config)` that builds the static DAG.
- Integrate into `ff dry-run` to execute this compiler, ensuring SQL plans and I/O shapes are correct statically.

**Future Phases (Not in Phase 1):**
- Translating the compiled topology into runtime Tokio tasks.
- Cross-pipeline routing.
- Advanced processor chain registry.

# Design

logfwd is an Arrow-native data processing pipeline. The core moves `RecordBatch` between
receivers, processors, and exporters. OTel semantics are optional adapters at the edges.

**RecordBatch in, RecordBatch out.** The pipeline doesn't care whether the data is logs,
metrics, traces, or CSV files.

## Target architecture

```
logfwd-core         Proven pure logic. #![no_std], #![forbid(unsafe_code)]
                    Parsing, encoding, pipeline state machine.
                    StructuralIndex consumers (framer, CRI, scanner).
                    Dependencies: memchr, wide (portable SIMD).
                    Every public function has a Kani proof or proptest.

logfwd-arrow        Arrow integration. Implements core's ScanBuilder trait.
                    StreamingBuilder, Scanner.
                    Bridge between parsed fields and RecordBatch.

logfwd-io           Produces RecordBatch from external sources.
                    File tailer, OTLP/TCP/UDP receivers, Arrow IPC reader.
                    Raw data goes through core→arrow→RecordBatch.
                    Arrow-native sources skip straight to RecordBatch.

logfwd-transform    RecordBatch → RecordBatch via DataFusion SQL.
                    UDFs, enrichment tables, JOINs.

logfwd-output       Consumes RecordBatch, sends externally.
                    OTLP, Arrow IPC, Parquet, ClickHouse, JSON lines.

logfwd              Async shell. CLI, config, pipeline orchestration,
                    diagnostics, signal handling.
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

1M+ events/sec end-to-end. Key bottleneck is HTTP output — the async pipeline overlaps IO
with CPU. SIMD structural scanning runs at 3.0 GiB/s (9 structural characters, NEON,
~760KB NDJSON, 2026-03-30 benchmark).

## What we're not building

- A general-purpose OTel Collector
- A database or query engine (we use DataFusion)
- A distributed system (scale by running more instances)

---

## Architecture decisions

### no_std structural enforcement for logfwd-core

`#![no_std]` with `extern crate alloc`. The compiler blocks IO — not lints that can be
`#[allow]`'d. This is the rustls/s2n-quic pattern. Four independent research studies
unanimously recommended this over std + clippy.

**Cost:** `core::`/`alloc::` import paths, hashbrown for HashMap if needed, memchr with
`default-features = false`.  
**Benefit:** Impossible to accidentally add IO to the proven core.

### FieldSink trait boundary (zero-allocation parsing)

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

### Scalar SIMD fallback in logfwd-core

`#![forbid(unsafe_code)]` is in core. SIMD intrinsics require unsafe. Solution: core has a
safe scalar `find_structural_chars_scalar` that is the Kani-provable specification. SIMD
impls live in logfwd-arrow behind a `CharDetector` trait. proptest verifies SIMD matches
scalar for random inputs.

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
**Research:** `dev-docs/research/offset-checkpoint-research.md`. **Related:** #270.

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

### Suffix only on type conflict

Bare column names by default. Suffixed columns (`_int`, `_str`, `_float`) only when a
field has multiple types within a batch.

**Core requirement — round-trip type fidelity.** A field that enters as `int 200` must
exit as `int 200`. No type promotion across documents.  
**Core requirement — clean schemas stay clean.** OTLP, Arrow IPC, CSV, and consistent JSON
produce bare column names with native types. The suffix machinery only activates when there
is an actual per-row type conflict.

Output always serializes with the bare JSON key, dispatching on DataType. A view layer
provides bare-name access for suffixed columns.

**Research:** `dev-docs/research/type-suffix-redesign.md`. **Related:** #445.

### Flat schema (not OTAP star schema)

Single `RecordBatch` with all fields as columns. `_resource_*` prefix for resource
attributes. Directly queryable by DuckDB, Polars, DataFusion with zero schema knowledge.
OTAP's star schema (4+ tables with foreign keys) is optimized for wire efficiency, not
queryability. Convert at the boundary when needed.

### bytes::Bytes for pipeline buffer ownership

Pipeline accumulation buffers use `BytesMut` (mutable, single-owner) in each
stage, converting to `Bytes` (immutable, refcounted) at stage boundaries via
`split().freeze()`. This is the standard Rust pattern for I/O pipelines where
buffers cross thread/async boundaries.

Why `Bytes` instead of `Vec<u8>`: The Arrow `StreamingBuilder` needs the input
buffer to outlive the scan phase — `StringViewArray` stores `(offset, len)` views
into the buffer, and the `RecordBatch` carries these through SQL transform and
output serialization. `Bytes` provides this lifetime extension via refcounting.
`Vec<u8>` cannot be shared without cloning.

Why not `Arc<Vec<u8>>`: `Bytes` supports zero-copy `slice()` and `split()` that
`Arc<Vec<u8>>` does not. Future FramedInput work (#608) will use `Bytes::slice()`
for zero-copy newline framing.

Current boundary: logfwd-io produces `Vec<u8>` (tailer, FramedInput). The Bytes
transition happens at `input_poll_loop` in pipeline.rs. logfwd-core is untouched
(scanner takes `&[u8]` via `Bytes::Deref`).

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

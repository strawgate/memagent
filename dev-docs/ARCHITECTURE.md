# Architecture

How data flows through ffwd, from bytes on disk to serialized output.

## Layers

```
┌─────────────────────────────────────────────────────────┐
│  ffwd (binary)                                        │
│  CLI entrypoints, config loading, signal handling       │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  ffwd-runtime                                          │
│  Async orchestration, worker pool, processor chain       │
│                                                         │
│  ┌──────────┐   ┌───────────┐   ┌───────────────────┐  │
│  │  Input   │──▶│ Transform │──▶│     Output        │  │
│  │ threads  │   │  (SQL)    │   │  (HTTP/stdout)    │  │
│  └──────────┘   └───────────┘   └───────────────────┘  │
└─────────────────────────────────────────────────────────┘
       │              │                    │
       ▼              ▼                    ▼
┌──────────────────────────────────────────────────┐
│  ffwd-arrow                                    │
│  ScanBuilder impls, SIMD backends, RecordBatch   │
└──────────────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────┐
│  ffwd-core                                     │
│  Pure logic, proven, no_std, forbid(unsafe)      │
└──────────────────────────────────────────────────┘
```

Dependencies flow downward. ffwd-core knows nothing about Arrow,
IO, or async. ffwd-arrow bridges core's parsing to Arrow types.
`ffwd-runtime` owns the long-lived async runtime, and the binary crate
now stays focused on startup/CLI/bootstrap concerns.

`ffwd-diagnostics` owns the diagnostics control-plane surface (diagnostics
HTTP endpoints, readiness/status shaping, stderr/span buffering, and the
generated `dashboard.html` asset served by diagnostics).

`ffwd-bench` is not in the production data path. It owns Criterion benchmarks
and profiling binaries used to measure scanner, pipeline, output, and source
metadata behavior under representative cardinality and allocation pressure.

## Ingest vocabulary

Use these terms consistently when discussing the raw-byte path:

- **Source**: input-specific byte production and lifecycle (`FileInput`, TCP, UDP, OTLP receiver).
- **Framing**: newline and per-source remainder handling in `FramedInput`.
- **Normalization**: format-aware shaping that preserves scanner semantics (for example CRI sidecar metadata).
- **Batching**: accumulation of scanner-ready bytes before scan in the runtime input path.
- **Parsing**: `Scanner::scan(Bytes)` turning contiguous bytes into Arrow arrays.
- **Materialization**: Arrow `RecordBatch` construction plus sidecar column attachment.
- **Transform**: DataFusion SQL execution over the scanned batch.
- **Sink**: output encoding and transport.

Ownership boundaries cut across those semantic layers:

- The source side owns mutable read buffers (`BytesMut`) while bytes are still being filled.
- `SourceEvent::Data` carries immutable `Bytes` once ownership crosses a component boundary.
- The scanner contract remains a contiguous `Bytes` buffer even when bytes arrived in fragments upstream.

## Data flow

### 1. Reading: bytes enter the system

```text
Disk → FileReader (`BytesMut`) → SourceEvent::Data { bytes: Bytes }
```

**FileTailer** (`ffwd-io/src/tail.rs`) is composed of two internal layers:
- **FileDiscovery**: path watching via notify (kqueue/inotify), glob evaluation,
  rotation detection, deleted-file cleanup, LRU eviction.
- **FileReader**: open file descriptors, per-file `BytesMut` read buffers, byte reading.

File-discovery reliability notes: `dev-docs/references/file-discovery.md`.

> **Note:** the `Bytes` boundary is now inside `ffwd-io`, not only in
> `ffwd-runtime`. The tailer owns mutable per-source `BytesMut` read buffers,
> `SourceEvent::Data` carries immutable `Bytes`, and `FramedInput` keeps only
> small per-source `Vec<u8>` remainders where a line is incomplete or tainted.

Each input source is polled on its own input-side worker thread. That thread
owns the mutable accumulation buffer used before scan and sends immutable
`Bytes` chunks to a per-input CPU worker over a bounded `tokio::sync::mpsc`
channel.

### 2. Framing: input bytes → complete lines

```text
Bytes → FramedInput::poll() → scanner-ready `SourceEvent::Data { bytes: Bytes }`
```

**FramedInput** (`ffwd-io/src/framed.rs`) combines format detection
with per-source remainder tracking. It handles three formats:

- **Json**: Lines are already JSON. Splits on `\n`, carries
  partial lines across reads via per-source remainder buffers.
- **Cri**: Kubernetes container log format. Parses timestamp,
  stream, flags, message. Reassembles P (partial) lines into complete
  messages. Emits `_timestamp` and `_stream` as sidecar metadata for
  post-scan Arrow column attachment; the payload JSON is not rewritten.
- **Text/Raw**: Passes lines through verbatim. Line capture into `body` is
  controlled by scanner `line_field_name`.

For passthrough formats, `FramedInput` already has a zero-copy fast path: if a
chunk arrives with no carried remainder and no overflow-tainted state, complete
lines can be forwarded directly as `Bytes` without an intermediate output copy.
For the shared-buffer batching path, `FramedInput::poll_into()` can now also
append scanner-ready output directly into the runtime-owned `BytesMut` batch
buffer, including CRI/Auto normalization and CRI sidecar metadata collection.
The remaining complexity in this area is about when the runtime chooses that
path, not about changing the scanner contract away from contiguous `Bytes`.

**NewlineFramer** (`ffwd-core/src/framer.rs`): fixed-size
output (4096 lines, 64KB stack), no heap, Kani-proven. It returns byte
ranges into the input buffer — zero-copy.

**CriReassembler** (`ffwd-core/src/reassembler.rs`):
zero-copy for F-only lines (99% of traffic), copies only for P+F
reassembly. Kani-proven.

**Current scanner boundary:** the scanner still consumes one contiguous `Bytes`
buffer per scan. Upstream work may reduce or relocate copies, but default scan
entry remains `Scanner::scan(Bytes)`.

**Current transition state:** the shared-buffer `FramedInput` path exists, but
`ffwd-runtime` still keeps the legacy `SourceEvent::Data { bytes: Bytes }`
route for fresh-buffer scans. The runtime currently switches to
`FramedInput::poll_into()` once a batch already has buffered bytes, preserving
the old single-chunk direct-flush behavior while removing the extra append on
continuation polls (#2539).

### 3. Structural classification: SIMD pre-pass

```text
&[u8] → find_structural_chars(block) → StreamingClassifier → processed bitmasks
```

**StreamingClassifier** (`ffwd-core/src/structural.rs`) is the
simdjson-inspired stage-1 algorithm. It processes 64-byte blocks,
producing bitmasks for structural character positions. From quote and
backslash positions it computes:

- **real_quotes**: unescaped quote positions (escaped quotes filtered out)
- **in_string**: which bytes are inside JSON strings vs structural

The scanner consumes these per-block masks directly. It stores only the
quote and delimiter masks needed for bounded per-line string extraction
and nested object/array skipping.

Platform-specific SIMD:
- **aarch64**: NEON (4 × 16-byte loads, vpaddl reduction)
- **x86_64**: AVX2 preferred (2 × 32-byte loads), SSE2 fallback
- **other**: scalar loop (LLVM auto-vectorizes)

The classifier detects `\n`, space, `"`, `\`, `,`, `:`, `{`, `}`, `[`,
and `]` in one pass. This eliminates separate newline scanning and
enables bitmask-based JSON field extraction.

### 4. Scanning: JSON → typed fields

```text
&[u8] + streaming structural bitmasks → ScanBuilder callbacks → typed column values
```

**scan_streaming** (`ffwd-core/src/json_scanner.rs`) walks each line
left-to-right, extracting key-value pairs. It uses streaming structural
bitmasks for string boundary detection and the **ScanBuilder** trait for
output:

```rust
pub trait ScanBuilder {
    fn begin_row(&mut self);
    fn end_row(&mut self);
    fn resolve_field(&mut self, key: &[u8]) -> usize;
    fn append_str_by_idx(&mut self, idx: usize, value: &[u8]);
    fn append_decoded_str_by_idx(&mut self, idx: usize, value: &[u8]);
    fn append_int_by_idx(&mut self, idx: usize, value: &[u8]);
    fn append_float_by_idx(&mut self, idx: usize, value: &[u8]);
    fn append_bool_by_idx(&mut self, idx: usize, value: bool);
    fn append_null_by_idx(&mut self, idx: usize);
    fn append_line(&mut self, line: &[u8]);
}
```

This is the **key abstraction boundary**. The scanner lives in the system's pure
logic kernel (`ffwd-core` and `ffwd-kani`, both proven, no_std). It doesn't
know about Arrow: it calls trait methods from `ffwd-core/src/scanner.rs` that
ffwd-arrow implements.

**ScanConfig** controls which fields to extract (field pushdown from
SQL analysis) and type detection. Fields are typed per-value: when a
field has a single type across all rows it gets a bare column name
(`status` as `Int64`). When a field has mixed types, the builder emits
a `StructArray` conflict column (`status: Struct { int, str }`).

### 5. Building: typed fields → Arrow RecordBatch

```
ScanBuilder callbacks → StreamingBuilder → RecordBatch
```

**StreamingBuilder** (`ffwd-arrow/src/streaming_builder.rs`):
Implements `ScanBuilder`. Stores `(offset, len)` views into the input
`bytes::Bytes` buffer. Produces `StringViewArray` (zero-copy) via
`finish_batch()` or `StringArray` (detached) via `finish_batch_detached()`.

**Scanner** (`ffwd-arrow/src/scanner.rs`) wraps StreamingBuilder:

- `Scanner::scan(Bytes) → RecordBatch` — zero-copy `StringViewArray`,
  buffer must stay alive
- `Scanner::scan_detached(Bytes) → RecordBatch` — self-contained
  `StringArray`, buffer can be freed. For persistence/compression.

### 6. Metadata attach and transform: RecordBatch → RecordBatch

```
RecordBatch → sidecar attach/replace → SqlTransform::execute_blocking() → RecordBatch
```

Before SQL execution, the runtime attaches sidecar metadata as Arrow columns on
the scanned `RecordBatch`. Source metadata is appended/replaced according to
the configured source metadata style. CRI `_timestamp` and `_stream` are
appended or replaced from the CRI sidecar; rows without CRI values preserve any
existing payload columns and otherwise materialize as nulls. This stage is the
only place CRI metadata becomes columns.

**SqlTransform** (`ffwd-transform/src/lib.rs`) runs user SQL via
DataFusion. Each batch is registered as a `"logs"` MemTable, the SQL
executes, and the result is collected.

**QueryAnalyzer** parses the SQL at startup and extracts referenced
columns. This produces a `ScanConfig` for field pushdown — the
scanner only extracts fields the SQL actually uses.

Custom UDFs: `int()`, `float()`, `regexp_extract()`, `grok()`,
`geo_lookup()`.

CSV enrichment tables are parsed in `ffwd-transform` and materialized through
`ColumnarBatchBuilder` as nullable `Utf8View` columns. Delimiter, quote, header,
duplicate-header, and row-alignment semantics remain local to the transform
crate; `ffwd-arrow` only owns the shared columnar builder and Arrow
finalization mechanics.

### 7. Output: RecordBatch → wire format

```
RecordBatch → OutputSink::send_batch() → HTTP/stdout/file
```

**OutputSink** (`ffwd-output/src/lib.rs`) implementations:

- **OtlpSink**: Encodes RecordBatch → OTLP protobuf, sends via HTTP.
  Handles resource attributes, observed timestamps, DataType-based dispatch.
- **ElasticsearchSink**: Bulk API with retry, compression, async reqwest.
- **LokiSink**: Loki push API with label grouping, dedup, async reqwest.
- **JsonLinesSink**: Converts RecordBatch → newline-delimited JSON,
  sends via HTTP with zstd compression.
- **StdoutSink**: Renders to terminal (JSON, console, or text format).
- **FanoutSink**: Sends to multiple sinks.

## Crate boundaries

Each boundary is a trait that ffwd-core defines and other crates
implement:

```
ffwd-core defines          ffwd-arrow implements
──────────────────────────    ──────────────────────────
ScanBuilder                   StreamingBuilder
                              Scanner (wrapper)

ffwd-io defines + implements
──────────────────────────────────────────
InputSource trait              FileInput, TcpInput, UdpInput,
                               OtlpReceiverInput

ffwd-output defines + implements
──────────────────────────────────────────
OutputSink / Sink traits       OtlpSink, ElasticsearchSink,
                               LokiSink, JsonLinesSink, StdoutSink
```

`ffwd-runtime` wires these together in `pipeline.rs`.

`ffwd-diagnostics` provides the diagnostics server and telemetry-facing
snapshot types consumed by runtime/bootstrap wiring.

## Pipeline loop

The runtime path is split into three stages:

```text
1. Input-side worker thread (`io_worker.rs`)
   - polls `InputSource`
   - receives `SourceEvent`s from `FramedInput`
   - accumulates scanner-ready bytes in `IngestState.buf: BytesMut`
   - direct-flushes large solitary chunks when possible
   - emits `IoWorkItem::RawBatch { bytes: Bytes, ... }`

2. Per-input CPU worker (`cpu_worker.rs`)
   - calls `Scanner::scan(Bytes)`
   - attaches source metadata and CRI sidecars
   - runs the per-input SQL transform
   - emits `ProcessedBatch { batch, checkpoints, ... }`

3. Async pipeline (`run_async()`)
   - receives already scanned + transformed batches
   - runs processor stages
   - submits to the async output worker pool
   - advances checkpoint lifecycle from output ACK/reject results
```

This separation matters for batching work:

- **Current pre-scan batching seam** is in the input-side worker (`IngestState.buf`).
- **Scanner boundary** is in the CPU worker and remains contiguous `Bytes`.
- **Pipeline async loop** no longer owns raw-byte accumulation.

## Buffer lifecycle

Understanding who owns what and when copies happen:

```text
Current on `main`:
  tailer reads → per-file BytesMut                       [kernel → userspace]
  tailer emits SourceEvent::Data { bytes: Bytes }         (ownership transfer)
  FramedInput passthrough fast path                      (zero-copy when no remainder/taint)
  FramedInput shared-buffer path via poll_into()         (direct append for buffered continuation polls)
  FramedInput legacy event path                          (still used for fresh-buffer scans)
  io_worker: input.buf.extend_from_slice(&bytes)         [remaining pre-scan copy on legacy path]
  io_worker flush: split().freeze() → Bytes              (zero-copy — refcount only)
  cpu_worker: Scanner::scan(Bytes)                       (no re-copy before scan)
  StreamingBuilder stores views → RecordBatch            (zero-copy)
  sidecar attach/replace appends Arrow columns before SQL
  Bytes dropped when RecordBatch is consumed

Current ownership boundary:
  source/framing side: per-source BytesMut + small Vec remainders
  runtime batching boundary: IngestState.buf as contiguous BytesMut
  scanner boundary: contiguous Bytes

Next target in this seam:
  use the shared-buffer path for more of the runtime batching lifecycle, not only buffered continuation polls
  remove more legacy `SourceEvent::Data` reassembly from the pre-scan hot path
  keep the scanner contract as `Scanner::scan(Bytes)`
  benchmark the wider enablement before deleting the fallback route
```

## Data-Oriented Design

The pipeline is structured for the CPU, not for object-oriented
convenience. This is not a stylistic choice — it is a load-bearing
architectural commitment.

- **Struct-of-Arrays (SoA) is the default representation.** Arrow
  `RecordBatch` is literally SoA. The scanner, builders, SQL transform,
  and OTLP encoder all operate on contiguous per-column buffers, not
  row objects. When adding a new per-record piece of data (a parsed
  timestamp, a source tag, a flag), the default answer is "another
  column," not "another field on a row struct."
- **Hot fields stay contiguous; cold fields go to a side table.**
  Per-source metadata that is not used in the scan/transform loop
  (source paths, file handles, receiver identity) lives in sidecar
  tables keyed by `SourceId`, not inline with payload bytes.
- **No `LinkedList`, no per-record boxing in hot paths.** Cache locality
  dominates runtime at our throughputs. `Vec<T>` with explicit capacity
  hints, `SmallVec` for small bounded collections, `Bytes` slices for
  shared byte views. `Box<[T]>` instead of `Vec<T>` when the length is
  fixed after construction.
- **Arena allocation at boundaries where it helps.** Short-lived
  per-batch object graphs (parse nodes, intermediate Arrow builders)
  that currently allocate per-record are candidates for arena or bump
  allocation. See `ffwd-arrow/src/columnar/` for the existing pattern.
- **No raw payload injection (see `CRATE_RULES.md` → `ffwd-io`).** The
  no-raw-injection rule is a data-layout rule: source metadata travels
  in a sidecar, not in the payload buffer. This is the architectural
  manifestation of SoA.

## Compile time as a quality attribute

Build time is a first-class quality attribute, not an aesthetic
concern. Long compile cycles degrade every iteration of every
contributor and agent, compound across CI, and actively harm code
quality because they discourage small-step verification. The
`default-members` / `--workspace` split (`~30s` vs `~3min`) is the
explicit recognition of this — it is load-bearing for iteration speed.

Practical rules:

- **Prefer `&dyn Trait` at crate seams where monomorphization cost is
  real.** At internal call sites generics are usually the right default
  (static dispatch, inlining). Across crate boundaries where the callee
  is instantiated for many distinct caller types, `dyn Trait` can be
  the right trade. Default to generics, reach for `dyn` when the
  compile-time cost is observable.
- **Gate heavy dependencies behind Cargo features.** DataFusion,
  reqwest, large proc-macro crates. The `datafusion` feature on
  `ffwd-runtime` and the `ffwd-transform` default-members exclusion
  exist for this reason — replicate the pattern for future heavy
  dependencies.
- **Split crates along natural seams.** Parallel compilation is cheap;
  monolithic crates are not. When a crate grows to the point where
  touching one module forces the whole thing to rebuild, split it.
- **Avoid `build.rs` unless genuinely required.** `build.rs` serializes
  compilation and invalidates caches aggressively.
- **Reserve proc macros for cases `macro_rules!` cannot handle.** Proc
  macros pull in syn/quote and extend compile time per crate that uses
  them.
- **Use `cargo --timings` when investigating slow builds.** Find the
  actual long pole before refactoring.

The WASM build target (`ffwd-config-wasm`) makes this discipline
non-optional: heavy dependencies leak across the boundary and inflate
the WASM artifact.

## Verification strategy

Each pipeline layer has appropriate verification. See `dev-docs/VERIFICATION.md` for tool
selection guidance, proof quality requirements, and per-module status.

| Layer | Tool |
|-------|------|
| Structural scanning, framing, CRI, byte search, number parsing, OTLP encoding | Kani (exhaustive/bounded) |
| SIMD backends | proptest (SIMD ≡ scalar conformance) |
| Pipeline state machine | TLA+ (temporal: drain liveness, checkpoint ordering) + Kani (single-step) |
| End-to-end roundtrip | proptest |

## Related documents

- `dev-docs/DESIGN.md` — vision, goals, and architecture decision records
- `dev-docs/VERIFICATION.md` — verification tiers, tool selection, per-module status
- `dev-docs/CRATE_RULES.md` — per-crate enforcement rules
- `dev-docs/research/zero-copy-pipeline-design.md` — framing/buffer-lifecycle deep dive

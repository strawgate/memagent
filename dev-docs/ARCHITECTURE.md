# Architecture

How data flows through logfwd, from bytes on disk to serialized output.

## Ingest Glossary

This document uses one architectural vocabulary for the pre-Arrow path:

- **Source**: where data enters the system (`FileInput`, TCP, UDP, OTLP HTTP, Arrow IPC)
- **Framing**: line boundaries and per-source remainder management
- **Normalization**: CRI / auto / passthrough processing that produces scanner-ready NDJSON
- **Batching**: when to flush scanner-ready bytes to the scanner
- **Parsing**: NDJSON field extraction driven by structural classification
- **Materialization**: Arrow `RecordBatch` assembly
- **Enrichment**: source metadata and CRI sidecar attachment
- **Transform**: SQL / DataFusion processing
- **Sink**: OTLP, Elasticsearch, Loki, stdout, JSON lines, file

The code still uses implementation-specific names such as `InputEvent`,
`InputSource`, `FramedInput`, `io_worker`, and `cpu_worker`. Those names are
about to be cleaned up, but the architectural model above is the stable way to
talk about the ingest path.

## Crate Layers

```
┌─────────────────────────────────────────────────────────┐
│  logfwd (binary)                                        │
│  CLI entrypoints, config loading, signal handling       │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  logfwd-runtime                                          │
│  Async orchestration, worker pool, batching, SQL, sink   │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────┐
│  logfwd-arrow                                    │
│  Scanner wrapper, Arrow builders, RecordBatch    │
└──────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────┐
│  logfwd-core                                     │
│  Pure logic, proven, no_std, forbid(unsafe)      │
└──────────────────────────────────────────────────┘
```

Dependencies flow downward. `logfwd-core` knows nothing about Arrow, I/O, or
async. `logfwd-arrow` bridges core parsing to Arrow types. `logfwd-runtime`
owns the long-lived runtime shell, and the binary crate stays focused on
startup/CLI/bootstrap concerns.

`logfwd-diagnostics` owns the diagnostics control-plane surface (diagnostics
HTTP endpoints, readiness/status shaping, stderr/span buffering, and the
generated `dashboard.html` asset served by diagnostics).

`logfwd-bench` is not in the production data path. It owns Criterion benchmarks
and profiling binaries used to measure scanner, pipeline, output, and source
metadata behavior under representative cardinality and allocation pressure.

## Data Flow

There are two equally important ways to think about ingest:

- **Semantic layers**: Source -> Framing -> Normalization -> Batching -> Parsing -> Materialization
- **Ownership / control layers**: who owns mutable bytes, when batches flush, and which worker drives polling

Most prior confusion came from mixing those two views. `InputSource::poll()` is
mostly a control-plane interface; `FramedInput` is a semantic layer; runtime
batch accumulation is an ownership / flush-policy layer.

### 1. Source: bytes enter the system

```
Disk/TCP/UDP/HTTP → InputSource::poll() → InputEvent
```

**FileTailer** (`logfwd-io/src/tail.rs`) is composed of two internal layers:
- **FileDiscovery**: path watching via notify (kqueue/inotify), glob evaluation,
  rotation detection, deleted-file cleanup, LRU eviction.
- **FileReader**: open file descriptors, persistent read buffer, byte reading.

File-discovery reliability notes: `dev-docs/references/file-discovery.md`.

Today the source boundary is already `bytes::Bytes`, not `Vec<u8>`.
`InputEvent::Data` carries:

- `bytes: Bytes`
- `source_id: Option<SourceId>`
- `accounted_bytes: u64`
- optional CRI sidecar metadata

Structured receivers can skip the raw-byte path entirely and emit
`InputEvent::Batch { batch: RecordBatch, ... }`.

Each input source runs on its own OS thread. Reads feed a bounded
`tokio::sync::mpsc` channel to the async pipeline loop. The channel
carries immutable ownership (`Bytes` or `RecordBatch`) cleanly across the
thread boundary.

### 2. Framing and Normalization: source bytes -> scanner-ready NDJSON

```
InputEvent::Data { bytes: Bytes, ... } -> FramedInput::poll() -> scanner-ready bytes
```

**FramedInput** (`logfwd-io/src/framed.rs`) combines format detection
with per-source remainder tracking. Architecturally it owns:

- newline framing
- partial-line remainder state
- CRI partial-record aggregation state
- checkpoint frontier bookkeeping relative to complete newline boundaries

It handles three formats:

- **Json**: Lines are already JSON. Splits on `\n`, carries
  partial lines across reads via per-source remainder buffers.
- **Cri**: Kubernetes container log format. Parses timestamp,
  stream, flags, message. Reassembles P (partial) lines into complete
  messages. Emits `_timestamp` and `_stream` as sidecar metadata for
  post-scan Arrow column attachment; the payload JSON is not rewritten.
- **Text/Raw**: Passes lines through verbatim. Line capture into `body` is
  controlled by scanner `line_field_name`.

`FramedInput` is the semantic owner for raw-byte inputs. The runtime should not
grow a second framing path beside it.

**NewlineFramer** (`logfwd-core/src/framer.rs`): fixed-size
output (4096 lines, 64KB stack), no heap, Kani-proven. It returns byte
ranges into the input buffer — zero-copy.

**CriReassembler** (`logfwd-core/src/reassembler.rs`):
zero-copy for F-only lines (99% of traffic), copies only for P+F
reassembly. Kani-proven.

### 3. Batching: scanner-ready bytes -> contiguous scan input

The runtime batches scanner-ready bytes before scan. Today this is already a
deferred-copy design:

- one buffered scanner-ready `Bytes` chunk is kept without copying
- if more chunks arrive, they are held as separate `Bytes`
- concatenation happens once at flush time if the scanner needs a contiguous buffer

This means pre-scan ownership can be conceptually fragmented even though the
default scanner contract is contiguous.

The current hot boundary is:

```
FramedInput output -> pending_chunk | pending_chunks -> Scanner::scan(Bytes)
```

That distinction matters for future work:

- fragmented accumulation before scan is acceptable
- the default scan call still takes one contiguous `Bytes`
- removing copies should happen by filling that contiguous buffer earlier, not
  by making every layer pretend fragments are free

**Target:** Replace the remaining copy-heavy raw-byte path with a layered
shared-buffer model (#2424, #2426, #2539):

```
Source bytes
  → FramedInput shared-buffer append
  → contiguous scanner batch
  → streaming structural classifier
  → Scanner
```

### 4. Parsing: structural classification + field extraction

```text
&[u8] → find_structural_chars(block) → StreamingClassifier → processed bitmasks
```

**StreamingClassifier** (`logfwd-core/src/structural.rs`) is the
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

The classifier detects `\n`, space, `"`, `\`, `,`, `:`, `{`, `}`, `[`, and
`]` in one pass. This eliminates separate newline scanning and enables
bitmask-based JSON field extraction.

### 5. Materialization: parsed fields -> Arrow RecordBatch

```text
&[u8] + streaming structural bitmasks → ScanBuilder callbacks → typed column values
```

**scan_streaming** (`logfwd-core/src/json_scanner.rs`) walks each line
left-to-right, extracting key-value pairs. It uses streaming structural
bitmasks for string boundary detection and the **ScanBuilder** trait for
output:

```rust
pub trait ScanBuilder {
    fn begin_row(&mut self);
    fn end_row(&mut self);
    fn resolve_field(&mut self, key: &[u8]) -> usize;
    fn append_str_by_idx(&mut self, idx: usize, value: &[u8]);
    fn append_int_by_idx(&mut self, idx: usize, value: &[u8]);
    fn append_float_by_idx(&mut self, idx: usize, value: &[u8]);
    fn append_null_by_idx(&mut self, idx: usize);
    fn append_line(&mut self, line: &[u8]);
}
```

This is the **key abstraction boundary**. The scanner lives in
logfwd-core (proven, no_std). It doesn't know about Arrow: it calls trait
methods from `logfwd-core/src/scanner.rs` that logfwd-arrow implements.

**ScanConfig** controls which fields to extract (field pushdown from
SQL analysis) and type detection. Fields are typed per-value: when a
field has a single type across all rows it gets a bare column name
(`status` as `Int64`). When a field has mixed types, the builder emits
a `StructArray` conflict column (`status: Struct { int, str }`).

**StreamingBuilder** (`logfwd-arrow/src/streaming_builder.rs`):
Implements `ScanBuilder`. Stores `(offset, len)` views into the input
`bytes::Bytes` buffer. Produces `StringViewArray` (zero-copy) via
`finish_batch()` or `StringArray` (detached) via `finish_batch_detached()`.

**Scanner** (`logfwd-arrow/src/scanner.rs`) wraps StreamingBuilder:

- `Scanner::scan(Bytes) → RecordBatch` — zero-copy `StringViewArray`,
  buffer must stay alive
- `Scanner::scan_detached(Bytes) → RecordBatch` — self-contained
  `StringArray`, buffer can be freed. For persistence/compression.

The scanner boundary is intentionally contiguous. `Scanner::scan` takes a
single `Bytes` because the current scanner + `StreamingBuilder` pair stores
views into one backing buffer. Pre-scan accumulation may use one chunk or many
chunks, but the default scanner contract is still one contiguous `Bytes`.

### 6. Enrichment and Transform: RecordBatch -> RecordBatch

```
RecordBatch → sidecar attach/replace → SqlTransform::execute_blocking() → RecordBatch
```

Before SQL execution, the runtime attaches sidecar metadata as Arrow columns on
the scanned `RecordBatch`. Source metadata is appended/replaced according to
the configured source metadata style. CRI `_timestamp` and `_stream` are
appended or replaced from the CRI sidecar; rows without CRI values preserve any
existing payload columns and otherwise materialize as nulls. This stage is the
only place CRI metadata becomes columns.

**SqlTransform** (`logfwd-transform/src/lib.rs`) runs user SQL via
DataFusion. Each batch is registered as a `"logs"` MemTable, the SQL
executes, and the result is collected.

**QueryAnalyzer** parses the SQL at startup and extracts referenced
columns. This produces a `ScanConfig` for field pushdown — the
scanner only extracts fields the SQL actually uses.

Custom UDFs: `int()`, `float()`, `regexp_extract()`, `grok()`,
`geo_lookup()`.

CSV enrichment tables are parsed in `logfwd-transform` and materialized through
`ColumnarBatchBuilder` as nullable `Utf8View` columns. Delimiter, quote, header,
duplicate-header, and row-alignment semantics remain local to the transform
crate; `logfwd-arrow` only owns the shared columnar builder and Arrow
finalization mechanics.

### 7. Sink: RecordBatch -> wire format

```
RecordBatch → OutputSink::send_batch() → HTTP/stdout/file
```

**OutputSink** (`logfwd-output/src/lib.rs`) implementations:

- **OtlpSink**: Encodes RecordBatch → OTLP protobuf, sends via HTTP.
  Handles resource attributes, observed timestamps, DataType-based dispatch.
- **ElasticsearchSink**: Bulk API with retry, compression, async reqwest.
- **LokiSink**: Loki push API with label grouping, dedup, async reqwest.
- **JsonLinesSink**: Converts RecordBatch → newline-delimited JSON,
  sends via HTTP with zstd compression.
- **StdoutSink**: Renders to terminal (JSON, console, or text format).
- **FanoutSink**: Sends to multiple sinks.

## Crate boundaries

Each boundary is a trait that logfwd-core defines and other crates
implement:

```
logfwd-core defines          logfwd-arrow implements
──────────────────────────    ──────────────────────────
ScanBuilder                   StreamingBuilder
                              Scanner (wrapper)

logfwd-io defines + implements
──────────────────────────────────────────
InputSource trait              FileInput, TcpInput, UdpInput,
                               OtlpReceiverInput

logfwd-output defines + implements
──────────────────────────────────────────
OutputSink / Sink traits       OtlpSink, ElasticsearchSink,
                               LokiSink, JsonLinesSink, StdoutSink
```

`logfwd-runtime` wires these together in `pipeline.rs`.

`logfwd-diagnostics` provides the diagnostics server and telemetry-facing
snapshot types consumed by runtime/bootstrap wiring.

## Pipeline Loop

The async runtime shell in `run_async()` (`logfwd-runtime/src/pipeline.rs`)
drives polling, batching, transform, and sink delivery:

```
loop {
    select! {
        // Receive source output via bounded channel
        msg = rx.recv() => {
            // Accumulate scanner-ready bytes or direct RecordBatch output
            // If batch_target_bytes or timer threshold is hit:
            //   flush_batch()
        }
        // Timeout: flush partial batch
        _ = sleep(batch_timeout) => {
            flush_batch()
        }
        // Shutdown signal
        _ = shutdown.cancelled() => {
            // Drain channel, flush remaining, break
        }
    }
}

fn flush_batch():
    raw = build_contiguous_scan_input_if_needed()
    batch = scanner.scan(raw)        // parsing + materialization
    batch = attach_sidecars(batch)   // enrichment
    batch = transform.execute(batch) // SQL / transform
    output.send_batch(batch)         // sink delivery
```

Source threads are OS threads (blocking I/O). The runtime loop is
async (tokio). Scanner and output use `block_in_place` for sync
operations.

## Buffer Lifecycle

Understanding who owns what and when copies happen:

```
Current runtime shape:
  source reads / receives bytes                           [kernel/network -> userspace]
  InputEvent::Data carries Bytes                          (ownership transfer)
  FramedInput may copy for remainder prepend or format rewrite
  runtime batches one Bytes or many Bytes                 (no eager copy on arrival)
  flush: concatenate once if needed for scan boundary     [contiguous scan input]
  Scanner receives one Bytes                              (zero-copy into builder views)
  StreamingBuilder stores views -> RecordBatch            (zero-copy)
  Enrichment attaches source / CRI sidecars as columns

Current ownership boundaries:
  source boundary: InputEvent::{Data(Bytes), Batch(RecordBatch), ...}
  batching boundary: pending_chunk | pending_chunks in runtime
  scan boundary: one contiguous Bytes for Scanner::scan()

Shared-buffer target:
  source reads / receives bytes
  FramedInput appends scanner-ready bytes into shared BytesMut
  runtime flushes contiguous buffer directly
  Scanner receives one Bytes
  StreamingBuilder stores views -> RecordBatch
```

The important architectural point is not "everything is always contiguous."
It is:

- mutable raw-byte ownership should be explicit
- pre-scan accumulation may be fragmented
- the default scanner boundary is still contiguous
- `FramedInput` remains the one framing contract for raw-byte sources

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
  allocation. See `logfwd-arrow/src/columnar/` for the existing pattern.
- **No raw payload injection (see `CRATE_RULES.md` → `logfwd-io`).** The
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
  `logfwd-runtime` and the `logfwd-transform` default-members exclusion
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

The WASM build target (`logfwd-config-wasm`) makes this discipline
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

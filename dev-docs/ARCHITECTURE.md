# Architecture

How data flows through logfwd, from bytes on disk to serialized output.

## Layers

```
┌─────────────────────────────────────────────────────────┐
│  logfwd (binary)                                        │
│  CLI entrypoints, config loading, signal handling       │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  logfwd-runtime                                          │
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
│  logfwd-arrow                                    │
│  ScanBuilder impls, SIMD backends, RecordBatch   │
└──────────────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────┐
│  logfwd-core                                     │
│  Pure logic, proven, no_std, forbid(unsafe)      │
└──────────────────────────────────────────────────┘
```

Dependencies flow downward. logfwd-core knows nothing about Arrow,
IO, or async. logfwd-arrow bridges core's parsing to Arrow types.
`logfwd-runtime` owns the long-lived async runtime, and the binary crate
now stays focused on startup/CLI/bootstrap concerns.

`logfwd-diagnostics` owns the diagnostics control-plane surface (diagnostics
HTTP endpoints, readiness/status shaping, stderr/span buffering, and the
generated `dashboard.html` asset served by diagnostics).

`logfwd-bench` is not in the production data path. It owns Criterion benchmarks
and profiling binaries used to measure scanner, pipeline, output, and source
metadata behavior under representative cardinality and allocation pressure.

## Data flow

### 1. Reading: bytes enter the system

```
Disk → FileReader (Vec<u8>) → InputEvent::Data { bytes: Vec<u8> }
```

**FileTailer** (`logfwd-io/src/tail.rs`) is composed of two internal layers:
- **FileDiscovery**: path watching via notify (kqueue/inotify), glob evaluation,
  rotation detection, deleted-file cleanup, LRU eviction.
- **FileReader**: open file descriptors, `Vec<u8>` read buffer, byte reading.

File-discovery reliability notes: `dev-docs/references/file-discovery.md`.

> **Note:** logfwd-io (tailer, InputEvent, FramedInput) still uses `Vec<u8>`.
> Only `pipeline.rs` uses `BytesMut`/`Bytes`. The Bytes boundary is at the
> `input_poll_loop` → `ChannelMsg` transition.

Each input source runs on its own OS thread. Reads feed a bounded
`tokio::sync::mpsc` channel to the async pipeline loop. The channel
carries `Bytes` (immutable, refcounted) — ownership transfers cleanly
across the thread boundary.

### 2. Framing: input bytes → complete lines

```
Vec<u8> → FramedInput::poll() → newline-delimited JSON as Vec<u8>
```

**FramedInput** (`logfwd-io/src/framed.rs`) combines format detection
with per-source remainder tracking. It handles three formats:

- **Json**: Lines are already JSON. Splits on `\n`, carries
  partial lines across reads via per-source remainder buffers.
- **Cri**: Kubernetes container log format. Parses timestamp,
  stream, flags, message. Reassembles P (partial) lines into complete
  messages. Injects `_timestamp` and `_stream` as JSON fields.
- **Text/Raw**: Passes lines through verbatim. Line capture into `body` is
  controlled by scanner `line_field_name`.

**NewlineFramer** (`logfwd-core/src/framer.rs`): fixed-size
output (4096 lines, 64KB stack), no heap, Kani-proven. It returns byte
ranges into the input buffer — zero-copy.

**CriReassembler** (`logfwd-core/src/reassembler.rs`):
zero-copy for F-only lines (99% of traffic), copies only for P+F
reassembly. Kani-proven.

**Target:** Replace the copy-based framing with a layered zero-copy model (#303):

```
Bytes → streaming structural classifier [per-block SIMD bitmasks]
      → NewlineFramer                  [consumes \n positions → line ranges]
      → CriReassembler (if CRI format) [merges P/F partials]
      → Scanner                        [consumes per-line structural bitmasks]
```

### 3. Structural classification: SIMD pre-pass

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

The classifier detects `\n`, space, `"`, `\`, `,`, `:`, `{`, `}`, `[`,
and `]` in one pass. This eliminates separate newline scanning and
enables bitmask-based JSON field extraction.

### 4. Scanning: JSON → typed fields

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

### 5. Building: typed fields → Arrow RecordBatch

```
ScanBuilder callbacks → StreamingBuilder → RecordBatch
```

**StreamingBuilder** (`logfwd-arrow/src/streaming_builder.rs`):
Implements `ScanBuilder`. Stores `(offset, len)` views into the input
`bytes::Bytes` buffer. Produces `StringViewArray` (zero-copy) via
`finish_batch()` or `StringArray` (detached) via `finish_batch_detached()`.

**Scanner** (`logfwd-arrow/src/scanner.rs`) wraps StreamingBuilder:

- `Scanner::scan(Bytes) → RecordBatch` — zero-copy `StringViewArray`,
  buffer must stay alive
- `Scanner::scan_detached(Bytes) → RecordBatch` — self-contained
  `StringArray`, buffer can be freed. For persistence/compression.

### 6. Transform: RecordBatch → RecordBatch

```
RecordBatch → SqlTransform::execute_blocking() → RecordBatch
```

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

### 7. Output: RecordBatch → wire format

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

## Pipeline loop

The async pipeline in `run_async()` (`logfwd-runtime/src/pipeline.rs`):

```
loop {
    select! {
        // Receive from input threads via bounded channel
        msg = rx.recv() => {
            // Append to json_buf via FramedInput
            // If buf >= batch_target_bytes OR timer expired:
            //   flush_batch(json_buf)
        }
        // Timeout: flush partial batch
        _ = sleep(batch_timeout) => {
            flush_batch(json_buf)
        }
        // Shutdown signal
        _ = shutdown.cancelled() => {
            // Drain channel, flush remaining, break
        }
    }
}

fn flush_batch(buf):
    batch = scanner.scan(buf)        // classify + scan + build
    batch = transform.execute(batch) // SQL
    output.send_batch(batch)         // serialize + send
```

Input threads are OS threads (blocking IO). The pipeline loop is
async (tokio). Scanner and output use `block_in_place` for sync
operations.

## Buffer lifecycle

Understanding who owns what and when copies happen:

```
Current (after #939 + #963 — 5 heap copies, 2 zero-copy transitions):
  tailer reads → Vec<u8>                                [COPY 1: kernel → userspace]
  FramedInput: remainder + extend_from_slice             [COPY 2: Vec prepend]
  FormatDecoder: chunk → out_buf                         [COPY 3: format processing]
  pipeline input.buf: extend_from_slice → BytesMut       [COPY 4: thread accumulation]
  channel: split().freeze() → Bytes                      (zero-copy — refcount only)
  scan_buf: extend_from_slice → BytesMut                 [COPY 5: async accumulation]
  flush: split().freeze() → Bytes                        (zero-copy — refcount only)
  Scanner receives Bytes directly                (zero-copy)
  StreamingBuilder stores views → RecordBatch            (zero-copy)
  Bytes dropped when RecordBatch is consumed

  logfwd-io boundary: Vec<u8> (tailer, InputEvent, FramedInput)
  pipeline.rs boundary: BytesMut/Bytes (ChannelMsg, scan_buf, scanner)

Target (zero-copy for 99% passthrough path — #608):
  tailer reads → BytesMut → freeze() → Bytes             (no copy)
  FramedInput: Bytes::slice() for line ranges             (no copy)
  Passthrough format: emit Bytes slice directly           (no copy)
  CRI format: metadata injection requires rewrite         [COPY 1: unavoidable]
  pipeline: scan_buf.extend_from_slice(&bytes)            [COPY 2: SIMD contiguity]
  Scanner receives Bytes directly                 (no copy)
  StreamingBuilder stores views → RecordBatch             (zero-copy)
  Bytes dropped when RecordBatch is consumed
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
- [Roadmap (GitHub issue #889)](https://github.com/strawgate/fastforward/issues/889) — implementation phases with issue references

# Architecture

How data flows through logfwd, from bytes on disk to serialized output.

## Layers

```
┌─────────────────────────────────────────────────────────┐
│  logfwd (binary)                                        │
│  Async orchestration, config, CLI, signal handling      │
│                                                         │
│  ┌──────────┐   ┌───────────┐   ┌───────────────────┐  │
│  │  Input   │──▶│ Transform │──▶│     Output        │  │
│  │ threads  │   │  (SQL)    │   │  (HTTP/stdout)    │  │
│  └──────────┘   └───────────┘   └───────────────────┘  │
│       │              │                    │              │
│       ▼              ▼                    ▼              │
│  ┌──────────────────────────────────────────────────┐   │
│  │  logfwd-arrow                                    │   │
│  │  ScanBuilder impls, SIMD backends, RecordBatch     │   │
│  └──────────────────────────────────────────────────┘   │
│       │                                                  │
│       ▼                                                  │
│  ┌──────────────────────────────────────────────────┐   │
│  │  logfwd-core                                     │   │
│  │  Pure logic, proven, no_std, forbid(unsafe)      │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

Dependencies flow downward. logfwd-core knows nothing about Arrow,
IO, or async. logfwd-arrow bridges core's parsing to Arrow types.
The binary crate wires everything together.

## Data flow

### 1. Reading: bytes enter the system

```
Disk → FileTailer → InputEvent::Data { bytes: Vec<u8> }
```

**FileTailer** (`logfwd-core/src/tail.rs`) watches log files via
OS notifications (kqueue/inotify) with polling as fallback. It reads
raw bytes at arbitrary boundaries — a read may split a line in half.

Each input source runs on its own OS thread. Reads feed a bounded
`tokio::sync::mpsc` channel to the async pipeline loop.

**Target:** Replace `Vec<u8>` with `bytes::Bytes` (#303). The reader
owns a `BytesMut` per file, reads into it, then `freeze()` produces
a refcounted `Bytes`. This buffer stays alive through the entire
pipeline — everything downstream references it instead of copying.

### 2. Framing: raw bytes → complete lines

```
Vec<u8> → FramedInput::poll() → newline-delimited JSON in buffer
```

**FramedInput** (`logfwd-io/src/framed.rs`) combines format detection
with per-source remainder tracking. It handles three formats:

- **Json**: Lines are already JSON. Splits on `\n`, carries
  partial lines across reads via per-source remainder buffers.
- **Cri**: Kubernetes container log format. Parses timestamp,
  stream, flags, message. Reassembles P (partial) lines into complete
  messages. Injects `_timestamp` and `_stream` as JSON fields.
- **Raw**: Wraps each line as `{"_raw":"<escaped>"}`.

**NewlineFramer** (`logfwd-core/src/framer.rs`): fixed-size
output (4096 lines, 64KB stack), no heap, Kani-proven. It returns byte
ranges into the input buffer — zero-copy.

**CriAggregator** (`logfwd-core/src/aggregator.rs`):
zero-copy for F-only lines (99% of traffic), copies only for P+F
reassembly. Kani-proven.

**Target:** Replace the copy-based framing with a layered zero-copy model (#303):

```
Bytes → StructuralIndex::new(buf)     [ONE SIMD pass, all characters]
      → NewlineFramer                  [consumes \n bitmask → line ranges]
      → CriAggregator (if CRI format) [merges P/F partials]
      → Scanner                        [consumes remaining bitmasks]
```

### 3. Structural classification: SIMD pre-pass

```
&[u8] → ChunkIndex::new(buf) → bitmasks for O(1) lookups
```

**ChunkIndex** (`logfwd-core/src/chunk_classify.rs`) is the simdjson
stage-1 algorithm. It processes the entire buffer in 64-byte blocks,
producing bitmasks for quote and backslash positions. From these it
computes:

- **real_quotes**: unescaped quote positions (escaped quotes filtered out)
- **in_string**: which bytes are inside JSON strings vs structural

The scanner uses these for O(1) lookups instead of byte-by-byte
scanning: `scan_string()` jumps from opening quote to closing quote,
`skip_nested()` skips objects/arrays using the string mask.

Platform-specific SIMD:
- **aarch64**: NEON (4 × 16-byte loads, vpaddl reduction)
- **x86_64**: AVX2 preferred (2 × 32-byte loads), SSE2 fallback
- **other**: scalar loop (LLVM auto-vectorizes)

**Target:** Extend to **StructuralIndex** detecting 9 characters in
one pass (#313): `\n`, space, `"`, `\`, `,`, `:`, `{`, `}`, `[`.
Benchmark shows linear scaling at ~28µs per character (NEON). At 9
characters: 256µs for 760KB buffer (3 GiB/s), 11x headroom over
target throughput. This eliminates separate newline scanning and
enables bitmask-based CRI field extraction and future CSV support.

### 4. Scanning: JSON → typed fields

```
&[u8] + ChunkIndex → ScanBuilder callbacks → typed column values
```

**scan_into** (`logfwd-core/src/scanner.rs`) walks each line
left-to-right, extracting key-value pairs. It uses ChunkIndex for
string boundary detection and the **ScanBuilder** trait for output:

```rust
pub trait ScanBuilder {
    fn begin_row(&mut self);
    fn end_row(&mut self);
    fn resolve_field(&mut self, key: &[u8]) -> usize;
    fn append_str_by_idx(&mut self, idx: usize, value: &[u8]);
    fn append_int_by_idx(&mut self, idx: usize, value: &[u8]);
    fn append_float_by_idx(&mut self, idx: usize, value: &[u8]);
    fn append_null_by_idx(&mut self, idx: usize);
    fn append_raw(&mut self, line: &[u8]);
}
```

This is the **key abstraction boundary**. The scanner lives in
logfwd-core (proven, no_std). It doesn't know about Arrow — it
calls trait methods that logfwd-arrow implements.

**ScanConfig** controls which fields to extract (field pushdown from
SQL analysis) and type detection. Fields are typed per-value: when a
field has a single type across all rows it gets a bare column name
(`status` as `Int64`). When a field has mixed types, the builder emits
a `StructArray` conflict column (`status: Struct { int, str }`).

### 5. Building: typed fields → Arrow RecordBatch

```
ScanBuilder callbacks → StreamingBuilder/StorageBuilder → RecordBatch
```

Two ScanBuilder implementations in logfwd-arrow:

**StreamingBuilder** (`logfwd-arrow/src/streaming_builder.rs`):
Hot path. Stores `(offset, len)` views into the input `bytes::Bytes`
buffer. Produces `StringViewArray` — Arrow's zero-copy string type
that references the original buffer. No string data is copied.

**StorageBuilder** (`logfwd-arrow/src/storage_builder.rs`):
Persistence path. Copies values into owned buffers. Supports
dictionary encoding for repeated values. Used when the RecordBatch
must outlive the input buffer (disk queue, Parquet write).

Both produce `RecordBatch` via `finish_batch()`.

**Scanner wrappers** in logfwd-arrow combine classification + scanning
+ building into one call:

- `SimdScanner::scan(&[u8]) → RecordBatch` (StorageBuilder, copies)
- `StreamingSimdScanner::scan(Bytes) → RecordBatch` (StreamingBuilder,
  zero-copy)

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

### 7. Output: RecordBatch → wire format

```
RecordBatch → OutputSink::send_batch() → HTTP/stdout/file
```

**OutputSink** (`logfwd-output/src/lib.rs`) implementations:

- **OtlpSink**: Encodes RecordBatch → OTLP protobuf, sends via HTTP.
  Handles resource attributes, observed timestamps, DataType-based dispatch.
- **ElasticsearchAsyncSink**: Bulk API with retry, compression, async reqwest.
- **LokiAsyncSink**: Loki push API with label grouping, dedup, async reqwest.
- **JsonLinesSink**: Converts RecordBatch → newline-delimited JSON,
  sends via HTTP with zstd compression.
- **StdoutSink**: Renders to terminal (JSON, console, or text format).
- **FanOut**: Sends to multiple sinks.

## Crate boundaries

Each boundary is a trait that logfwd-core defines and other crates
implement:

```
logfwd-core defines          logfwd-arrow implements
──────────────────────────    ──────────────────────────
ScanBuilder                   StreamingBuilder
                              StorageBuilder

logfwd-io defines + implements
──────────────────────────────────────────
InputSource trait              FileInput, TcpInput, UdpInput,
                               OtlpReceiverInput

logfwd-output defines + implements
──────────────────────────────────────────
OutputSink / Sink traits       OtlpSink, ElasticsearchAsyncSink,
                               LokiAsyncSink, JsonLinesSink, StdoutSink
```

The binary crate (`logfwd`) wires these together in `pipeline.rs`.

## Pipeline loop

The async pipeline in `run_async()` (`logfwd/src/pipeline.rs`):

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
Current (2 unnecessary copies):
  tailer reads → Vec<u8> [COPY 1: read_buf → fresh Vec]
  parser accumulates → json_buf [COPY 2: line → json_buf]
  scanner classifies → ChunkIndex (bitmasks, no copy)
  StreamingBuilder stores views → RecordBatch (zero-copy)

Target (zero-copy for 99% path):
  tailer reads → BytesMut (reusable per file, no alloc)
  freeze() → Bytes (refcounted, zero-copy)
  StructuralIndex classifies (bitmasks, no copy)
  NewlineFramer returns ranges (no copy)
  CriAggregator: F lines pass through (no copy)
                 P+F lines concat (one copy, ~1% of traffic)
  StreamingBuilder stores views → RecordBatch (zero-copy)
  Bytes dropped when RecordBatch is consumed
```

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
- [Roadmap (GitHub issue #889)](https://github.com/strawgate/memagent/issues/889) — implementation phases with issue references

# Architecture

## Data flow

```
Log files
    │
    ▼
FileTailer (notify + read)
    │
    ▼
Format Parser (CRI / JSON / Raw)
    │  strips CRI timestamp/stream prefix, accumulates JSON lines
    ▼
SIMD Scanner (ChunkIndex → scan_into → Builder)
    │  one NEON/SSE pass classifies entire buffer at ~16 GiB/s
    │  then walks structural positions directly into Arrow columns
    ▼
Arrow RecordBatch
    │  typed columns: {field}_int, {field}_float, {field}_str
    ▼
SQL Transform (DataFusion)
    │  user SQL: SELECT, WHERE, GROUP BY + UDFs (grok, regexp_extract, int, float)
    │  enrichment tables available via JOIN (K8s metadata, host info, static labels)
    ▼
Output Sink (OTLP / JSON lines / HTTP / stdout)
```

## Scanner architecture

The scanner is the performance-critical path. It has two stages:

**Stage 1 — Chunk classification** (`chunk_classify.rs`): Process the entire NDJSON buffer in 64-byte SIMD chunks. For each chunk, find all quote and backslash positions using NEON intrinsics, compute an escape-aware real-quote bitmask, and build a string-interior mask. Output: `ChunkIndex` with pre-computed bitmasks. Runs at ~16 GiB/s.

**Stage 2 — Field extraction** (`scanner.rs`): A scalar state machine walks top-level JSON objects. For each field, it resolves the key to an index (HashMap, once per field per batch) and routes the value to the builder via `append_*_by_idx`. String scanning uses the pre-computed `ChunkIndex` for O(1) closing-quote lookup.

The scan loop is generic over the `ScanBuilder` trait:

- **`StorageBuilder`**: collects `(row, value)` records. Builds columns independently at `finish_batch`. Correct by construction. Output is self-contained.
- **`StreamingBuilder`**: stores `(row, offset, len)` views into a `bytes::Bytes` buffer. Builds `StringViewArray` columns with zero copies. 20% faster. Buffer must stay alive.

## Typed column model

| JSON value | Arrow column | Name pattern |
|-----------|-------------|-------------|
| `"hello"` | StringArray / StringViewArray | `{field}_str` |
| `42` | Int64Array | `{field}_int` |
| `3.14` | Float64Array | `{field}_float` |
| `true`/`false` | StringArray (`"true"`/`"false"`) | `{field}_str` |
| `null` | null in all columns | — |
| `{...}` / `[...]` | StringArray (raw JSON) | `{field}_str` |

Type conflicts produce separate columns: `status_int` and `status_str` can coexist.

## Crate map

| Crate | Purpose |
|-------|---------|
| `logfwd` | Binary. CLI, pipeline orchestration, OTel metrics. |
| `logfwd-core` | Scanner, builders, parsers, diagnostics, enrichment, OTLP encoder. |
| `logfwd-config` | YAML config deserialization and validation. |
| `logfwd-transform` | DataFusion SQL. UDFs: `grok()`, `regexp_extract()`, `int()`, `float()`. |
| `logfwd-output` | Output sinks: OTLP, JSON lines, HTTP, stdout. Stubs: Elasticsearch, Loki, Parquet. |
| `logfwd-bench` | Criterion benchmarks. |

## What's implemented vs stub

**Implemented:** file input, CRI/JSON/Raw parsing, SIMD scanner, two builder backends, DataFusion SQL transforms, custom UDFs, enrichment (K8s path, host info, static labels), OTLP output, JSON lines output, stdout output, diagnostics server, OTel metrics.

**Not yet:** TCP/UDP/OTLP input, Elasticsearch/Loki/Parquet output, file offset checkpointing, disk queue, signal handling, graceful shutdown.

## Historical research

Design documents from the v1 prototype: `docs/research/BENCHMARKS_V1.md`, `docs/research/SCANNER_DESIGN_V1.md`.

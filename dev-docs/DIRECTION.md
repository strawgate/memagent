# Direction

logfwd is becoming a generic Arrow-native data processing pipeline.
The core processes Arrow RecordBatches — it doesn't care if the data
is logs, metrics, traces, or CSV files. OTel semantics are optional
adapters at the edges.

## Core principle

**RecordBatch in, RecordBatch out.** The pipeline moves Arrow tables
between receivers, processors, and exporters. Everything else is an
adapter.

## Crate architecture (target)

```
logfwd-core         Proven pure logic. #![no_std], #![forbid(unsafe_code)]
                    Parsing, encoding, pipeline state machine.
                    StructuralIndex consumers (framer, CRI, scanner).
                    Dependencies: memchr, wide (portable SIMD).
                    Every public function has a Kani proof or proptest.

logfwd-arrow        Arrow integration. Implements core's FieldSink trait.
                    StreamingBuilder, StorageBuilder.
                    Bridge between parsed fields and RecordBatch.

logfwd-input        Produces RecordBatch from external sources.
                    File tailer, Arrow IPC reader, OTAP receiver.
                    Raw data goes through core→arrow→RecordBatch.
                    Arrow-native sources skip straight to RecordBatch.

logfwd-transform    RecordBatch → RecordBatch via DataFusion SQL.
                    UDFs, enrichment tables, JOINs.

logfwd-output       Consumes RecordBatch, sends externally.
                    OTLP, Arrow IPC, Parquet, ClickHouse, JSON lines.

logfwd              Async shell. CLI, config, pipeline orchestration,
                    diagnostics, signal handling.
```

## Proven core

logfwd-core is being restructured into a formally verified crate:
- `#![no_std]` prevents IO structurally (compiler enforced, not lints)
- `#![forbid(unsafe_code)]` prevents unsafe
- Kani bounded model checker proves critical functions for ALL inputs
- proptest state-machine tests verify stateful components
- TLA+ specification proves pipeline liveness properties
- CI enforces: every public function needs a proof or test

This is the s2n-quic/rustls pattern. See `dev-docs/PROVEN_CORE.md`.

## Unified SIMD structural scanning

One SIMD pass over the entire buffer detects ALL structural characters
and produces bitmasks. Every downstream consumer (framer, CRI parser,
JSON scanner, future CSV parser) queries pre-computed bitmasks via O(1)
bit operations instead of scanning bytes.

```
Reader → Bytes → StructuralIndex::new(buf) [ONE SIMD pass]
  → newline bitmask   → Framer (line boundaries)
  → space bitmask     → CRI field extraction
  → quote bitmask     → String boundary detection
  → backslash bitmask → Escape handling
  → comma bitmask     → CSV/JSON field delimiters
  → colon bitmask     → JSON key-value pairs
  → brace bitmasks    → Nesting depth tracking
```

Benchmark (2026-03-30, NEON, ~760KB NDJSON):
- 2 chars (current ChunkIndex): 63µs / 12 GiB/s
- 5 chars (unified): 141µs / 5.4 GiB/s
- 9 chars (full structural): 256µs / 3.0 GiB/s

Scaling is linear at ~28µs per character. Adding new format support
(CSV, TSV, syslog) is nearly free — just add a comparison instruction.

Portable SIMD via the `wide` crate lives in logfwd-core. The scalar
`find_structural_chars_scalar` is the Kani-provable specification.
proptest verifies SIMD output matches scalar for random inputs.

## Arrow-native ecosystem

We're targeting interop with systems that speak Arrow natively:

| System | Receive from | Export to |
|--------|:---:|:---:|
| Arrow IPC files (local/S3) | ✓ | ✓ |
| Parquet files (local/S3) | ✓ | ✓ |
| ClickHouse (HTTP ArrowStream) | ✓ | ✓ |
| Arrow Flight (gRPC) | ✓ | ✓ |
| Elasticsearch ES\|QL | ✓ | — |
| OTLP protobuf | ✓ | ✓ |
| OTel Arrow (OTAP) | planned | planned |

## OTAP compatibility

We stay independent of the official otap-dataflow crates (unpublished,
heavy deps, unstable protocol). When we implement OTAP:
- Use the proto file for gRPC stubs
- Reference their schema code as specification
- Implement star-to-flat conversion for receiving
- Implement flat-to-star conversion for sending

## Performance target

1M+ events/sec end-to-end. Key bottleneck is HTTP output — the async
pipeline (#221) overlaps IO with CPU. Next: async HTTP client (#252),
UTF-8 validate once (#205), adaptive dictionary encoding (#70).

## What we're NOT building

- A general-purpose OTel Collector (that's a different project)
- A database or query engine (we use DataFusion)
- A distributed system (we scale by running more instances)

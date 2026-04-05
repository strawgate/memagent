# logfwd-bench — Benchmark Suite

Criterion microbenchmarks and profiling tools for logfwd.

## Quick Start

```bash
# Run Tier 1 Criterion benchmarks (fast, ~30s — pipeline, output_encode, full_chain)
just bench

# Run the dedicated FramedInput profiling report (stage timings + RSS + optional flamegraph)
just bench-framed-input -- --lines 200000 --iterations 5 --flamegraph /tmp/framed-input.svg

# Run the allocation-only FramedInput report (dhat-backed, slower)
just bench-framed-input-alloc -- --lines 200000

# Run all Criterion benchmarks (Tier 1 + 2, includes file_io, batch_formation)
just bench-full

# Run system-level pipeline benchmarks (Tier 3, requires built binary)
just bench-system

# Run a single benchmark group
cargo bench -p logfwd-bench --bench output_encode
cargo bench -p logfwd-bench --bench full_chain
cargo bench -p logfwd-bench --bench file_io
cargo bench -p logfwd-bench --bench batch_formation
cargo bench -p logfwd-bench --bench pipeline

# Generate markdown report from Criterion JSON
just bench-report
```

## Benchmark Tiers

| Tier | What | Speed | Command |
|------|------|-------|---------|
| 1 — Composed Functions | `pipeline`, `output_encode`, `full_chain` — real types, no heavy I/O | Fast (~30s) | `just bench` |
| 2 — I/O + Scaling | `file_io`, `batch_formation`, `builder_compare`, `elasticsearch_arrow` | Medium (~2-5min) | `just bench-full` |
| 3 — System-Level | Full pipeline end-to-end with real networking | Slow (~5-10min) | `just bench-system` |

## Benchmark Files

### Criterion Benches (`benches/`)

| File | Group | What it measures |
|------|-------|-----------------|
| `pipeline.rs` | `scanner`, `cri`, `transform`, `compress`, `output`, `end_to_end`, `elasticsearch` | Original pipeline stage benchmarks |
| `output_encode.rs` | `output_encode` | OTLP protobuf, JSON lines, JSON+zstd encoding cost (narrow vs wide, 1K/10K rows) |
| `full_chain.rs` | `full_chain_json`, `full_chain_cri`, `full_chain_grok` | Composed pipeline chains — reveals hidden handoff overhead |
| `file_io.rs` | `file_io_framing`, `file_io_cri` | Disk read + newline framing + CRI parse throughput |
| `batch_formation.rs` | `batch_scan`, `batch_transform`, `batch_pipeline` | Batch size scaling (100–100K rows) — amortization curve |
| `builder_compare.rs` | `bench_scan`, `bench_persist`, `bench_pipeline` | StringViewArray vs StringArray comparison |
| `elasticsearch_arrow.rs` | *(requires ES)* | ES\|QL Arrow IPC vs JSON |

### Profiling Tools (`src/`)

| File | Binary | What it does |
|------|--------|-------------|
| `main.rs` | `logfwd-bench` | Reads Criterion JSON, emits markdown tables |
| `e2e_profile.rs` | *(lib)* | Per-stage timing breakdown (scan → transform → encode → compress) |
| `es_throughput.rs` | `es-throughput` | Elasticsearch output throughput with worker scaling |
| `bin/framed_input_profile.rs` | `framed_input_profile` | FramedInput stage timings, RSS, optional flamegraph, optional dhat allocation report |
| `explore.rs` | *(lib)* | Multi-dimensional exploratory benchmark (CSV output) |
| `rss.rs` | *(lib)* | Resident set size at each pipeline stage |
| `sizes.rs` | *(lib)* | Data size analysis: raw → Arrow → IPC → Parquet |

## Shared Data Generators

All generators live in `src/generators.rs` and use seeded `fastrand::Rng` for
deterministic, reproducible output.

| Generator | Description | Typical size |
|-----------|-------------|-------------|
| `gen_cri_k8s(count, seed)` | CRI-formatted K8s logs with P/F mix | ~350 bytes/line |
| `gen_production_mixed(count, seed)` | Non-uniform JSON (70% short, 20% medium, 10% long) | ~250 bytes/line avg |
| `gen_narrow(count, seed)` | Simple 5-field JSON | ~130 bytes/line |
| `gen_wide(count, seed)` | Verbose 20+ field JSON | ~650 bytes/line |
| `make_metadata()` | Standard `BatchMetadata` with K8s resource attrs | — |

## Shared Bench Helpers (`src/lib.rs`)

| Helper | Description |
|--------|-------------|
| `NullSink` | Discards all data — measures pure iteration overhead |
| `make_otlp_sink(compression)` | Buffer-only `OtlpSink` (no HTTP) for encode benchmarks |

### Determinism Guarantee

Same `(count, seed)` → byte-identical output across runs, platforms, and CI.

```rust
use logfwd_bench::generators;

let a = generators::gen_cri_k8s(1000, 42);
let b = generators::gen_cri_k8s(1000, 42);
assert_eq!(a, b); // Always true
```

## Key Questions These Benchmarks Answer

1. **Output encode cost**: Which format is cheapest? How does schema width affect encoding?
2. **Composition overhead**: Does the composed pipeline cost more than sum-of-parts?
3. **I/O bottleneck**: Is file read + framing significant vs downstream scan/transform?
4. **Batch amortization**: At what batch size does per-row overhead stabilize?
5. **CRI overhead**: What's the cost of CRI parse + reassemble vs plain JSON?
6. **FramedInput overhead**: How much CPU / memory sits in newline framing, remainder handling, and format processing before the scanner?

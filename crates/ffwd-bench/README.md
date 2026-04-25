# ffwd-bench — Benchmark Suite

Criterion microbenchmarks and profiling tools for ffwd.

## Quick Start

```bash
# Run Tier 1 Criterion benchmarks (fast, ~30s — pipeline, output_encode, full_chain)
just bench

# Run the dedicated FramedInput profiling report (stage timings + RSS + optional flamegraph)
just bench-framed-input -- --lines 200000 --iterations 5 --flamegraph /tmp/framed-input.svg

# Run the CloudTrail realism profile (nested structure + cardinality + compression)
cargo run -p ffwd-bench --release --features bench-tools --bin cloudtrail_profile -- --lines 20000

# Run the allocation-only FramedInput report (dhat-backed, slower)
just bench-framed-input-alloc -- --lines 200000

# Run destination load from the main CLI wrappers (use two terminals)
just bench-devour-otlp
just bench-blast-otlp

# Run OTLP decision-grade stage-separated benchmarks
just bench-otlp-io

# Run OTLP benchmarks with faster local compile settings for iteration
just bench-otlp-io-fast -- --warm-up-time 1 --measurement-time 2 --sample-size 10 otlp_input_decode_materialize

# Profile OTLP decode/E2E CPU with the normal allocator
just profile-otlp-io -- --case attrs-heavy --mode projected_view_decode --iterations 1500 --flamegraph /tmp/otlp-view.svg

# Profile OTLP decode/E2E allocation counts with stats_alloc instrumentation
just profile-otlp-io-alloc -- --case attrs-heavy --iterations 100

# Profile source metadata attachment CPU and allocation counts
cargo run -p ffwd-bench --release --features bench-tools --bin source_metadata_profile -- --rows 50000 --sources 300 --iterations 200

# Run all Criterion benchmarks (Tier 1 + 2, includes file_io, batch_formation)
just bench-full

# Run system-level pipeline benchmarks (Tier 3, requires built binary)
just bench-system

# Run a single benchmark group
cargo bench -p ffwd-bench --bench output_encode
cargo bench -p ffwd-bench --bench otlp_io
cargo bench -p ffwd-bench --bench source_metadata
cargo bench -p ffwd-bench --bench full_chain
cargo bench -p ffwd-bench --bench file_io
cargo bench -p ffwd-bench --bench batch_formation
cargo bench -p ffwd-bench --bench pipeline

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
| `otlp_io.rs` | `otlp_input_parser_only`, `otlp_input_decode_materialize`, `otlp_input_decompress_decode`, `otlp_output_encode_only`, `otlp_output_compression`, `otlp_e2e_in_process`, `otlp_projected_fallback_mix` | OTLP fixture matrix with stage-separated decode, materialize, decompression, encode, compression, fallback mix, and in-process E2E measurements. |
| `source_metadata.rs` | `source_metadata_attach`, `source_metadata_scan_sql`, `source_metadata_raw_rewrite_baseline`, `source_metadata_snapshot_filter` | Post-scan source metadata attachment strategies, source cardinality/interleaving, raw rewrite baseline, and source path snapshot filtering. |
| `full_chain.rs` | `full_chain_json`, `full_chain_cri`, `full_chain_grok` | Composed pipeline chains — reveals hidden handoff overhead |
| `file_io.rs` | `file_io_framing`, `file_io_cri` | Disk read + newline framing + CRI parse throughput |
| `batch_formation.rs` | `batch_scan`, `batch_transform`, `batch_pipeline` | Batch size scaling (100–100K rows) — amortization curve |
| `builder_compare.rs` | `bench_scan`, `bench_persist`, `bench_pipeline` | StringViewArray vs StringArray comparison |
| `elasticsearch_arrow.rs` | *(requires ES)* | ES\|QL Arrow IPC vs JSON |

`otlp_io.rs` keeps fixture synthesis, protobuf encoding, and prost/parity
validation in setup only. The timed regions measure just the named stage:

- `otlp_input_parser_only` times protobuf decode only.
- `otlp_input_decode_materialize` times decode plus Arrow batch materialization.
- `otlp_input_decompress_decode` times zstd request decompression plus decode and Arrow materialization.
- `otlp_output_encode_only` times OTLP serialization from a prebuilt batch.
- `otlp_output_compression` times zstd compression of an already encoded payload.
- `otlp_e2e_in_process` times decode plus encode in one process, without transport or network effects.
- `otlp_projected_fallback_mix` times four projected-success payloads plus one unsupported-but-valid fallback payload.

The suite includes the experimental projected wire-to-Arrow decoder for
primitive fixtures; it is validated against the prost path before benchmark
timing starts. `projected_fallback_*` modes run the candidate production wrapper:
primitive shapes use the projected path, while unsupported-but-valid OTLP shapes
fall back to prost. Mode names are aligned between Criterion and the profile
binary:

| Decode path | Criterion (decode_materialize) | Criterion (e2e) | Profile binary |
|-------------|-------------------------------|-----------------|----------------|
| prost reference | `prost_reference_to_batch` | `prost_reference_to_*_encode` | `prost_reference_to_batch` |
| production current | `production_current_to_batch` | `production_current_to_*_encode` | `production_current_to_batch` |
| projected detached | `projected_detached_to_batch` | `projected_detached_to_*_encode` | `projected_detached_to_batch` |
| projected view | `projected_view_to_batch` | `projected_view_to_*_encode` | `projected_view_to_batch` |
| projected fallback | `projected_fallback_to_batch` | `projected_fallback_to_handwritten_encode` | `projected_fallback_to_batch`, `e2e_projected_fallback` |
| zstd + production current | `zstd_production_current_to_batch` | n/a | `zstd_production_current_to_batch` |
| zstd + projected fallback | `zstd_projected_fallback_to_batch` | n/a | `zstd_projected_fallback_to_batch` |
| projected fallback mix | `projected_success_4x_plus_fallback_1x` | n/a | `projected_fallback_mix` |

**Timing boundaries:**

- `otlp_input_parser_only` — protobuf decode only (no Arrow materialization).
- `otlp_input_decode_materialize` — protobuf decode + Arrow batch construction.
- `otlp_input_decompress_decode` — zstd request decompression + protobuf decode + Arrow batch construction.
- `otlp_output_encode_only` — OTLP serialization from a prebuilt batch.
- `otlp_output_compression` — zstd compression of an already-encoded payload.
- `otlp_e2e_in_process` — decode + encode in one pass, no transport or network.
- `otlp_projected_fallback_mix` — repeated in-process projected fallback decode with a 4:1 projected-success-to-fallback payload mix.

HTTP parsing, request body collection, and receiver channel handoff are covered
by `bin/http_receiver_apples.rs`; `otlp_io.rs` keeps those transport costs out
of the Criterion groups so decode-path comparisons stay stable.

All fixture synthesis, prost/reference parity checks, and encode-path assertions
run in setup before any Criterion timer starts. In the profile binary, warmup
exercises the same code path as the measured loop to pre-size reusable buffers;
allocation counting begins after warmup completes.

### Profiling Tools (`src/`)

| File | Binary | What it does |
|------|--------|-------------|
| `main.rs` | `ffwd-bench` | Reads Criterion JSON, emits markdown tables |
| `e2e_profile.rs` | `e2e_profile` | Per-stage timing breakdown (scan → transform → encode → compress) |
| `bin/cloudtrail_profile.rs` | `cloudtrail_profile` | CloudTrail-like generator profile (NDJSON vs direct RecordBatch generation, cardinality, compression) |
| `es_throughput.rs` | `es-throughput` | Elasticsearch output throughput with worker scaling |
| `bin/framed_input_profile.rs` | `framed_input_profile` | FramedInput stage timings, RSS, optional flamegraph, optional dhat allocation report |
| `bin/otlp_io_profile.rs` | `otlp_io_profile` | Focused OTLP decode and decode→encode CPU flamegraphs and allocation counts. Mode names match Criterion (`prost_reference_to_batch`, `production_current_to_batch`, `projected_detached_to_batch`, `projected_view_to_batch`, `projected_fallback_to_batch`, `zstd_*`, `projected_fallback_mix`, `e2e_*`). |
| `bin/source_metadata_profile.rs` | `source_metadata_profile` | Source metadata column attachment timing and allocation counts for source id, Utf8View path, Utf8 path, and dictionary path variants. |
| `explore.rs` | *(lib)* | Multi-dimensional exploratory benchmark (CSV output) |
| `rss.rs` | *(lib)* | Resident set size at each pipeline stage |
| `sizes.rs` | *(lib)* | Data size analysis: raw → Arrow → IPC → Parquet |

## Shared Data Generators

All generators live in `src/generators.rs` and use seeded `fastrand::Rng` for
deterministic, reproducible output.

Several profiles now share the reusable cardinality helper layer in
`src/cardinality.rs`, which introduces hierarchical identity reuse,
hot/warm/cold phase skew, and renewal windows for more realistic benchmark
payloads without hard-coding a single scenario.

| Generator | Description | Typical size |
|-----------|-------------|-------------|
| `gen_cri_k8s(count, seed)` | CRI-formatted K8s logs with P/F mix | ~350 bytes/line |
| `gen_production_mixed(count, seed)` | Non-uniform JSON with shared cardinality helper (70% short, 20% medium, 10% long) | ~250 bytes/line avg |
| `gen_narrow(count, seed)` | Simple 5-field JSON | ~130 bytes/line |
| `gen_wide(count, seed)` | Verbose 20+ field JSON with hierarchical identity reuse | ~650 bytes/line |
| `gen_envoy_access(count, seed)` | Envoy-like access logs with route/service/status/user-agent/IP correlation | ~500 bytes/line avg |
| `gen_cloudtrail_audit(count, seed)` | CloudTrail-like nested audit logs with optional substructures and high-cardinality principals | ~1.1 KB/line avg |
| `gen_cloudtrail_batch(count, seed)` | Direct Arrow `RecordBatch` materializer for the same CloudTrail workload model | 34 core columns |
| `make_metadata()` | Standard `BatchMetadata` with K8s resource attrs | — |

## Shared Bench Helpers (`src/lib.rs`)

| Helper | Description |
|--------|-------------|
| `NullSink` | Discards all data — measures pure iteration overhead |
| `make_otlp_sink(compression)` | Buffer-only `OtlpSink` (no HTTP) for encode benchmarks |

### Determinism Guarantee

Same `(count, seed)` → byte-identical output across runs, platforms, and CI.

```rust
use ffwd_bench::generators;

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

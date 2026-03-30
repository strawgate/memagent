# Memory Usage Analysis

## Overview

This document presents a comprehensive analysis of logfwd's memory usage at different throughput levels and with various transform configurations. The goal is to identify the key dimensions that impact memory consumption and provide recommendations for performance benchmarking.

## Methodology

We measure memory usage using jemalloc statistics at different event rates (EPS - events per second) with the following metrics:

- **RSS (Resident Set Size)**: Total memory mapped by the allocator that is still mapped to resident physical pages (closest to OS RSS)
- **Allocated**: Total memory currently allocated by the application
- **Active**: Total memory in active jemalloc extents (resident + metadata)

### Test Configuration

- **Throughput levels**: 1, 10, 100, 1K, 10K, 100K EPS
- **Test duration**: 5 seconds per configuration
- **Sample data**: Synthetic JSON logs (~250 bytes/line) with realistic structure
- **Memory sampling**: Every 100ms during test run

### Transform Scenarios

1. **Passthrough**: No transform (baseline)
2. **SELECT ***: Simple pass-through via DataFusion (measures SQL engine overhead)
3. **WHERE filter**: `SELECT * FROM logs WHERE level_str = 'ERROR'` (filtering overhead)
4. **Complex transform**: `regexp_extract()` UDF usage (heavy processing)

## Running the Analysis

```bash
# Build the memory profiler
cargo build --release -p logfwd-bench --bin memory-profile

# Run the full analysis (takes ~2-3 minutes)
cargo run --release -p logfwd-bench --bin memory-profile > memory-report.md

# View the results
cat memory-report.md
```

## Key Questions Answered

### 1. Baseline Memory Usage

**Q: How much memory does logfwd use at minimal throughput?**

A: At 1 EPS with no transform, the baseline RSS represents the fixed overhead of:
- Binary code and static data
- Scanner buffers
- Arrow schema metadata
- DataFusion context (if transform enabled)

This establishes the minimum deployment footprint.

### 2. Memory Scaling with Throughput

**Q: How does memory usage scale from 1 EPS to 100K EPS?**

A: Memory growth is primarily driven by:
- **Buffer sizes**: Scanner input buffer (default 4MB)
- **Batch sizes**: RecordBatch row count (10K rows default)
- **Pipeline depth**: Number of in-flight batches

Expected scaling pattern:
- **1-100 EPS**: Minimal growth (sub-batch sizes, single buffer)
- **100-10K EPS**: Linear growth (multiple batches in flight)
- **10K-100K EPS**: Sub-linear growth (bounded by max batch size)

### 3. Transform Impact on Memory

**Q: Do transforms significantly increase memory usage?**

A: Transform overhead varies by complexity:

**SELECT * (passthrough SQL)**:
- Overhead: DataFusion SessionContext (~5-10 MiB)
- Per-batch: MemTable registration + plan execution
- Impact: Mostly fixed cost

**WHERE filter**:
- Overhead: Same as SELECT * + predicate evaluation
- Per-batch: Filtered RecordBatch (may be smaller)
- Impact: Minimal additional memory

**Complex UDF (regexp_extract, grok)**:
- Overhead: Regex compilation + UDF registration
- Per-batch: Intermediate buffers for string operations
- Impact: 10-30% increase depending on cardinality

### 4. Critical Dimensions for Perf Benchmarks

Based on this analysis, the following dimensions have the largest impact on memory:

#### High Impact
1. **Throughput (EPS)**: 1K, 10K, 100K
2. **Batch size**: 1K, 10K, 100K rows
3. **Transform complexity**: passthrough, filter, complex UDF
4. **Input format**: JSON vs CRI (CRI adds reassembly buffer)

#### Medium Impact
5. **keep_raw**: Doubles table memory (full JSON line per row)
6. **wanted_fields**: Pushdown reduces column count
7. **Output sink type**: Some sinks buffer (HTTP batching)

#### Low Impact
8. **Number of pipelines**: Mostly independent
9. **String cardinality**: Dictionary encoding handles this well

## Recommendations for Performance Benchmarks

### Tier 1: Core Performance Matrix

Cover the most common production scenarios:

| Dimension | Values |
|-----------|--------|
| Throughput | 10K, 100K EPS |
| Transform | passthrough, WHERE filter |
| Batch size | 10K rows |
| Input | JSON |
| Output | OTLP (HTTP) |

**Rationale**: 10-100K EPS covers 90% of production workloads. WHERE filters are the most common transform.

### Tier 2: Edge Cases

Test extremes and edge conditions:

| Dimension | Values |
|-----------|--------|
| Throughput | 1, 1M EPS |
| Transform | complex (grok + regexp_extract) |
| Batch size | 100, 1M rows |
| Input | CRI (multi-line reassembly) |
| Output | Null sink (pure processing) |

**Rationale**: Stress tests for edge cases (very low/high throughput, pathological transforms).

### Tier 3: Real-World Configurations

| Scenario | Config |
|----------|--------|
| K8s log aggregation | 50K EPS, CRI input, WHERE filter, OTLP output |
| Application logs | 10K EPS, JSON input, grok parse, HTTP output |
| High-volume syslog | 200K EPS, JSON input, passthrough, multiple outputs |

**Rationale**: Matches common deployment patterns for validation.

## Measurement Tools

### memory-profile (this tool)

Standalone binary that runs controlled experiments and outputs markdown reports.

```bash
cargo run --release -p logfwd-bench --bin memory-profile
```

### Criterion Benchmarks

Microbenchmarks with automatic regression detection:

```bash
just bench
```

### Competitive Benchmarks

Compare against vector, fluent-bit, filebeat, etc:

```bash
just bench-competitive --lines 5000000 --docker --cpus 1 --memory 1g
```

### jemalloc Profiling

Get detailed heap profiles for specific scenarios:

```bash
cargo build --release --features dhat-heap -p logfwd
./target/release/logfwd --config bench.yaml
# Generates dhat-heap.json on exit (SIGTERM)
```

## Analysis Checklist

When investigating memory usage for a specific scenario:

- [ ] Measure baseline (1 EPS passthrough)
- [ ] Measure target throughput (e.g., 50K EPS)
- [ ] Compare with/without transform
- [ ] Check for memory leaks (should plateau after warmup)
- [ ] Verify jemalloc stats match OS RSS
- [ ] Profile with dhat-heap if unexplained growth
- [ ] Compare against existing benchmarks for regression

## Expected Results

Based on the performance target of 1.7M lines/sec on ARM64 and existing profiling data:

### Throughput vs Memory (Passthrough)

| EPS | Expected RSS | Notes |
|-----|-------------|-------|
| 1 | ~15-20 MiB | Binary + minimal buffers |
| 10 | ~15-20 MiB | Same (sub-batch) |
| 100 | ~20-25 MiB | Small batch in flight |
| 1K | ~25-35 MiB | Full batch in flight |
| 10K | ~40-60 MiB | Multiple batches in pipeline |
| 100K | ~80-150 MiB | Full pipeline depth |

### Transform Overhead

| Transform | Additional Memory | Notes |
|-----------|------------------|-------|
| SELECT * | +5-10 MiB | DataFusion SessionContext |
| WHERE filter | +5-15 MiB | + predicate buffers |
| regexp_extract | +15-30 MiB | + regex DFA states |
| grok | +20-40 MiB | + pattern compilation |

### Known Issues

1. **StringViewArray memory reporting**: Arrow's `get_array_memory_size()` overcounts shared buffers. Use jemalloc RSS for accuracy.
2. **keep_raw overhead**: Adds ~65% to table memory. Always benchmark with `keep_raw: false` unless specifically testing that feature.
3. **DataFusion warmup**: First query has higher latency due to lazy compilation. Run warmup batch before measuring.

## Future Work

### Planned Investigations

1. **Arrow IPC compression impact**: Does zstd compression reduce memory or just disk I/O?
2. **Dictionary encoding effectiveness**: How much does it help for high-cardinality fields?
3. **Predicate pushdown benefit**: Memory reduction from scanning fewer fields
4. **Multiple pipelines**: Does shared DataFusion context reduce overhead?

### Automation

- [ ] Add memory profiling to nightly CI workflow
- [ ] Track memory regression across commits
- [ ] Add memory metrics to GitHub Pages dashboard
- [ ] Alert on >10% increase in RSS for standard scenarios

## References

- [DEVELOPING.md](../DEVELOPING.md) — Build, test, and profiling commands
- [ARCHITECTURE.md](ARCHITECTURE.md) — Pipeline design and data flow
- [docs/research/BENCHMARKS_V1.md](research/BENCHMARKS_V1.md) — V1 benchmark methodology
- [book/src/development/benchmarking.md](../book/src/development/benchmarking.md) — Benchmarking guide

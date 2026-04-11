---
title: "Performance"
description: "Benchmarks, throughput targets, and optimization techniques"
---

## Throughput targets

logfwd targets 1M+ lines/sec on a single CPU core with CRI parsing and
OTLP encoding.

## Benchmark results

| Stage | Time (100K lines) | % of total |
|-------|-------------------|-----------|
| Scan (JSON→Arrow) | 21ms | 57% |
| Transform (SQL) | ~0ms | ~0% |
| OTLP encode | 9ms | 27% |
| zstd compress | 6ms | 16% |
| **Total CPU** | **36ms** | **2.8M lines/sec** |

## Key optimizations

- **SIMD structural indexing**: Classifies JSON structure in one vectorized pass
- **Zero-copy StringViewArray**: No string copies in the hot path
- **Field pushdown**: Scanner skips unused fields based on SQL analysis
- **Persistent zstd context**: Compression context reused across batches
- **Connection pooling**: HTTP agent reused for output requests
- **Block-in-place output**: Overlaps I/O with scanning via tokio

## Memory profile

At our default 4MB batch size (~23K lines):
- Arrow RecordBatch overhead: ~2MB
- Input buffer: 4MB (shared with StringView columns)
- Total per batch: ~6MB

For 1M lines (stress test only):
- Real RSS: ~205MB (not 926MB as `get_array_memory_size()` reports)
- StringViewArray shares the input buffer across all string columns

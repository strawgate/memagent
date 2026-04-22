---
title: "Performance"
description: "Benchmarks, throughput targets, and optimization techniques"
---

:::tip[Performance at a glance]
FastForward processes **~2.8 million log lines per second** on a single CPU core — parsing JSON, running SQL, encoding OTLP protobuf, and compressing with zstd. Total CPU time: 36ms per 100K lines.
:::

## Benchmark results

| Stage | Time (100K lines) | % of total |
|-------|-------------------|-----------|
| Scan (JSON to Arrow) | 21ms | 57% |
| Transform (SQL) | ~0ms | ~0% |
| OTLP encode | 9ms | 27% |
| zstd compress | 6ms | 16% |
| **Total CPU** | **36ms** | **2.8M lines/sec** |

The scanner dominates at 57% of CPU time. This is expected — parsing JSON structure is the hard work. The SQL transform is essentially free because DataFusion operates on Arrow columnar data that's already in the right format.

## Why it's fast

Each optimization compounds on the others:

- **SIMD structural indexing** — One vectorized pass classifies 10 JSON characters across 64 bytes simultaneously. Every subsequent string lookup is O(1) via bitmask + trailing_zeros. This is the same technique that powers simdjson.
- **Zero-copy StringViewArray** — String data is never copied during scanning. 16-byte views point directly into the input buffer, shared via reference counting. Five string columns sharing one buffer use 1x memory, not 5x.
- **Field pushdown** — FastForward analyzes your SQL query before scanning and only extracts referenced columns. If your query uses 3 of 20 fields, the scanner skips the other 17 — giving 2-3x throughput on wide data.
- **Persistent zstd context** — The compression dictionary is reused across batches, avoiding re-initialization overhead.
- **Connection pooling** — HTTP clients reuse connections for output requests, amortizing TLS handshake and TCP setup.

## Memory profile

At the default 4 MB batch size (~23K lines):

| Component | Memory |
|-----------|--------|
| Input buffer | 4 MB (shared with StringView columns) |
| Arrow RecordBatch | ~2 MB |
| **Total per batch** | **~6 MB** |

For stress tests at 1M lines:
- Real RSS: ~205 MB
- `get_array_memory_size()` reports ~926 MB — this overcounts because StringViewArray shares the backing buffer across all string columns

:::note
Memory usage scales with batch size, not total data volume. FastForward processes data in streaming batches and releases memory after each batch completes.
:::

## Scanner throughput by data shape

| Dataset | Fields | Throughput |
|---------|--------|-----------|
| Narrow (3 fields) | 3 | 3.4M lines/sec |
| Simple (6 fields) | 6 | 2.0M lines/sec |
| Wide (20 fields) | 20 | 560K lines/sec |
| Wide with pushdown (20 fields, 2 projected) | 20 to 2 | 1.4M lines/sec |

Field pushdown recovers most of the throughput loss from wide data. If your SQL only references 2 columns from 20-field logs, you get 1.4M lines/sec instead of 560K.

## What's next

| Topic | Where to go |
|-------|-------------|
| See how the scanner works | [Scanner Deep Dive](/learn/scanner/) (interactive) |
| Configure SQL transforms | [SQL Transforms](/configuration/sql-transforms/) |
| Deploy to production | [Kubernetes DaemonSet](/deployment/kubernetes/) |
| Contribute optimizations | [Contributing](https://github.com/strawgate/fastforward/blob/main/CONTRIBUTING.md) |

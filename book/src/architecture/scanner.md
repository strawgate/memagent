# Zero-Copy Scanner

The scanner converts newline-delimited JSON into Apache Arrow RecordBatches
using SIMD-accelerated structural classification.

## How it works

1. **Stage 1 (SIMD)**: Classify the entire buffer in one pass. Find all quotes
   and backslashes using platform-specific SIMD: AVX2/SSE2 on x86_64, NEON on
   aarch64. This produces 64-bit bitmasks for O(1) string boundary lookups.

2. **Stage 2 (Scalar)**: Walk the JSON structure using the pre-computed bitmasks.
   Extract field names and values, detect types (int/float/string), and build
   Arrow columns directly.

## Zero-copy mode

`Scanner` uses Arrow's `StringViewArray` to create 16-byte views
into the input buffer. String data is never copied — the original input buffer
is shared via reference counting.

## Field pushdown

The SQL transform is analyzed before scanning. If the query only references
`level` and `message`, the scanner skips extracting all other fields. On
wide data (20+ fields), this provides 2-3x throughput improvement.

## Performance

| Dataset | Fields | Throughput |
|---------|--------|-----------|
| Narrow (3 fields) | 3 | 3.4M lines/sec |
| Simple (6 fields) | 6 | 2.0M lines/sec |
| Wide (20 fields) | 20 | 560K lines/sec |
| Wide (2 fields projected) | 20→2 | 1.4M lines/sec |

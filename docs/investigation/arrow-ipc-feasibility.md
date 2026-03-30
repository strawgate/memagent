# Arrow IPC Feasibility Investigation

**Status:** Confirmed feasible — all success criteria met.
**Phase:** Architecture spec Phase 0 (DiskQueue spike).

## Summary

Arrow IPC `FileWriter` / `FileReader` (arrow-rs v54 with `ipc_compression`) is the
right serialisation primitive for the DiskQueue segment format.

Roundtrip is bit-identical. Zstd level-1 compression at 7.4× ratio far exceeds the
< 0.3× ratio target. Write and read throughputs (in-memory, release build, x86-64) are
well above the thresholds required for 1 M+ rows/sec sustained ingestion.

## Approach

Standalone example in `crates/logfwd-core/examples/arrow_ipc_roundtrip.rs`:

1. Generate 30 000 K8s-style log rows via `StorageBuilder` → `RecordBatch` (~5 MB IPC).
2. Serialise to Arrow IPC **file format** (footer gives random-access batch offsets needed for mmap replay).
3. Compress with zstd via `IpcWriteOptions::try_with_compression(Some(CompressionType::ZSTD))`.
4. Deserialise via `FileReader` (sequential) and via `memmap2::Mmap` (mmap).
5. Assert bit-identical roundtrip (`ArrayData` equality for every column).
6. Report throughput and compression ratio.

## Results (release build, x86-64 Linux)

| Metric                    | Result     | Target     | Status |
|---------------------------|-----------|------------|--------|
| Roundtrip correctness     | bit-identical | exact match | ✓ PASS |
| IPC uncompressed size     | 5.35 MB   | ~4 MB      | ✓      |
| IPC zstd size             | 0.72 MB   | —          | ✓      |
| Compression ratio         | 0.135     | < 0.3      | ✓ PASS |
| Write throughput (uncmp)  | 1 458 MB/s | > 500 MB/s | ✓ PASS |
| Write throughput (zstd)   | 758 MB/s  | —          | ✓      |
| Read sequential (uncmp)   | 3 207 MB/s | —          | ✓      |
| Read mmap (uncmp)         | 3 058 MB/s | > 1 GB/s   | ✓ PASS |

## Design Decisions

### Use `FileWriter` (not `StreamWriter`)

`FileWriter` writes a footer with per-batch record-count and byte-offset metadata.
This enables random-access replay: a reader can seek directly to any batch without
scanning the whole file — essential for the DiskQueue replay path.

`StreamWriter` produces a streaming format with no footer; every read must be
sequential from the start. Not suitable for mmap-based batch replay.

### Segment design

- One segment = one Arrow IPC file, target size 64 MB.
- Write-to-`.tmp` → `fsync` → atomic `rename` (POSIX guarantees).
- The file footer provides the batch-count for acknowledgement tracking.

### Compression

`CompressionType::ZSTD` (level controlled by Arrow's default, effectively level 1)
achieves 7.4× compression on structured K8s log data. The `ipc_compression` feature
is already declared in the workspace `Cargo.toml`, so no new dependency is required.

Arrow IPC compresses each column buffer independently, which is better than
compressing the whole IPC stream: it preserves seekability and enables partial
column decompression.

### `memmap2` for replay

`memmap2` is already a transitive dependency (via `arrow`). Adding it directly to
`[dev-dependencies]` in `logfwd-core` exposes it to the example and to future
integration tests without adding it to the production binary.

For the read path the OS page cache handles the heavy lifting — the ~3 GB/s
throughput reflects warm-cache performance typical of NVMe-backed storage.

## Reproduction

```bash
cargo run --example arrow_ipc_roundtrip --release -p logfwd-core
```

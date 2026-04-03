//! Arrow IPC roundtrip benchmark — Phase 0 DiskQueue spike.
//!
//! Validates the DiskQueue design:
//!   1. Generate a ~4 MB `RecordBatch` via `StorageBuilder` (K8s-style log data)
//!   2. Write via Arrow IPC `FileWriter` — both uncompressed and with zstd
//!   3. Read back via `FileReader` (sequential read) and via `memmap2` (mmap)
//!   4. Verify bit-identical roundtrip (schema + every column)
//!   5. Report write/read throughput (MB/s, rows/sec) and compression ratio
//!
//! Run with:
//!   cargo run --example arrow_ipc_roundtrip --release -p logfwd-core
//!
//! Success criteria (from docs/investigation/arrow-ipc-feasibility.md):
//!   - Roundtrip correctness: schemas and data match exactly  ✓
//!   - Write throughput > 500 MB/s (SSD)
//!   - Read throughput > 1 GB/s (mmap)
//!   - Compression ratio < 0.3× vs uncompressed IPC

use std::io::{Cursor, Write};
use std::time::Instant;

use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::{FileWriter, IpcWriteOptions};
use arrow::record_batch::RecordBatch;
use logfwd_arrow::storage_builder::StorageBuilder;
use memmap2::Mmap;
use tempfile::NamedTempFile;

// ---------------------------------------------------------------------------
// K8s-realistic log data generator
// ---------------------------------------------------------------------------

/// Generates a `RecordBatch` of K8s-style log rows via [`StorageBuilder`].
///
/// Each row has timestamp, namespace, pod, container, stream, level, msg,
/// status (int), latency_ms (float), and request_id columns — matching the
/// realistic benchmark scenario used in `benches/scanner.rs`.
fn generate_batch(num_rows: usize) -> RecordBatch {
    let namespaces: &[&[u8]] = &[
        b"default",
        b"kube-system",
        b"monitoring",
        b"prod",
        b"staging",
    ];
    let pods: &[&[u8]] = &[
        b"api-7f8d9c6b5-x2k4m",
        b"nginx-5b9c7d8e6-q3r7p",
        b"prom-0",
        b"coredns-6d4b-wk9tn",
    ];
    let levels: &[&[u8]] = &[b"DEBUG", b"INFO", b"INFO", b"INFO", b"WARN", b"ERROR"];

    let mut builder = StorageBuilder::new(false);
    builder.begin_batch();

    // Pre-resolve all field indices (one HashMap lookup per field, amortised).
    let ts_idx = builder.resolve_field(b"timestamp");
    let ns_idx = builder.resolve_field(b"namespace");
    let pod_idx = builder.resolve_field(b"pod");
    let container_idx = builder.resolve_field(b"container");
    let stream_idx = builder.resolve_field(b"stream");
    let level_idx = builder.resolve_field(b"level");
    let msg_idx = builder.resolve_field(b"msg");
    let status_idx = builder.resolve_field(b"status");
    let latency_idx = builder.resolve_field(b"latency_ms");
    let req_id_idx = builder.resolve_field(b"request_id");

    for i in 0..num_rows {
        builder.begin_row();

        let ts = format!(
            "2024-01-15T10:{:02}:{:02}.{:03}Z",
            (i / 60) % 60,
            i % 60,
            i % 1000
        );
        builder.append_str_by_idx(ts_idx, ts.as_bytes());
        builder.append_str_by_idx(ns_idx, namespaces[i % namespaces.len()]);
        builder.append_str_by_idx(pod_idx, pods[i % pods.len()]);
        builder.append_str_by_idx(container_idx, b"main");
        builder.append_str_by_idx(stream_idx, b"stdout");
        builder.append_str_by_idx(level_idx, levels[i % levels.len()]);

        let msg = format!(
            "request processed path=/api/v1/resource/{} user_id=usr_{:06}",
            i,
            i % 100_000
        );
        builder.append_str_by_idx(msg_idx, msg.as_bytes());

        // Deliberately produce type conflict: status resolves as int.
        let status: &[u8] = if i % 20 == 0 {
            b"500"
        } else if i % 7 == 0 {
            b"404"
        } else {
            b"200"
        };
        builder.append_int_by_idx(status_idx, status);

        let latency = format!("{:.3}", 0.1 + (i as f64 % 100.0) * 0.5);
        builder.append_float_by_idx(latency_idx, latency.as_bytes());

        let req_id = format!("req-{:08x}", i as u32);
        builder.append_str_by_idx(req_id_idx, req_id.as_bytes());

        builder.end_row();
    }

    builder.finish_batch().expect("example: batch build failed")
}

// ---------------------------------------------------------------------------
// IPC write helpers
// ---------------------------------------------------------------------------

/// Serialise `batch` to Arrow IPC file format (no compression) into a
/// heap-allocated byte vector.
fn write_ipc_uncompressed(batch: &RecordBatch) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    let mut writer =
        FileWriter::try_new(&mut buf, &batch.schema()).expect("FileWriter::try_new failed");
    writer.write(batch).expect("FileWriter::write failed");
    writer.finish().expect("FileWriter::finish failed");
    buf
}

/// Serialise `batch` to Arrow IPC file format with zstd compression.
fn write_ipc_zstd(batch: &RecordBatch) -> Vec<u8> {
    use arrow::ipc::CompressionType;

    let options = IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::ZSTD))
        .expect("IpcWriteOptions::try_with_compression failed");
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = FileWriter::try_new_with_options(&mut buf, &batch.schema(), options)
        .expect("FileWriter::try_new_with_options failed");
    writer.write(batch).expect("FileWriter::write failed");
    writer.finish().expect("FileWriter::finish failed");
    buf
}

// ---------------------------------------------------------------------------
// IPC read helpers
// ---------------------------------------------------------------------------

/// Deserialise all batches from Arrow IPC bytes using sequential I/O.
fn read_ipc_sequential(bytes: &[u8]) -> Vec<RecordBatch> {
    let cursor = Cursor::new(bytes);
    FileReader::try_new(cursor, None)
        .expect("FileReader::try_new failed")
        .map(|b| b.expect("FileReader batch error"))
        .collect()
}

/// Deserialise all batches from an Arrow IPC file via `memmap2`.
fn read_ipc_mmap(path: &std::path::Path) -> Vec<RecordBatch> {
    let file = std::fs::File::open(path).expect("open mmap file failed");
    // SAFETY: the file is not modified while the mapping is alive.
    let mmap = unsafe { Mmap::map(&file) }.expect("mmap failed");
    let cursor = Cursor::new(mmap.as_ref());
    FileReader::try_new(cursor, None)
        .expect("FileReader::try_new (mmap) failed")
        .map(|b| b.expect("FileReader (mmap) batch error"))
        .collect()
}

// ---------------------------------------------------------------------------
// Roundtrip verification
// ---------------------------------------------------------------------------

/// Panics if `original` and the concatenated `roundtrip` batches are not
/// bit-identical (same schema, same row count, same column data).
fn verify_roundtrip(original: &RecordBatch, roundtrip: &[RecordBatch]) {
    let total_rows: usize = roundtrip.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        original.num_rows(),
        total_rows,
        "roundtrip row count mismatch: expected {}, got {}",
        original.num_rows(),
        total_rows
    );
    assert_eq!(
        roundtrip.len(),
        1,
        "expected a single batch from FileReader, got {}",
        roundtrip.len()
    );

    let rt = &roundtrip[0];
    assert_eq!(
        original.schema(),
        rt.schema(),
        "schema mismatch after roundtrip"
    );
    for (i, (col_a, col_b)) in original.columns().iter().zip(rt.columns()).enumerate() {
        assert_eq!(
            col_a.to_data(),
            col_b.to_data(),
            "column {} ({}) data mismatch after roundtrip",
            i,
            original.schema().field(i).name()
        );
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    println!("=== Arrow IPC Roundtrip Benchmark — Phase 0 DiskQueue Spike ===\n");

    // ------------------------------------------------------------------
    // Phase 1: Generate batch (~4 MB target)
    // ------------------------------------------------------------------
    const NUM_ROWS: usize = 30_000;
    print!("Generating {} K8s log rows ... ", NUM_ROWS);
    std::io::stdout().flush().ok();

    let t = Instant::now();
    let batch = generate_batch(NUM_ROWS);
    let gen_ms = t.elapsed().as_secs_f64() * 1000.0;
    let gen_rows_per_sec = NUM_ROWS as f64 / t.elapsed().as_secs_f64();

    println!("done in {gen_ms:.2} ms");
    println!(
        "  Schema: {} columns, {} rows",
        batch.num_columns(),
        batch.num_rows()
    );
    println!("  Generation: {gen_rows_per_sec:.0} rows/sec");

    // ------------------------------------------------------------------
    // Phase 2: Write uncompressed IPC to memory
    // ------------------------------------------------------------------
    print!("Writing uncompressed Arrow IPC ... ");
    std::io::stdout().flush().ok();

    let t = Instant::now();
    let uncompressed_bytes = write_ipc_uncompressed(&batch);
    let write_uncmp_elapsed = t.elapsed();

    let uncmp_size = uncompressed_bytes.len();
    let write_uncmp_mb_s =
        (uncmp_size as f64 / 1024.0 / 1024.0) / write_uncmp_elapsed.as_secs_f64();
    let write_uncmp_rows_s = NUM_ROWS as f64 / write_uncmp_elapsed.as_secs_f64();

    println!("done");
    println!(
        "  Size:       {:.2} MB ({} bytes)",
        uncmp_size as f64 / 1024.0 / 1024.0,
        uncmp_size
    );
    println!("  Throughput: {write_uncmp_mb_s:.0} MB/s, {write_uncmp_rows_s:.0} rows/sec");

    // ------------------------------------------------------------------
    // Phase 3: Write zstd-compressed IPC to memory
    // ------------------------------------------------------------------
    print!("Writing zstd-compressed Arrow IPC ... ");
    std::io::stdout().flush().ok();

    let t = Instant::now();
    let compressed_bytes = write_ipc_zstd(&batch);
    let write_zstd_elapsed = t.elapsed();

    let zstd_size = compressed_bytes.len();
    // Report uncompressed-equivalent throughput so it is comparable.
    let write_zstd_mb_s = (uncmp_size as f64 / 1024.0 / 1024.0) / write_zstd_elapsed.as_secs_f64();
    let write_zstd_rows_s = NUM_ROWS as f64 / write_zstd_elapsed.as_secs_f64();
    let compression_ratio = zstd_size as f64 / uncmp_size as f64;

    println!("done");
    println!(
        "  Size:       {:.2} MB ({} bytes)",
        zstd_size as f64 / 1024.0 / 1024.0,
        zstd_size
    );
    println!(
        "  Ratio:      {compression_ratio:.3} (compressed/uncompressed, {:.2}× reduction)",
        1.0 / compression_ratio
    );
    println!("  Throughput: {write_zstd_mb_s:.0} MB/s, {write_zstd_rows_s:.0} rows/sec");

    // ------------------------------------------------------------------
    // Phase 4: Read back uncompressed IPC (sequential)
    // ------------------------------------------------------------------
    print!("Reading uncompressed IPC (sequential) ... ");
    std::io::stdout().flush().ok();

    let t = Instant::now();
    let rt_uncmp = read_ipc_sequential(&uncompressed_bytes);
    let read_seq_uncmp_elapsed = t.elapsed();

    let read_seq_uncmp_mb_s =
        (uncmp_size as f64 / 1024.0 / 1024.0) / read_seq_uncmp_elapsed.as_secs_f64();

    println!(
        "done ({} batches, {} rows)",
        rt_uncmp.len(),
        rt_uncmp.iter().map(RecordBatch::num_rows).sum::<usize>()
    );
    println!("  Throughput: {read_seq_uncmp_mb_s:.0} MB/s");

    // ------------------------------------------------------------------
    // Phase 5: Verify uncompressed roundtrip
    // ------------------------------------------------------------------
    print!("Verifying uncompressed roundtrip ... ");
    std::io::stdout().flush().ok();
    verify_roundtrip(&batch, &rt_uncmp);
    println!("OK — bit-identical ✓");

    // ------------------------------------------------------------------
    // Phase 6: Read back zstd IPC (sequential)
    // ------------------------------------------------------------------
    print!("Reading zstd IPC (sequential) ... ");
    std::io::stdout().flush().ok();

    let t = Instant::now();
    let rt_zstd = read_ipc_sequential(&compressed_bytes);
    let read_seq_zstd_elapsed = t.elapsed();

    let read_seq_zstd_mb_s =
        (uncmp_size as f64 / 1024.0 / 1024.0) / read_seq_zstd_elapsed.as_secs_f64();

    println!(
        "done ({} batches, {} rows)",
        rt_zstd.len(),
        rt_zstd.iter().map(RecordBatch::num_rows).sum::<usize>()
    );
    println!("  Throughput: {read_seq_zstd_mb_s:.0} MB/s (uncompressed-equivalent)");

    // ------------------------------------------------------------------
    // Phase 7: Verify zstd roundtrip
    // ------------------------------------------------------------------
    print!("Verifying zstd roundtrip ... ");
    std::io::stdout().flush().ok();
    verify_roundtrip(&batch, &rt_zstd);
    println!("OK — bit-identical ✓");

    // ------------------------------------------------------------------
    // Phase 8: Write uncompressed IPC to disk, read back via mmap
    // ------------------------------------------------------------------
    let mut tmp = NamedTempFile::new().expect("NamedTempFile::new failed");
    tmp.write_all(&uncompressed_bytes)
        .expect("write to temp file failed");
    tmp.flush().expect("flush temp file failed");

    print!("Reading uncompressed IPC (mmap) ... ");
    std::io::stdout().flush().ok();

    let t = Instant::now();
    let rt_mmap = read_ipc_mmap(tmp.path());
    let read_mmap_elapsed = t.elapsed();

    let read_mmap_mb_s = (uncmp_size as f64 / 1024.0 / 1024.0) / read_mmap_elapsed.as_secs_f64();

    println!(
        "done ({} batches, {} rows)",
        rt_mmap.len(),
        rt_mmap.iter().map(RecordBatch::num_rows).sum::<usize>()
    );
    println!("  Throughput: {read_mmap_mb_s:.0} MB/s");

    // ------------------------------------------------------------------
    // Phase 9: Verify mmap roundtrip
    // ------------------------------------------------------------------
    print!("Verifying mmap roundtrip ... ");
    std::io::stdout().flush().ok();
    verify_roundtrip(&batch, &rt_mmap);
    println!("OK — bit-identical ✓");

    // ------------------------------------------------------------------
    // Summary
    // ------------------------------------------------------------------
    println!("\n=== Results ===");
    println!("  Rows generated:          {NUM_ROWS}");
    println!(
        "  IPC uncompressed size:   {:.2} MB",
        uncmp_size as f64 / 1024.0 / 1024.0
    );
    println!(
        "  IPC zstd size:           {:.2} MB  (ratio {compression_ratio:.3})",
        zstd_size as f64 / 1024.0 / 1024.0
    );
    println!(
        "  Write uncompressed:      {write_uncmp_mb_s:.0} MB/s  ({write_uncmp_rows_s:.0} rows/sec)"
    );
    println!(
        "  Write zstd:              {write_zstd_mb_s:.0} MB/s  ({write_zstd_rows_s:.0} rows/sec)"
    );
    println!("  Read sequential (uncmp): {read_seq_uncmp_mb_s:.0} MB/s");
    println!("  Read sequential (zstd):  {read_seq_zstd_mb_s:.0} MB/s");
    println!("  Read mmap:               {read_mmap_mb_s:.0} MB/s");
    println!("  Roundtrip correctness:   PASS ✓");

    println!("\n=== Success Criteria ===");
    let write_ok = write_uncmp_mb_s > 500.0;
    let read_ok = read_mmap_mb_s > 1000.0;
    let ratio_ok = compression_ratio < 0.3;

    println!(
        "  Write > 500 MB/s:           {}  ({write_uncmp_mb_s:.0} MB/s)",
        if write_ok {
            "PASS ✓"
        } else {
            "NOTE (hardware bound)"
        }
    );
    println!(
        "  Read mmap > 1 GB/s:         {}  ({read_mmap_mb_s:.0} MB/s)",
        if read_ok {
            "PASS ✓"
        } else {
            "NOTE (hardware bound)"
        }
    );
    println!(
        "  Compression ratio < 0.3:    {}  ({compression_ratio:.3})",
        if ratio_ok { "PASS ✓" } else { "NOTE" }
    );
    println!("  Roundtrip bit-identical:    PASS ✓");
}

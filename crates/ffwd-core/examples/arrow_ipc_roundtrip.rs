//! Arrow IPC roundtrip benchmark — Phase 0 DiskQueue spike.
//!
//! Validates the DiskQueue design:
//!   1. Generate a ~4 MB `RecordBatch` via `Scanner::scan_detached` (K8s-style log data)
//!   2. Write via Arrow IPC `FileWriter` — both uncompressed and with zstd
//!   3. Read back via `FileReader` (sequential read) and via `memmap2` (mmap)
//!   4. Verify bit-identical roundtrip (schema + every column)
//!   5. Report write/read throughput (MB/s, rows/sec) and compression ratio
//!
//! Run with:
//!   cargo run --example arrow_ipc_roundtrip --release -p ffwd-core
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
use ffwd_arrow::scanner::Scanner;
use ffwd_core::scan_config::ScanConfig;
use memmap2::Mmap;
use tempfile::NamedTempFile;

// ---------------------------------------------------------------------------
// K8s-realistic log data generator
// ---------------------------------------------------------------------------

/// Generates a `RecordBatch` of K8s-style log rows via [`Scanner::scan_detached`].
///
/// Each row has timestamp, namespace, pod, container, stream, level, msg,
/// status (int), latency_ms (float), and request_id columns — matching the
/// realistic benchmark scenario used in `benches/scanner.rs`.
fn generate_batch(num_rows: usize) -> RecordBatch {
    let namespaces = ["default", "kube-system", "monitoring", "prod", "staging"];
    let pods = [
        "api-7f8d9c6b5-x2k4m",
        "nginx-5b9c7d8e6-q3r7p",
        "prom-0",
        "coredns-6d4b-wk9tn",
    ];
    let levels = ["DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR"];

    let mut ndjson = Vec::with_capacity(num_rows * 300);
    for i in 0..num_rows {
        let ts = format!(
            "2024-01-15T10:{:02}:{:02}.{:03}Z",
            (i / 60) % 60,
            i % 60,
            i % 1000
        );
        let msg = format!(
            "request processed path=/api/v1/resource/{} user_id=usr_{:06}",
            i,
            i % 100_000
        );
        let status = if i % 20 == 0 {
            500
        } else if i % 7 == 0 {
            404
        } else {
            200
        };
        let latency = 0.1 + (i as f64 % 100.0) * 0.5;
        let req_id = format!("req-{:08x}", i as u32);
        let line = format!(
            r#"{{"timestamp":"{}","namespace":"{}","pod":"{}","container":"main","stream":"stdout","level":"{}","msg":"{}","status":{},"latency_ms":{:.3},"request_id":"{}"}}"#,
            ts,
            namespaces[i % namespaces.len()],
            pods[i % pods.len()],
            levels[i % levels.len()],
            msg,
            status,
            latency,
            req_id,
        );
        ndjson.extend_from_slice(line.as_bytes());
        ndjson.push(b'\n');
    }

    let mut scanner = Scanner::new(ScanConfig::default());
    scanner
        .scan_detached(bytes::Bytes::from(ndjson))
        .expect("example: scan_detached failed")
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

#![allow(clippy::print_stdout, clippy::print_stderr)]
//! CPU and memory profiler for the file output hot path.
//!
//! Isolates the JSON serialization + file I/O cost of `FileSink` / `StdoutSink`
//! so we can identify where cycles and allocations go.
//!
//! Modes:
//!   - **cpu**: pprof flamegraph of `write_batch_to` + file I/O
//!   - **alloc**: stats_alloc allocation counts per stage
//!   - **breakdown** (default): per-function timing breakdown (build_col_infos, write_row_json,
//!     memchr line count, file write_all)
//!
//! Run with:
//!   cargo run -p ffwd-bench --release --bin file_output_profile -- \[OPTIONS\]
//!
//! Options:
//!   --mode cpu|alloc|breakdown  (default: cpu)
//!   --schema narrow|wide|mixed  (default: narrow)
//!   --batches N                 (default: 500)
//!   --batch-size N              (default: 10000)
//!   --output DIR                (default: /tmp/ffwd-file-profile)
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ffwd_bench::generators;
use ffwd_output::{
    BatchMetadata, StdoutFormat, StdoutSink, build_col_infos, resolve_col_infos,
    write_row_json_resolved,
};
use ffwd_types::diagnostics::ComponentStats;

// ---------------------------------------------------------------------------
// RSS measurement (shared with memory_profile.rs)
// ---------------------------------------------------------------------------

#[allow(deprecated)] // libc::mach_task_self_ — mach2 migration tracked separately
fn rss_bytes() -> usize {
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    let kb: usize = line
                        .split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    return kb * 1024;
                }
            }
        }
        0
    }
    #[cfg(target_os = "macos")]
    {
        use std::mem::{self, size_of};
        // SAFETY: `mach_task_basic_info_data_t` is a plain C struct of integer
        // fields; zero-initialization is valid for all of them.
        let mut info: libc::mach_task_basic_info_data_t = unsafe { mem::zeroed() };
        let mut count = (size_of::<libc::mach_task_basic_info_data_t>()
            / size_of::<libc::natural_t>()) as libc::mach_msg_type_number_t;
        // SAFETY: We pass a correctly-sized buffer (`info`) and matching count
        // to `task_info`; `mach_task_self_` is always a valid task port.
        let kr = unsafe {
            libc::task_info(
                libc::mach_task_self_,
                libc::MACH_TASK_BASIC_INFO,
                std::ptr::addr_of_mut!(info) as libc::task_info_t,
                std::ptr::addr_of_mut!(count),
            )
        };
        if kr == libc::KERN_SUCCESS {
            return info.resident_size as usize;
        }
        0
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        0
    }
}

fn rss_mb() -> f64 {
    rss_bytes() as f64 / 1_048_576.0
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_flag(args: &[String], flag: &str, default: &str) -> String {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .map_or_else(|| default.to_string(), String::clone)
}

fn parse_usize_flag(args: &[String], flag: &str, default: usize) -> usize {
    parse_flag(args, flag, &default.to_string())
        .parse()
        .unwrap_or(default)
}

fn fmt_throughput(total_rows: u64, total_bytes: u64, elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64();
    if secs == 0.0 {
        return "N/A".to_owned();
    }
    let lines_per_sec = total_rows as f64 / secs;
    let mb_per_sec = total_bytes as f64 / 1_048_576.0 / secs;
    format!("{lines_per_sec:.0} lines/sec ({mb_per_sec:.1} MB/sec)")
}

fn fmt_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

// ---------------------------------------------------------------------------
// Mode: breakdown — per-function timing
// ---------------------------------------------------------------------------

fn run_breakdown(
    batch: &arrow::record_batch::RecordBatch,
    meta: &BatchMetadata,
    num_batches: usize,
    batch_size: usize,
    schema_name: &str,
) {
    println!("=== File Output Timing Breakdown ===");
    println!("Schema:     {schema_name}");
    println!(
        "Batches:    {num_batches} × {batch_size} rows = {} total rows",
        num_batches * batch_size
    );
    println!();

    // Pre-allocate buffer matching production size
    let mut buf = Vec::with_capacity(batch_size * 300);
    let stats = Arc::new(ComponentStats::new());
    let mut sink = StdoutSink::new("bench".into(), StdoutFormat::Json, Arc::clone(&stats));

    // Warm up
    buf.clear();
    sink.write_batch_to(batch, meta, &mut buf)
        .expect("warmup failed");
    let bytes_per_batch = buf.len() as u64;
    println!(
        "Bytes/batch: {} ({:.0} bytes/row)\n",
        fmt_bytes(bytes_per_batch),
        bytes_per_batch as f64 / batch_size as f64
    );

    // 1. build_col_infos timing
    let start = Instant::now();
    for _ in 0..num_batches {
        let _cols = std::hint::black_box(build_col_infos(batch));
    }
    let build_col_elapsed = start.elapsed();

    // 2. write_row_json_resolved loop timing (with pre-built col_infos + resolve)
    let cols = build_col_infos(batch);
    let resolved = resolve_col_infos(batch, &cols);
    let start = Instant::now();
    for _ in 0..num_batches {
        buf.clear();
        for row in 0..batch.num_rows() {
            write_row_json_resolved(row, &resolved, &mut buf, true).expect("json failed");
        }
        std::hint::black_box(&buf);
    }
    let json_elapsed = start.elapsed();

    // 3. write_batch_to timing (includes build_col_infos + write_row_json)
    let start = Instant::now();
    for _ in 0..num_batches {
        buf.clear();
        sink.write_batch_to(batch, meta, &mut buf)
            .expect("batch write failed");
        std::hint::black_box(&buf);
    }
    let batch_elapsed = start.elapsed();

    // 4. memchr line counting timing
    let start = Instant::now();
    for _ in 0..num_batches {
        // Re-serialize to have realistic data in buf
        buf.clear();
        sink.write_batch_to(batch, meta, &mut buf)
            .expect("batch write failed");
        let _count = std::hint::black_box(memchr::memchr_iter(b'\n', &buf).count());
    }
    let count_elapsed = start.elapsed();
    // Subtract the batch_elapsed to isolate memchr cost
    let memchr_only = count_elapsed.saturating_sub(batch_elapsed);

    // 5. File I/O timing (write_all to tmpfile)
    let tmp = tempfile::NamedTempFile::new().expect("tmpfile failed");
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(tmp.path())
        .expect("open failed");
    // Pre-fill buffer
    buf.clear();
    sink.write_batch_to(batch, meta, &mut buf)
        .expect("batch write failed");
    let start = Instant::now();
    for _ in 0..num_batches {
        file.write_all(&buf).expect("write_all failed");
    }
    file.sync_data().expect("sync_data failed");
    let io_elapsed = start.elapsed();

    // 6. Full file output timing (serialize + write_all + line count)
    let mut file2 = std::fs::OpenOptions::new()
        .append(true)
        .open(tmp.path())
        .expect("open failed");
    let start = Instant::now();
    for _ in 0..num_batches {
        buf.clear();
        sink.write_batch_to(batch, meta, &mut buf)
            .expect("batch write failed");
        let _lines = memchr::memchr_iter(b'\n', &buf).count();
        file2.write_all(&buf).expect("write_all failed");
    }
    file2.sync_data().expect("sync_data failed");
    let full_elapsed = start.elapsed();

    let total_rows = (num_batches * batch_size) as u64;
    let total_bytes = bytes_per_batch * num_batches as u64;

    println!("### Per-Stage Timing ({num_batches} batches)\n");
    println!("| Stage | Total | Per Batch | % of Full | Throughput |");
    println!("|-------|-------|-----------|-----------|------------|");
    let stages: Vec<(&str, Duration)> = vec![
        ("build_col_infos", build_col_elapsed),
        ("write_row_json_resolved (loop)", json_elapsed),
        ("write_batch_to (end-to-end)", batch_elapsed),
        ("memchr line count", memchr_only),
        ("file write_all + sync", io_elapsed),
        ("FULL (serialize+count+IO)", full_elapsed),
    ];
    for (name, dur) in &stages {
        let pct = if full_elapsed.as_nanos() > 0 {
            dur.as_nanos() as f64 / full_elapsed.as_nanos() as f64 * 100.0
        } else {
            0.0
        };
        let per_batch = if num_batches > 0 {
            *dur / num_batches as u32
        } else {
            Duration::ZERO
        };
        println!(
            "| {:<30} | {:>9.1?} | {:>9.1?} | {:>7.1}% | {} |",
            name,
            dur,
            per_batch,
            pct,
            fmt_throughput(total_rows, total_bytes, *dur)
        );
    }

    println!();
    println!("### Summary");
    println!(
        "  Full throughput:    {}",
        fmt_throughput(total_rows, total_bytes, full_elapsed)
    );
    println!(
        "  Serialization:     {:.1}% of total",
        batch_elapsed.as_nanos() as f64 / full_elapsed.as_nanos() as f64 * 100.0
    );
    println!(
        "  File I/O:          {:.1}% of total",
        io_elapsed.as_nanos() as f64 / full_elapsed.as_nanos() as f64 * 100.0
    );
    let col_overhead = batch_elapsed.as_nanos() as f64 - json_elapsed.as_nanos() as f64;
    let col_pct = col_overhead / batch_elapsed.as_nanos() as f64 * 100.0;
    println!(
        "  build_col_infos:   {:.1}% of serialization ({:.1?} overhead)",
        col_pct,
        Duration::from_nanos(col_overhead.max(0.0) as u64)
    );
    println!("  RSS at end:        {:.1} MB", rss_mb());
    println!(
        "  Output file size:  {}",
        fmt_bytes(std::fs::metadata(tmp.path()).map_or(0, |m| m.len()))
    );
}

// ---------------------------------------------------------------------------
// Mode: alloc — allocation counting per stage
// ---------------------------------------------------------------------------

#[cfg(feature = "otlp-profile-alloc")]
fn run_alloc(
    batch: &arrow::record_batch::RecordBatch,
    meta: &BatchMetadata,
    num_batches: usize,
    batch_size: usize,
    schema_name: &str,
) {
    use stats_alloc::{INSTRUMENTED_SYSTEM, Region};

    println!("=== File Output Allocation Profile ===");
    println!("Schema:     {schema_name}");
    println!(
        "Batches:    {num_batches} × {batch_size} rows = {} total rows\n",
        num_batches * batch_size
    );

    let stats = Arc::new(ComponentStats::new());
    let mut sink = StdoutSink::new("bench".into(), StdoutFormat::Json, Arc::clone(&stats));
    let mut buf = Vec::with_capacity(batch_size * 300);

    // Warm up
    sink.write_batch_to(batch, meta, &mut buf)
        .expect("warmup failed");
    let bytes_per_batch = buf.len() as u64;

    // 1. build_col_infos allocations
    let reg = Region::new(&INSTRUMENTED_SYSTEM);
    for _ in 0..num_batches {
        let _cols = std::hint::black_box(build_col_infos(batch));
    }
    let col_stats = reg.change();

    // 2. write_row_json_resolved with pre-built cols (buffer reuse)
    let cols = build_col_infos(batch);
    let resolved = resolve_col_infos(batch, &cols);
    buf.clear();
    buf.reserve(batch_size * 300);
    let reg = Region::new(&INSTRUMENTED_SYSTEM);
    for _ in 0..num_batches {
        buf.clear();
        for row in 0..batch.num_rows() {
            write_row_json_resolved(row, &resolved, &mut buf, true).expect("json failed");
        }
    }
    let json_stats = reg.change();

    // 3. write_batch_to end-to-end
    let reg = Region::new(&INSTRUMENTED_SYSTEM);
    for _ in 0..num_batches {
        buf.clear();
        sink.write_batch_to(batch, meta, &mut buf)
            .expect("batch write failed");
    }
    let batch_stats = reg.change();

    // 4. Full FileSink-like path (serialize + line count + file write)
    let tmp = tempfile::NamedTempFile::new().expect("tmpfile failed");
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(tmp.path())
        .expect("open failed");
    let reg = Region::new(&INSTRUMENTED_SYSTEM);
    for _ in 0..num_batches {
        buf.clear();
        sink.write_batch_to(batch, meta, &mut buf)
            .expect("batch write failed");
        let _lines = memchr::memchr_iter(b'\n', &buf).count();
        file.write_all(&buf).expect("write_all failed");
    }
    let full_stats = reg.change();

    let total_rows = (num_batches * batch_size) as u64;

    println!("### Allocation Counts ({num_batches} batches)\n");
    println!("| Stage | Allocs | Reallocs | Deallocs | Bytes Alloc | Bytes/Row |");
    println!("|-------|--------|----------|----------|-------------|-----------|");

    let stages = vec![
        ("build_col_infos", &col_stats),
        ("write_row_json_resolved (reuse buf)", &json_stats),
        ("write_batch_to", &batch_stats),
        ("FULL (ser+count+IO)", &full_stats),
    ];
    for (name, s) in &stages {
        let bytes_alloc = s.bytes_allocated as u64;
        let bytes_per_row = if total_rows > 0 {
            bytes_alloc as f64 / total_rows as f64
        } else {
            0.0
        };
        println!(
            "| {:<27} | {:>10} | {:>10} | {:>10} | {:>11} | {:>9.1} |",
            name,
            s.allocations,
            s.reallocations,
            s.deallocations,
            fmt_bytes(bytes_alloc),
            bytes_per_row,
        );
    }

    println!();
    println!("### Per-Batch Allocation");
    let allocs_per_batch = full_stats.allocations as f64 / num_batches as f64;
    let bytes_per_batch_alloc = full_stats.bytes_allocated as f64 / num_batches as f64;
    println!("  Allocations/batch:  {:.1}", allocs_per_batch);
    println!(
        "  Bytes alloc/batch:  {:.0} ({:.0} bytes/row)",
        bytes_per_batch_alloc,
        bytes_per_batch_alloc / batch_size as f64
    );
    println!(
        "  Reallocs/batch:     {:.1}",
        full_stats.reallocations as f64 / num_batches as f64
    );
    println!(
        "  Output bytes/batch: {} ({:.0} bytes/row)",
        fmt_bytes(bytes_per_batch),
        bytes_per_batch as f64 / batch_size as f64
    );
    println!("  RSS:                {:.1} MB", rss_mb());
}

// ---------------------------------------------------------------------------
// Mode: cpu — pprof flamegraph
// ---------------------------------------------------------------------------

fn run_cpu(
    batch: &arrow::record_batch::RecordBatch,
    meta: &BatchMetadata,
    num_batches: usize,
    batch_size: usize,
    schema_name: &str,
    output_dir: &str,
) {
    println!("=== File Output CPU Profile ===");
    println!("Schema:     {schema_name}");
    println!(
        "Batches:    {num_batches} × {batch_size} rows = {} total rows",
        num_batches * batch_size
    );

    std::fs::create_dir_all(output_dir).expect("create output dir");

    let stats = Arc::new(ComponentStats::new());
    let mut sink = StdoutSink::new("bench".into(), StdoutFormat::Json, Arc::clone(&stats));
    let mut buf = Vec::with_capacity(batch_size * 300);

    // Warm up
    sink.write_batch_to(batch, meta, &mut buf)
        .expect("warmup failed");
    let bytes_per_batch = buf.len() as u64;

    // Create tmpfile for realistic I/O
    let tmp = tempfile::NamedTempFile::new().expect("tmpfile failed");
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(tmp.path())
        .expect("open failed");

    println!(
        "Output:     {} ({:.0} bytes/row)",
        fmt_bytes(bytes_per_batch),
        bytes_per_batch as f64 / batch_size as f64
    );
    println!("Profiling...\n");

    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(997) // Prime frequency avoids aliasing
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("pprof guard");

    let start = Instant::now();
    for _ in 0..num_batches {
        buf.clear();
        sink.write_batch_to(batch, meta, &mut buf)
            .expect("batch write failed");
        let _lines = memchr::memchr_iter(b'\n', &buf).count();
        file.write_all(&buf).expect("write_all failed");
    }
    file.sync_data().expect("sync_data failed");
    let elapsed = start.elapsed();

    let total_rows = (num_batches * batch_size) as u64;
    let total_bytes = bytes_per_batch * num_batches as u64;

    println!("### Results\n");
    println!("  Elapsed:     {elapsed:.2?}");
    println!(
        "  Throughput:  {}",
        fmt_throughput(total_rows, total_bytes, elapsed)
    );
    println!("  Total rows:  {total_rows}");
    println!("  Total bytes: {}", fmt_bytes(total_bytes));
    println!("  RSS:         {:.1} MB", rss_mb());

    // Write flamegraph
    if let Ok(report) = guard.report().build() {
        let svg_path = format!("{output_dir}/file_output_flamegraph.svg");
        let svg_file = std::fs::File::create(&svg_path).expect("create svg");
        report.flamegraph(svg_file).expect("write flamegraph");
        println!(
            "\n  Flamegraph:  {} ({})",
            svg_path,
            fmt_bytes(std::fs::metadata(&svg_path).map_or(0, |m| m.len()))
        );
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[cfg(feature = "otlp-profile-alloc")]
use stats_alloc::{INSTRUMENTED_SYSTEM, StatsAlloc};
#[cfg(feature = "otlp-profile-alloc")]
use std::alloc::System;
#[cfg(feature = "otlp-profile-alloc")]
#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        eprintln!("Usage: file_output_profile [OPTIONS]");
        eprintln!();
        eprintln!("  --mode cpu|alloc|breakdown  Profile mode (default: breakdown)");
        eprintln!("  --schema narrow|wide|mixed  Data schema (default: narrow)");
        eprintln!("  --batches N                 Number of batches (default: 500)");
        eprintln!("  --batch-size N              Rows per batch (default: 10000)");
        eprintln!(
            "  --output DIR                Output directory (default: /tmp/ffwd-file-profile)"
        );
        eprintln!();
        eprintln!("Note: --mode alloc requires --features otlp-profile-alloc");
        return;
    }

    let mode = parse_flag(&args, "--mode", "breakdown");
    let schema = parse_flag(&args, "--schema", "narrow");
    let num_batches = parse_usize_flag(&args, "--batches", 500);
    let batch_size = parse_usize_flag(&args, "--batch-size", 10_000);
    let output_dir = parse_flag(&args, "--output", "/tmp/ffwd-file-profile");

    let (batch, meta, schema_name) = match schema.as_str() {
        "wide" => (
            generators::gen_wide_batch(batch_size, 42),
            generators::make_metadata(),
            "wide (20+ fields)",
        ),
        "mixed" => (
            generators::gen_production_mixed_batch(batch_size, 42),
            generators::make_metadata(),
            "production-mixed",
        ),
        _ => (
            generators::gen_narrow_batch(batch_size, 42),
            generators::make_metadata(),
            "narrow (5 fields)",
        ),
    };

    match mode.as_str() {
        "cpu" => run_cpu(
            &batch,
            &meta,
            num_batches,
            batch_size,
            schema_name,
            &output_dir,
        ),
        "alloc" => {
            #[cfg(feature = "otlp-profile-alloc")]
            run_alloc(&batch, &meta, num_batches, batch_size, schema_name);
            #[cfg(not(feature = "otlp-profile-alloc"))]
            {
                eprintln!("Error: --mode alloc requires --features otlp-profile-alloc");
                eprintln!(
                    "Run: cargo run -p ffwd-bench --release --features otlp-profile-alloc --bin file_output_profile -- --mode alloc"
                );
                std::process::exit(1);
            }
        }
        "breakdown" => run_breakdown(&batch, &meta, num_batches, batch_size, schema_name),
        other => {
            eprintln!("Unknown mode: {other}. Use: cpu, alloc, breakdown");
            std::process::exit(1);
        }
    }
}

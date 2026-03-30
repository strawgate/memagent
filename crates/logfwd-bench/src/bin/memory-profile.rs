//! Memory usage profiling tool.
//!
//! Measures RSS, allocated, and active memory at different throughput levels
//! (1, 10, 100, 1K, 10K, 100K events per second) with and without transforms.
//!
//! Usage:
//!   cargo run --release -p logfwd-bench --bin memory-profile
//!
//! Outputs markdown table with memory usage at each throughput level.

use std::fmt::Write as FmtWrite;
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use logfwd_core::diagnostics::MemoryStats;
use logfwd_core::scan_config::ScanConfig;
use logfwd_core::scanner::SimdScanner;
use logfwd_output::{BatchMetadata, OutputSink};
use logfwd_transform::SqlTransform;

// ---------------------------------------------------------------------------
// jemalloc integration
// ---------------------------------------------------------------------------

#[cfg(unix)]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(unix)]
fn get_memory_stats() -> Option<MemoryStats> {
    use tikv_jemalloc_ctl::{epoch, stats};

    epoch::mib().ok()?.advance().ok()?;

    let resident = stats::resident::mib().ok()?.read().ok()?;
    let allocated = stats::allocated::mib().ok()?.read().ok()?;
    let active = stats::active::mib().ok()?.read().ok()?;

    Some(MemoryStats {
        resident,
        allocated,
        active,
    })
}

#[cfg(not(unix))]
fn get_memory_stats() -> Option<MemoryStats> {
    None
}

// ---------------------------------------------------------------------------
// Test data generation
// ---------------------------------------------------------------------------

/// Generate N newline-delimited JSON log lines (~250 bytes each).
fn gen_json_lines(n: usize) -> Vec<u8> {
    let levels = ["INFO", "ERROR", "DEBUG", "WARN"];
    let paths = [
        "/api/users",
        "/api/orders",
        "/api/health",
        "/api/auth/login",
        "/api/metrics",
    ];
    let mut s = String::with_capacity(n * 260);
    for i in 0..n {
        let _ = write!(
            s,
            r#"{{"timestamp":"2024-01-15T10:30:{:02}.{:09}Z","level":"{}","message":"GET {} HTTP/1.1","status":{},"duration_ms":{},"request_id":"req-{:08x}","service":"api-gateway"}}"#,
            i % 60,
            i % 1_000_000_000,
            levels[i % levels.len()],
            paths[i % paths.len()],
            [200, 200, 200, 500, 404][i % 5],
            (i % 500) + 1,
            i,
        );
        s.push('\n');
    }
    s.into_bytes()
}

// ---------------------------------------------------------------------------
// Output sinks
// ---------------------------------------------------------------------------

/// Null sink that discards all data — measures pure processing overhead.
struct NullSink {
    lines: AtomicU64,
    bytes: AtomicU64,
}

impl NullSink {
    fn new() -> Self {
        Self {
            lines: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }

    fn lines(&self) -> u64 {
        self.lines.load(Ordering::Relaxed)
    }
}

impl OutputSink for NullSink {
    fn send_batch(
        &mut self,
        batch: &arrow::record_batch::RecordBatch,
        _metadata: &BatchMetadata,
    ) -> std::io::Result<()> {
        self.lines.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        // Estimate bytes as rows * 250 (average line size)
        self.bytes.fetch_add(batch.num_rows() as u64 * 250, Ordering::Relaxed);
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        "null"
    }
}

// ---------------------------------------------------------------------------
// Pipeline simulation
// ---------------------------------------------------------------------------

struct PipelineConfig {
    eps: u64,
    transform_sql: Option<String>,
    duration_secs: u64,
}

struct PipelineResult {
    eps: u64,
    transform: String,
    lines_processed: u64,
    duration_secs: u64,
    memory_start: Option<MemoryStats>,
    memory_end: Option<MemoryStats>,
    memory_peak: Option<MemoryStats>,
}

fn run_pipeline(config: PipelineConfig) -> io::Result<PipelineResult> {
    let memory_start = get_memory_stats();

    // Generate test data
    // For simplicity, we'll generate batches that arrive at the specified rate
    let batch_size = config.eps.min(10_000) as usize; // Max 10K lines per batch
    let data = gen_json_lines(batch_size);

    let interval = Duration::from_millis((batch_size as u64 * 1000) / config.eps.max(1));

    let mut scanner = SimdScanner::new(ScanConfig::default());
    let mut transform = config.transform_sql.as_ref().map(|sql| {
        SqlTransform::new(sql).expect("failed to create transform")
    });
    let mut sink = NullSink::new();
    let metadata = BatchMetadata {
        resource_attrs: vec![("service.name".into(), "bench".into())],
        observed_time_ns: 0,
    };

    let stop = Arc::new(AtomicBool::new(false));
    let peak_resident = Arc::new(AtomicU64::new(0));
    let peak_allocated = Arc::new(AtomicU64::new(0));
    let peak_active = Arc::new(AtomicU64::new(0));

    // Memory monitoring thread
    let stop_clone = Arc::clone(&stop);
    let peak_resident_clone = Arc::clone(&peak_resident);
    let peak_allocated_clone = Arc::clone(&peak_allocated);
    let peak_active_clone = Arc::clone(&peak_active);

    let monitor = thread::spawn(move || {
        while !stop_clone.load(Ordering::Relaxed) {
            if let Some(stats) = get_memory_stats() {
                peak_resident_clone.fetch_max(stats.resident as u64, Ordering::Relaxed);
                peak_allocated_clone.fetch_max(stats.allocated as u64, Ordering::Relaxed);
                peak_active_clone.fetch_max(stats.active as u64, Ordering::Relaxed);
            }
            thread::sleep(Duration::from_millis(100));
        }
    });

    // Run pipeline
    let start = Instant::now();
    let end_time = start + Duration::from_secs(config.duration_secs);

    while Instant::now() < end_time {
        let batch = scanner.scan(&data)
            .map_err(|e| io::Error::other(format!("scan failed: {e}")))?;

        let result = if let Some(ref mut t) = transform {
            t.execute_blocking(batch)
                .map_err(|e| io::Error::other(format!("transform failed: {e}")))?
        } else {
            batch
        };

        sink.send_batch(&result, &metadata)?;

        thread::sleep(interval);
    }

    stop.store(true, Ordering::Relaxed);
    monitor.join().ok();

    let memory_end = get_memory_stats();
    let memory_peak = Some(MemoryStats {
        resident: peak_resident.load(Ordering::Relaxed) as usize,
        allocated: peak_allocated.load(Ordering::Relaxed) as usize,
        active: peak_active.load(Ordering::Relaxed) as usize,
    });

    Ok(PipelineResult {
        eps: config.eps,
        transform: config.transform_sql.clone().unwrap_or_else(|| "passthrough".to_string()),
        lines_processed: sink.lines(),
        duration_secs: config.duration_secs,
        memory_start,
        memory_end,
        memory_peak,
    })
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

fn format_bytes(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

fn print_result(result: &PipelineResult) {
    println!("\n{} EPS | {} transform", result.eps, result.transform);
    println!("  Lines processed: {}", result.lines_processed);
    println!("  Duration: {} seconds", result.duration_secs);

    if let (Some(start), Some(end), Some(peak)) = (&result.memory_start, &result.memory_end, &result.memory_peak) {
        println!("\n  Memory (start):");
        println!("    Resident: {}", format_bytes(start.resident));
        println!("    Allocated: {}", format_bytes(start.allocated));
        println!("    Active: {}", format_bytes(start.active));

        println!("\n  Memory (end):");
        println!("    Resident: {}", format_bytes(end.resident));
        println!("    Allocated: {}", format_bytes(end.allocated));
        println!("    Active: {}", format_bytes(end.active));

        println!("\n  Memory (peak):");
        println!("    Resident: {}", format_bytes(peak.resident));
        println!("    Allocated: {}", format_bytes(peak.allocated));
        println!("    Active: {}", format_bytes(peak.active));

        let delta_resident = peak.resident as i64 - start.resident as i64;
        let delta_allocated = peak.allocated as i64 - start.allocated as i64;

        println!("\n  Memory (delta to peak):");
        println!("    Resident: {:+}", format_bytes(delta_resident.abs() as usize));
        println!("    Allocated: {:+}", format_bytes(delta_allocated.abs() as usize));
    }
}

fn print_markdown_table(results: &[PipelineResult]) {
    println!("\n\n## Memory Usage Summary\n");
    println!("| EPS | Transform | Peak RSS | Peak Allocated | Delta RSS (from start) | Throughput (lines/sec) |");
    println!("|-----|-----------|----------|----------------|------------------------|------------------------|");

    for r in results {
        if let (Some(start), Some(peak)) = (&r.memory_start, &r.memory_peak) {
            let delta_resident = peak.resident as i64 - start.resident as i64;
            let throughput = r.lines_processed / r.duration_secs;

            println!(
                "| {} | {} | {} | {} | {:+} | {} |",
                r.eps,
                r.transform,
                format_bytes(peak.resident),
                format_bytes(peak.allocated),
                format_bytes(delta_resident.abs() as usize),
                throughput,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> io::Result<()> {
    println!("logfwd memory profiler");
    println!("======================\n");
    println!("Measuring memory usage at different throughput levels...\n");

    if get_memory_stats().is_none() {
        eprintln!("WARNING: jemalloc stats not available. Memory measurements will be skipped.");
    }

    let throughput_levels = vec![1, 10, 100, 1_000, 10_000, 100_000];
    let test_duration = 5; // seconds

    let mut results = Vec::new();

    // Test 1: Passthrough (no transform)
    println!("\n=== Passthrough (no transform) ===\n");
    for &eps in &throughput_levels {
        print!("Testing {} eps... ", eps);
        io::stdout().flush()?;

        let config = PipelineConfig {
            eps,
            transform_sql: None,
            duration_secs: test_duration,
        };

        let result = run_pipeline(config)?;
        println!("done");
        print_result(&result);
        results.push(result);
    }

    // Test 2: Simple SELECT * transform
    println!("\n\n=== SELECT * transform ===\n");
    for &eps in &throughput_levels {
        print!("Testing {} eps... ", eps);
        io::stdout().flush()?;

        let config = PipelineConfig {
            eps,
            transform_sql: Some("SELECT * FROM logs".to_string()),
            duration_secs: test_duration,
        };

        let result = run_pipeline(config)?;
        println!("done");
        print_result(&result);
        results.push(result);
    }

    // Test 3: Filter transform
    println!("\n\n=== WHERE filter transform ===\n");
    for &eps in &throughput_levels {
        print!("Testing {} eps... ", eps);
        io::stdout().flush()?;

        let config = PipelineConfig {
            eps,
            transform_sql: Some("SELECT * FROM logs WHERE level_str = 'ERROR'".to_string()),
            duration_secs: test_duration,
        };

        let result = run_pipeline(config)?;
        println!("done");
        print_result(&result);
        results.push(result);
    }

    // Test 4: Complex transform with regexp_extract
    println!("\n\n=== Complex transform (regexp_extract) ===\n");
    for &eps in &throughput_levels {
        print!("Testing {} eps... ", eps);
        io::stdout().flush()?;

        let config = PipelineConfig {
            eps,
            transform_sql: Some(
                "SELECT level_str, regexp_extract(message_str, '(GET|POST) (\\S+)', 2) AS path, status_int FROM logs".to_string()
            ),
            duration_secs: test_duration,
        };

        let result = run_pipeline(config)?;
        println!("done");
        print_result(&result);
        results.push(result);
    }

    // Print summary table
    print_markdown_table(&results);

    // Analysis and recommendations
    println!("\n\n## Analysis\n");

    // Find baseline (1 eps passthrough)
    if let Some(baseline) = results.iter().find(|r| r.eps == 1 && r.transform == "passthrough") {
        if let Some(peak) = &baseline.memory_peak {
            println!("**Baseline memory (1 eps, no transform):**");
            println!("- RSS: {}", format_bytes(peak.resident));
            println!("- Allocated: {}", format_bytes(peak.allocated));
            println!();
        }
    }

    // Compare memory growth
    let passthrough_results: Vec<_> = results.iter()
        .filter(|r| r.transform == "passthrough")
        .collect();

    if passthrough_results.len() >= 2 {
        if let (Some(low), Some(high)) = (passthrough_results.first(), passthrough_results.last())
            && let (Some(low_peak), Some(high_peak)) = (&low.memory_peak, &high.memory_peak) {
                let growth = high_peak.resident as f64 / low_peak.resident as f64;
                println!("**Memory scaling (passthrough):**");
                println!("- {} eps: {}", low.eps, format_bytes(low_peak.resident));
                println!("- {} eps: {}", high.eps, format_bytes(high_peak.resident));
                println!("- Growth factor: {:.2}x", growth);
                println!();
            }
    }

    // Compare transform impact
    let transform_impact: Vec<_> = results.windows(2)
        .filter(|w| w[0].eps == w[1].eps && w[0].transform == "passthrough")
        .map(|w| {
            let passthrough = &w[0];
            let with_transform = &w[1];
            (passthrough.eps, passthrough, with_transform)
        })
        .collect();

    if !transform_impact.is_empty() {
        println!("**Transform impact on memory:**");
        for (eps, passthrough, with_transform) in transform_impact {
            if let (Some(p_peak), Some(t_peak)) = (&passthrough.memory_peak, &with_transform.memory_peak) {
                let overhead = t_peak.resident as i64 - p_peak.resident as i64;
                let overhead_pct = (overhead as f64 / p_peak.resident as f64) * 100.0;
                println!("- {} eps: {:+} ({:+.1}%)", eps, format_bytes(overhead.unsigned_abs() as usize), overhead_pct);
            }
        }
        println!();
    }

    println!("\n## Recommendations for Performance Benchmarks\n");
    println!("Based on this analysis, consider the following dimensions for perf benchmarks:\n");
    println!("1. **Throughput levels:** 1K, 10K, 100K eps (covers typical deployment range)");
    println!("2. **Transform complexity:**");
    println!("   - Passthrough (no transform)");
    println!("   - Simple SELECT * (DataFusion overhead only)");
    println!("   - WHERE filter (common use case)");
    println!("   - Complex UDF (regexp_extract, grok)");
    println!("3. **Batch size:** Test with different batch sizes (1K, 10K, 100K lines)");
    println!("4. **Output sinks:** Null sink vs real sinks (OTLP, JSON lines)");
    println!("5. **Input format:** JSON vs CRI (CRI parsing adds overhead)");

    Ok(())
}

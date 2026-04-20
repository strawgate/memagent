#![allow(clippy::print_stdout, clippy::print_stderr)]
//! Sustained-load memory profiler for the logfwd pipeline.
//!
//! Runs scanner → SQL transform → null sink in a tight loop for a configurable
//! duration, sampling RSS and allocation stats at regular intervals. Reports:
//!
//! - RSS over time (timeline with growth detection)
//! - Allocation rate (bytes/sec, allocs/sec)
//! - Peak vs steady-state RSS
//! - Throughput (lines/sec, MB/sec)
//! - Whether anything leaks or grows unboundedly
//!
//! Run with: `cargo run -p logfwd-bench --release --bin memory-profile [duration_secs]`
//!   Default duration: 300 seconds (5 minutes)
//!
//! Options:
//!   --quick     30 seconds (CI/dev)
//!   --medium    120 seconds
//!   --duration  custom seconds
//!   --batch     batch size in lines (default: 10000)

#![allow(deprecated)] // Benchmarks use sync OutputSink; migration tracked separately.
#![allow(clippy::print_stdout, clippy::print_stderr)]
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::alloc::System;
use std::fmt::Write as _;

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

use std::io::Write;
use std::time::{Duration, Instant};

use logfwd_arrow::scanner::Scanner;
use logfwd_bench::generators;
use logfwd_transform::SqlTransform;

// ---------------------------------------------------------------------------
// RSS measurement
// ---------------------------------------------------------------------------

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
        // SAFETY: `zeroed()` is valid for `mach_task_basic_info_data_t`
        // (all-zero is a valid bit pattern for this plain-data struct).
        let mut info: libc::mach_task_basic_info_data_t = unsafe { mem::zeroed() };
        let mut count = (size_of::<libc::mach_task_basic_info_data_t>()
            / size_of::<libc::natural_t>()) as libc::mach_msg_type_number_t;
        // SAFETY: `mach_task_self_` is always a valid task port. `info` is a
        // mutable reference to a zeroed struct of the correct type and `count`
        // holds the matching element count. `task_info` only writes into `info`.
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
// Sample point
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Sample {
    elapsed_secs: f64,
    rss_mb: f64,
    total_allocated_bytes: u64,
    total_allocations: u64,
    total_deallocated_bytes: u64,
    total_rows: u64,
    total_batches: u64,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        eprintln!("Usage: memory-profile [OPTIONS]");
        eprintln!("  --quick          30 seconds");
        eprintln!("  --medium         120 seconds");
        eprintln!("  --duration SECS  custom duration (default: 300)");
        eprintln!("  --batch LINES    batch size (default: 10000)");
        eprintln!("  --wide           use wide 20-field schema");
        eprintln!("  --mixed          use production-mixed schema");
        return;
    }

    let duration_secs = if args.iter().any(|a| a == "--quick") {
        30
    } else if args.iter().any(|a| a == "--medium") {
        120
    } else {
        parse_positive_u64_flag(&args, "--duration", 300)
    };

    let batch_lines = parse_positive_usize_flag(&args, "--batch", 10_000);

    let use_wide = args.iter().any(|a| a == "--wide");
    let use_mixed = args.iter().any(|a| a == "--mixed");

    let schema_name = if use_wide {
        "wide (20 fields)"
    } else if use_mixed {
        "production-mixed"
    } else {
        "narrow (5 fields)"
    };

    let sample_interval = Duration::from_secs(5);
    let duration = Duration::from_secs(duration_secs);

    // -----------------------------------------------------------------------
    // Pre-generate data
    // -----------------------------------------------------------------------

    eprintln!(
        "=== Sustained Memory Profile ===\n\
         Duration:  {}s\n\
         Batch:     {} lines\n\
         Schema:    {}\n\
         Sample:    every {}s\n",
        duration_secs,
        batch_lines,
        schema_name,
        sample_interval.as_secs()
    );

    let data = if use_wide {
        generators::gen_wide(batch_lines, 42)
    } else if use_mixed {
        generators::gen_production_mixed(batch_lines, 42)
    } else {
        generators::gen_narrow(batch_lines, 42)
    };
    let data_bytes = bytes::Bytes::from(data);
    let input_bytes_per_batch = data_bytes.len() as u64;

    eprintln!(
        "  Input data:  {:.1} MB per batch ({} lines, {:.0} bytes/line)",
        input_bytes_per_batch as f64 / 1_048_576.0,
        batch_lines,
        input_bytes_per_batch as f64 / batch_lines as f64,
    );

    // -----------------------------------------------------------------------
    // Set up pipeline components
    // -----------------------------------------------------------------------

    let sql = "SELECT * FROM logs WHERE level != 'DEBUG'";
    let mut transform = SqlTransform::new(sql).expect("bad SQL");
    let scan_config = transform.scan_config();
    let mut scanner = Scanner::new(scan_config);
    let mut null_sink = logfwd_bench::NullSink;
    let metadata = generators::make_metadata();

    // Warm up: run one batch to initialise DataFusion, JIT, etc.
    {
        let batch = scanner
            .scan_detached(data_bytes.clone())
            .expect("scan failed");
        let result = transform.execute_blocking(batch).expect("transform failed");
        null_sink
            .send_batch(&result, &metadata)
            .expect("sink failed");
    }

    let warmup_rss = rss_mb();
    eprintln!("  Warmup RSS:  {:.1} MB\n", warmup_rss);

    // -----------------------------------------------------------------------
    // Sustained load loop with sampling
    // -----------------------------------------------------------------------

    let start = Instant::now();
    let mut last_sample = Instant::now();
    let expected_samples = (duration_secs / sample_interval.as_secs()) as usize + 2;
    let mut samples: Vec<Sample> = Vec::with_capacity(expected_samples);

    let mut total_rows: u64 = 0;
    let mut total_batches: u64 = 0;
    let mut total_input_bytes: u64 = 0;

    // Record initial sample
    let mut region = Region::new(GLOBAL);

    // Take initial sample
    samples.push(Sample {
        elapsed_secs: 0.0,
        rss_mb: rss_mb(),
        total_allocated_bytes: 0,
        total_allocations: 0,
        total_deallocated_bytes: 0,
        total_rows: 0,
        total_batches: 0,
    });

    eprint!("  Running: ");
    let _ = std::io::stderr().flush();

    while start.elapsed() < duration {
        // Scan → transform → sink
        let batch = scanner
            .scan_detached(data_bytes.clone())
            .expect("scan failed");
        let rows = batch.num_rows() as u64;
        let result = transform.execute_blocking(batch).expect("transform failed");
        null_sink
            .send_batch(&result, &metadata)
            .expect("sink failed");

        total_rows += rows;
        total_batches += 1;
        total_input_bytes += input_bytes_per_batch;

        // Sample at intervals
        if last_sample.elapsed() >= sample_interval {
            let stats = region.change_and_reset();
            let elapsed = start.elapsed().as_secs_f64();

            samples.push(Sample {
                elapsed_secs: elapsed,
                rss_mb: rss_mb(),
                total_allocated_bytes: stats.bytes_allocated as u64,
                total_allocations: stats.allocations as u64,
                total_deallocated_bytes: stats.bytes_deallocated as u64,
                total_rows,
                total_batches,
            });

            eprint!(".");
            let _ = std::io::stderr().flush();
            last_sample = Instant::now();
        }
    }

    // Final sample
    let stats = region.change_and_reset();
    samples.push(Sample {
        elapsed_secs: start.elapsed().as_secs_f64(),
        rss_mb: rss_mb(),
        total_allocated_bytes: stats.bytes_allocated as u64,
        total_allocations: stats.allocations as u64,
        total_deallocated_bytes: stats.bytes_deallocated as u64,
        total_rows,
        total_batches,
    });

    let total_elapsed = start.elapsed();
    eprintln!(" done ({:.1}s)\n", total_elapsed.as_secs_f64());

    // -----------------------------------------------------------------------
    // Analysis and reporting
    // -----------------------------------------------------------------------

    print_report(&ProfileRun {
        samples: &samples,
        total_elapsed,
        total_rows,
        total_batches,
        total_input_bytes,
        batch_lines,
        schema_name,
        sql,
    });
}

fn parse_positive_u64_flag(args: &[String], flag: &str, default: u64) -> u64 {
    parse_positive_flag(args, flag, default, |raw| raw.parse::<u64>().ok())
}

fn parse_positive_usize_flag(args: &[String], flag: &str, default: usize) -> usize {
    parse_positive_flag(args, flag, default, |raw| raw.parse::<usize>().ok())
}

fn parse_positive_flag<T>(
    args: &[String],
    flag: &str,
    default: T,
    parse: impl Fn(&str) -> Option<T>,
) -> T
where
    T: PartialEq + From<u8> + Copy + std::fmt::Display,
{
    match args.iter().rposition(|arg| arg == flag) {
        Some(idx) => match args.get(idx + 1) {
            Some(raw) => match parse(raw) {
                Some(value) if value != T::from(0) => value,
                _ => {
                    eprintln!(
                        "warning: {flag} expects a positive integer; using default {default}"
                    );
                    default
                }
            },
            None => {
                eprintln!("warning: {flag} expects a value; using default {default}");
                default
            }
        },
        None => default,
    }
}

struct ProfileRun<'a> {
    samples: &'a [Sample],
    total_elapsed: Duration,
    total_rows: u64,
    total_batches: u64,
    total_input_bytes: u64,
    batch_lines: usize,
    schema_name: &'a str,
    sql: &'a str,
}

fn print_report(run: &ProfileRun<'_>) {
    print!("{}", render_report(run));
}

fn render_report(run: &ProfileRun<'_>) -> String {
    let ProfileRun {
        samples,
        total_elapsed,
        total_rows,
        total_batches,
        total_input_bytes,
        batch_lines,
        schema_name,
        sql,
    } = *run;
    let elapsed_secs = total_elapsed.as_secs_f64();
    let mut out = String::new();

    // --- Throughput ---
    let lines_per_sec = if elapsed_secs > 0.0 {
        total_rows as f64 / elapsed_secs
    } else {
        0.0
    };
    let mb_per_sec = if elapsed_secs > 0.0 {
        total_input_bytes as f64 / 1_048_576.0 / elapsed_secs
    } else {
        0.0
    };
    let batches_per_sec = if elapsed_secs > 0.0 {
        total_batches as f64 / elapsed_secs
    } else {
        0.0
    };

    // --- RSS analysis ---
    let rss_values: Vec<f64> = samples.iter().map(|s| s.rss_mb).collect();
    let (peak_rss, min_rss) = if rss_values.is_empty() {
        (0.0, 0.0)
    } else {
        rss_values
            .iter()
            .copied()
            .fold((f64::NEG_INFINITY, f64::INFINITY), |(peak, min), v| {
                (peak.max(v), min.min(v))
            })
    };

    // Steady-state: average of last half of samples
    let steady_start = samples.len() / 2;
    let steady_rss = if samples.len() > steady_start {
        let steady_samples = &rss_values[steady_start..];
        steady_samples.iter().sum::<f64>() / steady_samples.len() as f64
    } else {
        rss_values.last().copied().unwrap_or(0.0)
    };

    // Growth detection: linear regression on RSS samples
    let (slope_mb_per_min, r_squared) = linear_regression(samples);
    let growth_verdict = if r_squared > 0.7 && slope_mb_per_min > 0.5 {
        format!(
            "⚠ GROWING: {:.2} MB/min (R²={:.2}) — possible leak",
            slope_mb_per_min, r_squared
        )
    } else if slope_mb_per_min.abs() < 0.1 {
        format!(
            "✓ STABLE: {:.3} MB/min (R²={:.2})",
            slope_mb_per_min, r_squared
        )
    } else {
        format!(
            "~ MINOR DRIFT: {:.2} MB/min (R²={:.2})",
            slope_mb_per_min, r_squared
        )
    };

    // --- Allocation analysis ---
    // Sum all interval allocations (skip first sample which is the starting point)
    let total_allocated: u64 = samples
        .iter()
        .skip(1)
        .map(|s| s.total_allocated_bytes)
        .sum();
    let total_allocs: u64 = samples.iter().skip(1).map(|s| s.total_allocations).sum();
    let total_freed: u64 = samples
        .iter()
        .skip(1)
        .map(|s| s.total_deallocated_bytes)
        .sum();

    let alloc_rate_mb_per_sec = if elapsed_secs > 0.0 {
        total_allocated as f64 / 1_048_576.0 / elapsed_secs
    } else {
        0.0
    };
    let alloc_rate_per_sec = if elapsed_secs > 0.0 {
        total_allocs as f64 / elapsed_secs
    } else {
        0.0
    };
    let bytes_per_row = if total_rows > 0 {
        total_allocated as f64 / total_rows as f64
    } else {
        0.0
    };
    let allocs_per_row = if total_rows > 0 {
        total_allocs as f64 / total_rows as f64
    } else {
        0.0
    };
    let net_retained = total_allocated as i64 - total_freed as i64;

    // -----------------------------------------------------------------------
    // Print markdown report
    // -----------------------------------------------------------------------

    let _ = writeln!(out, "## Sustained Memory Profile Results\n");
    let _ = writeln!(out, "| Parameter | Value |");
    let _ = writeln!(out, "|-----------|-------|");
    let _ = writeln!(out, "| Schema | {} |", schema_name);
    let _ = writeln!(out, "| SQL | `{}` |", sql);
    let _ = writeln!(out, "| Batch size | {} lines |", batch_lines);
    let _ = writeln!(out, "| Duration | {:.1}s |", elapsed_secs);
    let _ = writeln!(out, "| Total rows | {} |", total_rows);
    let _ = writeln!(out, "| Total batches | {} |", total_batches);
    let _ = writeln!(
        out,
        "| Total input | {:.1} MB |",
        total_input_bytes as f64 / 1_048_576.0
    );
    let _ = writeln!(out);

    let _ = writeln!(out, "### Throughput\n");
    let _ = writeln!(out, "| Metric | Value |");
    let _ = writeln!(out, "|--------|-------|");
    let _ = writeln!(out, "| Lines/sec | {:.0} |", lines_per_sec);
    let _ = writeln!(out, "| MB/sec (input) | {:.1} |", mb_per_sec);
    let _ = writeln!(out, "| Batches/sec | {:.1} |", batches_per_sec);
    let _ = writeln!(out);

    let _ = writeln!(out, "### Memory (RSS)\n");
    let _ = writeln!(out, "| Metric | Value |");
    let _ = writeln!(out, "|--------|-------|");
    let _ = writeln!(out, "| Peak RSS | {:.1} MB |", peak_rss);
    let _ = writeln!(out, "| Min RSS | {:.1} MB |", min_rss);
    let _ = writeln!(out, "| Steady-state RSS | {:.1} MB |", steady_rss);
    let _ = writeln!(out, "| RSS range | {:.1} MB |", peak_rss - min_rss);
    let _ = writeln!(out, "| Growth rate | {} |", growth_verdict);
    let _ = writeln!(out);

    let _ = writeln!(out, "### Allocations\n");
    let _ = writeln!(out, "| Metric | Value |");
    let _ = writeln!(out, "|--------|-------|");
    let _ = writeln!(
        out,
        "| Total allocated | {:.1} MB |",
        total_allocated as f64 / 1_048_576.0
    );
    let _ = writeln!(
        out,
        "| Total freed | {:.1} MB |",
        total_freed as f64 / 1_048_576.0
    );
    let _ = writeln!(
        out,
        "| Net retained | {:.1} MB |",
        net_retained as f64 / 1_048_576.0
    );
    let _ = writeln!(out, "| Alloc rate | {:.1} MB/sec |", alloc_rate_mb_per_sec);
    let _ = writeln!(
        out,
        "| Alloc count rate | {:.0} allocs/sec |",
        alloc_rate_per_sec
    );
    let _ = writeln!(out, "| Bytes/row | {:.0} |", bytes_per_row);
    let _ = writeln!(out, "| Allocs/row | {:.1} |", allocs_per_row);
    let _ = writeln!(out);

    // --- RSS timeline ---
    let _ = writeln!(out, "### RSS Timeline\n");
    let _ = writeln!(
        out,
        "| Time (s) | RSS (MB) | Δ Alloc (MB) | Δ Allocs | Rows | Batches |"
    );
    let _ = writeln!(
        out,
        "|----------|----------|-------------|----------|------|---------|"
    );
    for s in samples {
        let _ = writeln!(
            out,
            "| {:.0} | {:.1} | {:.1} | {} | {} | {} |",
            s.elapsed_secs,
            s.rss_mb,
            s.total_allocated_bytes as f64 / 1_048_576.0,
            s.total_allocations,
            s.total_rows,
            s.total_batches,
        );
    }
    let _ = writeln!(out);

    // --- Allocation rate per interval ---
    if samples.len() > 2 {
        let _ = writeln!(out, "### Allocation Rate Per Interval\n");
        let _ = writeln!(
            out,
            "| Interval | Alloc Rate (MB/s) | Alloc Count/s | Rows/s |"
        );
        let _ = writeln!(
            out,
            "|----------|------------------|---------------|--------|"
        );
        for w in samples.windows(2) {
            let dt = w[1].elapsed_secs - w[0].elapsed_secs;
            if dt > 0.0 {
                let d_rows = w[1].total_rows - w[0].total_rows;
                let _ = writeln!(
                    out,
                    "| {:.0}–{:.0}s | {:.1} | {:.0} | {:.0} |",
                    w[0].elapsed_secs,
                    w[1].elapsed_secs,
                    w[1].total_allocated_bytes as f64 / 1_048_576.0 / dt,
                    w[1].total_allocations as f64 / dt,
                    d_rows as f64 / dt,
                );
            }
        }
        let _ = writeln!(out);
    }

    // --- Verdict ---
    let _ = writeln!(out, "### Verdict\n");
    if r_squared > 0.7 && slope_mb_per_min > 0.5 {
        let _ = writeln!(
            out,
            "**⚠ WARNING**: RSS is growing at {:.2} MB/min with R²={:.2}. \
             This suggests a possible memory leak or unbounded growth. \
             After {} minutes, RSS grew from {:.1} MB to {:.1} MB.",
            slope_mb_per_min,
            r_squared,
            elapsed_secs / 60.0,
            rss_values.first().unwrap_or(&0.0),
            rss_values.last().unwrap_or(&0.0),
        );
    } else if peak_rss - min_rss > 50.0 {
        let _ = writeln!(
            out,
            "**~ CAUTION**: RSS fluctuates by {:.1} MB (range: {:.1}–{:.1} MB). \
             No sustained growth detected, but allocation spikes are present.",
            peak_rss - min_rss,
            min_rss,
            peak_rss,
        );
    } else {
        let _ = writeln!(
            out,
            "**✓ HEALTHY**: RSS is stable at {:.1} MB (±{:.1} MB). \
             No leaks or unbounded growth detected over {:.0}s.",
            steady_rss,
            peak_rss - min_rss,
            elapsed_secs,
        );
    }
    let _ = writeln!(out);

    // Per-row summary for easy comparison
    let _ = writeln!(out, "### Per-Row Cost Summary\n");
    let _ = writeln!(out, "| Metric | Value |");
    let _ = writeln!(out, "|--------|-------|");
    let _ = writeln!(
        out,
        "| Input bytes/row | {:.0} |",
        if total_rows > 0 {
            total_input_bytes as f64 / total_rows as f64
        } else {
            0.0
        }
    );
    let _ = writeln!(out, "| Allocated bytes/row | {:.0} |", bytes_per_row);
    let _ = writeln!(
        out,
        "| Amplification factor | {:.1}x |",
        if total_rows > 0 {
            bytes_per_row / (total_input_bytes as f64 / total_rows as f64)
        } else {
            0.0
        }
    );
    let _ = writeln!(out, "| Allocations/row | {:.1} |", allocs_per_row);
    let _ = writeln!(
        out,
        "| RSS per batch ({} rows) | {:.1} MB |",
        batch_lines, steady_rss
    );

    out
}

/// Simple linear regression: RSS_mb = slope * time_min + intercept.
/// Returns (slope_mb_per_min, r_squared).
fn linear_regression(samples: &[Sample]) -> (f64, f64) {
    let n = samples.len() as f64;
    if n < 3.0 {
        return (0.0, 0.0);
    }

    // Convert to minutes for clearer slope interpretation
    let xs: Vec<f64> = samples.iter().map(|s| s.elapsed_secs / 60.0).collect();
    let ys: Vec<f64> = samples.iter().map(|s| s.rss_mb).collect();

    let x_mean = xs.iter().sum::<f64>() / n;
    let y_mean = ys.iter().sum::<f64>() / n;

    let mut ss_xy = 0.0;
    let mut ss_xx = 0.0;
    let mut ss_yy = 0.0;

    for i in 0..samples.len() {
        let dx = xs[i] - x_mean;
        let dy = ys[i] - y_mean;
        ss_xy += dx * dy;
        ss_xx += dx * dx;
        ss_yy += dy * dy;
    }

    if ss_xx < f64::EPSILON || ss_yy < f64::EPSILON {
        return (0.0, 0.0);
    }

    let slope = ss_xy / ss_xx;
    let r_squared = (ss_xy * ss_xy) / (ss_xx * ss_yy);

    (slope, r_squared)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(
        elapsed_secs: f64,
        rss_mb: f64,
        total_allocated_bytes: u64,
        total_allocations: u64,
        total_deallocated_bytes: u64,
        total_rows: u64,
        total_batches: u64,
    ) -> Sample {
        Sample {
            elapsed_secs,
            rss_mb,
            total_allocated_bytes,
            total_allocations,
            total_deallocated_bytes,
            total_rows,
            total_batches,
        }
    }

    #[test]
    fn linear_regression_reports_stable_series() {
        let samples = vec![
            sample(0.0, 40.0, 0, 0, 0, 0, 0),
            sample(5.0, 40.0, 10, 2, 10, 100, 1),
            sample(10.0, 40.0, 10, 2, 10, 200, 2),
        ];

        let (slope, r_squared) = linear_regression(&samples);
        assert!(slope.abs() < f64::EPSILON);
        assert!(r_squared.abs() < f64::EPSILON);
    }

    #[test]
    fn render_report_contains_expected_sections() {
        let samples = vec![
            sample(0.0, 40.0, 0, 0, 0, 0, 0),
            sample(5.0, 41.0, 1_048_576, 100, 1_048_576, 10_000, 1),
            sample(10.0, 40.5, 1_048_576, 100, 1_048_576, 20_000, 2),
        ];
        let run = ProfileRun {
            samples: &samples,
            total_elapsed: Duration::from_secs(10),
            total_rows: 20_000,
            total_batches: 2,
            total_input_bytes: 2_097_152,
            batch_lines: 10_000,
            schema_name: "narrow (5 fields)",
            sql: "SELECT * FROM logs",
        };

        let report = render_report(&run);
        assert!(report.contains("## Sustained Memory Profile Results"));
        assert!(report.contains("### Throughput"));
        assert!(report.contains("### Memory (RSS)"));
        assert!(report.contains("### RSS Timeline"));
        assert!(report.contains("### Verdict"));
        assert!(report.contains("| Schema | narrow (5 fields) |"));
        assert!(report.contains("| RSS per batch (10000 rows) |"));
    }

    #[test]
    fn parse_positive_flags_fall_back_for_zero_and_invalid_values() {
        let args = vec![
            "memory-profile".to_string(),
            "--duration".to_string(),
            "0".to_string(),
            "--batch".to_string(),
            "nope".to_string(),
        ];

        assert_eq!(parse_positive_u64_flag(&args, "--duration", 300), 300);
        assert_eq!(parse_positive_usize_flag(&args, "--batch", 10_000), 10_000);
    }

    #[test]
    fn parse_positive_flags_use_last_occurrence() {
        let args = vec![
            "memory-profile".to_string(),
            "--duration".to_string(),
            "0".to_string(),
            "--duration".to_string(),
            "60".to_string(),
        ];

        assert_eq!(parse_positive_u64_flag(&args, "--duration", 300), 60);
    }

    #[test]
    fn parse_positive_flags_fall_back_for_missing_values() {
        let args = vec!["memory-profile".to_string(), "--duration".to_string()];

        assert_eq!(parse_positive_u64_flag(&args, "--duration", 300), 300);
    }

    #[test]
    fn render_report_handles_empty_samples() {
        let run = ProfileRun {
            samples: &[],
            total_elapsed: Duration::from_secs(0),
            total_rows: 0,
            total_batches: 0,
            total_input_bytes: 0,
            batch_lines: 10_000,
            schema_name: "narrow (5 fields)",
            sql: "SELECT * FROM logs",
        };

        let report = render_report(&run);
        assert!(report.contains("## Sustained Memory Profile Results"));
        assert!(report.contains("| Peak RSS | 0.0 MB |"));
    }
}

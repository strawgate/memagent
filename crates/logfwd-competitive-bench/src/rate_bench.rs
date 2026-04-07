//! Ingest benchmark: measures RSS memory and CPU usage for logfwd at
//! controlled event rates and at maximum throughput.
//!
//! This is a non-competitive benchmark — it only runs logfwd.
//!
//! EPS tiers: 1, 10, 100, 1K, 10K, 100K, 1M, then an **unlimited** tier
//! that writes as fast as possible to measure the resource ceiling.
//!
//! For each target rate the benchmark:
//!   1. Writes a logfwd config that tails a temp file and forwards to the
//!      provided blackhole address.
//!   2. Spawns logfwd, then a writer thread (rate-limited or unlimited).
//!   3. After a short warmup it samples RSS (from `/proc/{pid}/status`) and
//!      CPU (from `/proc/{pid}/stat`) every 500 ms for 20 s.
//!   4. Reports the averages.

use std::io::{BufWriter, Write};
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// EPS levels tested by the benchmark.
/// The final `0` entry means "unlimited" — write as fast as possible.
pub const DEFAULT_EPS_LEVELS: &[u64] = &[1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 0];

/// Warmup duration before sampling begins.
const WARMUP_SECS: u64 = 5;

/// Measurement window during which samples are collected.
const MEASURE_SECS: u64 = 20;

/// Interval between resource-usage samples.
const SAMPLE_INTERVAL_MS: u64 = 500;

/// Port used for logfwd's diagnostics server during rate-bench runs.
/// Intentionally different from the port used by the competitive bench (19876).
const DIAGNOSTICS_PORT: u16 = 19880;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Resource-usage result for a single EPS level.
#[derive(serde::Serialize)]
pub struct RateBenchResult {
    /// Target events per second.
    pub eps: u64,
    /// Average RSS in KiB over the measurement window.
    pub rss_kb: u64,
    /// Average CPU utilisation % over the measurement window.
    pub cpu_percent: f64,
    /// Measured throughput (events written / total elapsed seconds).
    pub actual_eps: f64,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Run the rate-ingest benchmark for all default EPS levels.
///
/// * `logfwd_binary` — path to the logfwd binary  
/// * `bench_dir`     — scratch directory for configs and data files  
/// * `blackhole_addr` — `host:port` for the HTTP blackhole sink
pub fn run_rate_bench(
    logfwd_binary: &Path,
    bench_dir: &Path,
    blackhole_addr: &str,
) -> Vec<RateBenchResult> {
    let mut results = Vec::new();
    for &eps in DEFAULT_EPS_LEVELS {
        let label = if eps == 0 {
            "unlimited".to_string()
        } else {
            format!("{eps}")
        };
        eprintln!("=== Rate bench: {label} eps ===");
        match run_single_rate(logfwd_binary, bench_dir, blackhole_addr, eps) {
            Ok(r) => {
                eprintln!(
                    "  RSS: {} KiB, CPU: {:.1}%, actual: {:.1} eps",
                    r.rss_kb, r.cpu_percent, r.actual_eps
                );
                results.push(r);
            }
            Err(e) => eprintln!("  ERROR: {e}"),
        }
        eprintln!();
    }
    results
}

// ---------------------------------------------------------------------------
// Single-rate run
// ---------------------------------------------------------------------------

fn run_single_rate(
    logfwd_binary: &Path,
    bench_dir: &Path,
    blackhole_addr: &str,
    eps: u64,
) -> Result<RateBenchResult, String> {
    let data_file = bench_dir.join(format!("rate_{eps}.jsonl"));
    let cfg_path = bench_dir.join(format!("rate_{eps}.yaml"));

    // Create an empty file so logfwd can open it immediately.
    std::fs::write(&data_file, b"").map_err(|e| e.to_string())?;
    write_logfwd_config(&cfg_path, &data_file, blackhole_addr)?;

    // Spawn logfwd.
    let mut child = Command::new(logfwd_binary)
        .arg("--config")
        .arg(&cfg_path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|e| format!("failed to spawn logfwd: {e}"))?;

    let pid = child.id();

    // Brief pause so logfwd can open the file before we start writing.
    std::thread::sleep(Duration::from_millis(500));

    // Start rate-limited writer thread.
    let stop_flag = Arc::new(AtomicBool::new(false));
    let lines_written = Arc::new(AtomicU64::new(0));
    let writer = {
        let stop = Arc::clone(&stop_flag);
        let written = Arc::clone(&lines_written);
        let path = data_file.clone();
        std::thread::spawn(move || write_at_rate(&path, eps, &stop, &written))
    };

    let total_start = Instant::now();

    // Warmup.
    std::thread::sleep(Duration::from_secs(WARMUP_SECS));

    // Measurement window: sample RSS and CPU every SAMPLE_INTERVAL_MS.
    let mut rss_samples: Vec<u64> = Vec::new();
    let mut cpu_samples: Vec<f64> = Vec::new();
    let mut prev: Option<(u64, Instant)> = None; // (proc_ticks, wall_time)

    let deadline = Instant::now() + Duration::from_secs(MEASURE_SECS);
    while Instant::now() < deadline {
        if let Some(rss) = read_rss_kb(pid) {
            rss_samples.push(rss);
        }

        if let Some(ticks) = read_proc_ticks(pid) {
            let now = Instant::now();
            if let Some((prev_ticks, prev_time)) = prev {
                let delta_ticks = ticks.saturating_sub(prev_ticks);
                let delta_secs = now.duration_since(prev_time).as_secs_f64();
                // CLK_TCK is 100 Hz on Linux (the standard value).
                let cpu_pct = (delta_ticks as f64 / 100.0) / delta_secs * 100.0;
                cpu_samples.push(cpu_pct);
            }
            prev = Some((ticks, now));
        }

        std::thread::sleep(Duration::from_millis(SAMPLE_INTERVAL_MS));
    }

    let elapsed = total_start.elapsed().as_secs_f64();
    let total = lines_written.load(Ordering::Relaxed);

    // Stop writer, kill logfwd.
    stop_flag.store(true, Ordering::Relaxed);
    let _ = writer.join();
    let _ = child.kill();
    let _ = child.wait();

    // Clean up scratch files.
    let _ = std::fs::remove_file(&data_file);
    let _ = std::fs::remove_file(&cfg_path);

    Ok(RateBenchResult {
        eps,
        rss_kb: avg_u64(&rss_samples),
        cpu_percent: avg_f64(&cpu_samples),
        actual_eps: total as f64 / elapsed,
    })
}

// ---------------------------------------------------------------------------
// Config writer
// ---------------------------------------------------------------------------

fn write_logfwd_config(
    cfg_path: &Path,
    data_file: &Path,
    blackhole_addr: &str,
) -> Result<(), String> {
    let config = format!(
        r#"server:
  diagnostics: "127.0.0.1:{DIAGNOSTICS_PORT}"
pipelines:
  bench:
    inputs:
      - type: file
        path: "{data}"
        format: json
    transform: "SELECT * FROM logs"
    outputs:
      - type: http
        endpoint: "http://{sink}"
        format: json
"#,
        data = data_file.display(),
        sink = blackhole_addr,
    );
    std::fs::write(cfg_path, config).map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// Rate-limited writer thread
// ---------------------------------------------------------------------------

/// Append one JSON log line per slot to `path` at `eps` events per second
/// until `stop` is set.  When `eps == 0` the writer runs as fast as possible
/// (unlimited throughput mode).
fn write_at_rate(path: &Path, eps: u64, stop: &AtomicBool, written: &AtomicU64) {
    let file = match std::fs::OpenOptions::new().append(true).open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("  rate-bench writer: open failed: {e}");
            return;
        }
    };
    let mut w = BufWriter::with_capacity(64 * 1024, file);
    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    let mut last_flush = Instant::now();
    let mut count = 0u64;

    if eps == 0 {
        // Unlimited mode: write as fast as possible.
        while !stop.load(Ordering::Relaxed) {
            let level = levels[(count % 4) as usize];
            let _ = writeln!(
                w,
                r#"{{"ts":"2024-01-01T00:00:{:02}Z","level":"{level}","msg":"bench","n":{count}}}"#,
                count % 60,
            );
            count += 1;
            written.fetch_add(1, Ordering::Relaxed);
            let now = Instant::now();
            if now.duration_since(last_flush) >= Duration::from_millis(100) {
                let _ = w.flush();
                last_flush = now;
            }
        }
    } else {
        // Rate-limited mode.
        let interval = Duration::from_nanos(1_000_000_000 / eps);
        let mut next = Instant::now();
        while !stop.load(Ordering::Relaxed) {
            let now = Instant::now();
            if now >= next {
                let level = levels[(count % 4) as usize];
                let _ = writeln!(
                    w,
                    r#"{{"ts":"2024-01-01T00:00:{:02}Z","level":"{level}","msg":"bench","n":{count}}}"#,
                    count % 60,
                );
                if now.duration_since(last_flush) >= Duration::from_secs(1) {
                    let _ = w.flush();
                    last_flush = now;
                }
                count += 1;
                written.fetch_add(1, Ordering::Relaxed);
                next += interval;
            } else {
                let remaining = next - now;
                if remaining > Duration::from_micros(200) {
                    std::thread::sleep(remaining / 2);
                } else {
                    std::hint::spin_loop();
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Linux /proc helpers
// ---------------------------------------------------------------------------

/// Read RSS from `/proc/{pid}/status` (KiB).
/// Returns `None` if the process has exited or the file cannot be parsed.
fn read_rss_kb(pid: u32) -> Option<u64> {
    let content = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.split_whitespace().next()?.parse().ok();
        }
    }
    None
}

/// Read cumulative CPU ticks (utime + stime) from `/proc/{pid}/stat`.
/// Returns `None` if the process has exited or the file cannot be parsed.
fn read_proc_ticks(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The format is: `pid (comm) state ppid ... utime stime ...`
    // The process name (comm) may contain spaces and parentheses, so we find
    // the *last* ')' to locate the end of the comm field.
    let after_name = stat.rfind(')')?;
    let mut fields = stat[after_name + 2..].split_whitespace();
    // Skip: state(0) ppid(1) pgrp(2) session(3) tty_nr(4) tpgid(5)
    //        flags(6) minflt(7) cminflt(8) majflt(9) cmajflt(10)
    for _ in 0..11 {
        fields.next()?;
    }
    let utime: u64 = fields.next()?.parse().ok()?;
    let stime: u64 = fields.next()?.parse().ok()?;
    Some(utime + stime)
}

// ---------------------------------------------------------------------------
// Output formatters
// ---------------------------------------------------------------------------

/// Print results as a Markdown table to stdout.
pub fn print_rate_results_markdown(results: &[RateBenchResult]) {
    println!("### Low and Slow Ingest Benchmark\n");
    println!(
        "Resource usage sampled over {MEASURE_SECS}s steady-state at each ingest rate \
         (logfwd only, passthrough scenario).\n"
    );
    println!("| Target (eps) | Actual (eps) | RSS Memory | CPU % |");
    println!("|:------------:|:------------:|:----------:|:-----:|");
    for r in results {
        let target = if r.eps == 0 {
            "unlimited".to_string()
        } else {
            r.eps.to_string()
        };
        println!(
            "| {:>12} | {:>12.1} | {:>7} KiB | {:>5.1}% |",
            target, r.actual_eps, r.rss_kb, r.cpu_percent
        );
    }
    println!();
}

/// Print results as a plain-text table to stdout.
pub fn print_rate_results_table(results: &[RateBenchResult]) {
    println!("===========================================");
    println!("  Low and Slow Ingest Benchmark");
    println!("===========================================");
    println!(
        "  {:<14} {:<14} {:<14} {:<10}",
        "Target (eps)", "Actual (eps)", "RSS (KiB)", "CPU %"
    );
    println!(
        "  {:<14} {:<14} {:<14} {:<10}",
        "------------", "------------", "---------", "-----"
    );
    for r in results {
        let target = if r.eps == 0 {
            "unlimited".to_string()
        } else {
            r.eps.to_string()
        };
        println!(
            "  {:<14} {:<14.1} {:<14} {:<10.1}",
            target, r.actual_eps, r.rss_kb, r.cpu_percent
        );
    }
    println!("===========================================");
}

/// Write results in github-action-benchmark's `customSmallerIsBetter` format
/// (RSS and CPU are resources — smaller is better for tracking regressions).
pub fn write_rate_gh_bench_json(results: &[RateBenchResult], path: &Path) {
    #[derive(serde::Serialize)]
    struct Entry {
        name: String,
        unit: String,
        value: f64,
    }

    let entries: Vec<Entry> = results
        .iter()
        .flat_map(|r| {
            let tag = if r.eps == 0 {
                "unlimited".to_string()
            } else {
                format!("{}", r.eps)
            };
            [
                Entry {
                    name: format!("rate-bench/{tag}-eps/rss-kb"),
                    unit: "KiB".to_string(),
                    value: r.rss_kb as f64,
                },
                Entry {
                    name: format!("rate-bench/{tag}-eps/cpu-pct"),
                    unit: "%".to_string(),
                    value: r.cpu_percent,
                },
            ]
        })
        .collect();

    let json = serde_json::to_string_pretty(&entries).unwrap();
    match std::fs::write(path, json) {
        Ok(()) => eprintln!("rate-bench gh-bench JSON written to {}", path.display()),
        Err(e) => eprintln!("ERROR: failed to write rate-bench gh-bench JSON: {e}"),
    }
}

/// Serialise results to a JSON report file.
pub fn write_rate_json_file(results: &[RateBenchResult], path: &Path) {
    #[derive(serde::Serialize)]
    struct Report<'a> {
        timestamp: String,
        commit: String,
        warmup_secs: u64,
        measure_secs: u64,
        results: &'a [RateBenchResult],
    }

    let commit = std::env::var("GITHUB_SHA")
        .or_else(|_| {
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .output()
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        })
        .unwrap_or_default();

    let report = Report {
        timestamp: crate::utc_timestamp(),
        commit,
        warmup_secs: WARMUP_SECS,
        measure_secs: MEASURE_SECS,
        results,
    };

    let json = serde_json::to_string_pretty(&report).unwrap();
    match std::fs::write(path, json) {
        Ok(()) => eprintln!("rate-bench JSON written to {}", path.display()),
        Err(e) => eprintln!("ERROR: failed to write rate-bench JSON: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn avg_u64(v: &[u64]) -> u64 {
    if v.is_empty() {
        0
    } else {
        v.iter().sum::<u64>() / v.len() as u64
    }
}

fn avg_f64(v: &[f64]) -> f64 {
    if v.is_empty() {
        0.0
    } else {
        v.iter().sum::<f64>() / v.len() as f64
    }
}

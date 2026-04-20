//! Reads criterion JSON results from target/criterion/ and emits a markdown table.
//!
//! Usage: cargo run -p logfwd-bench [criterion_dir]
#![allow(clippy::print_stdout, clippy::print_stderr)]
//!   Default criterion_dir: target/criterion

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Deserialize)]
struct BenchmarkJson {
    group_id: String,
    function_id: Option<String>,
    value_str: Option<String>,
    throughput: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct EstimatesJson {
    median: Estimate,
}

#[derive(Deserialize)]
struct Estimate {
    point_estimate: f64,
    confidence_interval: ConfidenceInterval,
}

#[derive(Deserialize)]
struct ConfidenceInterval {
    lower_bound: f64,
    upper_bound: f64,
}

struct BenchResult {
    name: String,
    median_ns: f64,
    low_ns: f64,
    high_ns: f64,
    throughput_bytes: Option<u64>,
}

fn format_time(ns: f64) -> String {
    if ns >= 1_000_000_000.0 {
        format!("{:.2} s", ns / 1_000_000_000.0)
    } else if ns >= 1_000_000.0 {
        format!("{:.2} ms", ns / 1_000_000.0)
    } else if ns >= 1_000.0 {
        format!("{:.1} us", ns / 1_000.0)
    } else {
        format!("{ns:.0} ns")
    }
}

fn format_throughput(ns: f64, bytes: u64) -> String {
    let bytes_per_sec = bytes as f64 / (ns / 1_000_000_000.0);
    if bytes_per_sec >= 1_073_741_824.0 {
        format!("{:.2} GiB/s", bytes_per_sec / 1_073_741_824.0)
    } else {
        format!("{:.0} MiB/s", bytes_per_sec / 1_048_576.0)
    }
}

fn format_rate(ns: f64, count: u64) -> String {
    let rate = count as f64 / (ns / 1_000_000_000.0);
    if rate >= 1_000_000.0 {
        format!("{:.1}M lines/s", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.0}K lines/s", rate / 1_000.0)
    } else {
        format!("{rate:.0} lines/s")
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("Usage: logfwd-bench [criterion_dir]");
        println!("  Default criterion_dir: target/criterion");
        return;
    }

    let criterion_dir = args
        .get(1)
        .map_or_else(|| PathBuf::from("target/criterion"), PathBuf::from);

    if !criterion_dir.exists() {
        eprintln!(
            "No criterion results at {}. Run `cargo bench -p logfwd-bench` first.",
            criterion_dir.display()
        );
        std::process::exit(1);
    }

    let mut groups: BTreeMap<String, Vec<BenchResult>> = BTreeMap::new();
    collect_results(&criterion_dir, &mut groups);

    if groups.is_empty() {
        println!("> No benchmark results found.");
        return;
    }

    for (group, benches) in &groups {
        println!("\n### {group}\n");
        println!("| Benchmark | Time (median) | Range | Throughput |");
        println!("|-----------|-----:|------:|----------:|");
        for b in benches {
            let time = format_time(b.median_ns);
            let range = format!("{}–{}", format_time(b.low_ns), format_time(b.high_ns));
            let tp = match b.throughput_bytes {
                Some(bytes) => format_throughput(b.median_ns, bytes),
                None => b
                    .name
                    .rsplit('/')
                    .next()
                    .and_then(|s| s.parse::<u64>().ok())
                    .map_or_else(|| "—".to_string(), |n| format_rate(b.median_ns, n)),
            };
            println!("| {} | {} | {} | {} |", b.name, time, range, tp);
        }
    }
}

fn collect_results(dir: &PathBuf, groups: &mut BTreeMap<String, Vec<BenchResult>>) {
    let mut bench_files = Vec::new();
    find_bench_files(dir, &mut bench_files);

    for bench_json_path in bench_files {
        let new_dir = bench_json_path.parent().unwrap();
        let estimates_path = new_dir.join("estimates.json");
        if !estimates_path.exists() {
            continue;
        }

        let bench: BenchmarkJson = match read_json(&bench_json_path) {
            Some(b) => b,
            None => continue,
        };
        let estimates: EstimatesJson = match read_json(&estimates_path) {
            Some(e) => e,
            None => continue,
        };

        let display_name = if let (Some(func), Some(val)) = (&bench.function_id, &bench.value_str) {
            format!("{func}/{val}")
        } else if let Some(func) = &bench.function_id {
            func.clone()
        } else {
            bench.group_id.clone()
        };

        // Throughput JSON is {"Bytes": N} or {"Elements": N} or null.
        let throughput_bytes = bench
            .throughput
            .as_ref()
            .and_then(|v| v.get("Bytes"))
            .and_then(serde_json::Value::as_u64);

        groups
            .entry(bench.group_id.clone())
            .or_default()
            .push(BenchResult {
                name: display_name,
                median_ns: estimates.median.point_estimate,
                low_ns: estimates.median.confidence_interval.lower_bound,
                high_ns: estimates.median.confidence_interval.upper_bound,
                throughput_bytes,
            });
    }
}

/// Recursively find all `new/benchmark.json` files under a directory.
fn find_bench_files(dir: &PathBuf, results: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            find_bench_files(&path, results);
        } else if path.file_name().is_some_and(|n| n == "benchmark.json")
            && path
                .parent()
                .is_some_and(|p| p.file_name().is_some_and(|n| n == "new"))
        {
            results.push(path);
        }
    }
}

fn read_json<T: serde::de::DeserializeOwned>(path: &PathBuf) -> Option<T> {
    let data = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&data).ok()
}

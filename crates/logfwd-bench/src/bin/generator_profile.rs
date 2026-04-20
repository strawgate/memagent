#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::time::Instant;

use logfwd_bench::generators::cloudtrail::gen_cloudtrail_batch_with_profile;
use logfwd_bench::generators::{
    CloudTrailProfile, EnvoyAccessProfile, gen_envoy_access_batch_with_profile, gen_narrow_batch,
    gen_production_mixed_batch, gen_wide_batch,
};

const DEFAULT_LINES: usize = 100_000;
const DEFAULT_ITERATIONS: usize = 20;
const DEFAULT_SEED: u64 = 42;

#[derive(Clone, Copy)]
struct Case {
    name: &'static str,
    rows: usize,
    run: fn(usize, u64),
}

fn run_cloudtrail(rows: usize, seed: u64) {
    let profile = CloudTrailProfile::benchmark_default();
    let _ = std::hint::black_box(gen_cloudtrail_batch_with_profile(rows, seed, profile));
}

fn run_envoy(rows: usize, seed: u64) {
    let profile = EnvoyAccessProfile::benchmark();
    let _ = std::hint::black_box(gen_envoy_access_batch_with_profile(rows, seed, profile));
}

fn run_narrow(rows: usize, seed: u64) {
    let _ = std::hint::black_box(gen_narrow_batch(rows, seed));
}

fn run_wide(rows: usize, seed: u64) {
    let _ = std::hint::black_box(gen_wide_batch(rows, seed));
}

fn run_production_mixed(rows: usize, seed: u64) {
    let _ = std::hint::black_box(gen_production_mixed_batch(rows, seed));
}

fn parse_usize(args: &[String], key: &str, default: usize) -> usize {
    args.windows(2)
        .find_map(|w| {
            if w[0] == key {
                w[1].parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(default)
}

fn parse_u64(args: &[String], key: &str, default: u64) -> u64 {
    args.windows(2)
        .find_map(|w| {
            if w[0] == key {
                w[1].parse::<u64>().ok()
            } else {
                None
            }
        })
        .unwrap_or(default)
}

fn parse_name_filter(args: &[String]) -> Option<String> {
    args.windows(2).find_map(|w| {
        if w[0] == "--only" {
            Some(w[1].to_ascii_lowercase())
        } else {
            None
        }
    })
}

fn print_help() {
    println!("Usage: generator_profile [--lines N] [--iterations N] [--seed N] [--only NAME]");
    println!("  --lines N       row count for most generators (default: {DEFAULT_LINES})");
    println!("  --iterations N  iterations per generator (default: {DEFAULT_ITERATIONS})");
    println!("  --seed N        base seed (default: {DEFAULT_SEED})");
    println!(
        "  --only NAME     run one case (cloudtrail_audit | envoy_access | infra_mixed | app_minimal | infra_wide)"
    );
}

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_help();
        return;
    }

    let lines = parse_usize(&args, "--lines", DEFAULT_LINES);
    let iterations = parse_usize(&args, "--iterations", DEFAULT_ITERATIONS).max(1);
    let seed = parse_u64(&args, "--seed", DEFAULT_SEED);
    let only = parse_name_filter(&args);

    // Wide has much larger rows; cap default rows to avoid skewing memory pressure.
    let wide_rows = lines.min(25_000);

    let cases = [
        Case {
            name: "cloudtrail_audit",
            rows: lines,
            run: run_cloudtrail,
        },
        Case {
            name: "envoy_access",
            rows: lines,
            run: run_envoy,
        },
        Case {
            name: "infra_mixed",
            rows: lines,
            run: run_production_mixed,
        },
        Case {
            name: "app_minimal",
            rows: lines,
            run: run_narrow,
        },
        Case {
            name: "infra_wide",
            rows: wide_rows,
            run: run_wide,
        },
    ];

    println!(
        "batch-generator-profile lines={lines} iterations={iterations} seed={seed} wide_rows={wide_rows}"
    );

    for case in cases {
        if let Some(ref only_filter) = only
            && case.name != only_filter
        {
            continue;
        }

        let started = Instant::now();
        for i in 0..iterations {
            (case.run)(case.rows, seed.wrapping_add(i as u64));
        }
        let elapsed = started.elapsed();
        let total_rows = case.rows.saturating_mul(iterations);
        let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
        println!(
            "case={} rows={} iterations={} elapsed_ms={:.3} rows_per_sec={:.1}",
            case.name,
            case.rows,
            iterations,
            elapsed.as_secs_f64() * 1000.0,
            rows_per_sec
        );
    }
}

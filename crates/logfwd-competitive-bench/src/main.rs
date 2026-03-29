//! Competitive benchmark harness: logfwd vs vector vs fluent-bit vs filebeat
//! vs otelcol-contrib vs vlagent.
//!
//! Downloads competitor binaries into a temp directory, generates a JSON log
//! file, then runs each agent against a blackhole HTTP sink and measures
//! wall-clock throughput.
//!
//! Usage:
//!   cargo run -p logfwd-competitive-bench --release -- [OPTIONS]
//!
//!   --lines N        Number of JSON log lines to generate (default: 5_000_000)
//!   --agents A,B,C   Comma-separated agents to run (default: all available)
//!   --markdown        Output results as markdown table
//!   --no-download    Only use agents already on PATH

mod agents;
mod blackhole;
mod datagen;
mod download;
mod fake_k8s;
mod runner;

use std::path::PathBuf;
use std::process;

use runner::{BenchContext, BenchResult};

use crate::agents::{Agent, all_agents};

fn main() {
    let args = Args::parse();

    let bench_dir = tempfile::tempdir().unwrap_or_else(|e| {
        eprintln!("ERROR: failed to create temp dir: {e}");
        process::exit(1);
    });
    let bin_dir = bench_dir.path().join("bin");
    std::fs::create_dir_all(&bin_dir).unwrap_or_else(|e| {
        eprintln!("ERROR: failed to create bin dir: {e}");
        process::exit(1);
    });

    // Resolve which agents to run.
    let all = all_agents();
    let agents: Vec<&dyn Agent> = if args.agents.is_empty() {
        all.iter().map(|a| a.as_ref()).collect()
    } else {
        let mut selected = Vec::new();
        for name in &args.agents {
            match all.iter().find(|a| a.name() == name.as_str()) {
                Some(a) => selected.push(a.as_ref()),
                None => {
                    eprintln!("Unknown agent: {name}");
                    eprintln!(
                        "Available: {}",
                        all.iter().map(|a| a.name()).collect::<Vec<_>>().join(", ")
                    );
                    process::exit(1);
                }
            }
        }
        selected
    };

    // Resolve binaries: env override → PATH → download.
    eprintln!("=== Resolving agent binaries ===");
    let mut available: Vec<(&dyn Agent, PathBuf)> = Vec::new();
    for agent in &agents {
        match resolve_binary(agent, &bin_dir, args.no_download) {
            Some(path) => {
                eprintln!("  {:<14} {}", format!("{}:", agent.name()), path.display());
                available.push((*agent, path));
            }
            None => {
                eprintln!("  {:<14} (skipped)", format!("{}:", agent.name()));
            }
        }
    }
    eprintln!();

    if available.is_empty() {
        eprintln!("No agents available. Nothing to benchmark.");
        process::exit(1);
    }

    // Generate test data.
    let data_file = bench_dir.path().join("test.jsonl");
    eprintln!("=== Generating {} JSON log lines ===", args.lines);
    let file_size = datagen::generate_json_log_file(args.lines, &data_file).unwrap_or_else(|e| {
        eprintln!("ERROR: failed to generate test data: {e}");
        process::exit(1);
    });
    eprintln!(
        "  File: {} ({:.1} MB)",
        data_file.display(),
        file_size as f64 / 1_048_576.0
    );
    eprintln!();

    // Start blackhole.
    let blackhole_addr = "127.0.0.1:19877";
    let logfwd_bin = available
        .iter()
        .find(|(a, _)| a.name() == "logfwd")
        .map(|(_, p)| p.clone());

    let agent_names: Vec<_> = available.iter().map(|(a, _)| a.name()).collect();
    eprintln!("=== Agents: {} ===", agent_names.join(","));
    eprintln!("=== Blackhole sink: http://{blackhole_addr} ===");
    eprintln!();

    // Start blackhole once, reuse for all agents.
    let blackhole = blackhole::Blackhole::start(blackhole_addr).unwrap_or_else(|e| {
        eprintln!("ERROR: failed to start blackhole: {e}");
        process::exit(1);
    });

    // Wait for blackhole to be ready.
    for _ in 0..50 {
        if ureq::get(&format!("http://{blackhole_addr}/stats"))
            .call()
            .is_ok()
        {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Run benchmarks.
    let ctx = BenchContext {
        bench_dir: bench_dir.path().to_path_buf(),
        data_file,
        blackhole_addr: blackhole_addr.to_string(),
        lines: args.lines,
        logfwd_binary: logfwd_bin,
    };

    let mut results: Vec<BenchResult> = Vec::new();
    for (agent, binary) in &available {
        match runner::run_agent(*agent, binary, &ctx, &blackhole) {
            Ok(result) => {
                print_result_stderr(&result, args.lines);
                results.push(result);
            }
            Err(e) => {
                eprintln!("--- {} ---", agent.name());
                eprintln!("  ERROR: {e}");
                eprintln!();
                results.push(BenchResult {
                    name: agent.name().to_string(),
                    lines_done: 0,
                    elapsed_ms: 0,
                });
            }
        }
    }

    // Output summary.
    if args.markdown {
        print_markdown(&results, args.lines, file_size);
    } else {
        print_table(&results, args.lines, file_size);
    }
}

// ---------------------------------------------------------------------------
// Binary resolution
// ---------------------------------------------------------------------------

fn resolve_binary(agent: &&dyn Agent, bin_dir: &std::path::Path, no_download: bool) -> Option<PathBuf> {
    // Check env override.
    let env_var = agent.name().to_uppercase().replace('-', "_");
    if let Ok(val) = std::env::var(&env_var) {
        let p = PathBuf::from(&val);
        if p.exists() {
            return Some(p);
        }
    }

    // Check PATH.
    if let Ok(output) = std::process::Command::new("which")
        .arg(agent.binary_name())
        .output()
        && output.status.success()
    {
        let path_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !path_str.is_empty() {
            return Some(PathBuf::from(path_str));
        }
    }

    if no_download {
        return None;
    }

    // Download.
    let (os, arch) = download::platform();
    match agent.download_url(&os, &arch) {
        Some(url) => match download::download_and_extract(&url, agent.binary_name(), bin_dir) {
            Ok(path) => Some(path),
            Err(e) => {
                eprintln!("  download failed: {e}");
                None
            }
        },
        None => None,
    }
}

// ---------------------------------------------------------------------------
// Output formatting
// ---------------------------------------------------------------------------

fn fmt_rate(lines: usize, ms: u64) -> String {
    if ms == 0 {
        return "N/A".to_string();
    }
    let lps = lines as f64 / (ms as f64 / 1000.0);
    if lps >= 1_000_000.0 {
        format!("{:.2}M lines/sec", lps / 1_000_000.0)
    } else if lps >= 1_000.0 {
        format!("{:.0}K lines/sec", lps / 1_000.0)
    } else {
        format!("{:.0} lines/sec", lps)
    }
}

fn print_result_stderr(result: &BenchResult, total_lines: usize) {
    eprintln!("--- {} ---", result.name);
    if result.lines_done == 0 && result.elapsed_ms == 0 {
        eprintln!("  FAILED");
    } else {
        let rate = fmt_rate(total_lines, result.elapsed_ms);
        eprintln!("  Lines: {} / {}", result.lines_done, total_lines);
        eprintln!("  Time:  {}ms", result.elapsed_ms);
        eprintln!("  Rate:  {rate}");
    }
    eprintln!();
}

fn print_markdown(results: &[BenchResult], lines: usize, file_size: u64) {
    let mb = file_size as f64 / 1_048_576.0;
    println!("### Competitive Throughput ({lines} lines, {mb:.1} MB)\n");
    println!("| Agent | Time | Throughput |");
    println!("|-------|-----:|-----------:|");
    for r in results {
        let rate = if r.lines_done == 0 && r.elapsed_ms == 0 {
            "FAILED".to_string()
        } else {
            fmt_rate(lines, r.elapsed_ms)
        };
        println!("| {} | {}ms | {} |", r.name, r.elapsed_ms, rate);
    }

    // Comparison ratios.
    if results.len() > 1 && results[0].elapsed_ms > 0 {
        println!();
        let base = &results[0];
        for r in &results[1..] {
            if r.elapsed_ms > 0 {
                let ratio = r.elapsed_ms as f64 / base.elapsed_ms as f64;
                println!(
                    "> **{}** is **{:.1}x faster** than {}",
                    base.name, ratio, r.name
                );
            }
        }
    }
}

fn print_table(results: &[BenchResult], lines: usize, file_size: u64) {
    let mb = file_size as f64 / 1_048_576.0;
    println!("===========================================");
    println!("  RESULTS ({lines} lines, {mb:.1} MB)");
    println!("===========================================");
    println!("  {:<16} {:>10} {:>20}", "Agent", "Time", "Throughput");
    println!("  {:<16} {:>10} {:>20}", "-----", "----", "----------");

    for r in results {
        let rate = if r.lines_done == 0 && r.elapsed_ms == 0 {
            "FAILED".to_string()
        } else {
            fmt_rate(lines, r.elapsed_ms)
        };
        println!("  {:<16} {:>8}ms {:>20}", r.name, r.elapsed_ms, rate);
    }
    println!("===========================================");

    if results.len() > 1 && results[0].elapsed_ms > 0 {
        println!();
        let base = &results[0];
        for r in &results[1..] {
            if r.elapsed_ms > 0 {
                let ratio = r.elapsed_ms as f64 / base.elapsed_ms as f64;
                println!("  {} is {:.1}x vs {}", base.name, ratio, r.name);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Args
// ---------------------------------------------------------------------------

struct Args {
    lines: usize,
    agents: Vec<String>,
    markdown: bool,
    no_download: bool,
}

impl Args {
    fn parse() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let mut result = Args {
            lines: 5_000_000,
            agents: Vec::new(),
            markdown: false,
            no_download: false,
        };

        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--lines" => {
                    i += 1;
                    result.lines = args[i].parse().expect("invalid --lines value");
                }
                "--agents" => {
                    i += 1;
                    result.agents = args[i].split(',').map(|s| s.to_string()).collect();
                }
                "--markdown" => result.markdown = true,
                "--no-download" => result.no_download = true,
                other => {
                    eprintln!("Unknown argument: {other}");
                    eprintln!("Usage: logfwd-competitive-bench [--lines N] [--agents a,b] [--markdown] [--no-download]");
                    process::exit(1);
                }
            }
            i += 1;
        }
        result
    }
}

//! Competitive benchmark harness: logfwd vs vector vs fluent-bit vs filebeat
//! vs otelcol-contrib vs vlagent.
//!
//! Downloads competitor binaries (or pulls Docker images) into a temp directory,
//! generates a JSON log file, then runs each agent against a blackhole HTTP sink
//! and measures wall-clock throughput.
//!
//! Usage:
//!   cargo run -p logfwd-competitive-bench --release -- \[OPTIONS]
//!
//!   --lines N        Number of JSON log lines to generate (default: 5_000_000)
//!   --agents A,B,C   Comma-separated agents to run (default: all available)
//!   --scenarios S,S  Scenarios to run (default: passthrough)
//!                    Available: passthrough, json_parse, filter
//!   --markdown       Output results as markdown table
//!   --json           Output structured JSON (for CI dashboards)
//!   --no-download        Only use agents already on PATH (binary mode only)
//!   --docker             Run agents in Docker containers with resource limits
//!   --cpus N             CPU limit per container (default: 1, Docker mode only)
//!   --memory N           Memory limit per container (default: 1g, Docker mode only)
//!   --profile DIR        Write CPU flamegraph + memory profile to DIR
//!   --dhat-binary PATH   logfwd binary built with --features dhat-heap
#![allow(clippy::print_stdout, clippy::print_stderr)]

mod agents;
mod blackhole;
mod datagen;
mod download;
mod fake_k8s;
mod rate_bench;
mod runner;
mod summarize;

use std::io::Write;
use std::path::PathBuf;
use std::process;

use runner::{BenchContext, BenchResult, DockerLimits};

use crate::agents::{Agent, Scenario, all_agents};

fn main() {
    let args = Args::parse();

    // Handle subcommands.
    if let Some(ref cmd) = args.subcommand {
        match cmd.as_str() {
            "summarize" => {
                let dir = args.results_dir.as_ref().unwrap_or_else(|| {
                    eprintln!("ERROR: summarize requires --results-dir DIR");
                    process::exit(1);
                });
                summarize::run(
                    dir,
                    args.markdown,
                    args.gh_bench_file.as_deref(),
                    args.dashboard_file.as_deref(),
                )
                .unwrap_or_else(|e| {
                    eprintln!("ERROR: {e}");
                    process::exit(1);
                });
                return;
            }
            other => {
                eprintln!("Unknown subcommand: {other}");
                process::exit(1);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Rate-ingest benchmark: non-competitive, ff only.
    // -----------------------------------------------------------------------
    if args.rate_bench {
        run_rate_bench_main(&args);
        return;
    }

    let needs_docker = args.docker || args.mode == "docker" || args.mode == "both";
    if needs_docker && !runner::docker_available() {
        eprintln!("ERROR: --docker requires Docker to be installed and running");
        process::exit(1);
    }

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
        all.iter().map(AsRef::as_ref).collect()
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

    // Resolve agent binaries and/or Docker images based on mode.
    let run_binary = args.mode == "binary" || args.mode == "both";
    let run_docker = args.mode == "docker" || args.mode == "both";
    eprintln!("=== Resolving agents (mode: {}) ===", args.mode);

    let mut available: Vec<ResolvedAgent> = Vec::new();
    for agent in &agents {
        let binary = if run_binary {
            resolve_binary(agent, &bin_dir, args.no_download)
        } else {
            None
        };
        let image = if run_docker {
            resolve_docker(agent)
        } else {
            None
        };

        let label = format!("{}:", agent.name());
        match (&binary, &image) {
            (Some(b), Some(img)) => {
                eprintln!("  {label:<14} {} + {img}", b.display());
            }
            (Some(b), None) => eprintln!("  {label:<14} {}", b.display()),
            (None, Some(img)) => eprintln!("  {label:<14} {img}"),
            (None, None) => {
                eprintln!("  {label:<14} (skipped)");
                continue;
            }
        }

        available.push(ResolvedAgent {
            agent: *agent,
            binary,
            image,
        });
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
    let agent_names: Vec<_> = available.iter().map(|r| r.agent.name()).collect();
    eprintln!("=== Agents: {} ===", agent_names.join(","));
    if args.docker {
        eprintln!(
            "=== Docker: --cpus {} --memory {} ===",
            args.docker_limits.cpus, args.docker_limits.memory
        );
    }
    eprintln!("=== Blackhole sink: http://{blackhole_addr} ===");
    eprintln!();

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

    // In Docker on macOS, containers reach the host via host.docker.internal.
    // On Linux with --network=host, localhost works directly.
    let docker_blackhole_addr = if args.docker && cfg!(not(target_os = "linux")) {
        blackhole_addr.replace("127.0.0.1", "host.docker.internal")
    } else {
        blackhole_addr.to_string()
    };

    let ctx = BenchContext {
        bench_dir: bench_dir.path().to_path_buf(),
        data_file,
        blackhole_addr: blackhole_addr.to_string(),
        docker_blackhole_addr,
        lines: args.lines,
    };

    // Run benchmarks across all scenarios and modes.
    let mut results: Vec<BenchResult> = Vec::new();
    let mut results_writer = args.results_file.as_ref().map(|p| {
        let f = std::fs::File::create(p).unwrap_or_else(|e| {
            eprintln!("ERROR: failed to create results file {}: {e}", p.display());
            process::exit(1);
        });
        std::io::BufWriter::new(f)
    });

    for scenario in &args.scenarios {
        eprintln!(
            "=== Scenario: {} ({}) ===",
            scenario.name(),
            scenario.description()
        );
        eprintln!();

        for resolved in &available {
            // vlagent only supports passthrough (CLI-flag based, no transform support).
            if *scenario != Scenario::Passthrough && resolved.agent.name() == "vlagent" {
                eprintln!(
                    "--- {} --- (skipped: {} not supported)",
                    resolved.agent.name(),
                    scenario.name()
                );
                eprintln!();
                continue;
            }

            for iter in 1..=args.iterations.max(1) {
                // Binary mode.
                if let Some(binary) = resolved.binary.as_ref().filter(|_| run_binary) {
                    run_one(
                        resolved.agent,
                        Some(binary),
                        None,
                        &ctx,
                        &blackhole,
                        &args.docker_limits,
                        *scenario,
                        args.lines,
                        iter,
                        &mut results,
                        &mut results_writer,
                    );
                }

                // Docker mode.
                if let Some(image) = resolved.image.as_ref().filter(|_| run_docker) {
                    run_one(
                        resolved.agent,
                        None,
                        Some(image.as_str()),
                        &ctx,
                        &blackhole,
                        &args.docker_limits,
                        *scenario,
                        args.lines,
                        iter,
                        &mut results,
                        &mut results_writer,
                    );
                } else if run_docker && !run_binary {
                    // Docker requested but no image — fall back to binary.
                    if let Some(binary) = &resolved.binary {
                        eprintln!(
                            "  WARN: no Docker image for {}, falling back to binary",
                            resolved.agent.name()
                        );
                        run_one(
                            resolved.agent,
                            Some(binary),
                            None,
                            &ctx,
                            &blackhole,
                            &args.docker_limits,
                            *scenario,
                            args.lines,
                            iter,
                            &mut results,
                            &mut results_writer,
                        );
                    }
                }
            }
        }
    }

    if let Some(ref mut w) = results_writer {
        let _ = w.flush();
    }

    // Output summary.
    if args.json {
        print_json(&results, args.lines, file_size, &args);
    } else if args.markdown {
        print_markdown(&results, args.lines, file_size, &args);
    } else {
        print_table(&results, args.lines, file_size);
    }

    // Optionally write JSON to a file (can be combined with any output mode).
    if let Some(ref json_path) = args.json_file {
        write_json_file(&results, args.lines, file_size, &args, json_path);
    }

    // Optionally write github-action-benchmark format.
    if let Some(ref path) = args.gh_bench_file {
        write_gh_bench_json(&results, args.lines, path);
    }

    // Profiling pass (ff only).
    if let Some(ref profile_dir) = args.profile_dir {
        std::fs::create_dir_all(profile_dir).unwrap_or_else(|e| {
            eprintln!("ERROR: failed to create profile dir: {e}");
            process::exit(1);
        });

        // Find the ff binary.
        let logfwd_resolved = available.iter().find(|r| r.agent.name() == "ff");

        if let Some(resolved) = logfwd_resolved.filter(|r| r.binary.is_some()) {
            let binary = resolved.binary.as_ref().unwrap();
            let logfwd_agent = resolved.agent;

            eprintln!("=== Profiling ff ===");

            // CPU profiling with perf (Linux only).
            if runner::perf_available() {
                eprintln!("--- CPU profile (perf record) ---");
                match runner::run_agent_perf(logfwd_agent, binary, &ctx, &blackhole, profile_dir) {
                    Ok(perf_data) => {
                        if runner::inferno_available() {
                            match runner::generate_flamegraph(&perf_data, profile_dir) {
                                Ok(_) => {}
                                Err(e) => eprintln!("  WARN: flamegraph generation failed: {e}"),
                            }
                        } else {
                            eprintln!(
                                "  WARN: inferno not found, skipping flamegraph (cargo install inferno)"
                            );
                        }
                    }
                    Err(e) => eprintln!("  WARN: perf profiling failed: {e}"),
                }
            } else {
                eprintln!("  SKIP: perf not available (Linux only)");
            }

            // Memory profiling with dhat-heap.
            if let Some(ref dhat_binary) = args.dhat_binary {
                let dhat_path = PathBuf::from(dhat_binary);
                if dhat_path.exists() {
                    eprintln!("--- Memory profile (dhat-heap) ---");
                    match runner::run_agent_dhat(
                        logfwd_agent,
                        &dhat_path,
                        &ctx,
                        &blackhole,
                        profile_dir,
                    ) {
                        Ok(_) => {}
                        Err(e) => eprintln!("  WARN: dhat profiling failed: {e}"),
                    }
                } else {
                    eprintln!("  WARN: dhat binary not found at {dhat_binary}");
                }
            } else {
                eprintln!(
                    "  SKIP: --dhat-binary not set (build with: cargo build --release --features dhat-heap -p logfwd)"
                );
            }

            eprintln!();
        } else {
            eprintln!("WARN: ff binary not available, skipping profiling");
        }
    }
}

// ---------------------------------------------------------------------------
// Rate-ingest benchmark dispatch
// ---------------------------------------------------------------------------

/// Run the low-and-slow rate-ingest benchmark (non-competitive, ff only).
fn run_rate_bench_main(args: &Args) {
    // Locate ff binary via LOGFWD env var or PATH.
    let logfwd_binary = find_logfwd_binary().unwrap_or_else(|| {
        eprintln!(
            "ERROR: ff binary not found. \
             Set the LOGFWD env var or ensure ff is on PATH."
        );
        process::exit(1);
    });
    eprintln!("=== Rate Ingest Benchmark (ff only) ===");
    eprintln!("  binary: {}", logfwd_binary.display());
    eprintln!();

    let bench_dir = tempfile::tempdir().unwrap_or_else(|e| {
        eprintln!("ERROR: failed to create temp dir: {e}");
        process::exit(1);
    });

    // Use a separate blackhole port to avoid collisions with competitive bench.
    let blackhole_addr = "127.0.0.1:19878";
    eprintln!("=== Blackhole sink: http://{blackhole_addr} ===");
    eprintln!();

    // Keep _blackhole alive for the duration of the benchmark; it runs a
    // background server thread that is stopped when the value is dropped.
    let _blackhole = blackhole::Blackhole::start(blackhole_addr).unwrap_or_else(|e| {
        eprintln!("ERROR: failed to start blackhole: {e}");
        process::exit(1);
    });

    // Wait for the blackhole to be ready.
    for _ in 0..50 {
        if ureq::get(&format!("http://{blackhole_addr}/stats"))
            .call()
            .is_ok()
        {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    let results = rate_bench::run_rate_bench(&logfwd_binary, bench_dir.path(), blackhole_addr);

    if results.is_empty() {
        eprintln!("No results collected.");
        process::exit(1);
    }

    // Print results.
    if args.markdown {
        rate_bench::print_rate_results_markdown(&results);
    } else {
        rate_bench::print_rate_results_table(&results);
    }

    // Write JSON file.
    if let Some(ref path) = args.json_file {
        rate_bench::write_rate_json_file(&results, path);
    }

    // Write github-action-benchmark JSON.
    if let Some(ref path) = args.gh_bench_file {
        rate_bench::write_rate_gh_bench_json(&results, path);
    }
}

/// Locate the logfwd binary.  Checks `LOGFWD` env var first, then `PATH`.
fn find_logfwd_binary() -> Option<PathBuf> {
    if let Ok(val) = std::env::var("LOGFWD") {
        let p = PathBuf::from(&val);
        if p.exists() {
            return Some(p);
        }
    }
    if let Ok(output) = process::Command::new("which").arg("ff").output()
        && output.status.success()
    {
        let path_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !path_str.is_empty() {
            return Some(PathBuf::from(path_str));
        }
    }
    None
}

/// Run a single agent benchmark (binary or Docker) and collect the result.
#[expect(clippy::too_many_arguments)]
fn run_one(
    agent: &dyn Agent,
    binary: Option<&std::path::Path>,
    image: Option<&str>,
    ctx: &BenchContext,
    blackhole: &blackhole::Blackhole,
    limits: &DockerLimits,
    scenario: Scenario,
    total_lines: usize,
    iteration: usize,
    results: &mut Vec<BenchResult>,
    jsonl_writer: &mut Option<std::io::BufWriter<std::fs::File>>,
) {
    let mode_label = if image.is_some() { "docker" } else { "binary" };
    let result = if let Some(img) = image {
        runner::run_agent_docker(agent, img, ctx, blackhole, limits, scenario, iteration)
    } else if let Some(bin) = binary {
        runner::run_agent(agent, bin, ctx, blackhole, scenario, iteration)
    } else {
        return;
    };

    match result {
        Ok(r) => {
            print_result_stderr(&r, total_lines);
            if let Some(w) = jsonl_writer {
                match serde_json::to_string(&r) {
                    Ok(json) => {
                        if let Err(e) = writeln!(w, "{json}") {
                            eprintln!("WARN: failed writing JSONL result: {e}");
                        }
                    }
                    Err(e) => eprintln!("WARN: failed to serialize benchmark result: {e}"),
                }
            }
            results.push(r);
        }
        Err(e) => {
            eprintln!("--- {} ({mode_label}) ---", agent.name());
            eprintln!("  ERROR: {e}");
            eprintln!();
            results.push(BenchResult {
                name: agent.name().to_string(),
                scenario,
                mode: mode_label.to_string(),
                lines_done: 0,
                elapsed_ms: 0,
                timed_out: false,
                iteration,
                samples: Vec::new(),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Agent resolution
// ---------------------------------------------------------------------------

struct ResolvedAgent<'a> {
    agent: &'a dyn Agent,
    binary: Option<PathBuf>,
    image: Option<String>,
}

fn resolve_docker(agent: &&dyn Agent) -> Option<String> {
    let image = agent.docker_image()?;
    match runner::docker_pull(&image) {
        Ok(()) => Some(image),
        Err(e) => {
            eprintln!("  {}: {e}", agent.name());
            None
        }
    }
}

fn resolve_binary(
    agent: &&dyn Agent,
    bin_dir: &std::path::Path,
    no_download: bool,
) -> Option<PathBuf> {
    // Check env override.
    let env_var = agent.name().to_uppercase().replace('-', "_");
    if let Ok(val) = std::env::var(&env_var) {
        let p = PathBuf::from(&val);
        if p.exists() {
            return Some(p);
        }
    }

    // Check PATH.
    if let Ok(output) = process::Command::new("which")
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
        format!("{lps:.0} lines/sec")
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

fn print_markdown(results: &[BenchResult], lines: usize, file_size: u64, args: &Args) {
    let mb = file_size as f64 / 1_048_576.0;

    // Group results by scenario.
    for scenario in &args.scenarios {
        let scenario_results: Vec<&BenchResult> =
            results.iter().filter(|r| r.scenario == *scenario).collect();
        if scenario_results.is_empty() {
            continue;
        }

        if args.docker {
            println!(
                "### {} ({lines} lines, {mb:.1} MB, Docker: {} CPU / {} RAM)\n",
                scenario.description(),
                args.docker_limits.cpus,
                args.docker_limits.memory
            );
        } else {
            println!(
                "### {} ({lines} lines, {mb:.1} MB)\n",
                scenario.description()
            );
        }
        println!("| Agent | Mode | Time | Throughput |");
        println!("|-------|------|-----:|-----------:|");
        for r in &scenario_results {
            let rate = if r.lines_done == 0 && r.elapsed_ms == 0 {
                "FAILED".to_string()
            } else {
                fmt_rate(lines, r.elapsed_ms)
            };
            println!(
                "| {} | {} | {}ms | {} |",
                r.name, r.mode, r.elapsed_ms, rate
            );
        }

        if scenario_results.len() > 1 && scenario_results[0].elapsed_ms > 0 {
            println!();
            let base = scenario_results[0];
            for r in &scenario_results[1..] {
                if r.elapsed_ms > 0 {
                    let ratio = r.elapsed_ms as f64 / base.elapsed_ms as f64;
                    println!(
                        "> **{}** is **{:.1}x faster** than {}",
                        base.name, ratio, r.name
                    );
                }
            }
        }
        println!();
    }
}

fn print_json(results: &[BenchResult], lines: usize, file_size: u64, args: &Args) {
    println!("{}", build_json_report(results, lines, file_size, args));
}

fn build_json_report(results: &[BenchResult], lines: usize, file_size: u64, args: &Args) -> String {
    #[derive(serde::Serialize)]
    struct JsonReport<'a> {
        timestamp: String,
        commit: String,
        lines: usize,
        file_size_bytes: u64,
        docker: bool,
        cpus: &'a str,
        memory: &'a str,
        results: &'a [BenchResult],
    }

    let commit = std::env::var("GITHUB_SHA")
        .or_else(|_| {
            process::Command::new("git")
                .args(["rev-parse", "HEAD"])
                .output()
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        })
        .unwrap_or_default();

    let report = JsonReport {
        timestamp: utc_timestamp(),
        commit,
        lines,
        file_size_bytes: file_size,
        docker: args.docker,
        cpus: &args.docker_limits.cpus,
        memory: &args.docker_limits.memory,
        results,
    };

    serde_json::to_string_pretty(&report).unwrap()
}

/// Returns current UTC time as ISO 8601 string.
pub fn utc_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    // Manual UTC breakdown — no external crate needed.
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let h = time_secs / 3600;
    let m = (time_secs % 3600) / 60;
    let s = time_secs % 60;
    // Days since 1970-01-01 → year/month/day.
    let (y, mo, d) = days_to_ymd(days);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

fn days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    // Civil calendar conversion from days since epoch.
    let mut y = 1970;
    loop {
        let yd = if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
            366
        } else {
            365
        };
        if days < yd {
            break;
        }
        days -= yd;
        y += 1;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let mdays = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut mo = 0;
    for md in &mdays {
        if days < *md {
            break;
        }
        days -= md;
        mo += 1;
    }
    (y, mo + 1, days + 1)
}

/// Write results in github-action-benchmark's `customBiggerIsBetter` format.
fn write_gh_bench_json(results: &[BenchResult], lines: usize, path: &std::path::Path) {
    #[derive(serde::Serialize)]
    struct Entry {
        name: String,
        unit: String,
        value: f64,
    }

    let entries: Vec<Entry> = results
        .iter()
        .filter(|r| r.elapsed_ms > 0)
        .map(|r| {
            let lps = lines as f64 / (r.elapsed_ms as f64 / 1000.0);
            Entry {
                name: format!("{}/{} ({})", r.scenario.name(), r.name, r.mode),
                unit: "lines/sec".to_string(),
                value: lps,
            }
        })
        .collect();

    let json = serde_json::to_string_pretty(&entries).unwrap();
    match std::fs::write(path, json) {
        Ok(()) => eprintln!("github-action-benchmark JSON written to {}", path.display()),
        Err(e) => eprintln!("ERROR: failed to write gh-bench JSON: {e}"),
    }
}

fn write_json_file(
    results: &[BenchResult],
    lines: usize,
    file_size: u64,
    args: &Args,
    path: &std::path::Path,
) {
    let json = build_json_report(results, lines, file_size, args);
    match std::fs::write(path, json) {
        Ok(()) => eprintln!("JSON results written to {}", path.display()),
        Err(e) => eprintln!("ERROR: failed to write JSON to {}: {e}", path.display()),
    }
}

fn print_table(results: &[BenchResult], lines: usize, file_size: u64) {
    let mb = file_size as f64 / 1_048_576.0;

    // Collect unique scenarios from results.
    let scenarios: Vec<Scenario> =
        results
            .iter()
            .map(|r| r.scenario)
            .fold(Vec::new(), |mut seen, scenario| {
                if !seen.contains(&scenario) {
                    seen.push(scenario);
                }
                seen
            });

    for scenario in &scenarios {
        let scenario_results: Vec<&BenchResult> =
            results.iter().filter(|r| r.scenario == *scenario).collect();

        println!("===========================================");
        println!("  {} ({lines} lines, {mb:.1} MB)", scenario.description());
        println!("===========================================");
        println!(
            "  {:<16} {:<8} {:>10} {:>20}",
            "Agent", "Mode", "Time", "Throughput"
        );
        println!(
            "  {:<16} {:<8} {:>10} {:>20}",
            "-----", "----", "----", "----------"
        );

        for r in &scenario_results {
            let rate = if r.lines_done == 0 && r.elapsed_ms == 0 {
                "FAILED".to_string()
            } else {
                fmt_rate(lines, r.elapsed_ms)
            };
            println!(
                "  {:<16} {:<8} {:>8}ms {:>20}",
                r.name, r.mode, r.elapsed_ms, rate
            );
        }
        println!("===========================================");

        if scenario_results.len() > 1 && scenario_results[0].elapsed_ms > 0 {
            println!();
            let base = scenario_results[0];
            for r in &scenario_results[1..] {
                if r.elapsed_ms > 0 {
                    let ratio = r.elapsed_ms as f64 / base.elapsed_ms as f64;
                    println!("  {} is {:.1}x vs {}", base.name, ratio, r.name);
                }
            }
        }
        println!();
    }
}

// ---------------------------------------------------------------------------
// Args
// ---------------------------------------------------------------------------

fn detect_subcommand(args: &[String]) -> Option<(String, usize)> {
    args.iter()
        .enumerate()
        .skip(1)
        .find(|(_, arg)| matches!(arg.as_str(), "summarize"))
        .map(|(idx, arg)| (arg.clone(), idx))
}

struct Args {
    lines: usize,
    agents: Vec<String>,
    scenarios: Vec<Scenario>,
    /// Execution mode: "binary", "docker", or "both".
    mode: String,
    markdown: bool,
    json: bool,
    /// Write JSON results to a file (can be combined with --markdown).
    json_file: Option<PathBuf>,
    /// Write github-action-benchmark format to a file.
    gh_bench_file: Option<PathBuf>,
    no_download: bool,
    docker: bool,
    docker_limits: DockerLimits,
    /// Directory to write CPU/memory profiles into.
    profile_dir: Option<PathBuf>,
    /// Path to a logfwd binary built with --features dhat-heap.
    dhat_binary: Option<String>,
    /// Run the low-and-slow rate-ingest benchmark instead of the competitive bench.
    rate_bench: bool,
    /// Number of iterations per agent/scenario (default: 1).
    iterations: usize,
    /// Write per-run JSONL results to a file.
    results_file: Option<PathBuf>,
    /// Subcommand: "summarize" reads result files from matrix cells.
    subcommand: Option<String>,
    /// Positional index where subcommand was found.
    subcommand_index: Option<usize>,
    /// Directory containing result artifacts for summarize subcommand.
    results_dir: Option<PathBuf>,
    /// Write dashboard data JSON.
    dashboard_file: Option<PathBuf>,
}

impl Args {
    fn parse() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let mut result = Args {
            lines: 5_000_000,
            agents: Vec::new(),
            scenarios: Vec::new(),
            mode: "binary".to_string(),
            markdown: false,
            json: false,
            json_file: None,
            gh_bench_file: None,
            no_download: false,
            docker: false,
            docker_limits: DockerLimits::default(),
            profile_dir: None,
            dhat_binary: None,
            rate_bench: false,
            iterations: 1,
            results_file: None,
            subcommand: None,
            subcommand_index: None,
            results_dir: None,
            dashboard_file: None,
        };

        // Check for subcommand as first non-flag argument anywhere.
        if let Some((subcommand, idx)) = detect_subcommand(&args) {
            result.subcommand = Some(subcommand);
            result.subcommand_index = Some(idx);
        }

        let mut i = 1;
        while i < args.len() {
            let arg = &args[i];
            if arg == "--help" || arg == "-h" {
                eprintln!("Usage: logfwd-competitive-bench [OPTIONS]");
                eprintln!();
                eprintln!("  --lines N            Lines to generate (default: 5000000)");
                eprintln!("  --agents A,B,C       Agents to run (default: all)");
                eprintln!("  --scenarios S,S      Scenarios to run (default: passthrough)");
                eprintln!("                       Available: passthrough, json_parse, filter");
                eprintln!("  --markdown           Output markdown table");
                eprintln!("  --json               Output structured JSON (for CI dashboards)");
                eprintln!(
                    "  --json-file PATH     Write JSON results to file (combinable with --markdown)"
                );
                eprintln!("  --no-download        Skip binary downloads");
                eprintln!(
                    "  --mode M             Execution mode: binary, docker, both (default: binary)"
                );
                eprintln!("  --docker             Enable Docker (sets mode to docker)");
                eprintln!("  --cpus N             CPU limit per container (default: 1)");
                eprintln!("  --memory N           Memory limit per container (default: 1g)");
                eprintln!("  --profile DIR        Write CPU/memory profiles to DIR");
                eprintln!("  --dhat-binary PATH   logfwd binary built with --features dhat-heap");
                eprintln!(
                    "  --rate-bench         Run low-and-slow rate-ingest benchmark (ff only)"
                );
                process::exit(0);
            }
            match arg.as_str() {
                "--lines" => {
                    i += 1;
                    if i >= args.len() {
                        eprintln!("ERROR: --lines requires a value");
                        process::exit(1);
                    }
                    result.lines = args[i].parse().unwrap_or_else(|e| {
                        eprintln!("ERROR: invalid --lines value '{}': {e}", args[i]);
                        process::exit(1);
                    });
                }
                "--agents" => {
                    i += 1;
                    if i >= args.len() {
                        eprintln!("ERROR: --agents requires a value");
                        process::exit(1);
                    }
                    result.agents = args[i].split(',').map(ToString::to_string).collect();
                }
                "--scenarios" => {
                    i += 1;
                    if i >= args.len() {
                        eprintln!("ERROR: --scenarios requires a value");
                        process::exit(1);
                    }
                    result.scenarios = args[i]
                        .split(',')
                        .map(|s| {
                            Scenario::from_name(s.trim()).unwrap_or_else(|| {
                                eprintln!("Unknown scenario: {s}");
                                eprintln!(
                                    "Available: {}",
                                    Scenario::all()
                                        .iter()
                                        .map(Scenario::name)
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                );
                                process::exit(1);
                            })
                        })
                        .collect();
                }
                "--markdown" => result.markdown = true,
                "--json" => result.json = true,
                "--json-file" => {
                    i += 1;
                    if i >= args.len() {
                        eprintln!("ERROR: --json-file requires a path");
                        process::exit(1);
                    }
                    result.json_file = Some(PathBuf::from(&args[i]));
                }
                "--gh-bench-file" => {
                    i += 1;
                    if i >= args.len() {
                        eprintln!("ERROR: --gh-bench-file requires a path");
                        process::exit(1);
                    }
                    result.gh_bench_file = Some(PathBuf::from(&args[i]));
                }
                "--no-download" => result.no_download = true,
                "--docker" => result.docker = true,
                "--mode" => {
                    i += 1;
                    if i >= args.len() {
                        eprintln!("ERROR: --mode requires a value (binary, docker, both)");
                        process::exit(1);
                    }
                    let m = args[i].as_str();
                    if !["binary", "docker", "both"].contains(&m) {
                        eprintln!("Invalid --mode: {m} (expected: binary, docker, both)");
                        process::exit(1);
                    }
                    result.mode = m.to_string();
                }
                "--cpus" => {
                    i += 1;
                    if i >= args.len() {
                        eprintln!("ERROR: --cpus requires a value");
                        process::exit(1);
                    }
                    result.docker_limits.cpus = args[i].clone();
                }
                "--memory" => {
                    i += 1;
                    if i >= args.len() {
                        eprintln!("ERROR: --memory requires a value");
                        process::exit(1);
                    }
                    result.docker_limits.memory = args[i].clone();
                }
                "--profile" => {
                    i += 1;
                    if i >= args.len() {
                        eprintln!("ERROR: --profile requires a directory path");
                        process::exit(1);
                    }
                    result.profile_dir = Some(PathBuf::from(&args[i]));
                }
                "--dhat-binary" => {
                    i += 1;
                    if i >= args.len() {
                        eprintln!("ERROR: --dhat-binary requires a path");
                        process::exit(1);
                    }
                    result.dhat_binary = Some(args[i].clone());
                }
                "--rate-bench" => result.rate_bench = true,
                "--iterations" => {
                    i += 1;
                    if i >= args.len() {
                        eprintln!("ERROR: --iterations requires a value");
                        process::exit(1);
                    }
                    result.iterations = args[i].parse().unwrap_or_else(|e| {
                        eprintln!("ERROR: invalid --iterations value '{}': {e}", args[i]);
                        process::exit(1);
                    });
                }
                "--results-file" => {
                    i += 1;
                    if i >= args.len() {
                        eprintln!("ERROR: --results-file requires a path");
                        process::exit(1);
                    }
                    result.results_file = Some(PathBuf::from(&args[i]));
                }
                "--results-dir" => {
                    i += 1;
                    if i >= args.len() {
                        eprintln!("ERROR: --results-dir requires a path");
                        process::exit(1);
                    }
                    result.results_dir = Some(PathBuf::from(&args[i]));
                }
                "--dashboard-file" => {
                    i += 1;
                    if i >= args.len() {
                        eprintln!("ERROR: --dashboard-file requires a path");
                        process::exit(1);
                    }
                    result.dashboard_file = Some(PathBuf::from(&args[i]));
                }
                other
                    if result.subcommand.is_some()
                        && result.subcommand_index.is_some_and(|idx| idx == i)
                        && !other.starts_with('-') =>
                {
                    // Skip the detected subcommand word itself.
                }
                other => {
                    eprintln!("Unknown argument: {other}");
                    eprintln!("Usage: logfwd-competitive-bench [OPTIONS]");
                    eprintln!();
                    eprintln!("  --lines N            Lines to generate (default: 5000000)");
                    eprintln!("  --agents A,B,C       Agents to run (default: all)");
                    eprintln!("  --scenarios S,S      Scenarios to run (default: passthrough)");
                    eprintln!("                       Available: passthrough, json_parse, filter");
                    eprintln!("  --markdown           Output markdown table");
                    eprintln!("  --json               Output structured JSON (for CI dashboards)");
                    eprintln!(
                        "  --json-file PATH     Write JSON results to file (combinable with --markdown)"
                    );
                    eprintln!("  --no-download        Skip binary downloads");
                    eprintln!(
                        "  --mode M             Execution mode: binary, docker, both (default: binary)"
                    );
                    eprintln!("  --docker             Enable Docker (sets mode to docker)");
                    eprintln!("  --cpus N             CPU limit per container (default: 1)");
                    eprintln!("  --memory N           Memory limit per container (default: 1g)");
                    eprintln!("  --profile DIR        Write CPU/memory profiles to DIR");
                    eprintln!(
                        "  --dhat-binary PATH   logfwd binary built with --features dhat-heap"
                    );
                    eprintln!(
                        "  --rate-bench         Run low-and-slow rate-ingest benchmark (ff only)"
                    );
                    process::exit(1);
                }
            }
            i += 1;
        }
        // Default: passthrough only (backward compat).
        if result.scenarios.is_empty() {
            result.scenarios.push(Scenario::Passthrough);
        }
        // --docker without --mode → set mode to "docker" for backward compat.
        if result.docker && result.mode == "binary" {
            result.mode = "docker".to_string();
        }
        result
    }
}

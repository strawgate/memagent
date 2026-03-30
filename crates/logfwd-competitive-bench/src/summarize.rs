//! Aggregate results from matrix CI cells into summary reports.
//!
//! Reads JSONL result files from `--results-dir` (one per matrix cell),
//! computes averages and standard deviation, and outputs:
//! - Markdown summary table (to stdout)
//! - github-action-benchmark JSON (throughput + efficiency, via --gh-bench-file)
//! - Custom dashboard JSON (via --dashboard-file)

use std::collections::BTreeMap;
use std::io::BufRead;
use std::path::Path;

use crate::runner::BenchResult;

/// Key for grouping results: (agent, scenario, mode).
type GroupKey = (String, String, String);

struct AggResult {
    name: String,
    scenario: String,
    mode: String,
    runs: Vec<BenchResult>,
}

impl AggResult {
    fn avg_elapsed_ms(&self) -> u64 {
        let valid: Vec<u64> = self
            .runs
            .iter()
            .filter(|r| r.elapsed_ms > 0)
            .map(|r| r.elapsed_ms)
            .collect();
        if valid.is_empty() {
            return 0;
        }
        valid.iter().sum::<u64>() / valid.len() as u64
    }

    fn avg_lines_done(&self) -> u64 {
        let valid: Vec<u64> = self
            .runs
            .iter()
            .filter(|r| r.lines_done > 0)
            .map(|r| r.lines_done)
            .collect();
        if valid.is_empty() {
            return 0;
        }
        valid.iter().sum::<u64>() / valid.len() as u64
    }

    fn avg_lps(&self) -> f64 {
        let avg_ms = self.avg_elapsed_ms();
        let avg_lines = self.avg_lines_done();
        if avg_ms == 0 {
            return 0.0;
        }
        avg_lines as f64 / (avg_ms as f64 / 1000.0)
    }

    fn lps_values(&self) -> Vec<f64> {
        self.runs
            .iter()
            .filter(|r| r.elapsed_ms > 0)
            .map(|r| r.lines_done as f64 / (r.elapsed_ms as f64 / 1000.0))
            .collect()
    }

    fn stddev_elapsed_ms(&self) -> f64 {
        let valid: Vec<f64> = self
            .runs
            .iter()
            .filter(|r| r.elapsed_ms > 0)
            .map(|r| r.elapsed_ms as f64)
            .collect();
        if valid.len() < 2 {
            return 0.0;
        }
        let mean = valid.iter().sum::<f64>() / valid.len() as f64;
        let variance =
            valid.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (valid.len() - 1) as f64;
        variance.sqrt()
    }

    fn individual_elapsed(&self) -> String {
        self.runs
            .iter()
            .map(|r| r.elapsed_ms.to_string())
            .collect::<Vec<_>>()
            .join("|")
    }
}

/// Load all JSONL result files from a directory tree.
fn load_results(dir: &Path) -> Vec<BenchResult> {
    let mut results = Vec::new();

    for path in walkdir(dir) {
        if path.extension().is_some_and(|ext| ext == "jsonl")
            && let Ok(file) = std::fs::File::open(&path)
        {
            for line in std::io::BufReader::new(file).lines().map_while(Result::ok) {
                if let Ok(result) = serde_json::from_str::<BenchResult>(&line) {
                    results.push(result);
                }
            }
        }
    }

    results
}

/// Simple recursive directory walk (avoids adding walkdir crate).
fn walkdir(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                files.extend(walkdir(&path));
            } else {
                files.push(path);
            }
        }
    }
    files
}

/// Group results by (agent, scenario, mode).
fn group_results(results: Vec<BenchResult>) -> Vec<AggResult> {
    let mut groups: BTreeMap<GroupKey, Vec<BenchResult>> = BTreeMap::new();

    for r in results {
        let key = (
            r.name.clone(),
            r.scenario.name().to_string(),
            r.mode.clone(),
        );
        groups.entry(key).or_default().push(r);
    }

    groups
        .into_iter()
        .map(|((name, scenario, mode), runs)| AggResult {
            name,
            scenario,
            mode,
            runs,
        })
        .collect()
}

fn fmt_rate(lines: u64, ms: u64) -> String {
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

pub fn run(
    results_dir: &Path,
    markdown: bool,
    gh_bench_file: Option<&Path>,
    dashboard_file: Option<&Path>,
) {
    let results = load_results(results_dir);
    if results.is_empty() {
        eprintln!("No results found in {}", results_dir.display());
        std::process::exit(1);
    }

    eprintln!("Loaded {} individual run results", results.len());

    let groups = group_results(results);

    // Collect unique scenarios in order.
    let mut scenarios: Vec<String> = Vec::new();
    for g in &groups {
        if !scenarios.contains(&g.scenario) {
            scenarios.push(g.scenario.clone());
        }
    }

    if markdown {
        print_markdown_summary(&groups, &scenarios);
    } else {
        print_table_summary(&groups, &scenarios);
    }

    if let Some(path) = gh_bench_file {
        write_dual_gh_bench_json(&groups, path);
    }

    if let Some(path) = dashboard_file {
        write_dashboard_json(&groups, &scenarios, path);
    }
}

fn print_markdown_summary(groups: &[AggResult], scenarios: &[String]) {
    for scenario in scenarios {
        let scenario_groups: Vec<&AggResult> =
            groups.iter().filter(|g| g.scenario == *scenario).collect();
        if scenario_groups.is_empty() {
            continue;
        }

        let iterations = scenario_groups.first().map(|g| g.runs.len()).unwrap_or(1);
        println!("### {scenario} ({iterations} iterations)\n");
        println!("| Agent | Mode | Avg Time | Stddev | Throughput | Runs |");
        println!("|-------|------|--------:|---------:|-----------:|------|");

        for g in &scenario_groups {
            let avg_ms = g.avg_elapsed_ms();
            let stddev = g.stddev_elapsed_ms();
            let avg_lines = g.avg_lines_done();
            let rate = fmt_rate(avg_lines, avg_ms);
            let runs = g.individual_elapsed();

            if avg_ms == 0 {
                println!("| {} | {} | FAILED | - | - | {} |", g.name, g.mode, runs);
            } else {
                println!(
                    "| {} | {} | {}ms | {:.0}ms | {} | {} |",
                    g.name, g.mode, avg_ms, stddev, rate, runs,
                );
            }
        }

        // Comparison ratios.
        if scenario_groups.len() > 1 {
            let base = scenario_groups[0];
            let base_ms = base.avg_elapsed_ms();
            if base_ms > 0 {
                println!();
                for g in &scenario_groups[1..] {
                    let g_ms = g.avg_elapsed_ms();
                    if g_ms > 0 {
                        let ratio = g_ms as f64 / base_ms as f64;
                        println!(
                            "> **{}** is **{:.1}x faster** than {}",
                            base.name, ratio, g.name
                        );
                    }
                }
            }
        }
        println!();
    }
}

fn print_table_summary(groups: &[AggResult], scenarios: &[String]) {
    for scenario in scenarios {
        let scenario_groups: Vec<&AggResult> =
            groups.iter().filter(|g| g.scenario == *scenario).collect();
        if scenario_groups.is_empty() {
            continue;
        }

        let iterations = scenario_groups.first().map(|g| g.runs.len()).unwrap_or(1);
        println!("===========================================");
        println!("  {scenario} ({iterations} iterations)");
        println!("===========================================");
        println!(
            "  {:<16} {:<8} {:>10} {:>10} {:>20}",
            "Agent", "Mode", "Avg Time", "Stddev", "Throughput"
        );
        println!(
            "  {:<16} {:<8} {:>10} {:>10} {:>20}",
            "-----", "----", "--------", "------", "----------"
        );

        for g in &scenario_groups {
            let avg_ms = g.avg_elapsed_ms();
            let stddev = g.stddev_elapsed_ms();
            let avg_lines = g.avg_lines_done();
            let rate = fmt_rate(avg_lines, avg_ms);

            if avg_ms == 0 {
                println!(
                    "  {:<16} {:<8} {:>10} {:>10} {:>20}",
                    g.name, g.mode, "FAILED", "-", "-"
                );
            } else {
                println!(
                    "  {:<16} {:<8} {:>8}ms {:>8.0}ms {:>20}",
                    g.name, g.mode, avg_ms, stddev, rate
                );
            }
        }
        println!("===========================================");
        println!();
    }
}

/// Write dual gh-bench JSON files: throughput (bigger=better) + efficiency (smaller=better).
/// The path given is for the "bigger" file; the "smaller" file is derived by suffix.
fn write_dual_gh_bench_json(groups: &[AggResult], path: &Path) {
    #[derive(serde::Serialize)]
    struct Entry {
        name: String,
        unit: String,
        value: f64,
        #[serde(skip_serializing_if = "String::is_empty")]
        extra: String,
    }

    let mut bigger: Vec<Entry> = Vec::new();
    let mut smaller: Vec<Entry> = Vec::new();

    for g in groups {
        let avg_ms = g.avg_elapsed_ms();
        if avg_ms == 0 {
            continue;
        }
        let avg_lines = g.avg_lines_done();
        let lps = avg_lines as f64 / (avg_ms as f64 / 1000.0);
        let stddev = g.stddev_elapsed_ms();
        let n = g.runs.len();
        let label = format!("{}/{} ({})", g.scenario, g.name, g.mode);

        // Throughput: bigger is better.
        bigger.push(Entry {
            name: format!("{label} lines/sec"),
            unit: "lines/sec".to_string(),
            value: lps,
            extra: format!("avg={avg_ms}ms stddev={stddev:.0}ms n={n}"),
        });

        // Efficiency: smaller is better — time per million lines.
        let ms_per_m = if avg_lines > 0 {
            avg_ms as f64 / (avg_lines as f64 / 1_000_000.0)
        } else {
            0.0
        };
        smaller.push(Entry {
            name: format!("{label} ms/M-lines"),
            unit: "ms".to_string(),
            value: ms_per_m,
            extra: format!("n={n}"),
        });
    }

    // Write bigger (throughput).
    let json = serde_json::to_string_pretty(&bigger).unwrap();
    match std::fs::write(path, &json) {
        Ok(()) => eprintln!("gh-bench throughput JSON written to {}", path.display()),
        Err(e) => eprintln!("ERROR: failed to write gh-bench JSON: {e}"),
    }

    // Write smaller (efficiency) — derive filename.
    let smaller_path = path.with_file_name(
        path.file_stem()
            .map(|s| format!("{}-efficiency.json", s.to_string_lossy()))
            .unwrap_or_else(|| "gh-bench-efficiency.json".to_string()),
    );
    let json = serde_json::to_string_pretty(&smaller).unwrap();
    match std::fs::write(&smaller_path, &json) {
        Ok(()) => eprintln!(
            "gh-bench efficiency JSON written to {}",
            smaller_path.display()
        ),
        Err(e) => eprintln!("ERROR: failed to write efficiency JSON: {e}"),
    }
}

/// Write structured dashboard JSON for the custom GitHub Pages dashboard.
///
/// Format:
/// ```json
/// {
///   "id": "12345678",
///   "date": "2026-03-30T...",
///   "commit": "abc1234",
///   "scenarios": {
///     "passthrough": {
///       "logfwd": { "lps": [1500000, 1520000], "avg_lps": 1510000, "avg_ms": 3300 },
///       "vector": { "lps": [600000], "avg_lps": 600000, "avg_ms": 8300 }
///     }
///   },
///   "agents": ["logfwd", "vector", ...],
///   "scenario_names": ["passthrough", "json_parse", "filter"]
/// }
/// ```
fn write_dashboard_json(groups: &[AggResult], scenarios: &[String], path: &Path) {
    use serde_json::{Map, Value, json};

    let run_id = std::env::var("GITHUB_RUN_ID")
        .or_else(|_| std::env::var("GITHUB_RUN_NUMBER"))
        .unwrap_or_else(|_| "local".to_string());

    let commit = std::env::var("GITHUB_SHA")
        .or_else(|_| {
            std::process::Command::new("git")
                .args(["rev-parse", "--short", "HEAD"])
                .output()
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        })
        .unwrap_or_default();

    let date = crate::utc_timestamp();

    // Build scenarios map: scenario -> agent -> metrics.
    let mut scenario_map: Map<String, Value> = Map::new();
    let mut agent_names: Vec<String> = Vec::new();

    for g in groups {
        if g.avg_elapsed_ms() == 0 {
            continue;
        }

        if !agent_names.contains(&g.name) {
            agent_names.push(g.name.clone());
        }

        let scenario_entry = scenario_map
            .entry(g.scenario.clone())
            .or_insert_with(|| Value::Object(Map::new()));

        let agent_data = json!({
            "lps": g.lps_values(),
            "avg_lps": g.avg_lps().round() as u64,
            "avg_ms": g.avg_elapsed_ms(),
            "stddev_ms": g.stddev_elapsed_ms().round() as u64,
            "lines_done": g.avg_lines_done(),
            "iterations": g.runs.len(),
            "mode": g.mode,
        });

        if let Value::Object(map) = scenario_entry {
            map.insert(g.name.clone(), agent_data);
        }
    }

    let dashboard = json!({
        "id": run_id,
        "date": date,
        "commit": commit,
        "scenarios": scenario_map,
        "agents": agent_names,
        "scenario_names": scenarios,
    });

    // Write run data.
    let json = serde_json::to_string_pretty(&dashboard).unwrap();
    match std::fs::write(path, &json) {
        Ok(()) => eprintln!("Dashboard JSON written to {}", path.display()),
        Err(e) => eprintln!("ERROR: failed to write dashboard JSON: {e}"),
    }

    // Also write an index entry (compact, for the manifest).
    let index_entry = json!({
        "id": run_id,
        "date": date,
        "commit": commit,
        "agents": agent_names,
        "scenarios": scenarios,
    });

    let index_path = path.with_file_name("dashboard-index-entry.json");
    let json = serde_json::to_string_pretty(&index_entry).unwrap();
    match std::fs::write(&index_path, &json) {
        Ok(()) => eprintln!("Dashboard index entry written to {}", index_path.display()),
        Err(e) => eprintln!("ERROR: failed to write index entry: {e}"),
    }
}

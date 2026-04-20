//! Aggregate results from matrix CI cells into summary reports.

use std::collections::BTreeMap;
use std::io::BufRead;
use std::path::Path;

use crate::runner::BenchResult;

type GroupKey = (String, String, String);

struct AggResult {
    name: String,
    scenario: String,
    mode: String,
    runs: Vec<BenchResult>,
}

impl AggResult {
    fn any_timed_out(&self) -> bool {
        self.runs.iter().any(|r| r.timed_out)
    }

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
            .filter(|r| r.elapsed_ms > 0)
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

    fn avg_max_rss(&self) -> u64 {
        let valid: Vec<u64> = self
            .runs
            .iter()
            .map(|r| r.samples.iter().map(|s| s.rss_bytes).max().unwrap_or(0))
            .filter(|&v| v > 0)
            .collect();
        if valid.is_empty() {
            return 0;
        }
        valid.iter().sum::<u64>() / valid.len() as u64
    }

    fn avg_max_cpu(&self) -> f64 {
        let valid: Vec<f64> = self
            .runs
            .iter()
            .map(|r| {
                if r.samples.len() < 2 {
                    return 0.0;
                }
                let mut max_pct = 0.0;
                for i in 1..r.samples.len() {
                    let prev = &r.samples[i - 1];
                    let curr = &r.samples[i];
                    let diff_ms = (curr.cpu_user_ms + curr.cpu_sys_ms)
                        .saturating_sub(prev.cpu_user_ms + prev.cpu_sys_ms);
                    let pct = diff_ms as f64 / 10.0;
                    if pct > max_pct {
                        max_pct = pct;
                    }
                }
                max_pct
            })
            .filter(|&v| v > 0.0)
            .collect();
        if valid.is_empty() {
            return 0.0;
        }
        valid.iter().sum::<f64>() / valid.len() as f64
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
            .map(|r| {
                if r.timed_out {
                    format!("{}ms(TIMEOUT)", r.elapsed_ms)
                } else {
                    format!("{}ms", r.elapsed_ms)
                }
            })
            .collect::<Vec<_>>()
            .join(" | ")
    }
}

fn load_results(dir: &Path) -> Result<Vec<BenchResult>, String> {
    let mut results = Vec::new();
    for path in walkdir(dir) {
        if path.extension().is_none_or(|ext| ext != "jsonl") {
            continue;
        }
        let file = std::fs::File::open(&path)
            .map_err(|e| format!("failed to open {}: {e}", path.display()))?;
        for (line_no, line) in std::io::BufReader::new(file).lines().enumerate() {
            let line = line.map_err(|e| {
                format!(
                    "failed to read {} at line {}: {e}",
                    path.display(),
                    line_no + 1
                )
            })?;
            let result = serde_json::from_str::<BenchResult>(&line).map_err(|e| {
                format!(
                    "failed to parse {} at line {}: {e}",
                    path.display(),
                    line_no + 1
                )
            })?;
            results.push(result);
        }
    }
    Ok(results)
}

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
        format!("{:.1}K lines/sec", lps / 1_000.0)
    } else {
        format!("{lps:.0} lines/sec")
    }
}

fn fmt_bytes(bytes: u64) -> String {
    if bytes == 0 {
        return "-".to_string();
    }
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

pub fn run(
    results_dir: &Path,
    markdown: bool,
    gh_bench_file: Option<&Path>,
    dashboard_file: Option<&Path>,
) -> Result<(), String> {
    let results = load_results(results_dir)?;
    if results.is_empty() {
        return Err(format!("No results found in {}", results_dir.display()));
    }
    eprintln!("Loaded {} individual run results", results.len());

    let groups = group_results(results);
    let mut scenarios: Vec<String> = Vec::new();
    for g in &groups {
        if !scenarios.contains(&g.scenario) {
            scenarios.push(g.scenario.clone());
        }
    }

    if markdown {
        print_markdown(&groups, &scenarios);
    } else {
        print_table(&groups, &scenarios);
    }

    if let Some(path) = gh_bench_file {
        write_dual_gh_bench(&groups, path);
    }

    if let Some(path) = dashboard_file {
        write_dashboard_json(&groups, &scenarios, path);
    }

    Ok(())
}

fn print_markdown(groups: &[AggResult], scenarios: &[String]) {
    for scenario in scenarios {
        let sg: Vec<&AggResult> = groups.iter().filter(|g| g.scenario == *scenario).collect();
        if sg.is_empty() {
            continue;
        }
        let n = sg.first().map_or(1, |g| g.runs.len());
        println!("### {scenario} ({n} iterations)\n");
        println!("| Agent | Mode | Avg Time | Stddev | Throughput | Peak RSS | Runs |");
        println!("|-------|------|--------:|---------:|-----------:|---------:|------|");
        for g in &sg {
            let avg_ms = g.avg_elapsed_ms();
            let rss = fmt_bytes(g.avg_max_rss());
            if avg_ms == 0 {
                println!(
                    "| {} | {} | FAILED | - | - | {} | {} |",
                    g.name,
                    g.mode,
                    rss,
                    g.individual_elapsed()
                );
            } else if g.any_timed_out() {
                println!(
                    "| {} | {} | **TIMEOUT** ({}ms) | {:.0}ms | {} | {} | {} |",
                    g.name,
                    g.mode,
                    avg_ms,
                    g.stddev_elapsed_ms(),
                    if g.avg_lines_done() == 0 {
                        "0 lines (timed out)".to_string()
                    } else {
                        fmt_rate(g.avg_lines_done(), avg_ms)
                    },
                    rss,
                    g.individual_elapsed(),
                );
            } else {
                let rate = fmt_rate(g.avg_lines_done(), avg_ms);
                println!(
                    "| {} | {} | {}ms | {:.0}ms | {} | {} | {} |",
                    g.name,
                    g.mode,
                    avg_ms,
                    g.stddev_elapsed_ms(),
                    rate,
                    rss,
                    g.individual_elapsed(),
                );
            }
        }
        // Only compare agents that completed without timeout.
        let completed: Vec<&&AggResult> = sg
            .iter()
            .filter(|g| !g.any_timed_out() && g.avg_elapsed_ms() > 0)
            .collect();
        if completed.len() > 1 {
            let base = completed
                .iter()
                .max_by_key(|g| g.avg_elapsed_ms())
                .copied()
                .unwrap();
            let base_ms = base.avg_elapsed_ms();
            println!();
            for g in &completed {
                if g.name == base.name && g.mode == base.mode {
                    continue;
                }
                let g_ms = g.avg_elapsed_ms();
                if g_ms > 0 {
                    let ratio = base_ms as f64 / g_ms as f64;
                    println!(
                        "> **{}** is **{:.1}x faster** than {}",
                        g.name, ratio, base.name
                    );
                }
            }
        }
        println!();
    }
}

fn print_table(groups: &[AggResult], scenarios: &[String]) {
    for scenario in scenarios {
        let sg: Vec<&AggResult> = groups.iter().filter(|g| g.scenario == *scenario).collect();
        if sg.is_empty() {
            continue;
        }
        let n = sg.first().map_or(1, |g| g.runs.len());
        println!("===========================================");
        println!("  {scenario} ({n} iterations)");
        println!("===========================================");
        println!(
            "  {:<16} {:<8} {:>10} {:>10} {:>20} {:>12}",
            "Agent", "Mode", "Avg Time", "Stddev", "Throughput", "Peak RSS"
        );
        for g in &sg {
            let avg_ms = g.avg_elapsed_ms();
            let rate = fmt_rate(g.avg_lines_done(), avg_ms);
            let rss = fmt_bytes(g.avg_max_rss());
            if avg_ms == 0 {
                println!(
                    "  {:<16} {:<8} {:>10} {:>10} {:>20} {:>12}",
                    g.name, g.mode, "FAILED", "-", "-", rss
                );
            } else {
                println!(
                    "  {:<16} {:<8} {:>8}ms {:>8.0}ms {:>20} {:>12}",
                    g.name,
                    g.mode,
                    avg_ms,
                    g.stddev_elapsed_ms(),
                    rate,
                    rss
                );
            }
        }
        println!();
    }
}

fn write_dual_gh_bench(groups: &[AggResult], path: &Path) {
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
        let n = g.runs.len();
        let label = format!("{}/{} ({})", g.scenario, g.name, g.mode);

        bigger.push(Entry {
            name: format!("{label} lines/sec"),
            unit: "lines/sec".to_string(),
            value: lps,
            extra: format!("avg={avg_ms}ms stddev={:.0}ms n={n}", g.stddev_elapsed_ms()),
        });

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

    let json = serde_json::to_string_pretty(&bigger).expect("serialize throughput gh-bench JSON");
    match std::fs::write(path, &json) {
        Ok(()) => eprintln!("gh-bench throughput JSON written to {}", path.display()),
        Err(e) => eprintln!("ERROR: write gh-bench: {e}"),
    }

    let smaller_path = path.with_file_name(path.file_stem().map_or_else(
        || "gh-bench-efficiency.json".to_string(),
        |s| format!("{}-efficiency.json", s.to_string_lossy()),
    ));
    let json = serde_json::to_string_pretty(&smaller).expect("serialize efficiency gh-bench JSON");
    match std::fs::write(&smaller_path, &json) {
        Ok(()) => eprintln!(
            "gh-bench efficiency JSON written to {}",
            smaller_path.display()
        ),
        Err(e) => eprintln!("ERROR: write efficiency: {e}"),
    }
}

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
            "avg_max_rss": g.avg_max_rss(),
            "avg_max_cpu": g.avg_max_cpu(),
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

    let json = serde_json::to_string_pretty(&dashboard).expect("serialize dashboard JSON");
    match std::fs::write(path, &json) {
        Ok(()) => eprintln!("Dashboard JSON written to {}", path.display()),
        Err(e) => eprintln!("ERROR: write dashboard: {e}"),
    }

    let index_entry = json!({
        "id": run_id,
        "date": date,
        "commit": commit,
        "agents": agent_names,
        "scenarios": scenarios,
    });
    let index_path = path.with_file_name("dashboard-index-entry.json");
    let json =
        serde_json::to_string_pretty(&index_entry).expect("serialize dashboard index entry JSON");
    match std::fs::write(&index_path, &json) {
        Ok(()) => eprintln!("Index entry written to {}", index_path.display()),
        Err(e) => eprintln!("ERROR: write index entry: {e}"),
    }
}

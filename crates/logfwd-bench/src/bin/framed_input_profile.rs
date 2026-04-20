#![allow(deprecated)]
#![allow(clippy::print_stdout, clippy::print_stderr)]

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::collections::VecDeque;
use std::fs::File;
use std::hint::black_box;
use std::io;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use logfwd_arrow::scanner::Scanner;
use logfwd_bench::{generators, make_otlp_sink};
use logfwd_core::scan_config::ScanConfig;
use logfwd_io::format::FormatDecoder;
use logfwd_io::framed::FramedInput;
use logfwd_io::input::{InputEvent, InputSource};
use logfwd_output::{BatchMetadata, Compression};
use logfwd_types::diagnostics::{ComponentHealth, ComponentStats};
use logfwd_types::pipeline::SourceId;
use pprof::ProfilerGuardBuilder;

const DEFAULT_LINES: usize = 200_000;
const DEFAULT_ITERATIONS: usize = 5;
const DEFAULT_CHUNK_BYTES: usize = 64 * 1024;
const REMAINDER_CHUNK_BYTES: usize = 97;

fn main() {
    let cli = Cli::parse();
    let scenarios = vec![
        Scenario::passthrough_full_lines(cli.lines),
        Scenario::passthrough_remainder_heavy(cli.lines),
        Scenario::passthrough_narrow_raw(cli.lines),
        Scenario::passthrough_json(cli.lines),
        Scenario::passthrough_json_source_sidecar(cli.lines),
        Scenario::cri_full(cli.lines / 2),
    ];

    if cli.alloc_only {
        print_allocation_report(&scenarios);
        return;
    }

    let mut baseline_rows = Vec::new();
    for scenario in &scenarios {
        baseline_rows.push(measure_pipeline_scenario(scenario, cli.iterations));
    }

    let copy_site_rows = measure_copy_site_candidates(cli.lines);

    if let Some(path) = &cli.flamegraph {
        write_flamegraph(path, &Scenario::passthrough_full_lines(cli.lines));
    }

    println!("# FramedInput profiling report\n");
    println!(
        "- Workload size: {} lines per JSON scenario, {} iterations",
        cli.lines, cli.iterations
    );
    println!(
        "- Flamegraph: {}",
        cli.flamegraph
            .as_ref()
            .map_or_else(|| "not requested".to_string(), |p| p.display().to_string())
    );
    println!();
    println!("## Baseline stage timings\n");
    println!(
        "| Scenario | Input format | Workload | FramedInput | Scanner | Encode | Total | Framed share | Throughput | Peak RSS | Steady RSS |"
    );
    println!("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|");
    for row in &baseline_rows {
        println!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            row.name,
            row.format,
            row.workload,
            format_duration(row.framed_ns),
            format_duration(row.scanner_ns),
            format_duration(row.encode_ns),
            format_duration(row.total_ns),
            format_percent(row.framed_share),
            format_throughput(row.lines, row.input_bytes, row.total_ns),
            format_rss(row.peak_rss_kb),
            format_rss(row.steady_rss_kb),
        );
    }
    println!();
    println!("## Copy-site microbenchmarks\n");
    println!("| Candidate | What changes | Time | Throughput | Relative |");
    println!("|---|---|---:|---:|---:|");
    let baseline_ns = copy_site_rows
        .iter()
        .find(|row| row.name == "current_round_trip")
        .map_or(1, |row| row.ns);
    for row in &copy_site_rows {
        println!(
            "| {} | {} | {} | {} | {} |",
            row.name,
            row.description,
            format_duration(row.ns),
            format_lines_per_sec(row.lines, row.ns),
            format_percent(baseline_ns as f64 / row.ns as f64),
        );
    }
    println!();
    println!("## Copy sites observed\n");
    println!(
        "- `crates/logfwd-io/src/framed.rs` — `remainder` + `extend_from_slice(&bytes)` copies every new chunk into a fresh/retaken `Vec<u8>`, even when the remainder is empty."
    );
    println!(
        "- `crates/logfwd-io/src/format.rs` — passthrough and passthrough-json extend `out` from `chunk`, creating a second full-buffer copy."
    );
    println!(
        "- `crates/logfwd-runtime/src/pipeline/input_pipeline.rs` — `InputState.buf.extend_from_slice(&bytes)` copies FramedInput output into the async `BytesMut` batch buffer."
    );
    println!();
    println!("## Recommendations\n");
    println!(
        "1. Add an owned-input fast path in `FramedInput` for `remainder.is_empty()` so complete newline-delimited chunks can move straight through without the current `Vec -> Vec` round-trip."
    );
    println!(
        "2. Treat trusted file JSON as `raw` / passthrough where possible; `passthrough_json` pays an extra full-buffer validation pass for diagnostics only."
    );
    println!(
        "3. Keep read chunks line-aligned or larger when possible; remainder-heavy chunking is materially more expensive than the no-remainder path."
    );
    println!(
        "4. Keep source metadata out of FramedInput entirely; attach it after scan from row-origin sidecars so source bytes are never rewritten."
    );
    println!(
        "5. Keep CRI metadata in sidecars and packed `StringView` blocks; the next broad copy wins are still the owned-input fast path and runtime buffer handoff."
    );
}

struct Cli {
    lines: usize,
    iterations: usize,
    alloc_only: bool,
    flamegraph: Option<PathBuf>,
}

impl Cli {
    fn parse() -> Self {
        Self::parse_from(std::env::args().skip(1).collect::<Vec<_>>())
    }

    fn parse_from(args: Vec<String>) -> Self {
        for arg in &args {
            if matches!(arg.as_str(), "--help" | "-h") {
                print_help();
                std::process::exit(0);
            }
        }

        let lines = parse_positive_usize_flag(&args, "--lines", DEFAULT_LINES);
        let iterations = parse_positive_usize_flag(&args, "--iterations", DEFAULT_ITERATIONS);
        let alloc_only = args.iter().any(|arg| arg == "--alloc-only");
        let flamegraph = parse_path_flag(&args, "--flamegraph");
        Self {
            lines,
            iterations,
            alloc_only,
            flamegraph,
        }
    }
}

fn parse_positive_usize_flag(args: &[String], flag: &str, default: usize) -> usize {
    match args.iter().rposition(|arg| arg == flag) {
        Some(idx) => match args.get(idx + 1) {
            Some(value) => match value.parse::<usize>() {
                Ok(parsed) if parsed > 0 => parsed,
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

fn parse_path_flag(args: &[String], flag: &str) -> Option<PathBuf> {
    match args.iter().rposition(|arg| arg == flag) {
        Some(idx) => match args.get(idx + 1) {
            Some(value) if !value.starts_with('-') => Some(PathBuf::from(value)),
            _ => {
                eprintln!("warning: {flag} expects a value; ignoring");
                None
            }
        },
        None => None,
    }
}

fn print_help() {
    println!("Usage: cargo run -p logfwd-bench --release --bin framed_input_profile -- [OPTIONS]");
    println!("  --lines <N>         Number of lines per JSON scenario (default: {DEFAULT_LINES})");
    println!(
        "  --iterations <N>    Number of measured iterations per scenario (default: {DEFAULT_ITERATIONS})"
    );
    println!("  --alloc-only        Print allocation metrics (requires --features dhat-heap)");
    println!("  --flamegraph <SVG>  Write a flamegraph for the baseline passthrough scenario");
}

#[derive(Clone, Copy)]
enum ScenarioFormat {
    Passthrough,
    PassthroughJson,
    Cri,
}

impl ScenarioFormat {
    fn label(self) -> &'static str {
        match self {
            Self::Passthrough => "raw/passthrough",
            Self::PassthroughJson => "json/validate",
            Self::Cri => "cri",
        }
    }

    fn decoder(self, stats: Arc<ComponentStats>) -> FormatDecoder {
        match self {
            Self::Passthrough => FormatDecoder::passthrough(stats),
            Self::PassthroughJson => FormatDecoder::passthrough_json(stats),
            Self::Cri => FormatDecoder::cri(2 * 1024 * 1024, stats),
        }
    }
}

struct Scenario {
    name: &'static str,
    format: ScenarioFormat,
    chunks: Vec<Vec<u8>>,
    input_bytes: usize,
    lines: usize,
    workload: &'static str,
    source_path: Option<&'static str>,
}

impl Scenario {
    fn passthrough_full_lines(lines: usize) -> Self {
        let data = generators::gen_production_mixed(lines, 42);
        let chunks = chunk_on_line_boundaries(&data, DEFAULT_CHUNK_BYTES);
        Self {
            name: "passthrough_complete",
            format: ScenarioFormat::Passthrough,
            input_bytes: data.len(),
            lines: memchr::memchr_iter(b'\n', &data).count(),
            chunks,
            workload: "aligned 64KiB chunks",
            source_path: None,
        }
    }

    fn passthrough_remainder_heavy(lines: usize) -> Self {
        let data = generators::gen_production_mixed(lines, 7);
        let chunks = chunk_fixed(&data, REMAINDER_CHUNK_BYTES);
        Self {
            name: "passthrough_remainder",
            format: ScenarioFormat::Passthrough,
            input_bytes: data.len(),
            lines: memchr::memchr_iter(b'\n', &data).count(),
            chunks,
            workload: "97-byte split-heavy chunks",
            source_path: None,
        }
    }

    fn passthrough_json(lines: usize) -> Self {
        let data = generators::gen_narrow(lines, 9);
        let chunks = chunk_on_line_boundaries(&data, DEFAULT_CHUNK_BYTES);
        Self {
            name: "passthrough_narrow_json",
            format: ScenarioFormat::PassthroughJson,
            input_bytes: data.len(),
            lines: memchr::memchr_iter(b'\n', &data).count(),
            chunks,
            workload: "aligned 64KiB chunks",
            source_path: None,
        }
    }

    fn passthrough_json_source_sidecar(lines: usize) -> Self {
        let data = generators::gen_narrow(lines, 9);
        let chunks = chunk_on_line_boundaries(&data, DEFAULT_CHUNK_BYTES);
        Self {
            name: "passthrough_json_source_sidecar",
            format: ScenarioFormat::PassthroughJson,
            input_bytes: data.len(),
            lines: memchr::memchr_iter(b'\n', &data).count(),
            chunks,
            workload: "aligned 64KiB chunks + source identity sidecar",
            source_path: Some("/var/log/pods/ns_pod_uid/c/main.log"),
        }
    }

    fn passthrough_narrow_raw(lines: usize) -> Self {
        let data = generators::gen_narrow(lines, 9);
        let chunks = chunk_on_line_boundaries(&data, DEFAULT_CHUNK_BYTES);
        Self {
            name: "passthrough_narrow_raw",
            format: ScenarioFormat::Passthrough,
            input_bytes: data.len(),
            lines: memchr::memchr_iter(b'\n', &data).count(),
            chunks,
            workload: "aligned 64KiB chunks",
            source_path: None,
        }
    }

    fn cri_full(lines: usize) -> Self {
        let data = generators::gen_cri_k8s(lines, 11);
        let chunks = chunk_on_line_boundaries(&data, DEFAULT_CHUNK_BYTES);
        Self {
            name: "cri_full",
            format: ScenarioFormat::Cri,
            input_bytes: data.len(),
            lines: memchr::memchr_iter(b'\n', &data).count(),
            chunks,
            workload: "aligned 64KiB chunks",
            source_path: None,
        }
    }
}

struct MockSource {
    events: VecDeque<Vec<InputEvent>>,
    source_path: Option<PathBuf>,
}

impl MockSource {
    fn new(chunks: &[Vec<u8>], source_path: Option<&'static str>) -> Self {
        let source_id = source_path.map(|_| SourceId(1));
        let mut events: VecDeque<Vec<InputEvent>> = chunks
            .iter()
            .map(|chunk| {
                vec![InputEvent::Data {
                    bytes: chunk.clone(),
                    source_id,
                    accounted_bytes: chunk.len() as u64,
                    cri_metadata: None,
                }]
            })
            .collect();
        events.push_back(vec![InputEvent::EndOfFile { source_id }]);
        Self {
            events,
            source_path: source_path.map(PathBuf::from),
        }
    }
}

impl InputSource for MockSource {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        Ok(self.events.pop_front().unwrap_or_default())
    }

    fn name(&self) -> &'static str {
        "mock"
    }

    fn source_paths(&self) -> Vec<(SourceId, PathBuf)> {
        self.source_path
            .clone()
            .map(|path| vec![(SourceId(1), path)])
            .unwrap_or_default()
    }

    fn health(&self) -> ComponentHealth {
        // Benchmark input is deterministic in-process test scaffolding with no
        // separate bind/startup/failure lifecycle.
        ComponentHealth::Healthy
    }
}

struct StageSummary {
    name: &'static str,
    format: &'static str,
    workload: &'static str,
    input_bytes: usize,
    lines: usize,
    framed_ns: u64,
    scanner_ns: u64,
    encode_ns: u64,
    total_ns: u64,
    framed_share: f64,
    peak_rss_kb: u64,
    steady_rss_kb: u64,
}

fn measure_pipeline_scenario(scenario: &Scenario, iterations: usize) -> StageSummary {
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        samples.push(run_pipeline_once(scenario));
    }
    samples.sort_by_key(|s| s.total_ns);
    assert!(!samples.is_empty(), "--iterations must be at least 1");
    let median = &samples[samples.len() / 2];
    StageSummary {
        name: scenario.name,
        format: scenario.format.label(),
        workload: scenario.workload,
        input_bytes: scenario.input_bytes,
        lines: scenario.lines,
        framed_ns: median.framed_ns,
        scanner_ns: median.scanner_ns,
        encode_ns: median.encode_ns,
        total_ns: median.total_ns,
        framed_share: median.framed_ns as f64 / median.total_ns as f64,
        peak_rss_kb: median.peak_rss_kb,
        steady_rss_kb: median.steady_rss_kb,
    }
}

struct StageSample {
    framed_ns: u64,
    scanner_ns: u64,
    encode_ns: u64,
    total_ns: u64,
    peak_rss_kb: u64,
    steady_rss_kb: u64,
}

fn run_pipeline_once(scenario: &Scenario) -> StageSample {
    let rss = RssSampler::start();

    let stats = Arc::new(ComponentStats::new());
    let source = Box::new(MockSource::new(&scenario.chunks, scenario.source_path));
    let format = scenario.format.decoder(Arc::clone(&stats));
    let mut framed = FramedInput::new(source, format, Arc::clone(&stats));

    let framed_start = Instant::now();
    let mut framed_bytes = Vec::with_capacity(scenario.input_bytes);
    for _ in 0..=scenario.chunks.len() {
        let events = framed.poll().expect("FramedInput poll should succeed");
        for event in events {
            if let InputEvent::Data { bytes, .. } = event {
                framed_bytes.extend_from_slice(&bytes);
            }
        }
    }
    let framed_ns = framed_start.elapsed().as_nanos() as u64;

    let scan_start = Instant::now();
    let mut scanner = Scanner::new(ScanConfig::default());
    let batch = scanner
        .scan_detached(Bytes::from(framed_bytes))
        .expect("scan_detached should succeed");
    let scanner_ns = scan_start.elapsed().as_nanos() as u64;

    let encode_start = Instant::now();
    let mut sink = make_otlp_sink(Compression::None);
    let metadata = BatchMetadata {
        resource_attrs: Arc::new(vec![]),
        observed_time_ns: 0,
    };
    sink.encode_batch(&batch, &metadata);
    let encode_ns = encode_start.elapsed().as_nanos() as u64;

    let total_ns = framed_ns + scanner_ns + encode_ns;
    let (peak_rss_kb, steady_rss_kb) = rss.finish();

    StageSample {
        framed_ns,
        scanner_ns,
        encode_ns,
        total_ns,
        peak_rss_kb,
        steady_rss_kb,
    }
}

struct CopySiteRow {
    name: &'static str,
    description: &'static str,
    ns: u64,
    lines: usize,
}

fn measure_copy_site_candidates(lines: usize) -> Vec<CopySiteRow> {
    let data = generators::gen_narrow(lines, 99);
    let iterations = DEFAULT_ITERATIONS * 2;
    vec![
        CopySiteRow {
            name: "current_round_trip",
            description: "copy input chunk into `chunk`, then copy complete lines into `out_buf`",
            ns: median_time_ns(iterations, || {
                black_box(current_round_trip(&data));
            }),
            lines,
        },
        CopySiteRow {
            name: "owned_input_fast_path",
            description: "move the owned input `Vec<u8>` through when `remainder.is_empty()`",
            ns: median_time_ns(iterations, || {
                black_box(owned_input_fast_path(data.clone()));
            }),
            lines,
        },
        CopySiteRow {
            name: "passthrough_json_validate",
            description: "current passthrough-json validation pass plus output copy",
            ns: median_time_ns(iterations, || {
                black_box(passthrough_json_round_trip(&data));
            }),
            lines,
        },
    ]
}

fn median_time_ns(iterations: usize, mut f: impl FnMut()) -> u64 {
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        f();
        samples.push(start.elapsed().as_nanos() as u64);
    }
    samples.sort_unstable();
    if samples.is_empty() {
        return 0;
    }
    samples[samples.len() / 2]
}

fn current_round_trip(data: &[u8]) -> usize {
    let mut chunk = Vec::new();
    chunk.extend_from_slice(data);
    let mut out = Vec::with_capacity(chunk.len());
    out.extend_from_slice(&chunk);
    black_box(&out);
    out.len()
}

fn owned_input_fast_path(mut data: Vec<u8>) -> usize {
    black_box(data.as_mut_ptr());
    black_box(&data);
    data.len()
}

fn passthrough_json_round_trip(data: &[u8]) -> usize {
    let stats = ComponentStats::new();
    let mut decoder = FormatDecoder::passthrough_json(Arc::new(stats));
    let mut out = Vec::with_capacity(data.len());
    decoder.process_lines(data, &mut out);
    black_box(&out);
    out.len()
}

fn write_flamegraph(path: &PathBuf, scenario: &Scenario) {
    let guard = ProfilerGuardBuilder::default()
        .frequency(999)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .expect("pprof guard");
    let _ = run_pipeline_once(scenario);
    let report = guard.report().build().expect("pprof report");
    let file = File::create(path).expect("flamegraph output");
    report.flamegraph(file).expect("write flamegraph");
}

struct RssSampler {
    stop: Arc<AtomicBool>,
    peak_rss_kb: Arc<AtomicU64>,
    last_rss_kb: Arc<AtomicU64>,
    handle: Option<thread::JoinHandle<()>>,
}

impl RssSampler {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let initial_rss = current_rss_kb();
        let peak_rss_kb = Arc::new(AtomicU64::new(initial_rss));
        let last_rss_kb = Arc::new(AtomicU64::new(initial_rss));
        let thread_stop = Arc::clone(&stop);
        let thread_peak = Arc::clone(&peak_rss_kb);
        let thread_last = Arc::clone(&last_rss_kb);
        let handle = thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                let rss = current_rss_kb();
                thread_last.store(rss, Ordering::Relaxed);
                let current_peak = thread_peak.load(Ordering::Relaxed);
                if rss > current_peak {
                    thread_peak.store(rss, Ordering::Relaxed);
                }
                thread::sleep(Duration::from_millis(5));
            }
        });
        Self {
            stop,
            peak_rss_kb,
            last_rss_kb,
            handle: Some(handle),
        }
    }

    fn finish(mut self) -> (u64, u64) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        (
            self.peak_rss_kb.load(Ordering::Relaxed),
            self.last_rss_kb.load(Ordering::Relaxed),
        )
    }
}

fn current_rss_kb() -> u64 {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")
                .and_then(|rest| rest.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .unwrap_or(0)
}

fn chunk_fixed(data: &[u8], chunk_bytes: usize) -> Vec<Vec<u8>> {
    data.chunks(chunk_bytes).map(<[u8]>::to_vec).collect()
}

fn chunk_on_line_boundaries(data: &[u8], target_bytes: usize) -> Vec<Vec<u8>> {
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < data.len() {
        let mut end = (start + target_bytes).min(data.len());
        if end < data.len() {
            let search = &data[end..];
            if let Some(offset) = memchr::memchr(b'\n', search) {
                end += offset + 1;
            } else {
                end = data.len();
            }
        }
        chunks.push(data[start..end].to_vec());
        start = end;
    }
    chunks
}

fn print_allocation_report(_scenarios: &[Scenario]) {
    #[cfg(not(feature = "dhat-heap"))]
    {
        eprintln!("--alloc-only requires `cargo run ... --features dhat-heap`");
        std::process::exit(1);
    }

    #[cfg(feature = "dhat-heap")]
    {
        println!("# FramedInput allocation report\n");
        println!("| Scenario | Total allocs | Total bytes | Peak bytes |");
        println!("|---|---:|---:|---:|");
        for scenario in _scenarios {
            let metrics = allocation_metrics(|| {
                let sample = run_pipeline_once(scenario);
                black_box(sample.total_ns);
            });
            println!(
                "| {} | {} | {} | {} |",
                scenario.name,
                metrics.total_blocks,
                format_bytes(metrics.total_bytes),
                format_bytes(metrics.max_bytes),
            );
        }
    }
}

#[cfg(feature = "dhat-heap")]
struct AllocationMetrics {
    total_blocks: u64,
    total_bytes: u64,
    max_bytes: u64,
}

#[cfg(feature = "dhat-heap")]
fn allocation_metrics(mut f: impl FnMut()) -> AllocationMetrics {
    let _prof = dhat::Profiler::builder().testing().build();
    let before = dhat::HeapStats::get();
    f();
    let after = dhat::HeapStats::get();
    AllocationMetrics {
        total_blocks: after.total_blocks.saturating_sub(before.total_blocks),
        total_bytes: after.total_bytes.saturating_sub(before.total_bytes),
        max_bytes: after.max_bytes as u64,
    }
}

fn format_duration(ns: u64) -> String {
    if ns >= 1_000_000_000 {
        format!("{:.2}s", ns as f64 / 1_000_000_000.0)
    } else if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.1}µs", ns as f64 / 1_000.0)
    }
}

fn format_percent(value: f64) -> String {
    format!("{:.1}%", value * 100.0)
}

fn format_throughput(lines: usize, bytes: usize, ns: u64) -> String {
    format!(
        "{}/{}",
        format_lines_per_sec(lines, ns),
        format_bytes_per_sec(bytes, ns)
    )
}

fn format_lines_per_sec(lines: usize, ns: u64) -> String {
    if ns == 0 {
        return "0K l/s".to_string();
    }
    let rate = lines as f64 / (ns as f64 / 1_000_000_000.0);
    if rate >= 1_000_000.0 {
        format!("{:.2}M l/s", rate / 1_000_000.0)
    } else {
        format!("{:.0}K l/s", rate / 1_000.0)
    }
}

fn format_bytes_per_sec(bytes: usize, ns: u64) -> String {
    if ns == 0 {
        return "0MiB/s".to_string();
    }
    let rate = bytes as f64 / (ns as f64 / 1_000_000_000.0);
    if rate >= 1_073_741_824.0 {
        format!("{:.2}GiB/s", rate / 1_073_741_824.0)
    } else {
        format!("{:.0}MiB/s", rate / 1_048_576.0)
    }
}

fn format_rss(kb: u64) -> String {
    format!("{:.1}MiB", kb as f64 / 1024.0)
}

#[cfg(feature = "dhat-heap")]
fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_048_576 {
        format!("{:.2}MiB", bytes as f64 / 1_048_576.0)
    } else {
        format!("{:.1}KiB", bytes as f64 / 1024.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligned_chunker_keeps_all_bytes() {
        let data = generators::gen_narrow(128, 1);
        let chunks = chunk_on_line_boundaries(&data, 1024);
        let rebuilt: Vec<u8> = chunks.into_iter().flatten().collect();
        assert_eq!(rebuilt, data);
    }

    #[test]
    fn fixed_chunker_keeps_all_bytes() {
        let data = generators::gen_narrow(128, 2);
        let chunks = chunk_fixed(&data, 97);
        let rebuilt: Vec<u8> = chunks.into_iter().flatten().collect();
        assert_eq!(rebuilt, data);
    }

    #[test]
    fn copy_site_fast_path_is_lossless() {
        let data = generators::gen_narrow(256, 3);
        assert_eq!(
            current_round_trip(&data),
            owned_input_fast_path(data.clone())
        );
        assert_eq!(data.len(), passthrough_json_round_trip(&data));
    }

    #[test]
    fn cli_zero_values_fall_back_to_defaults() {
        let cli = Cli::parse_from(vec![
            "--lines".to_string(),
            "0".to_string(),
            "--iterations".to_string(),
            "0".to_string(),
        ]);
        assert_eq!(cli.lines, DEFAULT_LINES);
        assert_eq!(cli.iterations, DEFAULT_ITERATIONS);
    }

    #[test]
    fn cli_missing_values_fall_back_to_defaults() {
        let cli = Cli::parse_from(vec!["--lines".to_string(), "--iterations".to_string()]);
        assert_eq!(cli.lines, DEFAULT_LINES);
        assert_eq!(cli.iterations, DEFAULT_ITERATIONS);
    }

    #[test]
    fn cli_missing_value_does_not_swallow_next_flag() {
        let cli = Cli::parse_from(vec![
            "--lines".to_string(),
            "--iterations".to_string(),
            "5".to_string(),
        ]);
        assert_eq!(cli.lines, DEFAULT_LINES);
        assert_eq!(cli.iterations, 5);
    }

    #[test]
    fn format_rates_handle_zero_duration() {
        assert_eq!(format_lines_per_sec(10, 0), "0K l/s");
        assert_eq!(format_bytes_per_sec(1024, 0), "0MiB/s");
    }
}

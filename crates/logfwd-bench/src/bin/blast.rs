//! Lovable destination benchmarking CLI.
//!
//! This tool generates deterministic log data, scans it into Arrow,
//! optionally runs a SQL transform, and blasts batches at one configured
//! destination sink.
//!
//! Examples:
//!   cargo run -p logfwd-bench --release --bin blast -- run \
//!     --destination otlp \
//!     --endpoint http://localhost:4318/v1/logs \
//!     --auth-bearer-token "token" \
//!     --workers 4 --batch-lines 5000 --duration-secs 30
//!
//!   cargo run -p logfwd-bench --release --bin blast -- run \
//!     --config ./logfwd.yaml --pipeline default --dry-run

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand, ValueEnum};
use logfwd_arrow::scanner::Scanner;
use logfwd_config::{Config, OutputConfig, OutputType};
use logfwd_core::scan_config::ScanConfig;
use logfwd_output::build_sink_factory;
use logfwd_output::sink::SendResult;
use logfwd_transform::SqlTransform;
use logfwd_types::diagnostics::ComponentStats;

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run a destination throughput blast.
    Run(RunArgs),
    /// Print config.yaml key to CLI-flag mapping.
    ExplainFlags,
}

#[derive(Debug, Clone, Parser)]
struct RunArgs {
    /// Optional config.yaml to bootstrap sink settings.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Pipeline name in config.yaml (defaults to first pipeline).
    #[arg(long)]
    pipeline: Option<String>,

    /// Output index in selected pipeline (defaults to 0).
    #[arg(long, default_value_t = 0)]
    output_index: usize,

    /// Destination type.
    #[arg(long, value_enum)]
    destination: Option<DestinationKind>,

    /// Endpoint URL or host:port (depends on destination type).
    #[arg(long)]
    endpoint: Option<String>,

    /// OTLP protocol (http|grpc).
    #[arg(long)]
    protocol: Option<String>,

    /// Compression (none|gzip|zstd depending on destination support).
    #[arg(long)]
    compression: Option<String>,

    /// Elasticsearch request mode (buffered|streaming).
    #[arg(long)]
    request_mode: Option<String>,

    /// Elasticsearch index name.
    #[arg(long)]
    index: Option<String>,

    /// Loki tenant ID.
    #[arg(long)]
    tenant_id: Option<String>,

    /// Bearer token auth.
    #[arg(long)]
    auth_bearer_token: Option<String>,

    /// Extra header, repeatable: --auth-header 'Authorization=ApiKey xyz'.
    #[arg(long, value_name = "KEY=VALUE")]
    auth_header: Vec<String>,

    /// Worker count.
    #[arg(long, default_value_t = 1)]
    workers: usize,

    /// Lines per generated batch.
    #[arg(long, default_value_t = 5_000)]
    batch_lines: usize,

    /// Benchmark duration in seconds.
    #[arg(long, default_value_t = 30)]
    duration_secs: u64,

    /// Synthetic data profile.
    #[arg(long, value_enum, default_value_t = GeneratorKind::Envoy)]
    generator: GeneratorKind,

    /// Data generator seed.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// SQL transform applied after scan.
    #[arg(long, default_value = "SELECT * FROM logs")]
    transform_sql: String,

    /// Print resolved sink config and exit.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum DestinationKind {
    Otlp,
    Http,
    Elasticsearch,
    Loki,
    ArrowIpc,
    Udp,
    Tcp,
    Null,
}

impl DestinationKind {
    fn as_output_type(self) -> OutputType {
        match self {
            Self::Otlp => OutputType::Otlp,
            Self::Http => OutputType::Http,
            Self::Elasticsearch => OutputType::Elasticsearch,
            Self::Loki => OutputType::Loki,
            Self::ArrowIpc => OutputType::ArrowIpc,
            Self::Udp => OutputType::Udp,
            Self::Tcp => OutputType::Tcp,
            Self::Null => OutputType::Null,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum GeneratorKind {
    Narrow,
    Wide,
    Cri,
    Envoy,
}

struct Counters {
    rows: AtomicU64,
    batches: AtomicU64,
    errors: AtomicU64,
    raw_bytes: AtomicU64,
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::ExplainFlags => print_flag_mapping(),
        Commands::Run(args) => {
            if let Err(err) = run_benchmark(args) {
                eprintln!("error: {err}");
                std::process::exit(1);
            }
        }
    }
}

fn run_benchmark(args: RunArgs) -> Result<(), String> {
    if args.duration_secs == 0 {
        return Err("--duration-secs must be at least 1".to_string());
    }
    if args.workers == 0 {
        return Err("--workers must be at least 1".to_string());
    }
    if args.batch_lines == 0 {
        return Err("--batch-lines must be at least 1".to_string());
    }

    let output_cfg = resolve_output_config(&args)?;
    let resolved_yaml = render_output_yaml(&output_cfg);

    if args.dry_run {
        println!("Resolved output config:\n{resolved_yaml}");
        return Ok(());
    }

    println!("Starting benchmark blast...");
    println!(
        "workers={} batch_lines={} duration={}s generator={:?}",
        args.workers, args.batch_lines, args.duration_secs, args.generator
    );
    println!("transform_sql={}", args.transform_sql);
    println!("\nResolved output config:\n{resolved_yaml}");

    let sink_factory = build_sink_factory(
        "blast",
        &output_cfg,
        None,
        Arc::new(ComponentStats::default()),
    )
    .map_err(|e| format!("build sink factory: {e}"))?;

    let counters = Arc::new(Counters {
        rows: AtomicU64::new(0),
        batches: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        raw_bytes: AtomicU64::new(0),
    });

    let deadline = Instant::now() + Duration::from_secs(args.duration_secs);
    let mut handles = Vec::with_capacity(args.workers);

    for worker_id in 0..args.workers {
        let counters = Arc::clone(&counters);
        let sink_factory = Arc::clone(&sink_factory);
        let sql = args.transform_sql.clone();
        let kind = args.generator;
        let payload = generate_payload(kind, args.batch_lines, args.seed + worker_id as u64);
        let payload_len = payload.len() as u64;
        handles.push(std::thread::spawn(move || {
            run_worker(
                worker_id,
                sink_factory,
                counters,
                payload,
                payload_len,
                sql,
                deadline,
            )
        }));
    }

    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err),
            Err(_) => return Err("worker thread panicked".to_string()),
        }
    }

    let elapsed = Duration::from_secs(args.duration_secs).as_secs_f64();
    let rows = counters.rows.load(Ordering::Relaxed);
    let batches = counters.batches.load(Ordering::Relaxed);
    let errors = counters.errors.load(Ordering::Relaxed);
    let raw_bytes = counters.raw_bytes.load(Ordering::Relaxed);

    let lines_per_sec = (rows as f64 / elapsed).round() as u64;
    let mib_per_sec = raw_bytes as f64 / elapsed / 1_048_576.0;

    println!("\n=== Blast Results ===");
    println!("rows_sent        : {rows}");
    println!("batches_sent     : {batches}");
    println!("send_errors      : {errors}");
    println!("raw_throughput   : {:.2} MiB/s", mib_per_sec);
    println!("line_throughput  : {lines_per_sec} lines/s");

    Ok(())
}

fn run_worker(
    worker_id: usize,
    sink_factory: Arc<dyn logfwd_output::sink::SinkFactory>,
    counters: Arc<Counters>,
    payload: Vec<u8>,
    payload_len: u64,
    transform_sql: String,
    deadline: Instant,
) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("worker {worker_id}: tokio runtime: {e}"))?;

    let mut scanner = Scanner::new(ScanConfig::default());
    let mut transform = SqlTransform::new(&transform_sql)
        .map_err(|e| format!("worker {worker_id}: transform compile: {e}"))?;

    let mut sink = sink_factory
        .create()
        .map_err(|e| format!("worker {worker_id}: create sink: {e}"))?;

    let metadata = logfwd_bench::generators::make_metadata();
    let payload = bytes::Bytes::from(payload);

    while Instant::now() < deadline {
        let batch = match scanner.scan_detached(payload.clone()) {
            Ok(batch) => batch,
            Err(err) => {
                counters.errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("worker {worker_id}: scan error: {err}");
                continue;
            }
        };

        let transformed = match transform.execute_blocking(batch) {
            Ok(batch) => batch,
            Err(err) => {
                counters.errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("worker {worker_id}: transform error: {err}");
                continue;
            }
        };

        sink.begin_batch();
        match runtime.block_on(sink.send_batch(&transformed, &metadata)) {
            SendResult::Ok => {
                counters
                    .rows
                    .fetch_add(transformed.num_rows() as u64, Ordering::Relaxed);
                counters.batches.fetch_add(1, Ordering::Relaxed);
                counters.raw_bytes.fetch_add(payload_len, Ordering::Relaxed);
            }
            SendResult::IoError(err) => {
                counters.errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("worker {worker_id}: io error: {err}");
            }
            SendResult::RetryAfter(delay) => {
                counters.errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("worker {worker_id}: retry-after: {delay:?}");
            }
            SendResult::Rejected(reason) => {
                counters.errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("worker {worker_id}: rejected: {reason}");
            }
            _ => {
                counters.errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("worker {worker_id}: unknown send result");
            }
        }
    }

    runtime
        .block_on(sink.shutdown())
        .map_err(|e| format!("worker {worker_id}: shutdown sink: {e}"))?;

    Ok(())
}

fn resolve_output_config(args: &RunArgs) -> Result<OutputConfig, String> {
    let mut output_cfg = if let Some(config_path) = &args.config {
        load_output_from_config(config_path, args.pipeline.as_deref(), args.output_index)?
    } else {
        OutputConfig::default()
    };

    if let Some(destination) = args.destination {
        output_cfg.output_type = destination.as_output_type();
    }

    if let Some(endpoint) = &args.endpoint {
        output_cfg.endpoint = Some(endpoint.clone());
    }
    if let Some(protocol) = &args.protocol {
        output_cfg.protocol = Some(protocol.clone());
    }
    if let Some(compression) = &args.compression {
        output_cfg.compression = Some(compression.clone());
    }
    if let Some(request_mode) = &args.request_mode {
        output_cfg.request_mode = Some(request_mode.clone());
    }
    if let Some(index) = &args.index {
        output_cfg.index = Some(index.clone());
    }
    if let Some(tenant_id) = &args.tenant_id {
        output_cfg.tenant_id = Some(tenant_id.clone());
    }

    let mut headers = HashMap::new();
    for spec in &args.auth_header {
        let (key, value) = split_header(spec)?;
        headers.insert(key.to_string(), value.to_string());
    }

    if args.auth_bearer_token.is_some() || !headers.is_empty() {
        let mut merged = output_cfg.auth.clone().unwrap_or_default();
        if let Some(token) = &args.auth_bearer_token {
            merged.bearer_token = Some(token.clone());
        }
        for (k, v) in headers {
            merged.headers.insert(k, v);
        }
        output_cfg.auth = Some(merged);
    }

    if matches!(
        output_cfg.output_type,
        OutputType::Otlp
            | OutputType::Http
            | OutputType::Elasticsearch
            | OutputType::Loki
            | OutputType::ArrowIpc
            | OutputType::Udp
            | OutputType::Tcp
    ) && output_cfg.endpoint.is_none()
    {
        return Err(
            "endpoint is required for this destination type; use --endpoint or --config"
                .to_string(),
        );
    }

    Ok(output_cfg)
}

fn load_output_from_config(
    config_path: &PathBuf,
    pipeline: Option<&str>,
    output_index: usize,
) -> Result<OutputConfig, String> {
    let config = Config::load(config_path)
        .map_err(|e| format!("load config {}: {e}", config_path.display()))?;

    let pipeline_name = if let Some(name) = pipeline {
        name.to_string()
    } else {
        config
            .pipelines
            .keys()
            .next()
            .cloned()
            .ok_or_else(|| "config has no pipelines".to_string())?
    };

    let pipe = config
        .pipelines
        .get(&pipeline_name)
        .ok_or_else(|| format!("pipeline '{pipeline_name}' not found"))?;

    pipe.outputs
        .get(output_index)
        .cloned()
        .ok_or_else(|| format!("pipeline '{pipeline_name}' has no output at index {output_index}"))
}

fn split_header(spec: &str) -> Result<(&str, &str), String> {
    let Some((key, value)) = spec.split_once('=') else {
        return Err(format!(
            "invalid --auth-header '{spec}'; expected KEY=VALUE"
        ));
    };
    if key.is_empty() {
        return Err(format!(
            "invalid --auth-header '{spec}'; key must not be empty"
        ));
    }
    Ok((key, value))
}

fn generate_payload(kind: GeneratorKind, batch_lines: usize, seed: u64) -> Vec<u8> {
    match kind {
        GeneratorKind::Narrow => logfwd_bench::generators::gen_narrow(batch_lines, seed),
        GeneratorKind::Wide => logfwd_bench::generators::gen_wide(batch_lines, seed),
        GeneratorKind::Cri => logfwd_bench::generators::gen_cri_k8s(batch_lines, seed),
        GeneratorKind::Envoy => logfwd_bench::generators::gen_envoy_access(batch_lines, seed),
    }
}

fn render_output_yaml(output: &OutputConfig) -> String {
    let mut lines = vec![format!("type: {}", output.output_type)];

    if let Some(endpoint) = &output.endpoint {
        lines.push(format!("endpoint: {endpoint}"));
    }
    if let Some(protocol) = &output.protocol {
        lines.push(format!("protocol: {protocol}"));
    }
    if let Some(compression) = &output.compression {
        lines.push(format!("compression: {compression}"));
    }
    if let Some(request_mode) = &output.request_mode {
        lines.push(format!("request_mode: {request_mode}"));
    }
    if let Some(index) = &output.index {
        lines.push(format!("index: {index}"));
    }
    if let Some(tenant_id) = &output.tenant_id {
        lines.push(format!("tenant_id: {tenant_id}"));
    }
    if let Some(auth) = &output.auth {
        if auth.bearer_token.is_some() || !auth.headers.is_empty() {
            lines.push("auth:".to_string());
        }
        if auth.bearer_token.is_some() {
            lines.push("  bearer_token: <redacted>".to_string());
        }
        for (key, _value) in &auth.headers {
            lines.push(format!("  headers.{key}: <redacted>"));
        }
    }

    lines
        .into_iter()
        .map(|line| format!("  {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn print_flag_mapping() {
    println!("config.yaml key -> blast flag mapping\n");
    println!("  output.type           -> --destination");
    println!("  output.endpoint       -> --endpoint");
    println!("  output.protocol       -> --protocol");
    println!("  output.compression    -> --compression");
    println!("  output.request_mode   -> --request-mode");
    println!("  output.index          -> --index");
    println!("  output.tenant_id      -> --tenant-id");
    println!("  output.auth.bearer_token -> --auth-bearer-token");
    println!("  output.auth.headers.* -> --auth-header KEY=VALUE (repeatable)");
    println!("\nErgonomic patterns:");
    println!("  1) Bootstrap from config.yaml + override one-off knobs:");
    println!(
        "     blast run --config ./logfwd.yaml --destination otlp --endpoint http://collector:4318/v1/logs"
    );
    println!("  2) Fully flag-driven quick benchmark:");
    println!(
        "     blast run --destination elasticsearch --endpoint https://es:9200 --index bench --auth-header 'Authorization=ApiKey xyz'"
    );
    println!("  3) Safety preview before sending traffic:");
    println!("     blast run ... --dry-run");
}

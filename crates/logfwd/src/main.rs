#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[cfg(all(unix, not(feature = "dhat-heap")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::env;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::WithExportConfig;
use tokio_util::sync::CancellationToken;

const VERSION: &str = env!("CARGO_PKG_VERSION");

// Exit codes.
const EXIT_OK: i32 = 0;
const EXIT_CONFIG: i32 = 1;
const EXIT_RUNTIME: i32 = 2;

// ---------------------------------------------------------------------------
// Color support (respects NO_COLOR, checks stderr TTY)
// ---------------------------------------------------------------------------

fn use_color() -> bool {
    env::var_os("NO_COLOR").is_none() && unsafe { libc::isatty(libc::STDERR_FILENO) != 0 }
}

macro_rules! style {
    ($color:expr, $bold:expr) => {
        if use_color() {
            concat!("\x1b[", $bold, ";", $color, "m")
        } else {
            ""
        }
    };
}

fn green() -> &'static str {
    style!("32", "0")
}
fn red() -> &'static str {
    style!("31", "1")
}
fn yellow() -> &'static str {
    style!("33", "0")
}
fn bold() -> &'static str {
    if use_color() { "\x1b[1m" } else { "" }
}
fn dim() -> &'static str {
    if use_color() { "\x1b[2m" } else { "" }
}
fn reset() -> &'static str {
    if use_color() { "\x1b[0m" } else { "" }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 || args.iter().any(|a| a == "--help" || a == "-h") {
        print_usage();
        std::process::exit(EXIT_OK);
    }

    if args.iter().any(|a| a == "--version" || a == "-V") {
        println!("logfwd {VERSION}");
        std::process::exit(EXIT_OK);
    }

    let result = match args[1].as_str() {
        "--config" | "-c" => cmd_config(&args).await,
        "--blackhole" => cmd_blackhole(&args),
        "--generate-json" => cmd_generate_json(&args),
        other => {
            eprintln!("{}error{}: unknown command: {other}", red(), reset());
            eprintln!("Run {}logfwd --help{} for usage.", bold(), reset());
            std::process::exit(EXIT_CONFIG);
        }
    };

    if let Err(e) = result {
        eprintln!("{}error{}: {e}", red(), reset());
        std::process::exit(EXIT_RUNTIME);
    }
}

fn print_usage() {
    eprintln!(
        "{}logfwd{} {}v{VERSION}{} -- fast log forwarder with SQL transforms",
        bold(),
        reset(),
        dim(),
        reset(),
    );
    eprintln!();
    eprintln!("{}USAGE:{}", bold(), reset());
    eprintln!("  logfwd --config <config.yaml> [--validate] [--dry-run]");
    eprintln!("  logfwd --blackhole [bind_addr]");
    eprintln!("  logfwd --generate-json <num_lines> <output_file>");
    eprintln!();
    eprintln!("{}OPTIONS:{}", bold(), reset());
    eprintln!("  -c, --config <path>    Run pipeline from YAML config");
    eprintln!("      --validate         Validate config and exit (alias: --check)");
    eprintln!("      --dry-run          Build pipelines without running");
    eprintln!("      --blackhole [addr] Start blackhole sink (default: 127.0.0.1:4318)");
    eprintln!("      --generate-json    Generate synthetic JSON log file");
    eprintln!("  -h, --help             Show this help");
    eprintln!("  -V, --version          Show version");
    eprintln!();
    eprintln!("{}EXIT CODES:{}", bold(), reset());
    eprintln!("  0  Success");
    eprintln!("  1  Configuration error");
    eprintln!("  2  Runtime error");
    eprintln!();
    eprintln!(
        "{}Respects NO_COLOR (https://no-color.org){}",
        dim(),
        reset(),
    );
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

async fn cmd_config(args: &[String]) -> io::Result<()> {
    if args.len() < 3 {
        eprintln!("{}error{}: --config requires a path", red(), reset(),);
        eprintln!("  logfwd --config <config.yaml> [--validate] [--dry-run]");
        std::process::exit(EXIT_CONFIG);
    }

    let config_path = &args[2];
    let mut validate_only = false;
    let mut dry_run = false;

    // Parse flags after the config path — reject unknown flags.
    for arg in &args[3..] {
        match arg.as_str() {
            "--validate" | "--check" => validate_only = true,
            "--dry-run" => dry_run = true,
            other => {
                eprintln!("{}error{}: unknown flag: {other}", red(), reset());
                eprintln!("  logfwd --config <config.yaml> [--validate] [--dry-run]");
                std::process::exit(EXIT_CONFIG);
            }
        }
    }

    let config = match logfwd_config::Config::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{}error{}: {e}", red(), reset());
            std::process::exit(EXIT_CONFIG);
        }
    };

    let base_path = std::path::Path::new(config_path).parent();

    if validate_only || dry_run {
        // Both --validate and --dry-run build pipelines to catch SQL/wiring errors.
        return validate_pipelines(&config, dry_run, base_path);
    }

    // Startup summary.
    eprintln!("{}logfwd{} {}v{VERSION}{}", bold(), reset(), dim(), reset(),);
    for (name, pipe_cfg) in &config.pipelines {
        let n_in = pipe_cfg.inputs.len();
        let n_out = pipe_cfg.outputs.len();
        let sql = pipe_cfg
            .transform
            .as_deref()
            .unwrap_or("SELECT * FROM logs");
        eprintln!(
            "  {}pipeline{} {}{name}{}: {n_in} input(s) {dim}-> {sql} ->{r} {n_out} output(s)",
            dim(),
            reset(),
            bold(),
            reset(),
            dim = dim(),
            r = reset(),
        );
    }

    run_pipelines(config, base_path).await
}

fn cmd_blackhole(args: &[String]) -> io::Result<()> {
    let addr = args.get(2).map(|s| s.as_str()).unwrap_or("127.0.0.1:4318");
    run_blackhole(addr)
}

fn cmd_generate_json(args: &[String]) -> io::Result<()> {
    if args.len() < 4 {
        eprintln!(
            "{}error{}: --generate-json requires <num_lines> <output_file>",
            red(),
            reset(),
        );
        std::process::exit(EXIT_CONFIG);
    }
    let num_lines: usize = match args[2].parse() {
        Ok(n) => n,
        Err(e) => {
            eprintln!(
                "{}error{}: invalid num_lines '{}': {e}",
                red(),
                reset(),
                args[2]
            );
            std::process::exit(EXIT_CONFIG);
        }
    };
    generate_json_log_file(num_lines, &args[3])
}

// ---------------------------------------------------------------------------
// Pipeline runner
// ---------------------------------------------------------------------------

/// Validate config by building all pipelines. Used by --validate and --dry-run.
fn validate_pipelines(
    config: &logfwd_config::Config,
    dry_run: bool,
    base_path: Option<&std::path::Path>,
) -> io::Result<()> {
    use logfwd::pipeline::Pipeline;

    // Build a no-op meter for validation (no OTel export needed).
    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder().build();
    let meter = meter_provider.meter("logfwd");

    let mut errors = 0;
    for (name, pipe_cfg) in &config.pipelines {
        match Pipeline::from_config(name, pipe_cfg, &meter, base_path) {
            Ok(_) => {
                eprintln!("  {}ready{}: {}{name}{}", green(), reset(), bold(), reset());
            }
            Err(e) => {
                eprintln!("  {}error{}: pipeline '{name}': {e}", red(), reset());
                errors += 1;
            }
        }
    }

    if errors > 0 {
        eprintln!("\n{}validation failed{}: {errors} error(s)", red(), reset(),);
        std::process::exit(EXIT_CONFIG);
    }

    let label = if dry_run { "dry run ok" } else { "config ok" };
    eprintln!(
        "{}{label}{}: {} pipeline(s)",
        green(),
        reset(),
        config.pipelines.len(),
    );
    Ok(())
}

async fn run_pipelines(
    config: logfwd_config::Config,
    base_path: Option<&std::path::Path>,
) -> io::Result<()> {
    use logfwd::pipeline::Pipeline;
    use logfwd_core::diagnostics::DiagnosticsServer;
    let shutdown = CancellationToken::new();

    // Listen for SIGINT (Ctrl-C) and SIGTERM to trigger graceful shutdown.
    let shutdown_for_signal = shutdown.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
        }
        shutdown_for_signal.cancel();
    });

    let meter_provider = build_meter_provider(&config)?;
    let meter = meter_provider.meter("logfwd");

    let mut pipelines = Vec::new();
    for (name, pipe_cfg) in &config.pipelines {
        match Pipeline::from_config(name, pipe_cfg, &meter, base_path) {
            Ok(pipeline) => {
                eprintln!("  {}ready{}: {}{name}{}", green(), reset(), bold(), reset());
                pipelines.push(pipeline);
            }
            Err(e) => {
                eprintln!("  {}error{}: pipeline '{name}': {e}", red(), reset(),);
                std::process::exit(EXIT_CONFIG);
            }
        }
    }

    let _diag_handle = if let Some(ref addr) = config.server.diagnostics {
        let mut server = DiagnosticsServer::new(addr);
        for p in &pipelines {
            server.add_pipeline(Arc::clone(p.metrics()));
        }
        #[cfg(unix)]
        server.set_memory_stats_fn(jemalloc_stats);
        let handle = server.start()?;
        eprintln!("  {}diagnostics{}: http://{addr}", dim(), reset());
        Some(handle)
    } else {
        None
    };

    eprintln!(
        "{}logfwd running{} ({} pipeline(s))",
        green(),
        reset(),
        pipelines.len(),
    );

    let mut handles = Vec::new();
    let main_pipeline = pipelines.pop();

    for mut pipeline in pipelines {
        let sd = shutdown.clone();
        handles.push(tokio::spawn(async move { pipeline.run_async(&sd).await }));
    }

    if let Some(mut main_pipe) = main_pipeline {
        let result = main_pipe.run_async(&shutdown).await;
        // Always cancel + join siblings, even if main pipeline errored.
        shutdown.cancel();
        let mut had_sibling_error = false;
        for h in handles {
            match h.await {
                Ok(Err(e)) => {
                    eprintln!("pipeline error: {e}");
                    had_sibling_error = true;
                }
                Err(e) => {
                    eprintln!("pipeline task panicked: {e}");
                    had_sibling_error = true;
                }
                Ok(Ok(())) => {}
            }
        }
        result?;
        if had_sibling_error {
            return Err(io::Error::other("one or more sibling pipelines failed"));
        }
    } else {
        let mut had_error = false;
        for h in handles {
            match h.await {
                Ok(Err(e)) => {
                    eprintln!("pipeline error: {e}");
                    had_error = true;
                }
                Err(e) => {
                    eprintln!("pipeline task panicked: {e}");
                    had_error = true;
                }
                Ok(Ok(())) => {}
            }
        }
        if had_error {
            return Err(io::Error::other("one or more pipelines failed"));
        }
    }

    if let Err(e) = meter_provider.shutdown() {
        eprintln!(
            "{}warning{}: meter provider shutdown: {e}",
            yellow(),
            reset()
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Blackhole sink
// ---------------------------------------------------------------------------

fn run_blackhole(addr: &str) -> io::Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering};

    eprintln!(
        "{}logfwd blackhole{} listening on {}{}{}",
        bold(),
        reset(),
        bold(),
        addr,
        reset(),
    );
    eprintln!(
        "  {}POST any path -> 200 OK (counts bytes/lines){}",
        dim(),
        reset(),
    );
    eprintln!("  {}GET /stats -> JSON counters{}", dim(), reset());

    let server = tiny_http::Server::http(addr).map_err(|e| io::Error::other(e.to_string()))?;

    let total_requests = Arc::new(AtomicU64::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));
    let total_lines = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let reqs_clone = Arc::clone(&total_requests);
    let bytes_clone = Arc::clone(&total_bytes);
    let lines_clone = Arc::clone(&total_lines);
    std::thread::spawn(move || {
        let mut prev_lines = 0u64;
        let mut prev_bytes = 0u64;
        loop {
            std::thread::sleep(Duration::from_secs(1));
            let reqs = reqs_clone.load(Ordering::Relaxed);
            let lines = lines_clone.load(Ordering::Relaxed);
            let bytes = bytes_clone.load(Ordering::Relaxed);
            let d_lines = lines - prev_lines;
            let d_bytes = bytes - prev_bytes;
            if d_lines > 0 || d_bytes > 0 {
                eprint!(
                    "\r  {} reqs | {} lines ({}/s) | {:.1} MB ({:.1} MB/s)    ",
                    reqs,
                    lines,
                    d_lines,
                    bytes as f64 / (1024.0 * 1024.0),
                    d_bytes as f64 / (1024.0 * 1024.0),
                );
                io::stderr().flush().ok();
            }
            prev_lines = lines;
            prev_bytes = bytes;
        }
    });

    let es_bulk_response = r#"{"took":0,"errors":false,"items":[]}"#;
    let stats_reqs = Arc::clone(&total_requests);
    let stats_bytes = Arc::clone(&total_bytes);
    let stats_lines = Arc::clone(&total_lines);

    for mut request in server.incoming_requests() {
        if request.method() == &tiny_http::Method::Get && request.url() == "/stats" {
            let body = format!(
                r#"{{"requests":{},"lines":{},"bytes":{}}}"#,
                stats_reqs.load(Ordering::Relaxed),
                stats_lines.load(Ordering::Relaxed),
                stats_bytes.load(Ordering::Relaxed),
            );
            let header =
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .map_err(|()| io::Error::other("invalid HTTP header"))?;
            let resp = tiny_http::Response::from_string(body)
                .with_status_code(200)
                .with_header(header);
            let _ = request.respond(resp);
            continue;
        }

        let content_len = request.body_length().unwrap_or(0);
        let mut body = Vec::with_capacity(content_len);
        request.as_reader().read_to_end(&mut body).ok();

        let line_count = memchr::memchr_iter(b'\n', &body).count() as u64;
        total_bytes.fetch_add(body.len() as u64, Ordering::Relaxed);
        total_lines.fetch_add(line_count, Ordering::Relaxed);
        total_requests.fetch_add(1, Ordering::Relaxed);

        let is_bulk = request.url().contains("/_bulk");
        let resp_body = if is_bulk { es_bulk_response } else { "{}" };

        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .map_err(|()| io::Error::other("invalid HTTP header"))?;
        let resp = tiny_http::Response::from_string(resp_body)
            .with_status_code(200)
            .with_header(header);
        let _ = request.respond(resp);
    }

    let elapsed = start.elapsed().as_secs_f64();
    let reqs = total_requests.load(Ordering::Relaxed);
    let bytes = total_bytes.load(Ordering::Relaxed);
    let lines = total_lines.load(Ordering::Relaxed);
    eprintln!(
        "\nDone: {} requests, {} lines, {:.1} MB in {:.1}s",
        reqs,
        lines,
        bytes as f64 / (1024.0 * 1024.0),
        elapsed,
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// OTel metrics
// ---------------------------------------------------------------------------

fn build_meter_provider(
    config: &logfwd_config::Config,
) -> io::Result<opentelemetry_sdk::metrics::SdkMeterProvider> {
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    if let Some(ref endpoint) = config.server.metrics_endpoint {
        let interval_secs = config.server.metrics_interval_secs.unwrap_or(60);

        let otlp_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .build()
            .map_err(|e| io::Error::other(format!("OTLP metric exporter: {e}")))?;

        let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(otlp_exporter)
            .with_interval(std::time::Duration::from_secs(interval_secs))
            .build();

        eprintln!(
            "  {}metrics push{}: {endpoint} (every {interval_secs}s)",
            dim(),
            reset(),
        );

        Ok(SdkMeterProvider::builder().with_reader(reader).build())
    } else {
        Ok(SdkMeterProvider::builder().build())
    }
}

// ---------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------

fn generate_json_log_file(num_lines: usize, output: &str) -> io::Result<()> {
    use std::io::BufWriter;

    eprintln!(
        "Generating {}{num_lines}{} JSON log lines to {}{output}{}...",
        bold(),
        reset(),
        bold(),
        reset(),
    );

    let file = std::fs::File::create(output)?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);

    let levels = ["INFO", "DEBUG", "WARN", "ERROR"];
    let paths = [
        "/api/v1/users",
        "/api/v1/orders",
        "/api/v2/products",
        "/health",
        "/api/v1/auth",
    ];

    for i in 0..num_lines {
        let level = levels[i % 4];
        let path = paths[i % 5];
        let id = 10000 + (i * 7) % 90000;
        let dur = 1 + (i * 13) % 500;
        let rid = format!("{:016x}", (i as u64).wrapping_mul(0x517cc1b727220a95));

        write!(
            writer,
            r#"{{"timestamp":"2024-01-15T10:30:00.{:03}Z","level":"{}","message":"request handled GET {}/{}","duration_ms":{},"request_id":"{}","service":"myapp"}}"#,
            i % 1000,
            level,
            path,
            id,
            dur,
            rid,
        )?;
        writer.write_all(b"\n")?;
    }

    writer.flush()?;
    let size = std::fs::metadata(output)?.len();
    eprintln!(
        "{}done{}: {:.1} MB, avg {:.0} bytes/line",
        green(),
        reset(),
        size as f64 / (1024.0 * 1024.0),
        size as f64 / num_lines as f64,
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Allocator memory stats
// ---------------------------------------------------------------------------

#[cfg(unix)]
/// Read jemalloc memory stats: resident, allocated, and active bytes.
///
/// Returns `None` if the stats are unavailable (e.g. the epoch refresh fails).
/// This function is passed to [`DiagnosticsServer`] so the `/api/pipelines`
/// endpoint can expose live allocator metrics.
fn jemalloc_stats() -> Option<logfwd_core::diagnostics::MemoryStats> {
    use tikv_jemalloc_ctl::{epoch, stats};

    // Refresh the epoch so subsequent reads reflect current allocator state.
    epoch::mib().ok()?.advance().ok()?;

    let resident = stats::resident::mib().ok()?.read().ok()?;
    let allocated = stats::allocated::mib().ok()?.read().ok()?;
    let active = stats::active::mib().ok()?.read().ok()?;

    Some(logfwd_core::diagnostics::MemoryStats {
        resident,
        allocated,
        active,
    })
}

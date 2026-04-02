#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[cfg(all(unix, not(feature = "dhat-heap")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::env;
use std::io::{self, Write};
use std::sync::Arc;

use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::WithExportConfig;
use tokio_util::sync::CancellationToken;

const VERSION: &str = env!("CARGO_PKG_VERSION");

// Exit codes.
const EXIT_OK: i32 = 0;
const EXIT_CONFIG: i32 = 1;
const EXIT_RUNTIME: i32 = 2;

#[derive(Debug)]
enum CliError {
    Config(String),
    Runtime(io::Error),
}

impl CliError {
    fn exit_code(&self) -> i32 {
        match self {
            Self::Config(_) => EXIT_CONFIG,
            Self::Runtime(_) => EXIT_RUNTIME,
        }
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(msg) => write!(f, "{msg}"),
            Self::Runtime(err) => write!(f, "{err}"),
        }
    }
}

impl From<io::Error> for CliError {
    fn from(value: io::Error) -> Self {
        Self::Runtime(value)
    }
}

// ---------------------------------------------------------------------------
// Color support (respects NO_COLOR, checks stderr TTY)
// ---------------------------------------------------------------------------

fn use_color() -> bool {
    // SAFETY: isatty is a simple query on a well-known fd; no invariants to uphold.
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

fn main() {
    let code = main_inner();
    if code != 0 {
        std::process::exit(code);
    }
}

#[tokio::main]
async fn main_inner() -> i32 {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 || args.iter().any(|a| a == "--help" || a == "-h") {
        print_usage();
        return EXIT_OK;
    }

    if args.iter().any(|a| a == "--version" || a == "-V") {
        println!("logfwd {VERSION}");
        return EXIT_OK;
    }

    let result = match args[1].as_str() {
        "--config" | "-c" => cmd_config(&args).await,
        "--blackhole" => cmd_blackhole(&args).await,
        "--generate-json" => cmd_generate_json(&args),
        other => {
            eprintln!("{}error{}: unknown command: {other}", red(), reset());
            eprintln!("Run {}logfwd --help{} for usage.", bold(), reset());
            return EXIT_CONFIG;
        }
    };

    match result {
        Ok(()) => EXIT_OK,
        Err(e) => {
            eprintln!("{}error{}: {e}", red(), reset());
            e.exit_code()
        }
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
    eprintln!("      --blackhole [addr] OTLP blackhole receiver (default: 127.0.0.1:4318)");
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

async fn cmd_config(args: &[String]) -> Result<(), CliError> {
    if args.len() < 3 {
        eprintln!("{}error{}: --config requires a path", red(), reset(),);
        eprintln!("  logfwd --config <config.yaml> [--validate] [--dry-run]");
        return Err(CliError::Config("missing config path".to_owned()));
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
                return Err(CliError::Config(format!("unknown flag: {other}")));
            }
        }
    }

    let config_yaml = std::fs::read_to_string(config_path).unwrap_or_else(|e| {
        eprintln!("{}error{}: cannot read {config_path}: {e}", red(), reset());
        std::process::exit(EXIT_CONFIG);
    });
    let config = match logfwd_config::Config::load_str(&config_yaml) {
        Ok(c) => c,
        Err(e) => {
            return Err(CliError::Config(e.to_string()));
        }
    };

    let base_path = std::path::Path::new(config_path).parent();

    if validate_only || dry_run {
        // Both --validate and --dry-run build pipelines to catch SQL/wiring errors.
        return validate_pipelines(&config, dry_run, base_path);
    }

    run_pipelines(config, base_path, config_path, &config_yaml).await
}

async fn cmd_blackhole(args: &[String]) -> Result<(), CliError> {
    let addr = args
        .get(2)
        .map_or("127.0.0.1:4318", std::string::String::as_str);

    // Validate addr looks like host:port — reject anything that could inject YAML.
    if !addr.contains(':') || addr.contains('\n') || addr.contains(' ') {
        return Err(CliError::Config(format!("invalid bind address: {addr}")));
    }

    let yaml = format!(
        "input:\n  type: otlp\n  listen: {addr}\noutput:\n  type: null\nserver:\n  diagnostics: 127.0.0.1:9090\n"
    );
    let config = logfwd_config::Config::load_str(&yaml)
        .map_err(|e| CliError::Config(format!("internal config error: {e}")))?;

    eprintln!(
        "{}logfwd blackhole{} starting on {}{addr}{}",
        bold(),
        reset(),
        bold(),
        reset(),
    );

    run_pipelines(config, None, "<blackhole>", &yaml).await
}

fn cmd_generate_json(args: &[String]) -> Result<(), CliError> {
    if args.len() < 4 {
        eprintln!(
            "{}error{}: --generate-json requires <num_lines> <output_file>",
            red(),
            reset(),
        );
        return Err(CliError::Config("missing arguments".to_owned()));
    }
    let num_lines: usize = match args[2].parse() {
        Ok(n) => n,
        Err(e) => {
            return Err(CliError::Config(format!("invalid num_lines: {e}")));
        }
    };
    generate_json_log_file(num_lines, &args[3]).map_err(CliError::Runtime)
}

// ---------------------------------------------------------------------------
// Pipeline runner
// ---------------------------------------------------------------------------

/// Validate config by building all pipelines. Used by --validate and --dry-run.
fn validate_pipelines(
    config: &logfwd_config::Config,
    dry_run: bool,
    base_path: Option<&std::path::Path>,
) -> Result<(), CliError> {
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
        return Err(CliError::Config(format!(
            "{errors} error(s) during validation"
        )));
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

fn input_label(i: &logfwd_config::InputConfig) -> String {
    use logfwd_config::InputType;
    match i.input_type {
        InputType::File => format!("file  {}", i.path.as_deref().unwrap_or("*")),
        InputType::Tcp => format!("tcp   {}", i.listen.as_deref().unwrap_or(":514")),
        InputType::Udp => format!("udp   {}", i.listen.as_deref().unwrap_or(":514")),
        InputType::Otlp => format!("otlp  {}", i.listen.as_deref().unwrap_or(":4318")),
        InputType::Generator => "generator".to_string(),
        _ => "unknown".to_string(),
    }
}

fn output_label(o: &logfwd_config::OutputConfig) -> String {
    use logfwd_config::OutputType;
    match o.output_type {
        OutputType::Otlp => format!("otlp  {}", o.endpoint.as_deref().unwrap_or("")),
        OutputType::Http => format!("http  {}", o.endpoint.as_deref().unwrap_or("")),
        OutputType::Elasticsearch => {
            format!("elasticsearch  {}", o.endpoint.as_deref().unwrap_or(""))
        }
        OutputType::Loki => format!("loki  {}", o.endpoint.as_deref().unwrap_or("")),
        OutputType::TcpOut => format!("tcp   {}", o.endpoint.as_deref().unwrap_or("")),
        OutputType::UdpOut => format!("udp   {}", o.endpoint.as_deref().unwrap_or("")),
        OutputType::FileOut => format!("file  {}", o.path.as_deref().unwrap_or("")),
        OutputType::Parquet => format!("parquet  {}", o.path.as_deref().unwrap_or("")),
        OutputType::Stdout => "stdout".to_string(),
        OutputType::Null => "null".to_string(),
        _ => "unknown".to_string(),
    }
}

async fn run_pipelines(
    config: logfwd_config::Config,
    base_path: Option<&std::path::Path>,
    config_path: &str,
    config_yaml: &str,
) -> Result<(), CliError> {
    use logfwd::pipeline::Pipeline;
    use logfwd_io::diagnostics::DiagnosticsServer;
    let shutdown = CancellationToken::new();

    // Listen for SIGINT (Ctrl-C) and SIGTERM to trigger graceful shutdown.
    #[cfg(feature = "dhat-heap")]
    let profiler = dhat::Profiler::new_heap();

    #[cfg(feature = "cpu-profiling")]
    let pprof_guard = pprof::ProfilerGuardBuilder::default()
        .frequency(999)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| {
            CliError::Runtime(io::Error::other(format!(
                "failed to initialize pprof profiler: {e}"
            )))
        })?;

    let shutdown_for_signal = shutdown.clone();
    tokio::spawn(async move {
        #[cfg(feature = "dhat-heap")]
        let _profiler_to_drop = profiler;

        #[cfg(feature = "cpu-profiling")]
        let _pprof_to_drop = pprof_guard;

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

        #[cfg(feature = "cpu-profiling")]
        {
            if let Ok(report) = _pprof_to_drop.report().build() {
                if let Ok(file) = std::fs::File::create("flamegraph.svg") {
                    let _ = report.flamegraph(file);
                }
            }
        }

        #[cfg(feature = "dhat-heap")]
        drop(_profiler_to_drop);

        shutdown_for_signal.cancel();
    });

    let meter_provider = build_meter_provider(&config)?;
    let meter = meter_provider.meter("logfwd");

    let mut pipelines = Vec::new();
    for (name, pipe_cfg) in &config.pipelines {
        match Pipeline::from_config(name, pipe_cfg, &meter, base_path) {
            Ok(pipeline) => {
                pipelines.push(pipeline);
            }
            Err(e) => {
                return Err(CliError::Config(format!("pipeline '{name}': {e}")));
            }
        }
    }

    let diag_handle = if let Some(ref addr) = config.server.diagnostics {
        let mut server = DiagnosticsServer::new(addr);
        server.set_config(config_path, config_yaml);
        for p in &pipelines {
            server.add_pipeline(Arc::clone(p.metrics()));
        }
        #[cfg(unix)]
        server.set_memory_stats_fn(jemalloc_stats);
        let handle = server.start()?;
        Some((handle, addr.clone()))
    } else {
        None
    };

    eprintln!("{}logfwd{} {}v{VERSION}{}", bold(), reset(), dim(), reset());

    // Print startup summary after everything is ready.
    for (name, pipe_cfg) in &config.pipelines {
        eprintln!();
        eprintln!("  {}✓{}  {}{name}{}", green(), reset(), bold(), reset());
        for input in &pipe_cfg.inputs {
            eprintln!("     {}in{}   {}", dim(), reset(), input_label(input));
        }
        if let Some(sql) = pipe_cfg.transform.as_deref() {
            let sql = sql.trim();
            let first_line = sql.lines().next().unwrap_or(sql);
            let truncated = if first_line.chars().count() > 100 {
                format!("{}…", first_line.chars().take(100).collect::<String>())
            } else {
                first_line.to_string()
            };
            eprintln!("     {}sql{}  {truncated}", dim(), reset());
        }
        for output in &pipe_cfg.outputs {
            eprintln!("     {}out{}  {}", dim(), reset(), output_label(output));
        }
    }
    if let Some((_, ref addr)) = diag_handle {
        eprintln!();
        eprintln!("  {}dashboard{}  http://{addr}", bold(), reset());
    }
    eprintln!();
    let n = pipelines.len();
    eprintln!(
        "{}ready{} · {n} pipeline{}",
        green(),
        reset(),
        if n == 1 { "" } else { "s" },
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
            return Err(CliError::Runtime(io::Error::other(
                "one or more sibling pipelines failed",
            )));
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
            return Err(CliError::Runtime(io::Error::other(
                "one or more pipelines failed",
            )));
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
fn jemalloc_stats() -> Option<logfwd_io::diagnostics::MemoryStats> {
    use tikv_jemalloc_ctl::{epoch, stats};

    // Refresh the epoch so subsequent reads reflect current allocator state.
    epoch::mib().ok()?.advance().ok()?;

    let resident = stats::resident::mib().ok()?.read().ok()?;
    let allocated = stats::allocated::mib().ok()?.read().ok()?;
    let active = stats::active::mib().ok()?.read().ok()?;

    Some(logfwd_io::diagnostics::MemoryStats {
        resident,
        allocated,
        active,
    })
}

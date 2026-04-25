//! CLI bootstrap: user-facing stderr output (banners, version info, SIGHUP
//! warnings) is intentional and intentionally not routed through `tracing`.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use opentelemetry::metrics::MeterProvider;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::pipeline::Pipeline;

/// Error returned while constructing or running the runtime orchestration shell.
#[derive(Debug)]
pub enum RuntimeError {
    /// Configuration or pipeline-build validation failed before the runtime started.
    Config(String),
    /// Runtime I/O or OS integration failed while bootstrapping or running pipelines.
    Io(io::Error),
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(msg) => write!(f, "{msg}"),
            Self::Io(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<io::Error> for RuntimeError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

/// Inputs needed by the runtime bootstrap layer that are owned by the CLI facade.
pub struct RunOptions<'a> {
    /// Resolved config path shown in diagnostics and startup output.
    pub config_path: &'a str,
    /// Normalized config YAML snapshot exposed to diagnostics consumers.
    pub config_yaml: &'a str,
    /// Human-readable CLI version string used in the startup banner.
    pub version: &'a str,
    /// Whether ANSI color output should be used for stderr banners.
    pub use_color: bool,
    /// Whether stderr logs should be emitted as JSON instead of text.
    pub json_logs_for_stderr: bool,
    /// Optional auto-shutdown duration for benchmarking helpers.
    pub auto_shutdown_after: Option<Duration>,
}

pub async fn run_pipelines(
    config: ffwd_config::Config,
    base_path: Option<&Path>,
    options: RunOptions<'_>,
) -> Result<(), RuntimeError> {
    let startup_start = Instant::now();

    let has_file_inputs = config.pipelines.values().any(|pipe| {
        pipe.inputs
            .iter()
            .any(|input| matches!(&input.type_config, ffwd_config::InputTypeConfig::File(_)))
    });
    let _lock_guard = if has_file_inputs {
        acquire_instance_lock(&config)?
    } else {
        None
    };
    let configured_data_dir = config.storage.data_dir.as_ref().map(PathBuf::from);

    let shutdown = CancellationToken::new();

    #[cfg(unix)]
    let (mut sigterm, mut sighup, mut sigusr1, mut sigusr2) = {
        use tokio::signal::unix::{SignalKind, signal};
        let sigterm = signal(SignalKind::terminate()).map_err(|err| {
            RuntimeError::Io(io::Error::other(format!(
                "failed to register SIGTERM handler: {err}"
            )))
        })?;
        let sighup = signal(SignalKind::hangup()).map_err(|err| {
            RuntimeError::Io(io::Error::other(format!(
                "failed to register SIGHUP handler: {err}"
            )))
        })?;
        let sigusr1 = signal(SignalKind::user_defined1()).map_err(|err| {
            RuntimeError::Io(io::Error::other(format!(
                "failed to register SIGUSR1 handler: {err}"
            )))
        })?;
        let sigusr2 = signal(SignalKind::user_defined2()).map_err(|err| {
            RuntimeError::Io(io::Error::other(format!(
                "failed to register SIGUSR2 handler: {err}"
            )))
        })?;
        (sigterm, sighup, sigusr1, sigusr2)
    };

    #[cfg(feature = "dhat-heap")]
    let profiler = dhat::Profiler::new_heap();

    #[cfg(feature = "cpu-profiling")]
    let pprof_guard = pprof::ProfilerGuardBuilder::default()
        .frequency(999)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| {
            RuntimeError::Io(io::Error::other(format!(
                "failed to initialize pprof profiler: {e}"
            )))
        })?;

    let shutdown_for_signal = shutdown.clone();
    let use_color = options.use_color;
    tokio::spawn(async move {
        #[cfg(feature = "dhat-heap")]
        let profiler_to_drop = profiler;

        #[cfg(feature = "cpu-profiling")]
        let pprof_to_drop = pprof_guard;

        #[cfg(unix)]
        {
            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => break,
                    _ = sigterm.recv() => break,
                    _ = sigusr1.recv() => break,
                    _ = sigusr2.recv() => break,
                    _ = sighup.recv() => {
                        eprintln!(
                            "{}ffwd{}: SIGHUP received — config reload not yet implemented, ignoring",
                            yellow(use_color), reset(use_color),
                        );
                    }
                }
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
        }

        #[cfg(feature = "cpu-profiling")]
        {
            if let Ok(report) = pprof_to_drop.report().build()
                && let Ok(file) = std::fs::File::create("flamegraph.svg")
            {
                let _ = report.flamegraph(file);
            }
        }

        #[cfg(feature = "dhat-heap")]
        drop(profiler_to_drop);

        shutdown_for_signal.cancel();
    });

    if let Some(duration) = options.auto_shutdown_after {
        let shutdown_for_timer = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            shutdown_for_timer.cancel();
        });
    }

    let meter_provider = build_meter_provider(&config, use_color)?;
    let meter = meter_provider.meter("ffwd");

    let trace_buf = ffwd_diagnostics::span_exporter::SpanBuffer::new();
    let tracer_provider = build_tracer_provider(trace_buf.clone(), &config, use_color)?;
    let tracer = tracer_provider.tracer("ffwd");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let env_filter = tracing_subscriber::EnvFilter::try_from_env("LOGFWD_LOG")
        .or_else(|_| tracing_subscriber::EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let fmt_layer = if options.json_logs_for_stderr {
        tracing_subscriber::fmt::layer()
            .json()
            .with_writer(io::stderr)
            .with_target(true)
            .boxed()
    } else {
        tracing_subscriber::fmt::layer()
            .with_writer(io::stderr)
            .with_target(true)
            .boxed()
    };
    let _ = tracing_subscriber::registry()
        .with(fmt_layer.with_filter(env_filter))
        .with(otel_layer)
        .try_init();
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    let mut pipelines = Vec::new();
    for (name, pipe_cfg) in &config.pipelines {
        match Pipeline::from_config_with_data_dir(
            name,
            pipe_cfg,
            &meter,
            base_path,
            configured_data_dir.as_deref(),
        ) {
            Ok(pipeline) => pipelines.push(pipeline),
            Err(e) => return Err(RuntimeError::Config(format!("pipeline '{name}': {e}"))),
        }
    }

    let diag_handle = if let Some(ref addr) = config.server.diagnostics {
        let mut server = ffwd_diagnostics::diagnostics::DiagnosticsServer::new(addr);
        server.set_config(options.config_path, options.config_yaml);
        let expose_config = std::env::var("LOGFWD_UNSAFE_EXPOSE_CONFIG")
            .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
        server.set_config_endpoint_enabled(expose_config);
        server.set_trace_buffer(trace_buf);
        for p in &pipelines {
            server.add_pipeline(Arc::clone(p.metrics()));
        }
        #[cfg(unix)]
        server.set_memory_stats_fn(jemalloc_stats);
        let (handle, _) = server.start()?;
        Some((handle, addr.clone()))
    } else {
        None
    };

    let pipeline_metrics: Vec<_> = pipelines.iter().map(|p| Arc::clone(p.metrics())).collect();

    eprintln!(
        "{}ffwd{} {}v{}{}",
        bold(use_color),
        reset(use_color),
        dim(use_color),
        options.version,
        reset(use_color),
    );

    for (name, pipe_cfg) in &config.pipelines {
        eprintln!();
        eprintln!(
            "  {}✓{}  {}{name}{}",
            green(use_color),
            reset(use_color),
            bold(use_color),
            reset(use_color)
        );
        for input in &pipe_cfg.inputs {
            eprintln!(
                "     {}in{}   {}",
                dim(use_color),
                reset(use_color),
                input_label(input)
            );
        }
        if let Some(sql) = pipe_cfg.transform.as_deref() {
            let sql = sql.trim();
            let first_line = sql.lines().next().unwrap_or(sql);
            let truncated = if first_line.chars().count() > 100 {
                format!("{}…", first_line.chars().take(100).collect::<String>())
            } else {
                first_line.to_string()
            };
            eprintln!(
                "     {}sql{}  {truncated}",
                dim(use_color),
                reset(use_color)
            );
        }
        for output in &pipe_cfg.outputs {
            eprintln!(
                "     {}out{}  {}",
                dim(use_color),
                reset(use_color),
                output_label(output)
            );
        }
    }
    if let Some((_, ref addr)) = diag_handle {
        eprintln!();
        eprintln!(
            "  {}dashboard{}  http://{addr}",
            bold(use_color),
            reset(use_color)
        );
    }
    eprintln!();
    let n = pipelines.len();
    let startup_ms = startup_start.elapsed().as_millis();
    eprintln!(
        "{}ready{} · {n} pipeline{} {}(started in {startup_ms}ms){}",
        green(use_color),
        reset(use_color),
        if n == 1 { "" } else { "s" },
        dim(use_color),
        reset(use_color),
    );

    let mut handles = Vec::new();
    let main_pipeline = pipelines.pop();

    for mut pipeline in pipelines {
        let sd = shutdown.clone();
        handles.push(tokio::spawn(async move { pipeline.run_async(&sd).await }));
    }

    if let Some(mut main_pipe) = main_pipeline {
        let result = main_pipe.run_async(&shutdown).await;
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
            return Err(RuntimeError::Io(io::Error::other(
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
            return Err(RuntimeError::Io(io::Error::other(
                "one or more pipelines failed",
            )));
        }
    }

    print_shutdown_stats(&pipeline_metrics, startup_start.elapsed(), use_color);

    if let Err(e) = meter_provider.shutdown() {
        eprintln!(
            "{}warning{}: meter provider shutdown: {e}",
            yellow(use_color),
            reset(use_color)
        );
    }
    if let Err(e) = tracer_provider.shutdown() {
        eprintln!(
            "{}warning{}: tracer provider shutdown: {e}",
            yellow(use_color),
            reset(use_color)
        );
    }

    Ok(())
}

pub fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    }
}

pub fn format_count(n: u64) -> String {
    if n < 1_000 {
        n.to_string()
    } else if n < 1_000_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else if n < 1_000_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    }
}

pub fn format_bytes(b: u64) -> String {
    if b < 1024 {
        format!("{b} B")
    } else if b < 1024 * 1024 {
        format!("{:.1} KB", b as f64 / 1024.0)
    } else if b < 1024 * 1024 * 1024 {
        format!("{:.1} MB", b as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", b as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

fn input_label(i: &ffwd_config::InputConfig) -> String {
    use ffwd_config::InputTypeConfig;
    match &i.type_config {
        InputTypeConfig::File(f) => format!("file  {}", f.path),
        InputTypeConfig::Tcp(t) => format!("tcp   {}", t.listen),
        InputTypeConfig::Udp(u) => format!("udp   {}", u.listen),
        InputTypeConfig::Otlp(o) => format!("otlp  {}", o.listen),
        InputTypeConfig::Http(h) => format!("http  {}", h.listen),
        InputTypeConfig::Stdin(_) => "stdin".to_string(),
        InputTypeConfig::ArrowIpc(a) => format!("arrow_ipc  {}", a.listen),
        InputTypeConfig::Generator(_) => "generator".to_string(),
        InputTypeConfig::LinuxEbpfSensor(_) => "linux_ebpf_sensor".to_string(),
        InputTypeConfig::MacosEsSensor(_) => "macos_es_sensor".to_string(),
        InputTypeConfig::WindowsEbpfSensor(_) => "windows_ebpf_sensor".to_string(),
        InputTypeConfig::HostMetrics(_) => "host_metrics".to_string(),
        InputTypeConfig::Journald(_) => "journald".to_string(),
        InputTypeConfig::S3(s) => format!("s3    {}", s.s3.bucket),
        InputTypeConfig::MacosLog(_) => "macos log stream".to_string(),
    }
}

fn output_label(o: &ffwd_config::OutputConfigV2) -> String {
    use ffwd_config::OutputConfigV2;
    match o {
        OutputConfigV2::Otlp(config) => {
            format!("otlp  {}", config.endpoint.as_deref().unwrap_or(""))
        }
        OutputConfigV2::Http(config) => {
            format!("http  {}", config.endpoint.as_deref().unwrap_or(""))
        }
        OutputConfigV2::Elasticsearch(config) => {
            format!(
                "elasticsearch  {}",
                config.endpoint.as_deref().unwrap_or("")
            )
        }
        OutputConfigV2::Loki(config) => {
            format!("loki  {}", config.endpoint.as_deref().unwrap_or(""))
        }
        OutputConfigV2::Tcp(config) => {
            format!("tcp   {}", config.endpoint.as_deref().unwrap_or(""))
        }
        OutputConfigV2::Udp(config) => {
            format!("udp   {}", config.endpoint.as_deref().unwrap_or(""))
        }
        OutputConfigV2::File(config) => {
            format!("file  {}", config.path.as_deref().unwrap_or(""))
        }
        OutputConfigV2::Parquet(config) => {
            format!("parquet  {}", config.path.as_deref().unwrap_or(""))
        }
        OutputConfigV2::Stdout(_) => "stdout".to_string(),
        OutputConfigV2::Null(_) => "null".to_string(),
        OutputConfigV2::ArrowIpc(config) => {
            format!("arrow_ipc  {}", config.endpoint.as_deref().unwrap_or(""))
        }
        _ => o.output_type().to_string(),
    }
}

fn print_shutdown_stats(
    metrics: &[Arc<ffwd_diagnostics::diagnostics::PipelineMetrics>],
    uptime: Duration,
    use_color: bool,
) {
    use std::sync::atomic::Ordering::Relaxed;

    let total_lines_in: u64 = metrics
        .iter()
        .map(|m| m.transform_in.lines_total.load(Relaxed))
        .sum();
    let total_lines_out: u64 = metrics
        .iter()
        .map(|m| m.transform_out.lines_total.load(Relaxed))
        .sum();
    let total_bytes_in: u64 = metrics
        .iter()
        .map(|m| {
            m.inputs
                .iter()
                .map(|(_, _, stats)| stats.bytes_total.load(Relaxed))
                .sum::<u64>()
        })
        .sum();
    let total_batches: u64 = metrics.iter().map(|m| m.batches_total.load(Relaxed)).sum();
    let total_errors: u64 = metrics
        .iter()
        .map(|m| m.transform_errors.load(Relaxed))
        .sum();
    let total_dropped: u64 = metrics
        .iter()
        .map(|m| m.dropped_batches_total.load(Relaxed))
        .sum();

    eprintln!();
    eprintln!(
        "{}stopped{} · uptime {}",
        dim(use_color),
        reset(use_color),
        format_duration(uptime),
    );

    if total_lines_in > 0 || total_batches > 0 {
        eprintln!(
            "  lines  {} in → {} out  ({} batches)",
            format_count(total_lines_in),
            format_count(total_lines_out),
            format_count(total_batches),
        );
        if total_bytes_in > 0 {
            eprintln!("  bytes  {} in", format_bytes(total_bytes_in));
        }
        if total_errors > 0 || total_dropped > 0 {
            eprintln!(
                "  {}errors{} {} transform, {} dropped batches",
                yellow(use_color),
                reset(use_color),
                total_errors,
                total_dropped,
            );
        }
    }
}

fn acquire_instance_lock(
    config: &ffwd_config::Config,
) -> Result<Option<std::fs::File>, RuntimeError> {
    let data_dir = config
        .storage
        .data_dir
        .as_ref()
        .map_or_else(ffwd_io::checkpoint::default_data_dir, PathBuf::from);
    std::fs::create_dir_all(&data_dir)?;
    let lock_path = data_dir.join("ffwd.lock");
    let lock_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;

    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        // SAFETY: `lock_file` is opened just above and remains alive for the duration of
        // this call, so `as_raw_fd()` yields a valid Unix file descriptor for `lock_path`.
        // `libc::flock` only operates on that fd using `LOCK_EX | LOCK_NB`; it does not rely
        // on any additional Rust aliasing invariants, and we immediately check
        // `io::Error::last_os_error()` if the syscall returns failure.
        let ret = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if ret != 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock || err.raw_os_error() == Some(libc::EAGAIN) {
                return Err(RuntimeError::Io(io::Error::other(format!(
                    "another ffwd instance is already running (lock: {})",
                    lock_path.display()
                ))));
            }
            return Err(RuntimeError::Io(err));
        }
    }

    #[cfg(not(unix))]
    eprintln!("warn: file-based instance locking not supported on this platform");

    Ok(Some(lock_file))
}

fn build_meter_provider(
    config: &ffwd_config::Config,
    use_color: bool,
) -> io::Result<opentelemetry_sdk::metrics::SdkMeterProvider> {
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    if let Some(ref endpoint) = config.server.metrics_endpoint {
        let interval_secs = config
            .server
            .metrics_interval_secs
            .map_or(60, ffwd_config::PositiveSecs::get);

        let otlp_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .build()
            .map_err(|e| io::Error::other(format!("OTLP metric exporter: {e}")))?;

        let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(otlp_exporter)
            .with_interval(Duration::from_secs(interval_secs))
            .build();

        eprintln!(
            "  {}metrics push{}: {endpoint} (every {interval_secs}s)",
            dim(use_color),
            reset(use_color),
        );

        Ok(SdkMeterProvider::builder().with_reader(reader).build())
    } else {
        Ok(SdkMeterProvider::builder().build())
    }
}

fn build_tracer_provider(
    buf: ffwd_diagnostics::span_exporter::SpanBuffer,
    config: &ffwd_config::Config,
    use_color: bool,
) -> io::Result<opentelemetry_sdk::trace::SdkTracerProvider> {
    use ffwd_diagnostics::span_exporter::RingBufferExporter;
    use opentelemetry_sdk::trace::{SdkTracerProvider, SimpleSpanProcessor};

    let ring_processor = SimpleSpanProcessor::new(RingBufferExporter::new(buf));
    let mut builder = SdkTracerProvider::builder().with_span_processor(ring_processor);

    if let Some(ref endpoint) = config.server.traces_endpoint {
        let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .build()
            .map_err(|e| io::Error::other(format!("OTLP trace exporter: {e}")))?;
        builder = builder.with_span_processor(
            opentelemetry_sdk::trace::BatchSpanProcessor::builder(otlp_exporter).build(),
        );
        eprintln!(
            "  {}traces push{}: {}",
            dim(use_color),
            reset(use_color),
            redact_url(endpoint)
        );
    }

    Ok(builder.build())
}

fn redact_url(url: &str) -> String {
    let (scheme, rest) = if let Some(i) = url.find("://") {
        (&url[..i], &url[i + 3..])
    } else {
        ("", url)
    };

    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let (has_userinfo, host) = authority
        .rsplit_once('@')
        .map_or((false, authority), |(_, host)| (true, host));
    if host.is_empty() {
        if has_userinfo {
            return if scheme.is_empty() {
                String::new()
            } else {
                format!("{scheme}://")
            };
        }
        return url.to_string();
    }
    if scheme.is_empty() {
        host.to_string()
    } else {
        format!("{scheme}://{host}")
    }
}

#[cfg(unix)]
fn jemalloc_stats() -> Option<ffwd_diagnostics::diagnostics::MemoryStats> {
    use tikv_jemalloc_ctl::{epoch, stats};

    epoch::mib().ok()?.advance().ok()?;

    let resident = stats::resident::mib().ok()?.read().ok()?;
    let allocated = stats::allocated::mib().ok()?.read().ok()?;
    let active = stats::active::mib().ok()?.read().ok()?;

    Some(ffwd_diagnostics::diagnostics::MemoryStats {
        resident,
        allocated,
        active,
    })
}

fn green(use_color: bool) -> &'static str {
    style(use_color, "32", "0")
}

fn yellow(use_color: bool) -> &'static str {
    style(use_color, "33", "0")
}

fn bold(use_color: bool) -> &'static str {
    if use_color { "\x1b[1m" } else { "" }
}

fn dim(use_color: bool) -> &'static str {
    if use_color { "\x1b[2m" } else { "" }
}

fn reset(use_color: bool) -> &'static str {
    if use_color { "\x1b[0m" } else { "" }
}

fn style(use_color: bool, color: &'static str, bold: &'static str) -> &'static str {
    if use_color {
        match (color, bold) {
            ("32", "0") => "\x1b[0;32m",
            ("33", "0") => "\x1b[0;33m",
            _ => "",
        }
    } else {
        ""
    }
}

#[cfg(test)]
mod tests {
    use super::redact_url;

    #[test]
    fn redact_url_handles_urls_without_scheme() {
        assert_eq!(
            redact_url("collector.local:4318/v1/traces"),
            "collector.local:4318"
        );
    }

    #[test]
    fn redact_url_uses_last_at_in_userinfo() {
        assert_eq!(
            redact_url("https://user:pa@ss@example.com/v1/traces"),
            "https://example.com"
        );
    }

    #[test]
    fn redact_url_keeps_original_when_host_is_empty() {
        assert_eq!(redact_url("https:///v1/traces"), "https:///v1/traces");
    }

    #[test]
    fn redact_url_does_not_treat_query_at_as_userinfo() {
        assert_eq!(
            redact_url("https://example.com/path?email=user@domain.com"),
            "https://example.com"
        );
    }

    #[test]
    fn redact_url_redacts_hostless_userinfo() {
        assert_eq!(redact_url("http://user:pass@/v1/traces"), "http://");
    }
}

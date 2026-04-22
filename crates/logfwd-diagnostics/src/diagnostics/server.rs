use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::get;
use tokio::sync::{broadcast, oneshot};

use super::metrics::PipelineMetrics;
use super::models::{
    BatchStatus, BottleneckStatus, ComponentHealthSnapshot, ComponentStatus, FileTransportStatus,
    LiveResponse, MemoryStats, MemoryStatsResponse, PipelineStatus, ReadyResponse,
    STABLE_DIAGNOSTICS_CONTRACT_VERSION, StageSeconds, StatusSnapshot, StatusSnapshotResponse,
    SystemStatus, TcpTransportStatus, TraceLifecycleState, TransformStatus, TransportStatus,
    UdpTransportStatus,
};
use super::policy;
use super::process::process_metrics;
use super::render::{esc, now_nanos};
use crate::background_http_task::BackgroundHttpTask;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DASHBOARD_HTML: &str = include_str!("../dashboard.html");
const REDACTED_SECRET: &str = "***redacted***";
const REDACTED_CONFIG_UNAVAILABLE: &str = "<redacted config unavailable>";
const REDACTED_ENDPOINT: &str = "<redacted_endpoint>";

fn lifecycle_state_for_active_batch(stage: &str, worker_id: Option<u64>) -> TraceLifecycleState {
    match stage {
        "scan" => TraceLifecycleState::ScanInProgress,
        "transform" => TraceLifecycleState::TransformInProgress,
        "output" if worker_id.is_some() => TraceLifecycleState::OutputInProgress,
        "queued" | "output" => TraceLifecycleState::QueuedForOutput,
        _ => TraceLifecycleState::QueuedForOutput,
    }
}

fn redact_endpoint_credentials(endpoint: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(endpoint) else {
        if endpoint.contains('@') {
            return REDACTED_ENDPOINT.to_string();
        }
        return endpoint.to_string();
    };
    if parsed.username().is_empty() && parsed.password().is_none() {
        return endpoint.to_string();
    }
    let _ = parsed.set_username("");
    let _ = parsed.set_password(None);
    parsed.to_string()
}

fn redact_yaml_value(value: &mut serde_yaml_ng::Value, in_auth_block: bool) {
    if in_auth_block {
        match value {
            serde_yaml_ng::Value::Mapping(map) => {
                for (_, val) in map.iter_mut() {
                    redact_yaml_value(val, true);
                }
            }
            serde_yaml_ng::Value::Sequence(seq) => {
                for item in seq.iter_mut() {
                    redact_yaml_value(item, true);
                }
            }
            serde_yaml_ng::Value::String(_)
            | serde_yaml_ng::Value::Number(_)
            | serde_yaml_ng::Value::Bool(_) => {
                *value = serde_yaml_ng::Value::String(REDACTED_SECRET.to_string());
            }
            serde_yaml_ng::Value::Null => {}
            serde_yaml_ng::Value::Tagged(tagged) => {
                redact_yaml_value(&mut tagged.value, true);
            }
        }
        return;
    }

    match value {
        serde_yaml_ng::Value::Mapping(map) => {
            for (key, val) in map.iter_mut() {
                let Some(key_str) = key.as_str() else {
                    redact_yaml_value(val, in_auth_block);
                    continue;
                };

                let next_in_auth = in_auth_block || key_str == "auth";

                if key_str == "endpoint"
                    && let serde_yaml_ng::Value::String(endpoint) = val
                {
                    *endpoint = redact_endpoint_credentials(endpoint);
                }

                redact_yaml_value(val, next_in_auth);
            }
        }
        serde_yaml_ng::Value::Sequence(seq) => {
            for item in seq.iter_mut() {
                redact_yaml_value(item, in_auth_block);
            }
        }
        _ => {}
    }
}

/// Redact credentials (auth tokens, bearer tokens, endpoint userinfo) from a
/// config YAML string. Returns the redacted YAML, or a placeholder if parsing
/// fails.
pub fn redact_config_yaml(raw_yaml: &str) -> String {
    let Ok(mut parsed) = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(raw_yaml) else {
        return REDACTED_CONFIG_UNAVAILABLE.to_string();
    };
    redact_yaml_value(&mut parsed, false);
    serde_yaml_ng::to_string(&parsed).unwrap_or_else(|_| REDACTED_CONFIG_UNAVAILABLE.to_string())
}

// ---------------------------------------------------------------------------
// DiagnosticsState — shared state for axum handlers
// ---------------------------------------------------------------------------

struct DiagnosticsState {
    pipelines: Vec<Arc<PipelineMetrics>>,
    start_time: Instant,
    memory_stats_fn: Option<fn() -> Option<MemoryStats>>,
    config_yaml: String,
    config_path: String,
    config_endpoint_enabled: bool,
    stderr: crate::stderr_capture::StderrCapture,
    history: Arc<crate::metric_history::MetricHistory>,
    trace_buf: Option<crate::span_exporter::SpanBuffer>,
    telemetry: crate::telemetry_buffer::TelemetryBuffers,
    telemetry_tx: broadcast::Sender<String>,
}

// ---------------------------------------------------------------------------
// ServerHandle — owns the background thread spawned by DiagnosticsServer::start
// ---------------------------------------------------------------------------

/// Owns the background thread spawned by [`DiagnosticsServer::start`].
///
/// Dropping this value signals the HTTP/sampler thread to exit via the oneshot
/// shutdown channel. Keep this value alive for as long as the server should run.
pub struct ServerHandle {
    http_task: BackgroundHttpTask,
}

impl ServerHandle {
    /// Signal shutdown and block until the HTTP worker exits.
    pub fn shutdown_and_join(&mut self) {
        self.http_task.shutdown_and_join();
    }
}

/// Lightweight diagnostics HTTP server. Runs on a dedicated thread, reads
/// atomic counters — no locking on the hot path.
pub struct DiagnosticsServer {
    pipelines: Vec<Arc<PipelineMetrics>>,
    start_time: Instant,
    bind_addr: String,
    /// Optional callback that returns a snapshot of allocator memory stats.
    /// Set this to expose jemalloc (or any allocator) metrics on `/admin/v1/status`.
    memory_stats_fn: Option<fn() -> Option<MemoryStats>>,
    /// Raw YAML config text for the /admin/v1/config endpoint.
    config_yaml: String,
    config_path: String,
    /// Whether `/admin/v1/config` is allowed to return the loaded config body.
    /// Disabled by default to avoid accidental secret exposure on diagnostics
    /// listeners bound outside localhost.
    config_endpoint_enabled: bool,
    /// Stderr capture for /admin/v1/logs; started when diagnostics starts.
    stderr: crate::stderr_capture::StderrCapture,
    /// Server-side metric history (1 hour, reducing precision).
    history: Arc<crate::metric_history::MetricHistory>,
    /// Ring buffer of recent batch spans for /admin/v1/traces.
    trace_buf: Option<crate::span_exporter::SpanBuffer>,
    /// OTLP JSON telemetry buffers for the dashboard.
    telemetry: crate::telemetry_buffer::TelemetryBuffers,
}

impl DiagnosticsServer {
    pub fn new(bind_addr: &str) -> Self {
        Self {
            pipelines: Vec::new(),
            start_time: Instant::now(),
            bind_addr: bind_addr.to_string(),
            memory_stats_fn: None,
            config_yaml: String::new(),
            config_path: String::new(),
            config_endpoint_enabled: false,
            stderr: crate::stderr_capture::StderrCapture::new(),
            history: Arc::new(crate::metric_history::MetricHistory::new()),
            trace_buf: None,
            telemetry: crate::telemetry_buffer::TelemetryBuffers::new(),
        }
    }

    /// Attach a span buffer so `/admin/v1/traces` can serve batch trace data.
    pub fn set_trace_buffer(&mut self, buf: crate::span_exporter::SpanBuffer) {
        self.trace_buf = Some(buf);
    }

    /// Store the raw config YAML and file path for the /admin/v1/config endpoint.
    pub fn set_config(&mut self, path: &str, yaml: &str) {
        self.config_path = path.to_string();
        self.config_yaml = yaml.to_string();
    }

    /// Enable or disable `/admin/v1/config`.
    ///
    /// This endpoint is disabled by default because configuration often
    /// contains credentials (tokens, passwords, API keys).
    pub fn set_config_endpoint_enabled(&mut self, enabled: bool) {
        self.config_endpoint_enabled = enabled;
    }

    pub fn add_pipeline(&mut self, metrics: Arc<PipelineMetrics>) {
        self.pipelines.push(metrics);
    }

    /// Register a callback that returns allocator memory statistics.
    ///
    /// When set, the `/admin/v1/status` endpoint includes a `memory` object in
    /// the `system` section with `resident`, `allocated`, and `active` fields
    /// (all in bytes).
    pub fn set_memory_stats_fn(&mut self, f: fn() -> Option<MemoryStats>) {
        self.memory_stats_fn = Some(f);
    }

    /// Spawn the server on a background thread. Binds synchronously before
    /// returning so that port-in-use errors are reported at startup.
    ///
    /// Returns `(handle, bound_addr)` on success.  `bound_addr` reflects the
    /// actual address after OS port assignment (useful when `bind_addr` uses
    /// port 0).  Returns an `io::Error` on bind failure.
    ///
    /// The returned [`ServerHandle`] owns the background thread.  Drop it
    /// (or let it go out of scope) to shut the server down cleanly.
    pub fn start(self) -> io::Result<(ServerHandle, std::net::SocketAddr)> {
        // Bind synchronously so errors are reported at startup.
        let std_listener = std::net::TcpListener::bind(&self.bind_addr)
            .map_err(|e| io::Error::other(format!("diagnostics bind {}: {e}", self.bind_addr)))?;
        let bound_addr = std_listener.local_addr()?;
        std_listener.set_nonblocking(true).map_err(|e| {
            io::Error::other(format!("diagnostics set_nonblocking {bound_addr}: {e}"))
        })?;

        // Start capturing stderr into the 1 MiB ring buffer immediately so
        // log lines emitted before the first /admin/v1/logs request are not lost.
        // Non-fatal: if capture setup fails (e.g. out of fds), log to real stderr.
        if let Err(e) = self.stderr.start() {
            tracing::warn!(error = %e, "stderr capture failed");
        }

        let (telemetry_tx, _) = broadcast::channel::<String>(16);

        let state = Arc::new(DiagnosticsState {
            pipelines: self.pipelines,
            start_time: self.start_time,
            memory_stats_fn: self.memory_stats_fn,
            config_yaml: self.config_yaml,
            config_path: self.config_path,
            config_endpoint_enabled: self.config_endpoint_enabled,
            stderr: self.stderr,
            history: self.history,
            trace_buf: self.trace_buf,
            telemetry: self.telemetry,
            telemetry_tx,
        });

        let app = axum::Router::new()
            .route("/", get(serve_dashboard))
            .route("/live", get(serve_live))
            .route("/ready", get(serve_ready))
            .route("/admin/v1/status", get(serve_status))
            .route("/admin/v1/stats", get(serve_stats))
            .route("/admin/v1/config", get(serve_config))
            .route("/admin/v1/logs", get(serve_logs))
            .route("/admin/v1/history", get(serve_history))
            .route("/admin/v1/traces", get(serve_traces))
            .route("/admin/v1/telemetry", get(ws_telemetry))
            .route("/admin/v1/telemetry/metrics", get(serve_telemetry_metrics))
            .route("/admin/v1/telemetry/traces", get(serve_telemetry_traces))
            .route("/admin/v1/telemetry/logs", get(serve_telemetry_logs))
            .fallback(|| async { (StatusCode::NOT_FOUND, "not found") })
            .with_state(Arc::clone(&state));

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (startup_tx, startup_rx) = std::sync::mpsc::sync_channel::<Result<(), String>>(1);

        let handle = std::thread::Builder::new()
            .name("diagnostics-http".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        tracing::error!(error = %e, "diagnostics runtime creation failed");
                        startup_tx
                            .send(Err(format!("diagnostics runtime: {e}")))
                            .ok();
                        return;
                    }
                };

                runtime.block_on(async move {
                    let listener = match tokio::net::TcpListener::from_std(std_listener) {
                        Ok(l) => l,
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                "diagnostics TcpListener conversion failed"
                            );
                            startup_tx
                                .send(Err(format!("diagnostics listener: {e}")))
                                .ok();
                            return;
                        }
                    };

                    // Spawn the metric sampler task.
                    tokio::spawn(sampler_loop(Arc::clone(&state)));

                    startup_tx.send(Ok(())).ok();

                    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
                        let _ = shutdown_rx.await;
                    });
                    if let Err(e) = server.await {
                        tracing::error!(error = %e, "diagnostics server error");
                    }
                });
            })
            .map_err(|e| io::Error::other(format!("failed to spawn diagnostics-http: {e}")))?;

        // Wait for the server to confirm it's ready.
        match startup_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(msg)) => return Err(io::Error::other(msg)),
            Err(_) => return Err(io::Error::other("diagnostics startup channel dropped")),
        }

        Ok((
            ServerHandle {
                http_task: BackgroundHttpTask::new(shutdown_tx, handle),
            },
            bound_addr,
        ))
    }
}
// ---------------------------------------------------------------------------
// Payload builders — extracted from DiagnosticsServer methods for reuse
// ---------------------------------------------------------------------------

fn live_payload(state: &DiagnosticsState) -> LiveResponse {
    LiveResponse {
        contract_version: STABLE_DIAGNOSTICS_CONTRACT_VERSION,
        status: "live",
        uptime_seconds: state.start_time.elapsed().as_secs(),
        version: VERSION,
    }
}

fn ready_payload(state: &DiagnosticsState) -> (ReadyResponse, bool) {
    let snapshot = policy::readiness_snapshot(&state.pipelines);
    let observed_at_unix_ns = now_nanos().to_string();
    let ready = snapshot.ready;
    let payload = ReadyResponse {
        contract_version: STABLE_DIAGNOSTICS_CONTRACT_VERSION,
        status: if ready { "ready" } else { "not_ready" },
        reason: snapshot.reason,
        observed_at_unix_ns,
    };
    (payload, ready)
}

fn status_payload(state: &DiagnosticsState) -> StatusSnapshotResponse {
    let uptime = state.start_time.elapsed().as_secs();
    let uptime_s = state.start_time.elapsed().as_secs_f64();
    let observed_at_unix_ns = now_nanos();
    let mut pipelines = Vec::new();

    for pm in &state.pipelines {
        let inputs: Vec<ComponentStatus> = pm
            .inputs
            .iter()
            .map(|(name, typ, stats)| {
                let transport = match typ.as_str() {
                    "file" => Some(TransportStatus {
                        file: Some(FileTransportStatus {
                            consecutive_error_polls: stats
                                .file_error_polls
                                .load(Ordering::Relaxed)
                                .into(),
                        }),
                        tcp: None,
                        udp: None,
                    }),
                    "tcp" => Some(TransportStatus {
                        file: None,
                        tcp: Some(TcpTransportStatus {
                            accepted_connections: stats.tcp_accepted.load(Ordering::Relaxed),
                            active_connections: stats.tcp_active.load(Ordering::Relaxed) as u64,
                        }),
                        udp: None,
                    }),
                    "udp" => Some(TransportStatus {
                        file: None,
                        tcp: None,
                        udp: Some(UdpTransportStatus {
                            drops_detected: stats.udp_drops.load(Ordering::Relaxed),
                            recv_buffer_size: stats.udp_recv_buf.load(Ordering::Relaxed) as u64,
                        }),
                    }),
                    _ => None,
                };

                ComponentStatus {
                    name: name.clone(),
                    component_type: typ.clone(),
                    health: stats.health().as_str().to_string(),
                    lines_total: stats.lines(),
                    bytes_total: stats.bytes(),
                    errors: stats.errors(),
                    rotations: Some(stats.rotations()),
                    parse_errors: Some(stats.parse_errors()),
                    transport,
                    send_ns_total: None,
                    send_count: None,
                }
            })
            .collect();

        let lines_in = pm.transform_in.lines();
        // lines_out: derived from output-sink stats (single increment path —
        // each sink calls inc_lines once on successful delivery). For fan-out
        // pipelines, the maximum across all outputs is used as a proxy for
        // "lines delivered to the most successful output". This may undercount
        // in partial-failure fan-out scenarios where outputs succeed at
        // different rates.
        let lines_out: u64 = pm
            .outputs
            .iter()
            .map(|(_, _, s)| s.lines())
            .max()
            .unwrap_or(0);
        let drop_rate = if lines_in > 0 {
            (1.0 - (lines_out as f64 / lines_in as f64)).clamp(0.0, 1.0)
        } else {
            0.0
        };

        let outputs: Vec<ComponentStatus> = pm
            .outputs
            .iter()
            .map(|(name, typ, stats)| ComponentStatus {
                name: name.clone(),
                component_type: typ.clone(),
                health: stats.health().as_str().to_string(),
                lines_total: stats.lines(),
                bytes_total: stats.bytes(),
                errors: stats.errors(),
                rotations: None,
                parse_errors: None,
                transport: None,
                send_ns_total: Some(stats.send_ns_total()),
                send_count: Some(stats.send_count()),
            })
            .collect();

        let batches = pm.batches_total.load(Ordering::Relaxed);
        let batch_rows = pm.batch_rows_total.load(Ordering::Relaxed);
        let avg_rows = if batches > 0 {
            batch_rows as f64 / batches as f64
        } else {
            0.0
        };
        let scan_s = pm.scan_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;
        let transform_s = pm.transform_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;
        let output_s = pm.output_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;
        let queue_wait_s = pm.queue_wait_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;
        let send_s = pm.send_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;

        let last_batch_ns = pm.last_batch_time_ns.load(Ordering::Relaxed);

        // Compute batch latency using a consistent snapshot since they are
        // updated at different times. We retry until batches remains the same,
        // capping at 64 attempts to avoid spinning indefinitely under contention.
        // Observability counters only — stale reads are acceptable.
        // Use Relaxed uniformly to match all other load sites in this file.
        let mut latency_batches = pm.batches_total.load(Ordering::Relaxed);
        let mut batch_latency_total;
        let mut attempts = 0;
        loop {
            batch_latency_total = pm.batch_latency_nanos_total.load(Ordering::Relaxed);
            let current_batches = pm.batches_total.load(Ordering::Relaxed);
            if current_batches == latency_batches || attempts >= 64 {
                latency_batches = current_batches;
                break;
            }
            latency_batches = current_batches;
            attempts += 1;
        }

        let batch_latency_avg_ns = batch_latency_total
            .checked_div(latency_batches)
            .unwrap_or(0);
        let inflight = pm.inflight_batches.load(Ordering::Relaxed);
        let backpressure = pm.backpressure_stalls.load(Ordering::Relaxed);
        let transform_health = policy::transform_health(pm);

        // Compute bottleneck classification.
        // Ratios are cumulative worker-seconds divided by wall-clock uptime, so
        // they can exceed 1.0 when multiple workers run in parallel (e.g. 4
        // workers each spending 100% of wall-time → ratio = 4.0). We express
        // the result in "worker-seconds per wall-second" to avoid the misleading
        // "% of uptime" framing.
        let uptime_s_nonzero = uptime_s.max(1e-9);
        let queue_wait_ratio = queue_wait_s / uptime_s_nonzero;
        let transform_ratio = transform_s / uptime_s_nonzero;
        let scan_ratio = scan_s / uptime_s_nonzero;
        let stalls_per_sec = backpressure as f64 / uptime_s_nonzero;

        let (bottleneck_stage, bottleneck_reason): (&str, String) = if queue_wait_ratio > 0.5 {
            (
                "output",
                format!(
                    "workers spending {:.0}% of wall-time in output queue",
                    queue_wait_ratio * 100.0
                ),
            )
        } else if stalls_per_sec > 10.0 {
            (
                "input",
                format!(
                    "backpressure stalls at {stalls_per_sec:.1}/sec — input faster than pipeline can drain"
                ),
            )
        } else if transform_ratio > 0.3 {
            (
                "transform",
                format!(
                    "transform consuming {:.0}% of wall-time across workers",
                    transform_ratio * 100.0
                ),
            )
        } else if scan_ratio > 0.3 {
            (
                "scan",
                format!(
                    "scan consuming {:.0}% of wall-time across workers",
                    scan_ratio * 100.0
                ),
            )
        } else {
            ("none", "running well within capacity".to_string())
        };

        pipelines.push(PipelineStatus {
            name: pm.name.clone(),
            inputs,
            transform: TransformStatus {
                sql: pm.transform_sql.clone(),
                health: transform_health.as_str().to_string(),
                lines_in,
                lines_out,
                errors: pm.transform_errors.load(Ordering::Relaxed),
                filter_drop_rate: drop_rate,
            },
            outputs,
            batches: BatchStatus {
                total: batches,
                avg_rows,
                flush_by_size: pm.flush_by_size.load(Ordering::Relaxed),
                flush_by_timeout: pm.flush_by_timeout.load(Ordering::Relaxed),
                cadence_fast_repolls: pm.cadence_fast_repolls.load(Ordering::Relaxed),
                cadence_idle_sleeps: pm.cadence_idle_sleeps.load(Ordering::Relaxed),
                dropped_batches_total: pm.dropped_batches_total.load(Ordering::Relaxed),
                scan_errors_total: pm.scan_errors_total.load(Ordering::Relaxed),
                parse_errors_total: pm.parse_errors_total.load(Ordering::Relaxed),
                last_batch_time_ns: last_batch_ns.to_string(),
                batch_latency_avg_ns,
                inflight,
                channel_depth: pm.channel_depth.load(Ordering::Relaxed),
                channel_capacity: pm.channel_capacity.load(Ordering::Relaxed),
                rows_total: batch_rows,
            },
            stage_seconds: StageSeconds {
                scan: scan_s,
                transform: transform_s,
                output: output_s,
                queue_wait: queue_wait_s,
                send: send_s,
            },
            backpressure_stalls: backpressure,
            bottleneck: BottleneckStatus {
                stage: bottleneck_stage.to_string(),
                reason: bottleneck_reason,
            },
        });
    }

    let ready_snapshot = policy::readiness_snapshot(&state.pipelines);
    let ready = if ready_snapshot.ready {
        "ready"
    } else {
        "not_ready"
    };
    let component_health = ready_snapshot.component_health;
    let ready_reason = ready_snapshot.reason;
    let component_reason = policy::health_reason(component_health);
    let readiness_impact = policy::readiness_impact(component_health);

    StatusSnapshotResponse {
        contract_version: STABLE_DIAGNOSTICS_CONTRACT_VERSION,
        live: StatusSnapshot {
            status: "live".to_string(),
            reason: "process_running".to_string(),
            observed_at_unix_ns: observed_at_unix_ns.to_string(),
        },
        ready: StatusSnapshot {
            status: ready.to_string(),
            reason: ready_reason.to_string(),
            observed_at_unix_ns: observed_at_unix_ns.to_string(),
        },
        component_health: ComponentHealthSnapshot {
            status: component_health.as_str().to_string(),
            reason: component_reason.to_string(),
            readiness_impact: readiness_impact.to_string(),
            observed_at_unix_ns: observed_at_unix_ns.to_string(),
        },
        pipelines,
        system: SystemStatus {
            uptime_seconds: uptime,
            version: VERSION,
            memory: state
                .memory_stats_fn
                .and_then(|f| f())
                .map(|m| MemoryStatsResponse {
                    resident: m.resident,
                    allocated: m.allocated,
                    active: m.active,
                }),
        },
    }
}

fn build_stats_body(state: &DiagnosticsState) -> String {
    let uptime_s = state.start_time.elapsed().as_secs_f64();
    let process_json = match process_metrics() {
        Some((rss_bytes, cpu_user_ms, cpu_sys_ms)) => format!(
            r#","rss_bytes":{rss_bytes},"cpu_user_ms":{cpu_user_ms},"cpu_sys_ms":{cpu_sys_ms}"#
        ),
        None => String::from(r#","rss_bytes":null,"cpu_user_ms":null,"cpu_sys_ms":null"#),
    };

    let mut total_input_lines: u64 = 0;
    let mut total_input_bytes: u64 = 0;
    let mut total_output_lines: u64 = 0;
    let mut total_output_bytes: u64 = 0;
    let mut total_output_errors: u64 = 0;
    let mut total_batches: u64 = 0;
    let mut total_scan_ns: u64 = 0;
    let mut total_transform_ns: u64 = 0;
    let mut total_output_ns: u64 = 0;
    let mut total_backpressure: u64 = 0;
    let mut total_inflight: u64 = 0;
    let mut channel_depth: u64 = 0;
    let mut channel_capacity: u64 = 0;

    for pm in &state.pipelines {
        for (_, _, stats) in &pm.inputs {
            total_input_lines += stats.lines();
            total_input_bytes += stats.bytes();
        }
        for (_, _, stats) in &pm.outputs {
            total_output_lines += stats.lines();
            total_output_bytes += stats.bytes();
            total_output_errors += stats.errors();
        }
        total_batches += pm.batches_total.load(Ordering::Relaxed);
        total_scan_ns += pm.scan_nanos_total.load(Ordering::Relaxed);
        total_transform_ns += pm.transform_nanos_total.load(Ordering::Relaxed);
        total_output_ns += pm.output_nanos_total.load(Ordering::Relaxed);
        total_backpressure += pm.backpressure_stalls.load(Ordering::Relaxed);
        total_inflight += pm.inflight_batches.load(Ordering::Relaxed);
        channel_depth += pm.channel_depth.load(Ordering::Relaxed);
        channel_capacity += pm.channel_capacity.load(Ordering::Relaxed);
    }

    let mem_json = match state.memory_stats_fn.and_then(|f| f()) {
        Some(m) => format!(
            r#","mem_resident":{},"mem_allocated":{},"mem_active":{}"#,
            m.resident, m.allocated, m.active,
        ),
        None => String::new(),
    };

    format!(
        r#"{{"uptime_sec":{:.3}{},"input_lines":{},"input_bytes":{},"output_lines":{},"output_bytes":{},"output_errors":{},"batches":{},"scan_sec":{:.6},"transform_sec":{:.6},"output_sec":{:.6},"backpressure_stalls":{},"inflight_batches":{},"channel_depth":{},"channel_capacity":{}{}}}"#,
        uptime_s,
        process_json,
        total_input_lines,
        total_input_bytes,
        total_output_lines,
        total_output_bytes,
        total_output_errors,
        total_batches,
        total_scan_ns as f64 / 1e9,
        total_transform_ns as f64 / 1e9,
        total_output_ns as f64 / 1e9,
        total_backpressure,
        total_inflight,
        channel_depth,
        channel_capacity,
        mem_json,
    )
}

fn build_traces_body(state: &DiagnosticsState) -> String {
    use crate::span_exporter::TraceSpan;
    use std::collections::HashMap;
    use std::fmt::Write;

    if let Some(ref buf) = state.trace_buf {
        let all_spans = buf.get_spans();

        let mut roots: Vec<&TraceSpan> = Vec::new();
        let mut children: HashMap<&str, Vec<&TraceSpan>> = HashMap::new();
        let root_marker = "0000000000000000";

        for span in &all_spans {
            if span.parent_id == root_marker {
                roots.push(span);
            } else {
                children.entry(&span.trace_id).or_default().push(span);
            }
        }

        let mut out = String::with_capacity(64 * 1024);
        out.push_str(r#"{"traces":["#);
        let mut first = true;
        for root in roots.iter().rev().take(500) {
            if !first {
                out.push(',');
            }
            first = false;

            let mut scan_ns = 0u64;
            let mut scan_rows = 0u64;
            let mut transform_ns = 0u64;
            let mut output_ns = 0u64;
            let mut output_start_unix_ns = 0u64;
            let mut worker_id: i64 = -1;
            let mut send_ns = 0u64;
            let mut recv_ns = 0u64;
            let mut took_ms = 0u64;
            let mut retries = 0u64;
            let mut req_bytes = 0u64;
            let mut cmp_bytes = 0u64;
            let mut resp_bytes = 0u64;
            if let Some(kids) = children.get(root.trace_id.as_str()) {
                for kid in kids {
                    let kid_attr = |key: &str| -> u64 {
                        kid.attrs
                            .iter()
                            .find(|kv| kv[0] == key)
                            .and_then(|kv| kv[1].parse().ok())
                            .unwrap_or(0)
                    };
                    match kid.name.as_str() {
                        "scan" => {
                            scan_ns = kid.duration_ns;
                            scan_rows = kid_attr("rows");
                        }
                        "transform" => transform_ns = kid.duration_ns,
                        "output" => {
                            output_ns = kid.duration_ns;
                            output_start_unix_ns = kid.start_unix_ns;
                            worker_id = kid
                                .attrs
                                .iter()
                                .find(|kv| kv[0] == "worker_id")
                                .and_then(|kv| kv[1].parse().ok())
                                .unwrap_or(-1);
                            send_ns = kid_attr("send_ns");
                            recv_ns = kid_attr("recv_ns");
                            took_ms = kid_attr("took_ms");
                            retries = kid_attr("retries");
                            req_bytes = kid_attr("req_bytes");
                            cmp_bytes = kid_attr("cmp_bytes");
                            resp_bytes = kid_attr("resp_bytes");
                        }
                        _ => {}
                    }
                }
            }

            let root_attr_u64 = |key: &str| -> u64 {
                root.attrs
                    .iter()
                    .find(|kv| kv[0] == key)
                    .and_then(|kv| kv[1].parse().ok())
                    .unwrap_or(0)
            };
            if scan_ns == 0 {
                scan_ns = root_attr_u64("scan_ns");
            }
            if transform_ns == 0 {
                transform_ns = root_attr_u64("transform_ns");
            }

            let attr = |key: &str| -> &str {
                root.attrs
                    .iter()
                    .find(|kv| kv[0] == key)
                    .map_or("", |kv| kv[1].as_str())
            };
            let pipeline = attr("pipeline");
            let bytes_in: u64 = attr("bytes_in").parse().unwrap_or(0);
            let flush_reason = attr("flush_reason");
            let queue_wait_ns: u64 = attr("queue_wait_ns").parse().unwrap_or(0);
            let input_rows: u64 = attr("input_rows").parse().unwrap_or(0);
            let output_rows: u64 = attr("output_rows").parse().unwrap_or(0);
            let errors: u64 = attr("errors").parse().unwrap_or(0);

            let _ = write!(
                out,
                "{{\
                    \"trace_id\":\"{tid}\",\
                    \"pipeline\":\"{pl}\",\
                    \"start_unix_ns\":\"{st}\",\
                    \"total_ns\":\"{tot}\",\
                    \"scan_ns\":\"{scan}\",\
                    \"transform_ns\":\"{xfm}\",\
                    \"output_ns\":\"{out_ns}\",\
                    \"output_start_unix_ns\":\"{out_st}\",\
                    \"scan_rows\":{sr},\
                    \"input_rows\":{ir},\
                    \"output_rows\":{or},\
                    \"bytes_in\":{bi},\
                    \"queue_wait_ns\":\"{qw}\",\
                    \"worker_id\":{wid},\
                    \"send_ns\":\"{snd}\",\
                    \"recv_ns\":\"{rcv}\",\
                    \"took_ms\":{tk},\
                    \"retries\":{ret},\
                    \"req_bytes\":{rb},\
                    \"cmp_bytes\":{cb},\
                    \"resp_bytes\":{rspb},\
                    \"flush_reason\":\"{fr}\",\
                    \"errors\":{err},\
                    \"status\":\"{status}\",\
                    \"lifecycle_state\":\"{lifecycle_state}\"\
                }}",
                tid = root.trace_id,
                pl = esc(pipeline),
                st = root.start_unix_ns,
                tot = root.duration_ns,
                scan = scan_ns,
                xfm = transform_ns,
                out_ns = output_ns,
                out_st = output_start_unix_ns,
                sr = scan_rows,
                ir = input_rows,
                or = output_rows,
                bi = bytes_in,
                qw = queue_wait_ns,
                wid = worker_id,
                snd = send_ns,
                rcv = recv_ns,
                tk = took_ms,
                ret = retries,
                rb = req_bytes,
                cb = cmp_bytes,
                rspb = resp_bytes,
                fr = esc(flush_reason),
                err = errors,
                status = root.status,
                lifecycle_state = TraceLifecycleState::Completed.as_str(),
            );
        }
        // In-progress batches — live entries shown before completion.
        for pm in &state.pipelines {
            let active = pm
                .active_batches
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for (id, b) in active.iter() {
                if !first {
                    out.push(',');
                }
                first = false;
                let worker_id_json = b
                    .worker_id
                    .map_or_else(|| "null".to_string(), |wid| wid.to_string());
                let _ = write!(
                    out,
                    "{{\
                            \"trace_id\":\"live-{id}\",\
                            \"pipeline\":\"{pl}\",\
                            \"start_unix_ns\":\"{st}\",\
                            \"total_ns\":\"0\",\
                            \"scan_ns\":\"{scan}\",\
                            \"transform_ns\":\"{xfm}\",\
                            \"output_ns\":\"0\",\
                            \"output_start_unix_ns\":\"{out_st}\",\
                            \"scan_rows\":0,\
                            \"input_rows\":0,\
                            \"output_rows\":0,\
                            \"bytes_in\":0,\
                            \"queue_wait_ns\":\"0\",\
                            \"worker_id\":{wid},\
                            \"send_ns\":\"0\",\
                            \"recv_ns\":\"0\",\
                            \"took_ms\":0,\
                            \"retries\":0,\
                            \"req_bytes\":0,\
                            \"cmp_bytes\":0,\
                            \"resp_bytes\":0,\
                            \"flush_reason\":\"\",\
                            \"errors\":0,\
                            \"status\":\"unset\",\
                            \"lifecycle_state\":\"{lifecycle_state}\",\
                            \"lifecycle_state_start_unix_ns\":\"{state_start}\"\
                        }}",
                    id = id,
                    pl = esc(&pm.name),
                    st = b.start_unix_ns,
                    scan = b.scan_ns,
                    xfm = b.transform_ns,
                    out_st = b.output_start_unix_ns,
                    wid = worker_id_json,
                    lifecycle_state =
                        lifecycle_state_for_active_batch(b.stage, b.worker_id).as_str(),
                    state_start = b.stage_start_unix_ns,
                );
            }
        }
        out.push_str("]}");
        out
    } else {
        r#"{"traces":[]}"#.to_string()
    }
}

fn build_logs_body(state: &DiagnosticsState) -> String {
    let lines = state.stderr.get_logs();
    let mut body = String::with_capacity(lines.len() * 80 + 32);
    body.push_str(r#"{"lines":["#);
    for (i, line) in lines.iter().enumerate() {
        if i > 0 {
            body.push(',');
        }
        body.push('"');
        body.push_str(&esc(line));
        body.push('"');
    }
    body.push_str(r#"],"capturing":"#);
    body.push_str(if state.stderr.is_active() {
        "true"
    } else {
        "false"
    });
    body.push('}');
    body
}

// ---------------------------------------------------------------------------
// Axum endpoint handlers
// ---------------------------------------------------------------------------

async fn serve_dashboard() -> impl IntoResponse {
    Html(DASHBOARD_HTML)
}

async fn serve_live(State(state): State<Arc<DiagnosticsState>>) -> impl IntoResponse {
    Json(live_payload(&state))
}

async fn serve_ready(State(state): State<Arc<DiagnosticsState>>) -> impl IntoResponse {
    let (payload, ready) = ready_payload(&state);
    if ready {
        (StatusCode::OK, Json(payload))
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(payload))
    }
}

async fn serve_status(State(state): State<Arc<DiagnosticsState>>) -> impl IntoResponse {
    Json(status_payload(&state))
}

async fn serve_stats(State(state): State<Arc<DiagnosticsState>>) -> impl IntoResponse {
    let body = build_stats_body(&state);
    ([("content-type", "application/json")], body)
}

async fn serve_config(State(state): State<Arc<DiagnosticsState>>) -> impl IntoResponse {
    if !state.config_endpoint_enabled {
        let body = r#"{"error":"config_endpoint_disabled","message":"set LOGFWD_UNSAFE_EXPOSE_CONFIG=1 to enable /admin/v1/config"}"#;
        return (
            StatusCode::FORBIDDEN,
            [("content-type", "application/json")],
            body.to_string(),
        );
    }

    let redacted_yaml = redact_config_yaml(&state.config_yaml);
    let body = format!(
        r#"{{"path":"{}","raw_yaml":"{}"}}"#,
        esc(&state.config_path),
        esc(&redacted_yaml),
    );
    (StatusCode::OK, [("content-type", "application/json")], body)
}

async fn serve_history(State(state): State<Arc<DiagnosticsState>>) -> impl IntoResponse {
    let body = state.history.to_json();
    ([("content-type", "application/json")], body)
}

async fn serve_logs(State(state): State<Arc<DiagnosticsState>>) -> impl IntoResponse {
    let body = build_logs_body(&state);
    ([("content-type", "application/json")], body)
}

async fn serve_traces(State(state): State<Arc<DiagnosticsState>>) -> impl IntoResponse {
    let body = build_traces_body(&state);
    ([("content-type", "application/json")], body)
}

async fn serve_telemetry_metrics(State(state): State<Arc<DiagnosticsState>>) -> impl IntoResponse {
    let points = state.telemetry.metrics.snapshot();
    let body = crate::telemetry_buffer::metrics_to_otlp_json(&points);
    ([("content-type", "application/json")], body)
}

async fn serve_telemetry_traces(State(state): State<Arc<DiagnosticsState>>) -> impl IntoResponse {
    let spans =
        crate::telemetry_buffer::collect_all_spans(state.trace_buf.as_ref(), &state.pipelines);
    let body = crate::telemetry_buffer::traces_to_otlp_json(&spans);
    ([("content-type", "application/json")], body)
}

async fn serve_telemetry_logs(State(state): State<Arc<DiagnosticsState>>) -> impl IntoResponse {
    let logs = state.telemetry.logs.snapshot();
    let body = crate::telemetry_buffer::logs_to_otlp_json(&logs);
    ([("content-type", "application/json")], body)
}

// ---------------------------------------------------------------------------
// WebSocket telemetry push
// ---------------------------------------------------------------------------

async fn ws_telemetry(
    ws: WebSocketUpgrade,
    State(state): State<Arc<DiagnosticsState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_ws(socket, state))
}

async fn handle_ws(mut socket: WebSocket, state: Arc<DiagnosticsState>) {
    let mut rx = state.telemetry_tx.subscribe();
    loop {
        match rx.recv().await {
            Ok(text) => {
                if socket.send(Message::Text(text.into())).await.is_err() {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {} // skip missed
            Err(_) => break,
        }
    }
}

// ---------------------------------------------------------------------------
// Background sampler loop (runs as a tokio task inside the HTTP thread)
// ---------------------------------------------------------------------------

async fn sampler_loop(state: Arc<DiagnosticsState>) {
    let mut last_stderr_cursor: u64 = 0;
    let mut ws_last_log_cursor: u64 = 0;
    let mut ws_last_span_count: usize = 0;
    let mut prev_health = std::collections::HashMap::new();
    let mut prev_snapshot: Option<super::telemetry::MetricSnapshot> = None;
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Always sample metrics + logs into the history/telemetry buffers.
        sample_metrics(&state.pipelines, &state.history, state.memory_stats_fn);

        let uptime = state.start_time.elapsed().as_secs_f64();
        crate::telemetry_buffer::sample_all_metrics(
            &state.pipelines,
            state.memory_stats_fn,
            uptime,
            &state.telemetry,
        );
        crate::telemetry_buffer::sample_stderr_logs(
            &state.stderr,
            &state.telemetry.logs,
            &mut last_stderr_cursor,
        );
        crate::telemetry_buffer::sample_health_transitions(
            &state.pipelines,
            &state.telemetry.logs,
            &mut prev_health,
        );

        // Push OTLP JSON telemetry to WebSocket clients.
        // Three separate messages: resourceMetrics, resourceSpans, resourceLogs.
        if state.telemetry_tx.receiver_count() > 0 {
            let mut metrics = super::telemetry::sample_metrics(
                &state.pipelines,
                state.memory_stats_fn,
                state.start_time.elapsed(),
            );

            // Append pre-computed rate gauges (counter diffs → dashboard-ready rates).
            if let Some(prev) = &prev_snapshot {
                let rate_gauges = super::telemetry::compute_rate_gauges(&metrics, prev);
                metrics.extend(rate_gauges);
            }

            let _ = state
                .telemetry_tx
                .send(super::telemetry::metrics_to_otlp_json(&metrics));

            // Store current snapshot for next tick's rate computation.
            // Re-sample (cheap) to get a clean snapshot without the rate gauges mixed in.
            prev_snapshot = Some(super::telemetry::MetricSnapshot {
                points: super::telemetry::sample_metrics(
                    &state.pipelines,
                    state.memory_stats_fn,
                    state.start_time.elapsed(),
                ),
                time: Instant::now(),
            });

            let spans = super::telemetry::collect_new_spans(
                state.trace_buf.as_ref(),
                &state.pipelines,
                &mut ws_last_span_count,
            );
            // Always send traces — even empty payloads — so clients can
            // clear stale in-progress state when all batches complete.
            let _ = state
                .telemetry_tx
                .send(super::telemetry::spans_to_otlp_json(&spans));

            let logs = super::telemetry::collect_new_logs(&state.stderr, &mut ws_last_log_cursor);
            if !logs.is_empty() {
                let _ = state
                    .telemetry_tx
                    .send(super::telemetry::logs_to_otlp_json(&logs));
            }
        }
    }
}

/// Sample all pipeline + process metrics into the history buffer.
/// Called every 2s by the background sampler thread.
fn sample_metrics(
    pipelines: &[Arc<PipelineMetrics>],
    history: &crate::metric_history::MetricHistory,
    memory_fn: Option<fn() -> Option<MemoryStats>>,
) {
    let mut input_lines: u64 = 0;
    let mut input_bytes: u64 = 0;
    let mut output_lines: u64 = 0;
    let mut output_bytes: u64 = 0;
    let mut output_errors: u64 = 0;
    let mut scan_ns: u64 = 0;
    let mut transform_ns: u64 = 0;
    let mut output_ns: u64 = 0;
    let mut batches: u64 = 0;
    let mut inflight_batches: u64 = 0;
    let mut backpressure_stalls: u64 = 0;

    for pm in pipelines {
        for (_, _, s) in &pm.inputs {
            input_lines += s.lines();
            input_bytes += s.bytes();
        }
        for (_, _, s) in &pm.outputs {
            output_lines += s.lines();
            output_bytes += s.bytes();
            output_errors += s.errors();
        }
        scan_ns += pm.scan_nanos_total.load(Ordering::Relaxed);
        transform_ns += pm.transform_nanos_total.load(Ordering::Relaxed);
        output_ns += pm.output_nanos_total.load(Ordering::Relaxed);
        batches += pm.batches_total.load(Ordering::Relaxed);
        inflight_batches += pm.inflight_batches.load(Ordering::Relaxed);
        backpressure_stalls += pm.backpressure_stalls.load(Ordering::Relaxed);
    }

    history.record("input_lines", input_lines as f64);
    history.record("input_bytes", input_bytes as f64);
    history.record("output_lines", output_lines as f64);
    history.record("output_bytes", output_bytes as f64);
    history.record("output_errors", output_errors as f64);
    history.record("scan_sec", scan_ns as f64 / 1e9);
    history.record("transform_sec", transform_ns as f64 / 1e9);
    history.record("output_sec", output_ns as f64 / 1e9);
    history.record("batches", batches as f64);
    history.record("inflight_batches", inflight_batches as f64);
    history.record("backpressure_stalls", backpressure_stalls as f64);

    if let Some((rss, cpu_user, cpu_sys)) = process_metrics() {
        history.record("rss_bytes", rss as f64);
        history.record("cpu_user_ms", cpu_user as f64);
        history.record("cpu_sys_ms", cpu_sys as f64);
    }

    if let Some(m) = memory_fn.and_then(|f| f()) {
        history.record("mem_allocated", m.allocated as f64);
        history.record("mem_resident", m.resident as f64);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::process::{get_page_size, get_ticks_per_sec, parse_proc_stat};
    use crate::diagnostics::{ComponentHealth, ComponentStats};
    use std::io::Read;
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Instant;

    /// Build a server with one pipeline pre-populated with known counter values.
    /// Binds to port 0 so the OS assigns a free port; call `.start()` and use
    /// the returned `SocketAddr` to find the actual port.
    fn server_with_test_pipeline() -> DiagnosticsServer {
        let meter = opentelemetry::global::meter("test");
        let mut pm = PipelineMetrics::new(
            "default",
            "SELECT * FROM logs WHERE level != 'DEBUG'",
            &meter,
        );

        let inp = pm.add_input("pod_logs", "file");
        inp.set_health(ComponentHealth::Healthy);
        inp.inc_lines(1000);
        inp.inc_bytes(50000);
        inp.inc_rotations();

        pm.transform_in.inc_lines(1000);
        // The status endpoint derives lines_out from output-sink stats, so the
        // fixture only needs output lines populated here.

        let out = pm.add_output("collector", "otlp");
        out.inc_lines(900);
        out.inc_bytes(30000);
        out.inc_errors();
        out.inc_errors();

        // Batch-level metrics.
        pm.batches_total.store(50, Ordering::Relaxed);
        pm.batch_rows_total.store(4500, Ordering::Relaxed);
        pm.flush_by_size.store(30, Ordering::Relaxed);
        pm.flush_by_timeout.store(20, Ordering::Relaxed);
        pm.cadence_fast_repolls.store(11, Ordering::Relaxed);
        pm.cadence_idle_sleeps.store(44, Ordering::Relaxed);
        pm.dropped_batches_total.store(5, Ordering::Relaxed);
        pm.scan_errors_total.store(2, Ordering::Relaxed);
        pm.parse_errors_total.store(4, Ordering::Relaxed);
        pm.scan_nanos_total.store(100_000_000, Ordering::Relaxed); // 0.1s
        pm.transform_nanos_total
            .store(500_000_000, Ordering::Relaxed); // 0.5s
        pm.output_nanos_total.store(200_000_000, Ordering::Relaxed); // 0.2s
        pm.queue_wait_nanos_total
            .store(50_000_000, Ordering::Relaxed); // 0.05s
        pm.send_nanos_total.store(150_000_000, Ordering::Relaxed); // 0.15s
        pm.batch_latency_nanos_total
            .store(500_000_000, Ordering::Relaxed); // avg = 500_000_000/50 = 10_000_000
        pm.inflight_batches.store(3, Ordering::Relaxed);
        pm.backpressure_stalls.store(7, Ordering::Relaxed);
        pm.transform_errors.store(3, Ordering::Relaxed);

        let mut server = DiagnosticsServer::new("127.0.0.1:0");
        server.add_pipeline(Arc::new(pm));
        server
    }

    fn server_with_single_input_health(health: ComponentHealth) -> DiagnosticsServer {
        let meter = opentelemetry::global::meter("test");
        let mut pm = PipelineMetrics::new("default", "SELECT * FROM logs", &meter);
        let input = pm.add_input("receiver", "tcp");
        input.set_health(health);

        let mut server = DiagnosticsServer::new("127.0.0.1:0");
        server.add_pipeline(Arc::new(pm));
        server
    }

    /// Simple HTTP GET helper using raw TCP. Retries connection up to 20
    /// times with 50ms backoff to handle server startup race on macOS.
    fn http_get(port: u16, path: &str) -> (u16, String) {
        use std::io::Write;
        use std::net::TcpStream;

        let addr = format!("127.0.0.1:{port}");
        let mut stream = None;
        for _ in 0..20 {
            match TcpStream::connect(&addr) {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(_) => thread::sleep(std::time::Duration::from_millis(50)),
            }
        }
        let mut stream = stream.unwrap_or_else(|| {
            panic!("connect failed after retries to {addr}");
        });
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .ok();
        let req = format!(
            "GET {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            path
        );
        stream.write_all(req.as_bytes()).unwrap();

        let mut buf = Vec::new();
        let _ = stream.read_to_end(&mut buf);
        let text = String::from_utf8_lossy(&buf).to_string();

        // Parse status code from first line.
        let status = text
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(0);

        // Split headers from body.
        let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();

        (status, body)
    }

    fn wait_until<F>(timeout: std::time::Duration, mut predicate: F, failure_message: &str)
    where
        F: FnMut() -> bool,
    {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if predicate() {
                return;
            }
            thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(predicate(), "{failure_message}");
    }

    #[test]
    fn redact_config_yaml_masks_auth_and_endpoint_credentials() {
        let raw = r#"
output:
  type: http
  endpoint: "https://user:pass@example.com/ingest"
  auth:
    bearer_token: "super-secret"
    headers:
      X-Api-Key: "api-secret"
      Authorization: "Basic abc123"
    some_new_auth_block:
      nested:
        secret_key: "nested-secret"
        flag: true
        number: 42
        tagged_secret: !!str "tagged-secret-value"
    aws:
      access_key_id: "AKIAIOSFODNN7EXAMPLE"
      secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
"#;
        let redacted = redact_config_yaml(raw);
        assert!(
            !redacted.contains("super-secret"),
            "bearer token must not be exposed"
        );
        assert!(
            !redacted.contains("api-secret"),
            "auth header values must not be exposed"
        );
        assert!(
            !redacted.contains("abc123"),
            "authorization header value must not be exposed"
        );
        assert!(
            !redacted.contains("nested-secret"),
            "nested auth fields must not be exposed"
        );
        assert!(
            !redacted.contains("AKIAIOSFODNN7EXAMPLE"),
            "aws access key must not be exposed"
        );
        assert!(
            !redacted.contains("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
            "aws secret key must not be exposed"
        );
        // Verify specific auth field values are redacted (not broad token absence,
        // which would be brittle if unrelated YAML happened to contain those tokens).
        let redacted_yaml: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(&redacted).expect("redacted output must be valid YAML");
        let auth = &redacted_yaml["output"]["auth"];
        let nested = &auth["some_new_auth_block"]["nested"];
        assert_eq!(
            nested["flag"].as_str(),
            Some(REDACTED_SECRET),
            "boolean auth field 'flag' must be redacted to the sentinel"
        );
        assert_eq!(
            nested["number"].as_str(),
            Some(REDACTED_SECRET),
            "numeric auth field 'number' must be redacted to the sentinel"
        );
        assert_eq!(
            nested["tagged_secret"].as_str(),
            Some(REDACTED_SECRET),
            "tagged auth field 'tagged_secret' must be redacted to the sentinel"
        );
        assert!(
            !redacted.contains("user:pass@"),
            "endpoint credentials must not be exposed"
        );
        assert!(
            redacted.contains(REDACTED_SECRET),
            "expected redacted marker in output"
        );
        assert!(
            redacted.contains("https://example.com/ingest"),
            "expected endpoint to preserve host/path after redaction"
        );
    }

    #[test]
    fn redact_config_yaml_non_yaml_input_fails_closed() {
        let raw = "not: [valid: yaml";
        let redacted = redact_config_yaml(raw);
        assert_eq!(redacted, REDACTED_CONFIG_UNAVAILABLE);
    }

    #[test]
    fn redact_config_yaml_masks_non_mapping_headers() {
        let raw = r#"
output:
  type: http
  auth:
    headers:
      - "Bearer not-safe"
"#;
        let redacted = redact_config_yaml(raw);
        assert!(!redacted.contains("not-safe"));
        assert!(redacted.contains(REDACTED_SECRET));
    }

    #[test]
    fn redact_config_yaml_masks_malformed_endpoint_userinfo() {
        let raw = r#"
output:
  type: http
  endpoint: "https://user:pass@exa mple.com/ingest"
"#;
        let redacted = redact_config_yaml(raw);
        assert!(
            !redacted.contains("user:pass@"),
            "malformed endpoint credentials must not be exposed"
        );
        assert!(
            redacted.contains(REDACTED_ENDPOINT),
            "malformed endpoint with userinfo should be replaced with fail-closed marker"
        );
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn diagnostics_server_handle_drop_releases_port() {
        let server = DiagnosticsServer::new("127.0.0.1:0");
        let (handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        drop(handle);

        let rebound_addr = format!("127.0.0.1:{port}");
        wait_until(
            std::time::Duration::from_secs(1),
            || std::net::TcpListener::bind(&rebound_addr).is_ok(),
            &format!("failed to rebind diagnostics port {port} after drop"),
        );
    }

    #[test]
    fn test_component_stats() {
        let stats = ComponentStats::new();
        assert_eq!(stats.lines(), 0);
        assert_eq!(stats.bytes(), 0);
        assert_eq!(stats.errors(), 0);
        assert_eq!(stats.health(), ComponentHealth::Healthy);

        stats.inc_lines(10);
        stats.inc_lines(5);
        assert_eq!(stats.lines(), 15);

        stats.inc_bytes(1024);
        stats.inc_bytes(2048);
        assert_eq!(stats.bytes(), 3072);

        stats.inc_errors();
        stats.inc_errors();
        stats.inc_errors();
        assert_eq!(stats.errors(), 3);

        stats.set_health(ComponentHealth::Degraded);
        assert_eq!(stats.health(), ComponentHealth::Degraded);
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn test_live_endpoint() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        // Give the server a moment to bind.
        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/live");
        assert_eq!(status, 200);
        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("invalid JSON output from /live");
        assert_eq!(
            parsed["contract_version"],
            STABLE_DIAGNOSTICS_CONTRACT_VERSION
        );
        assert_eq!(parsed["status"], "live");
        assert_eq!(parsed["version"], env!("CARGO_PKG_VERSION"));
        assert!(parsed["uptime_seconds"].is_u64(), "body: {body}");
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn test_status_endpoint() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/admin/v1/status");
        assert_eq!(status, 200);
        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("invalid JSON output from /admin/v1/status");
        assert_eq!(
            parsed["contract_version"],
            STABLE_DIAGNOSTICS_CONTRACT_VERSION
        );
        assert_eq!(parsed["component_health"]["status"], "healthy");
        assert_eq!(
            parsed["component_health"]["reason"],
            "all_components_healthy"
        );
        assert_eq!(parsed["component_health"]["readiness_impact"], "ready");
        assert!(parsed["component_health"]["observed_at_unix_ns"].is_string());
        assert_eq!(parsed["ready"]["status"], "ready");
        assert_eq!(parsed["ready"]["reason"], "all_components_healthy");
        assert!(parsed["ready"]["observed_at_unix_ns"].is_string());
        assert_eq!(parsed["live"]["status"], "live");
        assert_eq!(parsed["live"]["reason"], "process_running");
        assert!(parsed["live"]["observed_at_unix_ns"].is_string());
        assert!(body.contains(r#""name":"default""#), "body: {}", body);
        assert!(body.contains(r#""health":"healthy""#), "body: {}", body);
        assert!(body.contains(r#""lines_total":1000"#), "body: {}", body);
        assert!(body.contains(r#""lines_in":1000"#), "body: {}", body);
        assert!(body.contains(r#""lines_out":900"#), "body: {}", body);
        assert!(body.contains(r#""errors":3"#), "body: {}", body);
        assert!(body.contains(r#""total":50"#), "body: {}", body);
        assert!(body.contains(r#""avg_rows":90.0"#), "body: {}", body);
        assert!(body.contains(r#""flush_by_size":30"#), "body: {}", body);
        assert!(body.contains(r#""flush_by_timeout":20"#), "body: {}", body);
        assert!(
            body.contains(r#""cadence_fast_repolls":11"#),
            "body: {}",
            body
        );
        assert!(
            body.contains(r#""cadence_idle_sleeps":44"#),
            "body: {}",
            body
        );
        assert!(
            body.contains(r#""dropped_batches_total":5"#),
            "body: {}",
            body
        );
        assert!(
            body.contains(r#""last_batch_time_ns":"0""#),
            "body: {}",
            body
        );
        assert!(body.contains(r#""scan_errors_total":2"#), "body: {}", body);
        assert!(body.contains(r#""parse_errors_total":4"#), "body: {}", body);
        assert!(body.contains(r#""rotations":1"#), "body: {}", body);
        assert!(body.contains(r#""parse_errors":0"#), "body: {}", body);
        assert!(
            body.contains(&format!(r#""version":"{}""#, env!("CARGO_PKG_VERSION"))),
            "body: {}",
            body
        );
        // New observability fields (#521, #522, #524)
        assert!(
            body.contains(r#""batch_latency_avg_ns":10000000"#),
            "body: {}",
            body
        );
        assert!(body.contains(r#""inflight":3"#), "body: {}", body);
        assert!(body.contains(r#""rows_total":4500"#), "body: {}", body);
        assert!(
            body.contains(r#""backpressure_stalls":7"#),
            "body: {}",
            body
        );
        assert_eq!(parsed["pipelines"][0]["stage_seconds"]["queue_wait"], 0.05);
        assert_eq!(parsed["pipelines"][0]["stage_seconds"]["send"], 0.15);
        // Bottleneck field must be present and well-formed.
        assert!(body.contains(r#""bottleneck":{"stage":"#), "body: {}", body);
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn test_stats_endpoint_contract() {
        let mut server = server_with_test_pipeline();
        server.set_memory_stats_fn(|| {
            Some(MemoryStats {
                resident: 1_000_000,
                allocated: 800_000,
                active: 900_000,
            })
        });
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/admin/v1/stats");
        assert_eq!(status, 200);
        assert!(body.contains(r#""uptime_sec":"#), "body: {}", body);
        assert!(body.contains(r#""rss_bytes":"#), "body: {}", body);
        assert!(body.contains(r#""cpu_user_ms":"#), "body: {}", body);
        assert!(body.contains(r#""cpu_sys_ms":"#), "body: {}", body);
        assert!(body.contains(r#""input_lines":1000"#), "body: {}", body);
        assert!(body.contains(r#""input_bytes":50000"#), "body: {}", body);
        assert!(body.contains(r#""output_lines":900"#), "body: {}", body);
        assert!(body.contains(r#""output_bytes":30000"#), "body: {}", body);
        assert!(body.contains(r#""output_errors":2"#), "body: {}", body);
        assert!(body.contains(r#""batches":50"#), "body: {}", body);
        assert!(body.contains(r#""scan_sec":0.100000"#), "body: {}", body);
        assert!(
            body.contains(r#""transform_sec":0.500000"#),
            "body: {}",
            body
        );
        assert!(body.contains(r#""output_sec":0.200000"#), "body: {}", body);
        assert!(body.contains(r#""mem_resident":1000000"#), "body: {}", body);
        assert!(body.contains(r#""mem_allocated":800000"#), "body: {}", body);
        assert!(body.contains(r#""mem_active":900000"#), "body: {}", body);
    }

    #[test]
    fn parse_stat_with_space_in_comm_returns_expected_metrics() {
        // We only rely on fields 14 (utime), 15 (stime), and 24 (rss).
        // This synthetic line keeps earlier fields simple while preserving
        // the comm-with-spaces shape: `pid (comm with spaces) ...`.
        let stat_line = "12345 (my process name) R 1 2 3 4 5 6 7 8 9 10 300 200 13 14 15 16 17 18 19 20 5 999 23";
        let parsed = parse_proc_stat(stat_line);
        assert!(parsed.is_some());

        let (rss_bytes, user_ms, sys_ms) = parsed.expect("synthetic stat line should parse");
        let ticks_per_sec = get_ticks_per_sec().expect("CLK_TCK should be available");
        let page_size = get_page_size().expect("PAGESIZE should be available");

        assert_eq!(user_ms, (300 * 1000) / ticks_per_sec);
        assert_eq!(sys_ms, (200 * 1000) / ticks_per_sec);
        assert_eq!(rss_bytes, 5 * page_size);
    }

    #[test]
    fn parse_stat_empty_input_returns_none() {
        assert_eq!(parse_proc_stat(""), None);
    }

    #[test]
    fn test_component_stats_parse_errors() {
        let stats = ComponentStats::new();
        assert_eq!(stats.parse_errors_total.load(Ordering::Relaxed), 0);

        stats.inc_parse_errors(3);
        assert_eq!(stats.parse_errors_total.load(Ordering::Relaxed), 3);

        stats.inc_parse_errors(2);
        assert_eq!(stats.parse_errors_total.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn test_component_stats_rotations() {
        let stats = ComponentStats::new();
        assert_eq!(stats.rotations_total.load(Ordering::Relaxed), 0);

        stats.inc_rotations();
        stats.inc_rotations();
        assert_eq!(stats.rotations_total.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_pipeline_metrics_record_queue_wait() {
        let meter = opentelemetry::global::meter("test");
        let pm = PipelineMetrics::new("test", "", &meter);
        assert_eq!(pm.queue_wait_nanos_total.load(Ordering::Relaxed), 0);

        pm.record_queue_wait(1_000_000);
        pm.record_queue_wait(2_000_000);
        assert_eq!(pm.queue_wait_nanos_total.load(Ordering::Relaxed), 3_000_000);
    }

    #[test]
    fn test_pipeline_metrics_record_send_latency() {
        let meter = opentelemetry::global::meter("test");
        let pm = PipelineMetrics::new("test", "", &meter);
        assert_eq!(pm.send_nanos_total.load(Ordering::Relaxed), 0);

        pm.record_send_latency(5_000_000);
        pm.record_send_latency(3_000_000);
        assert_eq!(pm.send_nanos_total.load(Ordering::Relaxed), 8_000_000);
    }

    #[test]
    fn test_pipeline_metrics_record_batch_latency() {
        let meter = opentelemetry::global::meter("test");
        let pm = PipelineMetrics::new("test", "", &meter);
        assert_eq!(pm.batch_latency_nanos_total.load(Ordering::Relaxed), 0);

        pm.record_batch_latency(10_000_000);
        pm.record_batch_latency(20_000_000);
        assert_eq!(
            pm.batch_latency_nanos_total.load(Ordering::Relaxed),
            30_000_000
        );
    }

    #[test]
    fn test_pipeline_metrics_failure_counters() {
        let meter = opentelemetry::global::meter("test");
        let mut pm = PipelineMetrics::new("test", "", &meter);
        pm.add_output("sink_a", "stdout");
        pm.add_output("sink_b", "stdout");

        pm.inc_dropped_batch();
        pm.inc_scan_error();
        pm.inc_parse_error();
        pm.output_error("sink_b");

        assert_eq!(pm.dropped_batches_total.load(Ordering::Relaxed), 1);
        assert_eq!(pm.scan_errors_total.load(Ordering::Relaxed), 1);
        assert_eq!(pm.parse_errors_total.load(Ordering::Relaxed), 1);
        assert_eq!(pm.outputs[0].2.errors(), 0);
        assert_eq!(pm.outputs[1].2.errors(), 1);
    }

    #[test]
    fn test_pipeline_metrics_output_error_broadcasts_fanout_runtime_name() {
        let meter = opentelemetry::global::meter("test");
        let mut pm = PipelineMetrics::new("test", "", &meter);
        pm.add_output("sink_a", "stdout");
        pm.add_output("sink_b", "stdout");

        pm.output_error("fanout(sink_a,sink_b)");

        assert_eq!(pm.outputs[0].2.errors(), 1);
        assert_eq!(pm.outputs[1].2.errors(), 1);
    }

    #[test]
    fn test_pipeline_metrics_output_error_broadcasts_pipeline_rollup_name() {
        let meter = opentelemetry::global::meter("test");
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        pm.add_output("sink_a", "stdout");
        pm.add_output("sink_b", "stdout");

        pm.output_error("pipe");

        assert_eq!(pm.outputs[0].2.errors(), 1);
        assert_eq!(pm.outputs[1].2.errors(), 1);
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn test_not_found() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, _body) = http_get(port, "/nonexistent");
        assert_eq!(status, 404);
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn test_status_endpoint_no_memory_stats() {
        // Without a memory_stats_fn set, the system section must NOT contain
        // a "memory" key — no partial or null fields.
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/admin/v1/status");
        assert_eq!(status, 200);
        assert!(body.contains(r#""rotations":1"#), "body: {}", body);
        assert!(
            !body.contains(r#""memory""#),
            "unexpected memory key: {}",
            body
        );
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn test_status_endpoint_with_memory_stats() {
        // With a memory_stats_fn set, the system section must include
        // "memory" with resident/allocated/active fields.
        let mut server = server_with_test_pipeline();
        server.set_memory_stats_fn(|| {
            Some(MemoryStats {
                resident: 1_000_000,
                allocated: 800_000,
                active: 900_000,
            })
        });
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/admin/v1/status");
        assert_eq!(status, 200);
        assert!(body.contains(r#""memory""#), "missing memory key: {}", body);
        assert!(body.contains(r#""resident":1000000"#), "body: {}", body);
        assert!(body.contains(r#""allocated":800000"#), "body: {}", body);
        assert!(body.contains(r#""active":900000"#), "body: {}", body);
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn test_ready_endpoint_no_pipelines_returns_503() {
        // No pipelines registered yet → not ready.
        let server = DiagnosticsServer::new("127.0.0.1:0");
        // Don't add any pipelines.
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/ready");
        assert_eq!(status, 503, "body: {}", body);
        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("invalid JSON output from /ready");
        assert_eq!(
            parsed["contract_version"],
            STABLE_DIAGNOSTICS_CONTRACT_VERSION
        );
        assert_eq!(parsed["status"], "not_ready");
        assert_eq!(parsed["reason"], "no_pipelines_registered");
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn test_ready_endpoint_with_pipeline_returns_200() {
        // A registered pipeline makes the server ready, regardless of
        // whether any batches have been processed.
        let meter = opentelemetry::global::meter("test");
        let pm = PipelineMetrics::new("default", "SELECT * FROM logs", &meter);
        // last_batch_time_ns stays at 0 — no data yet, but still ready.

        let mut server = DiagnosticsServer::new("127.0.0.1:0");
        server.add_pipeline(Arc::new(pm));
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/ready");
        assert_eq!(status, 200, "body: {}", body);
        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("invalid JSON output from /ready");
        assert_eq!(
            parsed["contract_version"],
            STABLE_DIAGNOSTICS_CONTRACT_VERSION
        );
        assert_eq!(parsed["status"], "ready");
        assert_eq!(parsed["reason"], "all_components_healthy");
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn test_ready_endpoint_with_starting_component_returns_503() {
        let meter = opentelemetry::global::meter("test");
        let mut pm = PipelineMetrics::new("default", "SELECT * FROM logs", &meter);
        let input = pm.add_input("receiver", "otlp");
        input.set_health(ComponentHealth::Starting);

        let mut server = DiagnosticsServer::new("127.0.0.1:0");
        server.add_pipeline(Arc::new(pm));
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/ready");
        assert_eq!(status, 503, "body: {}", body);
        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("invalid JSON output from /ready");
        assert_eq!(parsed["status"], "not_ready");
        assert_eq!(parsed["reason"], "components_starting");
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn test_ready_endpoint_with_degraded_input_stays_200() {
        let meter = opentelemetry::global::meter("test");
        let mut pm = PipelineMetrics::new("default", "SELECT * FROM logs", &meter);
        let input = pm.add_input("receiver", "tcp");
        input.set_health(ComponentHealth::Degraded);

        let mut server = DiagnosticsServer::new("127.0.0.1:0");
        server.add_pipeline(Arc::new(pm));
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/ready");
        assert_eq!(status, 200, "body: {}", body);
        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("invalid JSON output from /ready");
        assert_eq!(parsed["status"], "ready");
        assert_eq!(parsed["reason"], "components_degraded_but_operational");
    }

    #[test]
    fn test_ready_endpoint_stays_in_sync_with_admin_ready_snapshot() {
        // Test the policy layer directly rather than spinning up HTTP servers
        // for each health state. Both /ready and /admin/v1/status derive their
        // ready status and reason from the same `policy::readiness_snapshot`
        // call, so verifying the snapshot output is sufficient to prove they
        // are in sync without leaving immortal sampler threads behind.
        fn assert_ready_snapshot_sync(
            server: &DiagnosticsServer,
            expected_status: &str,
            expected_reason: &str,
            expected_ready: bool,
        ) {
            let snapshot = policy::readiness_snapshot(&server.pipelines);
            let actual_status = if snapshot.ready { "ready" } else { "not_ready" };
            assert_eq!(
                actual_status, expected_status,
                "health state mismatch: got status={actual_status} reason={} expected status={expected_status}",
                snapshot.reason
            );
            assert_eq!(
                snapshot.reason, expected_reason,
                "reason mismatch for status={expected_status}"
            );
            assert_eq!(
                snapshot.ready, expected_ready,
                "ready bool mismatch for status={expected_status}"
            );
        }

        assert_ready_snapshot_sync(
            &DiagnosticsServer::new("127.0.0.1:0"),
            "not_ready",
            "no_pipelines_registered",
            false,
        );

        assert_ready_snapshot_sync(
            &server_with_single_input_health(ComponentHealth::Healthy),
            "ready",
            "all_components_healthy",
            true,
        );

        assert_ready_snapshot_sync(
            &server_with_single_input_health(ComponentHealth::Starting),
            "not_ready",
            "components_starting",
            false,
        );

        assert_ready_snapshot_sync(
            &server_with_single_input_health(ComponentHealth::Degraded),
            "ready",
            "components_degraded_but_operational",
            true,
        );

        assert_ready_snapshot_sync(
            &server_with_single_input_health(ComponentHealth::Stopping),
            "not_ready",
            "components_stopping",
            false,
        );

        assert_ready_snapshot_sync(
            &server_with_single_input_health(ComponentHealth::Stopped),
            "not_ready",
            "components_stopped",
            false,
        );

        assert_ready_snapshot_sync(
            &server_with_single_input_health(ComponentHealth::Failed),
            "not_ready",
            "components_failed",
            false,
        );
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn test_status_endpoint_includes_transport_parity_fields() {
        let meter = opentelemetry::global::meter("test");
        let mut pm = PipelineMetrics::new("default", "SELECT * FROM logs", &meter);

        let file_in = pm.add_input("file_in", "file");
        file_in.file_error_polls.store(5, Ordering::Relaxed);

        let tcp_in = pm.add_input("tcp_in", "tcp");
        tcp_in.tcp_accepted.store(42, Ordering::Relaxed);
        tcp_in.tcp_active.store(3, Ordering::Relaxed);

        let udp_in = pm.add_input("udp_in", "udp");
        udp_in.udp_drops.store(100, Ordering::Relaxed);
        udp_in.udp_recv_buf.store(8388608, Ordering::Relaxed);

        let mut server = DiagnosticsServer::new("127.0.0.1:0");
        server.add_pipeline(Arc::new(pm));
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        let (status, body) = http_get(port, "/admin/v1/status");
        assert_eq!(status, 200);
        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("invalid JSON output from /admin/v1/status");
        let inputs = parsed["pipelines"][0]["inputs"]
            .as_array()
            .expect("status endpoint should include pipeline inputs");

        let file = inputs
            .iter()
            .find(|input| input["name"] == "file_in")
            .expect("missing file_in input");
        assert_eq!(file["transport"]["file"]["consecutive_error_polls"], 5);

        let tcp = inputs
            .iter()
            .find(|input| input["name"] == "tcp_in")
            .expect("missing tcp_in input");
        assert_eq!(tcp["transport"]["tcp"]["accepted_connections"], 42);
        assert_eq!(tcp["transport"]["tcp"]["active_connections"], 3);

        let udp = inputs
            .iter()
            .find(|input| input["name"] == "udp_in")
            .expect("missing udp_in input");
        assert_eq!(udp["transport"]["udp"]["drops_detected"], 100);
        assert_eq!(udp["transport"]["udp"]["recv_buffer_size"], 8_388_608);
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn test_status_endpoint_shows_degraded_input_as_non_blocking() {
        let meter = opentelemetry::global::meter("test");
        let mut pm = PipelineMetrics::new("default", "SELECT * FROM logs", &meter);
        let input = pm.add_input("receiver", "tcp");
        input.set_health(ComponentHealth::Degraded);

        let mut server = DiagnosticsServer::new("127.0.0.1:0");
        server.add_pipeline(Arc::new(pm));
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/admin/v1/status");
        assert_eq!(status, 200, "body: {}", body);
        assert!(
            body.contains(r#""ready":{"status":"ready""#),
            "body: {}",
            body
        );
        assert!(
            body.contains(r#""component_health":{"status":"degraded","reason":"components_degraded_but_operational","readiness_impact":"non_blocking""#),
            "body: {}",
            body
        );
        assert!(
            body.contains(r#""inputs":[{"name":"receiver","type":"tcp","health":"degraded""#),
            "body: {}",
            body
        );
    }

    #[derive(serde::Deserialize)]
    struct MinimalStatusCompatibilityView {
        contract_version: String,
        ready: MinimalReadyCompatibilityView,
    }

    #[derive(serde::Deserialize)]
    struct MinimalReadyCompatibilityView {
        status: String,
        reason: String,
    }

    #[test]
    fn test_stable_diagnostics_endpoints_expose_contract_version() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (_, live_body) = http_get(port, "/live");
        let live: serde_json::Value =
            serde_json::from_str(&live_body).expect("invalid JSON output from /live");
        assert_eq!(
            live["contract_version"],
            STABLE_DIAGNOSTICS_CONTRACT_VERSION
        );

        let (_, ready_body) = http_get(port, "/ready");
        let ready: serde_json::Value =
            serde_json::from_str(&ready_body).expect("invalid JSON output from /ready");
        assert_eq!(
            ready["contract_version"],
            STABLE_DIAGNOSTICS_CONTRACT_VERSION
        );

        let (_, status_body) = http_get(port, "/admin/v1/status");
        let status: serde_json::Value =
            serde_json::from_str(&status_body).expect("invalid JSON output from /admin/v1/status");
        assert_eq!(
            status["contract_version"],
            STABLE_DIAGNOSTICS_CONTRACT_VERSION
        );
    }

    #[test]
    fn test_status_endpoint_additive_compatibility_fixture() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (_status, body) = http_get(port, "/admin/v1/status");
        let fixture: MinimalStatusCompatibilityView =
            serde_json::from_str(&body).expect("status contract should stay additive-compatible");
        assert_eq!(
            fixture.contract_version,
            STABLE_DIAGNOSTICS_CONTRACT_VERSION.to_string()
        );
        assert_eq!(fixture.ready.status, "ready");
        assert_eq!(fixture.ready.reason, "all_components_healthy");
    }

    #[test]
    fn test_esc_control_chars() {
        assert_eq!(esc("hello\0world"), "hello\\u0000world");
        assert_eq!(esc("tab\tnewline\nreturn\r"), "tab\\tnewline\\nreturn\\r");
        assert_eq!(esc("bell\x07"), "bell\\u0007");
        assert_eq!(esc("escape\x1b"), "escape\\u001b");
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn test_status_endpoint_escaping() {
        let meter = opentelemetry::global::meter("test");
        // Control character in pipeline name.
        let pm = PipelineMetrics::new("pipe\x01line", "SELECT * FROM logs", &meter);

        let mut server = DiagnosticsServer::new("127.0.0.1:0");
        server.add_pipeline(Arc::new(pm));
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/admin/v1/status");
        assert_eq!(status, 200);
        // The name should be escaped as "pipe\u0001line".
        assert!(
            body.contains(r#""name":"pipe\u0001line""#),
            "body: {}",
            body
        );

        // Check that the overall JSON is valid (can be parsed).
        let _v: serde_json::Value =
            serde_json::from_str(&body).expect("invalid JSON output from /admin/v1/status");
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn test_traces_endpoint_empty() {
        // Server with no trace buffer attached — should return empty array.
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/admin/v1/traces");
        assert_eq!(status, 200);
        assert_eq!(body, r#"{"traces":[]}"#, "unexpected body: {body}");
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn test_traces_endpoint_with_data() {
        use crate::span_exporter::{SpanBuffer, TraceSpan};

        let trace_buf = SpanBuffer::new();

        // Push a root span (parent_id all-zeros = root).
        trace_buf.push_test_span(TraceSpan {
            trace_id: "aabbccdd00112233aabbccdd00112233".into(),
            span_id: "aabbccdd00112233".into(),
            parent_id: "0000000000000000".into(),
            name: "batch".into(),
            start_unix_ns: 1_000_000_000,
            duration_ns: 200_000_000,
            attrs: vec![
                ["pipeline".into(), "default".into()],
                ["bytes_in".into(), "4096".into()],
                ["input_rows".into(), "100".into()],
                ["output_rows".into(), "75".into()],
                ["errors".into(), "0".into()],
                ["flush_reason".into(), "size".into()],
                ["queue_wait_ns".into(), "5000000".into()],
            ],
            status: "ok",
        });
        // Push a scan child span.
        trace_buf.push_test_span(TraceSpan {
            trace_id: "aabbccdd00112233aabbccdd00112233".into(),
            span_id: "1122334455667788".into(),
            parent_id: "aabbccdd00112233".into(),
            name: "scan".into(),
            start_unix_ns: 1_000_000_000,
            duration_ns: 150_000_000,
            attrs: vec![["rows".into(), "100".into()]],
            status: "ok",
        });

        let mut server = server_with_test_pipeline();
        server.set_trace_buffer(trace_buf);
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/admin/v1/traces");
        assert_eq!(status, 200);

        // Must parse as valid JSON.
        let v: serde_json::Value =
            serde_json::from_str(&body).expect("invalid JSON from /admin/v1/traces");

        let traces = v["traces"].as_array().expect("traces must be array");
        assert_eq!(traces.len(), 1, "expected 1 trace, got {}", traces.len());

        let t = &traces[0];
        assert_eq!(t["pipeline"], "default");
        assert_eq!(t["input_rows"], 100);
        assert_eq!(t["output_rows"], 75);
        assert_eq!(t["errors"], 0);
        assert_eq!(t["flush_reason"], "size");
        assert_eq!(t["scan_ns"], "150000000");
        assert_eq!(t["total_ns"], "200000000");
        // All nanosecond fields must be serialized as JSON strings to avoid
        // JavaScript 53-bit precision loss.
        assert_eq!(t["start_unix_ns"], "1000000000");
        assert_eq!(t["transform_ns"], "0");
        assert_eq!(t["output_ns"], "0");
        assert_eq!(t["queue_wait_ns"], "5000000");
        assert_eq!(t["status"], "ok");
        assert_eq!(t["lifecycle_state"], "completed");
    }

    #[test]
    fn test_traces_endpoint_explicit_lifecycle_states_for_active_batches() {
        use crate::span_exporter::SpanBuffer;

        let meter = opentelemetry::global::meter("test");
        let pm = Arc::new(PipelineMetrics::new(
            "default",
            "SELECT * FROM logs",
            &meter,
        ));

        // One batch per active state.
        pm.begin_active_batch(1, 1_000_000_000, 0, 0);
        pm.advance_active_batch(1, "scan", 0, 1_000_100_000);

        pm.begin_active_batch(2, 1_100_000_000, 12_000_000, 0);
        pm.advance_active_batch(2, "scan", 12_000_000, 1_112_000_000);
        pm.advance_active_batch(2, "transform", 0, 1_112_500_000);

        pm.begin_active_batch(3, 1_200_000_000, 8_000_000, 5_000_000);

        pm.begin_active_batch(4, 1_300_000_000, 9_000_000, 4_000_000);
        pm.assign_worker_to_active_batch(4, 7, 1_313_500_000);

        let mut server = DiagnosticsServer::new("127.0.0.1:0");
        server.add_pipeline(Arc::clone(&pm));
        server.set_trace_buffer(SpanBuffer::new());
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/admin/v1/traces");
        assert_eq!(status, 200);

        let v: serde_json::Value =
            serde_json::from_str(&body).expect("invalid JSON from /admin/v1/traces");
        let traces = v["traces"].as_array().expect("traces must be array");
        assert_eq!(traces.len(), 4);

        let mut by_state = std::collections::HashMap::new();
        for trace in traces {
            by_state.insert(
                trace["lifecycle_state"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                trace.clone(),
            );
        }

        assert!(by_state.contains_key("scan_in_progress"));
        assert!(by_state.contains_key("transform_in_progress"));
        assert!(by_state.contains_key("queued_for_output"));
        assert!(by_state.contains_key("output_in_progress"));

        let queued = by_state
            .get("queued_for_output")
            .expect("queued trace missing");
        assert_eq!(queued["worker_id"], serde_json::Value::Null);
        assert_eq!(queued["lifecycle_state_start_unix_ns"], "1200000000");

        let output = by_state
            .get("output_in_progress")
            .expect("output trace missing");
        assert_eq!(output["worker_id"], 7);
        assert_eq!(output["output_start_unix_ns"], "1313500000");
        assert_eq!(output["lifecycle_state_start_unix_ns"], "1313500000");
    }

    /// Raw TCP POST helper for method-rejection tests.
    fn http_post(port: u16, path: &str) -> u16 {
        use std::io::Write;
        use std::net::TcpStream;

        let addr = format!("127.0.0.1:{port}");
        let mut stream = None;
        for _ in 0..20 {
            match TcpStream::connect(&addr) {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(_) => thread::sleep(std::time::Duration::from_millis(50)),
            }
        }
        let mut stream = stream.unwrap_or_else(|| panic!("connect failed after retries to {addr}"));
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .ok();
        let req = format!(
            "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(req.as_bytes()).unwrap();

        let mut buf = Vec::new();
        let _ = stream.read_to_end(&mut buf);
        let text = String::from_utf8_lossy(&buf).to_string();
        text.lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(0)
    }

    /// Regression test for #1695: backpressure_stalls was missing from sample_metrics,
    /// so /admin/v1/history never included it even though /admin/v1/stats did.
    #[test]
    fn sample_metrics_records_backpressure_stalls() {
        let meter = opentelemetry::global::meter("test");
        let pm = PipelineMetrics::new("p", "", &meter);
        pm.inc_backpressure_stall();
        pm.inc_backpressure_stall();
        pm.inc_backpressure_stall();

        let history = crate::metric_history::MetricHistory::new();
        sample_metrics(&[Arc::new(pm)], &history, None);

        let json = history.to_json();
        assert!(
            json.contains("\"backpressure_stalls\""),
            "backpressure_stalls key missing from history JSON: {json}"
        );
        // The recorded value should reflect the 3 stalls.
        assert!(
            json.contains("3.0000") || json.contains(",3."),
            "expected value 3 in history JSON: {json}"
        );
    }

    // Bug #728: diagnostics server should return 405 for non-GET methods.
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn non_get_returns_405() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        for path in &["/live", "/admin/v1/status", "/ready", "/admin/v1/stats"] {
            let status = http_post(port, path);
            assert_eq!(status, 405, "POST {path} should return 405, got {status}");
        }
    }

    // -- OTLP telemetry endpoint tests --
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn telemetry_metrics_endpoint_returns_valid_otlp() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        // Wait for at least one 2s sampler cycle.
        thread::sleep(std::time::Duration::from_millis(2500));

        let (status, body) = http_get(port, "/admin/v1/telemetry/metrics");
        assert_eq!(status, 200, "body: {body}");

        // Must be valid JSON.
        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("telemetry/metrics must be valid JSON");

        // Must have OTLP structure.
        assert!(
            parsed["resourceMetrics"].is_array(),
            "expected resourceMetrics array"
        );

        // After a 2s sample cycle, should have metrics.
        let rm = &parsed["resourceMetrics"];
        if rm.as_array().unwrap().is_empty() {
            // Sampler may not have run yet — just verify the structure is valid.
            return;
        }
        let resource = &rm[0]["resource"];
        assert_eq!(
            resource["attributes"][0]["key"], "service.name",
            "resource must have service.name"
        );
        assert_eq!(resource["attributes"][0]["value"]["stringValue"], "logfwd");

        let metrics = &rm[0]["scopeMetrics"][0]["metrics"];
        assert!(
            metrics.as_array().is_some_and(|a| !a.is_empty()),
            "expected at least one metric"
        );
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn telemetry_traces_endpoint_with_spans() {
        use crate::span_exporter::{SpanBuffer, TraceSpan};

        let trace_buf = SpanBuffer::new();
        trace_buf.push_test_span(TraceSpan {
            trace_id: "aabbccdd00112233aabbccdd00112233".into(),
            span_id: "aabbccdd00112233".into(),
            parent_id: "0000000000000000".into(),
            name: "batch".into(),
            start_unix_ns: 1_000_000_000,
            duration_ns: 200_000_000,
            attrs: vec![
                ["pipeline".into(), "default".into()],
                ["flush_reason".into(), "size".into()],
            ],
            status: "ok",
        });

        let mut server = server_with_test_pipeline();
        server.set_trace_buffer(trace_buf);
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/admin/v1/telemetry/traces");
        assert_eq!(status, 200, "body: {body}");

        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("telemetry/traces must be valid JSON");

        let spans = &parsed["resourceSpans"][0]["scopeSpans"][0]["spans"];
        assert!(
            spans.as_array().is_some_and(|a| !a.is_empty()),
            "expected at least one span, got: {body}"
        );

        // Verify the span has OTLP-compliant fields.
        let span = &spans[0];
        assert_eq!(span["traceId"], "aabbccdd00112233aabbccdd00112233");
        assert!(span["kind"].is_number(), "kind must be integer");
        assert!(
            span["startTimeUnixNano"].is_string(),
            "startTimeUnixNano must be a string"
        );
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn telemetry_logs_endpoint_returns_valid_otlp() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/admin/v1/telemetry/logs");
        assert_eq!(status, 200, "body: {body}");

        let parsed: serde_json::Value =
            serde_json::from_str(&body).expect("telemetry/logs must be valid JSON");

        // On a fresh server with no stderr capture output, logs may be empty.
        assert!(
            parsed["resourceLogs"].is_array(),
            "expected resourceLogs array"
        );
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn telemetry_endpoints_empty_on_fresh_start() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        // Immediately hit telemetry endpoints before any sampler run.
        thread::sleep(std::time::Duration::from_millis(100));

        for (path, key) in [
            ("/admin/v1/telemetry/metrics", "resourceMetrics"),
            ("/admin/v1/telemetry/logs", "resourceLogs"),
        ] {
            let (status, body) = http_get(port, path);
            assert_eq!(status, 200, "{path}: {body}");
            let parsed: serde_json::Value =
                serde_json::from_str(&body).expect("must be valid JSON");
            assert!(parsed[key].is_array(), "{path} must have {key} array");
        }

        // Traces endpoint should work even with no trace buffer.
        let (status, body) = http_get(port, "/admin/v1/telemetry/traces");
        assert_eq!(status, 200, "traces: {body}");
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("must be valid JSON");
        // May have active-batch spans from the test pipeline, or may be empty.
        assert!(
            parsed["resourceSpans"].is_array(),
            "traces must have resourceSpans array"
        );
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn existing_endpoints_unchanged_after_telemetry() {
        // Backward compatibility: existing endpoints must still work.
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        // Original endpoints must return 200 with their expected format.
        let (status, body) = http_get(port, "/admin/v1/stats");
        assert_eq!(status, 200, "stats: {body}");
        assert!(
            body.contains("\"input_lines\""),
            "stats must have input_lines: {body}"
        );

        let (status, body) = http_get(port, "/admin/v1/status");
        assert_eq!(status, 200, "status: {body}");
        assert!(
            body.contains("\"pipelines\""),
            "status must have pipelines: {body}"
        );

        let (status, body) = http_get(port, "/live");
        assert_eq!(status, 200, "live: {body}");
        assert!(body.contains("\"status\":\"live\""), "live: {body}");
    }
    #[test]
    #[ignore = "network integration test; run with `just test-network`"]
    fn removed_legacy_endpoints_return_404() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        for path in [
            "/health",
            "/api/pipelines",
            "/api/stats",
            "/api/config",
            "/api/logs",
            "/api/history",
            "/api/traces",
            "/metrics",
        ] {
            let (status, body) = http_get(port, path);
            assert_eq!(
                status, 404,
                "expected 404 for {path}, got {status} body={body}"
            );
        }
    }
}

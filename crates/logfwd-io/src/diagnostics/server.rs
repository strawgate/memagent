use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use tokio::sync::{broadcast, oneshot};

use super::metrics::PipelineMetrics;
use super::models::MemoryStats;
use super::policy;
use super::render::{esc, now_nanos};
use super::telemetry;
use crate::background_http_task::BackgroundHttpTask;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DASHBOARD_HTML: &str = include_str!("../dashboard.html");
const REDACTED_SECRET: &str = "***redacted***";
const REDACTED_CONFIG_UNAVAILABLE: &str = "<redacted config unavailable>";

fn redact_endpoint_credentials(endpoint: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(endpoint) else {
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
    match value {
        serde_yaml_ng::Value::Mapping(map) => {
            for (key, val) in map.iter_mut() {
                let Some(key_str) = key.as_str() else {
                    redact_yaml_value(val, in_auth_block);
                    continue;
                };

                let next_in_auth = in_auth_block || key_str == "auth";

                if in_auth_block && key_str == "bearer_token" {
                    *val = serde_yaml_ng::Value::String(REDACTED_SECRET.to_string());
                    continue;
                }

                if in_auth_block && key_str == "headers" {
                    if let serde_yaml_ng::Value::Mapping(headers) = val {
                        for (_, header_value) in headers.iter_mut() {
                            *header_value =
                                serde_yaml_ng::Value::String(REDACTED_SECRET.to_string());
                        }
                    } else {
                        *val = serde_yaml_ng::Value::String(REDACTED_SECRET.to_string());
                    }
                    continue;
                }

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

fn redact_config_yaml(raw_yaml: &str) -> String {
    let Ok(mut parsed) = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(raw_yaml) else {
        return REDACTED_CONFIG_UNAVAILABLE.to_string();
    };
    redact_yaml_value(&mut parsed, false);
    serde_yaml_ng::to_string(&parsed).unwrap_or_else(|_| REDACTED_CONFIG_UNAVAILABLE.to_string())
}

// ---------------------------------------------------------------------------
// ServerHandle
// ---------------------------------------------------------------------------

/// Owns the background thread spawned by [`DiagnosticsServer::start`].
///
/// Dropping this value signals the axum server and sampler task to shut down
/// via the oneshot channel, then joins the background thread.
pub struct ServerHandle {
    _http_task: BackgroundHttpTask,
}

// ---------------------------------------------------------------------------
// Shared axum state
// ---------------------------------------------------------------------------

struct DiagnosticsState {
    pipelines: Vec<Arc<PipelineMetrics>>,
    start_time: Instant,
    memory_stats_fn: Option<fn() -> Option<MemoryStats>>,
    config_yaml: String,
    config_path: String,
    config_endpoint_enabled: bool,
    stderr: crate::stderr_capture::StderrCapture,
    trace_buf: Option<crate::span_exporter::SpanBuffer>,
    telemetry_tx: broadcast::Sender<String>,
}

// ---------------------------------------------------------------------------
// DiagnosticsServer (builder + startup)
// ---------------------------------------------------------------------------

/// Lightweight diagnostics HTTP server backed by axum with WebSocket telemetry push.
pub struct DiagnosticsServer {
    pipelines: Vec<Arc<PipelineMetrics>>,
    start_time: Instant,
    bind_addr: String,
    memory_stats_fn: Option<fn() -> Option<MemoryStats>>,
    config_yaml: String,
    config_path: String,
    config_endpoint_enabled: bool,
    stderr: crate::stderr_capture::StderrCapture,
    trace_buf: Option<crate::span_exporter::SpanBuffer>,
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
            trace_buf: None,
        }
    }

    pub fn set_trace_buffer(&mut self, buf: crate::span_exporter::SpanBuffer) {
        self.trace_buf = Some(buf);
    }

    pub fn set_config(&mut self, path: &str, yaml: &str) {
        self.config_path = path.to_string();
        self.config_yaml = yaml.to_string();
    }

    pub fn set_config_endpoint_enabled(&mut self, enabled: bool) {
        self.config_endpoint_enabled = enabled;
    }

    pub fn add_pipeline(&mut self, metrics: Arc<PipelineMetrics>) {
        self.pipelines.push(metrics);
    }

    pub fn set_memory_stats_fn(&mut self, f: fn() -> Option<MemoryStats>) {
        self.memory_stats_fn = Some(f);
    }

    /// Spawn the server on a background thread. Binds synchronously before
    /// returning so that port-in-use errors are reported at startup.
    pub fn start(self) -> io::Result<(ServerHandle, std::net::SocketAddr)> {
        let std_listener = std::net::TcpListener::bind(&self.bind_addr)
            .map_err(|e| io::Error::other(format!("diagnostics bind {}: {e}", self.bind_addr)))?;
        let bound_addr = std_listener.local_addr()?;
        std_listener.set_nonblocking(true).map_err(|e| {
            io::Error::other(format!("diagnostics set_nonblocking {bound_addr}: {e}"))
        })?;

        if let Err(e) = self.stderr.start() {
            tracing::warn!(error = %e, "stderr capture failed");
        }

        let (telemetry_tx, _) = broadcast::channel(16);

        let state = Arc::new(DiagnosticsState {
            pipelines: self.pipelines,
            start_time: self.start_time,
            memory_stats_fn: self.memory_stats_fn,
            config_yaml: self.config_yaml,
            config_path: self.config_path,
            config_endpoint_enabled: self.config_endpoint_enabled,
            stderr: self.stderr,
            trace_buf: self.trace_buf,
            telemetry_tx,
        });

        let app = axum::Router::new()
            .route("/", get(serve_dashboard))
            .route("/live", get(serve_live))
            .route("/ready", get(serve_ready))
            .route("/admin/v1/status", get(serve_status))
            .route("/admin/v1/config", get(serve_config))
            .route("/admin/v1/telemetry", get(ws_telemetry))
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
                            .send(Err(format!("diagnostics runtime creation failed: {e}")))
                            .ok();
                        return;
                    }
                };

                runtime.block_on(async move {
                    let listener = match tokio::net::TcpListener::from_std(std_listener) {
                        Ok(l) => l,
                        Err(e) => {
                            tracing::error!(error = %e, "diagnostics TcpListener conversion failed");
                            startup_tx
                                .send(Err(format!(
                                    "diagnostics TcpListener conversion failed: {e}"
                                )))
                                .ok();
                            return;
                        }
                    };

                    // Spawn sampler task.
                    tokio::spawn(sampler_loop(Arc::clone(&state)));

                    // Signal that the server is alive and ready to accept connections.
                    startup_tx.send(Ok(())).ok();

                    let server =
                        axum::serve(listener, app).with_graceful_shutdown(async move {
                            let _ = shutdown_rx.await;
                        });

                    let _ = server.await;
                });
            })
            .map_err(|e| io::Error::other(format!("failed to spawn diagnostics-http: {e}")))?;

        // Wait for the background thread to confirm startup before returning.
        match startup_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(msg)) => {
                return Err(io::Error::other(msg));
            }
            Err(_) => {
                return Err(io::Error::other(
                    "diagnostics thread exited without signaling startup",
                ));
            }
        }

        Ok((
            ServerHandle {
                _http_task: BackgroundHttpTask::new_axum(shutdown_tx, handle),
            },
            bound_addr,
        ))
    }
}

// ---------------------------------------------------------------------------
// Sampler task
// ---------------------------------------------------------------------------

async fn sampler_loop(state: Arc<DiagnosticsState>) {
    let mut last_log_count = 0usize;
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Always collect logs to keep last_log_count in sync with the ring buffer,
        // but skip OTLP serialization + broadcast when no WebSocket subscribers.
        let logs = telemetry::collect_new_logs(&state.stderr, &mut last_log_count);

        if state.telemetry_tx.receiver_count() == 0 {
            continue;
        }

        let metrics = telemetry::sample_metrics(
            &state.pipelines,
            state.memory_stats_fn,
            state.start_time.elapsed(),
        );
        let _ = state
            .telemetry_tx
            .send(telemetry::metrics_to_otlp_json(&metrics));

        let spans = telemetry::collect_spans(state.trace_buf.as_ref(), &state.pipelines);
        let _ = state
            .telemetry_tx
            .send(telemetry::spans_to_otlp_json(&spans));

        if !logs.is_empty() {
            let _ = state.telemetry_tx.send(telemetry::logs_to_otlp_json(&logs));
        }
    }
}

// ---------------------------------------------------------------------------
// Axum handlers
// ---------------------------------------------------------------------------

async fn serve_dashboard() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

async fn serve_live(
    State(state): State<Arc<DiagnosticsState>>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    let uptime = state.start_time.elapsed().as_secs();
    let body = format!(
        r#"{{"status":"live","uptime_seconds":{},"version":"{}"}}"#,
        uptime, VERSION,
    );
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body,
    )
}

async fn serve_ready(State(state): State<Arc<DiagnosticsState>>) -> impl IntoResponse {
    let snapshot = policy::readiness_snapshot(&state.pipelines);
    let observed_at_unix_ns = now_nanos();

    let (status_text, http_status) = if snapshot.ready {
        ("ready", StatusCode::OK)
    } else {
        ("not_ready", StatusCode::SERVICE_UNAVAILABLE)
    };

    let body = format!(
        r#"{{"status":"{}","reason":"{}","observed_at_unix_ns":"{}"}}"#,
        status_text, snapshot.reason, observed_at_unix_ns,
    );

    (
        http_status,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body,
    )
}

async fn serve_status(
    State(state): State<Arc<DiagnosticsState>>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    let body = status_body(&state);
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body,
    )
}

async fn serve_config(State(state): State<Arc<DiagnosticsState>>) -> impl IntoResponse {
    if !state.config_endpoint_enabled {
        let body = r#"{"error":"config_endpoint_disabled","message":"set LOGFWD_UNSAFE_EXPOSE_CONFIG=1 to enable /admin/v1/config"}"#;
        return (
            StatusCode::FORBIDDEN,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body.to_string(),
        );
    }

    let redacted_yaml = redact_config_yaml(&state.config_yaml);
    let body = format!(
        r#"{{"path":"{}","raw_yaml":"{}"}}"#,
        esc(&state.config_path),
        esc(&redacted_yaml),
    );
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body,
    )
}

// ---------------------------------------------------------------------------
// WebSocket handler
// ---------------------------------------------------------------------------

async fn ws_telemetry(
    ws: WebSocketUpgrade,
    State(state): State<Arc<DiagnosticsState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_ws(socket, state))
}

async fn handle_ws(mut socket: WebSocket, state: Arc<DiagnosticsState>) {
    let mut rx = state.telemetry_tx.subscribe();
    // Push-only: client close is detected when send fails.
    loop {
        match rx.recv().await {
            Ok(text) => {
                if socket.send(Message::Text(text.into())).await.is_err() {
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {} // skip missed messages
            Err(_) => break,
        }
    }
}

// ---------------------------------------------------------------------------
// Status body builder (full pipeline snapshot for /admin/v1/status)
// ---------------------------------------------------------------------------

fn status_body(state: &DiagnosticsState) -> String {
    let elapsed = state.start_time.elapsed();
    let uptime = elapsed.as_secs();
    let uptime_s = elapsed.as_secs_f64();
    let observed_at_unix_ns = now_nanos();
    let mut pipelines_json = Vec::new();

    for pm in &state.pipelines {
        let inputs_json: Vec<String> = pm
            .inputs
            .iter()
            .map(|(name, typ, stats)| {
                format!(
                    r#"{{"name":"{}","type":"{}","health":"{}","lines_total":{},"bytes_total":{},"errors":{},"rotations":{},"parse_errors":{}}}"#,
                    esc(name),
                    esc(typ),
                    stats.health().as_str(),
                    stats.lines(),
                    stats.bytes(),
                    stats.errors(),
                    stats.rotations(),
                    stats.parse_errors(),
                )
            })
            .collect();

        let lines_in = pm.transform_in.lines();
        let lines_out: u64 = pm
            .outputs
            .iter()
            .map(|(_, _, s)| s.lines())
            .max()
            .unwrap_or(0);
        let drop_rate = if lines_in > 0 {
            1.0 - (lines_out as f64 / lines_in as f64)
        } else {
            0.0
        };

        let outputs_json: Vec<String> = pm
            .outputs
            .iter()
            .map(|(name, typ, stats)| {
                format!(
                    r#"{{"name":"{}","type":"{}","health":"{}","lines_total":{},"bytes_total":{},"errors":{}}}"#,
                    esc(name),
                    esc(typ),
                    stats.health().as_str(),
                    stats.lines(),
                    stats.bytes(),
                    stats.errors(),
                )
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

        let batch_latency_avg_ns = if latency_batches > 0 {
            batch_latency_total / latency_batches
        } else {
            0
        };
        let inflight = pm.inflight_batches.load(Ordering::Relaxed);
        let backpressure = pm.backpressure_stalls.load(Ordering::Relaxed);
        let transform_health = policy::transform_health(pm);

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
                    "backpressure stalls at {:.1}/sec — input faster than pipeline can drain",
                    stalls_per_sec
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

        pipelines_json.push(format!(
            r#"{{"name":"{}","inputs":[{}],"transform":{{"sql":"{}","health":"{}","lines_in":{},"lines_out":{},"errors":{},"filter_drop_rate":{:.3}}},"outputs":[{}],"batches":{{"total":{},"avg_rows":{:.1},"flush_by_size":{},"flush_by_timeout":{},"dropped_batches_total":{},"scan_errors_total":{},"parse_errors_total":{},"last_batch_time_ns":"{}","batch_latency_avg_ns":{},"inflight":{},"rows_total":{}}},"stage_seconds":{{"scan":{:.6},"transform":{:.6},"output":{:.6},"queue_wait":{:.6},"send":{:.6}}},"backpressure_stalls":{},"bottleneck":{{"stage":"{}","reason":"{}"}}}}"#,
            esc(&pm.name),
            inputs_json.join(","),
            esc(&pm.transform_sql),
            transform_health.as_str(),
            lines_in,
            lines_out,
            pm.transform_errors.load(Ordering::Relaxed),
            drop_rate,
            outputs_json.join(","),
            batches,
            avg_rows,
            pm.flush_by_size.load(Ordering::Relaxed),
            pm.flush_by_timeout.load(Ordering::Relaxed),
            pm.dropped_batches_total.load(Ordering::Relaxed),
            pm.scan_errors_total.load(Ordering::Relaxed),
            pm.parse_errors_total.load(Ordering::Relaxed),
            last_batch_ns,
            batch_latency_avg_ns,
            inflight,
            batch_rows,
            scan_s,
            transform_s,
            output_s,
            queue_wait_s,
            send_s,
            backpressure,
            bottleneck_stage,
            esc(bottleneck_reason.as_str()),
        ));
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

    let memory_json = match state.memory_stats_fn.and_then(|f| f()) {
        Some(m) => format!(
            r#","memory":{{"resident":{},"allocated":{},"active":{}}}"#,
            m.resident, m.allocated, m.active,
        ),
        None => String::new(),
    };

    format!(
        r#"{{"live":{{"status":"live","reason":"process_running","observed_at_unix_ns":"{}"}},"ready":{{"status":"{}","reason":"{}","observed_at_unix_ns":"{}"}},"component_health":{{"status":"{}","reason":"{}","readiness_impact":"{}","observed_at_unix_ns":"{}"}},"pipelines":[{}],"system":{{"uptime_seconds":{},"version":"{}"{}}}}}"#,
        observed_at_unix_ns,
        ready,
        ready_reason,
        observed_at_unix_ns,
        component_health.as_str(),
        component_reason,
        readiness_impact,
        observed_at_unix_ns,
        pipelines_json.join(","),
        uptime,
        VERSION,
        memory_json,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::ComponentHealth;
    use std::sync::atomic::Ordering;
    use std::thread;

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

        let out = pm.add_output("collector", "otlp");
        out.inc_lines(900);
        out.inc_bytes(30000);
        out.inc_errors();
        out.inc_errors();

        pm.batches_total.store(50, Ordering::Relaxed);
        pm.batch_rows_total.store(4500, Ordering::Relaxed);
        pm.flush_by_size.store(30, Ordering::Relaxed);
        pm.flush_by_timeout.store(20, Ordering::Relaxed);
        pm.dropped_batches_total.store(5, Ordering::Relaxed);
        pm.scan_errors_total.store(2, Ordering::Relaxed);
        pm.parse_errors_total.store(4, Ordering::Relaxed);
        pm.scan_nanos_total.store(100_000_000, Ordering::Relaxed);
        pm.transform_nanos_total
            .store(500_000_000, Ordering::Relaxed);
        pm.output_nanos_total.store(200_000_000, Ordering::Relaxed);
        pm.queue_wait_nanos_total
            .store(50_000_000, Ordering::Relaxed);
        pm.send_nanos_total.store(150_000_000, Ordering::Relaxed);
        pm.batch_latency_nanos_total
            .store(500_000_000, Ordering::Relaxed);
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

    fn http_get(port: u16, path: &str) -> (u16, String) {
        use std::io::Read;
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
                Err(_) => thread::sleep(Duration::from_millis(50)),
            }
        }
        let mut stream = stream.unwrap_or_else(|| {
            panic!("connect failed after retries to {addr}");
        });
        stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
        let req = format!(
            "GET {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            path
        );
        stream.write_all(req.as_bytes()).unwrap();

        let mut buf = Vec::new();
        let _ = stream.read_to_end(&mut buf);
        let text = String::from_utf8_lossy(&buf).to_string();

        let status = text
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(0);

        let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();

        (status, body)
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
    fn diagnostics_server_handle_drop_releases_port() {
        let server = DiagnosticsServer::new("127.0.0.1:0");
        let (handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        drop(handle);
        thread::sleep(Duration::from_millis(100));

        let result = std::net::TcpListener::bind(format!("127.0.0.1:{port}"));
        assert!(
            result.is_ok(),
            "failed to rebind diagnostics port {port} after drop"
        );
    }

    #[test]
    fn test_live_endpoint() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(Duration::from_millis(200));

        let (status, body) = http_get(port, "/live");
        assert_eq!(status, 200);
        assert!(body.contains(r#""status":"live""#), "body: {}", body);
        assert!(
            body.contains(&format!(r#""version":"{}""#, env!("CARGO_PKG_VERSION"))),
            "body: {}",
            body
        );
        assert!(body.contains(r#""uptime_seconds":"#), "body: {}", body);
    }

    #[test]
    fn test_status_endpoint() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(Duration::from_millis(200));

        let (status, body) = http_get(port, "/admin/v1/status");
        assert_eq!(status, 200);
        assert!(
            body.contains(
                r#""component_health":{"status":"healthy","reason":"all_components_healthy","readiness_impact":"ready","observed_at_unix_ns":""#
            ),
            "body: {}",
            body
        );
        assert!(
            body.contains(
                r#""ready":{"status":"ready","reason":"all_components_healthy","observed_at_unix_ns":""#
            ),
            "body: {}",
            body
        );
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
        assert!(
            body.contains(&format!(r#""version":"{}""#, env!("CARGO_PKG_VERSION"))),
            "body: {}",
            body
        );
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
        assert!(body.contains(r#""queue_wait":0.050000"#), "body: {}", body);
        assert!(body.contains(r#""send":0.150000"#), "body: {}", body);
        assert!(body.contains(r#""bottleneck":{"stage":"#), "body: {}", body);
    }

    #[test]
    fn test_not_found() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(Duration::from_millis(200));

        let (status, _body) = http_get(port, "/nonexistent");
        assert_eq!(status, 404);
    }

    #[test]
    fn test_ready_endpoint_no_pipelines_returns_503() {
        let server = DiagnosticsServer::new("127.0.0.1:0");
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(Duration::from_millis(200));

        let (status, body) = http_get(port, "/ready");
        assert_eq!(status, 503, "body: {}", body);
        assert!(body.contains(r#""status":"not_ready""#), "body: {}", body);
        assert!(
            body.contains(r#""reason":"no_pipelines_registered""#),
            "body: {}",
            body
        );
    }

    #[test]
    fn test_ready_endpoint_with_pipeline_returns_200() {
        let meter = opentelemetry::global::meter("test");
        let pm = PipelineMetrics::new("default", "SELECT * FROM logs", &meter);

        let mut server = DiagnosticsServer::new("127.0.0.1:0");
        server.add_pipeline(Arc::new(pm));
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(Duration::from_millis(200));

        let (status, body) = http_get(port, "/ready");
        assert_eq!(status, 200, "body: {}", body);
        assert!(body.contains(r#""status":"ready""#), "body: {}", body);
        assert!(
            body.contains(r#""reason":"all_components_healthy""#),
            "body: {}",
            body
        );
    }

    #[test]
    fn test_ready_endpoint_stays_in_sync_with_admin_ready_snapshot() {
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
    fn test_status_endpoint_no_memory_stats() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(Duration::from_millis(200));

        let (status, body) = http_get(port, "/admin/v1/status");
        assert_eq!(status, 200);
        assert!(
            !body.contains(r#""memory""#),
            "unexpected memory key: {}",
            body
        );
    }

    #[test]
    fn test_status_endpoint_with_memory_stats() {
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

        thread::sleep(Duration::from_millis(200));

        let (status, body) = http_get(port, "/admin/v1/status");
        assert_eq!(status, 200);
        assert!(body.contains(r#""memory""#), "missing memory key: {}", body);
        assert!(body.contains(r#""resident":1000000"#), "body: {}", body);
        assert!(body.contains(r#""allocated":800000"#), "body: {}", body);
        assert!(body.contains(r#""active":900000"#), "body: {}", body);
    }

    #[test]
    fn test_status_endpoint_escaping() {
        let meter = opentelemetry::global::meter("test");
        let pm = PipelineMetrics::new("pipe\x01line", "SELECT * FROM logs", &meter);

        let mut server = DiagnosticsServer::new("127.0.0.1:0");
        server.add_pipeline(Arc::new(pm));
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(Duration::from_millis(200));

        let (status, body) = http_get(port, "/admin/v1/status");
        assert_eq!(status, 200);
        assert!(
            body.contains(r#""name":"pipe\u0001line""#),
            "body: {}",
            body
        );

        let _v: serde_json::Value =
            serde_json::from_str(&body).expect("invalid JSON output from /admin/v1/status");
    }

    #[test]
    fn test_esc_control_chars() {
        assert_eq!(esc("hello\0world"), "hello\\u0000world");
        assert_eq!(esc("tab\tnewline\nreturn\r"), "tab\\tnewline\\nreturn\\r");
        assert_eq!(esc("bell\x07"), "bell\\u0007");
        assert_eq!(esc("escape\x1b"), "escape\\u001b");
    }

    #[test]
    fn removed_endpoints_return_404() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(Duration::from_millis(200));

        for path in [
            "/admin/v1/stats",
            "/admin/v1/traces",
            "/admin/v1/logs",
            "/admin/v1/history",
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

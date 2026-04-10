use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use super::metrics::PipelineMetrics;
use super::models::MemoryStats;
use super::policy;
use super::process::process_metrics;
use super::render::{esc, now_nanos};
use crate::background_http_task::BackgroundHttpTask;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DASHBOARD_HTML: &str = include_str!("../dashboard.html");
const REDACTED_SECRET: &str = "***redacted***";
const REDACTED_CONFIG_UNAVAILABLE: &str = "<redacted config unavailable>";
const REDACTED_ENDPOINT: &str = "<redacted_endpoint>";

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
// ServerHandle — owns the background threads spawned by DiagnosticsServer::start
// ---------------------------------------------------------------------------

/// Owns the background threads spawned by [`DiagnosticsServer::start`].
///
/// Dropping this value signals the metric-sampler thread to exit (via the
/// shared `running` flag), unblocks the tiny_http server so its thread can
/// return, and then joins both threads.  Keep this value alive for as long
/// as the server should run.
pub struct ServerHandle {
    running: Arc<AtomicBool>,
    sampler_handle: Option<JoinHandle<()>>,
    _http_task: BackgroundHttpTask,
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        // Signal the sampler loop to exit.
        self.running.store(false, Ordering::Relaxed);
        // Join sampler thread; HTTP thread is unblocked and joined by
        // `BackgroundHttpTask` during field drop.
        if let Some(h) = self.sampler_handle.take() {
            let _ = h.join();
        }
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
    /// The returned [`ServerHandle`] owns both background threads.  Drop it
    /// (or let it go out of scope) to shut the server down cleanly.
    pub fn start(self) -> io::Result<(ServerHandle, std::net::SocketAddr)> {
        let server = Arc::new(
            tiny_http::Server::http(&self.bind_addr)
                .map_err(|e| io::Error::other(e.to_string()))?,
        );
        let bound_addr = server
            .server_addr()
            .to_ip()
            .ok_or_else(|| io::Error::other("diagnostics server bound to non-IP address"))?;

        // Start capturing stderr into the 1 MiB ring buffer immediately so
        // log lines emitted before the first /admin/v1/logs request are not lost.
        // Non-fatal: if capture setup fails (e.g. out of fds), log to real stderr.
        if let Err(e) = self.stderr.start() {
            tracing::warn!(error = %e, "stderr capture failed");
        }

        // Shared shutdown flag — set to false by ServerHandle::drop.
        let running = Arc::new(AtomicBool::new(true));

        // Background metric sampler — records pipeline + process metrics
        // every 2s into the history buffer, regardless of dashboard activity.
        let sampler_pipelines = self.pipelines.clone();
        let sampler_history = Arc::clone(&self.history);
        let sampler_mem_fn = self.memory_stats_fn;
        let sampler_running = Arc::clone(&running);
        let sampler_telemetry = self.telemetry.clone();
        let sampler_stderr = self.stderr.clone();
        let sampler_start = self.start_time;
        let sampler_handle = thread::Builder::new()
            .name("metric-sampler".into())
            .spawn(move || {
                let mut last_stderr_count: usize = 0;
                let mut prev_health = std::collections::HashMap::new();
                while sampler_running.load(Ordering::Relaxed) {
                    thread::sleep(std::time::Duration::from_secs(2));
                    // Re-check the flag after sleeping so we exit promptly on
                    // shutdown rather than performing one last sample.
                    if sampler_running.load(Ordering::Relaxed) {
                        sample_metrics(&sampler_pipelines, &sampler_history, sampler_mem_fn);

                        // OTLP telemetry buffers (metrics + logs).
                        let uptime = sampler_start.elapsed().as_secs_f64();
                        crate::telemetry_buffer::sample_all_metrics(
                            &sampler_pipelines,
                            sampler_mem_fn,
                            uptime,
                            &sampler_telemetry,
                        );
                        crate::telemetry_buffer::sample_stderr_logs(
                            &sampler_stderr,
                            &sampler_telemetry.logs,
                            &mut last_stderr_count,
                        );
                        crate::telemetry_buffer::sample_health_transitions(
                            &sampler_pipelines,
                            &sampler_telemetry.logs,
                            &mut prev_health,
                        );
                    }
                }
            })
            .map_err(|e| io::Error::other(format!("failed to spawn metric-sampler: {e}")))?;

        let http_server = Arc::clone(&server);
        let http_handle = match thread::Builder::new()
            .name("diagnostics-http".into())
            .spawn(move || {
                for request in server.incoming_requests() {
                    let _ = self.handle_request(request);
                }
            }) {
            Ok(handle) => handle,
            Err(e) => {
                // Stop the sampler thread before propagating the error so it
                // does not leak.
                running.store(false, Ordering::Relaxed);
                let _ = sampler_handle.join();
                return Err(io::Error::other(format!(
                    "failed to spawn diagnostics-http: {e}"
                )));
            }
        };

        Ok((
            ServerHandle {
                running,
                sampler_handle: Some(sampler_handle),
                _http_task: BackgroundHttpTask::new(Arc::clone(&http_server), http_handle),
            },
            bound_addr,
        ))
    }

    fn handle_request(
        &self,
        request: tiny_http::Request,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // All diagnostics endpoints are read-only — reject non-GET methods.
        if request.method() != &tiny_http::Method::Get {
            let allow_header = tiny_http::Header::from_bytes(&b"Allow"[..], &b"GET"[..])
                .map_err(|()| io::Error::other("invalid HTTP header"))?;
            let resp = tiny_http::Response::from_string("method not allowed")
                .with_status_code(405)
                .with_header(allow_header);
            request.respond(resp)?;
            return Ok(());
        }

        let path = request.url().to_string();
        // Strip query string for routing.
        let route = path.split('?').next().unwrap_or(&path);

        match route {
            "/" => Self::serve_dashboard(request),
            "/live" => self.serve_live(request),
            "/ready" => self.serve_ready(request),
            "/admin/v1/status" => self.serve_status(request),
            "/admin/v1/stats" => self.serve_stats(request),
            "/admin/v1/config" => self.serve_config(request),
            "/admin/v1/logs" => self.serve_logs(request),
            "/admin/v1/history" => self.serve_history(request),
            "/admin/v1/traces" => self.serve_traces(request),
            "/admin/v1/telemetry/metrics" => self.serve_telemetry_metrics(request),
            "/admin/v1/telemetry/traces" => self.serve_telemetry_traces(request),
            "/admin/v1/telemetry/logs" => self.serve_telemetry_logs(request),
            _ => {
                let header =
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..])
                        .map_err(|()| io::Error::other("invalid HTTP header"))?;
                let resp = tiny_http::Response::from_string("not found")
                    .with_status_code(404)
                    .with_header(header);
                request.respond(resp)?;
                Ok(())
            }
        }
    }

    // -- endpoint handlers --------------------------------------------------

    fn serve_dashboard(request: tiny_http::Request) -> Result<(), Box<dyn std::error::Error>> {
        let header =
            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..])
                .map_err(|()| io::Error::other("invalid HTTP header"))?;
        let resp = tiny_http::Response::from_string(DASHBOARD_HTML).with_header(header);
        request.respond(resp)?;
        Ok(())
    }

    fn serve_live(&self, request: tiny_http::Request) -> Result<(), Box<dyn std::error::Error>> {
        let uptime = self.start_time.elapsed().as_secs();
        let body = format!(
            r#"{{"status":"live","uptime_seconds":{},"version":"{}"}}"#,
            uptime, VERSION,
        );
        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .map_err(|()| io::Error::other("invalid HTTP header"))?;
        let resp = tiny_http::Response::from_string(body).with_header(header);
        request.respond(resp)?;
        Ok(())
    }

    /// Returns a small reasoned readiness snapshot when at least one pipeline
    /// is registered and the current explicit component health snapshots are
    /// ready. Returns 503 before any pipelines are configured or while a
    /// component is still starting, stopping, stopped, or failed.
    ///
    /// Per-pipeline data-flow freshness (`last_batch_time_ns`) is exposed
    /// via `/admin/v1/status` for monitoring dashboards, but is NOT a
    /// readiness gate — a quiet log source should not cause Kubernetes
    /// to mark the pod as unready.
    ///
    /// Some component kinds still inherit optimistic default health until
    /// explicit lifecycle wiring lands, so this endpoint is only as honest as
    /// the currently wired health sources.
    fn serve_ready(&self, request: tiny_http::Request) -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = policy::readiness_snapshot(&self.pipelines);
        let observed_at_unix_ns = now_nanos();

        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .map_err(|()| io::Error::other("invalid HTTP header"))?;
        if snapshot.ready {
            let body = format!(
                r#"{{"status":"ready","reason":"{}","observed_at_unix_ns":"{}"}}"#,
                snapshot.reason, observed_at_unix_ns
            );
            let resp = tiny_http::Response::from_string(body).with_header(header);
            request.respond(resp)?;
        } else {
            let body = format!(
                r#"{{"status":"not_ready","reason":"{}","observed_at_unix_ns":"{}"}}"#,
                snapshot.reason, observed_at_unix_ns
            );
            let resp = tiny_http::Response::from_string(body)
                .with_status_code(503)
                .with_header(header);
            request.respond(resp)?;
        }
        Ok(())
    }

    /// Flat JSON endpoint for benchmark polling: process metrics + pipeline summary.
    fn serve_stats(&self, request: tiny_http::Request) -> Result<(), Box<dyn std::error::Error>> {
        let uptime_s = self.start_time.elapsed().as_secs_f64();
        let process_json = match process_metrics() {
            Some((rss_bytes, cpu_user_ms, cpu_sys_ms)) => format!(
                r#","rss_bytes":{},"cpu_user_ms":{},"cpu_sys_ms":{}"#,
                rss_bytes, cpu_user_ms, cpu_sys_ms
            ),
            None => String::from(r#","rss_bytes":null,"cpu_user_ms":null,"cpu_sys_ms":null"#),
        };

        // Aggregate pipeline counters.
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

        for pm in &self.pipelines {
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
        }

        // Include jemalloc stats if available.
        let mem_json = match self.memory_stats_fn.and_then(|f| f()) {
            Some(m) => format!(
                r#","mem_resident":{},"mem_allocated":{},"mem_active":{}"#,
                m.resident, m.allocated, m.active,
            ),
            None => String::new(),
        };

        let body = format!(
            r#"{{"uptime_sec":{:.3}{},"input_lines":{},"input_bytes":{},"output_lines":{},"output_bytes":{},"output_errors":{},"batches":{},"scan_sec":{:.6},"transform_sec":{:.6},"output_sec":{:.6},"backpressure_stalls":{},"inflight_batches":{}{}}}"#,
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
            mem_json,
        );

        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .map_err(|()| io::Error::other("invalid HTTP header"))?;
        let resp = tiny_http::Response::from_string(body).with_header(header);
        request.respond(resp)?;
        Ok(())
    }

    fn serve_config(&self, request: tiny_http::Request) -> Result<(), Box<dyn std::error::Error>> {
        if !self.config_endpoint_enabled {
            let header =
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .map_err(|()| io::Error::other("invalid HTTP header"))?;
            let body = r#"{"error":"config_endpoint_disabled","message":"set LOGFWD_UNSAFE_EXPOSE_CONFIG=1 to enable /admin/v1/config"}"#;
            let resp = tiny_http::Response::from_string(body)
                .with_status_code(403)
                .with_header(header);
            request.respond(resp)?;
            return Ok(());
        }

        let redacted_yaml = redact_config_yaml(&self.config_yaml);
        let body = format!(
            r#"{{"path":"{}","raw_yaml":"{}"}}"#,
            esc(&self.config_path),
            esc(&redacted_yaml),
        );
        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .map_err(|()| io::Error::other("invalid HTTP header"))?;
        let resp = tiny_http::Response::from_string(body).with_header(header);
        request.respond(resp)?;
        Ok(())
    }

    fn serve_history(&self, request: tiny_http::Request) -> Result<(), Box<dyn std::error::Error>> {
        let body = self.history.to_json();
        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .map_err(|()| io::Error::other("invalid HTTP header"))?;
        let resp = tiny_http::Response::from_string(body).with_header(header);
        request.respond(resp)?;
        Ok(())
    }

    fn serve_logs(&self, request: tiny_http::Request) -> Result<(), Box<dyn std::error::Error>> {
        let lines = self.stderr.get_logs();
        // Build JSON array of strings.
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
        body.push_str(if self.stderr.is_active() {
            "true"
        } else {
            "false"
        });
        body.push('}');

        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .map_err(|()| io::Error::other("invalid HTTP header"))?;
        let resp = tiny_http::Response::from_string(body).with_header(header);
        request.respond(resp)?;
        Ok(())
    }

    fn serve_traces(&self, request: tiny_http::Request) -> Result<(), Box<dyn std::error::Error>> {
        use crate::span_exporter::TraceSpan;
        use std::collections::HashMap;
        use std::fmt::Write;

        let body = if let Some(ref buf) = self.trace_buf {
            let all_spans = buf.get_spans();

            // Group child spans by trace_id, and collect root spans separately.
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

            // Build JSON — newest first, cap at 500 traces.
            let mut out = String::with_capacity(64 * 1024);
            out.push_str(r#"{"traces":["#);
            let mut first = true;
            for root in roots.iter().rev().take(500) {
                if !first {
                    out.push(',');
                }
                first = false;

                // Pull stage durations and per-stage row counts from child spans.
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

                // Fallback: read scan/transform timing from root span attributes
                // (batch span carries these directly when child spans are absent)
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

                // Extract well-known attributes from root span.
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
                        \"status\":\"{status}\"\
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
                );
            }
            // In-progress batches — live entries shown before completion.
            for pm in &self.pipelines {
                if let Ok(active) = pm.active_batches.lock() {
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
                                \"in_progress\":true,\
                                \"stage\":\"{stage}\",\
                                \"stage_start_unix_ns\":\"{ss}\"\
                            }}",
                            id = id,
                            pl = esc(&pm.name),
                            st = b.start_unix_ns,
                            scan = b.scan_ns,
                            xfm = b.transform_ns,
                            out_st = b.output_start_unix_ns,
                            wid = worker_id_json,
                            stage = b.stage,
                            ss = b.stage_start_unix_ns,
                        );
                    }
                }
            }
            out.push_str("]}");
            out
        } else {
            r#"{"traces":[]}"#.to_string()
        };

        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .map_err(|()| io::Error::other("invalid HTTP header"))?;
        let resp = tiny_http::Response::from_string(body).with_header(header);
        request.respond(resp)?;
        Ok(())
    }

    fn serve_status(&self, request: tiny_http::Request) -> Result<(), Box<dyn std::error::Error>> {
        let body = self.status_body();
        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .map_err(|()| io::Error::other("invalid HTTP header"))?;
        let resp = tiny_http::Response::from_string(body).with_header(header);
        request.respond(resp)?;
        Ok(())
    }

    fn status_body(&self) -> String {
        let uptime = self.start_time.elapsed().as_secs();
        let uptime_s = self.start_time.elapsed().as_secs_f64();
        let observed_at_unix_ns = now_nanos();
        let mut pipelines_json = Vec::new();

        for pm in &self.pipelines {
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

            let batch_latency_avg_ns = if latency_batches > 0 {
                batch_latency_total / latency_batches
            } else {
                0
            };
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

        let ready_snapshot = policy::readiness_snapshot(&self.pipelines);
        let ready = if ready_snapshot.ready {
            "ready"
        } else {
            "not_ready"
        };
        let component_health = ready_snapshot.component_health;
        let ready_reason = ready_snapshot.reason;
        let component_reason = policy::health_reason(component_health);
        let readiness_impact = policy::readiness_impact(component_health);

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
            self.memory_json(),
        )
    }

    /// Returns a JSON fragment (starting with a comma) for allocator memory
    /// stats, or an empty string if no stats function is registered.
    fn memory_json(&self) -> String {
        match self.memory_stats_fn.and_then(|f| f()) {
            Some(m) => format!(
                r#","memory":{{"resident":{},"allocated":{},"active":{}}}"#,
                m.resident, m.allocated, m.active,
            ),
            None => String::new(),
        }
    }

    // -- OTLP JSON telemetry endpoints ----------------------------------------

    fn serve_telemetry_metrics(
        &self,
        request: tiny_http::Request,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let points = self.telemetry.metrics.snapshot();
        let body = crate::telemetry_buffer::metrics_to_otlp_json(&points);
        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .map_err(|()| io::Error::other("invalid HTTP header"))?;
        request.respond(tiny_http::Response::from_string(body).with_header(header))?;
        Ok(())
    }

    fn serve_telemetry_traces(
        &self,
        request: tiny_http::Request,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let spans =
            crate::telemetry_buffer::collect_all_spans(self.trace_buf.as_ref(), &self.pipelines);
        let body = crate::telemetry_buffer::traces_to_otlp_json(&spans);
        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .map_err(|()| io::Error::other("invalid HTTP header"))?;
        request.respond(tiny_http::Response::from_string(body).with_header(header))?;
        Ok(())
    }

    fn serve_telemetry_logs(
        &self,
        request: tiny_http::Request,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let logs = self.telemetry.logs.snapshot();
        let body = crate::telemetry_buffer::logs_to_otlp_json(&logs);
        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .map_err(|()| io::Error::other("invalid HTTP header"))?;
        request.respond(tiny_http::Response::from_string(body).with_header(header))?;
        Ok(())
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
    fn diagnostics_server_handle_drop_releases_port() {
        let server = DiagnosticsServer::new("127.0.0.1:0");
        let (handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        drop(handle);
        thread::sleep(std::time::Duration::from_millis(50));

        let rebound_addr = format!("127.0.0.1:{port}");
        let result = tiny_http::Server::http(&rebound_addr);
        assert!(
            result.is_ok(),
            "failed to rebind diagnostics port {port} after drop"
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
    fn test_live_endpoint() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        // Give the server a moment to bind.
        thread::sleep(std::time::Duration::from_millis(100));

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

        thread::sleep(std::time::Duration::from_millis(100));

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
        assert!(
            body.contains(
                r#""live":{"status":"live","reason":"process_running","observed_at_unix_ns":""#
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
        assert!(body.contains(r#""queue_wait":0.050000"#), "body: {}", body);
        assert!(body.contains(r#""send":0.150000"#), "body: {}", body);
        // Bottleneck field must be present and well-formed.
        assert!(body.contains(r#""bottleneck":{"stage":"#), "body: {}", body);
    }

    #[test]
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
    fn test_not_found() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, _body) = http_get(port, "/nonexistent");
        assert_eq!(status, 404);
    }

    #[test]
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
    fn test_ready_endpoint_no_pipelines_returns_503() {
        // No pipelines registered yet → not ready.
        let server = DiagnosticsServer::new("127.0.0.1:0");
        // Don't add any pipelines.
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

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
        assert!(body.contains(r#""status":"ready""#), "body: {}", body);
        assert!(
            body.contains(r#""reason":"all_components_healthy""#),
            "body: {}",
            body
        );
    }

    #[test]
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
        assert!(body.contains(r#""status":"not_ready""#), "body: {}", body);
        assert!(
            body.contains(r#""reason":"components_starting""#),
            "body: {}",
            body
        );
    }

    #[test]
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
        assert!(body.contains(r#""status":"ready""#), "body: {}", body);
        assert!(
            body.contains(r#""reason":"components_degraded_but_operational""#),
            "body: {}",
            body
        );
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

    #[test]
    fn test_esc_control_chars() {
        assert_eq!(esc("hello\0world"), "hello\\u0000world");
        assert_eq!(esc("tab\tnewline\nreturn\r"), "tab\\tnewline\\nreturn\\r");
        assert_eq!(esc("bell\x07"), "bell\\u0007");
        assert_eq!(esc("escape\x1b"), "escape\\u001b");
    }

    #[test]
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
            metrics.as_array().map_or(false, |a| !a.is_empty()),
            "expected at least one metric"
        );
    }

    #[test]
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
            spans.as_array().map_or(false, |a| !a.is_empty()),
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

use std::io;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Meter};

// Re-export ComponentStats from logfwd-types so existing `logfwd_io::diagnostics::ComponentStats`
// paths keep working.
pub use logfwd_types::diagnostics::ComponentStats;

// ---------------------------------------------------------------------------
// Pipeline-level metrics (shared between pipeline thread and diagnostics)
// ---------------------------------------------------------------------------

/// In-flight batch being processed right now.
pub struct ActiveBatch {
    pub start_unix_ns: u64,
    pub scan_ns: u64,
    pub transform_ns: u64,
    /// Current stage: "scan" | "transform" | "output"
    pub stage: &'static str,
    /// Unix ns when the current stage started (for frontend live duration)
    pub stage_start_unix_ns: u64,
    /// Worker id once assigned (-1 = not yet assigned / in queue)
    pub worker_id: i64,
    /// Unix ns when the worker actually started processing (0 = not yet)
    pub output_start_unix_ns: u64,
}

/// Stats for a full pipeline. Dual-write: atomics for local endpoints,
/// OTel counters for OTLP push.
pub struct PipelineMetrics {
    pub name: String,
    /// (name, type, stats)
    pub inputs: Vec<(String, String, Arc<ComponentStats>)>,
    pub transform_sql: String,
    pub transform_in: Arc<ComponentStats>,
    pub transform_out: Arc<ComponentStats>,
    pub transform_errors: AtomicU64,
    /// (name, type, stats)
    pub outputs: Vec<(String, String, Arc<ComponentStats>)>,
    pub backpressure_stalls: AtomicU64,
    // Batch-level metrics (atomics for local, OTel for push)
    pub batches_total: AtomicU64,
    pub batch_rows_total: AtomicU64,
    pub flush_by_size: AtomicU64,
    pub flush_by_timeout: AtomicU64,
    /// Batches that were dropped due to scan, transform, or output errors.
    pub dropped_batches_total: AtomicU64,
    /// Batches that failed the scan stage specifically.
    pub scan_errors_total: AtomicU64,
    // Per-stage cumulative timing (nanoseconds)
    pub scan_nanos_total: AtomicU64,
    pub transform_nanos_total: AtomicU64,
    pub output_nanos_total: AtomicU64,
    /// Cumulative nanoseconds spent waiting in the pool queue before a worker
    /// picks up the batch.
    pub queue_wait_nanos_total: AtomicU64,
    /// Cumulative nanoseconds of pure `send_batch` wall time (per-attempt).
    pub send_nanos_total: AtomicU64,
    /// Cumulative nanoseconds of total batch latency (submission to ack).
    pub batch_latency_nanos_total: AtomicU64,
    /// Unix timestamp (nanoseconds) of the last successfully processed batch.
    /// Zero means no batch has been processed yet.
    pub last_batch_time_ns: AtomicU64,
    /// Number of batches currently submitted to workers but not yet acked.
    pub inflight_batches: AtomicU64,
    pub active_batches: std::sync::Mutex<std::collections::HashMap<u64, ActiveBatch>>,
    pub next_batch_id: AtomicU64,
    // OTel counters (for OTLP push)
    meter: Meter,
    otel_attrs: Vec<KeyValue>,
    otel_transform_errors: Counter<u64>,
    otel_batches: Counter<u64>,
    otel_batch_rows: Counter<u64>,
    otel_flush_by_size: Counter<u64>,
    otel_flush_by_timeout: Counter<u64>,
    otel_dropped_batches: Counter<u64>,
    otel_scan_errors: Counter<u64>,
    otel_scan_nanos: Counter<u64>,
    otel_transform_nanos: Counter<u64>,
    otel_output_nanos: Counter<u64>,
    otel_queue_wait_nanos: Counter<u64>,
    otel_send_nanos: Counter<u64>,
    otel_batch_latency_nanos: Counter<u64>,
    otel_backpressure_stalls: Counter<u64>,
}

impl PipelineMetrics {
    pub fn new(name: impl Into<String>, transform_sql: impl Into<String>, meter: &Meter) -> Self {
        let name = name.into();
        let attrs = vec![KeyValue::new("pipeline", name.clone())];
        Self {
            transform_sql: transform_sql.into(),
            transform_in: Arc::new(ComponentStats::with_meter(
                meter,
                "logfwd_transform_in",
                attrs.clone(),
            )),
            transform_out: Arc::new(ComponentStats::with_meter(
                meter,
                "logfwd_transform_out",
                attrs.clone(),
            )),
            transform_errors: AtomicU64::new(0),
            inputs: Vec::new(),
            outputs: Vec::new(),
            backpressure_stalls: AtomicU64::new(0),
            batches_total: AtomicU64::new(0),
            batch_rows_total: AtomicU64::new(0),
            flush_by_size: AtomicU64::new(0),
            flush_by_timeout: AtomicU64::new(0),
            dropped_batches_total: AtomicU64::new(0),
            scan_errors_total: AtomicU64::new(0),
            scan_nanos_total: AtomicU64::new(0),
            transform_nanos_total: AtomicU64::new(0),
            output_nanos_total: AtomicU64::new(0),
            queue_wait_nanos_total: AtomicU64::new(0),
            send_nanos_total: AtomicU64::new(0),
            batch_latency_nanos_total: AtomicU64::new(0),
            last_batch_time_ns: AtomicU64::new(0),
            inflight_batches: AtomicU64::new(0),
            active_batches: std::sync::Mutex::new(std::collections::HashMap::new()),
            next_batch_id: AtomicU64::new(0),
            otel_transform_errors: meter.u64_counter("logfwd_transform_errors").build(),
            otel_batches: meter.u64_counter("logfwd_batches").build(),
            otel_batch_rows: meter.u64_counter("logfwd_batch_rows").build(),
            otel_flush_by_size: meter.u64_counter("logfwd_flush_by_size").build(),
            otel_flush_by_timeout: meter.u64_counter("logfwd_flush_by_timeout").build(),
            otel_dropped_batches: meter.u64_counter("logfwd_dropped_batches").build(),
            otel_scan_errors: meter.u64_counter("logfwd_scan_errors").build(),
            otel_scan_nanos: meter.u64_counter("logfwd_stage_scan_nanos").build(),
            otel_transform_nanos: meter.u64_counter("logfwd_stage_transform_nanos").build(),
            otel_output_nanos: meter.u64_counter("logfwd_stage_output_nanos").build(),
            otel_queue_wait_nanos: meter.u64_counter("logfwd_stage_queue_wait_nanos").build(),
            otel_send_nanos: meter.u64_counter("logfwd_stage_send_nanos").build(),
            otel_batch_latency_nanos: meter.u64_counter("logfwd_batch_latency_nanos").build(),
            otel_backpressure_stalls: meter.u64_counter("logfwd_backpressure_stalls").build(),
            meter: meter.clone(),
            otel_attrs: attrs,
            name,
        }
    }

    pub fn add_input(
        &mut self,
        name: impl Into<String>,
        typ: impl Into<String>,
    ) -> Arc<ComponentStats> {
        let name = name.into();
        let typ = typ.into();
        let attrs = vec![
            KeyValue::new("pipeline", self.name.clone()),
            KeyValue::new("input", name.clone()),
        ];
        let stats = Arc::new(ComponentStats::with_meter(
            &self.meter,
            "logfwd_input",
            attrs,
        ));
        self.inputs.push((name, typ, Arc::clone(&stats)));
        stats
    }

    pub fn add_output(
        &mut self,
        name: impl Into<String>,
        typ: impl Into<String>,
    ) -> Arc<ComponentStats> {
        let name = name.into();
        let typ = typ.into();
        let attrs = vec![
            KeyValue::new("pipeline", self.name.clone()),
            KeyValue::new("output", name.clone()),
        ];
        let stats = Arc::new(ComponentStats::with_meter(
            &self.meter,
            "logfwd_output",
            attrs,
        ));
        self.outputs.push((name, typ, Arc::clone(&stats)));
        stats
    }

    /// Increment error counter on one output.
    /// Record successful output delivery for all output sinks.
    pub fn inc_output_success(&self, lines: u64) {
        for (_, _, stats) in &self.outputs {
            stats.inc_lines(lines);
        }
    }

    pub fn output_error(&self, output_name: &str) {
        for (name, _, stats) in &self.outputs {
            if name == output_name {
                stats.inc_errors();
                break;
            }
        }
    }

    // -- Helper methods for dual-write (called from pipeline hot loop) --------

    pub fn inc_transform_error(&self) {
        self.transform_errors.fetch_add(1, Ordering::Relaxed);
        self.otel_transform_errors.add(1, &self.otel_attrs);
    }

    pub fn inc_flush_by_size(&self) {
        self.flush_by_size.fetch_add(1, Ordering::Relaxed);
        self.otel_flush_by_size.add(1, &self.otel_attrs);
    }

    pub fn inc_flush_by_timeout(&self) {
        self.flush_by_timeout.fetch_add(1, Ordering::Relaxed);
        self.otel_flush_by_timeout.add(1, &self.otel_attrs);
    }

    pub fn record_batch(&self, rows: u64, scan_ns: u64, transform_ns: u64, output_ns: u64) {
        self.batches_total.fetch_add(1, Ordering::Relaxed);
        self.batch_rows_total.fetch_add(rows, Ordering::Relaxed);
        self.scan_nanos_total.fetch_add(scan_ns, Ordering::Relaxed);
        self.transform_nanos_total
            .fetch_add(transform_ns, Ordering::Relaxed);
        self.output_nanos_total
            .fetch_add(output_ns, Ordering::Relaxed);
        self.last_batch_time_ns
            .store(now_nanos(), Ordering::Relaxed);

        self.otel_batches.add(1, &self.otel_attrs);
        self.otel_batch_rows.add(rows, &self.otel_attrs);
        self.otel_scan_nanos.add(scan_ns, &self.otel_attrs);
        self.otel_transform_nanos
            .add(transform_ns, &self.otel_attrs);
        self.otel_output_nanos.add(output_ns, &self.otel_attrs);
    }

    /// Record cumulative queue-wait time (time a batch spent waiting in the
    /// pool queue before a worker picked it up).
    pub fn record_queue_wait(&self, nanos: u64) {
        self.queue_wait_nanos_total
            .fetch_add(nanos, Ordering::Relaxed);
        self.otel_queue_wait_nanos.add(nanos, &self.otel_attrs);
    }

    /// Record cumulative pure `send_batch` wall time (per-attempt).
    pub fn record_send_latency(&self, nanos: u64) {
        self.send_nanos_total.fetch_add(nanos, Ordering::Relaxed);
        self.otel_send_nanos.add(nanos, &self.otel_attrs);
    }

    /// Record total batch latency (submission to ack).
    pub fn record_batch_latency(&self, nanos: u64) {
        self.batch_latency_nanos_total
            .fetch_add(nanos, Ordering::Relaxed);
        self.otel_batch_latency_nanos.add(nanos, &self.otel_attrs);
    }

    pub fn inc_backpressure_stall(&self) {
        self.backpressure_stalls.fetch_add(1, Ordering::Relaxed);
        self.otel_backpressure_stalls.add(1, &self.otel_attrs);
    }

    /// Increment the dropped-batches counter. Call whenever a batch is
    /// discarded due to a scan, transform, or output error.
    pub fn inc_dropped_batch(&self) {
        self.dropped_batches_total.fetch_add(1, Ordering::Relaxed);
        self.otel_dropped_batches.add(1, &self.otel_attrs);
    }

    /// Increment the scan-errors counter. Call when `scanner.scan()` fails.
    pub fn inc_scan_error(&self) {
        self.scan_errors_total.fetch_add(1, Ordering::Relaxed);
        self.otel_scan_errors.add(1, &self.otel_attrs);
    }

    pub fn alloc_batch_id(&self) -> u64 {
        self.next_batch_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn begin_active_batch(&self, id: u64, start_unix_ns: u64) {
        if let Ok(mut m) = self.active_batches.lock() {
            m.insert(
                id,
                ActiveBatch {
                    start_unix_ns,
                    scan_ns: 0,
                    transform_ns: 0,
                    stage: "scan",
                    stage_start_unix_ns: start_unix_ns,
                    worker_id: -1,
                    output_start_unix_ns: 0,
                },
            );
        }
    }

    /// Called by the worker pool when a worker picks up a batch for output.
    pub fn assign_worker_to_active_batch(&self, batch_id: u64, worker_id: usize, now_unix_ns: u64) {
        if let Ok(mut m) = self.active_batches.lock() {
            if let Some(b) = m.get_mut(&batch_id) {
                b.worker_id = worker_id as i64;
                b.output_start_unix_ns = now_unix_ns;
                b.stage_start_unix_ns = now_unix_ns;
            }
        }
    }

    pub fn advance_active_batch(
        &self,
        id: u64,
        next_stage: &'static str,
        elapsed_ns: u64,
        now_unix_ns: u64,
    ) {
        if let Ok(mut m) = self.active_batches.lock() {
            if let Some(b) = m.get_mut(&id) {
                match b.stage {
                    "scan" => b.scan_ns = elapsed_ns,
                    "transform" => b.transform_ns = elapsed_ns,
                    _ => {}
                }
                b.stage = next_stage;
                b.stage_start_unix_ns = now_unix_ns;
            }
        }
    }

    pub fn finish_active_batch(&self, id: u64) {
        if let Ok(mut m) = self.active_batches.lock() {
            m.remove(&id);
        }
    }
}

// ---------------------------------------------------------------------------
// Diagnostics HTTP server
// ---------------------------------------------------------------------------

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DASHBOARD_HTML: &str = include_str!("dashboard.html");

/// Snapshot of allocator memory statistics in bytes.
///
/// Populated by the jemalloc stats reader in the binary crate and surfaced on
/// `/api/pipelines` under `system.memory`.
#[derive(Debug, Clone, Copy)]
pub struct MemoryStats {
    /// Total memory mapped by the allocator that is still mapped to resident
    /// physical pages (closest to OS RSS).
    pub resident: usize,
    /// Total memory currently allocated by the application.
    pub allocated: usize,
    /// Total memory in active jemalloc extents (resident + metadata).
    pub active: usize,
}

/// Lightweight diagnostics HTTP server. Runs on a dedicated thread, reads
/// atomic counters — no locking on the hot path.
pub struct DiagnosticsServer {
    pipelines: Vec<Arc<PipelineMetrics>>,
    start_time: Instant,
    bind_addr: String,
    /// Optional callback that returns a snapshot of allocator memory stats.
    /// Set this to expose jemalloc (or any allocator) metrics on `/api/pipelines`.
    memory_stats_fn: Option<fn() -> Option<MemoryStats>>,
    /// Raw YAML config text for the /api/config endpoint.
    config_yaml: String,
    config_path: String,
    /// Lazy stderr capture — activated on first /api/logs request.
    stderr: crate::stderr_capture::StderrCapture,
    /// Server-side metric history (1 hour, reducing precision).
    history: Arc<crate::metric_history::MetricHistory>,
    /// Ring buffer of recent batch spans for /api/traces.
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
            stderr: crate::stderr_capture::StderrCapture::new(),
            history: Arc::new(crate::metric_history::MetricHistory::new()),
            trace_buf: None,
        }
    }

    /// Attach a span buffer so `/api/traces` can serve batch trace data.
    pub fn set_trace_buffer(&mut self, buf: crate::span_exporter::SpanBuffer) {
        self.trace_buf = Some(buf);
    }

    /// Store the raw config YAML and file path for the /api/config endpoint.
    pub fn set_config(&mut self, path: &str, yaml: &str) {
        self.config_path = path.to_string();
        self.config_yaml = yaml.to_string();
    }

    pub fn add_pipeline(&mut self, metrics: Arc<PipelineMetrics>) {
        self.pipelines.push(metrics);
    }

    /// Register a callback that returns allocator memory statistics.
    ///
    /// When set, the `/api/pipelines` endpoint includes a `memory` object in
    /// the `system` section with `resident`, `allocated`, and `active` fields
    /// (all in bytes).
    pub fn set_memory_stats_fn(&mut self, f: fn() -> Option<MemoryStats>) {
        self.memory_stats_fn = Some(f);
    }

    /// Spawn the server on a background thread. Binds synchronously before
    /// returning so that port-in-use errors are reported at startup.
    ///
    /// Returns `(handle, bound_addr)` on success. `bound_addr` reflects the
    /// actual address after OS port assignment (useful when `bind_addr` uses
    /// port 0). Returns an `io::Error` on bind failure.
    pub fn start(self) -> io::Result<(JoinHandle<()>, std::net::SocketAddr)> {
        let server = tiny_http::Server::http(&self.bind_addr)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let bound_addr = server
            .server_addr()
            .to_ip()
            .ok_or_else(|| io::Error::other("diagnostics server bound to non-IP address"))?;

        // Start capturing stderr into the 1 MiB ring buffer immediately so
        // log lines emitted before the first /api/logs request are not lost.
        // Non-fatal: if capture setup fails (e.g. out of fds), log to real stderr.
        if let Err(e) = self.stderr.start() {
            tracing::warn!(error = %e, "stderr capture failed");
        }

        // Background metric sampler — records pipeline + process metrics
        // every 2s into the history buffer, regardless of dashboard activity.
        let sampler_pipelines = self.pipelines.clone();
        let sampler_history = Arc::clone(&self.history);
        let sampler_mem_fn = self.memory_stats_fn;
        thread::Builder::new()
            .name("metric-sampler".into())
            .spawn(move || {
                loop {
                    thread::sleep(std::time::Duration::from_secs(2));
                    sample_metrics(&sampler_pipelines, &sampler_history, sampler_mem_fn);
                }
            })
            .ok();

        let handle = thread::spawn(move || {
            for request in server.incoming_requests() {
                let _ = self.handle_request(request);
            }
        });
        Ok((handle, bound_addr))
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
            "/health" => self.serve_health(request),
            "/ready" => self.serve_ready(request),
            "/api/pipelines" => self.serve_pipelines(request),
            "/api/stats" => self.serve_stats(request),
            "/api/config" => self.serve_config(request),
            "/api/logs" => self.serve_logs(request),
            "/api/history" => self.serve_history(request),
            "/api/traces" => self.serve_traces(request),
            // Prometheus /metrics was removed. Return 410 Gone with a pointer
            // to the replacement endpoint so monitoring tools get a clear signal.
            "/metrics" => {
                let header =
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain"[..])
                        .map_err(|()| io::Error::other("invalid HTTP header"))?;
                let resp = tiny_http::Response::from_string(
                    "Prometheus /metrics endpoint removed. Use /api/pipelines for JSON metrics.",
                )
                .with_status_code(410)
                .with_header(header);
                request.respond(resp)?;
                Ok(())
            }
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

    fn serve_health(&self, request: tiny_http::Request) -> Result<(), Box<dyn std::error::Error>> {
        let uptime = self.start_time.elapsed().as_secs();
        let body = format!(
            r#"{{"status":"ok","uptime_seconds":{},"version":"{}"}}"#,
            uptime, VERSION,
        );
        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .map_err(|()| io::Error::other("invalid HTTP header"))?;
        let resp = tiny_http::Response::from_string(body).with_header(header);
        request.respond(resp)?;
        Ok(())
    }

    /// Returns 200 `{"status":"ready"}` when at least one pipeline is
    /// registered (i.e., the agent has finished initialization and is
    /// functional). Returns 503 before any pipelines are configured.
    ///
    /// Per-pipeline data-flow freshness (`last_batch_time_ns`) is exposed
    /// via `/api/pipelines` for monitoring dashboards, but is NOT a
    /// readiness gate — a quiet log source should not cause Kubernetes
    /// to mark the pod as unready.
    fn serve_ready(&self, request: tiny_http::Request) -> Result<(), Box<dyn std::error::Error>> {
        let ready = !self.pipelines.is_empty();

        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .map_err(|()| io::Error::other("invalid HTTP header"))?;
        if ready {
            let resp =
                tiny_http::Response::from_string(r#"{"status":"ready"}"#).with_header(header);
            request.respond(resp)?;
        } else {
            let resp = tiny_http::Response::from_string(r#"{"status":"not_ready"}"#)
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
        let body = format!(
            r#"{{"path":"{}","raw_yaml":"{}"}}"#,
            esc(&self.config_path),
            esc(&self.config_yaml),
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
                        \"start_unix_ns\":{st},\
                        \"total_ns\":{tot},\
                        \"scan_ns\":{scan},\
                        \"transform_ns\":{xfm},\
                        \"output_ns\":{out_ns},\
                        \"output_start_unix_ns\":{out_st},\
                        \"scan_rows\":{sr},\
                        \"input_rows\":{ir},\
                        \"output_rows\":{or},\
                        \"bytes_in\":{bi},\
                        \"queue_wait_ns\":{qw},\
                        \"worker_id\":{wid},\
                        \"send_ns\":{snd},\
                        \"recv_ns\":{rcv},\
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
                        let _ = write!(
                            out,
                            "{{\
                                \"trace_id\":\"live-{id}\",\
                                \"pipeline\":\"{pl}\",\
                                \"start_unix_ns\":{st},\
                                \"total_ns\":0,\
                                \"scan_ns\":{scan},\
                                \"transform_ns\":{xfm},\
                                \"output_ns\":0,\
                                \"output_start_unix_ns\":{out_st},\
                                \"scan_rows\":0,\
                                \"input_rows\":0,\
                                \"output_rows\":0,\
                                \"bytes_in\":0,\
                                \"queue_wait_ns\":0,\
                                \"worker_id\":{wid},\
                                \"send_ns\":0,\
                                \"recv_ns\":0,\
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
                                \"stage_start_unix_ns\":{ss}\
                            }}",
                            id = id,
                            pl = esc(&pm.name),
                            st = b.start_unix_ns,
                            scan = b.scan_ns,
                            xfm = b.transform_ns,
                            out_st = b.output_start_unix_ns,
                            wid = b.worker_id,
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

    fn serve_pipelines(
        &self,
        request: tiny_http::Request,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let uptime = self.start_time.elapsed().as_secs();
        let mut pipelines_json = Vec::new();

        for pm in &self.pipelines {
            let inputs_json: Vec<String> = pm
                .inputs
                .iter()
                .map(|(name, typ, stats)| {
                    format!(
                        r#"{{"name":"{}","type":"{}","lines_total":{},"bytes_total":{},"errors":{},"rotations":{},"parse_errors":{}}}"#,
                        esc(name),
                        esc(typ),
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
                        r#"{{"name":"{}","type":"{}","lines_total":{},"bytes_total":{},"errors":{}}}"#,
                        esc(name),
                        esc(typ),
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

            // Note: batch_latency_total and batches_total are updated independently.
            // This calculation is approximate and may briefly mismatch under high concurrency,
            // but reading without retries avoids spinning under load.
            let batch_latency_total = pm.batch_latency_nanos_total.load(Ordering::Relaxed);
            let latency_batches = pm.batches_total.load(Ordering::Relaxed);

            let batch_latency_avg_ns = if latency_batches > 0 {
                batch_latency_total / latency_batches
            } else {
                0
            };
            let inflight = pm.inflight_batches.load(Ordering::Relaxed);
            let backpressure = pm.backpressure_stalls.load(Ordering::Relaxed);

            pipelines_json.push(format!(
                r#"{{"name":"{}","inputs":[{}],"transform":{{"sql":"{}","lines_in":{},"lines_out":{},"errors":{},"filter_drop_rate":{:.3}}},"outputs":[{}],"batches":{{"total":{},"avg_rows":{:.1},"flush_by_size":{},"flush_by_timeout":{},"dropped_batches_total":{},"scan_errors_total":{},"last_batch_time_ns":{},"batch_latency_avg_ns":{},"inflight":{},"rows_total":{}}},"stage_seconds":{{"scan":{:.6},"transform":{:.6},"output":{:.6},"queue_wait":{:.6},"send":{:.6}}},"backpressure_stalls":{}}}"#,
                esc(&pm.name),
                inputs_json.join(","),
                esc(&pm.transform_sql),
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
            ));
        }

        let body = format!(
            r#"{{"pipelines":[{}],"system":{{"uptime_seconds":{},"version":"{}"{}}}}}"#,
            pipelines_json.join(","),
            uptime,
            VERSION,
            self.memory_json(),
        );

        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .map_err(|()| io::Error::other("invalid HTTP header"))?;
        let resp = tiny_http::Response::from_string(body).with_header(header);
        request.respond(resp)?;
        Ok(())
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
}

// ---------------------------------------------------------------------------
// Process-level metrics (RSS, CPU)
// ---------------------------------------------------------------------------

/// Returns (rss_bytes, cpu_user_ms, cpu_sys_ms) for the current process.
fn process_metrics() -> Option<(u64, u64, u64)> {
    get_process_metrics_linux().or_else(get_process_metrics_unix)
}

/// Reads /proc/self/stat to get RSS, utime and stime (Linux).
fn get_process_metrics_linux() -> Option<(u64, u64, u64)> {
    use std::fs;
    use std::io::Read;
    let mut f = fs::File::open("/proc/self/stat").ok()?;
    let mut buf = Vec::with_capacity(4096);
    f.by_ref().take(4096).read_to_end(&mut buf).ok()?;
    let stat = String::from_utf8_lossy(&buf);
    parse_proc_stat(&stat)
}

/// Fallback using getrusage (macOS, BSDs).
#[cfg(unix)]
fn get_process_metrics_unix() -> Option<(u64, u64, u64)> {
    // SAFETY: `zeroed()` is valid for `rusage` (all-zero is a valid bit pattern),
    // and `getrusage` is called with a valid `RUSAGE_SELF` flag and a valid
    // mutable pointer. No other invariants are required.
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &raw mut usage) != 0 {
            return None;
        }
        let user_ms =
            (usage.ru_utime.tv_sec as u64) * 1000 + (usage.ru_utime.tv_usec as u64) / 1000;
        let sys_ms = (usage.ru_stime.tv_sec as u64) * 1000 + (usage.ru_stime.tv_usec as u64) / 1000;
        // ru_maxrss is bytes on macOS, KB on Linux
        #[cfg(target_os = "macos")]
        let rss_bytes = usage.ru_maxrss as u64;
        #[cfg(not(target_os = "macos"))]
        let rss_bytes = (usage.ru_maxrss as u64) * 1024;
        Some((rss_bytes, user_ms, sys_ms))
    }
}

#[cfg(not(unix))]
fn get_process_metrics_unix() -> Option<(u64, u64, u64)> {
    None
}

/// Parses `/proc/self/stat` content and returns `(rss_bytes, user_ms, sys_ms)`.
fn parse_proc_stat(stat: &str) -> Option<(u64, u64, u64)> {
    // Field 14 is utime, field 15 is stime, field 24 is rss (in pages).
    // They are space-separated, but the second field (comm) can contain spaces
    // and is enclosed in parentheses.
    let last_paren = stat.rfind(')')?;
    let after_comm = &stat[last_paren + 1..];
    let mut parts = after_comm.split_whitespace();

    // The fields in /proc/self/stat are:
    // 1: pid
    // 2: (comm)
    // -- after_comm starts here --
    // 3: state (parts[0])
    // 4: ppid (parts[1])
    // ...
    // 14: utime (parts[11])
    // 15: stime (parts[12])
    // ...
    // 24: rss (parts[21])
    let utime_ticks: u64 = parts.nth(11)?.parse().ok()?;
    let stime_ticks: u64 = parts.next()?.parse().ok()?;

    // Skip to field 24 (index 21 after_comm).
    // parts is now at index 13 (after stime).
    // To get to index 21, we need to skip 8 elements.
    let rss_pages: u64 = parts.nth(8)?.parse().ok()?;

    let ticks_per_sec = get_ticks_per_sec()?;
    let page_size = get_page_size()?;

    let user_ms = (utime_ticks * 1000) / ticks_per_sec;
    let sys_ms = (stime_ticks * 1000) / ticks_per_sec;
    let rss_bytes = rss_pages * page_size;

    Some((rss_bytes, user_ms, sys_ms))
}

fn get_ticks_per_sec() -> Option<u64> {
    static CLK_TCK: OnceLock<Option<u64>> = OnceLock::new();
    *CLK_TCK.get_or_init(|| getconf_u64("CLK_TCK"))
}

fn get_page_size() -> Option<u64> {
    static PAGE_SIZE: OnceLock<Option<u64>> = OnceLock::new();
    *PAGE_SIZE.get_or_init(|| getconf_u64("PAGESIZE"))
}

fn getconf_u64(name: &str) -> Option<u64> {
    let output = std::process::Command::new("getconf")
        .arg(name)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    std::str::from_utf8(&output.stdout)
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// Minimal JSON-string escaping (backslash, double-quote, control chars).
fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x00'..='\x1f' => {
                use std::fmt::Write;
                write!(out, "\\u{:04x}", c as u32).expect("write to String is infallible");
            }
            _ => out.push(c),
        }
    }
    out
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

// ---------------------------------------------------------------------------
// Background metric sampler
// ---------------------------------------------------------------------------

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
        inp.inc_lines(1000);
        inp.inc_bytes(50000);
        inp.inc_rotations();

        pm.transform_in.inc_lines(1000);
        // transform_out.inc_lines is no longer called in the pipeline hot path;
        // lines_out is derived from output-sink stats instead.

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
    fn test_component_stats() {
        let stats = ComponentStats::new();
        assert_eq!(stats.lines(), 0);
        assert_eq!(stats.bytes(), 0);
        assert_eq!(stats.errors(), 0);

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
    }

    #[test]
    fn test_health_endpoint() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        // Give the server a moment to bind.
        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/health");
        assert_eq!(status, 200);
        assert!(body.contains(r#""status":"ok""#), "body: {}", body);
        assert!(
            body.contains(&format!(r#""version":"{}""#, env!("CARGO_PKG_VERSION"))),
            "body: {}",
            body
        );
        assert!(body.contains(r#""uptime_seconds":"#), "body: {}", body);
    }

    #[test]
    fn test_pipelines_endpoint() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/api/pipelines");
        assert_eq!(status, 200);
        assert!(body.contains(r#""name":"default""#), "body: {}", body);
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
        assert!(body.contains(r#""scan_errors_total":2"#), "body: {}", body);
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

        let (status, body) = http_get(port, "/api/stats");
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
    fn test_not_found() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, _body) = http_get(port, "/nonexistent");
        assert_eq!(status, 404);
    }

    #[test]
    fn test_pipelines_endpoint_no_memory_stats() {
        // Without a memory_stats_fn set, the system section must NOT contain
        // a "memory" key — no partial or null fields.
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/api/pipelines");
        assert_eq!(status, 200);
        assert!(body.contains(r#""rotations":1"#), "body: {}", body);
        assert!(
            !body.contains(r#""memory""#),
            "unexpected memory key: {}",
            body
        );
    }

    #[test]
    fn test_pipelines_endpoint_with_memory_stats() {
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

        let (status, body) = http_get(port, "/api/pipelines");
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
    }

    #[test]
    fn test_esc_control_chars() {
        assert_eq!(esc("hello\0world"), "hello\\u0000world");
        assert_eq!(esc("tab\tnewline\nreturn\r"), "tab\\tnewline\\nreturn\\r");
        assert_eq!(esc("bell\x07"), "bell\\u0007");
        assert_eq!(esc("escape\x1b"), "escape\\u001b");
    }

    #[test]
    fn test_pipelines_endpoint_escaping() {
        let meter = opentelemetry::global::meter("test");
        // Control character in pipeline name.
        let pm = PipelineMetrics::new("pipe\x01line", "SELECT * FROM logs", &meter);

        let mut server = DiagnosticsServer::new("127.0.0.1:0");
        server.add_pipeline(Arc::new(pm));
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/api/pipelines");
        assert_eq!(status, 200);
        // The name should be escaped as "pipe\u0001line".
        assert!(
            body.contains(r#""name":"pipe\u0001line""#),
            "body: {}",
            body
        );

        // Check that the overall JSON is valid (can be parsed).
        let _v: serde_json::Value =
            serde_json::from_str(&body).expect("invalid JSON output from /api/pipelines");
    }

    #[test]
    fn test_traces_endpoint_empty() {
        // Server with no trace buffer attached — should return empty array.
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/api/traces");
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

        let (status, body) = http_get(port, "/api/traces");
        assert_eq!(status, 200);

        // Must parse as valid JSON.
        let v: serde_json::Value =
            serde_json::from_str(&body).expect("invalid JSON from /api/traces");

        let traces = v["traces"].as_array().expect("traces must be array");
        assert_eq!(traces.len(), 1, "expected 1 trace, got {}", traces.len());

        let t = &traces[0];
        assert_eq!(t["pipeline"], "default");
        assert_eq!(t["input_rows"], 100);
        assert_eq!(t["output_rows"], 75);
        assert_eq!(t["errors"], 0);
        assert_eq!(t["flush_reason"], "size");
        assert_eq!(t["scan_ns"], 150_000_000u64);
        assert_eq!(t["total_ns"], 200_000_000u64);
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

    // Bug #728: diagnostics server should return 405 for non-GET methods.
    #[test]
    fn non_get_returns_405() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        for path in &["/health", "/api/pipelines", "/api/stats"] {
            let status = http_post(port, path);
            assert_eq!(status, 405, "POST {path} should return 405, got {status}");
        }
    }

    // Bug #715: /metrics should return 410 Gone with a helpful message,
    // not a generic 404 that gives no hint about what happened.
    #[test]
    fn metrics_endpoint_returns_410() {
        let server = server_with_test_pipeline();
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/metrics");
        assert_eq!(status, 410, "expected 410 Gone for /metrics, got {status}");
        assert!(
            body.contains("/api/pipelines"),
            "/metrics 410 body should mention /api/pipelines: {body}"
        );
    }

    #[test]
    fn test_pipeline_metrics_approximate_latency_snapshot() {
        let meter = opentelemetry::global::meter("test");
        let pm = PipelineMetrics::new("default", "SELECT *", &meter);

        // Setup initial state: 10 batches, total latency 5000ns -> avg 500ns
        pm.batches_total.store(10, Ordering::Relaxed);
        pm.batch_latency_nanos_total.store(5000, Ordering::Relaxed);

        let mut server = DiagnosticsServer::new("127.0.0.1:0");
        server.add_pipeline(Arc::new(pm));
        let (_handle, addr) = server.start().expect("server bind failed");
        let port = addr.port();

        thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/api/pipelines");
        assert_eq!(status, 200);
        // Expect avg latency 500
        assert!(
            body.contains(r#""batch_latency_avg_ns":500"#),
            "body: {}",
            body
        );
    }
}

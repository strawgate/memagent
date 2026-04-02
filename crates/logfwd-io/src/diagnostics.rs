use std::io;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Meter};

// ---------------------------------------------------------------------------
// Atomic stats structures (lock-free, hot-path friendly)
// ---------------------------------------------------------------------------

/// Stats for one component. Dual-write: atomics for /api/pipelines,
/// OTel counters for OTLP push. Both are lock-free on the hot path.
pub struct ComponentStats {
    pub lines_total: AtomicU64,
    pub bytes_total: AtomicU64,
    pub errors_total: AtomicU64,
    /// Expected input lifecycle events (rotation/truncation).
    pub rotations_total: AtomicU64,
    /// Lines that failed format parsing (e.g. malformed CRI lines).
    pub parse_errors_total: AtomicU64,
    // OTel counters (for OTLP push)
    otel_lines: Counter<u64>,
    otel_bytes: Counter<u64>,
    otel_errors: Counter<u64>,
    otel_rotations: Counter<u64>,
    otel_parse_errors: Counter<u64>,
    otel_attrs: Vec<KeyValue>,
}

impl ComponentStats {
    /// Create stats with OTel counters. `prefix` is e.g. "logfwd_input" or "logfwd_output".
    pub fn with_meter(meter: &Meter, prefix: &str, attrs: Vec<KeyValue>) -> Self {
        Self {
            lines_total: AtomicU64::new(0),
            bytes_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            rotations_total: AtomicU64::new(0),
            parse_errors_total: AtomicU64::new(0),
            otel_lines: meter.u64_counter(format!("{prefix}_lines")).build(),
            otel_bytes: meter.u64_counter(format!("{prefix}_bytes")).build(),
            otel_errors: meter.u64_counter(format!("{prefix}_errors")).build(),
            otel_rotations: meter.u64_counter(format!("{prefix}_rotations")).build(),
            otel_parse_errors: meter.u64_counter(format!("{prefix}_parse_errors")).build(),
            otel_attrs: attrs,
        }
    }

    /// Create stats without OTel (for tests and standalone use).
    pub fn new() -> Self {
        let noop = opentelemetry::global::meter("noop");
        Self::with_meter(&noop, "noop", vec![])
    }

    pub fn inc_lines(&self, n: u64) {
        self.lines_total.fetch_add(n, Ordering::Relaxed);
        self.otel_lines.add(n, &self.otel_attrs);
    }

    pub fn inc_bytes(&self, n: u64) {
        self.bytes_total.fetch_add(n, Ordering::Relaxed);
        self.otel_bytes.add(n, &self.otel_attrs);
    }

    pub fn inc_errors(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
        self.otel_errors.add(1, &self.otel_attrs);
    }

    /// Increment input rollover count for both file rotations and truncations.
    ///
    /// This updates the in-process atomic counter (`rotations_total`) and emits
    /// the corresponding OpenTelemetry metric (`otel_rotations`) with the
    /// component attributes.
    pub fn inc_rotations(&self) {
        self.rotations_total.fetch_add(1, Ordering::Relaxed);
        self.otel_rotations.add(1, &self.otel_attrs);
    }

    pub fn inc_parse_errors(&self, n: u64) {
        self.parse_errors_total.fetch_add(n, Ordering::Relaxed);
        self.otel_parse_errors.add(n, &self.otel_attrs);
    }

    fn lines(&self) -> u64 {
        self.lines_total.load(Ordering::Relaxed)
    }

    fn bytes(&self) -> u64 {
        self.bytes_total.load(Ordering::Relaxed)
    }

    fn errors(&self) -> u64 {
        self.errors_total.load(Ordering::Relaxed)
    }

    fn rotations(&self) -> u64 {
        self.rotations_total.load(Ordering::Relaxed)
    }

    fn parse_errors(&self) -> u64 {
        self.parse_errors_total.load(Ordering::Relaxed)
    }
}

impl Default for ComponentStats {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Pipeline-level metrics (shared between pipeline thread and diagnostics)
// ---------------------------------------------------------------------------

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
    /// Unix timestamp (nanoseconds) of the last successfully processed batch.
    /// Zero means no batch has been processed yet.
    pub last_batch_time_ns: AtomicU64,
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
            last_batch_time_ns: AtomicU64::new(0),
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
        }
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
    /// Returns the join handle on success or an `io::Error` on bind failure.
    pub fn start(self) -> io::Result<JoinHandle<()>> {
        let server = tiny_http::Server::http(&self.bind_addr)
            .map_err(|e| io::Error::other(e.to_string()))?;

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

        Ok(thread::spawn(move || {
            for request in server.incoming_requests() {
                let _ = self.handle_request(request);
            }
        }))
    }

    fn handle_request(
        &self,
        request: tiny_http::Request,
    ) -> Result<(), Box<dyn std::error::Error>> {
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
            // Prometheus /metrics removed — use OTLP metrics push instead.
            // The /api/pipelines endpoint provides the same data as JSON.
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
            r#"{{"uptime_sec":{:.3}{},"input_lines":{},"input_bytes":{},"output_lines":{},"output_bytes":{},"output_errors":{},"batches":{},"scan_sec":{:.6},"transform_sec":{:.6},"output_sec":{:.6},"backpressure_stalls":{}{}}}"#,
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
            let lines_out = pm.transform_out.lines();
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

            let last_batch_ns = pm.last_batch_time_ns.load(Ordering::Relaxed);

            pipelines_json.push(format!(
                r#"{{"name":"{}","inputs":[{}],"transform":{{"sql":"{}","lines_in":{},"lines_out":{},"errors":{},"filter_drop_rate":{:.3}}},"outputs":[{}],"batches":{{"total":{},"avg_rows":{:.1},"flush_by_size":{},"flush_by_timeout":{},"dropped_batches_total":{},"scan_errors_total":{},"last_batch_time_ns":{}}},"stage_seconds":{{"scan":{:.6},"transform":{:.6},"output":{:.6}}}}}"#,
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
                scan_s,
                transform_s,
                output_s,
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
    let mut output_errors: u64 = 0;
    let mut scan_ns: u64 = 0;
    let mut transform_ns: u64 = 0;
    let mut output_ns: u64 = 0;

    for pm in pipelines {
        for (_, _, s) in &pm.inputs {
            input_lines += s.lines();
            input_bytes += s.bytes();
        }
        for (_, _, s) in &pm.outputs {
            output_lines += s.lines();
            output_errors += s.errors();
        }
        scan_ns += pm.scan_nanos_total.load(Ordering::Relaxed);
        transform_ns += pm.transform_nanos_total.load(Ordering::Relaxed);
        output_ns += pm.output_nanos_total.load(Ordering::Relaxed);
    }

    history.record("input_lines", input_lines as f64);
    history.record("input_bytes", input_bytes as f64);
    history.record("output_lines", output_lines as f64);
    history.record("output_errors", output_errors as f64);
    history.record("scan_sec", scan_ns as f64 / 1e9);
    history.record("transform_sec", transform_ns as f64 / 1e9);
    history.record("output_sec", output_ns as f64 / 1e9);

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
    use std::net::TcpListener;
    use std::sync::Mutex;

    /// Serialize diagnostics tests to prevent port collisions.
    /// free_port() releases the port before the server binds — running
    /// tests in parallel means another test can grab the same port.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    /// Pick an available port by binding to :0.
    fn free_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    }

    /// Build a server with one pipeline pre-populated with known counter values.
    fn server_with_test_pipeline(port: u16) -> DiagnosticsServer {
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
        pm.transform_out.inc_lines(900);

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
        pm.transform_errors.store(3, Ordering::Relaxed);

        let mut server = DiagnosticsServer::new(&format!("127.0.0.1:{}", port));
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
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(50)),
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
        let _lock = TEST_LOCK.lock().unwrap();
        let port = free_port();
        let server = server_with_test_pipeline(port);
        let _handle = server.start().expect("server bind failed");

        // Give the server a moment to bind.
        std::thread::sleep(std::time::Duration::from_millis(100));

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
        let _lock = TEST_LOCK.lock().unwrap();
        let port = free_port();
        let server = server_with_test_pipeline(port);
        let _handle = server.start().expect("server bind failed");

        std::thread::sleep(std::time::Duration::from_millis(100));

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
    }

    #[test]
    fn test_stats_endpoint_contract() {
        let _lock = TEST_LOCK.lock().unwrap();
        let port = free_port();
        let mut server = server_with_test_pipeline(port);
        server.set_memory_stats_fn(|| {
            Some(MemoryStats {
                resident: 1_000_000,
                allocated: 800_000,
                active: 900_000,
            })
        });
        let _handle = server.start().expect("server bind failed");

        std::thread::sleep(std::time::Duration::from_millis(100));

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
    fn test_not_found() {
        let _lock = TEST_LOCK.lock().unwrap();
        let port = free_port();
        let server = server_with_test_pipeline(port);
        let _handle = server.start().expect("server bind failed");

        std::thread::sleep(std::time::Duration::from_millis(100));

        let (status, _body) = http_get(port, "/nonexistent");
        assert_eq!(status, 404);
    }

    #[test]
    fn test_pipelines_endpoint_no_memory_stats() {
        // Without a memory_stats_fn set, the system section must NOT contain
        // a "memory" key — no partial or null fields.
        let _lock = TEST_LOCK.lock().unwrap();
        let port = free_port();
        let server = server_with_test_pipeline(port);
        let _handle = server.start().expect("server bind failed");

        std::thread::sleep(std::time::Duration::from_millis(100));

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
        let _lock = TEST_LOCK.lock().unwrap();
        let port = free_port();
        let mut server = server_with_test_pipeline(port);
        server.set_memory_stats_fn(|| {
            Some(MemoryStats {
                resident: 1_000_000,
                allocated: 800_000,
                active: 900_000,
            })
        });
        let _handle = server.start().expect("server bind failed");

        std::thread::sleep(std::time::Duration::from_millis(100));

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
        let _lock = TEST_LOCK.lock().unwrap();
        let port = free_port();

        let server = DiagnosticsServer::new(&format!("127.0.0.1:{}", port));
        // Don't add any pipelines.
        let _handle = server.start().expect("server bind failed");

        std::thread::sleep(std::time::Duration::from_millis(100));

        let (status, body) = http_get(port, "/ready");
        assert_eq!(status, 503, "body: {}", body);
        assert!(body.contains(r#""status":"not_ready""#), "body: {}", body);
    }

    #[test]
    fn test_ready_endpoint_with_pipeline_returns_200() {
        // A registered pipeline makes the server ready, regardless of
        // whether any batches have been processed.
        let _lock = TEST_LOCK.lock().unwrap();
        let port = free_port();
        let meter = opentelemetry::global::meter("test");
        let pm = PipelineMetrics::new("default", "SELECT * FROM logs", &meter);
        // last_batch_time_ns stays at 0 — no data yet, but still ready.

        let mut server = DiagnosticsServer::new(&format!("127.0.0.1:{}", port));
        server.add_pipeline(Arc::new(pm));
        let _handle = server.start().expect("server bind failed");

        std::thread::sleep(std::time::Duration::from_millis(100));

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
        let _lock = TEST_LOCK.lock().unwrap();
        let port = free_port();
        let meter = opentelemetry::global::meter("test");
        // Control character in pipeline name.
        let pm = PipelineMetrics::new("pipe\x01line", "SELECT * FROM logs", &meter);

        let mut server = DiagnosticsServer::new(&format!("127.0.0.1:{}", port));
        server.add_pipeline(Arc::new(pm));
        let _handle = server.start().expect("server bind failed");

        std::thread::sleep(std::time::Duration::from_millis(100));

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
}

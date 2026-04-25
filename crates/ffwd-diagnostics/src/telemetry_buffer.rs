//! Unified telemetry ring buffers for the diagnostics dashboard.
//!
//! Three buffers — metrics, traces, logs — each storing pre-serialized OTLP
//! JSON snippets. The diagnostics server serves them as complete OTLP JSON
//! documents (`ExportMetricsServiceRequest`, `ExportTraceServiceRequest`,
//! `ExportLogsServiceRequest`), giving the dashboard a stable, spec-defined
//! contract with zero custom types.
//!
//! # Architecture
//!
//! ```text
//! Pipeline hot path          Diagnostics server        Dashboard (TypeScript)
//! ─────────────────          ──────────────────        ──────────────────────
//! PipelineMetrics ──┐
//! SpanBuffer ───────┤        GET /telemetry/metrics   @metrickit/otlpjson
//! stderr capture ───┘──▶     GET /telemetry/traces    @metrickit/views
//!                            GET /telemetry/logs      @metrickit/adapters
//! ```
//!
//! The contract is OTLP JSON — a versioned, well-tested spec. Neither the
//! Rust server nor the TypeScript dashboard invents schema; both sides speak
//! the same wire format that every observability tool understands.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Generic ring buffer
// ---------------------------------------------------------------------------

/// A thread-safe, bounded ring buffer for telemetry records.
///
/// When the buffer is full, the oldest entry is evicted. This is the same
/// pattern used by the existing `SpanBuffer`, generalized to metrics and logs.
#[derive(Clone)]
pub struct RingBuffer<T> {
    inner: Arc<Mutex<VecDeque<T>>>,
    capacity: usize,
}

impl<T: fmt::Debug> fmt::Debug for RingBuffer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let len = self.inner.lock().map_or(0, |b| b.len());
        write!(f, "RingBuffer({len}/{})", self.capacity)
    }
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
        }
    }

    pub fn push(&self, item: T) {
        if self.capacity == 0 {
            return;
        }
        if let Ok(mut buf) = self.inner.lock() {
            if buf.len() >= self.capacity {
                buf.pop_front();
            }
            buf.push_back(item);
        }
    }

    pub fn snapshot(&self) -> Vec<T>
    where
        T: Clone,
    {
        match self.inner.lock() {
            Ok(buf) => buf.iter().cloned().collect(),
            Err(_) => vec![],
        }
    }
}

// ---------------------------------------------------------------------------
// Metric point
// ---------------------------------------------------------------------------

/// A single metric gauge/counter data point, ready for OTLP JSON serialization.
#[derive(Debug, Clone)]
pub struct MetricPoint {
    pub name: &'static str,
    pub description: &'static str,
    pub unit: &'static str,
    /// Gauge value at the time of sampling.
    pub value: MetricValue,
    /// Dimensional attributes (pipeline name, component name, etc.).
    pub attributes: Vec<(&'static str, String)>,
    /// Unix nanoseconds when this point was sampled.
    pub time_unix_nano: u64,
}

#[derive(Debug, Clone)]
pub enum MetricValue {
    U64(u64),
    #[allow(dead_code)] // used by serializer; callers coming in follow-up PRs
    F64(f64),
}

// ---------------------------------------------------------------------------
// Log record
// ---------------------------------------------------------------------------

/// A single log record for OTLP JSON serialization.
#[derive(Debug, Clone)]
pub struct LogPoint {
    pub severity: Severity,
    pub body: String,
    pub attributes: Vec<(&'static str, String)>,
    pub time_unix_nano: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum Severity {
    Info = 9,
    Warn = 13,
    #[allow(dead_code)] // used in tests; callers coming in follow-up PRs
    Error = 17,
}

// ---------------------------------------------------------------------------
// Trace span (re-uses the shape from span_exporter::TraceSpan)
// ---------------------------------------------------------------------------

/// A single trace span for OTLP JSON serialization.
#[derive(Debug, Clone)]
pub struct SpanPoint {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub name: String,
    pub start_time_unix_nano: u64,
    pub end_time_unix_nano: u64,
    pub attributes: Vec<(String, String)>,
    /// OTLP SpanKind: 0=unspecified, 1=internal, 2=server, 3=client
    pub kind: u8,
    /// OTLP StatusCode: 0=unset, 1=ok, 2=error
    pub status_code: u8,
    pub status_message: String,
}

// ---------------------------------------------------------------------------
// Telemetry buffers (all three signals)
// ---------------------------------------------------------------------------

/// Holds the three OTLP signal buffers shared between the pipeline hot path
/// and the diagnostics server.
#[derive(Debug, Clone)]
pub struct TelemetryBuffers {
    pub metrics: RingBuffer<MetricPoint>,
    #[allow(dead_code)] // read by traces endpoint; callers coming in follow-up PRs
    pub traces: RingBuffer<SpanPoint>,
    pub logs: RingBuffer<LogPoint>,
}

impl TelemetryBuffers {
    pub fn new() -> Self {
        Self {
            // ~900 metric points = 45 metrics × 20 samples (40s at 2s interval)
            metrics: RingBuffer::new(4096),
            // ~8000 batches × 2 spans each
            traces: RingBuffer::new(16_000),
            // stderr + health transitions
            logs: RingBuffer::new(2048),
        }
    }
}

impl Default for TelemetryBuffers {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// OTLP JSON serialization
// ---------------------------------------------------------------------------

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Serialize metric points as an OTLP JSON `ExportMetricsServiceRequest`.
///
/// Groups all points under a single resource with `service.name = "logfwd"`.
/// Each unique metric name becomes one Gauge metric with all its data points.
pub fn metrics_to_otlp_json(points: &[MetricPoint]) -> String {
    if points.is_empty() {
        return r#"{"resourceMetrics":[]}"#.to_string();
    }

    // Group points by metric name, preserving insertion order.
    let mut metric_names: Vec<&'static str> = Vec::new();
    let mut groups: HashMap<&'static str, Vec<&MetricPoint>> = HashMap::new();
    for p in points {
        if !groups.contains_key(p.name) {
            metric_names.push(p.name);
        }
        groups.entry(p.name).or_default().push(p);
    }

    let mut metrics_json = Vec::new();
    for name in &metric_names {
        let pts = &groups[name];
        let first = pts[0];

        let mut data_points = Vec::new();
        for p in pts {
            let attrs = attrs_to_json(&p.attributes);
            let value = match p.value {
                MetricValue::U64(v) => format!("\"asInt\":\"{v}\""),
                MetricValue::F64(v) => {
                    if v.is_finite() {
                        format!("\"asDouble\":{v}")
                    } else {
                        "\"asDouble\":null".to_string()
                    }
                }
            };
            data_points.push(format!(
                "{{\"timeUnixNano\":\"{}\",{value},{attrs}}}",
                p.time_unix_nano
            ));
        }

        metrics_json.push(format!(
            "{{\"name\":\"{}\",\"description\":\"{}\",\"unit\":\"{}\",\"gauge\":{{\"dataPoints\":[{}]}}}}",
            json_escape(name),
            json_escape(first.description),
            json_escape(first.unit),
            data_points.join(",")
        ));
    }

    let version = env!("CARGO_PKG_VERSION");
    format!(
        "{{\"resourceMetrics\":[{{\"resource\":{{\"attributes\":[{{\"key\":\"service.name\",\"value\":{{\"stringValue\":\"logfwd\"}}}},{{\"key\":\"service.version\",\"value\":{{\"stringValue\":\"{version}\"}}}}]}},\"scopeMetrics\":[{{\"scope\":{{\"name\":\"logfwd.diagnostics\",\"version\":\"{version}\"}},\"metrics\":[{metrics}]}}]}}]}}",
        metrics = metrics_json.join(",")
    )
}

/// Serialize span points as an OTLP JSON `ExportTraceServiceRequest`.
pub fn traces_to_otlp_json(spans: &[SpanPoint]) -> String {
    if spans.is_empty() {
        return r#"{"resourceSpans":[]}"#.to_string();
    }

    let mut spans_json = Vec::new();
    for s in spans {
        let attrs = string_attrs_to_json(&s.attributes);
        let status = if s.status_code == 0 {
            String::new()
        } else {
            format!(
                ",\"status\":{{\"code\":{},\"message\":\"{}\"}}",
                s.status_code,
                json_escape(&s.status_message)
            )
        };

        spans_json.push(format!(
            "{{\"traceId\":\"{}\",\"spanId\":\"{}\",\"parentSpanId\":\"{}\",\"name\":\"{}\",\"kind\":{},\"startTimeUnixNano\":\"{}\",\"endTimeUnixNano\":\"{}\",{attrs}{status}}}",
            json_escape(&s.trace_id),
            json_escape(&s.span_id),
            json_escape(&s.parent_span_id),
            json_escape(&s.name),
            s.kind,
            s.start_time_unix_nano,
            s.end_time_unix_nano,
        ));
    }

    let version = env!("CARGO_PKG_VERSION");
    format!(
        "{{\"resourceSpans\":[{{\"resource\":{{\"attributes\":[{{\"key\":\"service.name\",\"value\":{{\"stringValue\":\"logfwd\"}}}},{{\"key\":\"service.version\",\"value\":{{\"stringValue\":\"{version}\"}}}}]}},\"scopeSpans\":[{{\"scope\":{{\"name\":\"logfwd.diagnostics\",\"version\":\"{version}\"}},\"spans\":[{spans}]}}]}}]}}",
        spans = spans_json.join(",")
    )
}

/// Serialize log points as an OTLP JSON `ExportLogsServiceRequest`.
pub fn logs_to_otlp_json(logs: &[LogPoint]) -> String {
    if logs.is_empty() {
        return r#"{"resourceLogs":[]}"#.to_string();
    }

    let mut logs_json = Vec::new();
    for l in logs {
        let attrs = attrs_to_json(&l.attributes);
        logs_json.push(format!(
            "{{\"timeUnixNano\":\"{}\",\"severityNumber\":{},\"body\":{{\"stringValue\":\"{}\"}},{attrs}}}",
            l.time_unix_nano,
            l.severity as u8,
            json_escape(&l.body),
        ));
    }

    let version = env!("CARGO_PKG_VERSION");
    format!(
        "{{\"resourceLogs\":[{{\"resource\":{{\"attributes\":[{{\"key\":\"service.name\",\"value\":{{\"stringValue\":\"logfwd\"}}}},{{\"key\":\"service.version\",\"value\":{{\"stringValue\":\"{version}\"}}}}]}},\"scopeLogs\":[{{\"scope\":{{\"name\":\"logfwd.diagnostics\",\"version\":\"{version}\"}},\"logRecords\":[{logs}]}}]}}]}}",
        logs = logs_json.join(",")
    )
}

// ---------------------------------------------------------------------------
// Metric sampling — snapshot PipelineMetrics into MetricPoints
// ---------------------------------------------------------------------------

use crate::diagnostics::{ActiveBatch, MemoryStats, PipelineMetrics};
use crate::span_exporter::TraceSpan;
use std::collections::HashMap;
use std::sync::atomic::Ordering;

/// Sample all pipeline metrics into OTLP MetricPoints.
///
/// Called by the background sampler (every 2s) to populate the metrics ring
/// buffer. Each metric is emitted as a gauge with pipeline/component attributes.
pub fn sample_pipeline_metrics(pipelines: &[Arc<PipelineMetrics>], buf: &RingBuffer<MetricPoint>) {
    let now = now_nanos();

    for pm in pipelines {
        let pipeline = pm.name.clone();

        // Macro to reduce boilerplate for pipeline-scoped gauges.
        macro_rules! gauge {
            ($name:expr, $desc:expr, $unit:expr, $val:expr) => {
                buf.push(MetricPoint {
                    name: $name,
                    description: $desc,
                    unit: $unit,
                    value: MetricValue::U64($val),
                    attributes: vec![("pipeline", pipeline.clone())],
                    time_unix_nano: now,
                });
            };
        }

        // Input aggregates
        let mut input_lines: u64 = 0;
        let mut input_bytes: u64 = 0;
        for (name, typ, stats) in &pm.inputs {
            let lines = stats.lines();
            let bytes = stats.bytes();
            input_lines += lines;
            input_bytes += bytes;

            // Per-component metrics
            let component_attrs = vec![
                ("pipeline", pipeline.clone()),
                ("component", name.clone()),
                ("component.type", typ.clone()),
            ];
            buf.push(MetricPoint {
                name: "logfwd.component.input.lines",
                description: "Lines read by input component",
                unit: "1",
                value: MetricValue::U64(lines),
                attributes: component_attrs.clone(),
                time_unix_nano: now,
            });
            buf.push(MetricPoint {
                name: "logfwd.component.input.bytes",
                description: "Bytes read by input component",
                unit: "By",
                value: MetricValue::U64(bytes),
                attributes: component_attrs,
                time_unix_nano: now,
            });
        }

        gauge!("logfwd.input.lines", "Total lines read", "1", input_lines);
        gauge!("logfwd.input.bytes", "Total bytes read", "By", input_bytes);

        // Output aggregates
        let mut output_lines: u64 = 0;
        let mut output_bytes: u64 = 0;
        let mut output_errors: u64 = 0;
        for (name, typ, stats) in &pm.outputs {
            let lines = stats.lines();
            let bytes = stats.bytes();
            let errors = stats.errors();
            output_lines += lines;
            output_bytes += bytes;
            output_errors += errors;

            let component_attrs = vec![
                ("pipeline", pipeline.clone()),
                ("component", name.clone()),
                ("component.type", typ.clone()),
            ];
            buf.push(MetricPoint {
                name: "logfwd.component.output.lines",
                description: "Lines written by output component",
                unit: "1",
                value: MetricValue::U64(lines),
                attributes: component_attrs.clone(),
                time_unix_nano: now,
            });
            buf.push(MetricPoint {
                name: "logfwd.component.output.bytes",
                description: "Bytes written by output component",
                unit: "By",
                value: MetricValue::U64(bytes),
                attributes: component_attrs.clone(),
                time_unix_nano: now,
            });
            buf.push(MetricPoint {
                name: "logfwd.component.output.errors",
                description: "Output component errors",
                unit: "1",
                value: MetricValue::U64(errors),
                attributes: component_attrs,
                time_unix_nano: now,
            });
        }

        gauge!(
            "logfwd.output.lines",
            "Total lines written",
            "1",
            output_lines
        );
        gauge!(
            "logfwd.output.bytes",
            "Total bytes written",
            "By",
            output_bytes
        );
        gauge!(
            "logfwd.output.errors",
            "Total output errors",
            "1",
            output_errors
        );

        // Batch metrics
        gauge!(
            "logfwd.batch.total",
            "Total batches processed",
            "1",
            pm.batches_total.load(Ordering::Relaxed)
        );
        gauge!(
            "logfwd.batch.inflight",
            "Currently processing batches",
            "1",
            pm.inflight_batches.load(Ordering::Relaxed)
        );
        gauge!(
            "logfwd.batch.rows",
            "Total rows across all batches",
            "1",
            pm.batch_rows_total.load(Ordering::Relaxed)
        );
        gauge!(
            "logfwd.batch.flush.by_size",
            "Batches flushed by size",
            "1",
            pm.flush_by_size.load(Ordering::Relaxed)
        );
        gauge!(
            "logfwd.batch.flush.by_timeout",
            "Batches flushed by timeout",
            "1",
            pm.flush_by_timeout.load(Ordering::Relaxed)
        );
        gauge!(
            "logfwd.input.poll_cadence.fast_repolls",
            "Idle polls that bypassed baseline sleep due to adaptive cadence",
            "1",
            pm.cadence_fast_repolls.load(Ordering::Relaxed)
        );
        gauge!(
            "logfwd.input.poll_cadence.idle_sleeps",
            "Idle polls that slept for baseline poll interval",
            "1",
            pm.cadence_idle_sleeps.load(Ordering::Relaxed)
        );
        gauge!(
            "logfwd.batch.dropped",
            "Dropped batches",
            "1",
            pm.dropped_batches_total.load(Ordering::Relaxed)
        );
        let avg_latency = pm.batch_latency_nanos_total.load(Ordering::Relaxed)
            / pm.batches_total.load(Ordering::Relaxed).max(1);
        gauge!(
            "logfwd.batch.latency.avg_ns",
            "Average batch latency",
            "ns",
            avg_latency
        );

        // Stage timing
        gauge!(
            "logfwd.stage.scan.nanos",
            "Cumulative scan time",
            "ns",
            pm.scan_nanos_total.load(Ordering::Relaxed)
        );
        gauge!(
            "logfwd.stage.transform.nanos",
            "Cumulative transform time",
            "ns",
            pm.transform_nanos_total.load(Ordering::Relaxed)
        );
        gauge!(
            "logfwd.stage.output.nanos",
            "Cumulative output time",
            "ns",
            pm.output_nanos_total.load(Ordering::Relaxed)
        );
        gauge!(
            "logfwd.stage.queue_wait.nanos",
            "Cumulative queue wait",
            "ns",
            pm.queue_wait_nanos_total.load(Ordering::Relaxed)
        );
        gauge!(
            "logfwd.stage.send.nanos",
            "Cumulative send time",
            "ns",
            pm.send_nanos_total.load(Ordering::Relaxed)
        );

        // Backpressure
        gauge!(
            "logfwd.backpressure.stalls",
            "Input backpressure stalls",
            "1",
            pm.backpressure_stalls.load(Ordering::Relaxed)
        );
    }
}

// ---------------------------------------------------------------------------
// Full metric sampling (pipeline + process + memory + health + bottleneck)
// ---------------------------------------------------------------------------

/// Sample all metrics into the telemetry buffer.
///
/// Wraps `sample_pipeline_metrics` and adds process-level, memory, component
/// health, and bottleneck gauges. Called every 2s by the background sampler.
pub fn sample_all_metrics(
    pipelines: &[Arc<PipelineMetrics>],
    memory_fn: Option<fn() -> Option<MemoryStats>>,
    uptime_secs: f64,
    buffers: &TelemetryBuffers,
) {
    let buf = &buffers.metrics;
    let now = now_nanos();

    // Pipeline metrics (input/output/batch/stage/backpressure).
    sample_pipeline_metrics(pipelines, buf);

    // Per-component health gauges.
    for pm in pipelines {
        let pipeline = pm.name.clone();

        // Transform errors / scan errors / parse errors
        let transform_errors = pm.transform_errors.load(Ordering::Relaxed);
        let scan_errors = pm.scan_errors_total.load(Ordering::Relaxed);
        let parse_errors = pm.parse_errors_total.load(Ordering::Relaxed);

        for (metric_name, desc, val) in [
            (
                "logfwd.pipeline.transform_errors",
                "Transform SQL errors",
                transform_errors,
            ),
            (
                "logfwd.pipeline.scan_errors",
                "Scan stage errors",
                scan_errors,
            ),
            ("logfwd.pipeline.parse_errors", "Parse errors", parse_errors),
        ] {
            buf.push(MetricPoint {
                name: metric_name,
                description: desc,
                unit: "1",
                value: MetricValue::U64(val),
                attributes: vec![("pipeline", pipeline.clone())],
                time_unix_nano: now,
            });
        }

        // Per-input component health + parse errors
        for (name, typ, stats) in &pm.inputs {
            let health = stats.health();
            let component_attrs = vec![
                ("pipeline", pipeline.clone()),
                ("component", name.clone()),
                ("component.type", typ.clone()),
                ("role", "input".to_string()),
            ];
            buf.push(MetricPoint {
                name: "logfwd.component.health",
                description: "Component health state (0=healthy, 5=failed)",
                unit: "1",
                value: MetricValue::U64(health as u64),
                attributes: component_attrs.clone(),
                time_unix_nano: now,
            });
            buf.push(MetricPoint {
                name: "logfwd.component.input.parse_errors",
                description: "Input parse errors",
                unit: "1",
                value: MetricValue::U64(stats.parse_errors()),
                attributes: component_attrs,
                time_unix_nano: now,
            });
        }

        // Per-output component health
        for (name, typ, stats) in &pm.outputs {
            let health = stats.health();
            buf.push(MetricPoint {
                name: "logfwd.component.health",
                description: "Component health state (0=healthy, 5=failed)",
                unit: "1",
                value: MetricValue::U64(health as u64),
                attributes: vec![
                    ("pipeline", pipeline.clone()),
                    ("component", name.clone()),
                    ("component.type", typ.clone()),
                    ("role", "output".to_string()),
                ],
                time_unix_nano: now,
            });
        }

        // Bottleneck detection
        if uptime_secs > 0.0 {
            let scan_s = pm.scan_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;
            let transform_s = pm.transform_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;
            let queue_wait_s = pm.queue_wait_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;
            let backpressure = pm.backpressure_stalls.load(Ordering::Relaxed);

            let queue_wait_ratio = queue_wait_s / uptime_secs;
            let stalls_per_sec = backpressure as f64 / uptime_secs;
            let transform_ratio = transform_s / uptime_secs;
            let scan_ratio = scan_s / uptime_secs;

            let stage = if queue_wait_ratio > 0.5 {
                "output"
            } else if stalls_per_sec > 10.0 {
                "input"
            } else if transform_ratio > 0.3 {
                "transform"
            } else if scan_ratio > 0.3 {
                "scan"
            } else {
                "none"
            };

            buf.push(MetricPoint {
                name: "logfwd.pipeline.bottleneck",
                description: "Current bottleneck stage",
                unit: "1",
                value: MetricValue::U64(1),
                attributes: vec![("pipeline", pipeline.clone()), ("stage", stage.to_string())],
                time_unix_nano: now,
            });
        }
    }

    // Process-level metrics (RSS, CPU).
    if let Some((rss, cpu_user, cpu_sys)) = crate::diagnostics::process_metrics() {
        for (name, desc, unit, val) in [
            ("logfwd.process.rss_bytes", "Resident set size", "By", rss),
            (
                "logfwd.process.cpu_user_ms",
                "User CPU time",
                "ms",
                cpu_user,
            ),
            (
                "logfwd.process.cpu_sys_ms",
                "System CPU time",
                "ms",
                cpu_sys,
            ),
        ] {
            buf.push(MetricPoint {
                name,
                description: desc,
                unit,
                value: MetricValue::U64(val),
                attributes: vec![],
                time_unix_nano: now,
            });
        }
    }

    // Allocator memory stats.
    if let Some(mem) = memory_fn.and_then(|f| f()) {
        for (name, desc, val) in [
            (
                "logfwd.memory.resident",
                "Allocator resident memory",
                mem.resident as u64,
            ),
            (
                "logfwd.memory.allocated",
                "Allocator allocated memory",
                mem.allocated as u64,
            ),
            (
                "logfwd.memory.active",
                "Allocator active memory",
                mem.active as u64,
            ),
        ] {
            buf.push(MetricPoint {
                name,
                description: desc,
                unit: "By",
                value: MetricValue::U64(val),
                attributes: vec![],
                time_unix_nano: now,
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Trace conversion (TraceSpan / ActiveBatch → SpanPoint)
// ---------------------------------------------------------------------------

/// Convert a completed `TraceSpan` (from SpanBuffer) to an OTLP `SpanPoint`.
pub fn trace_span_to_span_point(span: &TraceSpan) -> SpanPoint {
    let status_code = match span.status {
        "ok" => 1,
        "error" => 2,
        _ => 0,
    };
    let status_message = if status_code == 2 {
        span.status.to_string()
    } else {
        String::new()
    };

    SpanPoint {
        trace_id: span.trace_id.clone(),
        span_id: span.span_id.clone(),
        parent_span_id: span.parent_id.clone(),
        name: span.name.clone(),
        start_time_unix_nano: span.start_unix_ns,
        end_time_unix_nano: span.start_unix_ns.saturating_add(span.duration_ns),
        attributes: span
            .attrs
            .iter()
            .map(|[k, v]| (k.clone(), v.clone()))
            .collect(),
        kind: 1, // INTERNAL
        status_code,
        status_message,
    }
}

/// Convert an in-progress `ActiveBatch` to an OTLP `SpanPoint`.
///
/// Uses a synthetic hex trace ID derived from the batch ID. The span's
/// `end_time_unix_nano` is 0 to indicate it is still in progress.
pub fn active_batch_to_span_point(id: u64, batch: &ActiveBatch, pipeline: &str) -> SpanPoint {
    // Mix pipeline name hash into IDs so different pipelines with the same
    // batch ID don't collide.
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    pipeline.hash(&mut hasher);
    let pipeline_hash = hasher.finish();

    let trace_id = format!("{pipeline_hash:016x}{id:016x}");
    let span_id = format!("{:016x}", id ^ pipeline_hash);

    let mut attrs = vec![
        ("pipeline".to_string(), pipeline.to_string()),
        ("in_progress".to_string(), "true".to_string()),
        ("stage".to_string(), batch.stage.to_string()),
        (
            "stage_start_unix_ns".to_string(),
            batch.stage_start_unix_ns.to_string(),
        ),
        ("scan_ns".to_string(), batch.scan_ns.to_string()),
        ("transform_ns".to_string(), batch.transform_ns.to_string()),
    ];
    if let Some(wid) = batch.worker_id {
        attrs.push(("worker_id".to_string(), wid.to_string()));
    }

    SpanPoint {
        trace_id,
        span_id,
        parent_span_id: "0000000000000000".to_string(),
        name: "batch".to_string(),
        start_time_unix_nano: batch.start_unix_ns,
        end_time_unix_nano: 0, // in-progress
        attributes: attrs,
        kind: 1,        // INTERNAL
        status_code: 0, // UNSET (still running)
        status_message: String::new(),
    }
}

/// Build a complete list of SpanPoints from SpanBuffer + active batches,
/// ready for OTLP JSON serialization. Called on-demand by the traces endpoint.
pub fn collect_all_spans(
    trace_buf: Option<&crate::span_exporter::SpanBuffer>,
    pipelines: &[Arc<PipelineMetrics>],
) -> Vec<SpanPoint> {
    let mut spans = Vec::new();

    // Completed spans from SpanBuffer.
    if let Some(buf) = trace_buf {
        for span in buf.get_spans() {
            spans.push(trace_span_to_span_point(&span));
        }
    }

    // In-progress spans from active batches.
    for pm in pipelines {
        if let Ok(active) = pm.active_batches.lock() {
            for (&id, batch) in active.iter() {
                spans.push(active_batch_to_span_point(id, batch, &pm.name));
            }
        }
    }

    spans
}

// ---------------------------------------------------------------------------
// Log sampling (stderr + health transitions)
// ---------------------------------------------------------------------------

/// Sample new stderr lines into the log buffer.
///
/// Tracks the last seen line count to avoid re-pushing already-sampled lines.
/// Each new line becomes a `LogPoint` with `Severity::Info`.
pub fn sample_stderr_logs(
    stderr: &crate::stderr_capture::StderrCapture,
    buf: &RingBuffer<LogPoint>,
    last_cursor: &mut u64,
) {
    let (new_lines, new_cursor) = stderr.get_logs_since(*last_cursor);
    *last_cursor = new_cursor;
    let now = now_nanos();

    for line in new_lines {
        buf.push(LogPoint {
            severity: Severity::Info,
            body: line,
            attributes: vec![("source", "stderr".to_string())],
            time_unix_nano: now,
        });
    }
}

/// Detect component health transitions and emit log records for changes.
///
/// Compares current health state against `prev_health` map. On change, pushes
/// a `LogPoint` describing the transition. Updates the map in place.
pub fn sample_health_transitions<S: std::hash::BuildHasher>(
    pipelines: &[Arc<PipelineMetrics>],
    buf: &RingBuffer<LogPoint>,
    prev_health: &mut HashMap<(String, String, String), u8, S>,
) {
    use crate::diagnostics::ComponentHealth;
    let now = now_nanos();

    for pm in pipelines {
        // Check all input + output components.
        let components: Vec<(&str, &str, u8)> = pm
            .inputs
            .iter()
            .map(|(name, _, stats)| (name.as_str(), "input", stats.health() as u8))
            .chain(
                pm.outputs
                    .iter()
                    .map(|(name, _, stats)| (name.as_str(), "output", stats.health() as u8)),
            )
            .collect();

        for (name, role, health_val) in components {
            let key = (pm.name.clone(), role.to_string(), name.to_string());
            let prev = prev_health.entry(key).or_insert(health_val);
            if *prev != health_val {
                let old_name = ComponentHealth::from_repr(*prev);
                let new_name = ComponentHealth::from_repr(health_val);
                let severity = if health_val > *prev {
                    Severity::Warn // degradation
                } else {
                    Severity::Info // recovery
                };
                buf.push(LogPoint {
                    severity,
                    body: format!(
                        "component '{name}' ({role}) health changed: {old_name:?} -> {new_name:?}"
                    ),
                    attributes: vec![
                        ("pipeline", pm.name.clone()),
                        ("component", name.to_string()),
                        ("role", role.to_string()),
                    ],
                    time_unix_nano: now,
                });
                *prev = health_val;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

fn attrs_to_json(attrs: &[(&str, String)]) -> String {
    if attrs.is_empty() {
        return "\"attributes\":[]".to_string();
    }
    let kvs: Vec<String> = attrs
        .iter()
        .map(|(k, v)| {
            format!(
                "{{\"key\":\"{}\",\"value\":{{\"stringValue\":\"{}\"}}}}",
                json_escape(k),
                json_escape(v)
            )
        })
        .collect();
    format!("\"attributes\":[{}]", kvs.join(","))
}

fn string_attrs_to_json(attrs: &[(String, String)]) -> String {
    if attrs.is_empty() {
        return "\"attributes\":[]".to_string();
    }
    let kvs: Vec<String> = attrs
        .iter()
        .map(|(k, v)| {
            format!(
                "{{\"key\":\"{}\",\"value\":{{\"stringValue\":\"{}\"}}}}",
                json_escape(k),
                json_escape(v)
            )
        })
        .collect();
    format!("\"attributes\":[{}]", kvs.join(","))
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_buffer_evicts_oldest() {
        let buf = RingBuffer::new(3);
        buf.push(1u32);
        buf.push(2);
        buf.push(3);
        buf.push(4); // evicts 1
        let snapshot = buf.snapshot();
        assert_eq!(snapshot, vec![2, 3, 4]);
    }

    #[test]
    fn metrics_to_otlp_json_empty() {
        let json = metrics_to_otlp_json(&[]);
        assert_eq!(json, r#"{"resourceMetrics":[]}"#);
    }

    #[test]
    fn metrics_to_otlp_json_single_gauge() {
        let points = vec![MetricPoint {
            name: "logfwd.input.lines",
            description: "Total lines read",
            unit: "1",
            value: MetricValue::U64(42),
            attributes: vec![("pipeline", "default".to_string())],
            time_unix_nano: 1_000_000_000,
        }];
        let json = metrics_to_otlp_json(&points);

        // Verify it's valid JSON
        let parsed: serde_json::Value =
            serde_json::from_str(&json).expect("metrics OTLP JSON must be valid");

        // Verify OTLP structure
        let rm = &parsed["resourceMetrics"][0];
        assert_eq!(rm["resource"]["attributes"][0]["key"], "service.name");
        assert_eq!(
            rm["resource"]["attributes"][0]["value"]["stringValue"],
            "logfwd"
        );

        let metric = &rm["scopeMetrics"][0]["metrics"][0];
        assert_eq!(metric["name"], "logfwd.input.lines");
        assert_eq!(metric["unit"], "1");

        let dp = &metric["gauge"]["dataPoints"][0];
        assert_eq!(dp["asInt"], "42");
        assert_eq!(dp["timeUnixNano"], "1000000000");
        assert_eq!(dp["attributes"][0]["key"], "pipeline");
        assert_eq!(dp["attributes"][0]["value"]["stringValue"], "default");
    }

    #[test]
    fn metrics_to_otlp_json_multiple_metrics_grouped() {
        let points = vec![
            MetricPoint {
                name: "logfwd.input.lines",
                description: "Lines",
                unit: "1",
                value: MetricValue::U64(100),
                attributes: vec![("pipeline", "p1".to_string())],
                time_unix_nano: 1_000,
            },
            MetricPoint {
                name: "logfwd.input.bytes",
                description: "Bytes",
                unit: "By",
                value: MetricValue::U64(5000),
                attributes: vec![("pipeline", "p1".to_string())],
                time_unix_nano: 1_000,
            },
            MetricPoint {
                name: "logfwd.input.lines",
                description: "Lines",
                unit: "1",
                value: MetricValue::U64(200),
                attributes: vec![("pipeline", "p2".to_string())],
                time_unix_nano: 1_000,
            },
        ];
        let json = metrics_to_otlp_json(&points);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        let metrics = parsed["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
            .as_array()
            .unwrap();
        // Two unique metric names: input.lines (2 data points) and input.bytes (1 data point)
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0]["name"], "logfwd.input.lines");
        assert_eq!(
            metrics[0]["gauge"]["dataPoints"].as_array().unwrap().len(),
            2
        );
        assert_eq!(metrics[1]["name"], "logfwd.input.bytes");
        assert_eq!(
            metrics[1]["gauge"]["dataPoints"].as_array().unwrap().len(),
            1
        );
    }

    #[test]
    fn traces_to_otlp_json_roundtrip() {
        let spans = vec![SpanPoint {
            trace_id: "aabbccdd00112233aabbccdd00112233".to_string(),
            span_id: "aabbccdd00112233".to_string(),
            parent_span_id: "0000000000000000".to_string(),
            name: "batch".to_string(),
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 1_200_000_000,
            attributes: vec![
                ("pipeline".to_string(), "default".to_string()),
                ("flush_reason".to_string(), "size".to_string()),
            ],
            kind: 1,
            status_code: 1,
            status_message: "ok".to_string(),
        }];

        let json = traces_to_otlp_json(&spans);
        let parsed: serde_json::Value =
            serde_json::from_str(&json).expect("traces OTLP JSON must be valid");

        let span = &parsed["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert_eq!(span["traceId"], "aabbccdd00112233aabbccdd00112233");
        assert_eq!(span["name"], "batch");
        assert_eq!(span["startTimeUnixNano"], "1000000000");
        assert_eq!(span["endTimeUnixNano"], "1200000000");
        assert_eq!(span["kind"], 1);
        assert_eq!(span["status"]["code"], 1);
    }

    #[test]
    fn logs_to_otlp_json_roundtrip() {
        let logs = vec![LogPoint {
            severity: Severity::Warn,
            body: "backpressure detected on output \"elasticsearch\"".to_string(),
            attributes: vec![("pipeline", "default".to_string())],
            time_unix_nano: 1_000_000_000,
        }];

        let json = logs_to_otlp_json(&logs);
        let parsed: serde_json::Value =
            serde_json::from_str(&json).expect("logs OTLP JSON must be valid");

        let lr = &parsed["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0];
        assert_eq!(lr["severityNumber"], 13); // WARN
        assert!(
            lr["body"]["stringValue"]
                .as_str()
                .unwrap()
                .contains("backpressure")
        );
        assert_eq!(lr["attributes"][0]["key"], "pipeline");
    }

    #[test]
    fn json_escape_special_chars() {
        assert_eq!(json_escape("hello\nworld"), "hello\\nworld");
        assert_eq!(json_escape("say \"hi\""), "say \\\"hi\\\"");
        assert_eq!(json_escape("back\\slash"), "back\\\\slash");
        assert_eq!(json_escape("tab\there"), "tab\\there");
    }

    #[test]
    fn sample_pipeline_metrics_populates_buffer() {
        let meter = opentelemetry::global::meter("test");
        let pm = PipelineMetrics::new("test-pipeline", "", &meter);

        // Simulate some activity
        pm.batches_total.store(10, Ordering::Relaxed);
        pm.backpressure_stalls.store(3, Ordering::Relaxed);
        pm.cadence_fast_repolls.store(5, Ordering::Relaxed);
        pm.cadence_idle_sleeps.store(2, Ordering::Relaxed);
        pm.scan_nanos_total.store(500_000_000, Ordering::Relaxed);

        let buf = RingBuffer::new(1024);
        sample_pipeline_metrics(&[Arc::new(pm)], &buf);

        let points = buf.snapshot();
        assert!(
            !points.is_empty(),
            "sample_pipeline_metrics must produce points"
        );

        // Verify a specific metric exists
        let batch_total = points
            .iter()
            .find(|p| p.name == "logfwd.batch.total")
            .expect("logfwd.batch.total must be sampled");
        match batch_total.value {
            MetricValue::U64(v) => assert_eq!(v, 10),
            _ => panic!("expected U64"),
        }

        // Verify backpressure is included
        let bp = points
            .iter()
            .find(|p| p.name == "logfwd.backpressure.stalls")
            .expect("logfwd.backpressure.stalls must be sampled");
        match bp.value {
            MetricValue::U64(v) => assert_eq!(v, 3),
            _ => panic!("expected U64"),
        }

        let fast_repolls = points
            .iter()
            .find(|p| p.name == "logfwd.input.poll_cadence.fast_repolls")
            .expect("logfwd.input.poll_cadence.fast_repolls must be sampled");
        match fast_repolls.value {
            MetricValue::U64(v) => assert_eq!(v, 5),
            _ => panic!("expected U64"),
        }

        let idle_sleeps = points
            .iter()
            .find(|p| p.name == "logfwd.input.poll_cadence.idle_sleeps")
            .expect("logfwd.input.poll_cadence.idle_sleeps must be sampled");
        match idle_sleeps.value {
            MetricValue::U64(v) => assert_eq!(v, 2),
            _ => panic!("expected U64"),
        }

        // All points should have pipeline attribute
        for p in &points {
            assert!(
                p.attributes.iter().any(|(k, _)| *k == "pipeline"),
                "metric {} missing pipeline attribute",
                p.name
            );
        }

        // The full roundtrip: sample → serialize → parse as valid OTLP JSON
        let json = metrics_to_otlp_json(&points);
        let _parsed: serde_json::Value =
            serde_json::from_str(&json).expect("sampled metrics must produce valid OTLP JSON");
    }

    // -- Step 2: sample_all_metrics tests --

    #[test]
    fn sample_all_metrics_includes_process_and_memory() {
        let meter = opentelemetry::global::meter("test");
        let pm = PipelineMetrics::new("p", "", &meter);
        let buffers = TelemetryBuffers::new();

        let mem_fn: fn() -> Option<MemoryStats> = || {
            Some(MemoryStats {
                resident: 1_000_000,
                allocated: 800_000,
                active: 900_000,
            })
        };

        sample_all_metrics(&[Arc::new(pm)], Some(mem_fn), 10.0, &buffers);
        let points = buffers.metrics.snapshot();

        // Memory stats should be present.
        assert!(
            points.iter().any(|p| p.name == "logfwd.memory.resident"),
            "expected logfwd.memory.resident metric"
        );
        assert!(
            points.iter().any(|p| p.name == "logfwd.memory.allocated"),
            "expected logfwd.memory.allocated metric"
        );
        assert!(
            points.iter().any(|p| p.name == "logfwd.memory.active"),
            "expected logfwd.memory.active metric"
        );

        // Verify memory values.
        let resident = points
            .iter()
            .find(|p| p.name == "logfwd.memory.resident")
            .unwrap();
        match resident.value {
            MetricValue::U64(v) => assert_eq!(v, 1_000_000),
            _ => panic!("expected U64"),
        }

        // Pipeline-level error counters should be present.
        assert!(
            points
                .iter()
                .any(|p| p.name == "logfwd.pipeline.transform_errors"),
            "expected logfwd.pipeline.transform_errors"
        );

        // The full roundtrip produces valid OTLP JSON.
        let json = metrics_to_otlp_json(&points);
        let _: serde_json::Value =
            serde_json::from_str(&json).expect("sample_all_metrics must produce valid OTLP JSON");
    }

    #[test]
    fn sample_all_metrics_includes_component_health() {
        use crate::diagnostics::ComponentHealth;

        let meter = opentelemetry::global::meter("test");
        let mut pm = PipelineMetrics::new("p", "", &meter);
        let inp = pm.add_input("file_input", "file");
        inp.set_health(ComponentHealth::Degraded);
        let out = pm.add_output("es_output", "elasticsearch");
        out.set_health(ComponentHealth::Healthy);

        let buffers = TelemetryBuffers::new();
        sample_all_metrics(&[Arc::new(pm)], None, 10.0, &buffers);
        let points = buffers.metrics.snapshot();

        let health_points: Vec<_> = points
            .iter()
            .filter(|p| p.name == "logfwd.component.health")
            .collect();

        // Should have 2 health gauges (one per component).
        assert_eq!(
            health_points.len(),
            2,
            "expected 2 component health gauges, got {}",
            health_points.len()
        );

        // The input (Degraded = 1) should have value 1.
        let input_health = health_points
            .iter()
            .find(|p| {
                p.attributes
                    .iter()
                    .any(|(k, v)| *k == "component" && v == "file_input")
            })
            .expect("expected health gauge for file_input");
        match input_health.value {
            MetricValue::U64(v) => assert_eq!(v, ComponentHealth::Degraded as u64),
            _ => panic!("expected U64"),
        }
    }

    #[test]
    fn sample_all_metrics_includes_bottleneck() {
        let meter = opentelemetry::global::meter("test");
        let pm = PipelineMetrics::new("p", "", &meter);
        // Simulate high queue wait → output bottleneck.
        pm.queue_wait_nanos_total
            .store(10_000_000_000, Ordering::Relaxed); // 10s queue wait
        let uptime_secs = 10.0; // 10s uptime → ratio = 1.0 > 0.5

        let buffers = TelemetryBuffers::new();
        sample_all_metrics(&[Arc::new(pm)], None, uptime_secs, &buffers);
        let points = buffers.metrics.snapshot();

        let bottleneck = points
            .iter()
            .find(|p| p.name == "logfwd.pipeline.bottleneck")
            .expect("expected logfwd.pipeline.bottleneck gauge");

        let stage_attr = bottleneck
            .attributes
            .iter()
            .find(|(k, _)| *k == "stage")
            .expect("bottleneck must have stage attribute");
        assert_eq!(stage_attr.1, "output", "expected output bottleneck");
    }

    // -- Step 3: trace conversion tests --

    #[test]
    fn trace_span_to_span_point_field_mapping() {
        let span = TraceSpan {
            trace_id: "aabbccdd00112233aabbccdd00112233".to_string(),
            span_id: "1122334455667788".to_string(),
            parent_id: "0000000000000000".to_string(),
            name: "batch".to_string(),
            start_unix_ns: 1_000_000_000,
            duration_ns: 200_000_000,
            attrs: vec![
                ["pipeline".to_string(), "default".to_string()],
                ["flush_reason".to_string(), "size".to_string()],
            ],
            status: "ok",
        };

        let sp = trace_span_to_span_point(&span);
        assert_eq!(sp.trace_id, "aabbccdd00112233aabbccdd00112233");
        assert_eq!(sp.span_id, "1122334455667788");
        assert_eq!(sp.parent_span_id, "0000000000000000");
        assert_eq!(sp.name, "batch");
        assert_eq!(sp.start_time_unix_nano, 1_000_000_000);
        assert_eq!(sp.end_time_unix_nano, 1_200_000_000);
        assert_eq!(sp.kind, 1); // INTERNAL
        assert_eq!(sp.status_code, 1); // OK
        assert!(sp.status_message.is_empty());
        assert_eq!(sp.attributes.len(), 2);
        assert_eq!(
            sp.attributes[0],
            ("pipeline".to_string(), "default".to_string())
        );
    }

    #[test]
    fn trace_span_error_status_maps_to_code_2() {
        let span = TraceSpan {
            trace_id: "a".repeat(32),
            span_id: "b".repeat(16),
            parent_id: "0".repeat(16),
            name: "batch".to_string(),
            start_unix_ns: 0,
            duration_ns: 100,
            attrs: vec![],
            status: "error",
        };
        let sp = trace_span_to_span_point(&span);
        assert_eq!(sp.status_code, 2); // ERROR
        assert_eq!(sp.status_message, "error");
    }

    #[test]
    fn active_batch_to_span_point_in_progress() {
        let batch = ActiveBatch {
            start_unix_ns: 5_000_000_000,
            scan_ns: 100_000,
            transform_ns: 200_000,
            stage: "output",
            stage_start_unix_ns: 5_000_300_000,
            worker_id: Some(3),
            output_start_unix_ns: 5_000_300_000,
        };

        let sp = active_batch_to_span_point(42, &batch, "default");

        // Synthetic trace ID is valid 32-char hex.
        assert_eq!(sp.trace_id.len(), 32);
        assert!(sp.trace_id.chars().all(|c| c.is_ascii_hexdigit()));

        // In-progress: end_time is 0, status is UNSET.
        assert_eq!(sp.end_time_unix_nano, 0);
        assert_eq!(sp.status_code, 0);

        // Has in_progress attribute.
        assert!(
            sp.attributes
                .iter()
                .any(|(k, v)| k == "in_progress" && v == "true")
        );

        // Has stage and worker_id attributes.
        assert!(
            sp.attributes
                .iter()
                .any(|(k, v)| k == "stage" && v == "output")
        );
        assert!(
            sp.attributes
                .iter()
                .any(|(k, v)| k == "worker_id" && v == "3")
        );

        // Serializes to valid OTLP JSON.
        let json = traces_to_otlp_json(&[sp]);
        let parsed: serde_json::Value =
            serde_json::from_str(&json).expect("active batch span must produce valid OTLP JSON");
        let span_json = &parsed["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert_eq!(span_json["endTimeUnixNano"], "0");
    }

    // -- Step 4: log tests --

    #[test]
    fn health_transition_emits_log() {
        use crate::diagnostics::ComponentHealth;

        let meter = opentelemetry::global::meter("test");
        let mut pm = PipelineMetrics::new("p", "", &meter);
        let inp = pm.add_input("src", "file");
        // inp is Arc<ComponentStats>, shared with pm.

        inp.set_health(ComponentHealth::Healthy);
        let pm = Arc::new(pm);

        let buf = RingBuffer::new(100);
        let mut prev = HashMap::new();

        // First call: no transition (initializes baseline).
        sample_health_transitions(&[Arc::clone(&pm)], &buf, &mut prev);
        assert!(buf.snapshot().is_empty(), "no log on first sample");

        // Now degrade the component via the shared Arc<ComponentStats>.
        inp.set_health(ComponentHealth::Degraded);
        sample_health_transitions(&[Arc::clone(&pm)], &buf, &mut prev);
        let logs = buf.snapshot();
        assert_eq!(logs.len(), 1, "expected 1 health transition log");
        assert!(
            logs[0].body.contains("Healthy"),
            "log body should mention old state: {}",
            logs[0].body
        );
        assert!(
            logs[0].body.contains("Degraded"),
            "log body should mention new state: {}",
            logs[0].body
        );
        assert!(
            matches!(logs[0].severity, Severity::Warn),
            "degradation should be Warn severity"
        );
    }

    // -- OTLP spec compliance tests --

    #[test]
    fn otlp_json_64bit_integers_as_strings() {
        let points = vec![MetricPoint {
            name: "logfwd.big_value",
            description: "big",
            unit: "1",
            value: MetricValue::U64(u64::MAX),
            attributes: vec![],
            time_unix_nano: u64::MAX,
        }];
        let json = metrics_to_otlp_json(&points);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let dp = &parsed["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]["gauge"]["dataPoints"]
            [0];

        // Per OTLP spec: 64-bit integers are decimal strings.
        assert_eq!(dp["asInt"], u64::MAX.to_string());
        assert_eq!(dp["timeUnixNano"], u64::MAX.to_string());
    }

    #[test]
    fn otlp_json_trace_ids_are_hex() {
        let span = TraceSpan {
            trace_id: "aabbccdd00112233aabbccdd00112233".to_string(),
            span_id: "1122334455667788".to_string(),
            parent_id: "0000000000000000".to_string(),
            name: "test".to_string(),
            start_unix_ns: 1000,
            duration_ns: 500,
            attrs: vec![],
            status: "ok",
        };
        let sp = trace_span_to_span_point(&span);
        let json = traces_to_otlp_json(&[sp]);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let s = &parsed["resourceSpans"][0]["scopeSpans"][0]["spans"][0];

        // Trace IDs must be hex (not base64).
        let tid = s["traceId"].as_str().unwrap();
        assert!(
            tid.chars().all(|c| c.is_ascii_hexdigit()),
            "traceId must be hex, got: {tid}"
        );
        assert_eq!(tid.len(), 32, "traceId must be 32 hex chars");

        let sid = s["spanId"].as_str().unwrap();
        assert!(
            sid.chars().all(|c| c.is_ascii_hexdigit()),
            "spanId must be hex, got: {sid}"
        );
        assert_eq!(sid.len(), 16, "spanId must be 16 hex chars");
    }

    #[test]
    fn otlp_json_enums_are_integers() {
        // Span kind, status code, severity must be integers (not strings).
        let span = SpanPoint {
            trace_id: "a".repeat(32),
            span_id: "b".repeat(16),
            parent_span_id: "0".repeat(16),
            name: "test".to_string(),
            start_time_unix_nano: 1000,
            end_time_unix_nano: 2000,
            attributes: vec![],
            kind: 2,
            status_code: 1,
            status_message: String::new(),
        };
        let json = traces_to_otlp_json(&[span]);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let s = &parsed["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert!(s["kind"].is_number(), "kind must be integer");
        assert_eq!(s["kind"], 2);
        assert!(
            s["status"]["code"].is_number(),
            "status.code must be integer"
        );

        // Severity in logs.
        let log = LogPoint {
            severity: Severity::Error,
            body: "test".to_string(),
            attributes: vec![],
            time_unix_nano: 1000,
        };
        let json = logs_to_otlp_json(&[log]);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let lr = &parsed["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0];
        assert!(
            lr["severityNumber"].is_number(),
            "severityNumber must be integer"
        );
        assert_eq!(lr["severityNumber"], 17); // ERROR
    }

    /// Regression: attrs_to_json must properly escape keys containing quotes.
    /// Before the fix, unescaped quotes in keys would produce invalid JSON.
    #[test]
    fn attrs_to_json_key_with_quote_produces_valid_json() {
        let attrs = vec![("key\"with\"quotes", "normal_value".to_string())];
        let json_fragment = attrs_to_json(&attrs);
        // Wrap in braces to make it parseable as a JSON object.
        let json = format!("{{{json_fragment}}}");
        let parsed: serde_json::Value = serde_json::from_str(&json)
            .expect("attrs_to_json with quoted key must produce valid JSON");
        let arr = parsed["attributes"].as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["key"], "key\"with\"quotes");
        assert_eq!(arr[0]["value"]["stringValue"], "normal_value");
    }
}

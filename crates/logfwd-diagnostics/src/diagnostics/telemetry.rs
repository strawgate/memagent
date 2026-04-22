//! OTLP JSON telemetry: sampling, collection, and serialization.
//!
//! The diagnostics WebSocket pushes three OTLP JSON signals every 2 s:
//! - `resourceMetrics` — gauge/sum snapshot of pipeline + process metrics
//! - `resourceSpans`  — completed batch spans + in-progress active batches
//! - `resourceLogs`   — new stderr lines since last push

use std::fmt::Write;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use super::metrics::PipelineMetrics;
use super::models::MemoryStats;
use super::process::process_metrics;
use super::render::{esc, now_nanos};

// ---------------------------------------------------------------------------
// Intermediate types (not OTLP — just collection containers)
// ---------------------------------------------------------------------------

/// Whether a metric is a monotonic counter or a point-in-time gauge.
pub(super) enum MetricValue {
    /// A point-in-time measurement (e.g. inflight batches, memory usage).
    Gauge(f64),
    /// A monotonically increasing counter (e.g. input lines, output bytes).
    Sum(u64),
}

/// A single metric observation with optional key-value attributes.
pub(super) struct MetricPoint {
    pub name: &'static str,
    pub value: MetricValue,
    pub attributes: Vec<(&'static str, String)>,
}

/// A snapshot of metric points with a timestamp, used for rate computation.
pub(super) struct MetricSnapshot {
    pub points: Vec<MetricPoint>,
    pub time: Instant,
}

/// A single span record ready for OTLP JSON serialization.
pub(super) struct SpanRecord {
    pub trace_id: String,
    pub span_id: String,
    pub parent_id: String,
    pub name: String,
    pub start_unix_ns: u64,
    pub duration_ns: u64,
    pub attributes: Vec<(&'static str, String)>,
    pub status: &'static str,
    pub in_progress: bool,
}

/// A single log record ready for OTLP JSON serialization.
pub(super) struct LogRecord {
    pub timestamp_unix_ns: u64,
    pub severity: &'static str,
    pub body: String,
}

// ---------------------------------------------------------------------------
// Sampling / collection
// ---------------------------------------------------------------------------

/// Read all pipeline + process + memory metrics into `MetricPoint` values.
pub(super) fn sample_metrics(
    pipelines: &[Arc<PipelineMetrics>],
    memory_fn: Option<fn() -> Option<MemoryStats>>,
    uptime: Duration,
) -> Vec<MetricPoint> {
    let mut pts = Vec::with_capacity(32);

    pts.push(MetricPoint {
        name: "logfwd.uptime_seconds",
        value: MetricValue::Gauge(uptime.as_secs_f64()),
        attributes: vec![],
    });

    for pm in pipelines {
        let pipe = pm.name.clone();

        let mut pipe_in_lines: u64 = 0;
        let mut pipe_in_bytes: u64 = 0;
        for (_, _, s) in &pm.inputs {
            pipe_in_lines += s.lines();
            pipe_in_bytes += s.bytes();
        }
        let mut pipe_out_lines: u64 = 0;
        let mut pipe_out_bytes: u64 = 0;
        let mut pipe_out_errors: u64 = 0;
        for (_, _, s) in &pm.outputs {
            pipe_out_lines += s.lines();
            pipe_out_bytes += s.bytes();
            pipe_out_errors += s.errors();
        }

        let p_batches = pm.batches_total.load(Ordering::Relaxed);
        let p_batch_rows = pm.batch_rows_total.load(Ordering::Relaxed);
        let p_scan_ns = pm.scan_nanos_total.load(Ordering::Relaxed);
        let p_transform_ns = pm.transform_nanos_total.load(Ordering::Relaxed);
        let p_output_ns = pm.output_nanos_total.load(Ordering::Relaxed);
        let p_queue_ns = pm.queue_wait_nanos_total.load(Ordering::Relaxed);
        let p_send_ns = pm.send_nanos_total.load(Ordering::Relaxed);
        let p_inflight = pm.inflight_batches.load(Ordering::Relaxed);
        let p_backpressure = pm.backpressure_stalls.load(Ordering::Relaxed);
        let p_dropped = pm.dropped_batches_total.load(Ordering::Relaxed);
        let p_scan_errors = pm.scan_errors_total.load(Ordering::Relaxed);
        let p_parse_errors = pm.parse_errors_total.load(Ordering::Relaxed);

        let attr = |extra: &'static str| -> Vec<(&'static str, String)> {
            if extra.is_empty() {
                vec![("pipeline", pipe.clone())]
            } else {
                vec![("pipeline", pipe.clone()), ("stage", extra.to_string())]
            }
        };

        pts.push(MetricPoint {
            name: "logfwd.input_lines",
            value: MetricValue::Sum(pipe_in_lines),
            attributes: attr(""),
        });
        pts.push(MetricPoint {
            name: "logfwd.input_bytes",
            value: MetricValue::Sum(pipe_in_bytes),
            attributes: attr(""),
        });
        pts.push(MetricPoint {
            name: "logfwd.output_lines",
            value: MetricValue::Sum(pipe_out_lines),
            attributes: attr(""),
        });
        pts.push(MetricPoint {
            name: "logfwd.output_bytes",
            value: MetricValue::Sum(pipe_out_bytes),
            attributes: attr(""),
        });
        pts.push(MetricPoint {
            name: "logfwd.output_errors",
            value: MetricValue::Sum(pipe_out_errors),
            attributes: attr(""),
        });
        pts.push(MetricPoint {
            name: "logfwd.batches",
            value: MetricValue::Sum(p_batches),
            attributes: attr(""),
        });
        pts.push(MetricPoint {
            name: "logfwd.batch_rows",
            value: MetricValue::Sum(p_batch_rows),
            attributes: attr(""),
        });
        pts.push(MetricPoint {
            name: "logfwd.stage_nanos",
            value: MetricValue::Sum(p_scan_ns),
            attributes: attr("scan"),
        });
        pts.push(MetricPoint {
            name: "logfwd.stage_nanos",
            value: MetricValue::Sum(p_transform_ns),
            attributes: attr("transform"),
        });
        pts.push(MetricPoint {
            name: "logfwd.stage_nanos",
            value: MetricValue::Sum(p_output_ns),
            attributes: attr("output"),
        });
        pts.push(MetricPoint {
            name: "logfwd.queue_wait_nanos",
            value: MetricValue::Sum(p_queue_ns),
            attributes: attr(""),
        });
        pts.push(MetricPoint {
            name: "logfwd.send_nanos",
            value: MetricValue::Sum(p_send_ns),
            attributes: attr(""),
        });
        pts.push(MetricPoint {
            name: "logfwd.inflight_batches",
            value: MetricValue::Gauge(p_inflight as f64),
            attributes: attr(""),
        });
        pts.push(MetricPoint {
            name: "logfwd.backpressure_stalls",
            value: MetricValue::Sum(p_backpressure),
            attributes: attr(""),
        });
        pts.push(MetricPoint {
            name: "logfwd.dropped_batches",
            value: MetricValue::Sum(p_dropped),
            attributes: attr(""),
        });
        pts.push(MetricPoint {
            name: "logfwd.scan_errors",
            value: MetricValue::Sum(p_scan_errors),
            attributes: attr(""),
        });
        pts.push(MetricPoint {
            name: "logfwd.parse_errors",
            value: MetricValue::Sum(p_parse_errors),
            attributes: attr(""),
        });
    }

    // Process-level metrics.
    if let Some((rss, cpu_user, cpu_sys)) = process_metrics() {
        pts.push(MetricPoint {
            name: "process.memory.rss",
            value: MetricValue::Gauge(rss as f64),
            attributes: vec![],
        });
        pts.push(MetricPoint {
            name: "process.cpu.user_ms",
            value: MetricValue::Sum(cpu_user),
            attributes: vec![],
        });
        pts.push(MetricPoint {
            name: "process.cpu.sys_ms",
            value: MetricValue::Sum(cpu_sys),
            attributes: vec![],
        });
    }

    if let Some(m) = memory_fn.and_then(|f| f()) {
        pts.push(MetricPoint {
            name: "process.memory.resident",
            value: MetricValue::Gauge(m.resident as f64),
            attributes: vec![],
        });
        pts.push(MetricPoint {
            name: "process.memory.allocated",
            value: MetricValue::Gauge(m.allocated as f64),
            attributes: vec![],
        });
        pts.push(MetricPoint {
            name: "process.memory.active",
            value: MetricValue::Gauge(m.active as f64),
            attributes: vec![],
        });
    }

    pts
}

// Rate gauge definitions: (counter_name, rate_gauge_name, multiplier).
// multiplier converts from "per second" to the desired unit (e.g. 60 for per minute).
const RATE_DEFS: &[(&str, &str, f64)] = &[
    ("logfwd.input_lines", "logfwd.input_lines_per_sec", 1.0),
    ("logfwd.input_bytes", "logfwd.input_bytes_per_sec", 1.0),
    ("logfwd.output_bytes", "logfwd.output_bytes_per_sec", 1.0),
    ("logfwd.output_errors", "logfwd.output_errors_per_sec", 1.0),
    ("logfwd.batches", "logfwd.batches_per_min", 60.0),
    (
        "logfwd.backpressure_stalls",
        "logfwd.backpressure_stalls_per_sec",
        1.0,
    ),
];

/// Compute pre-computed rate gauges by diffing the current snapshot against a
/// previous one. Returns gauge `MetricPoint`s for the dashboard's chart layer.
///
/// CPU percent is derived specially: `(Δcpu_user_ms + Δcpu_sys_ms) / Δt_ms / 10`.
pub(super) fn compute_rate_gauges(
    current: &[MetricPoint],
    prev: &MetricSnapshot,
) -> Vec<MetricPoint> {
    let dt = prev.time.elapsed().as_secs_f64();
    if dt <= 0.0 {
        return Vec::new();
    }

    // Build a lookup from (name, pipeline_attr) → Sum value for the previous snapshot.
    let prev_lookup: std::collections::HashMap<(&str, Option<&str>), u64> = prev
        .points
        .iter()
        .filter_map(|p| match p.value {
            MetricValue::Sum(v) => {
                let pipeline = p
                    .attributes
                    .iter()
                    .find(|(k, _)| *k == "pipeline")
                    .map(|(_, v)| v.as_str());
                Some(((p.name, pipeline), v))
            }
            MetricValue::Gauge(_) => None,
        })
        .collect();

    let mut out = Vec::new();

    // Per-pipeline rate gauges.
    for def in RATE_DEFS {
        for p in current {
            if p.name != def.0 {
                continue;
            }
            let MetricValue::Sum(cur_val) = p.value else {
                continue;
            };
            // Skip stage-attributed variants (e.g. stage_nanos with stage=scan).
            if p.attributes.iter().any(|(k, _)| *k == "stage") {
                continue;
            }
            let pipeline = p
                .attributes
                .iter()
                .find(|(k, _)| *k == "pipeline")
                .map(|(_, v)| v.as_str());
            if let Some(&prev_val) = prev_lookup.get(&(def.0, pipeline)) {
                let delta = cur_val.saturating_sub(prev_val);
                let rate = (delta as f64 / dt) * def.2;
                out.push(MetricPoint {
                    name: def.1,
                    value: MetricValue::Gauge(rate),
                    attributes: p.attributes.clone(),
                });
            }
        }
    }

    // CPU percent: (Δuser_ms + Δsys_ms) / Δt_s / 10.
    let cur_user = current
        .iter()
        .find(|p| p.name == "process.cpu.user_ms")
        .and_then(|p| match p.value {
            MetricValue::Sum(v) => Some(v),
            MetricValue::Gauge(_) => None,
        });
    let cur_sys = current
        .iter()
        .find(|p| p.name == "process.cpu.sys_ms")
        .and_then(|p| match p.value {
            MetricValue::Sum(v) => Some(v),
            MetricValue::Gauge(_) => None,
        });
    let prev_user = prev_lookup.get(&("process.cpu.user_ms", None)).copied();
    let prev_sys = prev_lookup.get(&("process.cpu.sys_ms", None)).copied();

    if let (Some(cu), Some(cs), Some(pu), Some(ps)) = (cur_user, cur_sys, prev_user, prev_sys) {
        let delta_ms = (cu.saturating_sub(pu) + cs.saturating_sub(ps)) as f64;
        let cpu_pct = delta_ms / (dt * 1000.0) * 100.0;
        out.push(MetricPoint {
            name: "logfwd.cpu_percent",
            value: MetricValue::Gauge(cpu_pct),
            attributes: vec![],
        });
    }

    out
}

/// Collect completed spans from the trace buffer + in-progress active batches.
/// Collect only NEW completed spans since `last_count`, plus all in-progress
/// active batches. Updates `last_count` for the next call.
pub(super) fn collect_new_spans(
    trace_buf: Option<&crate::span_exporter::SpanBuffer>,
    pipelines: &[Arc<PipelineMetrics>],
    last_count: &mut usize,
) -> Vec<SpanRecord> {
    let mut spans = Vec::new();

    // Only new completed spans since the last call.
    if let Some(buf) = trace_buf {
        for s in buf.get_spans_since(last_count) {
            let mut attrs: Vec<(&'static str, String)> = Vec::new();
            for kv in &s.attrs {
                // Allowlist: only forward known span attributes to the dashboard.
                // Unknown attributes (e.g. internal SDK metadata) are intentionally
                // dropped to keep the OTLP JSON payload compact. Update this list
                // when new user-facing attributes are added to batch spans.
                let key = match kv[0].as_str() {
                    "pipeline" => "pipeline",
                    "bytes_in" => "bytes_in",
                    "input_rows" => "input_rows",
                    "output_rows" => "output_rows",
                    "errors" => "errors",
                    "flush_reason" => "flush_reason",
                    "queue_wait_ns" => "queue_wait_ns",
                    "rows" => "rows",
                    "worker_id" => "worker_id",
                    "send_ns" => "send_ns",
                    "recv_ns" => "recv_ns",
                    "took_ms" => "took_ms",
                    "retries" => "retries",
                    "req_bytes" => "req_bytes",
                    "cmp_bytes" => "cmp_bytes",
                    "resp_bytes" => "resp_bytes",
                    "scan_ns" => "scan_ns",
                    "transform_ns" => "transform_ns",
                    _ => continue,
                };
                attrs.push((key, kv[1].clone()));
            }
            spans.push(SpanRecord {
                trace_id: s.trace_id,
                span_id: s.span_id,
                parent_id: s.parent_id,
                name: s.name,
                start_unix_ns: s.start_unix_ns,
                duration_ns: s.duration_ns,
                attributes: attrs,
                status: s.status,
                in_progress: false,
            });
        }
    }

    // In-progress active batches (always sent in full).
    let now = now_nanos();
    for pm in pipelines {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        pm.name.hash(&mut hasher);
        let pipeline_hash = hasher.finish();

        let active = pm
            .active_batches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (id, b) in active.iter() {
            let elapsed = now.saturating_sub(b.start_unix_ns);
            let mut attrs: Vec<(&'static str, String)> = vec![
                ("pipeline", pm.name.clone()),
                ("stage", b.stage.to_string()),
                ("stage_start_unix_ns", b.stage_start_unix_ns.to_string()),
                ("scan_ns", b.scan_ns.to_string()),
                ("transform_ns", b.transform_ns.to_string()),
            ];
            if let Some(wid) = b.worker_id {
                attrs.push(("worker_id", wid.to_string()));
            }
            if b.output_start_unix_ns > 0 {
                attrs.push(("output_start_unix_ns", b.output_start_unix_ns.to_string()));
            }
            spans.push(SpanRecord {
                trace_id: format!("{pipeline_hash:016x}{id:016x}"),
                span_id: format!("{:016x}", id ^ pipeline_hash),
                parent_id: "0000000000000000".to_string(),
                name: "batch".to_string(),
                start_unix_ns: b.start_unix_ns,
                duration_ns: elapsed,
                attributes: attrs,
                status: "unset",
                in_progress: true,
            });
        }
    }

    spans
}

/// Collect new stderr log lines since `last_cursor`.
/// Updates `last_cursor` to the new position. The cursor is monotonic and
/// survives ring-buffer eviction, unlike plain `len()`.
pub(super) fn collect_new_logs(
    stderr: &crate::stderr_capture::StderrCapture,
    last_cursor: &mut u64,
) -> Vec<LogRecord> {
    let (new_lines, new_cursor) = stderr.get_logs_since(*last_cursor);
    *last_cursor = new_cursor;
    let ts = now_nanos();
    // All stderr lines share a single timestamp and "INFO" severity because
    // the capture layer cannot reliably parse log levels from arbitrary stderr
    // output. A per-line parser could be added later if severity matters.
    new_lines
        .into_iter()
        .map(|line| LogRecord {
            timestamp_unix_ns: ts,
            severity: "INFO",
            body: line,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// OTLP JSON serialization
// ---------------------------------------------------------------------------

/// Serialize metric points into an OTLP JSON `ExportMetricsServiceRequest`.
pub(super) fn metrics_to_otlp_json(points: &[MetricPoint]) -> String {
    let now = now_nanos();
    let mut out = String::with_capacity(4096);
    out.push_str(r#"{"resourceMetrics":[{"resource":{"attributes":[]},"scopeMetrics":[{"scope":{"name":"logfwd.diagnostics"},"metrics":["#);

    for (i, pt) in points.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(r#"{"name":""#);
        out.push_str(pt.name);
        out.push('"');

        match &pt.value {
            MetricValue::Gauge(v) => {
                out.push_str(r#","gauge":{"dataPoints":[{"timeUnixNano":""#);
                let _ = write!(out, "{now}");
                out.push_str(r#"","asDouble":"#);
                if !v.is_finite() {
                    let _ = write!(out, "null");
                } else if v.fract() == 0.0 && *v >= i64::MIN as f64 && *v <= i64::MAX as f64 {
                    let vi = *v as i64;
                    let _ = write!(out, "{vi}");
                } else {
                    let _ = write!(out, "{v}");
                }
                write_attributes(&mut out, &pt.attributes);
                out.push_str("}]}");
            }
            MetricValue::Sum(v) => {
                out.push_str(r#","sum":{"dataPoints":[{"timeUnixNano":""#);
                let _ = write!(out, "{now}");
                out.push_str(r#"","asInt":""#);
                let _ = write!(out, "{v}");
                out.push('"');
                write_attributes(&mut out, &pt.attributes);
                out.push_str(r#"}],"aggregationTemporality":2,"isMonotonic":true}"#);
            }
        }

        out.push('}');
    }

    out.push_str("]}]}]}");
    out
}

/// Serialize span records into an OTLP JSON `ExportTraceServiceRequest`.
pub(super) fn spans_to_otlp_json(spans: &[SpanRecord]) -> String {
    let mut out = String::with_capacity(8192);
    out.push_str(r#"{"resourceSpans":[{"resource":{"attributes":[]},"scopeSpans":[{"scope":{"name":"logfwd.diagnostics"},"spans":["#);

    for (i, s) in spans.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(r#"{"traceId":""#);
        out.push_str(&esc(&s.trace_id));
        out.push_str(r#"","spanId":""#);
        out.push_str(&esc(&s.span_id));
        out.push_str(r#"","parentSpanId":""#);
        out.push_str(&esc(&s.parent_id));
        out.push_str(r#"","name":""#);
        out.push_str(&esc(&s.name));
        out.push_str(r#"","startTimeUnixNano":""#);
        let _ = write!(out, "{}", s.start_unix_ns);
        out.push_str(r#"","endTimeUnixNano":""#);
        let _ = write!(out, "{}", s.start_unix_ns.saturating_add(s.duration_ns));
        out.push('"');

        out.push_str(r#","attributes":["#);
        let mut first_attr = true;
        for (k, v) in &s.attributes {
            if !first_attr {
                out.push(',');
            }
            first_attr = false;
            out.push_str(r#"{"key":""#);
            out.push_str(k);
            out.push_str(r#"","value":{"stringValue":""#);
            out.push_str(&esc(v));
            out.push_str(r#""}}"#);
        }
        if s.in_progress {
            if !first_attr {
                out.push(',');
            }
            out.push_str(r#"{"key":"in_progress","value":{"boolValue":true}}"#);
        }
        out.push(']');

        let status_code = match s.status {
            "ok" => 1,
            "error" => 2,
            _ => 0,
        };
        let _ = write!(out, r#","status":{{"code":{status_code}}}}}"#);
    }

    out.push_str("]}]}]}");
    out
}

/// Serialize log records into an OTLP JSON `ExportLogsServiceRequest`.
pub(super) fn logs_to_otlp_json(logs: &[LogRecord]) -> String {
    let mut out = String::with_capacity(2048);
    out.push_str(r#"{"resourceLogs":[{"resource":{"attributes":[]},"scopeLogs":[{"scope":{"name":"logfwd.diagnostics"},"logRecords":["#);

    for (i, log) in logs.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(r#"{"timeUnixNano":""#);
        let _ = write!(out, "{}", log.timestamp_unix_ns);
        out.push_str(r#"","severityText":""#);
        out.push_str(log.severity);
        out.push_str(r#"","body":{"stringValue":""#);
        out.push_str(&esc(&log.body));
        out.push_str(r#""}}"#);
    }

    out.push_str("]}]}]}");
    out
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn write_attributes(out: &mut String, attrs: &[(&str, String)]) {
    if attrs.is_empty() {
        return;
    }
    out.push_str(r#","attributes":["#);
    for (i, (k, v)) in attrs.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(r#"{"key":""#);
        out.push_str(k);
        out.push_str(r#"","value":{"stringValue":""#);
        out.push_str(&esc(v));
        out.push_str(r#""}}"#);
    }
    out.push(']');
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_json_is_valid_otlp() {
        let pts = vec![
            MetricPoint {
                name: "test.gauge",
                value: MetricValue::Gauge(42.5),
                attributes: vec![("host", "localhost".into())],
            },
            MetricPoint {
                name: "test.sum",
                value: MetricValue::Sum(100),
                attributes: vec![("pipeline", "default".into())],
            },
        ];
        let json = metrics_to_otlp_json(&pts);
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
        let metrics = &parsed["resourceMetrics"][0]["scopeMetrics"][0]["metrics"];
        assert_eq!(metrics.as_array().unwrap().len(), 2);
        // Gauge point has asDouble.
        assert!(
            metrics[0]["gauge"]["dataPoints"][0]["asDouble"]
                .as_f64()
                .is_some()
        );
        // asInt is a string per OTLP JSON spec (64-bit safe).
        assert_eq!(
            metrics[1]["sum"]["dataPoints"][0]["asInt"].as_str(),
            Some("100")
        );
    }

    #[test]
    fn spans_json_is_valid_otlp() {
        let spans = vec![SpanRecord {
            trace_id: "abc123".into(),
            span_id: "def456".into(),
            parent_id: "0000000000000000".into(),
            name: "batch".into(),
            start_unix_ns: 1_000_000_000,
            duration_ns: 500_000,
            attributes: vec![("pipeline", "default".into())],
            status: "ok",
            in_progress: false,
        }];
        let json = spans_to_otlp_json(&spans);
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
        let otlp_spans = &parsed["resourceSpans"][0]["scopeSpans"][0]["spans"];
        assert_eq!(otlp_spans.as_array().unwrap().len(), 1);
        assert_eq!(otlp_spans[0]["name"], "batch");
        assert_eq!(otlp_spans[0]["status"]["code"], 1);
    }

    #[test]
    fn in_progress_span_has_flag() {
        let spans = vec![SpanRecord {
            trace_id: "aaa".into(),
            span_id: "bbb".into(),
            parent_id: "0000000000000000".into(),
            name: "batch".into(),
            start_unix_ns: 1_000_000_000,
            duration_ns: 100_000,
            attributes: vec![],
            status: "unset",
            in_progress: true,
        }];
        let json = spans_to_otlp_json(&spans);
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
        let attrs = &parsed["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"];
        let ip = attrs
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["key"] == "in_progress")
            .expect("in_progress attribute");
        assert_eq!(ip["value"]["boolValue"], true);
    }

    #[test]
    fn logs_json_is_valid_otlp() {
        let logs = vec![LogRecord {
            timestamp_unix_ns: 999,
            severity: "WARN",
            body: "test line".into(),
        }];
        let json = logs_to_otlp_json(&logs);
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
        let records = &parsed["resourceLogs"][0]["scopeLogs"][0]["logRecords"];
        assert_eq!(records.as_array().unwrap().len(), 1);
        assert_eq!(records[0]["severityText"], "WARN");
    }

    #[test]
    fn empty_collections_produce_valid_json() {
        let m = metrics_to_otlp_json(&[]);
        let _: serde_json::Value = serde_json::from_str(&m).expect("empty metrics valid JSON");

        let s = spans_to_otlp_json(&[]);
        let _: serde_json::Value = serde_json::from_str(&s).expect("empty spans valid JSON");

        let l = logs_to_otlp_json(&[]);
        let _: serde_json::Value = serde_json::from_str(&l).expect("empty logs valid JSON");
    }
}

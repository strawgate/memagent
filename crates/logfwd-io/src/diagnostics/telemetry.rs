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
use std::time::Duration;

use super::metrics::PipelineMetrics;
use super::models::MemoryStats;
use super::process::process_metrics;
use super::render::{esc, now_nanos};

// ---------------------------------------------------------------------------
// Intermediate types (not OTLP — just collection containers)
// ---------------------------------------------------------------------------

pub(super) enum MetricValue {
    Gauge(f64),
    Sum(u64),
}

pub(super) struct MetricPoint {
    pub name: &'static str,
    pub value: MetricValue,
    pub attributes: Vec<(&'static str, String)>,
}

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

        // Per-pipeline input/output totals.
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

/// Collect completed spans from the trace buffer + in-progress active batches.
pub(super) fn collect_spans(
    trace_buf: Option<&crate::span_exporter::SpanBuffer>,
    pipelines: &[Arc<PipelineMetrics>],
) -> Vec<SpanRecord> {
    let mut spans = Vec::new();

    // Completed spans from the ring buffer.
    if let Some(buf) = trace_buf {
        for s in buf.get_spans() {
            let mut attrs: Vec<(&'static str, String)> = Vec::new();
            for kv in &s.attrs {
                // We leak the key into a &'static str via a match on known keys.
                // Unknown keys are dropped (skipped via `continue`).
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
                    _ => continue, // skip unknown attributes
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

    // In-progress active batches.
    let now = now_nanos();
    for pm in pipelines {
        // Mix pipeline name into synthetic trace/span IDs so different
        // pipelines with the same batch IDs don't collide.
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        pm.name.hash(&mut hasher);
        let pipeline_hash = hasher.finish();

        if let Ok(active) = pm.active_batches.lock() {
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
                    trace_id: format!("{:016x}{:016x}", pipeline_hash, id),
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
    }

    spans
}

/// Collect new stderr log lines since `last_count`.
/// Updates `last_count` to the current total.
pub(super) fn collect_new_logs(
    stderr: &crate::stderr_capture::StderrCapture,
    last_count: &mut usize,
) -> Vec<LogRecord> {
    let all = stderr.get_logs();
    let new_start = if all.len() < *last_count {
        // Buffer was evicted — can't know exact new items, take everything
        0
    } else {
        *last_count
    };
    *last_count = all.len();
    let ts = now_nanos();
    all[new_start..]
        .iter()
        .map(|line| LogRecord {
            timestamp_unix_ns: ts,
            severity: "INFO",
            body: line.clone(),
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
                let _ = write!(out, "{}", now);
                out.push_str(r#"","asDouble":"#);
                // Write f64 — JSON number. Emit null for non-finite values.
                // Guard against i64 saturation for values outside i64 range.
                if !v.is_finite() {
                    let _ = write!(out, "null");
                } else if v.fract() == 0.0 && *v >= i64::MIN as f64 && *v <= i64::MAX as f64 {
                    let _ = write!(out, "{}", *v as i64);
                } else {
                    let _ = write!(out, "{}", v);
                }
                write_attributes(&mut out, &pt.attributes);
                out.push_str("}]}");
            }
            MetricValue::Sum(v) => {
                out.push_str(r#","sum":{"dataPoints":[{"timeUnixNano":""#);
                let _ = write!(out, "{}", now);
                out.push_str(r#"","asInt":""#);
                let _ = write!(out, "{}", v);
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

        // Attributes — include in_progress flag.
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

        // Status.
        let status_code = match s.status {
            "ok" => 1,
            "error" => 2,
            _ => 0,
        };
        let _ = write!(out, r#","status":{{"code":{}}}}}"#, status_code);
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
        let v: serde_json::Value =
            serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\n{json}"));
        assert!(v["resourceMetrics"].is_array());
        let metrics = &v["resourceMetrics"][0]["scopeMetrics"][0]["metrics"];
        assert_eq!(metrics.as_array().unwrap().len(), 2);
        assert_eq!(metrics[0]["name"], "test.gauge");
        assert!(metrics[0]["gauge"]["dataPoints"][0]["asDouble"].is_number());
        assert_eq!(metrics[1]["name"], "test.sum");
        // asInt is a string per OTLP JSON spec (64-bit safe).
        assert_eq!(metrics[1]["sum"]["dataPoints"][0]["asInt"], "100");
    }

    #[test]
    fn spans_json_is_valid_otlp() {
        let spans = vec![SpanRecord {
            trace_id: "aabbccdd00112233aabbccdd00112233".into(),
            span_id: "aabbccdd00112233".into(),
            parent_id: "0000000000000000".into(),
            name: "batch".into(),
            start_unix_ns: 1_000_000_000,
            duration_ns: 200_000_000,
            attributes: vec![("pipeline", "default".into())],
            status: "ok",
            in_progress: false,
        }];
        let json = spans_to_otlp_json(&spans);
        let v: serde_json::Value =
            serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\n{json}"));
        let span = &v["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert_eq!(span["traceId"], "aabbccdd00112233aabbccdd00112233");
        assert_eq!(span["startTimeUnixNano"], "1000000000");
        assert_eq!(span["endTimeUnixNano"], "1200000000");
        assert_eq!(span["status"]["code"], 1); // ok
    }

    #[test]
    fn spans_json_in_progress_flag() {
        let spans = vec![SpanRecord {
            trace_id: "00000000000000000000000000000001".into(),
            span_id: "0000000000000001".into(),
            parent_id: "0000000000000000".into(),
            name: "batch".into(),
            start_unix_ns: 1_000_000_000,
            duration_ns: 0,
            attributes: vec![("stage", "scan".into())],
            status: "unset",
            in_progress: true,
        }];
        let json = spans_to_otlp_json(&spans);
        let v: serde_json::Value =
            serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\n{json}"));
        let attrs = v["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"]
            .as_array()
            .unwrap();
        let in_prog = attrs
            .iter()
            .find(|a| a["key"] == "in_progress")
            .expect("missing in_progress attribute");
        assert_eq!(in_prog["value"]["boolValue"], true);
    }

    #[test]
    fn logs_json_is_valid_otlp() {
        let logs = vec![LogRecord {
            timestamp_unix_ns: 1_000_000_000,
            severity: "WARN",
            body: "something happened".into(),
        }];
        let json = logs_to_otlp_json(&logs);
        let v: serde_json::Value =
            serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\n{json}"));
        let rec = &v["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0];
        assert_eq!(rec["severityText"], "WARN");
        assert_eq!(rec["body"]["stringValue"], "something happened");
        assert_eq!(rec["timeUnixNano"], "1000000000");
    }

    #[test]
    fn logs_json_escapes_special_chars() {
        let logs = vec![LogRecord {
            timestamp_unix_ns: 1,
            severity: "INFO",
            body: "line with \"quotes\" and\nnewline".into(),
        }];
        let json = logs_to_otlp_json(&logs);
        let v: serde_json::Value =
            serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\n{json}"));
        let body = v["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["body"]["stringValue"]
            .as_str()
            .unwrap();
        assert_eq!(body, "line with \"quotes\" and\nnewline");
    }

    #[test]
    fn empty_metrics_produces_valid_json() {
        let json = metrics_to_otlp_json(&[]);
        let _: serde_json::Value =
            serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\n{json}"));
    }

    #[test]
    fn empty_spans_produces_valid_json() {
        let json = spans_to_otlp_json(&[]);
        let _: serde_json::Value =
            serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\n{json}"));
    }

    #[test]
    fn empty_logs_produces_valid_json() {
        let json = logs_to_otlp_json(&[]);
        let _: serde_json::Value =
            serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\n{json}"));
    }

    #[test]
    fn collect_new_logs_incremental() {
        let capture = crate::stderr_capture::StderrCapture::new();
        // StderrCapture starts empty — simulate by checking incremental behavior.
        let mut last = 0;
        let logs = collect_new_logs(&capture, &mut last);
        assert!(logs.is_empty());
        assert_eq!(last, 0);
    }
}

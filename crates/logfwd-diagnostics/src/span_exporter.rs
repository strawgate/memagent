//! In-process ring-buffer span exporter for the diagnostics trace explorer.
//!
//! `RingBufferExporter` implements `SpanExporter` and stores the last
//! `MAX_SPANS` completed spans in an `Arc<Mutex<VecDeque>>`. The
//! `SpanBuffer` handle is shared with the diagnostics server, which reads
//! from it to serve `/admin/v1/traces`.
//!
//! Spans are converted to `TraceSpan` (a lightweight, serde-serializable
//! snapshot) on export so the raw SDK types don't escape this module.

use opentelemetry::KeyValue;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SpanData, SpanExporter};
use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

const MAX_SPANS: usize = 16_000; // ~8000 batches × 2 spans each

// ---------------------------------------------------------------------------
// Serializable span snapshot
// ---------------------------------------------------------------------------

/// Compact, serializable span snapshot served by diagnostics endpoints.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TraceSpan {
    /// 32-char lowercase hex trace ID.
    pub trace_id: String,
    /// 16-char lowercase hex span ID.
    pub span_id: String,
    /// 16-char lowercase hex parent span ID.  All-zeros = root span.
    pub parent_id: String,
    /// Span name.
    pub name: String,
    /// Unix nanoseconds at span start.
    pub start_unix_ns: u64,
    /// Wall-clock duration in nanoseconds.
    pub duration_ns: u64,
    /// Key-value attributes as strings.
    pub attrs: Vec<[String; 2]>,
    /// "ok", "error", or "unset".
    pub status: &'static str,
}

// ---------------------------------------------------------------------------
// Shared buffer
// ---------------------------------------------------------------------------

/// Cloneable handle to the in-process span ring buffer.
#[derive(Clone)]
pub struct SpanBuffer {
    inner: Arc<Mutex<VecDeque<TraceSpan>>>,
}

impl fmt::Debug for SpanBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let len = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        write!(f, "SpanBuffer({len} spans)")
    }
}

impl SpanBuffer {
    /// Create a new shared ring buffer for completed spans.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_SPANS))),
        }
    }

    /// Returns all buffered spans in insertion order (oldest first).
    pub fn get_spans(&self) -> Vec<TraceSpan> {
        let buf = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        buf.iter().cloned().collect()
    }
}

impl Default for SpanBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl SpanBuffer {
    /// Push a span directly — test-only helper to populate the buffer.
    pub fn push_test_span(&self, span: TraceSpan) {
        let mut buf = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if buf.len() >= MAX_SPANS {
            buf.pop_front();
        }
        buf.push_back(span);
    }
}

// ---------------------------------------------------------------------------
// Exporter
// ---------------------------------------------------------------------------

/// `SpanExporter` that pushes completed spans into a [`SpanBuffer`].
#[derive(Clone, Debug)]
pub struct RingBufferExporter {
    buf: SpanBuffer,
}

impl RingBufferExporter {
    /// Create an exporter that pushes completed spans into `buf`.
    pub fn new(buf: SpanBuffer) -> Self {
        Self { buf }
    }
}

impl SpanExporter for RingBufferExporter {
    fn export(&self, batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> + Send {
        let mut buf = self
            .buf
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for span_data in batch {
            let span = convert(span_data);
            if buf.len() >= MAX_SPANS {
                buf.pop_front();
            }
            buf.push_back(span);
        }
        std::future::ready(Ok(()))
    }
}

// ---------------------------------------------------------------------------
// Conversion helper
// ---------------------------------------------------------------------------

fn convert(s: SpanData) -> TraceSpan {
    let start_ns = s
        .start_time
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos() as u64;
    let end_ns = s
        .end_time
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos() as u64;

    let trace_id = {
        let b = s.span_context.trace_id().to_bytes();
        format!("{:032x}", u128::from_be_bytes(b))
    };
    let span_id = {
        let b = s.span_context.span_id().to_bytes();
        format!("{:016x}", u64::from_be_bytes(b))
    };
    let parent_id = {
        let b = s.parent_span_id.to_bytes();
        format!("{:016x}", u64::from_be_bytes(b))
    };

    let attrs: Vec<[String; 2]> = s
        .attributes
        .iter()
        .map(|KeyValue { key, value, .. }| [key.to_string(), value.to_string()])
        .collect();

    let status = match s.status {
        opentelemetry::trace::Status::Ok => "ok",
        opentelemetry::trace::Status::Error { .. } => "error",
        opentelemetry::trace::Status::Unset => "unset",
    };

    TraceSpan {
        trace_id,
        span_id,
        parent_id,
        name: s.name.into_owned(),
        start_unix_ns: start_ns,
        duration_ns: end_ns.saturating_sub(start_ns),
        attrs,
        status,
    }
}

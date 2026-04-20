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

/// Inner state for the span ring buffer.
struct SpanBufferInner {
    buf: VecDeque<TraceSpan>,
    /// Monotonically increasing count of total spans ever written.
    /// Used by `get_spans_since` to detect new entries even when the
    /// deque length is constant (at capacity).
    total_written: usize,
}

/// Cloneable handle to the in-process span ring buffer.
#[derive(Clone)]
pub struct SpanBuffer {
    inner: Arc<Mutex<SpanBufferInner>>,
}

impl fmt::Debug for SpanBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        write!(f, "SpanBuffer({} spans)", inner.buf.len())
    }
}

impl SpanBuffer {
    /// Create a new shared ring buffer for completed spans.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SpanBufferInner {
                buf: VecDeque::with_capacity(MAX_SPANS),
                total_written: 0,
            })),
        }
    }

    /// Returns all buffered spans in insertion order (oldest first).
    pub fn get_spans(&self) -> Vec<TraceSpan> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.buf.iter().cloned().collect()
    }

    /// Returns the current number of buffered spans.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .buf
            .len()
    }

    /// Returns true if the buffer contains no spans.
    pub fn is_empty(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .buf
            .is_empty()
    }

    /// Returns spans added since the cursor `since`, which tracks
    /// the monotonic `total_written` counter (not the deque length).
    ///
    /// If spans were evicted past the cursor, returns all buffered spans.
    /// Updates `since` to the current `total_written`.
    pub fn get_spans_since(&self, since: &mut usize) -> Vec<TraceSpan> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let oldest_available = inner.total_written.saturating_sub(inner.buf.len());
        let skip = (*since).saturating_sub(oldest_available);
        *since = inner.total_written;
        inner.buf.iter().skip(skip).cloned().collect()
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
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if inner.buf.len() >= MAX_SPANS {
            inner.buf.pop_front();
        }
        inner.buf.push_back(span);
        inner.total_written += 1;
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
        let mut inner = self
            .buf
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for span_data in batch {
            let span = convert(span_data);
            if inner.buf.len() >= MAX_SPANS {
                inner.buf.pop_front();
            }
            inner.buf.push_back(span);
            inner.total_written += 1;
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

//! Component-level diagnostic counters and lifecycle snapshots.

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Meter};

/// Coarse runtime health for one pipeline component.
///
/// This is intentionally small and lock-free so inputs, transform stages, and
/// sinks can expose lifecycle state through shared diagnostics without adding
/// mutexes to the hot path.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComponentHealth {
    /// Component exists but is still starting up or binding resources.
    Starting = 0,
    /// Component is healthy and able to participate in the pipeline.
    Healthy = 1,
    /// Component is functioning but degraded (for example, retrying).
    Degraded = 2,
    /// Component is shutting down and should no longer be considered ready.
    Stopping = 3,
    /// Component has stopped and is not available for work.
    Stopped = 4,
    /// Component hit a fatal condition and is not able to make progress.
    Failed = 5,
}

impl ComponentHealth {
    /// Stable lowercase string used in diagnostics JSON.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }

    /// Returns `true` when the component should count toward readiness.
    pub fn is_ready(self) -> bool {
        matches!(self, Self::Healthy | Self::Degraded)
    }

    /// Combine two component states by keeping the less-ready one.
    pub fn combine(self, other: Self) -> Self {
        if self.severity() >= other.severity() {
            self
        } else {
            other
        }
    }

    fn severity(self) -> u8 {
        self as u8
    }

    fn from_repr(value: u8) -> Self {
        match value {
            0 => Self::Starting,
            1 => Self::Healthy,
            2 => Self::Degraded,
            3 => Self::Stopping,
            4 => Self::Stopped,
            5 => Self::Failed,
            _ => Self::Failed,
        }
    }
}

/// Stats for one component. Dual-write: atomics for /api/pipelines,
/// OTel counters for OTLP push. Both are lock-free on the hot path.
pub struct ComponentStats {
    /// Total lines processed by this component.
    pub lines_total: AtomicU64,
    /// Total bytes processed by this component.
    pub bytes_total: AtomicU64,
    /// Total errors encountered by this component.
    pub errors_total: AtomicU64,
    /// Expected input lifecycle events (rotation/truncation).
    pub rotations_total: AtomicU64,
    /// Lines that failed format parsing (e.g. malformed CRI lines).
    pub parse_errors_total: AtomicU64,
    /// Coarse lifecycle and health snapshot for readiness/diagnostics.
    health: AtomicU8,
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
            health: AtomicU8::new(ComponentHealth::Healthy as u8),
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

    /// Increment line counter by `n` (atomic + OTel).
    pub fn inc_lines(&self, n: u64) {
        self.lines_total.fetch_add(n, Ordering::Relaxed);
        self.otel_lines.add(n, &self.otel_attrs);
    }

    /// Increment byte counter by `n` (atomic + OTel).
    pub fn inc_bytes(&self, n: u64) {
        self.bytes_total.fetch_add(n, Ordering::Relaxed);
        self.otel_bytes.add(n, &self.otel_attrs);
    }

    /// Increment error counter by 1 (atomic + OTel).
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

    /// Increment parse-error counter by `n` (atomic + OTel).
    pub fn inc_parse_errors(&self, n: u64) {
        self.parse_errors_total.fetch_add(n, Ordering::Relaxed);
        self.otel_parse_errors.add(n, &self.otel_attrs);
    }

    /// Current line count (relaxed load).
    pub fn lines(&self) -> u64 {
        self.lines_total.load(Ordering::Relaxed)
    }

    /// Current byte count (relaxed load).
    pub fn bytes(&self) -> u64 {
        self.bytes_total.load(Ordering::Relaxed)
    }

    /// Current error count (relaxed load).
    pub fn errors(&self) -> u64 {
        self.errors_total.load(Ordering::Relaxed)
    }

    /// Current rotation count (relaxed load).
    pub fn rotations(&self) -> u64 {
        self.rotations_total.load(Ordering::Relaxed)
    }

    /// Current parse-error count (relaxed load).
    pub fn parse_errors(&self) -> u64 {
        self.parse_errors_total.load(Ordering::Relaxed)
    }

    /// Update the component's coarse health snapshot.
    pub fn set_health(&self, health: ComponentHealth) {
        self.health.store(health as u8, Ordering::Relaxed);
    }

    /// Current component health (relaxed load).
    pub fn health(&self) -> ComponentHealth {
        ComponentHealth::from_repr(self.health.load(Ordering::Relaxed))
    }
}

impl Default for ComponentStats {
    fn default() -> Self {
        Self::new()
    }
}

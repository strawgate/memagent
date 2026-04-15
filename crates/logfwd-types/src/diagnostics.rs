//! Component-level diagnostic counters and lifecycle snapshots.

mod health;

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Meter};

pub use health::ComponentHealth;

/// Stats for one component. Dual-write: atomics for /admin/v1/status,
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
    pub fn with_meter(
        meter: &Meter,
        prefix: &str,
        attrs: Vec<KeyValue>,
        initial_health: ComponentHealth,
    ) -> Self {
        Self {
            lines_total: AtomicU64::new(0),
            bytes_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            rotations_total: AtomicU64::new(0),
            parse_errors_total: AtomicU64::new(0),
            health: AtomicU8::new(initial_health.as_repr()),
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
        Self::new_with_health(ComponentHealth::Healthy)
    }

    /// Create stats without OTel and an explicit initial health.
    pub fn new_with_health(initial_health: ComponentHealth) -> Self {
        let noop = opentelemetry::global::meter("noop");
        Self::with_meter(&noop, "noop", vec![], initial_health)
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
        self.health.store(health.as_repr(), Ordering::Relaxed);
    }

    /// Atomically reduce the component's health snapshot.
    ///
    /// This is used when multiple workers can publish lifecycle transitions
    /// concurrently and later observations must be derived from the most recent
    /// committed state rather than a stale read.
    pub fn update_health<F>(&self, reducer: F) -> ComponentHealth
    where
        F: Fn(ComponentHealth) -> ComponentHealth,
    {
        let mut observed = self.health.load(Ordering::Relaxed);
        loop {
            let current = ComponentHealth::from_repr(observed);
            let next = reducer(current);
            match self.health.compare_exchange_weak(
                observed,
                next.as_repr(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return next,
                Err(retry) => observed = retry,
            }
        }
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

#[cfg(test)]
mod tests {
    use super::{ComponentHealth, ComponentStats};

    #[test]
    fn update_health_uses_latest_committed_state() {
        let stats = ComponentStats::new();

        let next = stats.update_health(|current| {
            assert_eq!(current, ComponentHealth::Healthy);
            ComponentHealth::Failed
        });
        assert_eq!(next, ComponentHealth::Failed);

        let next = stats.update_health(|current| {
            assert_eq!(current, ComponentHealth::Failed);
            ComponentHealth::Healthy
        });
        assert_eq!(next, ComponentHealth::Healthy);
        assert_eq!(stats.health(), ComponentHealth::Healthy);
    }
}

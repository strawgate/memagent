//! Component-level diagnostic counters and lifecycle snapshots.

mod health;

use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};

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
    /// OTLP protobuf requests decoded through the projected fast path.
    pub otlp_projected_success_total: AtomicU64,
    /// OTLP protobuf requests that used prost fallback after unsupported projection.
    pub otlp_projected_fallback_total: AtomicU64,
    /// OTLP protobuf requests rejected after projection reported malformed input.
    pub otlp_projection_invalid_total: AtomicU64,
    /// Coarse lifecycle and health snapshot for readiness/diagnostics.
    health: AtomicU8,
    // Transport specific
    /// File: consecutive error polls.
    pub file_error_polls: AtomicU32,
    /// TCP: total accepted connections.
    pub tcp_accepted: AtomicU64,
    /// TCP: currently active connections.
    pub tcp_active: AtomicUsize,
    /// UDP: datagram drops detected.
    pub udp_drops: AtomicU64,
    /// eBPF: ring buffer overflow drops.
    pub ebpf_drops: AtomicU64,
    /// UDP: actual kernel receive buffer size.
    pub udp_recv_buf: AtomicUsize,
    /// Cumulative wall-clock nanoseconds spent in HTTP/gRPC send calls
    /// (network round-trip only, excludes serialization).
    #[cfg(not(kani))]
    pub send_ns_total: AtomicU64,
    /// Number of completed send operations (HTTP/gRPC round-trips).
    #[cfg(not(kani))]
    pub send_count: AtomicU64,
    // OTel counters (for OTLP push)
    otel_lines: Counter<u64>,
    otel_bytes: Counter<u64>,
    otel_errors: Counter<u64>,
    otel_rotations: Counter<u64>,
    otel_parse_errors: Counter<u64>,
    otel_otlp_projected_success: Counter<u64>,
    otel_otlp_projected_fallback: Counter<u64>,
    otel_otlp_projection_invalid: Counter<u64>,
    otel_ebpf_drops: Counter<u64>,
    otel_send_ns: Counter<u64>,
    otel_send_count: Counter<u64>,
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
            otlp_projected_success_total: AtomicU64::new(0),
            otlp_projected_fallback_total: AtomicU64::new(0),
            otlp_projection_invalid_total: AtomicU64::new(0),
            health: AtomicU8::new(initial_health.as_repr()),
            file_error_polls: AtomicU32::new(0),
            tcp_accepted: AtomicU64::new(0),
            tcp_active: AtomicUsize::new(0),
            udp_drops: AtomicU64::new(0),
            ebpf_drops: AtomicU64::new(0),
            udp_recv_buf: AtomicUsize::new(0),
            #[cfg(not(kani))]
            send_ns_total: AtomicU64::new(0),
            #[cfg(not(kani))]
            send_count: AtomicU64::new(0),
            otel_lines: meter.u64_counter(format!("{prefix}_lines")).build(),
            otel_bytes: meter.u64_counter(format!("{prefix}_bytes")).build(),
            otel_errors: meter.u64_counter(format!("{prefix}_errors")).build(),
            otel_rotations: meter.u64_counter(format!("{prefix}_rotations")).build(),
            otel_parse_errors: meter.u64_counter(format!("{prefix}_parse_errors")).build(),
            otel_otlp_projected_success: meter
                .u64_counter(format!("{prefix}_otlp_projected_success"))
                .build(),
            otel_otlp_projected_fallback: meter
                .u64_counter(format!("{prefix}_otlp_projected_fallback"))
                .build(),
            otel_otlp_projection_invalid: meter
                .u64_counter(format!("{prefix}_otlp_projection_invalid"))
                .build(),
            otel_ebpf_drops: meter.u64_counter(format!("{prefix}_ebpf_drops")).build(),
            otel_send_ns: meter.u64_counter(format!("{prefix}_send_ns")).build(),
            otel_send_count: meter.u64_counter(format!("{prefix}_send_count")).build(),
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

    /// Increment eBPF ring buffer drop counter by `n` (atomic + OTel).
    pub fn inc_ebpf_drops(&self, n: u64) {
        self.ebpf_drops.fetch_add(n, Ordering::Relaxed);
        self.otel_ebpf_drops.add(n, &self.otel_attrs);
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

    /// Increment OTLP projected fast-path success count.
    pub fn inc_otlp_projected_success(&self) {
        self.otlp_projected_success_total
            .fetch_add(1, Ordering::Relaxed);
        self.otel_otlp_projected_success.add(1, &self.otel_attrs);
    }

    /// Increment OTLP projected fallback-to-prost count.
    pub fn inc_otlp_projected_fallback(&self) {
        self.otlp_projected_fallback_total
            .fetch_add(1, Ordering::Relaxed);
        self.otel_otlp_projected_fallback.add(1, &self.otel_attrs);
    }

    /// Increment OTLP malformed projection rejection count.
    pub fn inc_otlp_projection_invalid(&self) {
        self.otlp_projection_invalid_total
            .fetch_add(1, Ordering::Relaxed);
        self.otel_otlp_projection_invalid.add(1, &self.otel_attrs);
    }

    /// Record a send operation and its duration in nanoseconds (atomic + OTel).
    #[cfg(not(kani))]
    pub fn inc_send(&self, ns: u64) {
        self.send_ns_total.fetch_add(ns, Ordering::Relaxed);
        self.send_count.fetch_add(1, Ordering::Relaxed);
        self.otel_send_ns.add(ns, &self.otel_attrs);
        self.otel_send_count.add(1, &self.otel_attrs);
    }

    #[cfg(kani)]
    pub fn inc_send(&self, _ns: u64) {}

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

    /// Current OTLP projected success count.
    pub fn otlp_projected_success(&self) -> u64 {
        self.otlp_projected_success_total.load(Ordering::Relaxed)
    }

    /// Current OTLP projected fallback count.
    pub fn otlp_projected_fallback(&self) -> u64 {
        self.otlp_projected_fallback_total.load(Ordering::Relaxed)
    }

    /// Current OTLP malformed projection rejection count.
    pub fn otlp_projection_invalid(&self) -> u64 {
        self.otlp_projection_invalid_total.load(Ordering::Relaxed)
    }

    /// Current send duration total in nanoseconds.
    #[cfg(not(kani))]
    pub fn send_ns_total(&self) -> u64 {
        self.send_ns_total.load(Ordering::Relaxed)
    }

    #[cfg(kani)]
    pub fn send_ns_total(&self) -> u64 {
        0
    }

    /// Current send operation count.
    #[cfg(not(kani))]
    pub fn send_count(&self) -> u64 {
        self.send_count.load(Ordering::Relaxed)
    }

    #[cfg(kani)]
    pub fn send_count(&self) -> u64 {
        0
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

use serde::Serialize;

/// Stable diagnostics payload contract version for JSON endpoints.
pub const STABLE_DIAGNOSTICS_CONTRACT_VERSION: &str = "v1";

/// In-flight batch being processed right now.
#[derive(Debug, Clone)]
pub struct ActiveBatch {
    pub start_unix_ns: u64,
    pub scan_ns: u64,
    pub transform_ns: u64,
    /// Current stage: "queued" | "scan" | "transform" | "output"
    pub stage: &'static str,
    /// Unix ns when the current stage started (for frontend live duration)
    pub stage_start_unix_ns: u64,
    /// Worker id once assigned (`None` = not yet assigned / in queue).
    pub worker_id: Option<u64>,
    /// Unix ns when the worker actually started processing (0 = not yet)
    pub output_start_unix_ns: u64,
}

/// Explicit lifecycle state for `/admin/v1/traces`.
///
/// The diagnostics payload contract uses this state machine:
/// - `scan_in_progress`: scanner is running for this batch.
/// - `transform_in_progress`: SQL transform is running.
/// - `queued_for_output`: scan+transform are complete and the batch is waiting
///   for worker assignment.
/// - `output_in_progress`: output execution started on a worker.
/// - `completed`: terminal state (both success and error terminal results).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceLifecycleState {
    ScanInProgress,
    TransformInProgress,
    QueuedForOutput,
    OutputInProgress,
    Completed,
}

impl TraceLifecycleState {
    /// Returns the stable snake_case string representation of this lifecycle state.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ScanInProgress => "scan_in_progress",
            Self::TransformInProgress => "transform_in_progress",
            Self::QueuedForOutput => "queued_for_output",
            Self::OutputInProgress => "output_in_progress",
            Self::Completed => "completed",
        }
    }
}

/// Snapshot of allocator memory statistics in bytes.
///
/// Populated by the jemalloc stats reader in the binary crate and surfaced on
/// `/admin/v1/status` under `system.memory`.
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

/// Typed response payload for `/live`.
#[derive(Debug, Serialize)]
pub struct LiveResponse {
    pub contract_version: &'static str,
    pub status: &'static str,
    pub uptime_seconds: u64,
    pub version: &'static str,
}

/// Typed response payload for `/ready`.
#[derive(Debug, Serialize)]
pub struct ReadyResponse {
    pub contract_version: &'static str,
    pub status: &'static str,
    pub reason: &'static str,
    pub observed_at_unix_ns: String,
}

/// Typed response payload for `/admin/v1/status`.
#[derive(Debug, Serialize)]
pub struct StatusSnapshotResponse {
    pub contract_version: &'static str,
    pub live: StatusSnapshot,
    pub ready: StatusSnapshot,
    pub component_health: ComponentHealthSnapshot,
    pub pipelines: Vec<PipelineStatus>,
    pub system: SystemStatus,
}

/// Shared status snapshot used by liveness and readiness sections.
#[derive(Debug, Serialize)]
pub struct StatusSnapshot {
    pub status: String,
    pub reason: String,
    pub observed_at_unix_ns: String,
}

/// Aggregated component health summary for status output.
#[derive(Debug, Serialize)]
pub struct ComponentHealthSnapshot {
    pub status: String,
    pub reason: String,
    pub readiness_impact: String,
    pub observed_at_unix_ns: String,
}

/// Per-pipeline health and throughput snapshot.
#[derive(Debug, Serialize)]
pub struct PipelineStatus {
    pub name: String,
    pub inputs: Vec<ComponentStatus>,
    pub transform: TransformStatus,
    pub outputs: Vec<ComponentStatus>,
    pub batches: BatchStatus,
    pub stage_seconds: StageSeconds,
    pub backpressure_stalls: u64,
    pub bottleneck: BottleneckStatus,
}

/// Generic component status shape for inputs and outputs.
#[derive(Debug, Serialize)]
pub struct ComponentStatus {
    pub name: String,
    #[serde(rename = "type")]
    pub component_type: String,
    pub health: String,
    pub lines_total: u64,
    pub bytes_total: u64,
    pub errors: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rotations: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parse_errors: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<TransportStatus>,
    /// Cumulative nanoseconds spent in HTTP/gRPC send calls (network round-trip only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub send_ns_total: Option<u64>,
    /// Number of completed send operations (HTTP/gRPC round-trips).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub send_count: Option<u64>,
}

/// Transport-specific status details for a component.
#[derive(Debug, Serialize)]
pub struct TransportStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<FileTransportStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tcp: Option<TcpTransportStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub udp: Option<UdpTransportStatus>,
}

/// File input transport counters.
#[derive(Debug, Serialize)]
pub struct FileTransportStatus {
    pub consecutive_error_polls: u64,
}

/// TCP input/output transport counters.
#[derive(Debug, Serialize)]
pub struct TcpTransportStatus {
    pub accepted_connections: u64,
    pub active_connections: u64,
}

/// UDP transport counters and socket buffer sizing.
#[derive(Debug, Serialize)]
pub struct UdpTransportStatus {
    pub drops_detected: u64,
    pub recv_buffer_size: u64,
}

/// Transform-stage status and filtering statistics.
#[derive(Debug, Serialize)]
pub struct TransformStatus {
    pub sql: String,
    pub health: String,
    pub lines_in: u64,
    pub lines_out: u64,
    pub errors: u64,
    pub filter_drop_rate: f64,
}

/// Batch processing and checkpoint-related counters.
#[derive(Debug, Serialize)]
pub struct BatchStatus {
    pub total: u64,
    pub avg_rows: f64,
    pub flush_by_size: u64,
    pub flush_by_timeout: u64,
    pub cadence_fast_repolls: u64,
    pub cadence_idle_sleeps: u64,
    pub dropped_batches_total: u64,
    pub scan_errors_total: u64,
    pub parse_errors_total: u64,
    pub last_batch_time_ns: String,
    pub batch_latency_avg_ns: u64,
    pub inflight: u64,
    pub channel_depth: u64,
    pub channel_capacity: u64,
    pub rows_total: u64,
}

/// Stage latency totals in seconds.
#[derive(Debug, Serialize)]
pub struct StageSeconds {
    pub scan: f64,
    pub transform: f64,
    pub output: f64,
    pub queue_wait: f64,
    pub send: f64,
}

/// Dominant bottleneck stage and explanatory reason.
#[derive(Debug, Serialize)]
pub struct BottleneckStatus {
    pub stage: String,
    pub reason: String,
}

/// Process-level status included in `/admin/v1/status`.
#[derive(Debug, Serialize)]
pub struct SystemStatus {
    pub uptime_seconds: u64,
    pub version: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<MemoryStatsResponse>,
}

/// Serializable memory counters for diagnostics responses.
#[derive(Debug, Serialize)]
pub struct MemoryStatsResponse {
    pub resident: usize,
    pub allocated: usize,
    pub active: usize,
}

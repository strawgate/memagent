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

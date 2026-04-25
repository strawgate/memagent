//! Shared types between the eBPF kernel program and userspace loader.
//!
//! These types must be `repr(C)` and have no heap allocations.

/// Maximum bytes we capture per write() call.
/// Log lines are typically 200-500 bytes. We cap at 4KB to stay
/// well within eBPF per-CPU array limits.
pub const MAX_CAPTURE_BYTES: usize = 4096;

/// Event emitted from the eBPF program to the ring buffer.
/// Each event represents one pipe_write() call from a container process.
#[repr(C)]
pub struct PipeWriteEvent {
    /// PID of the writing process (container entrypoint or child).
    pub pid: u32,
    /// Thread group ID.
    pub tgid: u32,
    /// Cgroup ID of the writing process (identifies the K8s pod).
    pub cgroup_id: u64,
    /// Number of bytes in the write (may exceed MAX_CAPTURE_BYTES).
    pub write_len: u32,
    /// Number of bytes actually captured (min of write_len, MAX_CAPTURE_BYTES).
    pub captured_len: u32,
    /// 1 = stdout (fd 1), 2 = stderr (fd 2), 0 = other pipe.
    pub stream: u8,
    pub pad: [u8; 3],
    /// The captured bytes from the write buffer.
    pub data: [u8; MAX_CAPTURE_BYTES],
}

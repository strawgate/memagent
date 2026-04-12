//! Shared types between eBPF kernel programs and userspace loader.
//!
//! All types are `repr(C)` for stable ABI across the BPF↔userspace boundary.

#![no_std]

/// Maximum bytes captured from a filename/path in exec events.
pub const MAX_FILENAME: usize = 256;

/// Maximum comm (process name) length in Linux.
pub const COMM_SIZE: usize = 16;

// ── Event discriminant ──────────────────────────────────────────────────

/// Event type tag stored in every event header.
#[repr(u32)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    ProcessExec = 1,
    ProcessExit = 2,
    TcpConnect = 3,
    TcpAccept = 4,
    FileOpen = 5,
}

// ── Common header ───────────────────────────────────────────────────────

/// Every event starts with this fixed header so userspace can dispatch
/// without knowing the payload size up front.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EventHeader {
    /// Monotonic timestamp from `bpf_ktime_get_ns`.
    pub timestamp_ns: u64,
    /// Event type discriminant.
    pub kind: u32,
    /// Thread group ID (userspace PID).
    pub tgid: u32,
    /// Thread ID.
    pub pid: u32,
    /// Parent thread group ID.
    pub ppid: u32,
    /// UID of the calling process.
    pub uid: u32,
    /// GID of the calling process.
    pub gid: u32,
    /// Cgroup v2 ID (from `bpf_get_current_cgroup_id`).
    pub cgroup_id: u64,
    /// Process comm (truncated to 16 bytes).
    pub comm: [u8; COMM_SIZE],
}

// ── Process exec ────────────────────────────────────────────────────────

/// Emitted on `tracepoint/sched/sched_process_exec`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ProcessExecEvent {
    pub header: EventHeader,
    /// Length of the filename actually captured (may be < MAX_FILENAME).
    pub filename_len: u32,
    pub _pad: u32,
    /// Filename from the exec (the binary path).
    pub filename: [u8; MAX_FILENAME],
}

// ── Process exit ────────────────────────────────────────────────────────

/// Emitted on `tracepoint/sched/sched_process_exit`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ProcessExitEvent {
    pub header: EventHeader,
    /// Exit code (from task->exit_code).
    pub exit_code: i32,
    pub _pad: u32,
}

// ── TCP connect (outbound) ──────────────────────────────────────────────

/// Emitted on `kprobe/tcp_v4_connect` return.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TcpConnectEvent {
    pub header: EventHeader,
    /// Source address (network byte order for v4, or v6 stored in full).
    pub saddr: u32,
    /// Destination address.
    pub daddr: u32,
    /// Source port (host byte order).
    pub sport: u16,
    /// Destination port (host byte order).
    pub dport: u16,
}

// ── TCP accept (inbound) ────────────────────────────────────────────────

/// Emitted on `kretprobe/inet_csk_accept`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TcpAcceptEvent {
    pub header: EventHeader,
    pub saddr: u32,
    pub daddr: u32,
    pub sport: u16,
    pub dport: u16,
}

// ── File open ───────────────────────────────────────────────────────────

/// Emitted on `tracepoint/syscalls/sys_enter_openat`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FileOpenEvent {
    pub header: EventHeader,
    /// Open flags (O_RDONLY, O_WRONLY, etc.).
    pub flags: u32,
    /// Length of filename captured.
    pub filename_len: u32,
    /// Filename argument to openat.
    pub filename: [u8; MAX_FILENAME],
}

// ── Process info stash (kernel-internal, for kprobe→tracepoint correlation) ─

/// Stashed in a BPF HashMap by the kprobe on `tcp_v4_connect` (which runs in
/// process context), then looked up by `inet_sock_set_state` (softirq context)
/// to attribute connections to the correct process.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ConnProcessInfo {
    pub tgid: u32,
    pub pid: u32,
    pub uid: u32,
    pub gid: u32,
    pub cgroup_id: u64,
    pub comm: [u8; COMM_SIZE],
}

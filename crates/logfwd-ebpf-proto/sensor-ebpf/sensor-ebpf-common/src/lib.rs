//! Shared types between eBPF kernel programs and userspace loader.
//!
//! All types are `repr(C)` for stable ABI across the BPF↔userspace boundary.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "std")]
pub mod dns;

/// Maximum bytes captured from a filename/path in events.
pub const MAX_FILENAME: usize = 256;

/// Maximum kernel module name length.
pub const MAX_MODULE_NAME: usize = 64;

/// Maximum DNS question section bytes: up to 255 bytes of wire-format QNAME
/// (labels + length octets + root terminator) plus 4 bytes for QTYPE/QCLASS.
pub const MAX_DNS_NAME: usize = 259;

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
    FileDelete = 6,
    FileRename = 7,
    Setuid = 8,
    Setgid = 9,
    ModuleLoad = 10,
    Ptrace = 11,
    MemfdCreate = 12,
    /// DNS query captured from `sys_enter_sendto` to port 53.
    DnsQuery = 13,
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
    /// Parent thread group ID (currently set to 0; requires task_struct reading).
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
    pub pad: u32,
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
    pub pad: u32,
}

// ── TCP connect (outbound) ──────────────────────────────────────────────

/// Emitted on `kprobe/tcp_v4_connect` + `inet_sock_set_state`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TcpConnectEvent {
    pub header: EventHeader,
    pub saddr: u32,
    pub daddr: u32,
    pub sport: u16,
    pub dport: u16,
}

// ── TCP accept (inbound) ────────────────────────────────────────────────

/// Emitted on `inet_sock_set_state` (SYN_RECV → ESTABLISHED).
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
    pub flags: u32,
    pub filename_len: u32,
    pub filename: [u8; MAX_FILENAME],
}

// ── File delete (unlinkat) ──────────────────────────────────────────────

/// Emitted on `tracepoint/syscalls/sys_enter_unlinkat`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FileDeleteEvent {
    pub header: EventHeader,
    /// unlinkat flags (e.g. AT_REMOVEDIR).
    pub flags: u32,
    pub pathname_len: u32,
    pub pathname: [u8; MAX_FILENAME],
}

// ── File rename (renameat2) ─────────────────────────────────────────────

/// Emitted on `tracepoint/syscalls/sys_enter_renameat2`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FileRenameEvent {
    pub header: EventHeader,
    pub oldname_len: u32,
    pub newname_len: u32,
    pub oldname: [u8; MAX_FILENAME],
    pub newname: [u8; MAX_FILENAME],
}

// ── Privilege escalation ────────────────────────────────────────────────

/// Emitted on `tracepoint/syscalls/sys_enter_setuid`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SetuidEvent {
    pub header: EventHeader,
    pub target_uid: u32,
    pub pad: u32,
}

/// Emitted on `tracepoint/syscalls/sys_enter_setgid`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SetgidEvent {
    pub header: EventHeader,
    pub target_gid: u32,
    pub pad: u32,
}

// ── Kernel module load ──────────────────────────────────────────────────

/// Emitted on `tracepoint/module/module_load`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ModuleLoadEvent {
    pub header: EventHeader,
    pub taints: u32,
    pub name_len: u32,
    pub name: [u8; MAX_MODULE_NAME],
}

// ── Ptrace ──────────────────────────────────────────────────────────────

/// Emitted on `tracepoint/syscalls/sys_enter_ptrace`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PtraceEvent {
    pub header: EventHeader,
    /// PTRACE request code (PTRACE_ATTACH=16, PTRACE_SEIZE=16902, etc.).
    pub request: u64,
    /// Target PID being traced.
    pub target_pid: u64,
}

// ── memfd_create (fileless malware staging) ─────────────────────────────

/// Emitted on `tracepoint/syscalls/sys_enter_memfd_create`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MemfdCreateEvent {
    pub header: EventHeader,
    pub flags: u32,
    pub name_len: u32,
    pub name: [u8; MAX_FILENAME],
}

// ── DNS query ───────────────────────────────────────────────────────────

/// Emitted on `tracepoint/syscalls/sys_enter_sendto` when destination port is 53.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DnsQueryEvent {
    pub header: EventHeader,
    /// DNS query name in raw wire format (label-encoded, e.g. `\x03www\x06google\x03com\x00`).
    /// Parsed to dotted notation in userspace.
    pub qname: [u8; MAX_DNS_NAME],
    /// Length of the valid portion of `qname`.
    pub qname_len: u16,
    /// DNS query type (A=1, AAAA=28, CNAME=5, MX=15, TXT=16).
    pub qtype: u16,
    /// DNS transaction ID for response correlation.
    pub tx_id: u16,
    /// Destination IP (DNS server).
    pub dst_addr: u32,
    /// Destination port (should be 53).
    pub dst_port: u16,
}

// ── eBPF runtime config (userspace → kernel) ───────────────────────────

/// Configuration passed from userspace to eBPF programs via a BPF Array map.
/// Index 0 stores this struct.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EbpfConfig {
    /// Byte offset of `exit_code` in `task_struct`. Set from BTF at load time.
    /// When 0, the eBPF program uses -1 (unknown) as the exit_code sentinel.
    pub task_exit_code_offset: u32,
    /// Byte offset of `group_dead` in `sched/sched_process_exit` tracepoint payload.
    /// Only valid when `sched_process_exit_has_group_dead != 0`.
    pub sched_process_exit_group_dead_offset: u32,
    /// Whether `sched/sched_process_exit` includes `group_dead` in this kernel.
    /// Kept separate from the offset value so offset 0 is never used as a
    /// presence sentinel.
    pub sched_process_exit_has_group_dead: u32,
    pub pad: u32,
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

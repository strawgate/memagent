//! eBPF-based platform sensor input producing Arrow `RecordBatch` directly.
//!
//! Loads eBPF programs via `aya`, attaches to tracepoints and kprobes, and
//! drains a ring buffer of kernel events into Arrow record batches. This
//! replaces the standalone JSON-emitting prototype in `sensor-ebpf`.
//!
//! The entire module is `cfg(target_os = "linux")` — `aya` only compiles on Linux.

// Ring buffer events are 8-byte aligned by the kernel, so casting from *const u8
// to a repr(C) struct pointer is safe despite clippy's alignment warning.
#![allow(clippy::cast_ptr_alignment)]

use std::io;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, Int32Array, StringArray, UInt16Array, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use aya::Ebpf;
use aya::maps::RingBuf;
use aya::programs::{KProbe, TracePoint};
use ffwd_types::diagnostics::ComponentHealth;
use sensor_ebpf_common::dns::dns_wire_to_dotted;
use sensor_ebpf_common::utils::*;
use sensor_ebpf_common::*;

/// Newtype wrapper for `EbpfConfig` to satisfy aya's `Pod` trait (orphan rule).
#[repr(transparent)]
#[derive(Clone, Copy)]
struct PodEbpfConfig(EbpfConfig);

// SAFETY: EbpfConfig is repr(C), Copy, and contains only primitive types (u32).
unsafe impl aya::Pod for PodEbpfConfig {}

use crate::input::{InputSource, SourceEvent};
use crate::platform_sensor_filter::is_event_type_enabled;

// Tracefs paths to probe for the sched_process_exit format file.
// Some systems mount tracefs at `/sys/kernel/tracing`, others at
// `/sys/kernel/debug/tracing`.

// ── Configuration ──────────────────────────────────────────────────────

/// Configuration for the eBPF platform sensor input.
pub struct PlatformSensorConfig {
    /// Path to the compiled eBPF kernel binary.
    pub ebpf_binary_path: PathBuf,
    /// Maximum events to drain per poll cycle.
    pub max_events_per_poll: usize,
    /// Whether to filter out events from our own process.
    pub filter_self: bool,
    /// Glob patterns for process names to include.
    pub include_process_names: Option<Vec<String>>,
    /// Glob patterns for process names to exclude.
    pub exclude_process_names: Option<Vec<String>>,
    /// Specific event types to enable.
    pub include_event_types: Option<Vec<String>>,
    /// Specific event types to disable.
    pub exclude_event_types: Option<Vec<String>>,
    /// Ring buffer size in kilobytes.
    pub ring_buffer_size_kb: Option<usize>,
    /// Poll interval in milliseconds.
    pub poll_interval_ms: Option<u64>,
}

// ── Schema ─────────────────────────────────────────────────────────────

/// Build the Arrow schema for eBPF platform sensor events.
///
/// Wide table with nullable columns for event-specific fields. Common header
/// columns are non-nullable.
fn platform_sensor_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        // ── Common header (non-null) ─────────────────────────
        Field::new("timestamp_unix_nano", DataType::UInt64, false),
        Field::new("event_kind", DataType::Utf8, false),
        Field::new("tgid", DataType::UInt32, false),
        Field::new("pid", DataType::UInt32, false),
        Field::new("ppid", DataType::UInt32, false),
        Field::new("uid", DataType::UInt32, false),
        Field::new("gid", DataType::UInt32, false),
        Field::new("cgroup_id", DataType::UInt64, false),
        Field::new("comm", DataType::Utf8, false),
        // ── Event-specific (nullable) ────────────────────────
        Field::new("filename", DataType::Utf8, true),
        Field::new("exit_code", DataType::Int32, true),
        Field::new("flags", DataType::UInt32, true),
        Field::new("old_path", DataType::Utf8, true),
        Field::new("new_path", DataType::Utf8, true),
        Field::new("target_uid", DataType::UInt32, true),
        Field::new("target_gid", DataType::UInt32, true),
        Field::new("module_taints", DataType::UInt32, true),
        Field::new("ptrace_request", DataType::Utf8, true),
        Field::new("ptrace_target_pid", DataType::UInt64, true),
        Field::new("src_addr", DataType::Utf8, true),
        Field::new("dst_addr", DataType::Utf8, true),
        Field::new("src_port", DataType::UInt16, true),
        Field::new("dst_port", DataType::UInt16, true),
        Field::new("dns_qname", DataType::Utf8, true),
        Field::new("dns_qtype", DataType::UInt16, true),
        Field::new("dns_tx_id", DataType::UInt16, true),
    ]))
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Extract process comm from a fixed-size null-terminated byte array.
fn comm_str(comm: &[u8; COMM_SIZE]) -> &str {
    let end = comm.iter().position(|&b| b == 0).unwrap_or(COMM_SIZE);
    std::str::from_utf8(&comm[..end]).unwrap_or("<invalid>")
}

/// Extract a variable-length string from a byte buffer, stopping at NUL.
fn safe_str(buf: &[u8], len: usize) -> &str {
    let end = len.min(buf.len());
    let slice = &buf[..end];
    let nul = slice.iter().position(|&b| b == 0).unwrap_or(end);
    std::str::from_utf8(&slice[..nul]).unwrap_or("<invalid>")
}

// dns_wire_to_dotted is provided by sensor_ebpf_common::dns::dns_wire_to_dotted

/// Map a ptrace request code to a human-readable name.
fn ptrace_request_name(req: u64) -> &'static str {
    match req {
        0 => "TRACEME",
        1 => "PEEKTEXT",
        2 => "PEEKDATA",
        4 => "POKETEXT",
        5 => "POKEDATA",
        12 => "GETREGS",
        13 => "SETREGS",
        16 => "ATTACH",
        17 => "DETACH",
        24 => "SYSCALL",
        7 => "CONT",
        8 => "KILL",
        9 => "SINGLESTEP",
        0x4200 => "SETOPTIONS",
        0x4201 => "GETEVENTMSG",
        0x4206 => "SEIZE",
        0x4207 => "INTERRUPT",
        _ => "UNKNOWN",
    }
}

/// Wall-clock timestamp in nanoseconds since the Unix epoch.
fn wall_clock_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Format an IPv4 address from a `__be32` u32.
///
/// The sensor reads network-order address bytes into a native `u32`, so native
/// bytes recover the original IPv4 octets.
fn format_addr(addr: u32) -> String {
    let ip = Ipv4Addr::from(addr.to_ne_bytes());
    ip.to_string()
}

// ── Per-event row accumulator ──────────────────────────────────────────

/// Intermediate row representation used to collect events before batching.
struct EventRow {
    timestamp_unix_nano: u64,
    event_kind: &'static str,
    tgid: u32,
    pid: u32,
    ppid: u32,
    uid: u32,
    gid: u32,
    cgroup_id: u64,
    comm: String,
    // Event-specific nullable fields.
    filename: Option<String>,
    exit_code: Option<i32>,
    flags: Option<u32>,
    old_path: Option<String>,
    new_path: Option<String>,
    target_uid: Option<u32>,
    target_gid: Option<u32>,
    module_taints: Option<u32>,
    ptrace_request: Option<String>,
    ptrace_target_pid: Option<u64>,
    src_addr: Option<String>,
    dst_addr: Option<String>,
    src_port: Option<u16>,
    dst_port: Option<u16>,
    dns_qname: Option<String>,
    dns_qtype: Option<u16>,
    dns_tx_id: Option<u16>,
}

impl EventRow {
    fn from_header(header: &EventHeader, kind: &'static str, mono_to_wall_offset_ns: u64) -> Self {
        Self {
            timestamp_unix_nano: header.timestamp_ns.saturating_add(mono_to_wall_offset_ns),
            event_kind: kind,
            tgid: header.tgid,
            pid: header.pid,
            ppid: header.ppid,
            uid: header.uid,
            gid: header.gid,
            cgroup_id: header.cgroup_id,
            comm: comm_str(&header.comm).to_string(),
            filename: None,
            exit_code: None,
            flags: None,
            old_path: None,
            new_path: None,
            target_uid: None,
            target_gid: None,
            module_taints: None,
            ptrace_request: None,
            ptrace_target_pid: None,
            src_addr: None,
            dst_addr: None,
            src_port: None,
            dst_port: None,
            dns_qname: None,
            dns_qtype: None,
            dns_tx_id: None,
        }
    }
}

// ── Tracepoint and kprobe definitions ──────────────────────────────────

/// Tracepoints to attach: (program name, category, tracepoint).
const TRACEPOINTS: &[(&str, &str, &str)] = &[
    ("sched_process_exec", "sched", "sched_process_exec"),
    ("sched_process_exit", "sched", "sched_process_exit"),
    ("sys_enter_openat", "syscalls", "sys_enter_openat"),
    ("sys_enter_unlinkat", "syscalls", "sys_enter_unlinkat"),
    ("sys_enter_renameat2", "syscalls", "sys_enter_renameat2"),
    ("sys_enter_setuid", "syscalls", "sys_enter_setuid"),
    ("sys_enter_setgid", "syscalls", "sys_enter_setgid"),
    ("module_load", "module", "module_load"),
    ("sys_enter_ptrace", "syscalls", "sys_enter_ptrace"),
    (
        "sys_enter_memfd_create",
        "syscalls",
        "sys_enter_memfd_create",
    ),
    ("inet_sock_set_state", "sock", "inet_sock_set_state"),
    ("sys_enter_sendto", "syscalls", "sys_enter_sendto"),
];

/// Kprobes to attach: (program name, kernel function).
const KPROBES: &[(&str, &str)] = &[("tcp_v4_connect", "tcp_v4_connect")];

// ── State machine ──────────────────────────────────────────────────────

/// eBPF platform sensor input that reads kernel events and produces
/// Arrow `RecordBatch`.
pub struct PlatformSensorInput {
    name: String,
    state: SensorState,
    stats: Arc<ffwd_types::diagnostics::ComponentStats>,
}

enum SensorState {
    /// Waiting for first poll to load eBPF programs.
    Init {
        config: PlatformSensorConfig,
        schema: Arc<Schema>,
    },
    /// eBPF loaded and attached; draining ring buffer on each poll.
    Running {
        schema: Arc<Schema>,
        config: PlatformSensorConfig,
        ebpf: Ebpf,
        health: ComponentHealth,
        self_tgid: u32,
        /// Probes that failed to attach (missing from eBPF binary).
        skipped_probes: Vec<String>,
        /// Capability degradation messages (e.g., missing exit_code offset).
        degraded_capabilities: Vec<String>,
    },
    /// Poisoned state after take().
    Poisoned,
}

struct EbpfConfigStatus {
    has_exit_code_support: bool,
    has_group_dead_support: bool,
}

impl PlatformSensorInput {
    /// Create a new platform sensor input. eBPF programs are not loaded
    /// until the first `poll()` call.
    pub fn new(
        name: impl Into<String>,
        config: PlatformSensorConfig,
        stats: Arc<ffwd_types::diagnostics::ComponentStats>,
    ) -> io::Result<Self> {
        if !config.ebpf_binary_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "eBPF binary not found: {}",
                    config.ebpf_binary_path.display()
                ),
            ));
        }

        Ok(Self {
            name: name.into(),
            stats,
            state: SensorState::Init {
                config,
                schema: platform_sensor_schema(),
            },
        })
    }

    /// Load eBPF programs and attach to tracepoints/kprobes.
    ///
    /// Returns `(ebpf, skipped_probes, degraded_capabilities)` where:
    /// - `skipped_probes`: probe attach failures (program missing from binary)
    /// - `degraded_capabilities`: capability warnings (e.g., missing BTF offsets)
    fn load_ebpf(config: &PlatformSensorConfig) -> io::Result<(Ebpf, Vec<String>, Vec<String>)> {
        let ebpf_bytes = std::fs::read(&config.ebpf_binary_path).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "failed to read eBPF binary {}: {e}",
                    config.ebpf_binary_path.display()
                ),
            )
        })?;

        let mut ebpf = Ebpf::load(&ebpf_bytes)
            .map_err(|e| io::Error::other(format!("failed to load eBPF programs: {e}")))?;

        let mut skipped_probes = Vec::new();
        let mut degraded_capabilities = Vec::new();

        // Configure runtime parameters via the CONFIG BPF map BEFORE attaching
        // any programs, so events emitted during startup have valid config.
        match Self::configure_ebpf_params(&mut ebpf) {
            Ok(status) => {
                tracing::debug!("configured eBPF runtime parameters (exit_code + group_dead)");
                if !status.has_exit_code_support {
                    let message = "task_struct.exit_code offset unavailable; using sentinel mode";
                    tracing::warn!("{message}");
                    degraded_capabilities.push(message.to_string());
                }
                if !status.has_group_dead_support {
                    let message =
                        "sched_process_exit group_dead missing; using pid==tgid fallback semantics";
                    tracing::warn!("{message}");
                    degraded_capabilities.push(message.to_string());
                }
            }
            Err(e) => {
                tracing::warn!("eBPF config unavailable; using degraded semantics: {e}");
                degraded_capabilities.push(format!("eBPF config unavailable: {e}"));
            }
        }

        // Attach tracepoints.
        for (prog_name, category, tracepoint) in TRACEPOINTS {
            match ebpf.program_mut(prog_name) {
                Some(prog) => {
                    let tp: &mut TracePoint = prog.try_into().map_err(|e| {
                        io::Error::other(format!("program {prog_name} is not a tracepoint: {e}"))
                    })?;
                    tp.load().map_err(|e| {
                        io::Error::other(format!("failed to load tracepoint {prog_name}: {e}"))
                    })?;
                    tp.attach(category, tracepoint).map_err(|e| {
                        io::Error::other(format!("failed to attach {category}/{tracepoint}: {e}"))
                    })?;
                    tracing::debug!("attached tracepoint {category}/{tracepoint}");
                }
                None => {
                    tracing::warn!("eBPF program {prog_name} not found in binary, skipping");
                    skipped_probes.push(format!("tracepoint:{category}/{tracepoint}"));
                }
            }
        }

        // Attach kprobes.
        for (prog_name, fn_name) in KPROBES {
            match ebpf.program_mut(prog_name) {
                Some(prog) => {
                    let kp: &mut KProbe = prog.try_into().map_err(|e| {
                        io::Error::other(format!("program {prog_name} is not a kprobe: {e}"))
                    })?;
                    kp.load().map_err(|e| {
                        io::Error::other(format!("failed to load kprobe {prog_name}: {e}"))
                    })?;
                    kp.attach(fn_name, 0).map_err(|e| {
                        io::Error::other(format!("failed to attach kprobe/{fn_name}: {e}"))
                    })?;
                    tracing::debug!("attached kprobe/{fn_name}");
                }
                None => {
                    tracing::warn!("eBPF program {prog_name} not found in binary, skipping");
                    skipped_probes.push(format!("kprobe:{fn_name}"));
                }
            }
        }

        Ok((ebpf, skipped_probes, degraded_capabilities))
    }

    /// Write runtime configuration to the eBPF CONFIG map.
    ///
    /// Discovers the byte offset of `task_struct.exit_code` from kernel BTF
    /// so the eBPF program can read real exit codes instead of using a sentinel.
    fn configure_ebpf_params(ebpf: &mut Ebpf) -> io::Result<EbpfConfigStatus> {
        let exit_code_offset = match find_exit_code_offset() {
            Ok(offset) => Some(offset),
            Err(e) => {
                tracing::warn!("exit_code offset detection failed: {e}");
                None
            }
        };
        let group_dead_offset = match find_sched_process_exit_group_dead_offset() {
            Ok(offset) => offset,
            Err(e) => {
                tracing::warn!("group_dead offset detection failed: {e}");
                None
            }
        };

        let config_map = ebpf
            .map_mut("CONFIG")
            .ok_or_else(|| io::Error::other("CONFIG map not found in eBPF binary"))?;
        let mut array: aya::maps::Array<_, PodEbpfConfig> = config_map
            .try_into()
            .map_err(|e| io::Error::other(format!("CONFIG map type error: {e}")))?;

        let cfg = PodEbpfConfig(EbpfConfig {
            task_exit_code_offset: exit_code_offset.unwrap_or(0),
            sched_process_exit_group_dead_offset: group_dead_offset.unwrap_or(0),
            sched_process_exit_has_group_dead: u32::from(group_dead_offset.is_some()),
            pad: 0,
        });
        array
            .set(0, cfg, 0)
            .map_err(|e| io::Error::other(format!("CONFIG map write: {e}")))?;

        if let Some(exit_code_offset) = exit_code_offset {
            tracing::info!(
                exit_code_offset,
                "set task_struct.exit_code offset from kernel BTF"
            );
        }
        if let Some(group_dead_offset) = group_dead_offset {
            tracing::info!(
                group_dead_offset,
                "set sched_process_exit group_dead offset from tracepoint format"
            );
        }
        Ok(EbpfConfigStatus {
            has_exit_code_support: exit_code_offset.is_some(),
            has_group_dead_support: group_dead_offset.is_some(),
        })
    }

    /// Discover `task_struct.exit_code` offset from `/sys/kernel/btf/vmlinux`.
    ///
    /// Uses `pahole` (from the `dwarves` package) to introspect kernel BTF.
    /// Returns the byte offset on success.
    fn check_drops(ebpf: &mut Ebpf) -> io::Result<u64> {
        let drops_map = match ebpf.map_mut("DROPS") {
            Some(map) => map,
            None => return Ok(0),
        };

        let mut drops_array = match aya::maps::PerCpuArray::<_, u64>::try_from(drops_map) {
            Ok(array) => array,
            Err(_) => return Ok(0),
        };

        let mut total_drops = 0;

        // Sum drops across all CPUs, then zero them out so we only report deltas.
        if let Ok(per_cpu_values) = drops_array.get(&0, 0) {
            for val in per_cpu_values.iter() {
                if *val > 0 {
                    total_drops += val;
                }
            }
            if total_drops > 0 {
                // Zero out the map to prevent overcounting on the next poll
                use aya::maps::PerCpuValues;
                let zero_vals: Vec<u64> = per_cpu_values.iter().map(|_| 0).collect();
                match PerCpuValues::try_from(zero_vals) {
                    Ok(zero_per_cpu_values) => {
                        if let Err(error) = drops_array.set(0, zero_per_cpu_values, 0) {
                            tracing::warn!(
                                total_drops,
                                ?error,
                                "failed to reset eBPF DROPS map after reporting drops"
                            );
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            total_drops,
                            ?error,
                            "failed to build eBPF DROPS reset values after reporting drops"
                        );
                    }
                }
            }
        }

        Ok(total_drops)
    }

    fn drain_events(
        ebpf: &mut Ebpf,
        schema: &Arc<Schema>,
        max_events: usize,
        self_tgid: u32,
        filter_self: bool,
        include_event_types: Option<&[String]>,
        exclude_event_types: Option<&[String]>,
    ) -> io::Result<Option<SourceEvent>> {
        let events_map = ebpf.map_mut("EVENTS").ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "EVENTS ring buffer map not found")
        })?;
        let mut ring = RingBuf::try_from(events_map)
            .map_err(|e| io::Error::other(format!("failed to create ring buffer: {e}")))?;

        // Compute monotonic-to-wall-clock offset once per drain batch.
        // bpf_ktime_get_ns() is CLOCK_MONOTONIC; we offset to CLOCK_REALTIME.
        let mono_to_wall_offset_ns = {
            let mut ts = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            // SAFETY: valid pointer to stack-allocated timespec.
            unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &raw mut ts) };
            let mono_now_ns = ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64;
            wall_clock_ns().saturating_sub(mono_now_ns)
        };

        let mut rows = Vec::with_capacity(max_events.min(256));
        let mut drained = 0_usize;

        while drained < max_events {
            let Some(item) = ring.next() else {
                break;
            };
            drained += 1;

            let ptr = item.as_ptr();
            let len = item.len();

            if len < size_of::<EventHeader>() {
                continue;
            }

            // SAFETY: Length checked >= size_of::<EventHeader>() above.
            // Ring buffer entries are 8-byte aligned by the kernel.
            let header = unsafe { &*(ptr.cast::<EventHeader>()) };

            if filter_self && header.tgid == self_tgid {
                continue;
            }

            if let Some(row) = parse_event(ptr, len, header, mono_to_wall_offset_ns)
                && is_event_type_enabled(row.event_kind, include_event_types, exclude_event_types)
            {
                rows.push(row);
            }
        }

        if rows.is_empty() {
            return Ok(None);
        }

        let batch = build_record_batch(schema, &rows)?;
        let accounted_bytes = rows.len() as u64 * 64; // approximate per-event cost

        Ok(Some(SourceEvent::Batch {
            batch,
            source_id: None,
            accounted_bytes,
        }))
    }
}

/// Parse a single ring buffer event into an `EventRow`.
///
/// Returns `None` for malformed or unknown events.
fn parse_event(
    ptr: *const u8,
    len: usize,
    header: &EventHeader,
    mono_to_wall_offset_ns: u64,
) -> Option<EventRow> {
    match header.kind {
        k if k == EventKind::ProcessExec as u32 && len >= size_of::<ProcessExecEvent>() => {
            // SAFETY: Length checked >= size_of::<ProcessExecEvent>(); ring buffer is 8-byte aligned.
            let ev = unsafe { &*(ptr.cast::<ProcessExecEvent>()) };
            let mut row = EventRow::from_header(header, "exec", mono_to_wall_offset_ns);
            row.filename = Some(safe_str(&ev.filename, ev.filename_len as usize).to_string());
            Some(row)
        }
        k if k == EventKind::ProcessExit as u32 && len >= size_of::<ProcessExitEvent>() => {
            // SAFETY: Length checked >= size_of::<ProcessExitEvent>(); ring buffer is 8-byte aligned.
            let ev = unsafe { &*(ptr.cast::<ProcessExitEvent>()) };
            let mut row = EventRow::from_header(header, "exit", mono_to_wall_offset_ns);
            // Sentinel -1 means offset was unavailable; treat as null.
            row.exit_code = if ev.exit_code == -1 {
                None
            } else {
                Some(ev.exit_code)
            };
            Some(row)
        }
        k if k == EventKind::TcpConnect as u32 && len >= size_of::<TcpConnectEvent>() => {
            // SAFETY: Length checked >= size_of::<TcpConnectEvent>(); ring buffer is 8-byte aligned.
            let ev = unsafe { &*(ptr.cast::<TcpConnectEvent>()) };
            let mut row = EventRow::from_header(header, "tcp_connect", mono_to_wall_offset_ns);
            row.src_addr = Some(format_addr(ev.saddr));
            row.dst_addr = Some(format_addr(ev.daddr));
            row.src_port = Some(ev.sport);
            row.dst_port = Some(ev.dport);
            Some(row)
        }
        k if k == EventKind::TcpAccept as u32 && len >= size_of::<TcpAcceptEvent>() => {
            // SAFETY: Length checked >= size_of::<TcpAcceptEvent>(); ring buffer is 8-byte aligned.
            let ev = unsafe { &*(ptr.cast::<TcpAcceptEvent>()) };
            let mut row = EventRow::from_header(header, "tcp_accept", mono_to_wall_offset_ns);
            row.src_addr = Some(format_addr(ev.saddr));
            row.dst_addr = Some(format_addr(ev.daddr));
            row.src_port = Some(ev.sport);
            row.dst_port = Some(ev.dport);
            Some(row)
        }
        k if k == EventKind::FileOpen as u32 && len >= size_of::<FileOpenEvent>() => {
            // SAFETY: Length checked >= size_of::<FileOpenEvent>(); ring buffer is 8-byte aligned.
            let ev = unsafe { &*(ptr.cast::<FileOpenEvent>()) };
            let mut row = EventRow::from_header(header, "file_open", mono_to_wall_offset_ns);
            row.filename = Some(safe_str(&ev.filename, ev.filename_len as usize).to_string());
            row.flags = Some(ev.flags);
            Some(row)
        }
        k if k == EventKind::FileDelete as u32 && len >= size_of::<FileDeleteEvent>() => {
            // SAFETY: Length checked >= size_of::<FileDeleteEvent>(); ring buffer is 8-byte aligned.
            let ev = unsafe { &*(ptr.cast::<FileDeleteEvent>()) };
            let mut row = EventRow::from_header(header, "file_delete", mono_to_wall_offset_ns);
            row.filename = Some(safe_str(&ev.pathname, ev.pathname_len as usize).to_string());
            row.flags = Some(ev.flags);
            Some(row)
        }
        k if k == EventKind::FileRename as u32 && len >= size_of::<FileRenameEvent>() => {
            // SAFETY: Length checked >= size_of::<FileRenameEvent>(); ring buffer is 8-byte aligned.
            let ev = unsafe { &*(ptr.cast::<FileRenameEvent>()) };
            let mut row = EventRow::from_header(header, "file_rename", mono_to_wall_offset_ns);
            row.old_path = Some(safe_str(&ev.oldname, ev.oldname_len as usize).to_string());
            row.new_path = Some(safe_str(&ev.newname, ev.newname_len as usize).to_string());
            Some(row)
        }
        k if k == EventKind::Setuid as u32 && len >= size_of::<SetuidEvent>() => {
            // SAFETY: Length checked >= size_of::<SetuidEvent>(); ring buffer is 8-byte aligned.
            let ev = unsafe { &*(ptr.cast::<SetuidEvent>()) };
            let mut row = EventRow::from_header(header, "setuid", mono_to_wall_offset_ns);
            row.target_uid = Some(ev.target_uid);
            Some(row)
        }
        k if k == EventKind::Setgid as u32 && len >= size_of::<SetgidEvent>() => {
            // SAFETY: Length checked >= size_of::<SetgidEvent>(); ring buffer is 8-byte aligned.
            let ev = unsafe { &*(ptr.cast::<SetgidEvent>()) };
            let mut row = EventRow::from_header(header, "setgid", mono_to_wall_offset_ns);
            row.target_gid = Some(ev.target_gid);
            Some(row)
        }
        k if k == EventKind::ModuleLoad as u32 && len >= size_of::<ModuleLoadEvent>() => {
            // SAFETY: Length checked >= size_of::<ModuleLoadEvent>(); ring buffer is 8-byte aligned.
            let ev = unsafe { &*(ptr.cast::<ModuleLoadEvent>()) };
            let mut row = EventRow::from_header(header, "module_load", mono_to_wall_offset_ns);
            row.filename = Some(safe_str(&ev.name, ev.name_len as usize).to_string());
            row.module_taints = Some(ev.taints);
            Some(row)
        }
        k if k == EventKind::Ptrace as u32 && len >= size_of::<PtraceEvent>() => {
            // SAFETY: Length checked >= size_of::<PtraceEvent>(); ring buffer is 8-byte aligned.
            let ev = unsafe { &*(ptr.cast::<PtraceEvent>()) };
            let mut row = EventRow::from_header(header, "ptrace", mono_to_wall_offset_ns);
            row.ptrace_request = Some(ptrace_request_name(ev.request).to_string());
            row.ptrace_target_pid = Some(ev.target_pid);
            Some(row)
        }
        k if k == EventKind::MemfdCreate as u32 && len >= size_of::<MemfdCreateEvent>() => {
            // SAFETY: Length checked >= size_of::<MemfdCreateEvent>(); ring buffer is 8-byte aligned.
            let ev = unsafe { &*(ptr.cast::<MemfdCreateEvent>()) };
            let mut row = EventRow::from_header(header, "memfd_create", mono_to_wall_offset_ns);
            row.filename = Some(safe_str(&ev.name, ev.name_len as usize).to_string());
            row.flags = Some(ev.flags);
            Some(row)
        }
        k if k == EventKind::DnsQuery as u32 && len >= size_of::<DnsQueryEvent>() => {
            // SAFETY: Length checked >= size_of::<DnsQueryEvent>(); ring buffer is 8-byte aligned.
            let ev = unsafe { &*(ptr.cast::<DnsQueryEvent>()) };
            let mut row = EventRow::from_header(header, "dns_query", mono_to_wall_offset_ns);
            let wire_len = (ev.qname_len as usize).min(MAX_DNS_NAME);
            let wire = &ev.qname[..wire_len];
            let (qname, qtype) = dns_wire_to_dotted(wire);
            // Write Arrow NULLs (None) when decoding fails instead of sentinels.
            row.dns_qname = qname;
            row.dns_qtype = qtype;
            row.dns_tx_id = Some(ev.tx_id);
            row.dst_addr = Some(format_addr(ev.dst_addr));
            row.dst_port = Some(ev.dst_port);
            Some(row)
        }
        _ => None,
    }
}

/// Build an Arrow `RecordBatch` from collected event rows.
fn build_record_batch(schema: &Arc<Schema>, rows: &[EventRow]) -> io::Result<RecordBatch> {
    let len = rows.len();

    // ── Common header columns (non-null) ─────────────────────
    let mut timestamp_unix_nano = Vec::with_capacity(len);
    let mut event_kind: Vec<&str> = Vec::with_capacity(len);
    let mut tgid = Vec::with_capacity(len);
    let mut pid = Vec::with_capacity(len);
    let mut ppid = Vec::with_capacity(len);
    let mut uid = Vec::with_capacity(len);
    let mut gid = Vec::with_capacity(len);
    let mut cgroup_id = Vec::with_capacity(len);
    let mut comm: Vec<&str> = Vec::with_capacity(len);

    // ── Event-specific columns (nullable) ────────────────────
    let mut filename: Vec<Option<&str>> = Vec::with_capacity(len);
    let mut exit_code: Vec<Option<i32>> = Vec::with_capacity(len);
    let mut flags: Vec<Option<u32>> = Vec::with_capacity(len);
    let mut old_path: Vec<Option<&str>> = Vec::with_capacity(len);
    let mut new_path: Vec<Option<&str>> = Vec::with_capacity(len);
    let mut target_uid: Vec<Option<u32>> = Vec::with_capacity(len);
    let mut target_gid: Vec<Option<u32>> = Vec::with_capacity(len);
    let mut module_taints: Vec<Option<u32>> = Vec::with_capacity(len);
    let mut ptrace_request: Vec<Option<&str>> = Vec::with_capacity(len);
    let mut ptrace_target_pid: Vec<Option<u64>> = Vec::with_capacity(len);
    let mut src_addr: Vec<Option<&str>> = Vec::with_capacity(len);
    let mut dst_addr: Vec<Option<&str>> = Vec::with_capacity(len);
    let mut src_port: Vec<Option<u16>> = Vec::with_capacity(len);
    let mut dst_port: Vec<Option<u16>> = Vec::with_capacity(len);
    let mut dns_qname: Vec<Option<&str>> = Vec::with_capacity(len);
    let mut dns_qtype: Vec<Option<u16>> = Vec::with_capacity(len);
    let mut dns_tx_id: Vec<Option<u16>> = Vec::with_capacity(len);

    for row in rows {
        timestamp_unix_nano.push(row.timestamp_unix_nano);
        event_kind.push(row.event_kind);
        tgid.push(row.tgid);
        pid.push(row.pid);
        ppid.push(row.ppid);
        uid.push(row.uid);
        gid.push(row.gid);
        cgroup_id.push(row.cgroup_id);
        comm.push(&row.comm);

        filename.push(row.filename.as_deref());
        exit_code.push(row.exit_code);
        flags.push(row.flags);
        old_path.push(row.old_path.as_deref());
        new_path.push(row.new_path.as_deref());
        target_uid.push(row.target_uid);
        target_gid.push(row.target_gid);
        module_taints.push(row.module_taints);
        ptrace_request.push(row.ptrace_request.as_deref());
        ptrace_target_pid.push(row.ptrace_target_pid);
        src_addr.push(row.src_addr.as_deref());
        dst_addr.push(row.dst_addr.as_deref());
        src_port.push(row.src_port);
        dst_port.push(row.dst_port);
        dns_qname.push(row.dns_qname.as_deref());
        dns_qtype.push(row.dns_qtype);
        dns_tx_id.push(row.dns_tx_id);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(timestamp_unix_nano)),
        Arc::new(StringArray::from(event_kind)),
        Arc::new(UInt32Array::from(tgid)),
        Arc::new(UInt32Array::from(pid)),
        Arc::new(UInt32Array::from(ppid)),
        Arc::new(UInt32Array::from(uid)),
        Arc::new(UInt32Array::from(gid)),
        Arc::new(UInt64Array::from(cgroup_id)),
        Arc::new(StringArray::from(comm)),
        Arc::new(StringArray::from(filename)),
        Arc::new(Int32Array::from(exit_code)),
        Arc::new(UInt32Array::from(flags)),
        Arc::new(StringArray::from(old_path)),
        Arc::new(StringArray::from(new_path)),
        Arc::new(UInt32Array::from(target_uid)),
        Arc::new(UInt32Array::from(target_gid)),
        Arc::new(UInt32Array::from(module_taints)),
        Arc::new(StringArray::from(ptrace_request)),
        Arc::new(UInt64Array::from(ptrace_target_pid)),
        Arc::new(StringArray::from(src_addr)),
        Arc::new(StringArray::from(dst_addr)),
        Arc::new(UInt16Array::from(src_port)),
        Arc::new(UInt16Array::from(dst_port)),
        Arc::new(StringArray::from(dns_qname)),
        Arc::new(UInt16Array::from(dns_qtype)),
        Arc::new(UInt16Array::from(dns_tx_id)),
    ];

    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to build Arrow RecordBatch: {e}"),
        )
    })
}

// ── InputSource implementation ─────────────────────────────────────────

impl InputSource for PlatformSensorInput {
    fn poll(&mut self) -> io::Result<Vec<SourceEvent>> {
        let state = std::mem::replace(&mut self.state, SensorState::Poisoned);

        match state {
            SensorState::Init { config, schema } => {
                match Self::load_ebpf(&config) {
                    Ok((ebpf, skipped_probes, degraded_capabilities)) => {
                        let is_degraded =
                            !skipped_probes.is_empty() || !degraded_capabilities.is_empty();
                        if !skipped_probes.is_empty() {
                            tracing::warn!(
                                "eBPF sensor skipped probes: {}",
                                skipped_probes.join(", ")
                            );
                        }
                        if !degraded_capabilities.is_empty() {
                            tracing::warn!(
                                "eBPF sensor degraded capabilities: {}",
                                degraded_capabilities.join(", ")
                            );
                        }
                        let health = if is_degraded {
                            ComponentHealth::Degraded
                        } else {
                            ComponentHealth::Healthy
                        };
                        self.state = SensorState::Running {
                            schema,
                            config,
                            ebpf,
                            health,
                            self_tgid: std::process::id(),
                            skipped_probes,
                            degraded_capabilities,
                        };
                    }
                    Err(e) => {
                        tracing::error!("eBPF load failed: {e}");
                        self.state = SensorState::Init { config, schema };
                        return Err(io::Error::other(format!(
                            "eBPF sensor initialization failed: {e}"
                        )));
                    }
                }
                // First poll after load returns empty — data arrives next cycle.
                Ok(Vec::new())
            }
            SensorState::Running {
                schema,
                config,
                mut ebpf,
                health,
                self_tgid,
                skipped_probes,
                degraded_capabilities,
            } => {
                if let Ok(drops) = Self::check_drops(&mut ebpf)
                    && drops > 0
                {
                    self.stats.inc_ebpf_drops(drops);
                }
                let result = Self::drain_events(
                    &mut ebpf,
                    &schema,
                    config.max_events_per_poll,
                    self_tgid,
                    config.filter_self,
                    config.include_event_types.as_deref(),
                    config.exclude_event_types.as_deref(),
                );
                self.state = SensorState::Running {
                    schema,
                    config,
                    ebpf,
                    health,
                    self_tgid,
                    skipped_probes,
                    degraded_capabilities,
                };
                match result {
                    Ok(Some(event)) => Ok(vec![event]),
                    Ok(None) => Ok(Vec::new()),
                    Err(e) => Err(e),
                }
            }
            SensorState::Poisoned => Err(io::Error::other("platform sensor state poisoned")),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        match &self.state {
            SensorState::Init { .. } => ComponentHealth::Starting,
            SensorState::Running { health, .. } => *health,
            SensorState::Poisoned => ComponentHealth::Failed,
        }
    }
}

#[cfg(test)]
mod tests {
    use sensor_ebpf_common::dns::dns_wire_to_dotted;
    use std::net::Ipv4Addr;

    fn format_addr(addr: u32) -> Ipv4Addr {
        Ipv4Addr::from(addr.to_ne_bytes())
    }
}

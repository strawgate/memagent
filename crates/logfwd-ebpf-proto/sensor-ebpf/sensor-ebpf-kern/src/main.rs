//! eBPF kernel programs for the logfwd EDR sensor.
//!
//! Tracepoints / kprobes:
//!   - sched_process_exec       → process execution
//!   - sched_process_exit       → process termination
//!   - kprobe/tcp_v4_connect    → stash PID for outbound TCP (runs in process context)
//!   - inet_sock_set_state      → TCP state transitions (connect + accept with IPs/ports)
//!   - sys_enter_openat         → file open
//!
//! Build:
//!   cargo +nightly build --target bpfel-unknown-none -Z build-std=core --release

#![no_std]
#![no_main]
#![allow(dangerous_implicit_autorefs)]

use aya_ebpf::{
    helpers::{
        bpf_get_current_cgroup_id, bpf_get_current_comm, bpf_get_current_pid_tgid,
        bpf_get_current_uid_gid, bpf_ktime_get_ns, bpf_probe_read_kernel_str_bytes,
        bpf_probe_read_user_str_bytes,
    },
    macros::{kprobe, map, tracepoint},
    maps::{HashMap, RingBuf},
    programs::{ProbeContext, TracePointContext},
    EbpfContext,
};
use sensor_ebpf_common::*;

// TCP state constants from include/net/tcp_states.h
const TCP_ESTABLISHED: i32 = 1;
const TCP_SYN_SENT: i32 = 2;
const TCP_SYN_RECV: i32 = 3;

/// 16 MB ring buffer — ~100ms headroom at high event rate.
#[map]
static EVENTS: RingBuf = RingBuf::with_byte_size(16 * 1024 * 1024, 0);

/// Stash process info from kprobe (process context) so the inet_sock_set_state
/// tracepoint (softirq context) can attribute connections to the right process.
/// Key: sock pointer address, Value: ProcessInfo with PID/comm.
#[map]
static SOCK_OWNERS: HashMap<u64, ConnProcessInfo> = HashMap::with_max_entries(8192, 0);

// ── Helpers ─────────────────────────────────────────────────────────────

#[inline(always)]
fn fill_header(header: &mut EventHeader, kind: EventKind) {
    let pid_tgid = bpf_get_current_pid_tgid();
    let uid_gid = bpf_get_current_uid_gid();

    header.timestamp_ns = unsafe { bpf_ktime_get_ns() };
    header.kind = kind as u32;
    header.pid = pid_tgid as u32;
    header.tgid = (pid_tgid >> 32) as u32;
    header.uid = uid_gid as u32;
    header.gid = (uid_gid >> 32) as u32;
    header.cgroup_id = unsafe { bpf_get_current_cgroup_id() };

    // ppid is not directly available from helpers — set to 0.
    // Userspace enriches from /proc if needed.
    header.ppid = 0;

    match bpf_get_current_comm() {
        Ok(comm) => header.comm = comm,
        Err(_) => header.comm = [0u8; COMM_SIZE],
    }
}

// ── Process exec ────────────────────────────────────────────────────────

/// sched_process_exec tracepoint format (from /sys/kernel/debug/tracing):
///   field: int __data_loc filename    offset:16  size:4
///   field: pid_t pid                  offset:20  size:4
///   field: pid_t old_pid              offset:24  size:4
#[tracepoint]
pub fn sched_process_exec(ctx: TracePointContext) -> u32 {
    match try_process_exec(&ctx) {
        Ok(()) => 0,
        Err(_) => 0,
    }
}

fn try_process_exec(ctx: &TracePointContext) -> Result<(), i64> {
    let mut entry = match EVENTS.reserve::<ProcessExecEvent>(0) {
        Some(e) => e,
        None => return Ok(()),
    };

    let event = entry.as_mut_ptr();
    unsafe {
        fill_header(&mut (*event).header, EventKind::ProcessExec);

        // __data_loc filename at offset 8 in the tracepoint struct.
        // Format: bits [15:0] = offset from struct start, bits [31:16] = length.
        let data_loc: u32 = ctx.read_at(8).unwrap_or(0);
        let offset = (data_loc & 0xFFFF) as usize;

        // Read the filename from the tracepoint buffer (kernel memory).
        let base = ctx.as_ptr() as *const u8;
        let filename_ptr = base.add(offset);

        match bpf_probe_read_kernel_str_bytes(filename_ptr, &mut (*event).filename) {
            Ok(s) => (*event).filename_len = s.len() as u32,
            Err(_) => (*event).filename_len = 0,
        }
        (*event)._pad = 0;
    }

    entry.submit(0);
    Ok(())
}

// ── Process exit ────────────────────────────────────────────────────────

/// sched_process_exit tracepoint format:
///   field: char comm[16]   offset:8   size:16
///   field: pid_t pid       offset:24  size:4
///   field: int prio        offset:28  size:4
#[tracepoint]
pub fn sched_process_exit(ctx: TracePointContext) -> u32 {
    match try_process_exit(&ctx) {
        Ok(()) => 0,
        Err(_) => 0,
    }
}

fn try_process_exit(_ctx: &TracePointContext) -> Result<(), i64> {
    let mut entry = match EVENTS.reserve::<ProcessExitEvent>(0) {
        Some(e) => e,
        None => return Ok(()),
    };

    let event = entry.as_mut_ptr();
    unsafe {
        fill_header(&mut (*event).header, EventKind::ProcessExit);
        // Exit code is not directly in the tracepoint args for sched_process_exit.
        // The tracepoint gives comm + pid + prio. Exit code requires task_struct access.
        // Set to 0 for now; can be enriched from kprobe on do_exit if needed.
        (*event).exit_code = 0;
        (*event)._pad = 0;
    }

    entry.submit(0);
    Ok(())
}

// ── File open (openat) ──────────────────────────────────────────────────

/// sys_enter_openat tracepoint format:
///   field: int __syscall_nr   offset:8   size:4
///   field: int dfd            offset:16  size:8 (sign-extended to long)
///   field: const char *filename  offset:24  size:8
///   field: int flags          offset:32  size:8
///   field: umode_t mode      offset:40  size:8
#[tracepoint]
pub fn sys_enter_openat(ctx: TracePointContext) -> u32 {
    match try_file_open(&ctx) {
        Ok(()) => 0,
        Err(_) => 0,
    }
}

fn try_file_open(ctx: &TracePointContext) -> Result<(), i64> {
    let mut entry = match EVENTS.reserve::<FileOpenEvent>(0) {
        Some(e) => e,
        None => return Ok(()),
    };

    let event = entry.as_mut_ptr();
    unsafe {
        fill_header(&mut (*event).header, EventKind::FileOpen);

        // flags at offset 32 (sign-extended long on 64-bit)
        let flags: u64 = ctx.read_at(32).unwrap_or(0);
        (*event).flags = flags as u32;

        // filename pointer at offset 24
        let filename_ptr: *const u8 = ctx.read_at(24).unwrap_or(core::ptr::null());
        if !filename_ptr.is_null() {
            match bpf_probe_read_user_str_bytes(filename_ptr, &mut (*event).filename) {
                Ok(s) => (*event).filename_len = s.len() as u32,
                Err(_) => (*event).filename_len = 0,
            }
        } else {
            (*event).filename_len = 0;
        }
    }

    entry.submit(0);
    Ok(())
}

// ── TCP connect (kprobe stash + tracepoint emit) ────────────────────────

/// kprobe on `tcp_v4_connect` — fires in the connecting process's context.
/// Stashes process info keyed by sock pointer for later lookup.
/// Signature: int tcp_v4_connect(struct sock *sk, struct sockaddr *uaddr, int addr_len)
#[kprobe]
pub fn tcp_v4_connect(ctx: ProbeContext) -> u32 {
    match try_tcp_v4_connect(&ctx) {
        Ok(()) => 0,
        Err(_) => 0,
    }
}

fn try_tcp_v4_connect(ctx: &ProbeContext) -> Result<(), i64> {
    // arg(0) = struct sock *sk
    let sk: u64 = ctx.arg(0).ok_or(1i64)?;

    let pid_tgid = bpf_get_current_pid_tgid();
    let uid_gid = bpf_get_current_uid_gid();

    let info = ConnProcessInfo {
        tgid: (pid_tgid >> 32) as u32,
        pid: pid_tgid as u32,
        uid: uid_gid as u32,
        gid: (uid_gid >> 32) as u32,
        cgroup_id: unsafe { bpf_get_current_cgroup_id() },
        comm: bpf_get_current_comm().unwrap_or([0u8; COMM_SIZE]),
    };

    SOCK_OWNERS.insert(&sk, &info, 0).ok();
    Ok(())
}

// ── TCP state transition (inet_sock_set_state) ──────────────────────────

/// inet_sock_set_state tracepoint format:
///   field: const void *skaddr   offset:8   size:8
///   field: int oldstate          offset:16  size:4
///   field: int newstate          offset:20  size:4
///   field: __u16 sport           offset:24  size:2
///   field: __u16 dport           offset:26  size:2
///   field: __u16 family          offset:28  size:2
///   field: __u16 protocol        offset:30  size:2
///   field: __u8 saddr[4]         offset:32  size:4
///   field: __u8 daddr[4]         offset:36  size:4
#[tracepoint]
pub fn inet_sock_set_state(ctx: TracePointContext) -> u32 {
    match try_sock_state(&ctx) {
        Ok(()) => 0,
        Err(_) => 0,
    }
}

fn try_sock_state(ctx: &TracePointContext) -> Result<(), i64> {
    let oldstate: i32 = unsafe { ctx.read_at(16)? };
    let newstate: i32 = unsafe { ctx.read_at(20)? };

    // Outbound connect: SYN_SENT → ESTABLISHED
    // Use stashed process info from kprobe for correct PID attribution.
    if oldstate == TCP_SYN_SENT && newstate == TCP_ESTABLISHED {
        let skaddr: u64 = unsafe { ctx.read_at(8).unwrap_or(0) };

        let mut entry = match EVENTS.reserve::<TcpConnectEvent>(0) {
            Some(e) => e,
            None => return Ok(()),
        };

        let event = entry.as_mut_ptr();
        unsafe {
            // Look up the stashed process info from the kprobe.
            if let Some(info) = SOCK_OWNERS.get(&skaddr) {
                fill_header_from_info(&mut (*event).header, EventKind::TcpConnect, info);
            } else {
                // Fallback: use current context (may be softirq/swapper).
                fill_header(&mut (*event).header, EventKind::TcpConnect);
            }
            read_sock_addrs(ctx, &mut (*event).saddr, &mut (*event).daddr,
                            &mut (*event).sport, &mut (*event).dport);
            // Clean up the stash entry.
            SOCK_OWNERS.remove(&skaddr).ok();
        }
        entry.submit(0);
    }

    // Inbound accept: SYN_RECV → ESTABLISHED
    // No kprobe stash for inbound — PID will be the softirq handler.
    // Userspace can correlate via /proc/net/tcp if needed.
    if oldstate == TCP_SYN_RECV && newstate == TCP_ESTABLISHED {
        let mut entry = match EVENTS.reserve::<TcpAcceptEvent>(0) {
            Some(e) => e,
            None => return Ok(()),
        };

        let event = entry.as_mut_ptr();
        unsafe {
            fill_header(&mut (*event).header, EventKind::TcpAccept);
            read_sock_addrs(ctx, &mut (*event).saddr, &mut (*event).daddr,
                            &mut (*event).sport, &mut (*event).dport);
        }
        entry.submit(0);
    }

    Ok(())
}

/// Fill event header from stashed ConnProcessInfo (for kprobe→tracepoint handoff).
#[inline(always)]
fn fill_header_from_info(header: &mut EventHeader, kind: EventKind, info: &ConnProcessInfo) {
    header.timestamp_ns = unsafe { bpf_ktime_get_ns() };
    header.kind = kind as u32;
    header.tgid = info.tgid;
    header.pid = info.pid;
    header.uid = info.uid;
    header.gid = info.gid;
    header.cgroup_id = info.cgroup_id;
    header.ppid = 0;
    header.comm = info.comm;
}

/// Read socket address fields from the inet_sock_set_state tracepoint.
#[inline(always)]
unsafe fn read_sock_addrs(
    ctx: &TracePointContext,
    saddr: &mut u32,
    daddr: &mut u32,
    sport: &mut u16,
    dport: &mut u16,
) {
    // sport at offset 24, dport at 26
    *sport = ctx.read_at(24).unwrap_or(0);
    *dport = ctx.read_at(26).unwrap_or(0);

    // saddr[4] at offset 32, daddr[4] at offset 36
    // Read as u32 (4 bytes in network byte order).
    *saddr = ctx.read_at(32).unwrap_or(0);
    *daddr = ctx.read_at(36).unwrap_or(0);
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}

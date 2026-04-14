//! eBPF kernel programs for the logfwd EDR sensor.
//!
//! Hooks:
//!   - sched_process_exec       → process execution with binary path
//!   - sched_process_exit       → process termination
//!   - kprobe/tcp_v4_connect    → stash PID for outbound TCP
//!   - inet_sock_set_state      → TCP state transitions (connect + accept)
//!   - sys_enter_openat         → file open
//!   - sys_enter_unlinkat       → file delete
//!   - sys_enter_renameat2      → file rename
//!   - sys_enter_setuid         → privilege escalation
//!   - sys_enter_setgid         → privilege escalation
//!   - module/module_load       → kernel module loading
//!   - sys_enter_ptrace         → process injection / debugging
//!   - sys_enter_memfd_create   → fileless malware staging
//!
//! Build:
//!   cargo +nightly build --target bpfel-unknown-none -Z build-std=core --release

#![no_std]
#![no_main]
#![allow(dangerous_implicit_autorefs)]

use aya_ebpf::{
    helpers::{
        bpf_get_current_cgroup_id, bpf_get_current_comm, bpf_get_current_pid_tgid,
        bpf_get_current_task, bpf_get_current_uid_gid, bpf_ktime_get_ns,
        bpf_probe_read_kernel, bpf_probe_read_kernel_str_bytes,
        bpf_probe_read_user_str_bytes,
    },
    macros::{kprobe, map, tracepoint},
    maps::{Array, HashMap, RingBuf},
    programs::{ProbeContext, TracePointContext},
    EbpfContext,
};
use sensor_ebpf_common::*;

// TCP state constants from include/net/tcp_states.h
const TCP_ESTABLISHED: i32 = 1;
const TCP_SYN_SENT: i32 = 2;
const TCP_SYN_RECV: i32 = 3;
const TCP_CLOSE: i32 = 7;

/// 16 MB ring buffer — ~100ms headroom at high event rate.
#[map]
static EVENTS: RingBuf = RingBuf::with_byte_size(16 * 1024 * 1024, 0);

/// Stash process info from kprobe (process context) so the inet_sock_set_state
/// tracepoint (softirq context) can attribute connections to the right process.
#[map]
static SOCK_OWNERS: HashMap<u64, ConnProcessInfo> = HashMap::with_max_entries(8192, 0);

/// Runtime configuration from userspace (e.g., task_struct field offsets from BTF).
#[map]
static CONFIG: Array<EbpfConfig> = Array::with_max_entries(1, 0);

// ── Helpers ─────────────────────────────────────────────────────────────

#[inline(always)]
fn fill_header(header: &mut EventHeader, kind: EventKind) {
    let pid_tgid = bpf_get_current_pid_tgid();
    let uid_gid = bpf_get_current_uid_gid();

    // SAFETY: bpf_ktime_get_ns is always safe to call in BPF program context.
    header.timestamp_ns = unsafe { bpf_ktime_get_ns() };
    header.kind = kind as u32;
    header.pid = pid_tgid as u32;
    header.tgid = (pid_tgid >> 32) as u32;
    header.uid = uid_gid as u32;
    header.gid = (uid_gid >> 32) as u32;
    // SAFETY: bpf_get_current_cgroup_id is always safe to call in BPF program context.
    header.cgroup_id = unsafe { bpf_get_current_cgroup_id() };
    header.ppid = 0;

    match bpf_get_current_comm() {
        Ok(comm) => header.comm = comm,
        Err(_) => header.comm = [0u8; COMM_SIZE],
    }
}

/// Fill event header from stashed ConnProcessInfo (for kprobe→tracepoint handoff).
#[inline(always)]
fn fill_header_from_info(header: &mut EventHeader, kind: EventKind, info: &ConnProcessInfo) {
    // SAFETY: bpf_ktime_get_ns is always safe to call in BPF program context.
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

/// Read a user-space string into a buffer, returning the length captured.
///
/// # Safety
/// `ptr` must be a valid user-space pointer or null (null is handled).
/// `buf` must be a valid mutable slice with sufficient length for the BPF helper.
#[inline(always)]
unsafe fn read_user_str(ptr: *const u8, buf: &mut [u8]) -> u32 {
    if ptr.is_null() {
        return 0;
    }
    match bpf_probe_read_user_str_bytes(ptr, buf) {
        Ok(s) => s.len() as u32,
        Err(_) => 0,
    }
}

// ═══════════════════════════════════════════════════════════════════════
// PROCESS EVENTS
// ═══════════════════════════════════════════════════════════════════════

// ── Process exec ────────────────────────────────────────────────────────

/// sched_process_exec tracepoint:
///   __data_loc filename  offset:8   (bits [15:0]=offset, [31:16]=length)
#[tracepoint]
pub fn sched_process_exec(ctx: TracePointContext) -> u32 {
    match try_process_exec(&ctx) {
        Ok(()) | Err(_) => 0,
    }
}

fn try_process_exec(ctx: &TracePointContext) -> Result<(), i64> {
    let mut entry = match EVENTS.reserve::<ProcessExecEvent>(0) {
        Some(e) => e,
        None => return Ok(()),
    };

    let event = entry.as_mut_ptr();
    // SAFETY: `event` is a valid pointer from RingBuf::reserve; fields are written
    // before submit. ctx.as_ptr() points to the tracepoint payload and the
    // data_loc offset is kernel-provided, so the derived filename_ptr is valid
    // for a kernel string read.
    unsafe {

        let data_loc: u32 = ctx.read_at(8).unwrap_or(0);
        let offset = (data_loc & 0xFFFF) as usize;
        let base = ctx.as_ptr() as *const u8;
        let filename_ptr = base.add(offset);

        match bpf_probe_read_kernel_str_bytes(filename_ptr, &mut (*event).filename) {
            Ok(s) => (*event).filename_len = s.len() as u32,
            Err(_) => (*event).filename_len = 0,
        }
        (*event).pad = 0;

        fill_header(&mut (*event).header, EventKind::ProcessExec);
    }

    entry.submit(0);
    Ok(())
}

// ── Process exit ────────────────────────────────────────────────────────

#[tracepoint]
pub fn sched_process_exit(ctx: TracePointContext) -> u32 {
    match try_process_exit(&ctx) {
        Ok(()) | Err(_) => 0,
    }
}

fn try_process_exit(_ctx: &TracePointContext) -> Result<(), i64> {
    let mut entry = match EVENTS.reserve::<ProcessExitEvent>(0) {
        Some(e) => e,
        None => return Ok(()),
    };

    let event = entry.as_mut_ptr();
    // SAFETY: `event` is a valid pointer from RingBuf::reserve; all fields are
    // written before submit.
    unsafe {
        fill_header(&mut (*event).header, EventKind::ProcessExit);

        // Try to read exit_code from task_struct using BTF-provided offset.
        // Falls back to -1 sentinel when offset is unavailable or read fails.
        let mut exit_code: i32 = -1;
        if let Some(cfg) = CONFIG.get(0) {
            let offset = cfg.task_exit_code_offset;
            // Sanity check: offset must be within a reasonable task_struct range.
            if offset > 0 && offset < 16384 {
                // SAFETY: bpf_get_current_task returns a pointer to the current
                // task_struct; offset is from kernel BTF set by userspace loader.
                let task = bpf_get_current_task();
                if task != 0 {
                    let ptr = (task as *const u8).add(offset as usize) as *const i32;
                    // SAFETY: ptr points into task_struct at the BTF-verified offset.
                    if let Ok(code) = bpf_probe_read_kernel(ptr) {
                        // Linux task_struct.exit_code packs: (status << 8) | signal.
                        // Extract the exit status for userspace.
                        exit_code = code >> 8;
                    }
                }
            }
        }

        (*event).exit_code = exit_code;
        (*event).pad = 0;
    }

    entry.submit(0);
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════
// FILE EVENTS
// ═══════════════════════════════════════════════════════════════════════

// ── File open (openat) ──────────────────────────────────────────────────

/// sys_enter_openat: filename ptr at offset 24, flags at offset 32
#[tracepoint]
pub fn sys_enter_openat(ctx: TracePointContext) -> u32 {
    match try_file_open(&ctx) {
        Ok(()) | Err(_) => 0,
    }
}

fn try_file_open(ctx: &TracePointContext) -> Result<(), i64> {
    let mut entry = match EVENTS.reserve::<FileOpenEvent>(0) {
        Some(e) => e,
        None => return Ok(()),
    };

    let event = entry.as_mut_ptr();
    // SAFETY: `event` points to RingBuf-reserved memory; tracepoint offsets are
    // fixed by the kernel ABI. User-space pointer from ctx.read_at is passed to
    // read_user_str which null-checks before reading.
    unsafe {
        fill_header(&mut (*event).header, EventKind::FileOpen);
        let flags: u64 = ctx.read_at(32).unwrap_or(0);
        (*event).flags = flags as u32;
        let filename_ptr: *const u8 = ctx.read_at(24).unwrap_or(core::ptr::null());
        (*event).filename_len = read_user_str(filename_ptr, &mut (*event).filename);
    }

    entry.submit(0);
    Ok(())
}

// ── File delete (unlinkat) ──────────────────────────────────────────────

/// sys_enter_unlinkat: pathname ptr at offset 24, flag at offset 32
#[tracepoint]
pub fn sys_enter_unlinkat(ctx: TracePointContext) -> u32 {
    match try_file_delete(&ctx) {
        Ok(()) | Err(_) => 0,
    }
}

fn try_file_delete(ctx: &TracePointContext) -> Result<(), i64> {
    let mut entry = match EVENTS.reserve::<FileDeleteEvent>(0) {
        Some(e) => e,
        None => return Ok(()),
    };

    let event = entry.as_mut_ptr();
    // SAFETY: `event` points to RingBuf-reserved memory; user-space pointer
    // from ctx.read_at is null-checked by read_user_str.
    unsafe {
        fill_header(&mut (*event).header, EventKind::FileDelete);
        let flags: u64 = ctx.read_at(32).unwrap_or(0);
        (*event).flags = flags as u32;
        let pathname_ptr: *const u8 = ctx.read_at(24).unwrap_or(core::ptr::null());
        (*event).pathname_len = read_user_str(pathname_ptr, &mut (*event).pathname);
    }

    entry.submit(0);
    Ok(())
}

// ── File rename (renameat2) ─────────────────────────────────────────────

/// sys_enter_renameat2: oldname ptr at offset 24, newname ptr at offset 40
#[tracepoint]
pub fn sys_enter_renameat2(ctx: TracePointContext) -> u32 {
    match try_file_rename(&ctx) {
        Ok(()) | Err(_) => 0,
    }
}

fn try_file_rename(ctx: &TracePointContext) -> Result<(), i64> {
    let mut entry = match EVENTS.reserve::<FileRenameEvent>(0) {
        Some(e) => e,
        None => return Ok(()),
    };

    let event = entry.as_mut_ptr();
    // SAFETY: `event` points to RingBuf-reserved memory; user-space pointers
    // from ctx.read_at are null-checked by read_user_str.
    unsafe {
        fill_header(&mut (*event).header, EventKind::FileRename);
        let oldname_ptr: *const u8 = ctx.read_at(24).unwrap_or(core::ptr::null());
        (*event).oldname_len = read_user_str(oldname_ptr, &mut (*event).oldname);
        let newname_ptr: *const u8 = ctx.read_at(40).unwrap_or(core::ptr::null());
        (*event).newname_len = read_user_str(newname_ptr, &mut (*event).newname);
    }

    entry.submit(0);
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════
// SECURITY EVENTS
// ═══════════════════════════════════════════════════════════════════════

// ── Privilege escalation: setuid ────────────────────────────────────────

/// sys_enter_setuid: uid at offset 16 (sign-extended to long)
#[tracepoint]
pub fn sys_enter_setuid(ctx: TracePointContext) -> u32 {
    match try_setuid(&ctx) {
        Ok(()) | Err(_) => 0,
    }
}

fn try_setuid(ctx: &TracePointContext) -> Result<(), i64> {
    let mut entry = match EVENTS.reserve::<SetuidEvent>(0) {
        Some(e) => e,
        None => return Ok(()),
    };

    let event = entry.as_mut_ptr();
    // SAFETY: `event` points to RingBuf-reserved memory; tracepoint field at
    // fixed offset 16 read via ctx.read_at.
    unsafe {
        fill_header(&mut (*event).header, EventKind::Setuid);
        let uid: u64 = ctx.read_at(16).unwrap_or(0);
        (*event).target_uid = uid as u32;
        (*event).pad = 0;
    }

    entry.submit(0);
    Ok(())
}

// ── Privilege escalation: setgid ────────────────────────────────────────

/// sys_enter_setgid: gid at offset 16 (sign-extended to long)
#[tracepoint]
pub fn sys_enter_setgid(ctx: TracePointContext) -> u32 {
    match try_setgid(&ctx) {
        Ok(()) | Err(_) => 0,
    }
}

fn try_setgid(ctx: &TracePointContext) -> Result<(), i64> {
    let mut entry = match EVENTS.reserve::<SetgidEvent>(0) {
        Some(e) => e,
        None => return Ok(()),
    };

    let event = entry.as_mut_ptr();
    // SAFETY: `event` points to RingBuf-reserved memory; tracepoint field at
    // fixed offset 16 read via ctx.read_at.
    unsafe {
        fill_header(&mut (*event).header, EventKind::Setgid);
        let gid: u64 = ctx.read_at(16).unwrap_or(0);
        (*event).target_gid = gid as u32;
        (*event).pad = 0;
    }

    entry.submit(0);
    Ok(())
}

// ── Kernel module load ──────────────────────────────────────────────────

/// module_load tracepoint: taints (u32) at offset 8, __data_loc name at offset 12
#[tracepoint]
pub fn module_load(ctx: TracePointContext) -> u32 {
    match try_module_load(&ctx) {
        Ok(()) | Err(_) => 0,
    }
}

fn try_module_load(ctx: &TracePointContext) -> Result<(), i64> {
    let mut entry = match EVENTS.reserve::<ModuleLoadEvent>(0) {
        Some(e) => e,
        None => return Ok(()),
    };

    let event = entry.as_mut_ptr();
    // SAFETY: `event` points to RingBuf-reserved memory; data_loc offset from
    // the kernel gives a valid pointer for bpf_probe_read_kernel_str_bytes.
    unsafe {
        fill_header(&mut (*event).header, EventKind::ModuleLoad);
        (*event).taints = ctx.read_at(8).unwrap_or(0);

        // __data_loc name at offset 12: bits [15:0]=offset, [31:16]=length
        let data_loc: u32 = ctx.read_at(12).unwrap_or(0);
        let offset = (data_loc & 0xFFFF) as usize;
        let base = ctx.as_ptr() as *const u8;
        let name_ptr = base.add(offset);

        match bpf_probe_read_kernel_str_bytes(name_ptr, &mut (*event).name) {
            Ok(s) => (*event).name_len = s.len() as u32,
            Err(_) => (*event).name_len = 0,
        }
    }

    entry.submit(0);
    Ok(())
}

// ── Ptrace (process injection / debugging) ──────────────────────────────

/// sys_enter_ptrace: request at offset 16, pid at offset 24
#[tracepoint]
pub fn sys_enter_ptrace(ctx: TracePointContext) -> u32 {
    match try_ptrace(&ctx) {
        Ok(()) | Err(_) => 0,
    }
}

fn try_ptrace(ctx: &TracePointContext) -> Result<(), i64> {
    let mut entry = match EVENTS.reserve::<PtraceEvent>(0) {
        Some(e) => e,
        None => return Ok(()),
    };

    let event = entry.as_mut_ptr();
    // SAFETY: `event` points to RingBuf-reserved memory; tracepoint fields at
    // fixed offsets 16 and 24 read via ctx.read_at.
    unsafe {
        fill_header(&mut (*event).header, EventKind::Ptrace);
        (*event).request = ctx.read_at(16).unwrap_or(0);
        (*event).target_pid = ctx.read_at(24).unwrap_or(0);
    }

    entry.submit(0);
    Ok(())
}

// ── memfd_create (fileless malware staging) ─────────────────────────────

/// sys_enter_memfd_create: uname ptr at offset 16, flags at offset 24
#[tracepoint]
pub fn sys_enter_memfd_create(ctx: TracePointContext) -> u32 {
    match try_memfd_create(&ctx) {
        Ok(()) | Err(_) => 0,
    }
}

fn try_memfd_create(ctx: &TracePointContext) -> Result<(), i64> {
    let mut entry = match EVENTS.reserve::<MemfdCreateEvent>(0) {
        Some(e) => e,
        None => return Ok(()),
    };

    let event = entry.as_mut_ptr();
    // SAFETY: `event` points to RingBuf-reserved memory; user-space pointer
    // from ctx.read_at is null-checked by read_user_str.
    unsafe {
        fill_header(&mut (*event).header, EventKind::MemfdCreate);
        let flags: u64 = ctx.read_at(24).unwrap_or(0);
        (*event).flags = flags as u32;
        let name_ptr: *const u8 = ctx.read_at(16).unwrap_or(core::ptr::null());
        (*event).name_len = read_user_str(name_ptr, &mut (*event).name);
    }

    entry.submit(0);
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════
// NETWORK EVENTS
// ═══════════════════════════════════════════════════════════════════════

// ── TCP connect (kprobe stash) ──────────────────────────────────────────

/// kprobe on `tcp_v4_connect` — fires in the connecting process's context.
/// Stashes process info keyed by sock pointer for later lookup.
#[kprobe]
pub fn tcp_v4_connect(ctx: ProbeContext) -> u32 {
    match try_tcp_v4_connect(&ctx) {
        Ok(()) | Err(_) => 0,
    }
}

fn try_tcp_v4_connect(ctx: &ProbeContext) -> Result<(), i64> {
    let sk: u64 = ctx.arg(0).ok_or(1i64)?;

    let pid_tgid = bpf_get_current_pid_tgid();
    let uid_gid = bpf_get_current_uid_gid();

    let info = ConnProcessInfo {
        tgid: (pid_tgid >> 32) as u32,
        pid: pid_tgid as u32,
        uid: uid_gid as u32,
        gid: (uid_gid >> 32) as u32,
        cgroup_id: unsafe { bpf_get_current_cgroup_id() }, // SAFETY: always valid in BPF ctx
        comm: bpf_get_current_comm().unwrap_or([0u8; COMM_SIZE]),
    };

    SOCK_OWNERS.insert(&sk, &info, 0).ok();
    Ok(())
}

// ── TCP state transition (inet_sock_set_state) ──────────────────────────

/// inet_sock_set_state tracepoint:
///   skaddr at offset 8, oldstate at 16, newstate at 20,
///   sport at 24, dport at 26, saddr[4] at 32, daddr[4] at 36
#[tracepoint]
pub fn inet_sock_set_state(ctx: TracePointContext) -> u32 {
    match try_sock_state(&ctx) {
        Ok(()) | Err(_) => 0,
    }
}

fn try_sock_state(ctx: &TracePointContext) -> Result<(), i64> {
    // SAFETY: tracepoint field reads at fixed kernel ABI offsets.
    let oldstate: i32 = unsafe { ctx.read_at(16)? };
    let newstate: i32 = unsafe { ctx.read_at(20)? };

    // Outbound connect: SYN_SENT → ESTABLISHED
    if oldstate == TCP_SYN_SENT && newstate == TCP_ESTABLISHED {
        let skaddr: u64 = unsafe { ctx.read_at(8)? };

        let mut entry = match EVENTS.reserve::<TcpConnectEvent>(0) {
            Some(e) => e,
            None => return Ok(()),
        };

        let event = entry.as_mut_ptr();
        // SAFETY: `event` points to RingBuf-reserved memory; SOCK_OWNERS lookup
        // and read_sock_addrs use valid tracepoint offsets.
        unsafe {
            if let Some(info) = SOCK_OWNERS.get(&skaddr) {
                fill_header_from_info(&mut (*event).header, EventKind::TcpConnect, info);
            } else {
                fill_header(&mut (*event).header, EventKind::TcpConnect);
            }
            read_sock_addrs(ctx, &mut (*event).saddr, &mut (*event).daddr,
                            &mut (*event).sport, &mut (*event).dport);
            SOCK_OWNERS.remove(&skaddr).ok();
        }
        entry.submit(0);
    }

    // Failed connect: clean up SOCK_OWNERS on terminal failure states.
    // Excludes SYN_SENT → SYN_RECV (simultaneous open) and
    // SYN_SENT → ESTABLISHED (handled above). (#1934)
    if oldstate == TCP_SYN_SENT
        && newstate != TCP_ESTABLISHED
        && newstate != TCP_SYN_RECV
    {
        let skaddr: u64 = unsafe { ctx.read_at(8)? };
        SOCK_OWNERS.remove(&skaddr).ok();
    }

    // Safety net: on transition to CLOSE, remove any stale entries that
    // survived unexpected state paths (e.g. resets after ESTABLISHED).
    // Only fires for sockets that passed through tcp_v4_connect (kprobe),
    // so the hashmap lookup is bounded by the connect rate.
    if newstate == TCP_CLOSE && oldstate != TCP_SYN_SENT {
        let skaddr: u64 = unsafe { ctx.read_at(8)? };
        SOCK_OWNERS.remove(&skaddr).ok();
    }

    // Inbound accept: SYN_RECV → ESTABLISHED
    if oldstate == TCP_SYN_RECV && newstate == TCP_ESTABLISHED {
        let mut entry = match EVENTS.reserve::<TcpAcceptEvent>(0) {
            Some(e) => e,
            None => return Ok(()),
        };

        let event = entry.as_mut_ptr();
        // SAFETY: `event` points to RingBuf-reserved memory; read_sock_addrs
        // uses valid tracepoint offsets.
        unsafe {
            fill_header(&mut (*event).header, EventKind::TcpAccept);
            read_sock_addrs(ctx, &mut (*event).saddr, &mut (*event).daddr,
                            &mut (*event).sport, &mut (*event).dport);
        }
        entry.submit(0);
    }

    Ok(())
}

/// Read socket address fields from an inet_sock_set_state tracepoint context.
///
/// # Safety
/// The caller must ensure `ctx` points to a valid inet_sock_set_state tracepoint
/// payload with sport at offset 24, dport at 26, saddr at 32, daddr at 36.
#[inline(always)]
unsafe fn read_sock_addrs(
    ctx: &TracePointContext,
    saddr: &mut u32,
    daddr: &mut u32,
    sport: &mut u16,
    dport: &mut u16,
) {
    // The inet_sock_set_state tracepoint stores ports in host byte order
    // (the kernel's trace_inet_sock_set_state does ntohs() internally).
    *sport = ctx.read_at(24).unwrap_or(0);
    *dport = ctx.read_at(26).unwrap_or(0);
    *saddr = ctx.read_at(32).unwrap_or(0);
    *daddr = ctx.read_at(36).unwrap_or(0);
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}

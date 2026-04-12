//! Userspace loader for the logfwd eBPF EDR sensor.
//!
//! Loads eBPF programs, attaches to tracepoints/kprobes, and consumes events
//! from a shared ring buffer. Prints structured JSON to stdout.
//!
//! Usage:
//!   sudo ./sensor-ebpf `<ebpf-binary-path>` \[--json\] \[--duration `<seconds>`\] \[--no-file-open\]

// Ring buffer events are 8-byte aligned by the kernel, so casting from *const u8
// to a repr(C) struct pointer is safe despite clippy's alignment warning.
#![allow(clippy::cast_ptr_alignment)]

use aya::Ebpf;
use aya::maps::RingBuf;
use aya::programs::{KProbe, TracePoint};
use sensor_ebpf_common::*;
use std::io::Write;
use std::net::Ipv4Addr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let ebpf_path = args.get(1).map_or(
        "sensor-ebpf-kern/target/bpfel-unknown-none/release/sensor-ebpf",
        String::as_str,
    );
    let json_mode = args.iter().any(|a| a == "--json");
    let no_file_open = args.iter().any(|a| a == "--no-file-open");
    let duration_secs: u64 = args
        .iter()
        .position(|a| a == "--duration")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    let self_tgid = std::process::id();

    eprintln!("Loading eBPF from {ebpf_path}...");
    let ebpf_bytes = std::fs::read(ebpf_path)?;
    let mut ebpf = Ebpf::load(&ebpf_bytes)?;

    // Attach tracepoints.
    let tracepoints: &[(&str, &str, &str)] = &[
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
    ];

    for (prog_name, category, tracepoint) in tracepoints {
        match ebpf.program_mut(prog_name) {
            Some(prog) => {
                let tp: &mut TracePoint = prog.try_into()?;
                tp.load()?;
                tp.attach(category, tracepoint)?;
                eprintln!("  attached {category}/{tracepoint}");
            }
            None => {
                eprintln!("  SKIP {prog_name}: not found in eBPF binary");
            }
        }
    }

    // Attach kprobes.
    let kprobes: &[(&str, &str)] = &[("tcp_v4_connect", "tcp_v4_connect")];

    for (prog_name, fn_name) in kprobes {
        match ebpf.program_mut(prog_name) {
            Some(prog) => {
                let kp: &mut KProbe = prog.try_into()?;
                kp.load()?;
                kp.attach(fn_name, 0)?;
                eprintln!("  attached kprobe/{fn_name}");
            }
            None => {
                eprintln!("  SKIP kprobe/{prog_name}: not found in eBPF binary");
            }
        }
    }

    eprintln!("Sensor running for {duration_secs}s. Events on stdout.");

    let mut ring = RingBuf::try_from(ebpf.map_mut("EVENTS").unwrap())?;
    let mut stdout = std::io::stdout().lock();
    let start = Instant::now();
    let deadline = Duration::from_secs(duration_secs);

    let mut counts = EventCounts::default();

    while start.elapsed() < deadline {
        while let Some(item) = ring.next() {
            let ptr = item.as_ptr();
            let len = item.len();

            if len < size_of::<EventHeader>() {
                counts.malformed += 1;
                continue;
            }

            // SAFETY: length checked >= size_of::<EventHeader>() above; ring buffer is 8-byte aligned.
            let header = unsafe { &*(ptr.cast::<EventHeader>()) };

            if header.tgid == self_tgid {
                counts.self_filtered += 1;
                continue;
            }

            let now_wall = wall_clock_ns();
            let comm = comm_str(&header.comm);
            let comm_esc = json_escape(comm);

            match header.kind {
                // ── Process exec ──────────────────────────────
                k if k == EventKind::ProcessExec as u32 && len >= size_of::<ProcessExecEvent>() => {
                    // SAFETY: length checked >= size_of::<ProcessExecEvent>(); ring buffer is 8-byte aligned.
                    let ev = unsafe { &*(ptr.cast::<ProcessExecEvent>()) };
                    counts.exec += 1;
                    let fname = safe_str(&ev.filename, ev.filename_len as usize);
                    if json_mode {
                        writeln!(
                            stdout,
                            r#"{{"ts":{},"kind":"exec","tgid":{},"pid":{},"uid":{},"gid":{},"cgroup":{},"comm":"{}","filename":"{}"}}"#,
                            now_wall,
                            header.tgid,
                            header.pid,
                            header.uid,
                            header.gid,
                            header.cgroup_id,
                            comm_esc,
                            json_escape(fname)
                        )?;
                    } else {
                        writeln!(
                            stdout,
                            "EXEC  tgid={:<6} comm={:<16} exe={}",
                            header.tgid, comm, fname
                        )?;
                    }
                }

                // ── Process exit ──────────────────────────────
                k if k == EventKind::ProcessExit as u32 && len >= size_of::<ProcessExitEvent>() => {
                    // SAFETY: length checked >= size_of::<ProcessExitEvent>(); ring buffer is 8-byte aligned.
                    let ev = unsafe { &*(ptr.cast::<ProcessExitEvent>()) };
                    counts.exit += 1;
                    if json_mode {
                        writeln!(
                            stdout,
                            r#"{{"ts":{},"kind":"exit","tgid":{},"pid":{},"uid":{},"gid":{},"cgroup":{},"comm":"{}","exit_code":{}}}"#,
                            now_wall,
                            header.tgid,
                            header.pid,
                            header.uid,
                            header.gid,
                            header.cgroup_id,
                            comm_esc,
                            ev.exit_code
                        )?;
                    } else {
                        writeln!(
                            stdout,
                            "EXIT  tgid={:<6} comm={:<16} code={}",
                            header.tgid, comm, ev.exit_code
                        )?;
                    }
                }

                // ── File open ─────────────────────────────────
                k if k == EventKind::FileOpen as u32 && len >= size_of::<FileOpenEvent>() => {
                    counts.file_open += 1;
                    if no_file_open {
                        continue;
                    }
                    // SAFETY: length checked >= size_of::<FileOpenEvent>(); ring buffer is 8-byte aligned.
                    let ev = unsafe { &*(ptr.cast::<FileOpenEvent>()) };
                    let fname = safe_str(&ev.filename, ev.filename_len as usize);
                    if json_mode {
                        writeln!(
                            stdout,
                            r#"{{"ts":{},"kind":"file_open","tgid":{},"pid":{},"uid":{},"gid":{},"cgroup":{},"comm":"{}","flags":{},"filename":"{}"}}"#,
                            now_wall,
                            header.tgid,
                            header.pid,
                            header.uid,
                            header.gid,
                            header.cgroup_id,
                            comm_esc,
                            ev.flags,
                            json_escape(fname)
                        )?;
                    } else {
                        writeln!(
                            stdout,
                            "OPEN  tgid={:<6} comm={:<16} flags={:#06x} file={}",
                            header.tgid, comm, ev.flags, fname
                        )?;
                    }
                }

                // ── File delete ───────────────────────────────
                k if k == EventKind::FileDelete as u32 && len >= size_of::<FileDeleteEvent>() => {
                    // SAFETY: length checked >= size_of::<FileDeleteEvent>(); ring buffer is 8-byte aligned.
                    let ev = unsafe { &*(ptr.cast::<FileDeleteEvent>()) };
                    counts.file_delete += 1;
                    let path = safe_str(&ev.pathname, ev.pathname_len as usize);
                    if json_mode {
                        writeln!(
                            stdout,
                            r#"{{"ts":{},"kind":"file_delete","tgid":{},"pid":{},"uid":{},"gid":{},"cgroup":{},"comm":"{}","flags":{},"pathname":"{}"}}"#,
                            now_wall,
                            header.tgid,
                            header.pid,
                            header.uid,
                            header.gid,
                            header.cgroup_id,
                            comm_esc,
                            ev.flags,
                            json_escape(path)
                        )?;
                    } else {
                        writeln!(
                            stdout,
                            "DEL   tgid={:<6} comm={:<16} path={}",
                            header.tgid, comm, path
                        )?;
                    }
                }

                // ── File rename ───────────────────────────────
                k if k == EventKind::FileRename as u32 && len >= size_of::<FileRenameEvent>() => {
                    // SAFETY: length checked >= size_of::<FileRenameEvent>(); ring buffer is 8-byte aligned.
                    let ev = unsafe { &*(ptr.cast::<FileRenameEvent>()) };
                    counts.file_rename += 1;
                    let old = safe_str(&ev.oldname, ev.oldname_len as usize);
                    let new = safe_str(&ev.newname, ev.newname_len as usize);
                    if json_mode {
                        writeln!(
                            stdout,
                            r#"{{"ts":{},"kind":"file_rename","tgid":{},"pid":{},"uid":{},"gid":{},"cgroup":{},"comm":"{}","oldname":"{}","newname":"{}"}}"#,
                            now_wall,
                            header.tgid,
                            header.pid,
                            header.uid,
                            header.gid,
                            header.cgroup_id,
                            comm_esc,
                            json_escape(old),
                            json_escape(new)
                        )?;
                    } else {
                        writeln!(
                            stdout,
                            "RNAM  tgid={:<6} comm={:<16} {} -> {}",
                            header.tgid, comm, old, new
                        )?;
                    }
                }

                // ── Setuid ────────────────────────────────────
                k if k == EventKind::Setuid as u32 && len >= size_of::<SetuidEvent>() => {
                    // SAFETY: length checked >= size_of::<SetuidEvent>(); ring buffer is 8-byte aligned.
                    let ev = unsafe { &*(ptr.cast::<SetuidEvent>()) };
                    counts.setuid += 1;
                    if json_mode {
                        writeln!(
                            stdout,
                            r#"{{"ts":{},"kind":"setuid","tgid":{},"pid":{},"uid":{},"gid":{},"cgroup":{},"comm":"{}","target_uid":{}}}"#,
                            now_wall,
                            header.tgid,
                            header.pid,
                            header.uid,
                            header.gid,
                            header.cgroup_id,
                            comm_esc,
                            ev.target_uid
                        )?;
                    } else {
                        writeln!(
                            stdout,
                            "SUID  tgid={:<6} comm={:<16} uid={} -> {}",
                            header.tgid, comm, header.uid, ev.target_uid
                        )?;
                    }
                }

                // ── Setgid ────────────────────────────────────
                k if k == EventKind::Setgid as u32 && len >= size_of::<SetgidEvent>() => {
                    // SAFETY: length checked >= size_of::<SetgidEvent>(); ring buffer is 8-byte aligned.
                    let ev = unsafe { &*(ptr.cast::<SetgidEvent>()) };
                    counts.setgid += 1;
                    if json_mode {
                        writeln!(
                            stdout,
                            r#"{{"ts":{},"kind":"setgid","tgid":{},"pid":{},"uid":{},"gid":{},"cgroup":{},"comm":"{}","target_gid":{}}}"#,
                            now_wall,
                            header.tgid,
                            header.pid,
                            header.uid,
                            header.gid,
                            header.cgroup_id,
                            comm_esc,
                            ev.target_gid
                        )?;
                    } else {
                        writeln!(
                            stdout,
                            "SGID  tgid={:<6} comm={:<16} gid={} -> {}",
                            header.tgid, comm, header.gid, ev.target_gid
                        )?;
                    }
                }

                // ── Module load ───────────────────────────────
                k if k == EventKind::ModuleLoad as u32 && len >= size_of::<ModuleLoadEvent>() => {
                    // SAFETY: length checked >= size_of::<ModuleLoadEvent>(); ring buffer is 8-byte aligned.
                    let ev = unsafe { &*(ptr.cast::<ModuleLoadEvent>()) };
                    counts.module_load += 1;
                    let name = safe_str(&ev.name, ev.name_len as usize);
                    if json_mode {
                        writeln!(
                            stdout,
                            r#"{{"ts":{},"kind":"module_load","tgid":{},"pid":{},"uid":{},"gid":{},"cgroup":{},"comm":"{}","taints":{},"module":"{}"}}"#,
                            now_wall,
                            header.tgid,
                            header.pid,
                            header.uid,
                            header.gid,
                            header.cgroup_id,
                            comm_esc,
                            ev.taints,
                            json_escape(name)
                        )?;
                    } else {
                        writeln!(
                            stdout,
                            "KMOD  tgid={:<6} comm={:<16} module={} taints={}",
                            header.tgid, comm, name, ev.taints
                        )?;
                    }
                }

                // ── Ptrace ────────────────────────────────────
                k if k == EventKind::Ptrace as u32 && len >= size_of::<PtraceEvent>() => {
                    // SAFETY: length checked >= size_of::<PtraceEvent>(); ring buffer is 8-byte aligned.
                    let ev = unsafe { &*(ptr.cast::<PtraceEvent>()) };
                    counts.ptrace += 1;
                    let req_name = ptrace_request_name(ev.request);
                    if json_mode {
                        writeln!(
                            stdout,
                            r#"{{"ts":{},"kind":"ptrace","tgid":{},"pid":{},"uid":{},"gid":{},"cgroup":{},"comm":"{}","request":{},"request_name":"{}","target_pid":{}}}"#,
                            now_wall,
                            header.tgid,
                            header.pid,
                            header.uid,
                            header.gid,
                            header.cgroup_id,
                            comm_esc,
                            ev.request,
                            req_name,
                            ev.target_pid
                        )?;
                    } else {
                        writeln!(
                            stdout,
                            "PTRC  tgid={:<6} comm={:<16} {}({}) -> pid {}",
                            header.tgid, comm, req_name, ev.request, ev.target_pid
                        )?;
                    }
                }

                // ── memfd_create ──────────────────────────────
                k if k == EventKind::MemfdCreate as u32 && len >= size_of::<MemfdCreateEvent>() => {
                    // SAFETY: length checked >= size_of::<MemfdCreateEvent>(); ring buffer is 8-byte aligned.
                    let ev = unsafe { &*(ptr.cast::<MemfdCreateEvent>()) };
                    counts.memfd_create += 1;
                    let name = safe_str(&ev.name, ev.name_len as usize);
                    if json_mode {
                        writeln!(
                            stdout,
                            r#"{{"ts":{},"kind":"memfd_create","tgid":{},"pid":{},"uid":{},"gid":{},"cgroup":{},"comm":"{}","flags":{},"name":"{}"}}"#,
                            now_wall,
                            header.tgid,
                            header.pid,
                            header.uid,
                            header.gid,
                            header.cgroup_id,
                            comm_esc,
                            ev.flags,
                            json_escape(name)
                        )?;
                    } else {
                        writeln!(
                            stdout,
                            "MEMFD tgid={:<6} comm={:<16} flags={:#x} name={}",
                            header.tgid, comm, ev.flags, name
                        )?;
                    }
                }

                // ── TCP connect ───────────────────────────────
                k if k == EventKind::TcpConnect as u32 && len >= size_of::<TcpConnectEvent>() => {
                    // SAFETY: length checked >= size_of::<TcpConnectEvent>(); ring buffer is 8-byte aligned.
                    let ev = unsafe { &*(ptr.cast::<TcpConnectEvent>()) };
                    counts.tcp_connect += 1;
                    let src = Ipv4Addr::from(ev.saddr.to_be());
                    let dst = Ipv4Addr::from(ev.daddr.to_be());
                    if json_mode {
                        writeln!(
                            stdout,
                            r#"{{"ts":{},"kind":"tcp_connect","tgid":{},"comm":"{}","src":"{}:{}","dst":"{}:{}"}}"#,
                            now_wall, header.tgid, comm_esc, src, ev.sport, dst, ev.dport
                        )?;
                    } else {
                        writeln!(
                            stdout,
                            "CONN  tgid={:<6} comm={:<16} {}:{} -> {}:{}",
                            header.tgid, comm, src, ev.sport, dst, ev.dport
                        )?;
                    }
                }

                // ── TCP accept ────────────────────────────────
                k if k == EventKind::TcpAccept as u32 && len >= size_of::<TcpAcceptEvent>() => {
                    // SAFETY: length checked >= size_of::<TcpAcceptEvent>(); ring buffer is 8-byte aligned.
                    let ev = unsafe { &*(ptr.cast::<TcpAcceptEvent>()) };
                    counts.tcp_accept += 1;
                    let src = Ipv4Addr::from(ev.saddr.to_be());
                    let dst = Ipv4Addr::from(ev.daddr.to_be());
                    if json_mode {
                        writeln!(
                            stdout,
                            r#"{{"ts":{},"kind":"tcp_accept","tgid":{},"comm":"{}","src":"{}:{}","dst":"{}:{}"}}"#,
                            now_wall, header.tgid, comm_esc, src, ev.sport, dst, ev.dport
                        )?;
                    } else {
                        writeln!(
                            stdout,
                            "ACPT  tgid={:<6} comm={:<16} {}:{} <- {}:{}",
                            header.tgid, comm, src, ev.sport, dst, ev.dport
                        )?;
                    }
                }

                _ => {
                    counts.malformed += 1;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    eprintln!("\n=== Sensor Summary ({duration_secs}s) ===");
    eprintln!("  exec:          {}", counts.exec);
    eprintln!("  exit:          {}", counts.exit);
    eprintln!("  file_open:     {}", counts.file_open);
    eprintln!("  file_delete:   {}", counts.file_delete);
    eprintln!("  file_rename:   {}", counts.file_rename);
    eprintln!("  tcp_connect:   {}", counts.tcp_connect);
    eprintln!("  tcp_accept:    {}", counts.tcp_accept);
    eprintln!("  setuid:        {}", counts.setuid);
    eprintln!("  setgid:        {}", counts.setgid);
    eprintln!("  module_load:   {}", counts.module_load);
    eprintln!("  ptrace:        {}", counts.ptrace);
    eprintln!("  memfd_create:  {}", counts.memfd_create);
    eprintln!("  self_filtered: {}", counts.self_filtered);
    eprintln!("  malformed:     {}", counts.malformed);
    eprintln!(
        "  total:         {}/s",
        (counts.total() as f64 / duration_secs as f64) as u64
    );

    Ok(())
}

fn wall_clock_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn comm_str(comm: &[u8; COMM_SIZE]) -> &str {
    let end = comm.iter().position(|&b| b == 0).unwrap_or(COMM_SIZE);
    std::str::from_utf8(&comm[..end]).unwrap_or("<invalid>")
}

fn safe_str(buf: &[u8], len: usize) -> &str {
    let end = len.min(buf.len());
    let slice = &buf[..end];
    let nul = slice.iter().position(|&b| b == 0).unwrap_or(end);
    std::str::from_utf8(&slice[..nul]).unwrap_or("<invalid>")
}

/// Escape a string for safe interpolation into JSON string values.
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str(r#"\""#),
            '\\' => out.push_str(r"\\"),
            '\n' => out.push_str(r"\n"),
            '\r' => out.push_str(r"\r"),
            '\t' => out.push_str(r"\t"),
            c if c.is_control() => {
                use std::fmt::Write;
                let _ = write!(out, r"\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

fn size_of<T>() -> usize {
    core::mem::size_of::<T>()
}

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
        31 => "SETOPTIONS",
        16896 => "SEIZE",
        16897 => "INTERRUPT",
        _ => "UNKNOWN",
    }
}

#[derive(Default)]
struct EventCounts {
    exec: u64,
    exit: u64,
    file_open: u64,
    file_delete: u64,
    file_rename: u64,
    tcp_connect: u64,
    tcp_accept: u64,
    setuid: u64,
    setgid: u64,
    module_load: u64,
    ptrace: u64,
    memfd_create: u64,
    self_filtered: u64,
    malformed: u64,
}

impl EventCounts {
    fn total(&self) -> u64 {
        self.exec
            + self.exit
            + self.file_open
            + self.file_delete
            + self.file_rename
            + self.tcp_connect
            + self.tcp_accept
            + self.setuid
            + self.setgid
            + self.module_load
            + self.ptrace
            + self.memfd_create
    }
}

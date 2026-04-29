//! Userspace loader for the ffwd eBPF EDR sensor (standalone prototype).
//!
//! Loads eBPF programs, attaches to tracepoints/kprobes, and consumes events
//! from a shared ring buffer. Currently prints human-readable or JSON text to
//! stdout for debugging.
//!
//! **Integration path:** When wired into the ffwd pipeline, the ring buffer
//! consumer should build Arrow `RecordBatch` directly from the `repr(C)` event
//! structs — the same pattern `host_metrics.rs` uses. The JSON output here
//! is a prototype convenience and must NOT be routed through a JSON parser to
//! produce Arrow columns.
//!
//! Usage:
//!   sudo ./sensor-ebpf `<ebpf-binary-path>` \[--json\] \[--duration `<seconds>`\] \[--no-file-open\]

// Ring buffer events are 8-byte aligned by the kernel, so casting from *const u8
// to a repr(C) struct pointer is safe despite clippy's alignment warning.
#![allow(clippy::cast_ptr_alignment)]
#![allow(clippy::print_stdout, clippy::print_stderr)]

use aya::Ebpf;
use aya::maps::RingBuf;
use aya::programs::{KProbe, TracePoint};
use sensor_ebpf_common::dns::dns_wire_to_dotted;
use sensor_ebpf_common::*;
use std::io::Write;
use std::net::Ipv4Addr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Newtype wrapper for `EbpfConfig` to satisfy aya's `Pod` trait (orphan rule).
#[repr(transparent)]
#[derive(Clone, Copy)]
struct PodEbpfConfig(EbpfConfig);

// SAFETY: EbpfConfig is repr(C), Copy, and contains only primitive types (u32).
unsafe impl aya::Pod for PodEbpfConfig {}

/// Runs the Linux eBPF sensor loader and streams events until the configured
/// duration elapses or the process is interrupted.
pub(crate) fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let json_mode = args.iter().any(|a| a == "--json");
    let no_file_open = args.iter().any(|a| a == "--no-file-open");

    // First positional (non-flag) argument is the eBPF binary path.
    // Skip values that follow known flags (e.g., --duration 30).
    let flags_with_values: &[&str] = &["--duration"];
    let ebpf_path = {
        let mut skip_next = false;
        let mut found: Option<&str> = None;
        for arg in args.iter().skip(1) {
            if skip_next {
                skip_next = false;
                continue;
            }
            if flags_with_values.contains(&arg.as_str()) {
                skip_next = true;
                continue;
            }
            if arg.starts_with("--") {
                continue;
            }
            found = Some(arg.as_str());
            break;
        }
        found.unwrap_or("sensor-ebpf-kern/target/bpfel-unknown-none/release/sensor-ebpf")
    };
    let duration_secs: u64 = match args.iter().position(|a| a == "--duration") {
        Some(i) => {
            let Some(s) = args.get(i + 1) else {
                eprintln!("error: --duration requires a positive integer");
                std::process::exit(1);
            };
            s.parse().unwrap_or_else(|_| {
                eprintln!("error: --duration requires a positive integer");
                std::process::exit(1);
            })
        }
        None => 10,
    };

    if duration_secs == 0 {
        eprintln!("error: --duration must be > 0");
        std::process::exit(1);
    }

    let self_tgid = std::process::id();

    eprintln!("Loading eBPF from {ebpf_path}...");
    let ebpf_bytes = std::fs::read(ebpf_path)?;
    let mut ebpf = Ebpf::load(&ebpf_bytes)?;

    // Configure runtime offsets before attaching programs, so startup events
    // have the best available semantics.
    match configure_runtime_config(&mut ebpf) {
        Ok(config) => {
            if let Some(offset) = config.exit_code_offset {
                eprintln!("  configured exit_code offset: {offset}");
            } else {
                eprintln!("  WARNING: exit_code offset unavailable; using sentinel mode");
            }
            if let Some(offset) = config.group_dead_offset {
                eprintln!("  configured sched_process_exit.group_dead offset: {offset}");
            } else {
                eprintln!(
                    "  WARNING: sched_process_exit.group_dead missing; using pid==tgid fallback semantics"
                );
            }
        }
        Err(e) => eprintln!("  runtime config unavailable (degraded mode): {e}"),
    }

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
        ("sys_enter_sendto", "syscalls", "sys_enter_sendto"),
    ];

    for (prog_name, category, tracepoint) in tracepoints {
        if no_file_open && *tracepoint == "sys_enter_openat" {
            eprintln!("  SKIP {prog_name}: --no-file-open set");
            continue;
        }
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

    let events_map = ebpf
        .map_mut("EVENTS")
        .ok_or_else(|| std::io::Error::other("eBPF object is missing EVENTS ring buffer"))?;
    let mut ring = RingBuf::try_from(events_map)?;
    let mut stdout = std::io::stdout().lock();
    let start = Instant::now();
    let deadline = Duration::from_secs(duration_secs);
    let monotonic_to_wall_offset_ns =
        monotonic_now_ns().map(|mono_now_ns| wall_clock_ns().saturating_sub(mono_now_ns));

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

            let event_wall = match monotonic_to_wall_offset_ns {
                Some(offset_ns) => event_wall_clock_ns(header.timestamp_ns, offset_ns),
                None => wall_clock_ns(),
            };
            let comm = comm_str(&header.comm);
            let comm_esc = if json_mode {
                json_escape(comm)
            } else {
                String::new()
            };

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
                            event_wall,
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
                            event_wall,
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
                            event_wall,
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
                            event_wall,
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
                            event_wall,
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
                            event_wall,
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
                            event_wall,
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
                            event_wall,
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
                            event_wall,
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
                            event_wall,
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
                    let src = format_addr(ev.saddr);
                    let dst = format_addr(ev.daddr);
                    if json_mode {
                        writeln!(
                            stdout,
                            r#"{{"ts":{},"kind":"tcp_connect","tgid":{},"comm":"{}","src":"{}:{}","dst":"{}:{}"}}"#,
                            event_wall, header.tgid, comm_esc, src, ev.sport, dst, ev.dport
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
                    let src = format_addr(ev.saddr);
                    let dst = format_addr(ev.daddr);
                    if json_mode {
                        writeln!(
                            stdout,
                            r#"{{"ts":{},"kind":"tcp_accept","tgid":{},"comm":"{}","src":"{}:{}","dst":"{}:{}"}}"#,
                            event_wall, header.tgid, comm_esc, src, ev.sport, dst, ev.dport
                        )?;
                    } else {
                        writeln!(
                            stdout,
                            "ACPT  tgid={:<6} comm={:<16} {}:{} <- {}:{}",
                            header.tgid, comm, src, ev.sport, dst, ev.dport
                        )?;
                    }
                }

                // ── DNS query ─────────────────────────────────
                k if k == EventKind::DnsQuery as u32 && len >= size_of::<DnsQueryEvent>() => {
                    // SAFETY: length checked >= size_of::<DnsQueryEvent>(); ring buffer is 8-byte aligned.
                    let ev = unsafe { &*(ptr.cast::<DnsQueryEvent>()) };
                    counts.dns_query += 1;
                    let wire_len = (ev.qname_len as usize).min(MAX_DNS_NAME);
                    let wire = ev.qname.get(..wire_len).unwrap_or_default();
                    let (qname_opt, qtype_opt) = dns_wire_to_dotted(wire);
                    let qname = qname_opt.as_deref().unwrap_or("<undecoded>");
                    let qtype_str = match qtype_opt {
                        Some(qt) => format!("{qt}"),
                        None => "?".to_string(),
                    };
                    let dst = format_addr(ev.dst_addr);
                    if json_mode {
                        // Emit null JSON values for undecoded fields.
                        let qname_json = match qname_opt.as_deref() {
                            Some(s) => format!("\"{}\"", json_escape(s)),
                            None => "null".to_string(),
                        };
                        let qtype_json = match qtype_opt {
                            Some(qt) => format!("{qt}"),
                            None => "null".to_string(),
                        };
                        writeln!(
                            stdout,
                            r#"{{"ts":{},"kind":"dns_query","tgid":{},"pid":{},"uid":{},"gid":{},"cgroup":{},"comm":"{}","qname":{},"qtype":{},"tx_id":{},"dst":"{}:{}"}}"#,
                            event_wall,
                            header.tgid,
                            header.pid,
                            header.uid,
                            header.gid,
                            header.cgroup_id,
                            comm_esc,
                            qname_json,
                            qtype_json,
                            ev.tx_id,
                            dst,
                            ev.dst_port
                        )?;
                    } else {
                        writeln!(
                            stdout,
                            "DNS   tgid={:<6} comm={:<16} {} type={} txid={:#06x} -> {}:{}",
                            header.tgid, comm, qname, qtype_str, ev.tx_id, dst, ev.dst_port
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
    eprintln!("  dns_query:     {}", counts.dns_query);
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

fn event_wall_clock_ns(mono_event_ns: u64, mono_to_wall_offset_ns: u64) -> u64 {
    mono_event_ns.saturating_add(mono_to_wall_offset_ns)
}

fn monotonic_now_ns() -> Option<u64> {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid, writable pointer to a stack-allocated timespec.
    let ret = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &raw mut ts) };
    if ret == 0 {
        let secs = u64::try_from(ts.tv_sec).ok()?;
        let nanos = u64::try_from(ts.tv_nsec).ok()?;
        Some(secs.saturating_mul(1_000_000_000).saturating_add(nanos))
    } else {
        None
    }
}

fn format_addr(addr: u32) -> Ipv4Addr {
    Ipv4Addr::from(addr.to_ne_bytes())
}

fn comm_str(comm: &[u8; COMM_SIZE]) -> &str {
    let end = comm.iter().position(|&b| b == 0).unwrap_or(COMM_SIZE);
    std::str::from_utf8(comm.get(..end).unwrap_or_default()).unwrap_or("<invalid>")
}

fn safe_str(buf: &[u8], len: usize) -> &str {
    let end = len.min(buf.len());
    let slice = buf.get(..end).unwrap_or_default();
    let nul = slice.iter().position(|&b| b == 0).unwrap_or(end);
    std::str::from_utf8(slice.get(..nul).unwrap_or_default()).unwrap_or("<invalid>")
}

// dns_wire_to_dotted is provided by sensor_ebpf_common::dns::dns_wire_to_dotted

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
        0x4200 => "SETOPTIONS",
        0x4206 => "SEIZE",
        0x4207 => "INTERRUPT",
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
    dns_query: u64,
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
            + self.dns_query
    }
}

use sensor_ebpf_common::utils::*;

/// Discover runtime offsets and write them to the CONFIG map.
fn configure_runtime_config(ebpf: &mut Ebpf) -> Result<RuntimeConfig, Box<dyn std::error::Error>> {
    let exit_code_offset = find_exit_code_offset().ok();
    let group_dead_offset = find_sched_process_exit_group_dead_offset().ok().flatten();

    let config_map = ebpf
        .map_mut("CONFIG")
        .ok_or("CONFIG map not found in eBPF binary")?;
    let mut array: aya::maps::Array<_, PodEbpfConfig> =
        config_map.try_into().map_err(|e| format!("{e}"))?;

    let cfg = PodEbpfConfig(EbpfConfig {
        task_exit_code_offset: exit_code_offset.unwrap_or(0),
        sched_process_exit_group_dead_offset: group_dead_offset.unwrap_or(0),
        sched_process_exit_has_group_dead: u32::from(group_dead_offset.is_some()),
        pad: 0,
    });
    array.set(0, cfg, 0).map_err(|e| format!("{e}"))?;

    Ok(RuntimeConfig {
        exit_code_offset,
        group_dead_offset,
    })
}

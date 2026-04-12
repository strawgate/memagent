//! Userspace loader for the logfwd eBPF EDR sensor.
//!
//! Loads eBPF programs, attaches to tracepoints, and consumes events from
//! a shared ring buffer. Prints structured JSON to stdout for integration
//! testing and pipeline validation.
//!
//! Usage:
//!   sudo ./sensor-ebpf <ebpf-binary-path> [--json] [--duration <seconds>]

use aya::maps::RingBuf;
use aya::programs::{KProbe, TracePoint};
use aya::Ebpf;
use sensor_ebpf_common::*;
use std::io::Write;
use std::net::Ipv4Addr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let ebpf_path = args.get(1).map(String::as_str).unwrap_or(
        "sensor-ebpf-kern/target/bpfel-unknown-none/release/sensor-ebpf",
    );
    let json_mode = args.iter().any(|a| a == "--json");
    let no_file_open = args.iter().any(|a| a == "--no-file-open");
    let duration_secs: u64 = args
        .iter()
        .position(|a| a == "--duration")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    // Self-PID for filtering out our own events.
    let self_tgid = std::process::id();

    eprintln!("Loading eBPF from {ebpf_path}...");
    let ebpf_bytes = std::fs::read(ebpf_path)?;
    let mut ebpf = Ebpf::load(&ebpf_bytes)?;

    // Attach tracepoints.
    let tracepoints: &[(&str, &str, &str)] = &[
        ("sched_process_exec", "sched", "sched_process_exec"),
        ("sched_process_exit", "sched", "sched_process_exit"),
        ("sys_enter_openat", "syscalls", "sys_enter_openat"),
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
    let kprobes: &[(&str, &str)] = &[
        ("tcp_v4_connect", "tcp_v4_connect"),
    ];

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

            let header = unsafe { &*(ptr as *const EventHeader) };

            // Skip events from our own process tree.
            if header.tgid == self_tgid {
                counts.self_filtered += 1;
                continue;
            }

            let now_wall = wall_clock_ns();

            match header.kind {
                k if k == EventKind::ProcessExec as u32 && len >= size_of::<ProcessExecEvent>() => {
                    let ev = unsafe { &*(ptr as *const ProcessExecEvent) };
                    counts.exec += 1;
                    let fname = safe_str(&ev.filename, ev.filename_len as usize);
                    let comm = comm_str(&header.comm);
                    if json_mode {
                        writeln!(stdout,
                            r#"{{"ts":{},"kind":"exec","tgid":{},"pid":{},"uid":{},"gid":{},"cgroup":{},"comm":"{}","filename":"{}"}}"#,
                            now_wall, header.tgid, header.pid, header.uid, header.gid, header.cgroup_id, comm, fname
                        )?;
                    } else {
                        writeln!(stdout,
                            "EXEC  tgid={:<6} comm={:<16} exe={}",
                            header.tgid, comm, fname
                        )?;
                    }
                }
                k if k == EventKind::ProcessExit as u32 && len >= size_of::<ProcessExitEvent>() => {
                    let ev = unsafe { &*(ptr as *const ProcessExitEvent) };
                    counts.exit += 1;
                    let comm = comm_str(&header.comm);
                    if json_mode {
                        writeln!(stdout,
                            r#"{{"ts":{},"kind":"exit","tgid":{},"pid":{},"uid":{},"gid":{},"cgroup":{},"comm":"{}","exit_code":{}}}"#,
                            now_wall, header.tgid, header.pid, header.uid, header.gid, header.cgroup_id, comm, ev.exit_code
                        )?;
                    } else {
                        writeln!(stdout,
                            "EXIT  tgid={:<6} comm={:<16} code={}",
                            header.tgid, comm, ev.exit_code
                        )?;
                    }
                }
                k if k == EventKind::FileOpen as u32 && len >= size_of::<FileOpenEvent>() => {
                    if no_file_open {
                        counts.file_open += 1;
                        continue;
                    }
                    let ev = unsafe { &*(ptr as *const FileOpenEvent) };
                    counts.file_open += 1;
                    let fname = safe_str(&ev.filename, ev.filename_len as usize);
                    let comm = comm_str(&header.comm);
                    if json_mode {
                        writeln!(stdout,
                            r#"{{"ts":{},"kind":"file_open","tgid":{},"pid":{},"uid":{},"gid":{},"cgroup":{},"comm":"{}","flags":{},"filename":"{}"}}"#,
                            now_wall, header.tgid, header.pid, header.uid, header.gid, header.cgroup_id, comm, ev.flags, fname
                        )?;
                    } else {
                        writeln!(stdout,
                            "OPEN  tgid={:<6} comm={:<16} flags={:#06x} file={}",
                            header.tgid, comm, ev.flags, fname
                        )?;
                    }
                }
                k if k == EventKind::TcpConnect as u32 && len >= size_of::<TcpConnectEvent>() => {
                    let ev = unsafe { &*(ptr as *const TcpConnectEvent) };
                    counts.tcp_connect += 1;
                    let comm = comm_str(&header.comm);
                    let src = Ipv4Addr::from(ev.saddr.to_be());
                    let dst = Ipv4Addr::from(ev.daddr.to_be());
                    if json_mode {
                        writeln!(stdout,
                            r#"{{"ts":{},"kind":"tcp_connect","tgid":{},"comm":"{}","src":"{}:{}","dst":"{}:{}"}}"#,
                            now_wall, header.tgid, comm, src, ev.sport, dst, ev.dport
                        )?;
                    } else {
                        writeln!(stdout,
                            "CONN  tgid={:<6} comm={:<16} {}:{} -> {}:{}",
                            header.tgid, comm, src, ev.sport, dst, ev.dport
                        )?;
                    }
                }
                k if k == EventKind::TcpAccept as u32 && len >= size_of::<TcpAcceptEvent>() => {
                    let ev = unsafe { &*(ptr as *const TcpAcceptEvent) };
                    counts.tcp_accept += 1;
                    let comm = comm_str(&header.comm);
                    let src = Ipv4Addr::from(ev.saddr.to_be());
                    let dst = Ipv4Addr::from(ev.daddr.to_be());
                    if json_mode {
                        writeln!(stdout,
                            r#"{{"ts":{},"kind":"tcp_accept","tgid":{},"comm":"{}","src":"{}:{}","dst":"{}:{}"}}"#,
                            now_wall, header.tgid, comm, src, ev.sport, dst, ev.dport
                        )?;
                    } else {
                        writeln!(stdout,
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
    eprintln!("  tcp_connect:   {}", counts.tcp_connect);
    eprintln!("  tcp_accept:    {}", counts.tcp_accept);
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

fn size_of<T>() -> usize {
    core::mem::size_of::<T>()
}

#[derive(Default)]
struct EventCounts {
    exec: u64,
    exit: u64,
    file_open: u64,
    tcp_connect: u64,
    tcp_accept: u64,
    self_filtered: u64,
    malformed: u64,
}

impl EventCounts {
    fn total(&self) -> u64 {
        self.exec + self.exit + self.file_open + self.tcp_connect + self.tcp_accept
    }
}

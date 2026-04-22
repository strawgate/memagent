//! UDP syslog receiver benchmark.
//!
//! Measures throughput of recvmmsg-based batch UDP reception with
//! syslog priority parsing and severity filtering. This is the
//! non-eBPF baseline for comparison with the XDP syslog filter.
//!
//! Usage:
//!   udp-bench receive [--port 5514] [--severity 4] [--batch 64]
//!   udp-bench receive-batch [--port 5514] [--severity 4] [--batch 64]
//!   udp-bench generate [--port 5514] [--count 1000000] [--pps 0]

mod syslog_parser;

use std::env;
use std::io;
use std::net::UdpSocket;
use std::time::{Duration, Instant};

// ---- Syslog priority parser ----

/// Parsed syslog priority metadata.
#[derive(Debug, Clone, Copy)]
struct SyslogMeta {
    facility: u8,
    severity: u8,
    pri_len: u8, // bytes consumed including < and >
}

/// Parse syslog <priority> from the start of a buffer.
/// Returns None if the buffer doesn't start with a valid <NNN> prefix.
///
/// This is the scalar baseline. On a modern CPU this takes ~3-8ns per call
/// (branch-predicted, no memory allocation, no SIMD needed for 3-5 bytes).
#[inline(always)]
fn parse_syslog_pri(buf: &[u8]) -> Option<SyslogMeta> {
    if buf.len() < 3 || buf[0] != b'<' {
        return None;
    }

    let mut pri: u16 = 0;
    let mut i = 1;
    let mut has_digit = false;

    // Parse up to 3 digits — require at least one, reject <> and pri > 191
    while i < buf.len() && i <= 4 {
        let b = buf[i];
        if b == b'>' {
            if !has_digit || pri > 191 {
                return None;
            }
            return Some(SyslogMeta {
                facility: (pri >> 3) as u8,
                severity: (pri & 0x7) as u8,
                pri_len: (i + 1) as u8,
            });
        }
        if !b.is_ascii_digit() {
            return None;
        }
        has_digit = true;
        pri = pri * 10 + (b - b'0') as u16;
        i += 1;
    }

    None
}

// ---- recvmmsg wrapper ----

/// Receive a batch of UDP packets using recvmmsg(2).
/// Returns the number of messages received.
///
/// recvmmsg is the key syscall for high-throughput UDP — it pulls up to
/// `batch_size` packets in a single syscall, amortizing the syscall overhead.
///
/// # Safety
///
/// - `fd` must be a valid, non-blocking socket file descriptor.
/// - `bufs`, `iovecs`, and `msgvec` must all have the same length.
/// - Each `bufs[i]` must have sufficient len/capacity and its buffer pointer
///   must be valid for writes of up to `bufs[i].len()` bytes.
/// - No concurrent mutation of `bufs`, `iovecs`, or `msgvec` may occur while
///   the syscall is in progress.
unsafe fn recvmmsg_batch(
    fd: i32,
    bufs: &mut [Vec<u8>],
    iovecs: &mut [libc::iovec],
    msgvec: &mut [libc::mmsghdr],
) -> io::Result<usize> {
    let batch = bufs.len();

    // Set up scatter-gather for each message
    for i in 0..batch {
        iovecs[i] = libc::iovec {
            iov_base: bufs[i].as_mut_ptr() as *mut _,
            iov_len: bufs[i].len(),
        };
        // SAFETY: zero is a valid initial state for libc::mmsghdr; msg_iov and
        // msg_iovlen are populated below before the syscall observes the header.
        msgvec[i] = unsafe { std::mem::zeroed() };
        msgvec[i].msg_hdr.msg_iov = &mut iovecs[i];
        msgvec[i].msg_hdr.msg_iovlen = 1;
    }

    // Non-blocking recvmmsg with no timeout
    let ret = libc::recvmmsg(
        fd,
        msgvec.as_mut_ptr(),
        batch as u32,
        libc::MSG_DONTWAIT,
        std::ptr::null_mut(),
    );

    if ret < 0 {
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            return Ok(0);
        }
        return Err(err);
    }

    Ok(ret as usize)
}

// ---- Receiver ----

fn run_receiver(port: u16, severity_threshold: u8, batch_size: usize) -> io::Result<()> {
    let sock = UdpSocket::bind(format!("0.0.0.0:{}", port))?;
    sock.set_nonblocking(true)?;
    let fd = {
        use std::os::unix::io::AsRawFd;
        sock.as_raw_fd()
    };

    eprintln!(
        "Listening on UDP port {}, severity <= {}, batch size {}",
        port, severity_threshold, batch_size
    );

    // Pre-allocate buffers
    let mut bufs: Vec<Vec<u8>> = (0..batch_size).map(|_| vec![0u8; 2048]).collect();
    // SAFETY: zero is a valid initial state for libc::iovec; recvmmsg_batch
    // overwrites each entry with a valid buffer pointer and length before use.
    let mut iovecs: Vec<libc::iovec> = vec![unsafe { std::mem::zeroed() }; batch_size];
    // SAFETY: zero is a valid initial state for libc::mmsghdr; recvmmsg_batch
    // overwrites the fields required by recvmmsg before each syscall.
    let mut msgvec: Vec<libc::mmsghdr> = vec![unsafe { std::mem::zeroed() }; batch_size];

    let mut total_packets: u64 = 0;
    let mut total_matched: u64 = 0;
    let mut total_dropped: u64 = 0;
    let mut total_parse_fail: u64 = 0;
    let mut total_bytes: u64 = 0;

    let start = Instant::now();
    let mut last_report = start;
    let mut interval_packets: u64 = 0;
    let mut interval_matched: u64 = 0;

    loop {
        // SAFETY: fd comes from the live UDP socket above, and bufs/iovecs/msgvec
        // were allocated with equal batch_size lengths and are mutated only here.
        let n = unsafe { recvmmsg_batch(fd, &mut bufs, &mut iovecs, &mut msgvec)? };

        if n == 0 {
            // No data — check if we should report stats
            let now = Instant::now();
            if now.duration_since(last_report) >= Duration::from_secs(1) {
                if total_packets > 0 {
                    report_stats(
                        start,
                        &mut last_report,
                        now,
                        interval_packets,
                        interval_matched,
                        total_packets,
                        total_matched,
                        total_dropped,
                        total_parse_fail,
                        total_bytes,
                    );
                    interval_packets = 0;
                    interval_matched = 0;
                }
            }
            // Brief sleep to avoid busy-spinning when idle
            std::thread::sleep(Duration::from_micros(100));
            continue;
        }

        // Process batch
        for i in 0..n {
            let len = msgvec[i].msg_len as usize;
            let buf = &bufs[i][..len];
            total_bytes += len as u64;
            total_packets += 1;
            interval_packets += 1;

            match parse_syslog_pri(buf) {
                Some(meta) => {
                    if meta.severity <= severity_threshold {
                        total_matched += 1;
                        interval_matched += 1;
                        // In a real pipeline, matched packets would be
                        // forwarded to the Arrow scanner here.
                    } else {
                        total_dropped += 1;
                    }
                }
                None => {
                    total_parse_fail += 1;
                }
            }
        }

        // Periodic reporting
        let now = Instant::now();
        if now.duration_since(last_report) >= Duration::from_secs(1) {
            report_stats(
                start,
                &mut last_report,
                now,
                interval_packets,
                interval_matched,
                total_packets,
                total_matched,
                total_dropped,
                total_parse_fail,
                total_bytes,
            );
            interval_packets = 0;
            interval_matched = 0;
        }
    }
}

fn report_stats(
    start: Instant,
    last_report: &mut Instant,
    now: Instant,
    interval_pkts: u64,
    interval_matched: u64,
    total_pkts: u64,
    total_matched: u64,
    total_dropped: u64,
    total_fail: u64,
    total_bytes: u64,
) {
    let interval = now.duration_since(*last_report).as_secs_f64();
    let total_secs = now.duration_since(start).as_secs_f64();
    let pps = interval_pkts as f64 / interval;
    let matched_pps = interval_matched as f64 / interval;
    let avg_pps = total_pkts as f64 / total_secs;
    let mbps = (total_bytes as f64 / total_secs) / (1024.0 * 1024.0);

    eprintln!(
        "[{:.1}s] {:.0} pps ({:.0} matched) | total: {} pkts, {} matched, {} dropped, {} failed | {:.1} MB/s avg {:.0} pps",
        total_secs, pps, matched_pps,
        total_pkts, total_matched, total_dropped, total_fail, mbps, avg_pps,
    );
    *last_report = now;
}

// ---- Generator ----

fn run_generator(port: u16, count: u64, target_pps: u64) -> io::Result<()> {
    let sock = UdpSocket::bind("0.0.0.0:0")?;
    let dest = format!("127.0.0.1:{}", port);

    // Pre-generate messages for all 8 severity levels
    let sev_names = [
        "EMERG", "ALERT", "CRIT", "ERR", "WARN", "NOTICE", "INFO", "DEBUG",
    ];
    let messages: Vec<Vec<u8>> = (0..8).map(|sev| {
        let pri = (16 << 3) | sev; // facility=local0
        format!(
            "<{}>Jan 15 10:30:45 benchhost myapp[1234]: severity={} seq=XXXXXXXX this is a test syslog message with some realistic payload padding",
            pri, sev_names[sev as usize]
        ).into_bytes()
    }).collect();

    eprintln!(
        "Generating {} syslog packets to {} (target: {} pps)",
        count,
        dest,
        if target_pps == 0 {
            "max".to_string()
        } else {
            format!("{}", target_pps)
        }
    );

    let start = Instant::now();
    let mut sent: u64 = 0;
    let mut last_report = start;
    let mut interval_sent: u64 = 0;

    // Rate limiting: if target_pps > 0, pace sends
    let send_interval = if target_pps > 0 {
        Some(Duration::from_nanos(1_000_000_000 / target_pps))
    } else {
        None
    };
    let mut next_send = Instant::now();

    while sent < count {
        if let Some(interval) = send_interval {
            let now = Instant::now();
            if now < next_send {
                std::thread::sleep(next_send - now);
            }
            next_send += interval;
            // Reset if we've fallen behind significantly to avoid burst catch-up
            if next_send + interval < Instant::now() {
                next_send = Instant::now() + interval;
            }
        }

        let sev = (sent % 8) as usize;
        sock.send_to(&messages[sev], &dest)?;
        sent += 1;
        interval_sent += 1;

        let now = Instant::now();
        if now.duration_since(last_report) >= Duration::from_secs(1) {
            let elapsed = now.duration_since(start).as_secs_f64();
            let pps = interval_sent as f64 / now.duration_since(last_report).as_secs_f64();
            eprintln!("[{:.1}s] sent {} ({:.0} pps)", elapsed, sent, pps);
            last_report = now;
            interval_sent = 0;
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let avg_pps = sent as f64 / elapsed;
    eprintln!(
        "Done: {} packets in {:.2}s ({:.0} pps avg)",
        sent, elapsed, avg_pps
    );
    Ok(())
}

// ---- Batch receiver (SIMD parser) ----

fn run_batch_receiver(port: u16, severity_threshold: u8, batch_size: usize) -> io::Result<()> {
    let sock = UdpSocket::bind(format!("0.0.0.0:{}", port))?;
    sock.set_nonblocking(true)?;
    let fd = {
        use std::os::unix::io::AsRawFd;
        sock.as_raw_fd()
    };

    eprintln!(
        "Listening on UDP port {} (BATCH PARSER), severity <= {}, batch size {}",
        port, severity_threshold, batch_size
    );

    // Contiguous buffer for all packets + per-packet length tracking
    let mut bufs: Vec<Vec<u8>> = (0..batch_size).map(|_| vec![0u8; 2048]).collect();
    // SAFETY: zero is a valid initial state for libc::iovec; recvmmsg_batch
    // overwrites each entry with a valid buffer pointer and length before use.
    let mut iovecs: Vec<libc::iovec> = vec![unsafe { std::mem::zeroed() }; batch_size];
    // SAFETY: zero is a valid initial state for libc::mmsghdr; recvmmsg_batch
    // overwrites the fields required by recvmmsg before each syscall.
    let mut msgvec: Vec<libc::mmsghdr> = vec![unsafe { std::mem::zeroed() }; batch_size];

    // Contiguous buffer for batch parsing
    let mut concat_buf: Vec<u8> = Vec::with_capacity(batch_size * 256);
    let mut packet_lens: Vec<usize> = Vec::with_capacity(batch_size);

    let mut total_packets: u64 = 0;
    let mut total_matched: u64 = 0;
    let mut total_parse_fail: u64 = 0;
    let mut total_fields: u64 = 0;
    let mut total_bytes: u64 = 0;

    let start = Instant::now();
    let mut last_report = start;
    let mut interval_packets: u64 = 0;
    let mut interval_matched: u64 = 0;

    loop {
        // SAFETY: fd comes from the live UDP socket above, and bufs/iovecs/msgvec
        // were allocated with equal batch_size lengths and are mutated only here.
        let n = unsafe { recvmmsg_batch(fd, &mut bufs, &mut iovecs, &mut msgvec)? };

        if n == 0 {
            let now = Instant::now();
            if now.duration_since(last_report) >= Duration::from_secs(1) && total_packets > 0 {
                let elapsed = now.duration_since(start).as_secs_f64();
                let pps = interval_packets as f64 / now.duration_since(last_report).as_secs_f64();
                let mps = interval_matched as f64 / now.duration_since(last_report).as_secs_f64();
                let mb = (total_bytes as f64 / elapsed) / (1024.0 * 1024.0);
                eprintln!(
                    "[{:.1}s] {:.0} pps ({:.0} matched) | total: {} pkts, {} matched, {} failed, {} fields | {:.1} MB/s",
                    elapsed, pps, mps, total_packets, total_matched, total_parse_fail, total_fields, mb,
                );
                last_report = now;
                interval_packets = 0;
                interval_matched = 0;
            }
            std::thread::sleep(Duration::from_micros(100));
            continue;
        }

        // Concatenate packets into contiguous buffer for SIMD batch parsing
        concat_buf.clear();
        packet_lens.clear();
        for i in 0..n {
            let len = msgvec[i].msg_len as usize;
            concat_buf.extend_from_slice(&bufs[i][..len]);
            packet_lens.push(len);
            total_bytes += len as u64;
        }

        total_packets += n as u64;
        interval_packets += n as u64;

        // SIMD batch parse: parse all packets in one call
        let batch =
            syslog_parser::parse_recvmmsg_batch(&concat_buf, &packet_lens, severity_threshold);

        let matched = batch.lines.len() as u64;
        total_matched += matched;
        interval_matched += matched;
        total_parse_fail += batch.parse_errors as u64;

        // Count extracted fields to prevent dead-code elimination and
        // simulate downstream work (feeding Arrow columns)
        for line in &batch.lines {
            total_fields += (!line.timestamp.is_empty()) as u64;
            total_fields += (!line.hostname.is_empty()) as u64;
            total_fields += (!line.app_name.is_empty()) as u64;
            total_fields += (!line.pid.is_empty()) as u64;
            total_fields += (!line.message.is_empty()) as u64;
        }

        let now = Instant::now();
        if now.duration_since(last_report) >= Duration::from_secs(1) {
            let elapsed = now.duration_since(start).as_secs_f64();
            let pps = interval_packets as f64 / now.duration_since(last_report).as_secs_f64();
            let mps = interval_matched as f64 / now.duration_since(last_report).as_secs_f64();
            let mb = (total_bytes as f64 / elapsed) / (1024.0 * 1024.0);
            eprintln!(
                "[{:.1}s] {:.0} pps ({:.0} matched) | total: {} pkts, {} matched, {} failed, {} fields | {:.1} MB/s",
                elapsed, pps, mps, total_packets, total_matched, total_parse_fail, total_fields, mb,
            );
            last_report = now;
            interval_packets = 0;
            interval_matched = 0;
        }
    }
}

// ---- CLI ----

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage:");
        eprintln!("  udp-bench receive       [--port 5514] [--severity 4] [--batch 64]  (per-packet scalar)");
        eprintln!("  udp-bench receive-batch  [--port 5514] [--severity 4] [--batch 64]  (SIMD batch parser)");
        eprintln!("  udp-bench generate       [--port 5514] [--count 1000000] [--pps 0]");
        std::process::exit(1);
    }

    let mode = &args[1];
    let get_arg = |name: &str, default: &str| -> String {
        for i in 0..args.len() {
            if args[i] == format!("--{}", name) {
                if i + 1 < args.len() {
                    return args[i + 1].clone();
                } else {
                    eprintln!("ERROR: --{} requires a value", name);
                    std::process::exit(1);
                }
            }
        }
        default.to_string()
    };

    match mode.as_str() {
        "receive" => {
            let port_str = get_arg("port", "5514");
            let port: u16 = port_str.parse().unwrap_or_else(|e| {
                eprintln!("ERROR: invalid port value '{}': {e}", port_str);
                std::process::exit(1);
            });
            let sev_str = get_arg("severity", "4");
            let sev: u8 = sev_str.parse().unwrap_or_else(|e| {
                eprintln!("ERROR: invalid severity value '{}': {e}", sev_str);
                std::process::exit(1);
            });
            let batch_str = get_arg("batch", "64");
            let batch: usize = batch_str.parse().unwrap_or_else(|e| {
                eprintln!("ERROR: invalid batch value '{}': {e}", batch_str);
                std::process::exit(1);
            });
            run_receiver(port, sev, batch)
        }
        "receive-batch" => {
            let port_str = get_arg("port", "5514");
            let port: u16 = port_str.parse().unwrap_or_else(|e| {
                eprintln!("ERROR: invalid port value '{}': {e}", port_str);
                std::process::exit(1);
            });
            let sev_str = get_arg("severity", "4");
            let sev: u8 = sev_str.parse().unwrap_or_else(|e| {
                eprintln!("ERROR: invalid severity value '{}': {e}", sev_str);
                std::process::exit(1);
            });
            let batch_str = get_arg("batch", "64");
            let batch: usize = batch_str.parse().unwrap_or_else(|e| {
                eprintln!("ERROR: invalid batch value '{}': {e}", batch_str);
                std::process::exit(1);
            });
            run_batch_receiver(port, sev, batch)
        }
        "generate" => {
            let port_str = get_arg("port", "5514");
            let port: u16 = port_str.parse().unwrap_or_else(|e| {
                eprintln!("ERROR: invalid port value '{}': {e}", port_str);
                std::process::exit(1);
            });
            let count_str = get_arg("count", "1000000");
            let count: u64 = count_str.parse().unwrap_or_else(|e| {
                eprintln!("ERROR: invalid count value '{}': {e}", count_str);
                std::process::exit(1);
            });
            let pps_str = get_arg("pps", "0");
            let pps: u64 = pps_str.parse().unwrap_or_else(|e| {
                eprintln!("ERROR: invalid pps value '{}': {e}", pps_str);
                std::process::exit(1);
            });
            run_generator(port, count, pps)
        }
        _ => {
            eprintln!(
                "Unknown mode: {}. Use 'receive', 'receive-batch', or 'generate'.",
                mode
            );
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_priority_basic() {
        let m = parse_syslog_pri(b"<134>test").unwrap();
        assert_eq!(m.facility, 16); // 134 >> 3 = 16
        assert_eq!(m.severity, 6); // 134 & 7 = 6
        assert_eq!(m.pri_len, 5); // "<134>"
    }

    #[test]
    fn parse_priority_single_digit() {
        let m = parse_syslog_pri(b"<1>test").unwrap();
        assert_eq!(m.facility, 0);
        assert_eq!(m.severity, 1);
        assert_eq!(m.pri_len, 3);
    }

    #[test]
    fn parse_priority_zero_accepted() {
        // <0> = kern.emerg, valid per RFC 5424
        let m = parse_syslog_pri(b"<0>test").unwrap();
        assert_eq!(m.facility, 0);
        assert_eq!(m.severity, 0);
    }

    #[test]
    fn parse_priority_empty_rejected() {
        assert!(parse_syslog_pri(b"<>test").is_none()); // empty PRI
    }

    #[test]
    fn parse_priority_over_191() {
        assert!(parse_syslog_pri(b"<192>test").is_none()); // > 191
        assert!(parse_syslog_pri(b"<999>test").is_none());
    }

    #[test]
    fn parse_priority_two_digit() {
        let m = parse_syslog_pri(b"<13>test").unwrap();
        assert_eq!(m.facility, 1);
        assert_eq!(m.severity, 5);
        assert_eq!(m.pri_len, 4);
    }

    #[test]
    fn parse_priority_max() {
        let m = parse_syslog_pri(b"<191>test").unwrap();
        assert_eq!(m.facility, 23);
        assert_eq!(m.severity, 7);
    }

    #[test]
    fn parse_priority_invalid() {
        assert!(parse_syslog_pri(b"no angle bracket").is_none());
        assert!(parse_syslog_pri(b"<abc>test").is_none());
        assert!(parse_syslog_pri(b"<1234>test").is_none()); // 4+ digits
        assert!(parse_syslog_pri(b"<13").is_none()); // no closing >
        assert!(parse_syslog_pri(b"").is_none());
    }

    #[test]
    fn parse_priority_throughput() {
        let buf = b"<134>Jan 15 10:30:45 host app[1234]: test message";
        let start = std::time::Instant::now();
        let n = 10_000_000;
        for _ in 0..n {
            let m = parse_syslog_pri(std::hint::black_box(buf));
            std::hint::black_box(m);
        }
        let elapsed = start.elapsed();
        let ns_per = elapsed.as_nanos() as f64 / n as f64;
        eprintln!(
            "parse_syslog_pri: {:.1}ns/call ({:.1}M/sec)",
            ns_per,
            1000.0 / ns_per
        );
    }
}

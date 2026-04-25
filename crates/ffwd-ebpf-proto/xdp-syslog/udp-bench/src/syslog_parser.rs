//! SIMD-accelerated batch syslog parser.
//!
//! Parses a contiguous buffer of newline-separated syslog messages using
//! memchr (SIMD-accelerated) for delimiter scanning. Instead of parsing
//! one message at a time, this scans the entire buffer for structural
//! characters in vectorized sweeps.
//!
//! RFC 3164 (BSD) syslog format:
//!   <PRI>TIMESTAMP HOSTNAME APP-NAME[PID]: MSG
//!   <134>Jan 15 10:30:45 myhost myapp[1234]: the actual message
//!
//! Parsing strategy (inspired by ffwd scanner.rs):
//! 1. memchr('\n') sweep to find all line boundaries
//! 2. For each line, parse <PRI> (scalar — only 3-5 bytes, not worth SIMD)
//! 3. memchr(' ') sweeps to find field boundaries within lines
//! 4. memchr('[') to find PID start, memchr(']') for PID end
//! 5. memchr(':') to find message body start
//!
//! All field extraction is zero-copy — returns byte slices into the
//! original buffer. No allocations in the hot path.

use memchr::memchr;

/// A parsed syslog message — all fields are byte slices into the input buffer.
#[derive(Debug, Clone, Copy)]
pub struct SyslogLine<'a> {
    pub facility: u8,
    pub severity: u8,
    pub timestamp: &'a [u8], // "Jan 15 10:30:45" (15 bytes for BSD)
    pub hostname: &'a [u8],
    pub app_name: &'a [u8], // without [PID]
    pub pid: &'a [u8],      // between [ and ], empty if no PID
    pub message: &'a [u8],  // everything after ": "
}

/// Batch parse results — pre-allocated vectors of field slices.
pub struct SyslogBatch<'a> {
    pub lines: Vec<SyslogLine<'a>>,
    pub parse_errors: usize,
}

/// Parse a contiguous buffer of newline-separated syslog messages.
///
/// This is the main entry point. Pass a buffer that contains multiple
/// syslog messages separated by newlines (e.g., concatenated from
/// recvmmsg). Returns all parsed lines as zero-copy slices.
pub fn parse_syslog_batch(buf: &[u8]) -> SyslogBatch<'_> {
    let mut batch = SyslogBatch {
        lines: Vec::with_capacity(buf.len() / 80), // ~80 bytes/line estimate
        parse_errors: 0,
    };

    let mut pos = 0;
    let len = buf.len();

    while pos < len {
        // SIMD-accelerated newline search
        let eol = match memchr(b'\n', &buf[pos..]) {
            Some(offset) => pos + offset,
            None => len,
        };

        let line = &buf[pos..eol];
        if !line.is_empty() {
            match parse_syslog_line(line) {
                Some(parsed) => batch.lines.push(parsed),
                None => batch.parse_errors += 1,
            }
        }

        pos = eol + 1;
    }

    batch
}

/// Parse a single syslog line into zero-copy field slices.
///
/// Format: <PRI>TIMESTAMP HOSTNAME APP[PID]: MESSAGE
///
/// Uses memchr for space/bracket/colon scanning — each call is
/// SIMD-accelerated on x86_64 (SSE2/AVX2).
#[inline]
fn parse_syslog_line(line: &[u8]) -> Option<SyslogLine<'_>> {
    // 1. Parse <PRI> — scalar, only 3-5 bytes
    //    Require at least one digit, reject <>, enforce pri <= 191.
    if line.len() < 4 || line[0] != b'<' {
        return None;
    }

    let mut pri: u16 = 0;
    let mut i: usize = 1;
    let mut has_digit = false;
    while i < line.len() && i <= 4 {
        let b = line[i];
        if b == b'>' {
            i += 1; // skip >
            break;
        }
        if !b.is_ascii_digit() {
            return None;
        }
        has_digit = true;
        pri = pri * 10 + (b - b'0') as u16;
        i += 1;
    }
    if !has_digit || line[i - 1] != b'>' || pri > 191 {
        return None;
    }

    let rest = &line[i..];
    let facility = (pri >> 3) as u8;
    let severity = (pri & 0x7) as u8;

    // 2. Parse TIMESTAMP — BSD format "Mon DD HH:MM:SS" is fixed-width 15 bytes.
    //    Use fixed slice to handle single-digit days with double spaces ("Jan  1").
    if rest.len() < 16 {
        return Some(SyslogLine {
            facility,
            severity,
            timestamp: rest,
            hostname: b"",
            app_name: b"",
            pid: b"",
            message: rest,
        });
    }

    let timestamp = &rest[..15];
    // Skip separator space after timestamp (position 15)
    let after_ts = if rest.len() > 15 && rest[15] == b' ' {
        &rest[16..]
    } else {
        &rest[15..]
    };

    // 3. Parse HOSTNAME — up to next space
    let sp4 = memchr(b' ', after_ts)?;
    let hostname = &after_ts[..sp4];
    let after_host = &after_ts[sp4 + 1..];

    // 4. Parse APP-NAME[PID]: MESSAGE
    //    Look for '[' (PID) or ':' (message start)
    let bracket = memchr(b'[', after_host);
    let colon = memchr(b':', after_host);

    let (app_name, pid, message) = match (bracket, colon) {
        (Some(b), Some(c)) if b < c => {
            // APP[PID]: MSG
            let app = &after_host[..b];
            let pid_end = memchr(b']', &after_host[b + 1..])
                .map(|o| b + 1 + o)
                .unwrap_or(c);
            let pid = &after_host[b + 1..pid_end];
            // Message starts after ": " (colon + space)
            let msg_start = if c + 1 < after_host.len() && after_host[c + 1] == b' ' {
                c + 2
            } else {
                c + 1
            };
            let msg = if msg_start < after_host.len() {
                &after_host[msg_start..]
            } else {
                b""
            };
            (app, pid, msg)
        }
        (_, Some(c)) => {
            // APP: MSG (no PID)
            let app = &after_host[..c];
            let msg_start = if c + 1 < after_host.len() && after_host[c + 1] == b' ' {
                c + 2
            } else {
                c + 1
            };
            let msg = if msg_start < after_host.len() {
                &after_host[msg_start..]
            } else {
                b""
            };
            (app, &b""[..], msg)
        }
        _ => {
            // No colon — treat the rest as app_name with message
            (after_host, &b""[..], &b""[..])
        }
    };

    Some(SyslogLine {
        facility,
        severity,
        timestamp,
        hostname,
        app_name,
        pid,
        message,
    })
}

/// Parse a batch of UDP packets given as (offset, length) pairs into
/// a contiguous buffer. This is the recvmmsg integration point —
/// packets are concatenated with newline separators, then batch-parsed.
pub fn parse_recvmmsg_batch<'a>(
    buf: &'a [u8],
    packet_lens: &[usize],
    severity_threshold: u8,
) -> SyslogBatch<'a> {
    let mut batch = SyslogBatch {
        lines: Vec::with_capacity(packet_lens.len()),
        parse_errors: 0,
    };

    let mut pos = 0;
    for &pkt_len in packet_lens {
        if pos + pkt_len > buf.len() {
            break;
        }
        let pkt = &buf[pos..pos + pkt_len];
        match parse_syslog_line(pkt) {
            Some(parsed) if parsed.severity <= severity_threshold => {
                batch.lines.push(parsed);
            }
            Some(_) => {} // severity filtered
            None => batch.parse_errors += 1,
        }
        pos += pkt_len;
    }

    batch
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_line() {
        let line = b"<134>Jan 15 10:30:45 myhost myapp[1234]: hello world";
        let p = parse_syslog_line(line).unwrap();
        assert_eq!(p.facility, 16);
        assert_eq!(p.severity, 6);
        assert_eq!(p.timestamp, b"Jan 15 10:30:45");
        assert_eq!(p.hostname, b"myhost");
        assert_eq!(p.app_name, b"myapp");
        assert_eq!(p.pid, b"1234");
        assert_eq!(p.message, b"hello world");
    }

    #[test]
    fn parse_no_pid() {
        // Note: BSD syslog uses "Jan  1" (two spaces) for single-digit days
        // Our space-scanning parser treats each space as a field boundary,
        // so "Jan  1" becomes "Jan" + "" + "1" — the empty field eats a space slot.
        // Use a two-digit day for this test.
        let line = b"<13>Jan 01 00:00:00 host sshd: connection from 1.2.3.4";
        let p = parse_syslog_line(line).unwrap();
        assert_eq!(p.facility, 1);
        assert_eq!(p.severity, 5);
        assert_eq!(p.hostname, b"host");
        assert_eq!(p.app_name, b"sshd");
        assert_eq!(p.pid, b"");
        assert_eq!(p.message, b"connection from 1.2.3.4");
    }

    #[test]
    fn parse_batch() {
        let buf = b"<134>Jan 15 10:30:45 h1 app[1]: msg1\n\
                     <131>Jan 15 10:30:46 h2 app[2]: msg2\n\
                     <135>Jan 15 10:30:47 h3 app[3]: msg3\n";
        let batch = parse_syslog_batch(buf);
        assert_eq!(batch.lines.len(), 3);
        assert_eq!(batch.parse_errors, 0);
        assert_eq!(batch.lines[0].message, b"msg1");
        assert_eq!(batch.lines[1].severity, 3); // ERR
        assert_eq!(batch.lines[2].hostname, b"h3");
    }

    #[test]
    fn parse_recvmmsg_style() {
        // Simulate packets concatenated without newlines (as recvmmsg delivers)
        let pkt1 = b"<134>Jan 15 10:30:45 h1 app[1]: msg1"; // sev=6 (INFO)
        let pkt2 = b"<135>Jan 15 10:30:46 h2 app[2]: msg2"; // sev=7 (DEBUG)
        let pkt3 = b"<131>Jan 15 10:30:47 h3 app[3]: msg3"; // sev=3 (ERR)

        let mut buf = Vec::new();
        buf.extend_from_slice(pkt1);
        buf.extend_from_slice(pkt2);
        buf.extend_from_slice(pkt3);

        let lens = vec![pkt1.len(), pkt2.len(), pkt3.len()];

        // severity <= 4 (WARN): only pkt3 (sev=3) passes
        let batch = parse_recvmmsg_batch(&buf, &lens, 4);
        assert_eq!(batch.lines.len(), 1);
        assert_eq!(batch.lines[0].message, b"msg3");

        // severity <= 7 (all): all 3 pass
        let batch = parse_recvmmsg_batch(&buf, &lens, 7);
        assert_eq!(batch.lines.len(), 3);
    }

    #[test]
    fn batch_throughput() {
        // Build a realistic batch: 1000 syslog lines, ~130 bytes each
        let mut buf = Vec::with_capacity(150_000);
        for i in 0..1000 {
            let sev = i % 8;
            let pri = (16 << 3) | sev;
            let line = format!(
                "<{}>Jan 15 10:30:45 benchhost myapp[1234]: severity={} seq={:06} this is a realistic syslog message with padding\n",
                pri, sev, i
            );
            buf.extend_from_slice(line.as_bytes());
        }

        let n_iters = 10_000;
        let start = std::time::Instant::now();
        let mut total_lines = 0usize;
        for _ in 0..n_iters {
            let batch = parse_syslog_batch(std::hint::black_box(&buf));
            total_lines += batch.lines.len();
            std::hint::black_box(&batch);
        }
        let elapsed = start.elapsed();
        let total_msgs = n_iters as u64 * 1000;
        let ns_per = elapsed.as_nanos() as f64 / total_msgs as f64;
        let msgs_per_sec = total_msgs as f64 / elapsed.as_secs_f64();
        let mb_per_sec = (buf.len() as f64 * n_iters as f64) / elapsed.as_secs_f64() / 1e6;

        // Prevent dead code elimination
        assert!(total_lines > 0);

        eprintln!(
            "Batch syslog parse: {:.1}ns/msg, {:.1}M msgs/sec, {:.0} MB/s ({} lines × {} batches, {} bytes/batch)",
            ns_per, msgs_per_sec / 1e6, mb_per_sec, 1000, n_iters, buf.len()
        );
    }
}

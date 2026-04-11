//! UDP input source. Listens on a UDP socket and produces one InputEvent
//! per received datagram (or batch of datagrams).

use std::io;
use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use logfwd_types::diagnostics::ComponentHealth;
use socket2::{Domain, Protocol, Socket, Type};

use crate::input::{InputEvent, InputSource};
use crate::polling_input_health::{PollingInputHealthEvent, reduce_polling_input_health};

/// Maximum UDP payload based on UDP length field: 65535 - 8-byte UDP header.
const MAX_UDP_PAYLOAD: usize = 65527;

/// Desired kernel receive buffer size (8 MiB). Set best-effort — the OS may
/// cap it lower depending on `sysctl net.core.rmem_max`.
const RECV_BUF_SIZE: usize = 8 * 1024 * 1024;

/// Hard bound on datagrams drained in a single `poll()` call.
///
/// This prevents one busy socket from monopolizing an entire runtime tick.
const MAX_DATAGRAMS_PER_POLL: usize = 256;

/// Target bound on emitted bytes in a single `poll()` call.
///
/// This caps per-poll memory growth for high-rate UDP senders. Because UDP
/// datagrams are consumed atomically, one poll can exceed this limit by at
/// most one datagram (+ optional synthetic newline).
const MAX_EMIT_BYTES_PER_POLL: usize = 1024 * 1024;

#[inline]
const fn should_stop_udp_drain(datagrams_read: usize, emitted_bytes: usize) -> bool {
    datagrams_read >= MAX_DATAGRAMS_PER_POLL || emitted_bytes >= MAX_EMIT_BYTES_PER_POLL
}

/// UDP input that listens for datagrams. Each datagram is treated as one
/// or more newline-delimited log lines.
pub struct UdpInput {
    name: String,
    socket: UdpSocket,
    buf: Vec<u8>,
    /// Actual kernel receive buffer size after SO_RCVBUF was applied.
    actual_recv_buf: usize,
    /// Counter for detected drops (ENOBUFS or similar errors).
    drops_detected: Arc<AtomicU64>,
    /// Coarse control-plane health derived from the most recent poll cycle.
    health: ComponentHealth,
    stats: Arc<logfwd_types::diagnostics::ComponentStats>,
}

impl UdpInput {
    /// Bind to `addr` (e.g. "0.0.0.0:514" for syslog).
    pub fn new(
        name: impl Into<String>,
        addr: &str,
        stats: Arc<logfwd_types::diagnostics::ComponentStats>,
    ) -> io::Result<Self> {
        let parsed_addr: std::net::SocketAddr = addr.parse().map_err(io::Error::other)?;
        let domain = if parsed_addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };

        // Use socket2 to create the socket so we can tune SO_RCVBUF *before*
        // any datagrams arrive.
        let sock2 = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;

        // Bind before setting buffer — ensures the socket is valid.
        sock2.bind(&parsed_addr.into())?;

        // Tune kernel receive buffer to reduce packet loss under load.
        let _ = sock2.set_recv_buffer_size(RECV_BUF_SIZE); // best-effort

        // Read back actual buffer size — the OS may cap it.
        let actual_recv_buf = sock2.recv_buffer_size().unwrap_or(0);

        sock2.set_nonblocking(true)?;

        let socket: UdpSocket = sock2.into();

        stats.udp_recv_buf.store(actual_recv_buf, Ordering::Relaxed);

        Ok(Self {
            name: name.into(),
            socket,
            buf: vec![0u8; MAX_UDP_PAYLOAD],
            actual_recv_buf,
            drops_detected: Arc::new(AtomicU64::new(0)),
            health: ComponentHealth::Healthy,
            stats,
        })
    }

    /// Returns the local address this socket is bound to.
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.socket.local_addr()
    }

    /// Returns the actual kernel receive buffer size (as reported by
    /// `getsockopt`). Useful for diagnostics — compare with `RECV_BUF_SIZE`
    /// to see if the OS capped the requested value.
    pub fn recv_buffer_size(&self) -> usize {
        self.actual_recv_buf
    }

    /// Returns the number of detected drop events (ENOBUFS / similar errors
    /// observed during `recv`). This is a lower bound — the kernel may drop
    /// packets silently without signalling ENOBUFS.
    pub fn drops_detected(&self) -> u64 {
        self.drops_detected.load(Ordering::Relaxed)
    }

    /// Returns a clone of the drops counter for external monitoring.
    pub fn drops_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.drops_detected)
    }
}

impl InputSource for UdpInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let mut under_pressure = false;
        let mut datagrams_read = 0usize;

        // Accumulate into a single byte buffer; avoid per-datagram Vec alloc.
        // We re-use `self.buf` for recv and build the output in a separate vec
        // only when data actually arrives.
        let mut total: Option<Vec<u8>> = None;
        let mut source_bytes: u64 = 0;

        // Drain all available datagrams in one poll cycle.
        loop {
            // `recv` is cheaper than `recv_from` — we don't need the source addr.
            match self.socket.recv(&mut self.buf) {
                Ok(n) => {
                    datagrams_read = datagrams_read.saturating_add(1);
                    if n > 0 {
                        source_bytes = source_bytes.saturating_add(n as u64);
                        let data = &self.buf[..n];
                        let out = total.get_or_insert_with(|| Vec::with_capacity(4096));
                        out.extend_from_slice(data);
                        // Ensure newline termination so the scanner always sees
                        // complete lines, even if the sender omitted a trailing LF.
                        if !data.ends_with(b"\n") {
                            out.push(b'\n');
                        }
                    }
                    let emitted_bytes = total.as_ref().map_or(0, Vec::len);
                    if should_stop_udp_drain(datagrams_read, emitted_bytes) {
                        break;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                // ECONNREFUSED can arrive on a connected UDP socket (ICMP
                // port-unreachable cached by the kernel). Treat it like
                // WouldBlock — there is simply no data right now.
                Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => break,
                // ENOBUFS/ENOMEM: kernel ran out of buffer space — a drop signal.
                // Break to yield what we have; continuing would spin without progress.
                Err(ref e)
                    if e.raw_os_error() == Some(libc::ENOBUFS)
                        || e.raw_os_error() == Some(libc::ENOMEM) =>
                {
                    self.drops_detected.fetch_add(1, Ordering::Relaxed);
                    under_pressure = true;
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        self.stats.udp_drops.store(
            self.drops_detected.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );

        self.health = reduce_polling_input_health(
            self.health,
            if under_pressure {
                PollingInputHealthEvent::BackpressureObserved
            } else {
                PollingInputHealthEvent::PollHealthy
            },
        );

        match total {
            Some(bytes) => {
                let accounted_bytes = source_bytes;
                Ok(vec![InputEvent::Data {
                    bytes,
                    source_id: None,
                    accounted_bytes,
                }])
            }
            None => Ok(Vec::new()),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        self.health
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::net::UdpSocket as StdSocket;

    #[test]
    fn receives_datagrams() {
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        let sender = StdSocket::bind("127.0.0.1:0").unwrap();
        sender.send_to(b"hello world\n", addr).unwrap();
        sender.send_to(b"second line\n", addr).unwrap();

        // Give the OS a moment to deliver.
        std::thread::sleep(std::time::Duration::from_millis(50));

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        if let InputEvent::Data { bytes, .. } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            assert!(text.contains("hello world"), "got: {text}");
            assert!(text.contains("second line"), "got: {text}");
        }
    }

    #[test]
    fn adds_trailing_newline_to_bare_datagram() {
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        let sender = StdSocket::bind("127.0.0.1:0").unwrap();
        // No trailing newline — input must add one.
        sender.send_to(b"no newline", addr).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        if let InputEvent::Data { bytes, .. } = &events[0] {
            assert!(bytes.ends_with(b"\n"), "expected trailing newline");
        }
    }

    #[test]
    fn accounted_bytes_excludes_synthetic_trailing_newline() {
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        let sender = StdSocket::bind("127.0.0.1:0").unwrap();
        sender.send_to(b"no newline", addr).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        if let InputEvent::Data {
            bytes,
            accounted_bytes,
            ..
        } = &events[0]
        {
            assert_eq!(*accounted_bytes, 10);
            assert_eq!(bytes.len(), 11);
            assert!(bytes.ends_with(b"\n"), "expected trailing newline");
        } else {
            panic!("expected InputEvent::Data variant");
        }
    }

    #[test]
    fn handles_multi_line_datagram() {
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        let sender = StdSocket::bind("127.0.0.1:0").unwrap();
        sender.send_to(b"line1\nline2\nline3\n", addr).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        if let InputEvent::Data { bytes, .. } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            assert_eq!(text.matches('\n').count(), 3);
        }
    }

    #[test]
    fn empty_when_no_data() {
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let events = input.poll().unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn buffer_is_max_udp_payload_size() {
        let input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        assert_eq!(input.buf.len(), MAX_UDP_PAYLOAD);
    }

    #[test]
    fn udp_recv_buffer_size() {
        // Verify SO_RCVBUF was actually applied by reading it back.
        let input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let actual = input.recv_buffer_size();
        // The OS may double the requested value (Linux does this) or cap it,
        // but it should be at least something reasonable (> 64 KB).
        assert!(
            actual >= 65536,
            "expected recv buffer >= 64KB, got {actual}"
        );
    }

    #[test]
    fn udp_high_volume() {
        // Send 1000 datagrams in batches with pauses to avoid overwhelming
        // the kernel receive buffer on resource-constrained CI.
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        let sender = StdSocket::bind("127.0.0.1:0").unwrap();
        let total = 1000u32;
        let mut sent = 0u32;
        for batch_start in (0..total).step_by(100) {
            for i in batch_start..std::cmp::min(batch_start + 100, total) {
                let msg = format!("seq:{i}\n");
                if sender.send_to(msg.as_bytes(), addr).is_ok() {
                    sent += 1;
                }
            }
            // Brief pause between batches to let the receiver drain.
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Give the OS time to deliver remaining.
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Drain all available datagrams.
        let mut all_bytes = Vec::new();
        for _ in 0..50 {
            for event in input.poll().unwrap() {
                if let InputEvent::Data { bytes, .. } = event {
                    all_bytes.extend_from_slice(&bytes);
                }
            }
        }
        let text = String::from_utf8_lossy(&all_bytes);
        let mut received = 0u32;
        for i in 0..total {
            if text.contains(&format!("seq:{i}\n")) {
                received += 1;
            }
        }
        // Accept ≥50% of successful sends. UDP drops are expected but
        // on localhost with pacing most should arrive.
        let threshold = sent / 2;
        assert!(
            received >= threshold,
            "expected at least {threshold}/{sent} datagrams, got {received}"
        );
    }

    #[test]
    fn udp_empty_datagram() {
        // Sending a 0-byte datagram must not panic.
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        let sender = StdSocket::bind("127.0.0.1:0").unwrap();
        sender.send_to(b"", addr).unwrap();
        // Also send a real datagram after so we know poll drained.
        sender.send_to(b"after\n", addr).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));

        let events = input.poll().unwrap();
        // Should not panic. The empty datagram produces no output bytes,
        // but the "after" datagram should arrive.
        let mut all = Vec::new();
        for event in events {
            if let InputEvent::Data { bytes, .. } = event {
                all.extend_from_slice(&bytes);
            }
        }
        let text = String::from_utf8_lossy(&all);
        assert!(
            text.contains("after"),
            "expected 'after' datagram, got: {text}"
        );
    }

    #[test]
    fn udp_socket_is_nonblocking() {
        let input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        // Verify by attempting a recv on an empty socket — it should return
        // WouldBlock immediately, not block.
        let result = input.socket.recv(&mut [0u8; 1]);
        assert!(
            result.is_err(),
            "expected WouldBlock error on empty non-blocking socket"
        );
        assert_eq!(
            result.unwrap_err().kind(),
            io::ErrorKind::WouldBlock,
            "expected WouldBlock"
        );
    }

    #[test]
    fn drops_detected_starts_at_zero() {
        let input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        assert_eq!(input.drops_detected(), 0);
    }

    #[test]
    fn udp_health_recovers_after_clean_poll() {
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        input.health = ComponentHealth::Degraded;

        let events = input.poll().unwrap();
        assert!(events.is_empty());
        assert_eq!(input.health(), ComponentHealth::Healthy);
    }

    #[test]
    fn stop_predicate_matches_bounded_drain_policy() {
        assert!(!should_stop_udp_drain(0, 0));
        assert!(should_stop_udp_drain(MAX_DATAGRAMS_PER_POLL, 0));
        assert!(should_stop_udp_drain(0, MAX_EMIT_BYTES_PER_POLL));
    }

    proptest! {
        #[test]
        fn stop_predicate_equivalent_to_limit_expression(
            datagrams_read in 0usize..512,
            emitted_bytes in 0usize..(2 * MAX_EMIT_BYTES_PER_POLL),
        ) {
            let expected = datagrams_read >= MAX_DATAGRAMS_PER_POLL
                || emitted_bytes >= MAX_EMIT_BYTES_PER_POLL;
            prop_assert_eq!(should_stop_udp_drain(datagrams_read, emitted_bytes), expected);
        }
    }

    #[test]
    fn udp_poll_drain_is_bounded_and_recoverable_next_poll() {
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();
        let sender = StdSocket::bind("127.0.0.1:0").unwrap();

        let total = MAX_DATAGRAMS_PER_POLL + 32;
        for i in 0..total {
            let msg = format!("pkt-{i}\n");
            sender.send_to(msg.as_bytes(), addr).unwrap();
        }

        std::thread::sleep(std::time::Duration::from_millis(75));

        let first = input.poll().unwrap();
        assert_eq!(first.len(), 1);
        let first_lines = first
            .iter()
            .filter_map(|event| match event {
                InputEvent::Data { bytes, .. } => {
                    Some(bytes.iter().filter(|&&b| b == b'\n').count())
                }
                _ => None,
            })
            .sum::<usize>();
        assert_eq!(
            first_lines, MAX_DATAGRAMS_PER_POLL,
            "first poll must stop at datagram drain cap"
        );

        let mut seen = Vec::new();
        for event in first {
            if let InputEvent::Data { bytes, .. } = event {
                seen.extend_from_slice(&bytes);
            }
        }
        for _ in 0..8 {
            let events = input.poll().unwrap();
            if events.is_empty() {
                break;
            }
            for event in events {
                if let InputEvent::Data { bytes, .. } = event {
                    seen.extend_from_slice(&bytes);
                }
            }
        }

        let text = String::from_utf8_lossy(&seen);
        for i in 0..total {
            assert!(text.contains(&format!("pkt-{i}\n")), "missing pkt-{i}");
        }
    }
}

#[cfg(kani)]
mod verification {
    use super::{MAX_DATAGRAMS_PER_POLL, MAX_EMIT_BYTES_PER_POLL, should_stop_udp_drain};

    #[kani::proof]
    fn verify_zero_counters_do_not_stop_drain() {
        assert!(!should_stop_udp_drain(0, 0));
        kani::cover!(
            !should_stop_udp_drain(0, 0),
            "non-stopping path is reachable"
        );
    }

    #[kani::proof]
    fn verify_each_limit_independently_stops_drain() {
        let emitted_bytes = kani::any::<usize>();
        assert!(should_stop_udp_drain(MAX_DATAGRAMS_PER_POLL, emitted_bytes));
        kani::cover!(
            should_stop_udp_drain(MAX_DATAGRAMS_PER_POLL, 0),
            "datagram-cap stop path is reachable"
        );

        let datagrams_read = kani::any::<usize>();
        assert!(should_stop_udp_drain(
            datagrams_read,
            MAX_EMIT_BYTES_PER_POLL
        ));
        kani::cover!(
            should_stop_udp_drain(0, MAX_EMIT_BYTES_PER_POLL),
            "emit-byte-cap stop path is reachable"
        );
    }

    #[kani::proof]
    fn verify_stop_predicate_equivalence() {
        let datagrams_read = kani::any::<usize>();
        let emitted_bytes = kani::any::<usize>();
        let expected =
            datagrams_read >= MAX_DATAGRAMS_PER_POLL || emitted_bytes >= MAX_EMIT_BYTES_PER_POLL;
        assert_eq!(
            should_stop_udp_drain(datagrams_read, emitted_bytes),
            expected
        );
        kani::cover!(
            should_stop_udp_drain(MAX_DATAGRAMS_PER_POLL, 0),
            "stop branch reachable"
        );
        kani::cover!(!should_stop_udp_drain(0, 0), "continue branch reachable");
    }
}

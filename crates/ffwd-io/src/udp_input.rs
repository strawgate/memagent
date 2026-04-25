//! UDP input source. Listens on a UDP socket and produces one SourceEvent
//! per received datagram (or batch of datagrams).

use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use ffwd_types::diagnostics::ComponentHealth;
use ffwd_types::pipeline::SourceId;
use socket2::{Domain, Protocol, Socket, Type};

use crate::input::{InputSource, SourceEvent};
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

/// Derive a stable sender-scoped source id for UDP datagrams.
///
/// Domain-separated from other source-id families (file fingerprints, TCP
/// connection sequence ids, etc.) so maps keyed by SourceId are less likely to
/// collide across transport types.
fn source_id_for_sender(addr: SocketAddr) -> SourceId {
    let mut h = xxhash_rust::xxh64::Xxh64::new(0);
    h.update(b"udp:");
    match addr {
        SocketAddr::V4(v4) => {
            h.update(&[4u8]);
            h.update(&v4.ip().octets());
            h.update(&v4.port().to_le_bytes());
        }
        SocketAddr::V6(v6) => {
            h.update(&[6u8]);
            h.update(&v6.ip().octets());
            h.update(&v6.port().to_le_bytes());
            h.update(&v6.scope_id().to_le_bytes());
        }
    }
    let digest = h.digest();
    SourceId(if digest == 0 { 1 } else { digest })
}

fn flush_current_sender(
    events: &mut Vec<SourceEvent>,
    current: &mut Option<(SourceId, Vec<u8>, u64)>,
) {
    if let Some((source_id, bytes, accounted_bytes)) = current.take()
        && !bytes.is_empty()
    {
        events.push(SourceEvent::Data {
            bytes: Bytes::from(bytes),
            source_id: Some(source_id),
            accounted_bytes,
            cri_metadata: None,
        });
    }
}

#[derive(Debug, Clone)]
pub struct UdpInputOptions {
    /// Maximum UDP packet payload size in bytes. Defaults to `MAX_UDP_PAYLOAD`.
    pub max_message_size_bytes: usize,
    /// Desired socket receive buffer size (SO_RCVBUF). Defaults to `RECV_BUF_SIZE`.
    pub so_rcvbuf: usize,
}

impl Default for UdpInputOptions {
    fn default() -> Self {
        Self {
            max_message_size_bytes: MAX_UDP_PAYLOAD,
            so_rcvbuf: RECV_BUF_SIZE,
        }
    }
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
    stats: Arc<ffwd_types::diagnostics::ComponentStats>,
}

impl UdpInput {
    /// Bind to `addr` (e.g. "0.0.0.0:514" for syslog) with the given options.
    pub fn with_options(
        name: impl Into<String>,
        addr: &str,
        options: UdpInputOptions,
        stats: Arc<ffwd_types::diagnostics::ComponentStats>,
    ) -> io::Result<Self> {
        let parsed_addr: SocketAddr = addr.parse().map_err(io::Error::other)?;
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
        let _ = sock2.set_recv_buffer_size(options.so_rcvbuf); // best-effort

        // Read back actual buffer size — the OS may cap it.
        let actual_recv_buf = sock2.recv_buffer_size().unwrap_or(0);

        sock2.set_nonblocking(true)?;

        let socket: UdpSocket = sock2.into();

        stats.udp_recv_buf.store(actual_recv_buf, Ordering::Relaxed);

        Ok(Self {
            name: name.into(),
            socket,
            buf: vec![0u8; options.max_message_size_bytes],
            actual_recv_buf,
            drops_detected: Arc::new(AtomicU64::new(0)),
            health: ComponentHealth::Healthy,
            stats,
        })
    }

    /// Bind to `addr` (e.g. "0.0.0.0:514" for syslog) with the default options.
    pub fn new(
        name: impl Into<String>,
        addr: &str,
        stats: Arc<ffwd_types::diagnostics::ComponentStats>,
    ) -> io::Result<Self> {
        Self::with_options(name, addr, UdpInputOptions::default(), stats)
    }

    /// Returns the local address this socket is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
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
    fn poll(&mut self) -> io::Result<Vec<SourceEvent>> {
        let mut under_pressure = false;
        let mut datagrams_read = 0usize;

        let mut events = Vec::with_capacity(MAX_DATAGRAMS_PER_POLL);
        let mut current: Option<(SourceId, Vec<u8>, u64)> = None;
        let mut emitted_bytes = 0usize;

        // Drain all available datagrams in one poll cycle.
        loop {
            match self.socket.recv_from(&mut self.buf) {
                Ok((n, sender)) => {
                    datagrams_read = datagrams_read.saturating_add(1);
                    if n > 0 {
                        let source_id = source_id_for_sender(sender);
                        let data = &self.buf[..n];
                        let should_append_newline = !data.ends_with(b"\n");
                        if current
                            .as_ref()
                            .is_none_or(|(current_source_id, _, _)| *current_source_id != source_id)
                        {
                            flush_current_sender(&mut events, &mut current);
                            current = Some((
                                source_id,
                                Vec::with_capacity(
                                    n.saturating_add(usize::from(should_append_newline)),
                                ),
                                0,
                            ));
                        }
                        let (_, out, accounted_bytes) =
                            current.as_mut().expect("current sender just initialized");
                        out.extend_from_slice(data);
                        // Ensure newline termination so the scanner always sees
                        // complete lines, even if the sender omitted a trailing LF.
                        if should_append_newline {
                            out.push(b'\n');
                        }
                        *accounted_bytes = accounted_bytes.saturating_add(n as u64);
                        emitted_bytes =
                            emitted_bytes.saturating_add(n + usize::from(should_append_newline));
                    }
                    if should_stop_udp_drain(datagrams_read, emitted_bytes) {
                        under_pressure = true;
                        break;
                    }
                }
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
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                // ECONNREFUSED can arrive on a connected UDP socket (ICMP
                // port-unreachable cached by the kernel). Treat it like
                // WouldBlock — there is simply no data right now.
                Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => break,
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

        flush_current_sender(&mut events, &mut current);
        Ok(events)
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
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        let sender = StdSocket::bind("127.0.0.1:0").unwrap();
        sender.send_to(b"hello world\n", addr).unwrap();
        sender.send_to(b"second line\n", addr).unwrap();

        // Give the OS a moment to deliver.
        std::thread::sleep(std::time::Duration::from_millis(50));

        let events = input.poll().unwrap();
        assert!(!events.is_empty());
        let mut text = String::new();
        for event in &events {
            if let SourceEvent::Data { bytes, .. } = event {
                text.push_str(&String::from_utf8_lossy(bytes));
            }
        }
        assert!(text.contains("hello world"), "got: {text}");
        assert!(text.contains("second line"), "got: {text}");
    }

    #[test]
    fn adds_trailing_newline_to_bare_datagram() {
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        let sender = StdSocket::bind("127.0.0.1:0").unwrap();
        // No trailing newline — input must add one.
        sender.send_to(b"no newline", addr).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        if let SourceEvent::Data { bytes, .. } = &events[0] {
            assert!(bytes.ends_with(b"\n"), "expected trailing newline");
        }
    }

    #[test]
    fn accounted_bytes_excludes_synthetic_trailing_newline() {
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        let sender = StdSocket::bind("127.0.0.1:0").unwrap();
        sender.send_to(b"no newline", addr).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        if let SourceEvent::Data {
            bytes,
            accounted_bytes,
            ..
        } = &events[0]
        {
            assert_eq!(*accounted_bytes, 10);
            assert_eq!(bytes.len(), 11);
            assert!(bytes.ends_with(b"\n"), "expected trailing newline");
        } else {
            panic!("expected SourceEvent::Data variant");
        }
    }

    #[test]
    fn sender_addresses_produce_distinct_source_ids() {
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        let sender_a = StdSocket::bind("127.0.0.1:0").unwrap();
        let sender_b = StdSocket::bind("127.0.0.1:0").unwrap();
        sender_a.send_to(b"from-a\n", addr).unwrap();
        sender_b.send_to(b"from-b\n", addr).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));

        let events = input.poll().unwrap();
        let source_ids = events
            .iter()
            .filter_map(|event| match event {
                SourceEvent::Data { source_id, .. } => *source_id,
                _ => None,
            })
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(source_ids.len(), 2, "expected one source id per sender");
        assert!(source_ids.contains(&source_id_for_sender(sender_a.local_addr().unwrap())));
        assert!(source_ids.contains(&source_id_for_sender(sender_b.local_addr().unwrap())));
    }

    #[test]
    fn ipv6_sender_source_id_ignores_flowinfo_and_distinguishes_ports() {
        let base = SocketAddr::V6(std::net::SocketAddrV6::new(
            std::net::Ipv6Addr::LOCALHOST,
            12_345,
            0,
            0,
        ));
        let different_flowinfo = SocketAddr::V6(std::net::SocketAddrV6::new(
            std::net::Ipv6Addr::LOCALHOST,
            12_345,
            0x000f_ffff,
            0,
        ));
        let different_port = SocketAddr::V6(std::net::SocketAddrV6::new(
            std::net::Ipv6Addr::LOCALHOST,
            54_321,
            0,
            0,
        ));

        assert_eq!(
            source_id_for_sender(base),
            source_id_for_sender(different_flowinfo),
            "IPv6 flowinfo is packet/flow metadata, not sender identity"
        );
        assert_ne!(
            source_id_for_sender(base),
            source_id_for_sender(different_port),
            "sender port remains part of UDP source identity"
        );
    }

    #[test]
    fn handles_multi_line_datagram() {
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        let sender = StdSocket::bind("127.0.0.1:0").unwrap();
        sender.send_to(b"line1\nline2\nline3\n", addr).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        if let SourceEvent::Data { bytes, .. } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            assert_eq!(text.matches('\n').count(), 3);
        }
    }

    #[test]
    fn empty_when_no_data() {
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
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
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        assert_eq!(input.buf.len(), MAX_UDP_PAYLOAD);
    }

    #[test]
    fn test_udp_custom_options() {
        let options = UdpInputOptions {
            max_message_size_bytes: 1024,
            so_rcvbuf: 131072, // 128KB
        };
        let input = UdpInput::with_options(
            "test_custom",
            "127.0.0.1:0",
            options,
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        assert_eq!(input.buf.len(), 1024);
        assert!(input.recv_buffer_size() >= 131072);
    }

    #[test]
    fn udp_recv_buffer_size() {
        // Verify SO_RCVBUF was actually applied by reading it back.
        let input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
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
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
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
                if let SourceEvent::Data { bytes, .. } = event {
                    all_bytes.extend_from_slice(&bytes);
                }
            }
        }
        let text = String::from_utf8_lossy(&all_bytes);
        let received = (0..total)
            .filter(|i| text.contains(&format!("seq:{i}\n")))
            .count() as u32;
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
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
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
            if let SourceEvent::Data { bytes, .. } = event {
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
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
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
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        assert_eq!(input.drops_detected(), 0);
    }

    #[test]
    fn udp_health_recovers_after_clean_poll() {
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
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
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
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
        assert!(!first.is_empty());
        assert!(first.len() <= MAX_DATAGRAMS_PER_POLL);
        let first_lines = first
            .iter()
            .filter_map(|event| match event {
                SourceEvent::Data { bytes, .. } => Some(memchr::memchr_iter(b'\n', bytes).count()),
                _ => None,
            })
            .sum::<usize>();
        assert_eq!(
            first_lines, MAX_DATAGRAMS_PER_POLL,
            "first poll must stop at datagram drain cap"
        );

        let mut seen: Vec<u8> = first
            .into_iter()
            .filter_map(|event| match event {
                SourceEvent::Data { bytes, .. } => Some(bytes),
                _ => None,
            })
            .flatten()
            .collect();
        for _ in 0..8 {
            let events = input.poll().unwrap();
            if events.is_empty() {
                break;
            }
            seen.extend(
                events
                    .into_iter()
                    .filter_map(|event| match event {
                        SourceEvent::Data { bytes, .. } => Some(bytes),
                        _ => None,
                    })
                    .flatten(),
            );
        }

        let text = String::from_utf8_lossy(&seen);
        for i in 0..total {
            assert!(text.contains(&format!("pkt-{i}\n")), "missing pkt-{i}");
        }
    }

    #[test]
    fn source_id_is_present_for_received_datagrams() {
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();
        let sender = StdSocket::bind("127.0.0.1:0").unwrap();

        sender.send_to(b"hello\n", addr).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            SourceEvent::Data {
                source_id: Some(_), ..
            } => {}
            _ => panic!("expected UDP data to carry a source_id"),
        }
    }

    #[test]
    fn source_id_is_stable_per_sender_socket() {
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();
        let sender = StdSocket::bind("127.0.0.1:0").unwrap();

        sender.send_to(b"first\n", addr).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(25));
        let first_events = input.poll().unwrap();
        let first = match &first_events[0] {
            SourceEvent::Data {
                source_id: Some(id),
                ..
            } => *id,
            _ => panic!("expected source_id on first poll"),
        };

        sender.send_to(b"second\n", addr).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(25));
        let second_events = input.poll().unwrap();
        let second = match &second_events[0] {
            SourceEvent::Data {
                source_id: Some(id),
                ..
            } => *id,
            _ => panic!("expected source_id on second poll"),
        };

        assert_eq!(first, second);
    }

    #[test]
    fn source_ids_differ_for_distinct_senders() {
        let mut input = UdpInput::new(
            "test",
            "127.0.0.1:0",
            Arc::new(ffwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();
        let sender_a = StdSocket::bind("127.0.0.1:0").unwrap();
        let sender_b = StdSocket::bind("127.0.0.1:0").unwrap();

        sender_a.send_to(b"a\n", addr).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(25));
        let event_a = input.poll().unwrap();
        let source_a = match &event_a[0] {
            SourceEvent::Data {
                source_id: Some(id),
                ..
            } => *id,
            _ => panic!("expected source_id for sender_a"),
        };

        sender_b.send_to(b"b\n", addr).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(25));
        let event_b = input.poll().unwrap();
        let source_b = match &event_b[0] {
            SourceEvent::Data {
                source_id: Some(id),
                ..
            } => *id,
            _ => panic!("expected source_id for sender_b"),
        };

        assert_ne!(source_a, source_b);
    }

    #[test]
    fn source_id_ipv6_stable_and_distinct() {
        use std::net::{Ipv6Addr, SocketAddrV6};

        let addr_a = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 5000, 0, 0));
        let addr_b = SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1),
            5000,
            0,
            0,
        ));

        // Stability: same address yields the same id across calls.
        let id_a1 = source_id_for_sender(addr_a);
        let id_a2 = source_id_for_sender(addr_a);
        assert_eq!(id_a1, id_a2, "IPv6 source id must be stable");

        // Distinctness: different addresses yield different ids.
        let id_b = source_id_for_sender(addr_b);
        assert_ne!(
            id_a1, id_b,
            "different IPv6 addresses must produce different ids"
        );
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

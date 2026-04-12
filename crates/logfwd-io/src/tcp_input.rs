//! TCP input source. Listens on a TCP socket and produces newline-delimited
//! log lines from connected clients. Multiple concurrent connections supported.
//!
//! Each accepted connection receives a unique `SourceId` derived from a
//! monotonic counter so that `FramedInput`'s per-source remainder tracking
//! can distinguish data from different peers.

use std::io::{self, Read};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use logfwd_types::diagnostics::ComponentHealth;
use logfwd_types::pipeline::SourceId;
use socket2::SockRef;

use crate::input::{InputEvent, InputSource};
use crate::polling_input_health::{PollingInputHealthEvent, reduce_polling_input_health};

/// Maximum number of concurrent TCP client connections.
const MAX_CLIENTS: usize = 1024;

/// Per-poll read buffer size (64 KiB). Shared across all connections within a
/// single `poll` call; data is copied into per-client buffers immediately, so
/// one moderate buffer is sufficient.
const READ_BUF_SIZE: usize = 64 * 1024;

/// Default disconnect timeout for idle clients (no data received).
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Maximum frame/line payload length accepted from TCP senders.
///
/// Applies to both RFC 6587 octet-counted frames and newline-delimited lines.
/// Oversized records are discarded (not forwarded) to prevent memory abuse.
const MAX_LINE_LENGTH: usize = 1024 * 1024; // 1 MiB

/// Maximum total bytes buffered across all client `client_data` vecs within a
/// single `poll` call.  When this budget is exhausted we stop reading from
/// further clients in that poll, deferring them to the next call.  This
/// propagates TCP backpressure to senders and prevents OOM when many clients
/// flood data faster than the pipeline can drain it (fix for #576).
const MAX_TOTAL_BUFFERED_BYTES: usize = 256 * 1024 * 1024; // 256 MiB

/// Hard per-client byte bound for one `poll()` call.
///
/// Prevents one hot connection from monopolizing a full poll cycle.
const MAX_BYTES_PER_CLIENT_PER_POLL: usize = 512 * 1024;

/// Hard per-client read syscall bound for one `poll()` call.
///
/// Complements the byte cap so highly fragmented payloads are also bounded.
const MAX_READS_PER_CLIENT_PER_POLL: usize = 64;

#[inline]
const fn should_stop_client_read(bytes_read: usize, reads_done: usize) -> bool {
    bytes_read >= MAX_BYTES_PER_CLIENT_PER_POLL || reads_done >= MAX_READS_PER_CLIENT_PER_POLL
}

/// Derive a `SourceId` for a TCP connection from a monotonic counter.
///
/// The counter is hashed to avoid trivially predictable identifiers and to
/// spread keys evenly in hash maps.
fn source_id_for_connection(connection_seq: u64) -> SourceId {
    // Use a domain-separated hash so TCP source ids never collide with
    // file-based source ids (which hash device+inode+fingerprint).
    let mut h = xxhash_rust::xxh64::Xxh64::new(0);
    h.update(b"tcp:");
    h.update(&connection_seq.to_le_bytes());
    SourceId(h.digest())
}

/// A connected TCP client with an associated last-data timestamp.
struct Client {
    stream: TcpStream,
    source_id: SourceId,
    last_data: Instant,
    /// Unparsed bytes for this connection.
    pending: Vec<u8>,
    /// Set after the first successfully parsed octet-counted frame.
    octet_counting_mode: bool,
    /// Remaining bytes to drop from an oversized octet-counted frame.
    discard_octet_bytes: usize,
    /// Whether we are dropping newline-delimited bytes until a newline appears.
    discard_until_newline: bool,
}

fn parse_octet_prefix(buf: &[u8]) -> Option<(usize, usize)> {
    if buf.is_empty() || !buf[0].is_ascii_digit() {
        return None;
    }
    let mut i = 0usize;
    let mut len = 0usize;
    while i < buf.len() && buf[i].is_ascii_digit() {
        len = len.checked_mul(10)?.checked_add((buf[i] - b'0') as usize)?;
        i += 1;
    }
    if i == 0 || i >= buf.len() || buf[i] != b' ' {
        return None;
    }
    Some((len, i + 1))
}

fn advance_pending(client: &mut Client, consumed: usize) {
    if consumed == 0 {
        return;
    }
    let remaining = client.pending.len().saturating_sub(consumed);
    client.pending.copy_within(consumed.., 0);
    client.pending.truncate(remaining);
}

#[inline]
fn has_incomplete_octet_frame_tail(buf: &[u8]) -> bool {
    if buf.is_empty() || !buf[0].is_ascii_digit() {
        return false;
    }
    let mut i = 0usize;
    let mut len = 0usize;
    while i < buf.len() && buf[i].is_ascii_digit() {
        len = match len
            .checked_mul(10)
            .and_then(|v| v.checked_add((buf[i] - b'0') as usize))
        {
            Some(v) => v,
            None => return true,
        };
        i += 1;
    }
    if i >= buf.len() {
        // Digits with no delimiter can still be a truncated octet prefix.
        return true;
    }
    if buf[i] != b' ' {
        return false;
    }
    let prefix_len = i + 1;
    match prefix_len.checked_add(len) {
        Some(needed) => buf.len() < needed,
        None => true,
    }
}

fn extract_complete_records(client: &mut Client, out: &mut Vec<u8>) {
    let mut consumed = 0usize;

    loop {
        let pending = &client.pending[consumed..];
        if pending.is_empty() {
            break;
        }

        if client.discard_octet_bytes > 0 {
            let drop_n = client.discard_octet_bytes.min(pending.len());
            consumed += drop_n;
            client.discard_octet_bytes -= drop_n;
            if client.discard_octet_bytes > 0 {
                break;
            }
            continue;
        }

        if client.discard_until_newline {
            if let Some(pos) = memchr::memchr(b'\n', pending) {
                consumed += pos + 1;
                client.discard_until_newline = false;
                continue;
            }
            if pending.len() > MAX_LINE_LENGTH {
                consumed += pending.len() - MAX_LINE_LENGTH;
            }
            break;
        }

        if let Some((len, prefix_len)) = parse_octet_prefix(pending)
            && let Some(needed) = prefix_len.checked_add(len)
        {
            // Only commit to octet-counting when a complete, plausibly bounded
            // frame is available. This avoids legacy lines like "200 OK\n"
            // stalling behind an ambiguous "<digits><space>" prefix.
            let octet_frame_ready = pending.len() >= needed;
            let octet_boundary_is_plausible = octet_frame_ready
                && (pending.len() == needed
                    || matches!(pending.get(needed), Some(b'\n'))
                    || parse_octet_prefix(&pending[needed..]).is_some());

            if octet_frame_ready && octet_boundary_is_plausible {
                client.octet_counting_mode = true;
                if len > MAX_LINE_LENGTH {
                    client.discard_octet_bytes = len;
                    consumed += prefix_len;
                    continue;
                }
                out.extend_from_slice(&pending[prefix_len..needed]);
                out.push(b'\n');
                consumed += needed;
                continue;
            }
        }

        if let Some(pos) = memchr::memchr(b'\n', pending) {
            if pos > MAX_LINE_LENGTH {
                client.discard_until_newline = true;
                continue;
            }
            out.extend_from_slice(&pending[..=pos]);
            consumed += pos + 1;
            continue;
        }

        if pending.len() > MAX_LINE_LENGTH {
            client.discard_until_newline = true;
            continue;
        }
        break;
    }

    advance_pending(client, consumed);
}

/// TCP input that accepts connections and reads newline-delimited data.
///
/// Each connection is assigned a unique `SourceId` so downstream components
/// can track per-connection state (e.g., partial-line remainders).
pub struct TcpInput {
    name: String,
    listener: TcpListener,
    clients: Vec<Client>,
    buf: Vec<u8>,
    idle_timeout: Duration,
    /// Total connections accepted since creation (never decreases).
    connections_accepted: u64,
    /// Monotonic counter for generating per-connection `SourceId` values.
    next_connection_seq: u64,
    /// Coarse control-plane health derived from the most recent poll cycle.
    health: ComponentHealth,
    stats: std::sync::Arc<logfwd_types::diagnostics::ComponentStats>,
}

impl TcpInput {
    /// Bind to `addr` (e.g. "0.0.0.0:5140") with the default idle timeout.
    pub fn new(
        name: impl Into<String>,
        addr: &str,
        stats: std::sync::Arc<logfwd_types::diagnostics::ComponentStats>,
    ) -> io::Result<Self> {
        Self::with_idle_timeout(name, addr, DEFAULT_IDLE_TIMEOUT, stats)
    }

    /// Bind to `addr` with a custom idle timeout.
    pub fn with_idle_timeout(
        name: impl Into<String>,
        addr: &str,
        idle_timeout: Duration,
        stats: std::sync::Arc<logfwd_types::diagnostics::ComponentStats>,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            name: name.into(),
            listener,
            clients: Vec::new(),
            buf: vec![0u8; READ_BUF_SIZE],
            idle_timeout,
            connections_accepted: 0,
            next_connection_seq: 0,
            health: ComponentHealth::Healthy,
            stats,
        })
    }

    /// Returns the local address this listener is bound to.
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    /// Returns the number of currently tracked client connections.
    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    /// Returns the total number of connections accepted since creation.
    /// Monotonically increasing — useful for tests that need to verify a
    /// connection was accepted even if it was immediately disconnected.
    #[cfg(test)]
    pub fn connections_accepted(&self) -> u64 {
        self.connections_accepted
    }
}

impl InputSource for TcpInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let mut under_pressure = false;

        // Accept new connections up to the limit.
        loop {
            if self.clients.len() >= MAX_CLIENTS {
                // Drain (and drop) any pending connections beyond the limit so
                // the kernel accept queue does not fill up and stall.
                match self.listener.accept() {
                    Ok((_stream, _addr)) => {
                        self.connections_accepted += 1;
                        self.stats.tcp_accepted.store(
                            self.connections_accepted,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        under_pressure = true;
                        continue; // dropped immediately
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(_) => break, // transient accept error, not fatal
                }
            }
            match self.listener.accept() {
                Ok((stream, _addr)) => {
                    stream.set_nonblocking(true)?;

                    // Enable TCP keepalive so we detect dead peers promptly.
                    // Use SockRef to borrow the fd — no clone, no leak.
                    let sock_ref = SockRef::from(&stream);
                    let _ = sock_ref.set_keepalive(true);
                    let keepalive = socket2::TcpKeepalive::new()
                        .with_time(Duration::from_secs(60))
                        .with_interval(Duration::from_secs(10));
                    let _ = sock_ref.set_tcp_keepalive(&keepalive);

                    let sid = source_id_for_connection(self.next_connection_seq);
                    self.next_connection_seq += 1;
                    self.connections_accepted += 1;
                    self.stats.tcp_accepted.store(
                        self.connections_accepted,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    self.clients.push(Client {
                        stream,
                        source_id: sid,
                        last_data: Instant::now(),
                        pending: Vec::new(),
                        octet_counting_mode: false,
                        discard_octet_bytes: 0,
                        discard_until_newline: false,
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::ConnectionAborted => {
                    // Peer reset before we accepted — harmless, keep going.
                }
                Err(e) => return Err(e),
            }
        }

        // Read from all clients, collecting per-connection buffers.
        let now = Instant::now();
        // Track which connections are dead using a bitmap for O(1) lookup.
        let mut alive = vec![true; self.clients.len()];
        // Per-client data buffers — only allocated when data arrives.
        let mut client_data: Vec<Option<Vec<u8>>> = vec![None; self.clients.len()];

        // Running total of bytes stored in client_data during this poll.
        // When this reaches MAX_TOTAL_BUFFERED_BYTES we stop reading from
        // further clients (backpressure — fix for #576).
        let mut total_buffered: usize = 0;

        for (i, client) in self.clients.iter_mut().enumerate() {
            // If the global per-poll budget is exhausted, stop reading more
            // clients this poll.  They will be read on the next poll call,
            // which propagates TCP flow-control back to the senders.
            if total_buffered >= MAX_TOTAL_BUFFERED_BYTES {
                break;
            }

            let mut got_data = false;
            let mut bytes_read_for_client = 0usize;
            let mut reads_for_client = 0usize;
            loop {
                if should_stop_client_read(bytes_read_for_client, reads_for_client) {
                    break;
                }
                let remaining_bytes =
                    MAX_BYTES_PER_CLIENT_PER_POLL.saturating_sub(bytes_read_for_client);
                if remaining_bytes == 0 {
                    break;
                }
                let read_cap = remaining_bytes.min(self.buf.len());
                match client.stream.read(&mut self.buf[..read_cap]) {
                    Ok(0) => {
                        // Clean EOF.
                        alive[i] = false;
                        break;
                    }
                    Ok(n) => {
                        let chunk = &self.buf[..n];
                        client.last_data = now;
                        got_data = true;
                        bytes_read_for_client = bytes_read_for_client.saturating_add(n);
                        reads_for_client = reads_for_client.saturating_add(1);

                        // Snapshot totals before this read so we can compute the
                        // net increase in buffered memory after parsing.
                        let out = client_data[i].get_or_insert_with(Vec::new);
                        let out_before = out.len();
                        let pending_before = client.pending.len(); // before extend

                        client.pending.extend_from_slice(chunk);
                        extract_complete_records(client, out);

                        // Net new bytes held in memory for this client after this
                        // read.  We account for both the parsed output (out) and the
                        // unparsed remainder (client.pending) so that a client with a
                        // long in-flight partial record counts against the global
                        // budget.  Without this, a client can accumulate up to
                        // MAX_LINE_LENGTH unparsed bytes without tripping the
                        // backpressure threshold, undermining OOM protection (fix for
                        // accounting gap in #576).
                        //
                        // extract_complete_records may also discard oversized data, so
                        // this delta can be negative; saturating_sub keeps accounting
                        // monotonic and bounded.
                        let total_after = out.len() + client.pending.len();
                        let total_before = out_before + pending_before;
                        total_buffered += total_after.saturating_sub(total_before);

                        // We must store bytes we have already read from the
                        // socket (discarding them would be data loss), so the
                        // budget check necessarily happens after the increment.
                        // The maximum overage is one READ_BUF_SIZE chunk
                        // (64 KiB), which is negligible relative to 256 MiB.
                        if total_buffered >= MAX_TOTAL_BUFFERED_BYTES {
                            under_pressure = true;
                            break;
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) if e.kind() == io::ErrorKind::ConnectionReset => {
                        // Peer sent RST — treat as a close.
                        alive[i] = false;
                        break;
                    }
                    Err(_) => {
                        // Any other read error — drop this connection.
                        alive[i] = false;
                        break;
                    }
                }
            }
            // Check idle AFTER reading — data may have arrived since last poll.
            if !got_data && alive[i] && now.duration_since(client.last_data) > self.idle_timeout {
                alive[i] = false;
            }
        }

        // Build events before removing dead connections — we need client_data
        // indices to match the current clients vec.
        let mut events = Vec::new();

        // Step 1: Data events.
        for (i, data) in client_data.into_iter().enumerate() {
            if let Some(bytes) = data {
                if !bytes.is_empty() {
                    let accounted_bytes = bytes.len() as u64;
                    events.push(InputEvent::Data {
                        bytes,
                        source_id: Some(self.clients[i].source_id),
                        accounted_bytes,
                    });
                }
            }
        }

        // Step 2: EndOfFile events for every connection that is dying.
        //
        // A dying connection's SourceId may have an associated partial-line
        // remainder in FramedInput.  Emitting EndOfFile (after any Data for
        // the same source) signals FramedInput to flush that remainder so the
        // last unterminated record is not silently dropped — fixes #804/#580.
        //
        // Additionally, any bytes still held in client.pending were never
        // passed to FramedInput as a Data event (extract_complete_records only
        // emits complete newline-terminated records or octet-counted frames).
        // On a clean close those bytes are a valid partial line that would
        // otherwise be silently dropped.  Flush them now — with a synthetic
        // newline so FramedInput treats them as a complete record rather than
        // parking them in its own remainder — before the EndOfFile so that the
        // subsequent EndOfFile can still flush any *prior* remainder already
        // held by FramedInput.  Skip the flush if the client was mid-discard
        // (oversized frame/line): those bytes are intentionally garbage.
        for (i, &is_alive) in alive.iter().enumerate() {
            if !is_alive {
                let client = &mut self.clients[i];
                let has_pending = !client.pending.is_empty();
                let mid_discard = client.discard_octet_bytes > 0 || client.discard_until_newline;
                let incomplete_octet_tail =
                    client.octet_counting_mode && has_incomplete_octet_frame_tail(&client.pending);
                if has_pending && !mid_discard && !incomplete_octet_tail {
                    let mut tail = std::mem::take(&mut client.pending);
                    tail.push(b'\n');
                    let accounted_bytes = tail.len() as u64;
                    events.push(InputEvent::Data {
                        bytes: tail,
                        source_id: Some(client.source_id),
                        accounted_bytes,
                    });
                }
                events.push(InputEvent::EndOfFile {
                    source_id: Some(self.clients[i].source_id),
                });
            }
        }

        // Remove dead connections, preserving order of remaining ones.
        // `retain` is cleaner than manual swap_remove with index tracking.
        let mut idx = 0;
        self.clients.retain(|_| {
            let keep = alive[idx];
            idx += 1;
            keep
        });

        self.health = reduce_polling_input_health(
            self.health,
            if under_pressure {
                PollingInputHealthEvent::BackpressureObserved
            } else {
                PollingInputHealthEvent::PollHealthy
            },
        );

        self.stats
            .tcp_active
            .store(self.clients.len(), std::sync::atomic::Ordering::Relaxed);

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
    use std::io::Write;
    use std::net::TcpStream as StdTcpStream;

    #[test]
    fn receives_tcp_data() {
        let input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.listener.local_addr().unwrap();

        let mut client = StdTcpStream::connect(addr).unwrap();
        client.write_all(b"{\"msg\":\"hello\"}\n").unwrap();
        client.write_all(b"{\"msg\":\"world\"}\n").unwrap();
        client.flush().unwrap();

        std::thread::sleep(Duration::from_millis(50));

        let mut input = input; // make mutable
        let events = input.poll().unwrap();

        // Should have accepted the connection and read data.
        assert_eq!(events.len(), 1);
        if let InputEvent::Data {
            bytes, source_id, ..
        } = &events[0]
        {
            let text = String::from_utf8_lossy(bytes);
            assert!(text.contains("hello"), "got: {text}");
            assert!(text.contains("world"), "got: {text}");
            assert!(source_id.is_some(), "TCP data must have a source_id");
        }
    }

    #[test]
    fn handles_disconnect() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.listener.local_addr().unwrap();

        {
            let mut client = StdTcpStream::connect(addr).unwrap();
            client.write_all(b"line1\n").unwrap();
        } // client drops here -> connection closed

        std::thread::sleep(Duration::from_millis(50));

        let events = input.poll().unwrap();

        // After the clean disconnect we expect both a Data event and an
        // EndOfFile event.  The EndOfFile signals FramedInput to flush any
        // partial-line remainder held for this SourceId.
        let data_count = events
            .iter()
            .filter(|e| matches!(e, InputEvent::Data { .. }))
            .count();
        let eof_count = events
            .iter()
            .filter(|e| matches!(e, InputEvent::EndOfFile { .. }))
            .count();
        assert_eq!(data_count, 1, "expected 1 data event");
        assert_eq!(eof_count, 1, "expected 1 EndOfFile event on disconnect");

        // Second poll should clean up the closed connection.
        let events = input.poll().unwrap();
        assert!(events.is_empty());
        assert!(input.clients.is_empty());
    }

    #[test]
    fn tcp_health_recovers_after_clean_poll() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        input.health = ComponentHealth::Degraded;

        let events = input.poll().unwrap();
        assert!(events.is_empty());
        assert_eq!(input.health(), ComponentHealth::Healthy);
    }

    /// A TCP client that sends a partial line (no trailing newline) and then
    /// disconnects must cause an EndOfFile event so that FramedInput can flush
    /// the partial remainder — fixes #804 / #580.
    #[test]
    fn tcp_partial_line_on_disconnect_emits_eof() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        {
            let mut client = StdTcpStream::connect(addr).unwrap();
            // Intentionally no trailing newline — this is the partial line.
            client.write_all(b"partial line without newline").unwrap();
            client.flush().unwrap();
        } // client drops here -> EOF

        std::thread::sleep(Duration::from_millis(50));

        let events = input.poll().unwrap();

        let has_eof = events
            .iter()
            .any(|e| matches!(e, InputEvent::EndOfFile { source_id } if source_id.is_some()));

        assert!(
            has_eof,
            "should emit EndOfFile on disconnect so FramedInput can flush the partial line"
        );

        // EndOfFile source_id must match any source id observed in this poll.
        let data_sid = events.iter().find_map(|e| match e {
            InputEvent::Data { source_id, .. } => *source_id,
            _ => None,
        });
        let eof_sid = events.iter().find_map(|e| {
            if let InputEvent::EndOfFile { source_id } = e {
                *source_id
            } else {
                None
            }
        });
        if let Some(data_sid) = data_sid {
            assert_eq!(
                Some(data_sid),
                eof_sid,
                "Data and EndOfFile must carry the same SourceId"
            );
        } else {
            assert!(eof_sid.is_some(), "EndOfFile should carry SourceId");
        }
    }

    #[test]
    fn tcp_idle_timeout() {
        // Use a very short idle timeout so the test runs fast.
        let mut input = TcpInput::with_idle_timeout(
            "test",
            "127.0.0.1:0",
            Duration::from_millis(200),
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        // Connect but send nothing.
        let _client = StdTcpStream::connect(addr).unwrap();
        std::thread::sleep(Duration::from_millis(50));

        // First poll: accept the connection.
        let _ = input.poll().unwrap();
        assert_eq!(input.client_count(), 1, "should have 1 client after accept");

        // Wait longer than the idle timeout.
        std::thread::sleep(Duration::from_millis(300));

        // Next poll should evict the idle connection.
        let _ = input.poll().unwrap();
        assert_eq!(
            input.client_count(),
            0,
            "idle client should have been disconnected"
        );
    }

    #[test]
    fn tcp_max_line_length() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        // Spawn the writer in a background thread because write_all of >1MB
        // will block until the reader drains the kernel buffer.
        // We send the oversized record AND the follow-up "ok\n" on the SAME
        // connection so the test actually verifies that the connection remains
        // usable after an oversized record is discarded, not just that a brand
        // new connection works (fix for Bug 2 / test correctness).
        let writer = std::thread::spawn(move || -> StdTcpStream {
            let mut client = StdTcpStream::connect(addr).unwrap();
            let big = vec![b'A'; MAX_LINE_LENGTH + 1];
            // Ignore errors — the server may apply backpressure before we
            // finish sending >1MB; we just need the server to see enough to
            // trigger the oversized-discard path.
            let _ = client.write_all(&big);
            // Follow-up newline terminates the oversized record so the parser
            // can discard it and resume normal framing on this connection.
            let _ = client.write_all(b"\n");
            // Now send a well-sized record on the same connection.
            client.write_all(b"ok\n").unwrap();
            client.flush().unwrap();
            client
        });

        // Poll until the "ok" record is observed or deadline expires.
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut has_ok = false;
        while Instant::now() < deadline {
            let events = input.poll().unwrap();
            if events.iter().any(|e| match e {
                InputEvent::Data { bytes, .. } => bytes.windows(3).any(|w| w == b"ok\n"),
                _ => false,
            }) {
                has_ok = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        // Keep the client alive until after polling so the connection is not
        // torn down before we observe the "ok" record.
        let _client = writer.join().unwrap();

        assert!(
            input.connections_accepted() > 0,
            "server must have accepted the connection"
        );
        assert!(
            has_ok,
            "same connection should remain usable after oversized discard"
        );
    }

    #[test]
    fn tcp_max_line_length_exact_boundary() {
        // A line of exactly MAX_LINE_LENGTH bytes (content only, excluding \n)
        // is accepted; only records strictly larger are dropped.
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        let writer = std::thread::spawn(move || {
            let mut client = StdTcpStream::connect(addr).unwrap();
            // Exactly MAX_LINE_LENGTH content bytes followed by a newline.
            let mut line = vec![b'A'; MAX_LINE_LENGTH];
            line.push(b'\n');
            let _ = client.write_all(&line);
            client.flush().unwrap();
        });

        // Use a deadline/polling loop rather than a fixed sleep so the test is
        // not flaky on slow CI runners.  Poll until the expected Data event is
        // observed or a generous timeout elapses.
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut got_boundary = false;
        while Instant::now() < deadline {
            let events = input.poll().unwrap();
            if events.into_iter().any(|e| match e {
                InputEvent::Data { bytes, .. } => {
                    bytes.len() == (MAX_LINE_LENGTH + 1) && bytes.ends_with(b"\n")
                }
                _ => false,
            }) {
                got_boundary = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = writer.join();
        assert!(got_boundary, "boundary-sized line should be forwarded");
    }

    #[test]
    fn tcp_octet_counted_frame_with_embedded_newline_is_single_record() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();
        let mut client = StdTcpStream::connect(addr).unwrap();
        client.write_all(b"11 hello\nworld").unwrap();
        client.flush().unwrap();
        std::thread::sleep(Duration::from_millis(50));

        let events = input.poll().unwrap();
        let joined = events
            .into_iter()
            .filter_map(|e| match e {
                InputEvent::Data { bytes, .. } => Some(bytes),
                _ => None,
            })
            .flatten()
            .collect::<Vec<u8>>();
        assert_eq!(joined, b"hello\nworld\n");
    }

    #[test]
    fn tcp_legacy_line_starting_with_digits_space_is_not_stalled_as_octet() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();
        let mut client = StdTcpStream::connect(addr).unwrap();
        client.write_all(b"200 OK\n").unwrap();
        client.flush().unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut got = Vec::new();
        while Instant::now() < deadline {
            for event in input.poll().unwrap() {
                if let InputEvent::Data { bytes, .. } = event {
                    got.extend_from_slice(&bytes);
                }
            }
            if !got.is_empty() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(got, b"200 OK\n");
    }

    #[test]
    fn tcp_connection_storm() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        // Rapidly connect and disconnect 100 times.
        for _ in 0..100 {
            let _ = StdTcpStream::connect(addr).unwrap();
            // Immediately dropped — connection closed.
        }

        std::thread::sleep(Duration::from_millis(100));

        // Poll several times to accept and then clean up all connections.
        for _ in 0..20 {
            let _ = input.poll().unwrap();
            std::thread::sleep(Duration::from_millis(20));
        }

        assert_eq!(
            input.client_count(),
            0,
            "all storm connections should be cleaned up (no fd leak)"
        );
    }

    /// Pending bytes held in `client.pending` (an unterminated line with no
    /// trailing newline) must be emitted as a `Data` event — with a synthetic
    /// trailing newline — before the `EndOfFile` event when the connection
    /// closes.  Previously those bytes were silently dropped (data loss bug).
    #[test]
    fn pending_bytes_flushed_as_data_event_on_close() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        {
            let mut client = StdTcpStream::connect(addr).unwrap();
            // Write a partial line with NO trailing newline so it ends up
            // in client.pending without being emitted by extract_complete_records.
            client.write_all(b"no-newline-tail").unwrap();
            client.flush().unwrap();
        } // client drops -> clean EOF

        std::thread::sleep(Duration::from_millis(50));

        let events = input.poll().unwrap();

        // There must be a Data event whose bytes contain "no-newline-tail".
        let data_bytes: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                InputEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();

        assert!(
            !data_bytes.is_empty(),
            "pending tail bytes must be emitted as a Data event on close (data loss fix)"
        );
        assert!(
            data_bytes
                .windows(b"no-newline-tail".len())
                .any(|w| w == b"no-newline-tail"),
            "emitted Data must contain the pending tail bytes; got: {:?}",
            String::from_utf8_lossy(&data_bytes),
        );

        // The flushed tail must be followed by EndOfFile for the same source.
        let has_eof = events
            .iter()
            .any(|e| matches!(e, InputEvent::EndOfFile { source_id } if source_id.is_some()));
        assert!(
            has_eof,
            "EndOfFile must still be emitted after the pending Data"
        );
    }

    #[test]
    fn incomplete_octet_tail_is_not_flushed_on_close_after_octet_mode() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        {
            let mut client = StdTcpStream::connect(addr).unwrap();
            // Complete frame (`hello`) followed by an incomplete octet-counted tail.
            client.write_all(b"5 hello4 tes").unwrap();
            client.flush().unwrap();
        } // close connection

        std::thread::sleep(Duration::from_millis(50));
        let events = input.poll().unwrap();

        let data_bytes: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                InputEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        let rendered = String::from_utf8_lossy(&data_bytes);
        assert!(
            rendered.contains("hello\n"),
            "complete octet frame should still be emitted; got: {rendered}"
        );
        assert!(
            !rendered.contains("tes"),
            "incomplete octet-counted tail must be dropped on close; got: {rendered}"
        );
    }

    #[test]
    fn overflowing_octet_prefix_is_treated_as_incomplete_tail() {
        let buf = format!("{} ", usize::MAX).into_bytes();
        assert!(
            has_incomplete_octet_frame_tail(&buf),
            "overflowing octet prefix must be treated as incomplete to avoid flushing malformed tails"
        );
    }

    #[test]
    fn truncated_digits_only_octet_prefix_is_treated_as_incomplete_tail() {
        assert!(
            has_incomplete_octet_frame_tail(b"12"),
            "truncated octet-count prefix must be treated as incomplete to avoid flushing malformed tails"
        );
    }

    /// Two concurrent TCP connections must receive distinct `SourceId` values
    /// so that `FramedInput` can track per-connection remainders independently.
    #[test]
    fn distinct_source_ids_per_connection() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        let mut client_a = StdTcpStream::connect(addr).unwrap();
        let mut client_b = StdTcpStream::connect(addr).unwrap();

        client_a.write_all(b"from_a\n").unwrap();
        client_b.write_all(b"from_b\n").unwrap();
        client_a.flush().unwrap();
        client_b.flush().unwrap();

        std::thread::sleep(Duration::from_millis(50));

        let events = input.poll().unwrap();

        // Collect all source_ids from data events.
        let source_ids: Vec<SourceId> = events
            .iter()
            .filter_map(|e| match e {
                InputEvent::Data { source_id, .. } => *source_id,
                _ => None,
            })
            .collect();

        assert!(
            source_ids.len() >= 2,
            "expected at least 2 data events (one per connection), got {}",
            source_ids.len()
        );

        // All source_ids must be distinct.
        let unique: std::collections::HashSet<u64> = source_ids.iter().map(|s| s.0).collect();
        assert_eq!(
            unique.len(),
            source_ids.len(),
            "each TCP connection must have a distinct SourceId"
        );
    }

    #[test]
    fn stop_predicate_matches_per_client_bounded_policy() {
        assert!(!should_stop_client_read(0, 0));
        assert!(should_stop_client_read(MAX_BYTES_PER_CLIENT_PER_POLL, 0));
        assert!(should_stop_client_read(0, MAX_READS_PER_CLIENT_PER_POLL));
    }

    proptest! {
        #[test]
        fn stop_predicate_equivalent_to_limit_expression(
            bytes_read in 0usize..(2 * MAX_BYTES_PER_CLIENT_PER_POLL),
            reads_done in 0usize..(2 * MAX_READS_PER_CLIENT_PER_POLL),
        ) {
            let expected = bytes_read >= MAX_BYTES_PER_CLIENT_PER_POLL
                || reads_done >= MAX_READS_PER_CLIENT_PER_POLL;
            prop_assert_eq!(should_stop_client_read(bytes_read, reads_done), expected);
        }
    }

    #[test]
    fn noisy_client_is_bounded_and_quiet_client_still_progresses() {
        let mut input = TcpInput::new(
            "test",
            "127.0.0.1:0",
            std::sync::Arc::new(logfwd_types::diagnostics::ComponentStats::new()),
        )
        .unwrap();
        let addr = input.local_addr().unwrap();

        let mut noisy = StdTcpStream::connect(addr).unwrap();
        let mut quiet = StdTcpStream::connect(addr).unwrap();

        for _ in 0..10 {
            let _ = input.poll().unwrap();
            if input.client_count() >= 2 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            input.client_count(),
            2,
            "both clients must be accepted before write workload"
        );

        let noisy_payload = b"noisy-line\n".repeat((MAX_BYTES_PER_CLIENT_PER_POLL / 11) + 128);
        noisy.write_all(&noisy_payload).unwrap();
        noisy.flush().unwrap();

        let quiet_payload = b"quiet-line\n";
        quiet.write_all(quiet_payload).unwrap();
        quiet.flush().unwrap();

        std::thread::sleep(Duration::from_millis(75));

        let events = input.poll().unwrap();
        let mut bytes_by_source: std::collections::HashMap<SourceId, usize> =
            std::collections::HashMap::new();
        let mut saw_quiet = false;

        for event in events {
            if let InputEvent::Data {
                bytes, source_id, ..
            } = event
                && let Some(source_id) = source_id
            {
                if bytes
                    .windows(quiet_payload.len())
                    .any(|w| w == quiet_payload)
                {
                    saw_quiet = true;
                }
                *bytes_by_source.entry(source_id).or_insert(0) += bytes.len();
            }
        }

        assert!(saw_quiet, "quiet client should make progress in same poll");
        assert!(
            bytes_by_source.len() >= 2,
            "expected data from both quiet and noisy connections"
        );
        let max_forwarded = bytes_by_source.values().copied().max().unwrap_or(0);
        assert!(
            max_forwarded > quiet_payload.len(),
            "a larger noisy payload should also be forwarded"
        );
        assert!(
            max_forwarded <= MAX_BYTES_PER_CLIENT_PER_POLL,
            "a client's forwarded payload should respect per-poll bounded reads"
        );
    }
}

#[cfg(kani)]
mod verification {
    use super::{
        MAX_BYTES_PER_CLIENT_PER_POLL, MAX_READS_PER_CLIENT_PER_POLL, should_stop_client_read,
    };

    #[kani::proof]
    fn verify_zero_counters_do_not_stop_client_read() {
        assert!(!should_stop_client_read(0, 0));
        kani::cover!(
            !should_stop_client_read(0, 0),
            "non-stopping path is reachable"
        );
    }

    #[kani::proof]
    fn verify_each_limit_independently_stops_client_read() {
        let reads_done = kani::any::<usize>();
        assert!(should_stop_client_read(
            MAX_BYTES_PER_CLIENT_PER_POLL,
            reads_done
        ));
        kani::cover!(
            should_stop_client_read(MAX_BYTES_PER_CLIENT_PER_POLL, 0),
            "byte-cap stop path is reachable"
        );

        let bytes_read = kani::any::<usize>();
        assert!(should_stop_client_read(
            bytes_read,
            MAX_READS_PER_CLIENT_PER_POLL
        ));
        kani::cover!(
            should_stop_client_read(0, MAX_READS_PER_CLIENT_PER_POLL),
            "read-cap stop path is reachable"
        );
    }

    #[kani::proof]
    fn verify_stop_predicate_equivalence() {
        let bytes_read = kani::any::<usize>();
        let reads_done = kani::any::<usize>();
        let expected = bytes_read >= MAX_BYTES_PER_CLIENT_PER_POLL
            || reads_done >= MAX_READS_PER_CLIENT_PER_POLL;
        assert_eq!(should_stop_client_read(bytes_read, reads_done), expected);
        kani::cover!(
            should_stop_client_read(MAX_BYTES_PER_CLIENT_PER_POLL, 0),
            "stop branch reachable"
        );
        kani::cover!(!should_stop_client_read(0, 0), "continue branch reachable");
    }
}

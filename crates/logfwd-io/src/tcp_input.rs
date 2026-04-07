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

/// Maximum bytes a client may send without a newline before we disconnect them.
/// Prevents a misbehaving sender from consuming unbounded memory.
const MAX_LINE_LENGTH: usize = 1024 * 1024; // 1 MiB

/// Maximum total bytes buffered across all client `client_data` vecs within a
/// single `poll` call.  When this budget is exhausted we stop reading from
/// further clients in that poll, deferring them to the next call.  This
/// propagates TCP backpressure to senders and prevents OOM when many clients
/// flood data faster than the pipeline can drain it (fix for #576).
const MAX_TOTAL_BUFFERED_BYTES: usize = 256 * 1024 * 1024; // 256 MiB

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
    /// Bytes received since the last newline. Reset to 0 on every `\n`.
    bytes_since_newline: usize,
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
}

impl TcpInput {
    /// Bind to `addr` (e.g. "0.0.0.0:5140") with the default idle timeout.
    pub fn new(name: impl Into<String>, addr: &str) -> io::Result<Self> {
        Self::with_idle_timeout(name, addr, DEFAULT_IDLE_TIMEOUT)
    }

    /// Bind to `addr` with a custom idle timeout.
    pub fn with_idle_timeout(
        name: impl Into<String>,
        addr: &str,
        idle_timeout: Duration,
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
                    self.clients.push(Client {
                        stream,
                        source_id: sid,
                        last_data: Instant::now(),
                        bytes_since_newline: 0,
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
            loop {
                match client.stream.read(&mut self.buf) {
                    Ok(0) => {
                        // Clean EOF.
                        alive[i] = false;
                        break;
                    }
                    Ok(n) => {
                        let chunk = &self.buf[..n];
                        client.last_data = now;
                        got_data = true;

                        // Track bytes since last newline for max-line-length.
                        // Check BEFORE resetting at newline to catch lines that
                        // were already over the limit when the newline arrives.
                        if let Some(first_nl) = memchr::memchr(b'\n', chunk) {
                            // Line length = accumulated + bytes up to first \n.
                            let line_len = client.bytes_since_newline + first_nl;
                            if line_len >= MAX_LINE_LENGTH {
                                alive[i] = false;
                                break;
                            }
                            // Reset to bytes after the LAST newline in this chunk.
                            if let Some(last_nl) = memchr::memrchr(b'\n', chunk) {
                                client.bytes_since_newline = n - last_nl - 1;
                            }
                        } else {
                            client.bytes_since_newline += n;
                            if client.bytes_since_newline >= MAX_LINE_LENGTH {
                                alive[i] = false;
                                break;
                            }
                        }
                        client_data[i]
                            .get_or_insert_with(Vec::new)
                            .extend_from_slice(chunk);
                        total_buffered += n;

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
                    events.push(InputEvent::Data {
                        bytes,
                        source_id: Some(self.clients[i].source_id),
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
        for (i, &is_alive) in alive.iter().enumerate() {
            if !is_alive {
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
    use std::io::Write;
    use std::net::TcpStream as StdTcpStream;

    #[test]
    fn receives_tcp_data() {
        let input = TcpInput::new("test", "127.0.0.1:0").unwrap();
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
        if let InputEvent::Data { bytes, source_id } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            assert!(text.contains("hello"), "got: {text}");
            assert!(text.contains("world"), "got: {text}");
            assert!(source_id.is_some(), "TCP data must have a source_id");
        }
    }

    #[test]
    fn handles_disconnect() {
        let mut input = TcpInput::new("test", "127.0.0.1:0").unwrap();
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
        let mut input = TcpInput::new("test", "127.0.0.1:0").unwrap();
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
        let mut input = TcpInput::new("test", "127.0.0.1:0").unwrap();
        let addr = input.local_addr().unwrap();

        {
            let mut client = StdTcpStream::connect(addr).unwrap();
            // Intentionally no trailing newline — this is the partial line.
            client.write_all(b"partial line without newline").unwrap();
            client.flush().unwrap();
        } // client drops here -> EOF

        std::thread::sleep(Duration::from_millis(50));

        let events = input.poll().unwrap();

        let has_data = events
            .iter()
            .any(|e| matches!(e, InputEvent::Data { bytes, .. } if !bytes.is_empty()));
        let has_eof = events
            .iter()
            .any(|e| matches!(e, InputEvent::EndOfFile { source_id } if source_id.is_some()));

        assert!(has_data, "should have received the partial line bytes");
        assert!(
            has_eof,
            "should emit EndOfFile on disconnect so FramedInput can flush the partial line"
        );

        // EndOfFile source_id must match the Data source_id.
        let data_sid = events.iter().find_map(|e| {
            if let InputEvent::Data { source_id, .. } = e {
                *source_id
            } else {
                None
            }
        });
        let eof_sid = events.iter().find_map(|e| {
            if let InputEvent::EndOfFile { source_id } = e {
                *source_id
            } else {
                None
            }
        });
        assert_eq!(
            data_sid, eof_sid,
            "Data and EndOfFile must carry the same SourceId"
        );
    }

    #[test]
    fn tcp_idle_timeout() {
        // Use a very short idle timeout so the test runs fast.
        let mut input =
            TcpInput::with_idle_timeout("test", "127.0.0.1:0", Duration::from_millis(200)).unwrap();
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
        let mut input = TcpInput::new("test", "127.0.0.1:0").unwrap();
        let addr = input.local_addr().unwrap();

        // Spawn the writer in a background thread because write_all of >1MB
        // will block until the reader drains the kernel buffer.
        let writer = std::thread::spawn(move || {
            let mut client = StdTcpStream::connect(addr).unwrap();
            let big = vec![b'A'; MAX_LINE_LENGTH + 1];
            // Ignore errors — the server may reset the connection once
            // the limit is exceeded, causing a broken-pipe on our side.
            let _ = client.write_all(&big);
        });

        // Poll until the writer finishes (connection accepted and data read) or
        // deadline. The accept and disconnect may happen in the same poll() call
        // when the data arrives faster than the poll loop iterates, so we track
        // total connections_accepted() rather than the transient client_count().
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            let _ = input.poll().unwrap();
            if input.connections_accepted() > 0 && input.client_count() == 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        let _ = writer.join();

        assert!(
            input.connections_accepted() > 0,
            "server must have accepted the connection"
        );
        assert_eq!(
            input.client_count(),
            0,
            "client exceeding max_line_length should be disconnected"
        );
    }

    #[test]
    fn tcp_max_line_length_exact_boundary() {
        // A line of exactly MAX_LINE_LENGTH bytes (content only, excluding \n)
        // must be rejected — the check is `>=`, so the boundary is exclusive.
        let mut input = TcpInput::new("test", "127.0.0.1:0").unwrap();
        let addr = input.local_addr().unwrap();

        let writer = std::thread::spawn(move || {
            let mut client = StdTcpStream::connect(addr).unwrap();
            // Exactly MAX_LINE_LENGTH content bytes followed by a newline.
            let mut line = vec![b'A'; MAX_LINE_LENGTH];
            line.push(b'\n');
            let _ = client.write_all(&line);
        });

        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            let _ = input.poll().unwrap();
            if input.connections_accepted() > 0 && input.client_count() == 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        let _ = writer.join();

        assert!(
            input.connections_accepted() > 0,
            "server must have accepted the connection"
        );
        assert_eq!(
            input.client_count(),
            0,
            "client sending a line of exactly MAX_LINE_LENGTH bytes should be disconnected"
        );
    }

    #[test]
    fn tcp_connection_storm() {
        let mut input = TcpInput::new("test", "127.0.0.1:0").unwrap();
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

    /// Two concurrent TCP connections must receive distinct `SourceId` values
    /// so that `FramedInput` can track per-connection remainders independently.
    #[test]
    fn distinct_source_ids_per_connection() {
        let mut input = TcpInput::new("test", "127.0.0.1:0").unwrap();
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
}

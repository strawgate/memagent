//! TCP input source. Listens on a TCP socket and produces newline-delimited
//! log lines from connected clients. Multiple concurrent connections supported.

use std::io::{self, Read};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use socket2::SockRef;

use crate::input::{InputEvent, InputSource};

/// Maximum number of concurrent TCP client connections.
const MAX_CLIENTS: usize = 1024;

/// Per-poll read buffer size (64 KiB). Shared across all connections within a
/// single `poll` call; data is copied into `all_data` immediately, so one
/// moderate buffer is sufficient.
const READ_BUF_SIZE: usize = 64 * 1024;

/// Default disconnect timeout for idle clients (no data received).
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Maximum bytes a client may send without a newline before we disconnect them.
/// Prevents a misbehaving sender from consuming unbounded memory.
const MAX_LINE_LENGTH: usize = 1024 * 1024; // 1 MiB

/// A connected TCP client with an associated last-data timestamp.
struct Client {
    stream: TcpStream,
    last_data: Instant,
    /// Bytes received since the last newline. Reset to 0 on every `\n`.
    bytes_since_newline: usize,
}

/// TCP input that accepts connections and reads newline-delimited data.
pub struct TcpInput {
    name: String,
    listener: TcpListener,
    clients: Vec<Client>,
    buf: Vec<u8>,
    idle_timeout: Duration,
    /// Total connections accepted since creation (never decreases).
    connections_accepted: u64,
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
        // Accept new connections up to the limit.
        loop {
            if self.clients.len() >= MAX_CLIENTS {
                // Drain (and drop) any pending connections beyond the limit so
                // the kernel accept queue does not fill up and stall.
                match self.listener.accept() {
                    Ok((_stream, _addr)) => continue, // dropped immediately
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

                    self.connections_accepted += 1;
                    self.clients.push(Client {
                        stream,
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

        // Read from all clients.
        let mut all_data = Vec::new();
        let now = Instant::now();
        // Track which connections are dead using a bitmap for O(1) lookup.
        let mut alive = vec![true; self.clients.len()];

        for (i, client) in self.clients.iter_mut().enumerate() {
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
                        all_data.extend_from_slice(chunk);
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

        // Remove dead connections, preserving order of remaining ones.
        // `retain` is cleaner than manual swap_remove with index tracking.
        let mut idx = 0;
        self.clients.retain(|_| {
            let keep = alive[idx];
            idx += 1;
            keep
        });

        let mut events = Vec::new();
        if !all_data.is_empty() {
            events.push(InputEvent::Data { bytes: all_data });
        }

        Ok(events)
    }

    fn name(&self) -> &str {
        &self.name
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

        std::thread::sleep(std::time::Duration::from_millis(50));

        let mut input = input; // make mutable
        let events = input.poll().unwrap();

        // Should have accepted the connection and read data.
        assert_eq!(events.len(), 1);
        if let InputEvent::Data { bytes } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            assert!(text.contains("hello"), "got: {text}");
            assert!(text.contains("world"), "got: {text}");
        }
    }

    #[test]
    fn handles_disconnect() {
        let mut input = TcpInput::new("test", "127.0.0.1:0").unwrap();
        let addr = input.listener.local_addr().unwrap();

        {
            let mut client = StdTcpStream::connect(addr).unwrap();
            client.write_all(b"line1\n").unwrap();
        } // client drops here → connection closed

        std::thread::sleep(std::time::Duration::from_millis(50));

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);

        // Second poll should clean up the closed connection.
        let events = input.poll().unwrap();
        assert!(events.is_empty());
        assert!(input.clients.is_empty());
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
}

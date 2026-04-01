//! TCP input source. Listens on a TCP socket and produces newline-delimited
//! log lines from connected clients. Multiple concurrent connections supported.

use std::io::{self, Read};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use crate::input::{InputEvent, InputSource};

/// Maximum number of concurrent TCP client connections.
const MAX_CLIENTS: usize = 1024;

/// Per-poll read buffer size (64 KiB). Shared across all connections within a
/// single `poll` call; data is copied into `all_data` immediately, so one
/// moderate buffer is sufficient.
const READ_BUF_SIZE: usize = 64 * 1024;

/// Disconnect clients that have been idle longer than this.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// A connected TCP client with an associated last-data timestamp.
struct Client {
    stream: TcpStream,
    last_data: Instant,
}

/// TCP input that accepts connections and reads newline-delimited data.
pub struct TcpInput {
    name: String,
    listener: TcpListener,
    clients: Vec<Client>,
    buf: Vec<u8>,
}

impl TcpInput {
    /// Bind to `addr` (e.g. "0.0.0.0:5140").
    pub fn new(name: impl Into<String>, addr: &str) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            name: name.into(),
            listener,
            clients: Vec::new(),
            buf: vec![0u8; READ_BUF_SIZE],
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
                    let sock2 = socket2::Socket::from(stream.try_clone()?);
                    let _ = sock2.set_keepalive(true);
                    let keepalive = socket2::TcpKeepalive::new()
                        .with_time(Duration::from_secs(60))
                        .with_interval(Duration::from_secs(10));
                    let _ = sock2.set_tcp_keepalive(&keepalive);
                    // Don't close the fd — the original `stream` still owns it.
                    std::mem::forget(sock2);

                    self.clients.push(Client {
                        stream,
                        last_data: Instant::now(),
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
            // Disconnect clients that have been idle too long.
            if now.duration_since(client.last_data) > IDLE_TIMEOUT {
                alive[i] = false;
                continue;
            }

            loop {
                match client.stream.read(&mut self.buf) {
                    Ok(0) => {
                        // Clean EOF.
                        alive[i] = false;
                        break;
                    }
                    Ok(n) => {
                        all_data.extend_from_slice(&self.buf[..n]);
                        client.last_data = now;
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
}

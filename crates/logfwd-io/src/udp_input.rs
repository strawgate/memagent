//! UDP input source. Listens on a UDP socket and produces one InputEvent
//! per received datagram (or batch of datagrams).

use std::io;
use std::net::UdpSocket;

use socket2::{Domain, Protocol, Socket, Type};

use crate::input::{InputEvent, InputSource};

/// Maximum UDP payload: 65535 (IP max) - 20 (IP header) - 8 (UDP header).
const MAX_UDP_PAYLOAD: usize = 65507;

/// Desired kernel receive buffer size (8 MiB). Set best-effort — the OS may
/// cap it lower depending on `sysctl net.core.rmem_max`.
const RECV_BUF_SIZE: usize = 8 * 1024 * 1024;

/// UDP input that listens for datagrams. Each datagram is treated as one
/// or more newline-delimited log lines.
pub struct UdpInput {
    name: String,
    socket: UdpSocket,
    buf: Vec<u8>,
}

impl UdpInput {
    /// Bind to `addr` (e.g. "0.0.0.0:514" for syslog).
    pub fn new(name: impl Into<String>, addr: &str) -> io::Result<Self> {
        // Use socket2 to create the socket so we can tune SO_RCVBUF *before*
        // any datagrams arrive.
        let sock2 = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        // Tune kernel receive buffer to reduce packet loss under load.
        let _ = sock2.set_recv_buffer_size(RECV_BUF_SIZE); // best-effort
        sock2.set_nonblocking(true)?;
        sock2.bind(
            &addr
                .parse::<std::net::SocketAddr>()
                .map_err(io::Error::other)?
                .into(),
        )?;
        let socket: UdpSocket = sock2.into();

        Ok(Self {
            name: name.into(),
            socket,
            buf: vec![0u8; MAX_UDP_PAYLOAD],
        })
    }

    /// Returns the local address this socket is bound to.
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.socket.local_addr()
    }
}

impl InputSource for UdpInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        // Accumulate into a single byte buffer; avoid per-datagram Vec alloc.
        // We re-use `self.buf` for recv and build the output in a separate vec
        // only when data actually arrives.
        let mut total: Option<Vec<u8>> = None;

        // Drain all available datagrams in one poll cycle.
        loop {
            // `recv` is cheaper than `recv_from` — we don't need the source addr.
            match self.socket.recv(&mut self.buf) {
                Ok(0) => {} // no data in this datagram, loop again
                Ok(n) => {
                    let data = &self.buf[..n];
                    let out = total.get_or_insert_with(|| Vec::with_capacity(4096));
                    out.extend_from_slice(data);
                    // Ensure newline termination so the scanner always sees
                    // complete lines, even if the sender omitted a trailing LF.
                    if !data.ends_with(b"\n") {
                        out.push(b'\n');
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                // ECONNREFUSED can arrive on a connected UDP socket (ICMP
                // port-unreachable cached by the kernel). Treat it like
                // WouldBlock — there is simply no data right now.
                Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => break,
                Err(e) => return Err(e),
            }
        }

        match total {
            Some(bytes) => Ok(vec![InputEvent::Data { bytes }]),
            None => Ok(Vec::new()),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::UdpSocket as StdSocket;

    #[test]
    fn receives_datagrams() {
        let mut input = UdpInput::new("test", "127.0.0.1:0").unwrap();
        let addr = input.socket.local_addr().unwrap();

        let sender = StdSocket::bind("127.0.0.1:0").unwrap();
        sender.send_to(b"hello world\n", addr).unwrap();
        sender.send_to(b"second line\n", addr).unwrap();

        // Give the OS a moment to deliver.
        std::thread::sleep(std::time::Duration::from_millis(50));

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        if let InputEvent::Data { bytes } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            assert!(text.contains("hello world"), "got: {text}");
            assert!(text.contains("second line"), "got: {text}");
        }
    }

    #[test]
    fn adds_trailing_newline_to_bare_datagram() {
        let mut input = UdpInput::new("test", "127.0.0.1:0").unwrap();
        let addr = input.socket.local_addr().unwrap();

        let sender = StdSocket::bind("127.0.0.1:0").unwrap();
        // No trailing newline — input must add one.
        sender.send_to(b"no newline", addr).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        if let InputEvent::Data { bytes } = &events[0] {
            assert!(bytes.ends_with(b"\n"), "expected trailing newline");
        }
    }

    #[test]
    fn handles_multi_line_datagram() {
        let mut input = UdpInput::new("test", "127.0.0.1:0").unwrap();
        let addr = input.socket.local_addr().unwrap();

        let sender = StdSocket::bind("127.0.0.1:0").unwrap();
        sender.send_to(b"line1\nline2\nline3\n", addr).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
        if let InputEvent::Data { bytes } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            assert_eq!(text.matches('\n').count(), 3);
        }
    }

    #[test]
    fn empty_when_no_data() {
        let mut input = UdpInput::new("test", "127.0.0.1:0").unwrap();
        let events = input.poll().unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn buffer_is_max_udp_payload_size() {
        let input = UdpInput::new("test", "127.0.0.1:0").unwrap();
        assert_eq!(input.buf.len(), 65507);
    }
}

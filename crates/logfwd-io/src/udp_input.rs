//! UDP input source. Listens on a UDP socket and produces one InputEvent
//! per received datagram (or batch of datagrams).

use std::io;
use std::net::UdpSocket;

use crate::input::{InputEvent, InputSource};

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
        let socket = UdpSocket::bind(addr)?;
        // Non-blocking so poll() returns immediately when no data.
        socket.set_nonblocking(true)?;
        Ok(Self {
            name: name.into(),
            socket,
            buf: vec![0u8; 65536], // max UDP datagram
        })
    }
}

impl InputSource for UdpInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let mut events = Vec::new();
        let mut total = Vec::new();

        // Drain all available datagrams in one poll cycle.
        loop {
            match self.socket.recv_from(&mut self.buf) {
                Ok((n, _addr)) => {
                    total.extend_from_slice(&self.buf[..n]);
                    // Ensure newline termination.
                    if !total.ends_with(b"\n") {
                        total.push(b'\n');
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }

        if !total.is_empty() {
            events.push(InputEvent::Data { bytes: total });
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
    fn empty_when_no_data() {
        let mut input = UdpInput::new("test", "127.0.0.1:0").unwrap();
        let events = input.poll().unwrap();
        assert!(events.is_empty());
    }
}

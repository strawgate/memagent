//! TCP input source. Listens on a TCP socket and produces newline-delimited
//! log lines from connected clients. Multiple concurrent connections supported.

use std::io::{self, Read};
use std::net::{TcpListener, TcpStream};

use crate::input::{InputEvent, InputSource};

/// TCP input that accepts connections and reads newline-delimited data.
pub struct TcpInput {
    name: String,
    listener: TcpListener,
    clients: Vec<TcpStream>,
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
            buf: vec![0u8; 256 * 1024],
        })
    }
}

impl InputSource for TcpInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        // Accept new connections.
        loop {
            match self.listener.accept() {
                Ok((stream, _addr)) => {
                    stream.set_nonblocking(true)?;
                    self.clients.push(stream);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }

        // Read from all clients.
        let mut all_data = Vec::new();
        let mut closed = Vec::new();

        for (i, client) in self.clients.iter_mut().enumerate() {
            loop {
                match client.read(&mut self.buf) {
                    Ok(0) => {
                        closed.push(i);
                        break;
                    }
                    Ok(n) => {
                        all_data.extend_from_slice(&self.buf[..n]);
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(_) => {
                        closed.push(i);
                        break;
                    }
                }
            }
        }

        // Remove closed connections (reverse order to preserve indices).
        for &i in closed.iter().rev() {
            self.clients.swap_remove(i);
        }

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

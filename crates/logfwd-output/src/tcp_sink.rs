//! TCP output sink — connects to a TCP endpoint and sends newline-delimited
//! JSON lines. Reconnects on failure with a single retry per `send_batch` call.

use std::io::{self, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;

use logfwd_io::diagnostics::ComponentStats;

#[allow(deprecated)]
use crate::{BatchMetadata, OutputSink, build_col_infos, write_row_json};

/// Connect timeout. Kept short so the pipeline doesn't stall when the
/// downstream is unreachable.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Write timeout per `write_all` call.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

pub struct TcpSink {
    name: String,
    addr: String,
    stream: Option<TcpStream>,
    buf: Vec<u8>,
    stats: Arc<ComponentStats>,
}

impl TcpSink {
    pub fn new(
        name: impl Into<String>,
        addr: impl Into<String>,
        stats: Arc<ComponentStats>,
    ) -> Self {
        Self {
            name: name.into(),
            addr: addr.into(),
            stream: None,
            buf: Vec::with_capacity(64 * 1024),
            stats,
        }
    }

    /// Open a new connection, replacing any existing one.
    fn connect(&mut self) -> io::Result<()> {
        // Resolve the address ourselves so `connect_timeout` works with a
        // `SocketAddr` (it requires `SocketAddr`, not a string).
        use std::net::ToSocketAddrs;
        let sock_addr = self.addr.to_socket_addrs()?.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "address resolved to nothing")
        })?;

        let stream = TcpStream::connect_timeout(&sock_addr, CONNECT_TIMEOUT)?;
        stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
        stream.set_nodelay(true)?;
        self.stream = Some(stream);
        Ok(())
    }

    /// Ensure we have a live connection, connecting lazily if needed.
    fn ensure_connected(&mut self) -> io::Result<()> {
        if self.stream.is_some() {
            return Ok(());
        }
        self.connect()
    }

    /// Write the buffer to the stream. On failure, drop the connection and
    /// retry exactly once with a fresh connection.
    fn write_with_retry(&mut self) -> io::Result<()> {
        self.ensure_connected()?;

        // First attempt.
        if let Some(ref mut stream) = self.stream {
            match stream.write_all(&self.buf) {
                Ok(()) => return Ok(()),
                Err(_) => {
                    // Connection went bad — drop it and retry below.
                    self.stream = None;
                }
            }
        }

        // Single retry with a fresh connection.
        self.connect()?;
        if let Some(ref mut stream) = self.stream {
            if let Err(e) = stream.write_all(&self.buf) {
                self.stream = None;
                return Err(e);
            }
        }
        Ok(())
    }
}

#[allow(deprecated)]
impl OutputSink for TcpSink {
    fn send_batch(&mut self, batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        self.buf.clear();
        let cols = build_col_infos(batch);
        for row in 0..batch.num_rows() {
            write_row_json(batch, row, &cols, &mut self.buf)?;
            self.buf.push(b'\n');
        }

        self.write_with_retry()?;
        self.stats.inc_lines(batch.num_rows() as u64);
        self.stats.inc_bytes(self.buf.len() as u64);
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(ref mut stream) = self.stream {
            stream.flush()?;
        }
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

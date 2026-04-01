//! TCP output sink — connects to a TCP endpoint and sends newline-delimited
//! JSON lines. Reconnects on failure.

use std::io::{self, Write};
use std::net::TcpStream;
use std::time::Duration;

use arrow::record_batch::RecordBatch;

use crate::{BatchMetadata, OutputSink, build_col_infos, write_row_json};

pub struct TcpSink {
    name: String,
    addr: String,
    stream: Option<TcpStream>,
    buf: Vec<u8>,
}

impl TcpSink {
    pub fn new(name: impl Into<String>, addr: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            addr: addr.into(),
            stream: None,
            buf: Vec::with_capacity(64 * 1024),
        }
    }

    fn ensure_connected(&mut self) -> io::Result<()> {
        if self.stream.is_some() {
            return Ok(());
        }
        let stream = TcpStream::connect(&self.addr)?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;
        stream.set_nodelay(true)?;
        self.stream = Some(stream);
        Ok(())
    }
}

impl OutputSink for TcpSink {
    fn send_batch(&mut self, batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        self.buf.clear();
        let cols = build_col_infos(batch);
        for row in 0..batch.num_rows() {
            write_row_json(batch, row, &cols, &mut self.buf);
            self.buf.push(b'\n');
        }

        if let Err(e) = self.ensure_connected() {
            // Drop stale connection and retry once.
            self.stream = None;
            self.ensure_connected().map_err(|_| e)?;
        }

        if let Some(ref mut stream) = self.stream {
            if let Err(e) = stream.write_all(&self.buf) {
                self.stream = None; // force reconnect next time
                return Err(e);
            }
        }
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

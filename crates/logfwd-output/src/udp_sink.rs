//! UDP output sink — sends newline-delimited JSON lines as UDP datagrams.

use std::io;
use std::net::UdpSocket;

use arrow::record_batch::RecordBatch;

use crate::{BatchMetadata, OutputSink, build_col_infos, write_row_json};

pub struct UdpSink {
    name: String,
    socket: UdpSocket,
    target: String,
    buf: Vec<u8>,
}

impl UdpSink {
    pub fn new(name: impl Into<String>, target: impl Into<String>) -> io::Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        Ok(Self {
            name: name.into(),
            target: target.into(),
            socket,
            buf: Vec::with_capacity(64 * 1024),
        })
    }
}

impl OutputSink for UdpSink {
    fn send_batch(&mut self, batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let cols = build_col_infos(batch);
        for row in 0..batch.num_rows() {
            self.buf.clear();
            write_row_json(batch, row, &cols, &mut self.buf);
            self.buf.push(b'\n');
            // Send each line as a separate datagram.
            self.socket.send_to(&self.buf, &self.target)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

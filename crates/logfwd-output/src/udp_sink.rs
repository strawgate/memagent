//! UDP output sink — sends newline-delimited JSON lines as UDP datagrams.
//!
//! Rows are batched into datagrams up to `MAX_DATAGRAM_PAYLOAD` bytes to
//! amortize per-packet overhead. Each datagram contains one or more
//! newline-terminated JSON lines.

use std::io;
use std::net::UdpSocket;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use logfwd_io::diagnostics::ComponentStats;

#[allow(deprecated)]
use crate::{BatchMetadata, OutputSink, build_col_infos, write_row_json};

/// Safe MTU-friendly payload limit. Ethernet MTU is 1500; minus 20 (IP) and
/// 8 (UDP) headers leaves 1472. We use 1400 to leave room for IP options,
/// tunneling headers (VXLAN, GRE), etc.
const MAX_DATAGRAM_PAYLOAD: usize = 1400;

pub struct UdpSink {
    name: String,
    socket: UdpSocket,
    target: String,
    /// Scratch buffer for serializing a single row before deciding whether
    /// it fits in the current datagram.
    row_buf: Vec<u8>,
    /// Accumulation buffer for the current datagram being assembled.
    dgram_buf: Vec<u8>,
    stats: Arc<ComponentStats>,
}

impl UdpSink {
    pub fn new(
        name: impl Into<String>,
        target: impl Into<String>,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        Ok(Self {
            name: name.into(),
            target: target.into(),
            socket,
            row_buf: Vec::with_capacity(2048),
            dgram_buf: Vec::with_capacity(MAX_DATAGRAM_PAYLOAD),
            stats,
        })
    }

    /// Send the current datagram buffer if non-empty, then clear it.
    /// ECONNREFUSED (ICMP port-unreachable) is silently ignored because UDP
    /// is fire-and-forget — the destination may come back later.
    fn flush_dgram(&mut self) -> io::Result<()> {
        if self.dgram_buf.is_empty() {
            return Ok(());
        }
        match self.socket.send_to(&self.dgram_buf, &self.target) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                // Silently drop — UDP is best-effort.
            }
            Err(e) => return Err(e),
        }
        self.dgram_buf.clear();
        Ok(())
    }
}

#[allow(deprecated)]
impl OutputSink for UdpSink {
    fn send_batch(&mut self, batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let cols = build_col_infos(batch);
        let mut total_bytes: u64 = 0;

        for row in 0..batch.num_rows() {
            // Serialize row into scratch buffer.
            self.row_buf.clear();
            write_row_json(batch, row, &cols, &mut self.row_buf)?;
            self.row_buf.push(b'\n');

            let row_len = self.row_buf.len();
            total_bytes += row_len as u64;

            // If this single row exceeds the MTU, send it alone as an
            // oversized datagram (up to 65507). The network may fragment it
            // but that is better than silently dropping data.
            if row_len > MAX_DATAGRAM_PAYLOAD {
                self.flush_dgram()?;
                match self.socket.send_to(&self.row_buf, &self.target) {
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {}
                    Err(e) => return Err(e),
                }
                continue;
            }

            // Would adding this row overflow the current datagram?
            if self.dgram_buf.len() + row_len > MAX_DATAGRAM_PAYLOAD {
                self.flush_dgram()?;
            }

            self.dgram_buf.extend_from_slice(&self.row_buf);
        }

        // Flush any remaining rows.
        self.flush_dgram()?;
        self.stats.inc_lines(batch.num_rows() as u64);
        self.stats.inc_bytes(total_bytes);
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_dgram()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use logfwd_io::diagnostics::ComponentStats;
    use std::net::UdpSocket as StdSocket;
    use std::sync::Arc;

    fn make_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "msg_str",
            DataType::Utf8,
            false,
        )]));
        let values: Vec<String> = (0..n).map(|i| format!("row-{i}")).collect();
        let refs: Vec<&str> = values.iter().map(String::as_str).collect();
        let array = StringArray::from(refs);
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn batches_rows_into_single_datagram() {
        let receiver = StdSocket::bind("127.0.0.1:0").unwrap();
        let addr = receiver.local_addr().unwrap();
        receiver.set_nonblocking(true).unwrap();

        let mut sink =
            UdpSink::new("test", addr.to_string(), Arc::new(ComponentStats::new())).unwrap();
        let batch = make_batch(5);
        let meta = BatchMetadata {
            resource_attrs: Arc::default(),
            observed_time_ns: 0,
        };
        sink.send_batch(&batch, &meta).unwrap();

        // Give OS a moment to deliver.
        std::thread::sleep(std::time::Duration::from_millis(50));

        // All 5 small rows should fit in one datagram.
        let mut buf = [0u8; 65507];
        let n = receiver.recv(&mut buf).unwrap();
        let text = std::str::from_utf8(&buf[..n]).unwrap();
        assert_eq!(
            text.lines().count(),
            5,
            "expected 5 lines in one datagram, got: {text}"
        );

        // No second datagram.
        assert!(receiver.recv(&mut buf).is_err());
    }

    #[test]
    fn splits_when_exceeding_mtu() {
        let receiver = StdSocket::bind("127.0.0.1:0").unwrap();
        let addr = receiver.local_addr().unwrap();
        receiver.set_nonblocking(true).unwrap();

        let mut sink =
            UdpSink::new("test", addr.to_string(), Arc::new(ComponentStats::new())).unwrap();
        // Create enough rows that they cannot all fit in 1400 bytes.
        let batch = make_batch(100);
        let meta = BatchMetadata {
            resource_attrs: Arc::default(),
            observed_time_ns: 0,
        };
        sink.send_batch(&batch, &meta).unwrap();

        // Should receive multiple datagrams, each <= 1400 bytes.
        let mut buf = [0u8; 65507];
        let mut total_lines = 0;
        let mut dgram_count = 0;

        // Give OS a moment.
        std::thread::sleep(std::time::Duration::from_millis(50));

        while let Ok(n) = receiver.recv(&mut buf) {
            assert!(n <= MAX_DATAGRAM_PAYLOAD, "datagram too large: {n}");
            let text = std::str::from_utf8(&buf[..n]).unwrap();
            total_lines += text.lines().count();
            dgram_count += 1;
        }

        assert_eq!(total_lines, 100);
        assert!(
            dgram_count > 1,
            "expected multiple datagrams, got {dgram_count}"
        );
    }
}

//! UDP output sink — sends newline-delimited JSON lines as UDP datagrams.
//!
//! Rows are batched into datagrams up to `MAX_DATAGRAM_PAYLOAD` bytes to
//! amortize per-packet overhead. Each datagram contains one or more
//! newline-terminated JSON lines.
//!
//! Uses `tokio::net::UdpSocket` for non-blocking async I/O.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use tokio::net::UdpSocket;

use logfwd_types::diagnostics::ComponentStats;

use crate::sink::{SendResult, Sink, SinkFactory};
use crate::{BatchMetadata, build_col_infos, write_row_json};

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
    /// How many rows are currently buffered in `dgram_buf`.
    dgram_rows: u64,
    stats: Arc<ComponentStats>,
}

impl UdpSink {
    /// Create a new UDP sink.
    ///
    /// Binds an outbound UDP socket and defers target DNS resolution until send
    /// time so startup does not block on or fail due to transient DNS issues.
    pub fn new(
        name: impl Into<String>,
        target: impl Into<String>,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        let std_socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
        std_socket.set_nonblocking(true)?;
        let socket = UdpSocket::from_std(std_socket)?;
        Ok(Self {
            name: name.into(),
            socket,
            target: target.into(),
            row_buf: Vec::with_capacity(2048),
            dgram_buf: Vec::with_capacity(MAX_DATAGRAM_PAYLOAD),
            dgram_rows: 0,
            stats,
        })
    }

    async fn send_packet(&self, payload: &[u8]) -> io::Result<()> {
        let addr = tokio::net::lookup_host(&self.target)
            .await?
            .find(std::net::SocketAddr::is_ipv4)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::AddrNotAvailable,
                    format!("DNS lookup returned no IPv4 addresses for {}", self.target),
                )
            })?;
        match self.socket.send_to(payload, addr).await {
            Ok(n) if n == payload.len() => Ok(()),
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "UDP datagram was only partially sent",
            )),
            Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Send the current datagram buffer if non-empty, then clear it.
    /// ECONNREFUSED (ICMP port-unreachable) is silently ignored because UDP
    /// is fire-and-forget — the destination may come back later.
    async fn flush_dgram(&mut self) -> io::Result<()> {
        if self.dgram_buf.is_empty() {
            return Ok(());
        }
        let len = self.dgram_buf.len() as u64;
        self.send_packet(&self.dgram_buf).await?;
        self.stats.inc_lines(self.dgram_rows);
        self.stats.inc_bytes(len);
        self.dgram_buf.clear();
        self.dgram_rows = 0;
        Ok(())
    }

    /// Serialize and send a batch of log records as UDP datagrams.
    async fn do_send_batch(&mut self, batch: &RecordBatch) -> io::Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let cols = build_col_infos(batch);

        for row in 0..batch.num_rows() {
            // Serialize row into scratch buffer.
            self.row_buf.clear();
            write_row_json(batch, row, &cols, &mut self.row_buf)?;
            self.row_buf.push(b'\n');

            let row_len = self.row_buf.len();

            // If this single row exceeds the MTU, send it alone as an
            // oversized datagram (up to 65507). The network may fragment it
            // but that is better than silently dropping data.
            if row_len > MAX_DATAGRAM_PAYLOAD {
                self.flush_dgram().await?;
                self.send_packet(&self.row_buf).await?;
                self.stats.inc_lines(1);
                self.stats.inc_bytes(row_len as u64);
                continue;
            }

            // Would adding this row overflow the current datagram?
            if self.dgram_buf.len() + row_len > MAX_DATAGRAM_PAYLOAD {
                self.flush_dgram().await?;
            }

            self.dgram_buf.extend_from_slice(&self.row_buf);
            self.dgram_rows += 1;
        }

        // Flush any remaining rows.
        self.flush_dgram().await?;
        Ok(())
    }
}

impl Sink for UdpSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        _metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        Box::pin(async move {
            match self.do_send_batch(batch).await {
                Ok(()) => SendResult::Ok,
                Err(e) => SendResult::from_io_error(e),
            }
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move { self.flush_dgram().await })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move { self.flush_dgram().await })
    }
}

// ---------------------------------------------------------------------------
// UdpSinkFactory
// ---------------------------------------------------------------------------

/// Factory for creating [`UdpSink`] instances.
///
/// Each call to `create()` binds a fresh UDP socket on an ephemeral port.
pub struct UdpSinkFactory {
    name: String,
    target: String,
    stats: Arc<ComponentStats>,
}

impl UdpSinkFactory {
    /// Create a new factory.
    pub fn new(name: String, target: String, stats: Arc<ComponentStats>) -> Self {
        UdpSinkFactory {
            name,
            target,
            stats,
        }
    }
}

impl SinkFactory for UdpSinkFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
        let sink = UdpSink::new(
            self.name.clone(),
            self.target.clone(),
            Arc::clone(&self.stats),
        )?;
        Ok(Box::new(sink))
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn is_single_use(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use logfwd_types::diagnostics::ComponentStats;
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

    #[tokio::test]
    async fn batches_rows_into_single_datagram() {
        let receiver = StdSocket::bind("127.0.0.1:0").unwrap();
        let addr = receiver.local_addr().unwrap();
        receiver.set_nonblocking(true).unwrap();

        let mut sink =
            UdpSink::new("test", addr.to_string(), Arc::new(ComponentStats::new())).unwrap();
        let batch = make_batch(5);
        sink.do_send_batch(&batch).await.unwrap();

        // Give OS a moment to deliver.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

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

    #[tokio::test]
    async fn splits_when_exceeding_mtu() {
        let receiver = StdSocket::bind("127.0.0.1:0").unwrap();
        let addr = receiver.local_addr().unwrap();
        receiver.set_nonblocking(true).unwrap();

        let mut sink =
            UdpSink::new("test", addr.to_string(), Arc::new(ComponentStats::new())).unwrap();
        // Create enough rows that they cannot all fit in 1400 bytes.
        let batch = make_batch(100);
        sink.do_send_batch(&batch).await.unwrap();

        // Should receive multiple datagrams, each <= 1400 bytes.
        let mut buf = [0u8; 65507];
        let mut total_lines = 0;
        let mut dgram_count = 0;

        // Give OS a moment.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

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

    #[tokio::test]
    async fn factory_creates_sink() {
        let factory = UdpSinkFactory::new(
            "test-udp".to_string(),
            "127.0.0.1:9999".to_string(),
            Arc::new(ComponentStats::new()),
        );
        assert_eq!(factory.name(), "test-udp");
        assert!(!factory.is_single_use());

        let sink = factory.create().expect("create should succeed");
        assert_eq!(sink.name(), "test-udp");

        let _sink2 = factory.create().expect("second create should succeed");
    }
}

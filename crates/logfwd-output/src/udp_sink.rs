//! UDP output sink — sends newline-delimited JSON lines as UDP datagrams.
//!
//! Rows are batched into datagrams up to `MAX_DATAGRAM_PAYLOAD` bytes to
//! amortize per-packet overhead. Each datagram contains one or more
//! newline-terminated JSON lines.
//!
//! Uses `tokio::net::UdpSocket` for non-blocking async I/O.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
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
    resolved_target: Option<SocketAddr>,
    /// Scratch buffer for serializing a single row before deciding whether
    /// it fits in the current datagram.
    row_buf: Vec<u8>,
    /// Accumulation buffer for the current datagram being assembled.
    dgram_buf: Vec<u8>,
    stats: Arc<ComponentStats>,
}

impl UdpSink {
    /// Create a new UDP sink.
    ///
    /// Binds an outbound UDP socket and defers initial target DNS resolution
    /// until send time so startup does not block on transient DNS issues.
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
            resolved_target: None,
            row_buf: Vec::with_capacity(2048),
            dgram_buf: Vec::with_capacity(MAX_DATAGRAM_PAYLOAD),
            stats,
        })
    }

    /// Resolve the target to the first IPv4 address because the socket is IPv4-bound.
    async fn resolve_target(&self) -> io::Result<SocketAddr> {
        tokio::net::lookup_host(&self.target)
            .await?
            .find(SocketAddr::is_ipv4)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::AddrNotAvailable,
                    format!("DNS lookup returned no IPv4 addresses for {}", self.target),
                )
            })
    }

    async fn send_packet_once(&self, payload: &[u8], addr: SocketAddr) -> io::Result<()> {
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

    async fn send_packet(&mut self, payload: &[u8]) -> io::Result<()> {
        let cached = match self.resolved_target {
            Some(addr) => addr,
            None => {
                let addr = self.resolve_target().await?;
                self.resolved_target = Some(addr);
                addr
            }
        };
        match self.send_packet_once(payload, cached).await {
            Ok(()) => Ok(()),
            Err(first_send_error) => {
                // Refresh strategy: re-resolve and retry once on send failure.
                match self.resolve_target().await {
                    Ok(refreshed_addr) => {
                        self.resolved_target = Some(refreshed_addr);
                        self.send_packet_once(payload, refreshed_addr).await
                    }
                    Err(resolve_error) => {
                        // Clear cached address so next send starts with a fresh lookup.
                        self.resolved_target = None;
                        Err(io::Error::new(
                            resolve_error.kind(),
                            format!(
                                "failed to send UDP packet ({first_send_error}); re-resolve failed: {resolve_error}"
                            ),
                        ))
                    }
                }
            }
        }
    }

    /// Send the current datagram buffer if non-empty, then clear it.
    /// ECONNREFUSED (ICMP port-unreachable) is silently ignored because UDP
    /// is fire-and-forget — the destination may come back later.
    async fn flush_dgram(&mut self) -> io::Result<()> {
        if self.dgram_buf.is_empty() {
            return Ok(());
        }
        let mut payload = std::mem::take(&mut self.dgram_buf);
        match self.send_packet(&payload).await {
            Ok(()) => {
                payload.clear();
                self.dgram_buf = payload;
                Ok(())
            }
            Err(e) => {
                // Restore the buffer so data is not lost on transient failures.
                self.dgram_buf = payload;
                Err(e)
            }
        }
    }

    /// Serialize and send a batch of log records as UDP datagrams.
    async fn do_send_batch(&mut self, batch: &RecordBatch) -> io::Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let cols = build_col_infos(batch);
        let mut total_bytes: u64 = 0;

        for row in 0..batch.num_rows() {
            // Serialize row into scratch buffer.
            self.row_buf.clear();
            write_row_json(batch, row, &cols, &mut self.row_buf, false)?;
            self.row_buf.push(b'\n');

            let row_len = self.row_buf.len();
            total_bytes += row_len as u64;

            // If this single row exceeds the MTU, send it alone as an
            // oversized datagram (up to 65507). The network may fragment it
            // but that is better than silently dropping data.
            if row_len > MAX_DATAGRAM_PAYLOAD {
                self.flush_dgram().await?;
                let packet = std::mem::take(&mut self.row_buf);
                let result = self.send_packet(&packet).await;
                self.row_buf = packet;
                result?;
                continue;
            }

            // Would adding this row overflow the current datagram?
            if self.dgram_buf.len() + row_len > MAX_DATAGRAM_PAYLOAD {
                self.flush_dgram().await?;
            }

            self.dgram_buf.extend_from_slice(&self.row_buf);
        }

        // Flush any remaining rows.
        self.flush_dgram().await?;
        self.stats.inc_lines(batch.num_rows() as u64);
        self.stats.inc_bytes(total_bytes);
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

    async fn recv_with_retry(receiver: &StdSocket, buf: &mut [u8]) -> io::Result<usize> {
        for _ in 0..20 {
            match receiver.recv(buf) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(e) => return Err(e),
            }
        }
        receiver.recv(buf)
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

        // All 5 small rows should fit in one datagram.
        let mut buf = [0u8; 65507];
        let n = recv_with_retry(&receiver, &mut buf).await.unwrap();
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

        while let Ok(n) = recv_with_retry(&receiver, &mut buf).await {
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

    #[tokio::test]
    async fn reuses_cached_target_without_resolving_every_send() {
        let receiver1 = StdSocket::bind("127.0.0.1:0").unwrap();
        let addr1 = receiver1.local_addr().unwrap();
        receiver1.set_nonblocking(true).unwrap();

        let receiver2 = StdSocket::bind("127.0.0.1:0").unwrap();
        let addr2 = receiver2.local_addr().unwrap();
        receiver2.set_nonblocking(true).unwrap();

        let mut sink =
            UdpSink::new("test", addr1.to_string(), Arc::new(ComponentStats::new())).unwrap();
        sink.send_packet(b"first\n").await.unwrap();

        // If the sink re-resolves every send, this packet would go to receiver2.
        sink.target = addr2.to_string();
        sink.send_packet(b"second\n").await.unwrap();

        let mut buf = [0u8; 128];
        let first_n = recv_with_retry(&receiver1, &mut buf)
            .await
            .expect("receiver1 first packet");
        assert_eq!(&buf[..first_n], b"first\n");
        let second_n = recv_with_retry(&receiver1, &mut buf)
            .await
            .expect("receiver1 second packet");
        assert_eq!(&buf[..second_n], b"second\n");
        assert!(
            receiver2.recv(&mut buf).is_err(),
            "receiver2 should receive nothing when target is cached"
        );
    }

    #[tokio::test]
    async fn re_resolves_after_send_failure() {
        let receiver = StdSocket::bind("127.0.0.1:0").unwrap();
        let addr = receiver.local_addr().unwrap();
        receiver.set_nonblocking(true).unwrap();

        let mut sink = UdpSink::new(
            "test",
            "127.0.0.1:9".to_string(),
            Arc::new(ComponentStats::new()),
        )
        .unwrap();
        // Force first send attempt to fail on this IPv4-only socket, then
        // ensure re-resolution picks the valid configured target.
        sink.resolved_target = Some("[::1]:9".parse().expect("ipv6 addr"));
        sink.target = addr.to_string();

        sink.send_packet(b"retry\n").await.unwrap();

        let mut buf = [0u8; 128];
        let n = recv_with_retry(&receiver, &mut buf)
            .await
            .expect("packet after re-resolve");
        assert_eq!(&buf[..n], b"retry\n");
    }
}

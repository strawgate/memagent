//! UDP output sink — sends newline-delimited JSON lines as UDP datagrams.
//!
//! Rows are batched into datagrams up to `MAX_DATAGRAM_PAYLOAD` bytes to
//! amortize per-packet overhead. Each datagram contains one or more
//! newline-terminated JSON lines.
//!
//! Uses `tokio::net::UdpSocket` for non-blocking async I/O.

use std::future::Future;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
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
    socket_v4: Option<UdpSocket>,
    socket_v6: Option<UdpSocket>,
    targets: Vec<SocketAddr>,
    /// Scratch buffer for serializing a single row before deciding whether
    /// it fits in the current datagram.
    row_buf: Vec<u8>,
    /// Accumulation buffer for the current datagram being assembled.
    dgram_buf: Vec<u8>,
    stats: Arc<ComponentStats>,
}

impl UdpSink {
    fn bind_socket(bind_addr: &str) -> io::Result<UdpSocket> {
        let std_socket = std::net::UdpSocket::bind(bind_addr)?;
        std_socket.set_nonblocking(true)?;
        UdpSocket::from_std(std_socket)
    }

    /// Create a new UDP sink.
    ///
    /// Resolves all target addresses once and keeps family-specific sockets for
    /// outbound traffic.
    pub fn new(
        name: impl Into<String>,
        target: impl Into<String>,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        let target = target.into();
        let mut resolved_targets: Vec<SocketAddr> = target.to_socket_addrs()?.collect();
        if resolved_targets.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "target resolved to no address",
            ));
        }
        resolved_targets.sort_unstable();
        resolved_targets.dedup();

        let has_v4 = resolved_targets.iter().any(SocketAddr::is_ipv4);
        let has_v6 = resolved_targets.iter().any(SocketAddr::is_ipv6);
        let mut bind_error: Option<io::Error> = None;
        let socket_v4 = if has_v4 {
            match Self::bind_socket("0.0.0.0:0") {
                Ok(socket) => Some(socket),
                Err(err) => {
                    bind_error = Some(err);
                    None
                }
            }
        } else {
            None
        };
        let socket_v6 = if has_v6 {
            match Self::bind_socket("[::]:0") {
                Ok(socket) => Some(socket),
                Err(err) => {
                    bind_error = Some(err);
                    None
                }
            }
        } else {
            None
        };
        if socket_v4.is_none() && socket_v6.is_none() {
            return Err(bind_error.unwrap_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "resolved targets had no usable address family",
                )
            }));
        }

        Ok(Self {
            name: name.into(),
            socket_v4,
            socket_v6,
            targets: resolved_targets,
            row_buf: Vec::with_capacity(2048),
            dgram_buf: Vec::with_capacity(MAX_DATAGRAM_PAYLOAD),
            stats,
        })
    }

    async fn send_datagram_to_resolved_targets(&self, payload: &[u8]) -> io::Result<()> {
        let mut attempted = false;
        let mut saw_connection_refused = false;
        let mut last_error: Option<io::Error> = None;

        for target in &self.targets {
            let socket = if target.is_ipv4() {
                self.socket_v4.as_ref()
            } else {
                self.socket_v6.as_ref()
            };
            let Some(socket) = socket else {
                continue;
            };
            attempted = true;
            match socket.send_to(payload, *target).await {
                Ok(_) => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                    // Continue trying other resolved targets; a single refused
                    // address should not prevent delivery to healthy peers.
                    saw_connection_refused = true;
                }
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }

        if attempted {
            if let Some(err) = last_error {
                Err(err)
            } else if saw_connection_refused {
                // All attempted targets refused the datagram; keep best-effort
                // semantics and do not fail the batch.
                Ok(())
            } else {
                Err(io::Error::other("UDP send failed"))
            }
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "target resolved to no address",
            ))
        }
    }

    /// Send the current datagram buffer if non-empty, then clear it.
    /// ECONNREFUSED (ICMP port-unreachable) is silently ignored because UDP
    /// is fire-and-forget — the destination may come back later.
    async fn flush_dgram(&mut self) -> io::Result<()> {
        if self.dgram_buf.is_empty() {
            return Ok(());
        }
        self.send_datagram_to_resolved_targets(&self.dgram_buf)
            .await?;
        self.dgram_buf.clear();
        Ok(())
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
            write_row_json(batch, row, &cols, &mut self.row_buf)?;
            self.row_buf.push(b'\n');

            let row_len = self.row_buf.len();
            total_bytes += row_len as u64;

            // If this single row exceeds the MTU, send it alone as an
            // oversized datagram (up to 65507). The network may fragment it
            // but that is better than silently dropping data.
            if row_len > MAX_DATAGRAM_PAYLOAD {
                self.flush_dgram().await?;
                self.send_datagram_to_resolved_targets(&self.row_buf)
                    .await?;
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
                Err(e) => SendResult::IoError(e),
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

    #[tokio::test]
    async fn flush_tries_all_resolved_targets() {
        let receiver = StdSocket::bind("127.0.0.1:0").expect("bind receiver");
        let receiver_addr = receiver.local_addr().expect("receiver addr");
        receiver
            .set_nonblocking(true)
            .expect("receiver nonblocking");

        let send_socket_v4 = {
            let std_socket = StdSocket::bind("0.0.0.0:0").expect("bind sender v4");
            std_socket
                .set_nonblocking(true)
                .expect("sender v4 nonblocking");
            UdpSocket::from_std(std_socket).expect("tokio socket")
        };

        // First target is IPv6 and intentionally incompatible with the available
        // socket set; sink must continue and deliver to the IPv4 target.
        let mut sink = UdpSink {
            name: "test-fallback".to_string(),
            socket_v4: Some(send_socket_v4),
            socket_v6: None,
            targets: vec!["[::1]:9".parse().expect("valid ipv6 target"), receiver_addr],
            row_buf: Vec::with_capacity(2048),
            dgram_buf: Vec::with_capacity(MAX_DATAGRAM_PAYLOAD),
            stats: Arc::new(ComponentStats::new()),
        };
        sink.dgram_buf.extend_from_slice(b"hello\n");
        sink.flush_dgram().await.expect("flush should succeed");

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut buf = [0u8; 1024];
        let n = receiver
            .recv(&mut buf)
            .expect("datagram should be received");
        assert_eq!(
            std::str::from_utf8(&buf[..n]).expect("utf8"),
            "hello\n",
            "sink should deliver to a later resolved target when the first is incompatible"
        );
    }
}

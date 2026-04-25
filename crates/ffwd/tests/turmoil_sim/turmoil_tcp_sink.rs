//! `Sink` implementation that sends NDJSON over `turmoil::net::TcpStream`.
//!
//! This is a test-only sink used to exercise real network failure handling
//! through the worker pool retry logic. It connects lazily on the first
//! `send_batch` call and reconnects on error.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::record_batch::RecordBatch;
use tokio::io::AsyncWriteExt;

use ffwd_output::BatchMetadata;
use ffwd_output::sink::{SendResult, Sink};
use ffwd_output::{build_col_infos, write_row_json};

/// A Sink that writes NDJSON lines over a turmoil-simulated TCP connection.
///
/// Connects lazily on the first `send_batch` call. If the connection breaks,
/// the next call will attempt to reconnect.
pub struct TurmoilTcpSink {
    host: String,
    port: u16,
    stream: Option<turmoil::net::TcpStream>,
    delivered_rows: Arc<AtomicU64>,
}

#[allow(dead_code)]
impl TurmoilTcpSink {
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            host: host.to_string(),
            port,
            stream: None,
            delivered_rows: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn delivered_counter(&self) -> Arc<AtomicU64> {
        self.delivered_rows.clone()
    }

    async fn ensure_connected(&mut self) -> io::Result<()> {
        if self.stream.is_none() {
            let addr = (self.host.as_str(), self.port);
            let stream = turmoil::net::TcpStream::connect(addr).await?;
            self.stream = Some(stream);
        }
        Ok(())
    }
}

impl Sink for TurmoilTcpSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        _metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        Box::pin(async move {
            // Connect (or reconnect) if needed.
            if let Err(e) = self.ensure_connected().await {
                self.stream = None;
                return SendResult::IoError(e);
            }

            // Serialize each row as a JSON line.
            let cols = build_col_infos(batch);
            let mut buf = Vec::new();
            for row in 0..batch.num_rows() {
                buf.clear();
                if let Err(e) = write_row_json(batch, row, &cols, &mut buf, false) {
                    return SendResult::IoError(io::Error::other(e.to_string()));
                }
                buf.push(b'\n');

                let stream = self.stream.as_mut().unwrap();
                if let Err(e) = stream.write_all(&buf).await {
                    // Connection broken — drop it so next call reconnects.
                    self.stream = None;
                    return SendResult::IoError(e);
                }
            }

            self.delivered_rows
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            SendResult::Ok
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            if let Some(stream) = self.stream.as_mut() {
                stream.flush().await?;
            }
            Ok(())
        })
    }

    fn name(&self) -> &str {
        "turmoil-tcp"
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            if let Some(mut stream) = self.stream.take() {
                let _ = stream.shutdown().await;
            }
            Ok(())
        })
    }
}

/// Factory that creates `TurmoilTcpSink` instances for multi-worker testing.
#[allow(dead_code)]
pub struct TurmoilTcpSinkFactory {
    host: String,
    port: u16,
    delivered_rows: Arc<AtomicU64>,
}

#[allow(dead_code)]
impl TurmoilTcpSinkFactory {
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            host: host.to_string(),
            port,
            delivered_rows: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn delivered_counter(&self) -> Arc<AtomicU64> {
        self.delivered_rows.clone()
    }
}

impl ffwd_output::SinkFactory for TurmoilTcpSinkFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
        let mut sink = TurmoilTcpSink::new(&self.host, self.port);
        sink.delivered_rows = self.delivered_rows.clone();
        Ok(Box::new(sink))
    }

    fn name(&self) -> &str {
        "turmoil-tcp-factory"
    }

    fn is_single_use(&self) -> bool {
        false
    }
}

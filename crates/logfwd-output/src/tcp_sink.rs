//! TCP output sink — connects to a TCP endpoint and sends newline-delimited
//! JSON lines. Reconnects on failure with a single retry per `send_batch` call.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use logfwd_types::diagnostics::ComponentStats;

use crate::sink::{SendResult, Sink, SinkFactory};
use crate::{BatchMetadata, build_col_infos, write_row_json};

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
    async fn connect(&mut self) -> io::Result<()> {
        let result = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&self.addr)).await;

        let stream = match result {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "TCP connect timed out",
                ));
            }
        };

        stream.set_nodelay(true)?;
        self.stream = Some(stream);
        Ok(())
    }

    /// Ensure we have a live connection, connecting lazily if needed.
    async fn ensure_connected(&mut self) -> io::Result<()> {
        if self.stream.is_some() {
            return Ok(());
        }
        self.connect().await
    }

    /// Write the buffer to the stream. On failure, drop the connection and
    /// retry exactly once with a fresh connection.
    async fn write_with_retry(&mut self) -> io::Result<()> {
        self.ensure_connected().await?;

        // First attempt.
        if let Some(ref mut stream) = self.stream {
            let result = tokio::time::timeout(WRITE_TIMEOUT, stream.write_all(&self.buf)).await;
            match result {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(_)) | Err(_) => {
                    // Connection went bad or timed out — drop it and retry below.
                    self.stream = None;
                }
            }
        }

        // Single retry with a fresh connection.
        self.connect().await?;
        if let Some(ref mut stream) = self.stream {
            let result = tokio::time::timeout(WRITE_TIMEOUT, stream.write_all(&self.buf)).await;
            match result {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(e)) => {
                    self.stream = None;
                    return Err(e);
                }
                Err(_) => {
                    self.stream = None;
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "TCP write timed out",
                    ));
                }
            }
        }
        Ok(())
    }
}

impl Sink for TcpSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        _metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        Box::pin(async move {
            if batch.num_rows() == 0 {
                return SendResult::Ok;
            }

            self.buf.clear();
            let cols = build_col_infos(batch);
            for row in 0..batch.num_rows() {
                if let Err(e) = write_row_json(batch, row, &cols, &mut self.buf) {
                    return SendResult::IoError(e);
                }
                self.buf.push(b'\n');
            }

            if let Err(e) = self.write_with_retry().await {
                return SendResult::IoError(e);
            }
            self.stats.inc_lines(batch.num_rows() as u64);
            self.stats.inc_bytes(self.buf.len() as u64);
            SendResult::Ok
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            if let Some(ref mut stream) = self.stream {
                stream.flush().await?;
            }
            Ok(())
        })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            if let Some(ref mut stream) = self.stream {
                stream.flush().await?;
                stream.shutdown().await?;
            }
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// TcpSinkFactory
// ---------------------------------------------------------------------------

/// Factory for creating [`TcpSink`] instances.
///
/// Each worker gets its own TCP connection via a fresh `TcpSink`.
pub struct TcpSinkFactory {
    name: String,
    addr: String,
    stats: Arc<ComponentStats>,
}

impl TcpSinkFactory {
    pub fn new(name: String, addr: String, stats: Arc<ComponentStats>) -> Self {
        Self { name, addr, stats }
    }
}

impl SinkFactory for TcpSinkFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
        Ok(Box::new(TcpSink::new(
            self.name.clone(),
            self.addr.clone(),
            Arc::clone(&self.stats),
        )))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

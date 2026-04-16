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

use logfwd_config::{OutputEncoding, TcpFraming, TlsClientConfig};

pub struct TcpSink {
    name: String,
    addr: String,
    stream: Option<TcpStream>,
    buf: Vec<u8>,
    stats: Arc<ComponentStats>,
    #[allow(dead_code)]
    tls: Option<TlsClientConfig>,
    max_retries: Option<usize>,
    retry_backoff_ms: Option<u64>,
    connect_timeout_ms: Option<u64>,
    keepalive: Option<bool>,
    framing: Option<TcpFraming>,
    #[allow(dead_code)]
    encoding: Option<OutputEncoding>,
}

impl TcpSink {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: impl Into<String>,
        addr: impl Into<String>,
        stats: Arc<ComponentStats>,
        tls: Option<TlsClientConfig>,
        max_retries: Option<usize>,
        retry_backoff_ms: Option<u64>,
        connect_timeout_ms: Option<u64>,
        keepalive: Option<bool>,
        framing: Option<TcpFraming>,
        encoding: Option<OutputEncoding>,
    ) -> Self {
        Self {
            name: name.into(),
            addr: addr.into(),
            stream: None,
            buf: Vec::with_capacity(64 * 1024),
            stats,
            tls,
            max_retries,
            retry_backoff_ms,
            connect_timeout_ms,
            keepalive,
            framing,
            encoding,
        }
    }

    /// Open a new connection, replacing any existing one.
    async fn connect(&mut self) -> io::Result<()> {
        let connect_timeout = self
            .connect_timeout_ms
            .map_or(CONNECT_TIMEOUT, Duration::from_millis);

        let result = tokio::time::timeout(connect_timeout, TcpStream::connect(&self.addr)).await;

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

        if self.keepalive.unwrap_or(false) {
            let socket = socket2::Socket::from(stream.into_std()?);
            socket.set_keepalive(true)?;
            let stream = TcpStream::from_std(socket.into())?;
            self.stream = Some(stream);
        } else {
            self.stream = Some(stream);
        }

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

        // Retry logic based on max_retries (0 means 'no retries', i.e., 1 attempt)
        let max_retries = self.max_retries.unwrap_or(1);
        let max_attempts = if max_retries == 0 { 1 } else { max_retries + 1 };

        let mut backoff_ms = self.retry_backoff_ms.unwrap_or(100);

        for attempt in 0..max_attempts {
            self.connect().await?;
            if let Some(ref mut stream) = self.stream {
                let result = tokio::time::timeout(WRITE_TIMEOUT, stream.write_all(&self.buf)).await;
                match result {
                    Ok(Ok(())) => return Ok(()),
                    Ok(Err(e)) => {
                        self.stream = None;
                        if attempt == max_attempts - 1 {
                            return Err(e);
                        }
                    }
                    Err(_) => {
                        self.stream = None;
                        if attempt == max_attempts - 1 {
                            return Err(io::Error::new(
                                io::ErrorKind::TimedOut,
                                "TCP write timed out",
                            ));
                        }
                    }
                }
            }
            if attempt < max_attempts - 1 {
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = std::cmp::min(backoff_ms * 2, 5000); // Max backoff 5s
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
            let mut scratch = Vec::new();

            for row in 0..batch.num_rows() {
                scratch.clear();
                if let Err(e) = write_row_json(batch, row, &cols, &mut scratch) {
                    return SendResult::from_io_error(e);
                }
                scratch.push(b'\n');

                // Add framing bytes if necessary
                if matches!(self.framing, Some(TcpFraming::OctetCount)) {
                    self.buf
                        .extend_from_slice(scratch.len().to_string().as_bytes());
                    self.buf.push(b' ');
                }
                self.buf.extend_from_slice(&scratch);
                if matches!(self.framing, Some(TcpFraming::NullByte)) {
                    self.buf.pop(); // Remove newline
                    self.buf.push(b'\0');
                }
            }

            if let Err(e) = self.write_with_retry().await {
                return SendResult::from_io_error(e);
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
    tls: Option<TlsClientConfig>,
    max_retries: Option<usize>,
    retry_backoff_ms: Option<u64>,
    connect_timeout_ms: Option<u64>,
    keepalive: Option<bool>,
    framing: Option<TcpFraming>,
    encoding: Option<OutputEncoding>,
}

impl TcpSinkFactory {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        addr: String,
        stats: Arc<ComponentStats>,
        tls: Option<TlsClientConfig>,
        max_retries: Option<usize>,
        retry_backoff_ms: Option<u64>,
        connect_timeout_ms: Option<u64>,
        keepalive: Option<bool>,
        framing: Option<TcpFraming>,
        encoding: Option<OutputEncoding>,
    ) -> Self {
        Self {
            name,
            addr,
            stats,
            tls,
            max_retries,
            retry_backoff_ms,
            connect_timeout_ms,
            keepalive,
            framing,
            encoding,
        }
    }
}

impl SinkFactory for TcpSinkFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
        Ok(Box::new(TcpSink::new(
            self.name.clone(),
            self.addr.clone(),
            Arc::clone(&self.stats),
            self.tls.clone(),
            self.max_retries,
            self.retry_backoff_ms,
            self.connect_timeout_ms,
            self.keepalive,
            self.framing.clone(),
            self.encoding.clone(),
        )))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

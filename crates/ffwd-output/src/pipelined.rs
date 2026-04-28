//! Pipelined sink: separates serialization (CPU) from transport (I/O).
//!
//! The pipeline uses a dedicated OS thread for I/O, overlapping serialization
//! of batch N+1 with writing of batch N. A triple-buffered pool avoids
//! allocation churn.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐     sync_channel      ┌──────────────────┐
//! │  Caller thread   │ ──── filled buf ────▶ │  Writer thread    │
//! │  (tokio worker)  │ ◀─── empty buf ───── │  (OS thread)      │
//! │                  │                       │                   │
//! │  serialize(batch)│                       │  writer.write(buf)│
//! └─────────────────┘                       └──────────────────┘
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! let sink = PipelinedSink::new(serializer, writer, PipelineConfig::default());
//! // sink implements Sink trait — use it in the worker pool as normal
//! ```

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::mpsc::{self, SyncSender};
use std::thread::{self, JoinHandle};

use arrow::record_batch::RecordBatch;
use ffwd_types::diagnostics::ComponentStats;

use crate::BatchMetadata;
use crate::sink::{SendResult, Sink};

// ---------------------------------------------------------------------------
// BatchSerializer trait
// ---------------------------------------------------------------------------

/// Pure CPU serialization: converts a `RecordBatch` into bytes.
///
/// Implementors MUST NOT perform any I/O. This trait is intentionally
/// synchronous to enforce that constraint at the type level.
pub trait BatchSerializer: Send {
    /// Serialize `batch` into `buf`, appending bytes.
    ///
    /// Returns the number of logical rows written (may differ from
    /// `batch.num_rows()` if some rows are filtered/skipped).
    fn serialize(
        &mut self,
        batch: &RecordBatch,
        metadata: &BatchMetadata,
        buf: &mut Vec<u8>,
    ) -> io::Result<u64>;

    /// Human-readable name for logging and diagnostics.
    fn name(&self) -> &str;
}

// ---------------------------------------------------------------------------
// BatchWriter trait
// ---------------------------------------------------------------------------

/// I/O transport: takes serialized bytes and delivers them to a destination.
///
/// Runs on a dedicated OS thread. Implementations own the file descriptor,
/// socket, or HTTP client and perform blocking I/O.
pub trait BatchWriter: Send + 'static {
    /// Write the serialized payload to the destination.
    fn write(&mut self, buf: &[u8]) -> io::Result<()>;

    /// Flush internal buffers (if any) to the OS.
    fn flush(&mut self) -> io::Result<()>;

    /// Graceful shutdown: flush + fsync / close connection.
    fn shutdown(&mut self) -> io::Result<()>;
}

// ---------------------------------------------------------------------------
// PipelineConfig
// ---------------------------------------------------------------------------

/// Configuration for the pipelined sink.
pub struct PipelineConfig {
    /// Number of buffers in the pool (channel depth + 1 in-flight).
    /// Default: 3 (triple-buffered).
    pub num_buffers: usize,

    /// Initial capacity for each buffer in bytes.
    /// Default: 2 MB.
    pub buf_capacity: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            num_buffers: 3,
            buf_capacity: 2 * 1024 * 1024,
        }
    }
}

// ---------------------------------------------------------------------------
// Writer thread messages
// ---------------------------------------------------------------------------

enum WriterMsg {
    /// A filled buffer ready to be written.
    Data(Vec<u8>),
    /// Flush request with a one-shot ack channel.
    Flush(SyncSender<io::Result<()>>),
    /// Shutdown request — writer should flush, sync, then exit.
    Shutdown(SyncSender<io::Result<()>>),
}

// ---------------------------------------------------------------------------
// PipelinedSink
// ---------------------------------------------------------------------------

/// A `Sink` implementation that pipelines serialization and I/O on separate
/// threads for maximum throughput.
pub struct PipelinedSink<S: BatchSerializer> {
    serializer: S,
    stats: Arc<ComponentStats>,

    /// Channel to send filled buffers (and control messages) to the writer.
    filled_tx: Option<SyncSender<WriterMsg>>,

    /// Channel to receive empty (recycled) buffers back from the writer.
    empty_rx: mpsc::Receiver<Vec<u8>>,

    /// Handle to the writer thread (for join on shutdown).
    writer_handle: Option<JoinHandle<()>>,
}

impl<S: BatchSerializer> PipelinedSink<S> {
    /// Create a new pipelined sink.
    ///
    /// Spawns a dedicated OS thread for the writer immediately.
    pub fn new(
        serializer: S,
        writer: impl BatchWriter,
        stats: Arc<ComponentStats>,
        config: PipelineConfig,
    ) -> Self {
        let (filled_tx, filled_rx) = mpsc::sync_channel::<WriterMsg>(config.num_buffers);
        let (empty_tx, empty_rx) = mpsc::sync_channel::<Vec<u8>>(config.num_buffers);

        // Seed the empty buffer pool.
        for _ in 0..config.num_buffers {
            let _ = empty_tx.send(Vec::with_capacity(config.buf_capacity));
        }

        let writer_handle = thread::Builder::new()
            .name(format!("ffwd-writer-{}", serializer.name()))
            .spawn(move || {
                writer_thread_loop(writer, filled_rx, empty_tx);
            })
            .expect("failed to spawn writer thread");

        Self {
            serializer,
            stats,
            filled_tx: Some(filled_tx),
            empty_rx,
            writer_handle: Some(writer_handle),
        }
    }

    /// Get an empty buffer from the pool, blocking until one is available.
    fn acquire_buffer(&self) -> io::Result<Vec<u8>> {
        self.empty_rx.recv().map_err(|_recv| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "writer thread exited unexpectedly",
            )
        })
    }

    /// Send a filled buffer to the writer thread.
    fn send_filled(&self, buf: Vec<u8>) -> io::Result<()> {
        if let Some(ref tx) = self.filled_tx {
            tx.send(WriterMsg::Data(buf)).map_err(|_send| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "writer thread exited unexpectedly",
                )
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "sink already shut down",
            ))
        }
    }

    /// Send a control message and wait for the ack.
    fn send_control<F>(&self, make_msg: F) -> io::Result<()>
    where
        F: FnOnce(SyncSender<io::Result<()>>) -> WriterMsg,
    {
        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        if let Some(ref tx) = self.filled_tx {
            tx.send(make_msg(ack_tx)).map_err(|_send| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "writer thread exited unexpectedly",
                )
            })?;
            ack_rx.recv().map_err(|_recv| {
                io::Error::new(io::ErrorKind::BrokenPipe, "writer thread exited before ack")
            })?
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "sink already shut down",
            ))
        }
    }
}

impl<S: BatchSerializer> Sink for PipelinedSink<S> {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        Box::pin(async move {
            // Acquire an empty buffer from the pool (blocks if writer is behind).
            let mut buf = match self.acquire_buffer() {
                Ok(b) => b,
                Err(e) => return SendResult::from_io_error(e),
            };
            buf.clear();

            // Serialize on the caller thread (CPU work).
            let rows = match self.serializer.serialize(batch, metadata, &mut buf) {
                Ok(n) => n,
                Err(e) => {
                    // Return buffer to pool to avoid exhaustion on repeated errors.
                    // Send it through the writer thread as empty data — the writer
                    // will write zero bytes (no-op) and recycle the buffer.
                    buf.clear();
                    let _ = self.send_filled(buf);
                    return SendResult::from_io_error(e);
                }
            };

            let bytes = buf.len() as u64;

            // Hand off the filled buffer to the writer thread.
            if let Err(e) = self.send_filled(buf) {
                return SendResult::from_io_error(e);
            }

            self.stats.inc_lines(rows);
            self.stats.inc_bytes(bytes);
            SendResult::Ok
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move { self.send_control(WriterMsg::Flush) })
    }

    fn name(&self) -> &str {
        self.serializer.name()
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            // Send shutdown message and wait for ack.
            let result = self.send_control(WriterMsg::Shutdown);

            // Drop the sender so the writer thread exits its loop.
            self.filled_tx.take();

            // Join the writer thread.
            if let Some(handle) = self.writer_handle.take() {
                let _ = handle.join();
            }

            result
        })
    }
}

impl<S: BatchSerializer> Drop for PipelinedSink<S> {
    fn drop(&mut self) {
        // Best-effort shutdown if not already done.
        self.filled_tx.take();
        if let Some(handle) = self.writer_handle.take() {
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Writer thread loop
// ---------------------------------------------------------------------------

fn writer_thread_loop(
    mut writer: impl BatchWriter,
    filled_rx: mpsc::Receiver<WriterMsg>,
    empty_tx: SyncSender<Vec<u8>>,
) {
    while let Ok(msg) = filled_rx.recv() {
        match msg {
            WriterMsg::Data(buf) => {
                if let Err(e) = writer.write(&buf) {
                    tracing::error!(error = %e, "pipelined writer: write failed");
                    // Return the buffer anyway to avoid deadlock.
                }
                // Recycle the buffer back to the pool.
                let mut recycled = buf;
                recycled.clear();
                // If send fails, the sink is being dropped — just let the buffer go.
                let _ = empty_tx.try_send(recycled);
            }
            WriterMsg::Flush(ack) => {
                let result = writer.flush();
                let _ = ack.send(result);
            }
            WriterMsg::Shutdown(ack) => {
                let result = writer.shutdown();
                let _ = ack.send(result);
                return;
            }
        }
    }
    // Channel closed without Shutdown — best-effort shutdown.
    let _ = writer.shutdown();
}

// ---------------------------------------------------------------------------
// FileWriter
// ---------------------------------------------------------------------------

/// A [`BatchWriter`] that writes to a file using blocking std::fs I/O.
pub struct FileWriter {
    file: std::fs::File,
}

impl FileWriter {
    /// Create a new file writer from a std::fs::File.
    pub fn new(file: std::fs::File) -> Self {
        Self { file }
    }
}

impl BatchWriter for FileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<()> {
        use std::io::Write;
        self.file.write_all(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        use std::io::Write;
        self.file.flush()?;
        self.file.sync_data()
    }

    fn shutdown(&mut self) -> io::Result<()> {
        use std::io::Write;
        self.file.flush()?;
        self.file.sync_data()
    }
}

// ---------------------------------------------------------------------------
// JsonBatchSerializer
// ---------------------------------------------------------------------------

use crate::stdout::{StdoutFormat, StdoutSink};

/// A [`BatchSerializer`] that produces JSON-lines or text output.
///
/// Wraps the same serialization logic as `StdoutSink::write_batch_to`.
pub struct JsonBatchSerializer {
    inner: StdoutSink,
}

impl JsonBatchSerializer {
    /// Create a new JSON batch serializer.
    pub fn new(name: String, format: StdoutFormat, stats: Arc<ComponentStats>) -> Self {
        Self {
            inner: StdoutSink::new(name, format, stats),
        }
    }

    /// Create with a custom message field name.
    pub fn with_message_field(
        name: String,
        format: StdoutFormat,
        message_field: String,
        stats: Arc<ComponentStats>,
    ) -> Self {
        Self {
            inner: StdoutSink::with_message_field(name, format, message_field, stats),
        }
    }
}

impl BatchSerializer for JsonBatchSerializer {
    fn serialize(
        &mut self,
        batch: &RecordBatch,
        metadata: &BatchMetadata,
        buf: &mut Vec<u8>,
    ) -> io::Result<u64> {
        self.inner
            .write_batch_to(batch, metadata, buf)
            .map(|n| n as u64)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
}

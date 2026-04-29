//! Pipelined sink: separates serialization (CPU) from transport (I/O).
//!
//! The pipeline uses a dedicated OS thread for I/O and a small buffer pool to
//! keep blocking file writes off the caller's serialization path without
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
//! let sink = PipelinedSink::new(serializer, writer, stats, PipelineConfig::default())?;
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
    /// Number of buffers in the pool. These are cycled between the serializer
    /// and writer threads for zero-allocation steady-state operation.
    /// Default: 3 (triple-buffered: one being serialized, one in flight, one
    /// being written).
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
    /// A filled buffer ready to be written, with an ack for the write result.
    Data {
        buf: Vec<u8>,
        ack: SyncSender<io::Result<()>>,
    },
    /// Return a buffer to the pool without writing it.
    Recycle(Vec<u8>),
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
    /// Returns an error if the thread cannot be spawned or config is invalid.
    pub fn new(
        serializer: S,
        writer: impl BatchWriter,
        stats: Arc<ComponentStats>,
        config: PipelineConfig,
    ) -> io::Result<Self> {
        if config.num_buffers == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "PipelineConfig::num_buffers must be >= 1",
            ));
        }

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
            .map_err(io::Error::other)?;

        Ok(Self {
            serializer,
            stats,
            filled_tx: Some(filled_tx),
            empty_rx,
            writer_handle: Some(writer_handle),
        })
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

    /// Send a filled buffer to the writer thread and wait for it to be written.
    fn write_filled(&self, buf: Vec<u8>) -> io::Result<()> {
        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        if let Some(ref tx) = self.filled_tx {
            tx.send(WriterMsg::Data { buf, ack: ack_tx })
                .map_err(|_send| {
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

    /// Return an unused buffer to the writer-owned buffer pool.
    fn recycle_buffer(&self, buf: Vec<u8>) -> io::Result<()> {
        if let Some(ref tx) = self.filled_tx {
            tx.send(WriterMsg::Recycle(buf)).map_err(|_send| {
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
                    buf.clear();
                    let _ = self.recycle_buffer(buf);
                    return SendResult::from_io_error(e);
                }
            };

            let bytes = buf.len() as u64;

            // Hand off the filled buffer and wait for the write to succeed
            // before acknowledging delivery to the worker pool. Checkpoints are
            // allowed to advance only after the bytes are actually persisted.
            if let Err(e) = self.write_filled(buf) {
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
            WriterMsg::Data { buf, ack } => {
                let result = writer.write(&buf);
                if let Err(e) = &result {
                    tracing::error!(error = %e, "pipelined writer: write failed");
                }
                // Recycle the buffer back to the pool.
                let mut recycled = buf;
                recycled.clear();
                // If send fails, the sink is being dropped — just let the buffer go.
                let _ = empty_tx.try_send(recycled);
                let _ = ack.send(result);
            }
            WriterMsg::Recycle(mut buf) => {
                buf.clear();
                let _ = empty_tx.try_send(buf);
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

/// Pre-allocation chunk size: 64 MB.
///
/// When the file is opened (or needs more space), we ask the OS to reserve
/// this much contiguous space.  This avoids per-write metadata updates on
/// ext4/xfs/APFS that slow down sequential extending writes.
#[cfg(target_os = "linux")]
const PREALLOC_BYTES: u64 = 64 * 1024 * 1024;

/// A [`BatchWriter`] that writes to a file using blocking std::fs I/O.
///
/// Applies platform-specific optimizations on construction where safe wrappers
/// are available:
///
/// | Platform | Optimization |
/// |----------|-------------|
/// | Linux | `fallocate` pre-allocation, `POSIX_FADV_DONTNEED` after writes via `rustix` |
/// | macOS | standard `write_all` |
/// | Windows | (standard `write_all` — no extra APIs yet) |
pub struct FileWriter {
    file: std::fs::File,
    /// Cumulative bytes written — used for `FADV_DONTNEED` offset tracking.
    #[cfg(target_os = "linux")]
    bytes_written: u64,
}

impl FileWriter {
    /// Create a new file writer, applying platform-specific hints.
    pub fn new(file: std::fs::File) -> Self {
        Self::apply_platform_hints(&file);

        // Initialize bytes_written to the current file position so that
        // FADV_DONTNEED offsets are correct when appending to existing files.
        #[cfg(target_os = "linux")]
        let bytes_written = file.metadata().map_or_else(|_| 0, |m| m.len());

        Self {
            file,
            #[cfg(target_os = "linux")]
            bytes_written,
        }
    }

    /// Apply one-time platform-specific optimizations to the file descriptor.
    fn apply_platform_hints(_file: &std::fs::File) {
        #[cfg(target_os = "linux")]
        Self::apply_linux_hints(_file);
    }

    /// Linux: pre-allocate space with `fallocate` and set sequential advice.
    #[cfg(target_os = "linux")]
    fn apply_linux_hints(file: &std::fs::File) {
        use rustix::fs::{Advice, FallocateFlags, fadvise, fallocate};

        // Pre-allocate disk space to avoid metadata updates on every
        // extending write.  KEEP_SIZE means the visible file size is not
        // changed — only the underlying blocks are reserved.
        let _ = fallocate(file, FallocateFlags::KEEP_SIZE, 0, PREALLOC_BYTES);

        // Tell the kernel we're doing sequential writes so it can optimize
        // read-ahead and page cache management.
        let _ = fadvise(file, 0, None, Advice::Sequential);
    }

    /// Linux: advise the kernel to drop page cache for already-written data.
    ///
    /// This prevents the forwarder from bloating the host's page cache with
    /// write-only log data that will never be re-read by this process.
    #[cfg(target_os = "linux")]
    fn advise_dontneed(&self, offset: u64, len: u64) {
        use std::num::NonZeroU64;

        use rustix::fs::{Advice, fadvise};

        let Some(len) = NonZeroU64::new(len) else {
            return;
        };
        let _ = fadvise(&self.file, offset, Some(len), Advice::DontNeed);
    }
}

impl BatchWriter for FileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<()> {
        use std::io::Write;
        self.file.write_all(buf)?;

        // Tell the OS to drop the pages we just wrote — we won't re-read them.
        #[cfg(target_os = "linux")]
        {
            let offset = self.bytes_written;
            self.bytes_written += buf.len() as u64;
            self.advise_dontneed(offset, buf.len() as u64);
        }

        Ok(())
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::Ordering;

    use arrow::datatypes::Schema;

    use super::*;

    struct FixedSerializer {
        rows: u64,
        payload: &'static [u8],
    }

    impl BatchSerializer for FixedSerializer {
        fn serialize(
            &mut self,
            _batch: &RecordBatch,
            _metadata: &BatchMetadata,
            buf: &mut Vec<u8>,
        ) -> io::Result<u64> {
            buf.extend_from_slice(self.payload);
            Ok(self.rows)
        }

        fn name(&self) -> &str {
            "fixed"
        }
    }

    struct RecordingWriter {
        writes: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl BatchWriter for RecordingWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<()> {
            self.writes
                .lock()
                .expect("recording writer mutex")
                .push(buf.to_vec());
            Ok(())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn shutdown(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct FailingWriter;

    impl BatchWriter for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<()> {
            Err(io::Error::other("disk full"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn shutdown(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn empty_batch() -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    }

    fn metadata() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::default(),
            observed_time_ns: 0,
        }
    }

    #[tokio::test]
    async fn send_batch_returns_only_after_writer_success() {
        let stats = Arc::new(ComponentStats::new());
        let writes = Arc::new(Mutex::new(Vec::new()));
        let mut sink = PipelinedSink::new(
            FixedSerializer {
                rows: 2,
                payload: b"ok\n",
            },
            RecordingWriter {
                writes: Arc::clone(&writes),
            },
            Arc::clone(&stats),
            PipelineConfig {
                num_buffers: 1,
                buf_capacity: 16,
            },
        )
        .expect("sink");

        let batch = empty_batch();
        let result = sink.send_batch(&batch, &metadata()).await;

        assert!(result.is_ok(), "result: {result:?}");
        assert_eq!(
            writes.lock().expect("recording writer mutex").as_slice(),
            &[b"ok\n".to_vec()]
        );
        assert_eq!(stats.lines_total.load(Ordering::Relaxed), 2);
        assert_eq!(stats.bytes_total.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn send_batch_reports_writer_error_without_success_counters() {
        let stats = Arc::new(ComponentStats::new());
        let mut sink = PipelinedSink::new(
            FixedSerializer {
                rows: 2,
                payload: b"lost\n",
            },
            FailingWriter,
            Arc::clone(&stats),
            PipelineConfig {
                num_buffers: 1,
                buf_capacity: 16,
            },
        )
        .expect("sink");

        let batch = empty_batch();
        let result = sink.send_batch(&batch, &metadata()).await;

        assert!(
            matches!(result, SendResult::IoError(_)),
            "result: {result:?}"
        );
        assert_eq!(stats.lines_total.load(Ordering::Relaxed), 0);
        assert_eq!(stats.bytes_total.load(Ordering::Relaxed), 0);
    }
}

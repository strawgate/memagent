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

/// Pre-allocation chunk size: 64 MB.
///
/// When the file is opened (or needs more space), we ask the OS to reserve
/// this much contiguous space.  This avoids per-write metadata updates on
/// ext4/xfs/APFS that slow down sequential extending writes.
const PREALLOC_BYTES: i64 = 64 * 1024 * 1024;

/// A [`BatchWriter`] that writes to a file using blocking std::fs I/O.
///
/// Applies platform-specific optimizations on construction:
///
/// | Platform | Optimization |
/// |----------|-------------|
/// | Linux | `fallocate` pre-allocation, `POSIX_FADV_DONTNEED` after writes |
/// | macOS | `F_PREALLOCATE`, `F_NOCACHE` (bypass UBC for written pages) |
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
        Self {
            file,
            #[cfg(target_os = "linux")]
            bytes_written: 0,
        }
    }

    /// Apply one-time platform-specific optimizations to the file descriptor.
    fn apply_platform_hints(file: &std::fs::File) {
        #[cfg(target_os = "linux")]
        Self::apply_linux_hints(file);
        #[cfg(target_os = "macos")]
        Self::apply_macos_hints(file);
    }

    /// Linux: pre-allocate space with `fallocate` and set sequential advice.
    #[cfg(target_os = "linux")]
    fn apply_linux_hints(file: &std::fs::File) {
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();

        // Pre-allocate disk space to avoid metadata updates on every
        // extending write.  KEEP_SIZE means the visible file size is not
        // changed — only the underlying blocks are reserved.
        // SAFETY: fd is valid (owned by `file`), flags and offsets are correct.
        // fallocate failure is benign (returns -1, we ignore).
        unsafe {
            let _ = libc::fallocate(fd, libc::FALLOC_FL_KEEP_SIZE, 0, PREALLOC_BYTES);
        }

        // Tell the kernel we're doing sequential writes so it can optimize
        // read-ahead and page cache management.
        // SAFETY: fd is valid (owned by `file`), POSIX_FADV_SEQUENTIAL is an
        // advisory hint that cannot cause UB regardless of return value.
        unsafe {
            let _ = libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_SEQUENTIAL);
        }
    }

    /// macOS: set `F_NOCACHE` and pre-allocate with `F_PREALLOCATE`.
    #[cfg(target_os = "macos")]
    fn apply_macos_hints(file: &std::fs::File) {
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();

        // F_NOCACHE: bypass the Unified Buffer Cache for this fd.
        // Written pages won't linger in memory — good for a forwarder that
        // produces write-only data consumers won't re-read via this fd.
        // SAFETY: fd is valid (owned by `file`), F_NOCACHE with arg=1 is a
        // hint that cannot cause UB regardless of return value.
        unsafe {
            let _ = libc::fcntl(fd, libc::F_NOCACHE, 1i32);
        }

        // F_PREALLOCATE: reserve contiguous disk space.
        // This is macOS's equivalent of Linux fallocate — it avoids
        // fragmentation and repeated metadata updates during sequential
        // extending writes.
        // SAFETY: fd is valid (owned by `file`), fstore_t is zero-initialized
        // then fully populated.  fcntl(F_PREALLOCATE) reads the struct by
        // pointer and never writes through it.
        unsafe {
            let mut store: libc::fstore_t = std::mem::zeroed();
            store.fst_flags = libc::F_ALLOCATECONTIG;
            store.fst_posmode = libc::F_PEOFPOSMODE;
            store.fst_offset = 0;
            store.fst_length = PREALLOC_BYTES;
            let ret = libc::fcntl(fd, libc::F_PREALLOCATE, &store);
            if ret == -1 {
                // Contiguous allocation failed — retry allowing fragments.
                store.fst_flags = libc::F_ALLOCATEALL;
                let _ = libc::fcntl(fd, libc::F_PREALLOCATE, &store);
            }
        }
    }

    /// Linux: advise the kernel to drop page cache for already-written data.
    ///
    /// This prevents the forwarder from bloating the host's page cache with
    /// write-only log data that will never be re-read by this process.
    #[cfg(target_os = "linux")]
    fn advise_dontneed(&self, offset: u64, len: u64) {
        use std::os::unix::io::AsRawFd;
        let fd = self.file.as_raw_fd();
        // SAFETY: fd is valid (owned by `self.file`), offset and len describe
        // already-written data, POSIX_FADV_DONTNEED is an advisory hint.
        unsafe {
            let _ = libc::posix_fadvise(
                fd,
                offset as libc::off_t,
                len as libc::off_t,
                libc::POSIX_FADV_DONTNEED,
            );
        }
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

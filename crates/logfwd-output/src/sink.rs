//! Async `Sink` trait and `SendResult` for the v2 Arrow pipeline.
//!
//! The trait methods return `Pin<Box<dyn Future + Send>>` rather than
//! `impl Future + Send` so that the trait is **object-safe** and
//! `Box<dyn Sink>` can be used in heterogeneous collections such as
//! [`crate::FanOutSink`].  The boxing overhead is negligible for network
//! sinks (OtlpSink, JsonLinesSink) and tiny for the synchronous StdoutSink.
//!
//! The synchronous [`crate::OutputSink`] trait remains in place and coexists
//! with this new async interface throughout the migration period.

use std::io;
use std::pin::Pin;
use std::time::Duration;

use arrow::record_batch::RecordBatch;

use super::BatchMetadata;

// ---------------------------------------------------------------------------
// SendResult
// ---------------------------------------------------------------------------

/// The outcome of a [`Sink::send_batch`] call.
pub enum SendResult {
    /// The batch was accepted and delivered.
    Ok,
    /// The batch could not be delivered right now; retry after the given
    /// duration.
    RetryAfter(Duration),
    /// The batch was rejected and must not be retried.
    Rejected(String),
}

// Convenience alias used throughout the crate.
pub(crate) type SendFut = Pin<Box<dyn std::future::Future<Output = io::Result<SendResult>> + Send>>;
pub(crate) type FlushFut = Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;

// ---------------------------------------------------------------------------
// Sink trait
// ---------------------------------------------------------------------------

/// Async output sink for the v2 Arrow pipeline.
///
/// The trait is **object-safe**: all async methods return
/// `Pin<Box<dyn Future + Send>>` so that `Box<dyn Sink>` can hold
/// heterogeneous sinks (e.g. inside [`crate::FanOutSink`]).
///
/// Implementations are expected to do all synchronous work (encoding,
/// serialisation, compression) before returning the future so that the
/// future itself only contains owned data.
///
/// The existing synchronous [`crate::OutputSink`] trait coexists with this
/// interface during the migration from v1 to v2.
pub trait Sink: Send {
    /// Serialize and send a batch of log records.
    fn send_batch(
        &mut self,
        batch: &RecordBatch,
        metadata: &BatchMetadata,
    ) -> SendFut;

    /// Flush any internally buffered data to the destination.
    fn flush(&mut self) -> FlushFut;

    /// Return the human-readable name of this sink (from config).
    fn name(&self) -> &str;

    /// Gracefully shut down the sink, flushing and releasing resources.
    fn shutdown(&mut self) -> FlushFut;
}

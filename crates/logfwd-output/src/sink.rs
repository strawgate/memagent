//! Async `Sink` trait and `SendResult` for the v2 Arrow pipeline.
//!
//! Uses Rust's native async-fn-in-traits (stabilised in 1.75; no `async_trait`
//! crate required). The workspace MSRV is effectively Rust 1.86+ (edition
//! 2024), so native async traits are safe to use here.
//!
//! Async methods are desugared to `-> impl Future<Output = …> + Send` to keep
//! the `Send` bound explicit on public trait methods, as required by clippy's
//! `async_fn_in_trait` lint.
//!
//! The synchronous [`crate::OutputSink`] trait remains in place and coexists
//! with this new async interface throughout the migration period.

use std::future::Future;
use std::io;
use std::time::Duration;

use arrow::record_batch::RecordBatch;

use super::BatchMetadata;

// ---------------------------------------------------------------------------
// SendResult
// ---------------------------------------------------------------------------

/// The outcome of a [`Sink::send_batch`] call.
#[non_exhaustive]
pub enum SendResult {
    /// The batch was accepted and delivered.
    Ok,
    /// The batch could not be delivered right now; retry after the given
    /// duration.
    RetryAfter(Duration),
    /// The batch was rejected and must not be retried.
    Rejected(String),
}

// ---------------------------------------------------------------------------
// Sink trait
// ---------------------------------------------------------------------------

/// Async output sink for the v2 Arrow pipeline.
///
/// Implementors must be `Send` so they can be moved across thread boundaries.
/// Each async method returns `impl Future + Send` rather than using `async fn`
/// directly, which makes the `Send` bound on the returned future explicit.
///
/// The existing synchronous [`crate::OutputSink`] trait coexists with this
/// interface during the migration from v1 to v2.
pub trait Sink: Send {
    /// Serialize and send a batch of log records.
    fn send_batch(
        &mut self,
        batch: &RecordBatch,
        metadata: &BatchMetadata,
    ) -> impl Future<Output = io::Result<SendResult>> + Send;

    /// Flush any internally buffered data to the destination.
    fn flush(&mut self) -> impl Future<Output = io::Result<()>> + Send;

    /// Return the human-readable name of this sink (from config).
    fn name(&self) -> &str;

    /// Gracefully shut down the sink, flushing and releasing resources.
    fn shutdown(&mut self) -> impl Future<Output = io::Result<()>> + Send;
}

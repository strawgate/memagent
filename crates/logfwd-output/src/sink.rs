//! Async `Sink` trait and `SendResult` for the v2 Arrow pipeline.
//!
//! The `Sink` trait uses `Pin<Box<dyn Future>>` return types to remain
//! dyn-compatible, enabling `Box<dyn Sink>` without the `async_trait` crate.
//! This costs one heap allocation per call but allows heterogeneous sink
//! collections in the worker pool.
//!
//! The synchronous [`crate::OutputSink`] trait remains in place and coexists
//! with this new async interface throughout the migration period.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::time::Duration;

use arrow::record_batch::RecordBatch;

use super::{BatchMetadata, OutputSink};

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
/// Methods return `Pin<Box<dyn Future>>` so the trait is dyn-compatible and
/// `Box<dyn Sink>` works without the `async_trait` crate.
///
/// Implementors can define `async fn` wrappers internally and box them:
/// ```rust,ignore
/// fn send_batch<'a>(...) -> Pin<Box<dyn Future<...> + Send + 'a>> {
///     Box::pin(async move { /* impl */ })
/// }
/// ```
pub trait Sink: Send {
    /// Serialize and send a batch of log records.
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = io::Result<SendResult>> + Send + 'a>>;

    /// Flush any internally buffered data to the destination.
    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>>;

    /// Return the human-readable name of this sink (from config).
    fn name(&self) -> &str;

    /// Gracefully shut down the sink, flushing and releasing resources.
    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>>;
}

// ---------------------------------------------------------------------------
// SinkFactory trait
// ---------------------------------------------------------------------------

/// Factory for creating new [`Sink`] instances.
///
/// The worker pool holds one `Arc<dyn SinkFactory>` and calls `create()` each
/// time it spawns a new worker. This lets each worker own an independent HTTP
/// client (and therefore connection pool) while sharing configuration.
pub trait SinkFactory: Send + Sync + 'static {
    /// Create a new sink instance. Called once per worker spawn.
    fn create(&self) -> io::Result<Box<dyn Sink>>;

    /// Human-readable name for logging.
    fn name(&self) -> &str;

    /// Returns `true` if the factory wraps a non-replicable resource (e.g. a
    /// pre-built sync sink) and can successfully call `create()` at most once.
    ///
    /// When `true`, the worker pool must use `max_workers = 1` and should set
    /// a very long (or infinite) idle timeout so the sole worker is never
    /// evicted — because if it exits, `create()` will return an error and the
    /// output permanently stops.
    fn is_single_use(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// SyncSinkAdapter
// ---------------------------------------------------------------------------

/// Wraps a synchronous [`OutputSink`] as an async [`Sink`].
///
/// Uses `tokio::task::block_in_place` so the sync call doesn't block the
/// async executor. Suitable for sinks that haven't yet been ported to reqwest.
pub struct SyncSinkAdapter {
    inner: Box<dyn OutputSink>,
}

impl SyncSinkAdapter {
    pub fn new(inner: Box<dyn OutputSink>) -> Self {
        SyncSinkAdapter { inner }
    }
}

// ---------------------------------------------------------------------------
// Kani proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod kani_proofs {
    use super::*;
    use std::time::Duration;

    /// SendResult::Ok is not a RetryAfter or Rejected variant.
    #[kani::proof]
    fn verify_ok_is_not_retry_or_rejection() {
        let result = SendResult::Ok;
        assert!(matches!(result, SendResult::Ok));
        assert!(!matches!(result, SendResult::RetryAfter(_)));
        assert!(!matches!(result, SendResult::Rejected(_)));
        kani::cover!(true, "Ok variant exercised");
    }

    /// SendResult::RetryAfter preserves the Duration for any bounded duration.
    #[kani::proof]
    fn verify_retry_after_preserves_duration() {
        let secs: u64 = kani::any();
        kani::assume(secs <= 3600);
        let d = Duration::from_secs(secs);
        let result = SendResult::RetryAfter(d);
        assert!(!matches!(result, SendResult::Ok));
        assert!(!matches!(result, SendResult::Rejected(_)));
        if let SendResult::RetryAfter(dur) = result {
            assert_eq!(dur.as_secs(), secs);
        }
        kani::cover!(secs > 0, "non-zero retry duration");
        kani::cover!(secs == 0, "zero retry duration");
    }

    /// SendResult::Rejected is not Ok or RetryAfter.
    #[kani::proof]
    fn verify_rejected_is_not_ok_or_retry() {
        let result = SendResult::Rejected("error".to_string());
        assert!(!matches!(result, SendResult::Ok));
        assert!(!matches!(result, SendResult::RetryAfter(_)));
        assert!(matches!(result, SendResult::Rejected(_)));
        kani::cover!(true, "Rejected variant exercised");
    }

    /// The three SendResult variants are mutually exclusive.
    #[kani::proof]
    fn verify_send_result_variants_are_mutually_exclusive() {
        let ok = SendResult::Ok;
        let retry = SendResult::RetryAfter(Duration::from_millis(100));
        let rejected = SendResult::Rejected("fail".to_string());

        assert!(matches!(ok, SendResult::Ok));
        assert!(!matches!(ok, SendResult::RetryAfter(_)));
        assert!(!matches!(ok, SendResult::Rejected(_)));

        assert!(!matches!(retry, SendResult::Ok));
        assert!(matches!(retry, SendResult::RetryAfter(_)));
        assert!(!matches!(retry, SendResult::Rejected(_)));

        assert!(!matches!(rejected, SendResult::Ok));
        assert!(!matches!(rejected, SendResult::RetryAfter(_)));
        assert!(matches!(rejected, SendResult::Rejected(_)));

        kani::cover!(true, "all three variants are mutually exclusive");
    }
}

impl Sink for SyncSinkAdapter {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = io::Result<SendResult>> + Send + 'a>> {
        // SAFETY: block_in_place is safe here because we're within a
        // multi-threaded tokio runtime (logfwd always uses rt-multi-thread).
        Box::pin(async move {
            tokio::task::block_in_place(|| {
                self.inner
                    .send_batch(batch, metadata)
                    .map(|()| SendResult::Ok)
            })
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move { tokio::task::block_in_place(|| self.inner.flush()) })
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        // Flush buffered data before the worker exits. Sync sinks that buffer
        // (e.g. file sinks) would silently lose the last batch without this.
        Box::pin(async move { tokio::task::block_in_place(|| self.inner.flush()) })
    }
}

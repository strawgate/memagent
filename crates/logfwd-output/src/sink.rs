//! Async `Sink` trait — the canonical output interface for the Arrow pipeline.
//!
//! The `Sink` trait uses `Pin<Box<dyn Future>>` return types to remain
//! dyn-compatible, enabling `Box<dyn Sink>` without the `async_trait` crate.
//! This costs one heap allocation per call but allows heterogeneous sink
//! collections in the worker pool.
//!
//! The synchronous [`crate::OutputSink`] trait is **deprecated**. New sinks
//! should implement `Sink` directly. Legacy sync sinks are bridged via
//! [`SyncSinkAdapter`].

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::time::Duration;

use arrow::record_batch::RecordBatch;

#[allow(deprecated)]
use super::{BatchMetadata, OutputSink};

// ---------------------------------------------------------------------------
// SendResult
// ---------------------------------------------------------------------------

/// The outcome of a [`Sink::send_batch`] call.
#[must_use]
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
/// async executor. Suitable for sinks that haven't yet been ported to the
/// async [`Sink`] trait.
#[allow(deprecated)]
pub struct SyncSinkAdapter {
    inner: Box<dyn OutputSink>,
}

#[allow(deprecated)]
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

#[allow(deprecated)]
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

// ---------------------------------------------------------------------------
// OnceAsyncFactory
// ---------------------------------------------------------------------------

use std::sync::Mutex;

/// `SinkFactory` that wraps a single pre-built [`Sink`].
///
/// On the first `create()` call it transfers ownership of the sink to the
/// caller; subsequent calls return an error. This enforces `max_workers = 1`.
///
/// Unlike [`super::OnceFactory`] (which wraps a deprecated [`OutputSink`]
/// through [`SyncSinkAdapter`]), this wrapper accepts an async `Sink` directly
/// — no `block_in_place` overhead.
pub struct OnceAsyncFactory {
    name: String,
    inner: Mutex<Option<Box<dyn Sink>>>,
}

impl OnceAsyncFactory {
    /// Create a factory that yields `sink` on the first `create()` call and
    /// returns an error on every subsequent call, enforcing single-worker
    /// semantics.
    pub fn new(name: String, sink: Box<dyn Sink>) -> Self {
        OnceAsyncFactory {
            name,
            inner: Mutex::new(Some(sink)),
        }
    }
}

impl SinkFactory for OnceAsyncFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("OnceAsyncFactory mutex poisoned"))?;
        match guard.take() {
            Some(s) => Ok(s),
            None => Err(io::Error::other(
                "OnceAsyncFactory: sink already consumed (max_workers must be 1)",
            )),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn is_single_use(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A trivial async sink for testing OnceAsyncFactory.
    struct StubSink;

    impl Sink for StubSink {
        fn send_batch<'a>(
            &'a mut self,
            _batch: &'a RecordBatch,
            _metadata: &'a BatchMetadata,
        ) -> Pin<Box<dyn Future<Output = io::Result<SendResult>> + Send + 'a>> {
            Box::pin(async { Ok(SendResult::Ok) })
        }

        fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn name(&self) -> &str {
            "stub"
        }

        fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[test]
    fn once_async_factory_creates_once() {
        let factory = OnceAsyncFactory::new("test".into(), Box::new(StubSink));
        assert_eq!(factory.name(), "test");
        assert!(factory.is_single_use());

        let sink = factory.create().expect("first create should succeed");
        assert_eq!(sink.name(), "stub");

        match factory.create() {
            Ok(_) => panic!("second create should fail"),
            Err(err) => assert!(
                err.to_string().contains("already consumed"),
                "error should mention 'already consumed': {err}"
            ),
        }
    }
}

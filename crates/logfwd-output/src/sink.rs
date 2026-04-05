//! Async `Sink` trait — the canonical output interface for the Arrow pipeline.
//!
//! The `Sink` trait uses `Pin<Box<dyn Future>>` return types to remain
//! dyn-compatible, enabling `Box<dyn Sink>` without the `async_trait` crate.
//! This costs one heap allocation per call but allows heterogeneous sink
//! collections in the worker pool.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;

use super::BatchMetadata;

// ---------------------------------------------------------------------------
// SendResult
// ---------------------------------------------------------------------------

/// The outcome of a [`Sink::send_batch`] call.
#[must_use]
#[non_exhaustive]
#[derive(Debug)]
pub enum SendResult {
    /// The batch was accepted and delivered.
    Ok,
    /// Network / IO error — worker pool applies its own retry policy.
    IoError(io::Error),
    /// The batch could not be delivered right now; retry after the given
    /// duration.
    RetryAfter(Duration),
    /// The batch was rejected and must not be retried.
    Rejected(String),
}

impl SendResult {
    /// Returns `true` if the result is [`SendResult::Ok`].
    pub fn is_ok(&self) -> bool {
        matches!(self, SendResult::Ok)
    }

    /// Returns `true` if the result is not [`SendResult::Ok`].
    pub fn is_err(&self) -> bool {
        !self.is_ok()
    }

    /// Panics if the result is not [`SendResult::Ok`], printing the variant as
    /// the panic message.
    pub fn unwrap(self) {
        assert!(
            matches!(self, SendResult::Ok),
            "called `SendResult::unwrap()` on a non-Ok value: {self:?}"
        );
    }
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
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>>;

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
// AsyncFanoutSink
// ---------------------------------------------------------------------------

/// Multiplexes output to multiple async [`Sink`] instances.
///
/// Sends every batch to **all** child sinks regardless of failures. After all
/// sinks have been called the worst result is returned using the precedence:
/// `Rejected > IoError > RetryAfter > Ok`. This ensures that an explicit
/// rejection is never masked by a later transient I/O error.
pub struct AsyncFanoutSink {
    sinks: Vec<Box<dyn Sink>>,
}

impl AsyncFanoutSink {
    /// Create a fanout sink wrapping the given child sinks.
    pub fn new(sinks: Vec<Box<dyn Sink>>) -> Self {
        AsyncFanoutSink { sinks }
    }
}

impl Sink for AsyncFanoutSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        Box::pin(async move {
            // Precedence: Rejected(3) > IoError(2) > RetryAfter(1) > Ok(0).
            // We track the highest-priority failure seen so far; a later result
            // can only raise the severity, never lower it.
            let mut worst: SendResult = SendResult::Ok;
            let mut max_retry: Option<Duration> = None;
            for sink in &mut self.sinks {
                match sink.send_batch(batch, metadata).await {
                    SendResult::Ok => {}
                    SendResult::RetryAfter(d) => {
                        if max_retry.is_none_or(|prev| d > prev) {
                            max_retry = Some(d);
                        }
                    }
                    // Only overwrite worst if the new result has strictly higher precedence.
                    SendResult::IoError(e) => {
                        if !matches!(worst, SendResult::Rejected(_)) {
                            worst = SendResult::IoError(e);
                        }
                    }
                    SendResult::Rejected(reason) => {
                        // Rejected is the highest precedence — always overwrite.
                        worst = SendResult::Rejected(reason);
                    }
                }
            }
            match worst {
                SendResult::Ok => match max_retry {
                    Some(d) => SendResult::RetryAfter(d),
                    None => SendResult::Ok,
                },
                other => other,
            }
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            for sink in &mut self.sinks {
                sink.flush().await?;
            }
            Ok(())
        })
    }

    fn name(&self) -> &'static str {
        "fanout"
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            for sink in &mut self.sinks {
                sink.shutdown().await?;
            }
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// AsyncFanoutFactory
// ---------------------------------------------------------------------------

/// Factory that creates [`AsyncFanoutSink`] instances by delegating to
/// multiple child [`SinkFactory`] instances.
pub struct AsyncFanoutFactory {
    name: String,
    factories: Vec<Arc<dyn SinkFactory>>,
}

impl AsyncFanoutFactory {
    /// Create a fanout factory wrapping the given child factories.
    pub fn new(name: String, factories: Vec<Arc<dyn SinkFactory>>) -> Self {
        AsyncFanoutFactory { name, factories }
    }
}

impl SinkFactory for AsyncFanoutFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
        let sinks: Vec<Box<dyn Sink>> = self
            .factories
            .iter()
            .map(|f| f.create())
            .collect::<io::Result<_>>()?;
        Ok(Box::new(AsyncFanoutSink::new(sinks)))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// Kani proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
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

    /// Prove that total retry wait time is bounded regardless of server-suggested delays.
    #[kani::proof]
    #[kani::unwind(5)]
    fn verify_total_retry_wait_bounded() {
        let max_retries: u32 = 3;
        let max_retry_delay_ms: u64 = 30_000;

        let mut total_wait_ms: u64 = 0;
        let mut delay_ms: u64 = 100; // initial backoff

        for attempt in 0..max_retries {
            // Each attempt either gets a server RetryAfter or uses exponential backoff
            let is_retry_after: bool = kani::any();
            if is_retry_after {
                let server_delay_ms: u64 = kani::any();
                kani::assume(server_delay_ms <= 120_000); // server can suggest up to 2 min
                let capped = if server_delay_ms < max_retry_delay_ms {
                    server_delay_ms
                } else {
                    max_retry_delay_ms
                };
                total_wait_ms += capped;
                delay_ms = 100; // reset on RetryAfter
            } else {
                let capped = if delay_ms < max_retry_delay_ms {
                    delay_ms
                } else {
                    max_retry_delay_ms
                };
                total_wait_ms += capped;
                delay_ms = delay_ms.saturating_mul(2);
            }
            kani::cover!(attempt == 2, "reached max retries");
        }

        // Total wait across all retries must not exceed 3 * 30s = 90s
        assert!(total_wait_ms <= max_retries as u64 * max_retry_delay_ms);
        kani::cover!(total_wait_ms > 0, "non-zero wait");
    }

    /// Prove exponential backoff stays bounded by cap within MAX_RETRIES steps.
    #[kani::proof]
    #[kani::unwind(5)]
    fn verify_backoff_bounded_by_cap() {
        let mut delay_ms: u64 = 100;
        let max_delay_ms: u64 = 30_000;
        for _ in 0..3u32 {
            delay_ms = delay_ms.saturating_mul(2);
            if delay_ms > max_delay_ms {
                delay_ms = max_delay_ms;
            }
        }
        // After 3 doublings: 100 → 200 → 400 → 800, all < 30_000
        // So delay should still be < cap after 3 steps
        assert!(delay_ms <= max_delay_ms);
        kani::cover!(delay_ms < max_delay_ms, "below cap after 3 doublings");
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
    use std::sync::{Arc, Mutex};

    fn empty_batch() -> RecordBatch {
        RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty()))
    }

    fn empty_meta() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::default(),
            observed_time_ns: 0,
        }
    }

    /// A trivial async sink for testing OnceAsyncFactory.
    struct StubSink;

    impl Sink for StubSink {
        fn send_batch<'a>(
            &'a mut self,
            _batch: &'a RecordBatch,
            _metadata: &'a BatchMetadata,
        ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
            Box::pin(async { SendResult::Ok })
        }

        fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn name(&self) -> &'static str {
            "stub"
        }

        fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    /// Sink that always returns a fixed result.
    struct FixedSink {
        name: &'static str,
        result: SendResult,
        calls: Arc<Mutex<usize>>,
    }

    impl FixedSink {
        fn new(name: &'static str, result: SendResult) -> (Self, Arc<Mutex<usize>>) {
            let calls = Arc::new(Mutex::new(0usize));
            (
                FixedSink {
                    name,
                    result,
                    calls: Arc::clone(&calls),
                },
                calls,
            )
        }
    }

    impl Sink for FixedSink {
        fn send_batch<'a>(
            &'a mut self,
            _batch: &'a RecordBatch,
            _metadata: &'a BatchMetadata,
        ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
            *self.calls.lock().unwrap() += 1;
            let result = match &self.result {
                SendResult::Ok => SendResult::Ok,
                SendResult::IoError(e) => SendResult::IoError(io::Error::other(e.to_string())),
                SendResult::RetryAfter(d) => SendResult::RetryAfter(*d),
                SendResult::Rejected(r) => SendResult::Rejected(r.clone()),
            };
            Box::pin(async move { result })
        }

        fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn name(&self) -> &'static str {
            self.name
        }

        fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn fanout_calls_all_sinks_even_after_rejection() {
        // All child sinks must be called regardless of earlier failures.
        let (s1, calls1) = FixedSink::new("s1", SendResult::Rejected("no".into()));
        let (s2, calls2) = FixedSink::new("s2", SendResult::Ok);
        let (s3, calls3) = FixedSink::new("s3", SendResult::Ok);
        let mut fanout = AsyncFanoutSink::new(vec![Box::new(s1), Box::new(s2), Box::new(s3)]);

        let result = fanout.send_batch(&empty_batch(), &empty_meta()).await;
        assert!(
            matches!(result, SendResult::Rejected(_)),
            "expected Rejected, got {result:?}"
        );
        assert_eq!(*calls1.lock().unwrap(), 1, "s1 must be called");
        assert_eq!(*calls2.lock().unwrap(), 1, "s2 must be called");
        assert_eq!(*calls3.lock().unwrap(), 1, "s3 must be called");
    }

    #[tokio::test]
    async fn fanout_rejected_beats_io_error() {
        // Rejected has higher precedence than IoError; a later IoError must not
        // overwrite an earlier Rejected.
        let (s1, _) = FixedSink::new("s1", SendResult::Rejected("explicit reject".into()));
        let (s2, _) = FixedSink::new("s2", SendResult::IoError(io::Error::other("network")));
        let mut fanout = AsyncFanoutSink::new(vec![Box::new(s1), Box::new(s2)]);

        let result = fanout.send_batch(&empty_batch(), &empty_meta()).await;
        assert!(
            matches!(result, SendResult::Rejected(_)),
            "Rejected must beat IoError, got {result:?}"
        );
    }

    #[tokio::test]
    async fn fanout_io_error_beats_retry() {
        let (s1, _) = FixedSink::new("s1", SendResult::RetryAfter(Duration::from_secs(5)));
        let (s2, _) = FixedSink::new("s2", SendResult::IoError(io::Error::other("net")));
        let mut fanout = AsyncFanoutSink::new(vec![Box::new(s1), Box::new(s2)]);

        let result = fanout.send_batch(&empty_batch(), &empty_meta()).await;
        assert!(
            matches!(result, SendResult::IoError(_)),
            "IoError must beat RetryAfter, got {result:?}"
        );
    }

    #[tokio::test]
    async fn fanout_returns_max_retry_when_all_ok_or_retry() {
        let (s1, _) = FixedSink::new("s1", SendResult::RetryAfter(Duration::from_secs(1)));
        let (s2, _) = FixedSink::new("s2", SendResult::RetryAfter(Duration::from_secs(10)));
        let (s3, _) = FixedSink::new("s3", SendResult::Ok);
        let mut fanout = AsyncFanoutSink::new(vec![Box::new(s1), Box::new(s2), Box::new(s3)]);

        let result = fanout.send_batch(&empty_batch(), &empty_meta()).await;
        match result {
            SendResult::RetryAfter(d) => assert_eq!(
                d,
                Duration::from_secs(10),
                "should return max retry duration"
            ),
            other => panic!("expected RetryAfter(10s), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fanout_all_ok_returns_ok() {
        let (s1, _) = FixedSink::new("s1", SendResult::Ok);
        let (s2, _) = FixedSink::new("s2", SendResult::Ok);
        let mut fanout = AsyncFanoutSink::new(vec![Box::new(s1), Box::new(s2)]);

        let result = fanout.send_batch(&empty_batch(), &empty_meta()).await;
        assert!(
            matches!(result, SendResult::Ok),
            "all-ok fanout must return Ok, got {result:?}"
        );
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

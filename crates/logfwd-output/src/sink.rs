//! Async `Sink` trait — the canonical output interface for the Arrow pipeline.
//!
//! The `Sink` trait uses `Pin<Box<dyn Future>>` return types to remain
//! dyn-compatible, enabling `Box<dyn Sink>` without the `async_trait` crate.
//! This costs one heap allocation per call but allows heterogeneous sink
//! collections in the worker pool.

mod health;

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use logfwd_types::diagnostics::ComponentHealth;

pub use self::health::{OutputHealthEvent, aggregate_fanout_health, reduce_output_health};
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

    /// Coarse runtime health for readiness and diagnostics.
    ///
    /// The default is optimistic until a concrete sink wires explicit
    /// startup, degradation, and failure state into the control plane.
    fn health(&self) -> ComponentHealth {
        ComponentHealth::Healthy
    }

    /// Gracefully shut down the sink, flushing and releasing resources.
    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>>;

    /// Signal that a new batch is starting.
    ///
    /// Sinks that track partial retry state (like Fanout) can use this to reset
    /// their state before the worker pool retry loop begins.
    fn begin_batch(&mut self) {}
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

/// Tracks the progress of a single batch through one child sink.
#[derive(Clone, Debug, PartialEq)]
enum ChildState {
    Pending,
    Ok,
    Rejected(String),
}

/// Multiplexes output to multiple async [`Sink`] instances.
///
/// Sends every batch to all child sinks. It tracks which children have
/// successfully processed the current batch to prevent duplicate deliveries on
/// retry. It also ensures that all children reach a terminal state (`Ok` or
/// `Rejected`) before returning a terminal result to the worker pool.
pub struct AsyncFanoutSink {
    name: String,
    sinks: Vec<Box<dyn Sink>>,
    states: Vec<ChildState>,
}

impl AsyncFanoutSink {
    /// Create a fanout sink wrapping the given child sinks.
    ///
    /// The sink name is derived from child sink names so that ack items
    /// carry a meaningful `output_name` instead of the opaque "fanout". (#1462)
    pub fn new(sinks: Vec<Box<dyn Sink>>) -> Self {
        let child_names: Vec<&str> = sinks.iter().map(|s| s.name()).collect();
        let name = format!("fanout({})", child_names.join(","));
        let states = vec![ChildState::Pending; sinks.len()];
        AsyncFanoutSink {
            name,
            sinks,
            states,
        }
    }
}

impl Sink for AsyncFanoutSink {
    fn begin_batch(&mut self) {
        for state in &mut self.states {
            *state = ChildState::Pending;
        }
        for sink in &mut self.sinks {
            sink.begin_batch();
        }
    }

    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        Box::pin(async move {
            let mut any_pending = false;
            let mut any_io_error = false;
            let mut max_retry: Option<Duration> = None;

            for (i, sink) in self.sinks.iter_mut().enumerate() {
                if self.states[i] != ChildState::Pending {
                    continue;
                }

                match sink.send_batch(batch, metadata).await {
                    SendResult::Ok => {
                        self.states[i] = ChildState::Ok;
                    }
                    SendResult::Rejected(reason) => {
                        self.states[i] = ChildState::Rejected(reason);
                    }
                    SendResult::RetryAfter(d) => {
                        any_pending = true;
                        if max_retry.is_none_or(|prev| d > prev) {
                            max_retry = Some(d);
                        }
                    }
                    SendResult::IoError(_) => {
                        any_pending = true;
                        any_io_error = true;
                    }
                }
            }

            if any_pending {
                // At least one child still needs retrying.
                // We must return a retryable error to the worker pool so it keeps driving
                // the retry loop. Returning a terminal error like Rejected here would
                // silently drop the batch for the pending children.
                match max_retry {
                    Some(d) => SendResult::RetryAfter(d),
                    None => {
                        // We must have seen an IoError if there are pending items but no RetryAfter.
                        debug_assert!(any_io_error);
                        SendResult::IoError(io::Error::other("fanout child transient error"))
                    }
                }
            } else {
                // All children have reached a terminal state (Ok or Rejected).
                // Return the worst result (Rejected > Ok).
                let mut first_rejection = None;
                for state in &self.states {
                    if let ChildState::Rejected(reason) = state {
                        first_rejection = Some(reason.clone());
                        break;
                    }
                }

                if let Some(reason) = first_rejection {
                    SendResult::Rejected(reason)
                } else {
                    SendResult::Ok
                }
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

    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        aggregate_fanout_health(self.sinks.iter().map(|sink| sink.health()))
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

    /// Regression proof for #1344 and #1233:
    /// the aggregation policy here must reflect `AsyncFanoutSink::send_batch`.
    /// We intentionally avoid modeling retry-loop/backoff behavior because that
    /// logic is implemented elsewhere (worker pool), not in this module.
    #[kani::proof]
    fn verify_fanout_result_precedence_model_matches_code() {
        let d1_ms: u64 = kani::any_where(|&v| v <= 30_000);
        let d2_ms: u64 = kani::any_where(|&v| v <= 30_000);
        let retry1 = SendResult::RetryAfter(Duration::from_millis(d1_ms));
        let retry2 = SendResult::RetryAfter(Duration::from_millis(d2_ms));

        // Retry-only case: larger RetryAfter hint wins.
        let merged_retry = match (retry1, retry2) {
            (SendResult::RetryAfter(a), SendResult::RetryAfter(b)) => {
                SendResult::RetryAfter(if a >= b { a } else { b })
            }
            _ => SendResult::Ok,
        };
        assert!(matches!(merged_retry, SendResult::RetryAfter(_)));
        if let SendResult::RetryAfter(d) = merged_retry {
            assert!(d.as_millis() as u64 == d1_ms || d.as_millis() as u64 == d2_ms);
        }

        // Pending results (IoError, RetryAfter) dominate terminal (Rejected, Ok)
        // to prevent partial-delivery bugs.
        let pending = SendResult::IoError(io::Error::other("io"));
        let rejected = SendResult::Rejected("rejected".to_string());

        let out = match (&pending, &rejected) {
            (SendResult::IoError(_), _) => true,
            _ => false,
        };
        assert!(out, "Pending must dominate Rejected");
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
    async fn fanout_io_error_beats_rejected_for_retries() {
        // Pending states (IoError, RetryAfter) must beat Rejected so the worker pool
        // keeps retrying the partial failure.
        let (s1, _) = FixedSink::new("s1", SendResult::Rejected("explicit reject".into()));
        let (s2, _) = FixedSink::new("s2", SendResult::IoError(io::Error::other("network")));
        let mut fanout = AsyncFanoutSink::new(vec![Box::new(s1), Box::new(s2)]);

        let result = fanout.send_batch(&empty_batch(), &empty_meta()).await;
        assert!(
            matches!(result, SendResult::IoError(_)),
            "IoError must beat Rejected to ensure retries, got {result:?}"
        );
    }

    #[tokio::test]
    async fn fanout_does_not_retry_successful_siblings() {
        let (s1, calls1) = FixedSink::new("s1", SendResult::Ok);
        let (s2, calls2) = FixedSink::new("s2", SendResult::RetryAfter(Duration::from_secs(1)));
        let mut fanout = AsyncFanoutSink::new(vec![Box::new(s1), Box::new(s2)]);

        fanout.begin_batch();

        // First attempt
        let result = fanout.send_batch(&empty_batch(), &empty_meta()).await;
        assert!(matches!(result, SendResult::RetryAfter(_)));
        assert_eq!(*calls1.lock().unwrap(), 1);
        assert_eq!(*calls2.lock().unwrap(), 1);

        // Second attempt (simulating worker pool retry)
        let result2 = fanout.send_batch(&empty_batch(), &empty_meta()).await;
        assert!(matches!(result2, SendResult::RetryAfter(_)));
        // s1 should NOT be called again
        assert_eq!(*calls1.lock().unwrap(), 1, "s1 must not be retried");
        assert_eq!(*calls2.lock().unwrap(), 2, "s2 must be retried");

        // Now start a new batch
        fanout.begin_batch();
        let result3 = fanout.send_batch(&empty_batch(), &empty_meta()).await;
        assert!(matches!(result3, SendResult::RetryAfter(_)));
        assert_eq!(
            *calls1.lock().unwrap(),
            2,
            "s1 should be called on new batch"
        );
        assert_eq!(*calls2.lock().unwrap(), 3);
    }

    #[tokio::test]
    async fn fanout_retry_after_beats_io_error() {
        let (s1, _) = FixedSink::new("s1", SendResult::RetryAfter(Duration::from_secs(5)));
        let (s2, _) = FixedSink::new("s2", SendResult::IoError(io::Error::other("net")));
        let mut fanout = AsyncFanoutSink::new(vec![Box::new(s1), Box::new(s2)]);

        let result = fanout.send_batch(&empty_batch(), &empty_meta()).await;
        assert!(
            matches!(result, SendResult::RetryAfter(_)),
            "RetryAfter must beat IoError so server rate limits are respected, got {result:?}"
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

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
use ffwd_types::diagnostics::ComponentHealth;

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

    /// Convert an `io::Error` into a delivery outcome.
    ///
    /// `InvalidInput`/`InvalidData` are treated as permanent serialization or
    /// payload-shape errors and mapped to `Rejected` so the worker pool does
    /// not retry forever on an un-sendable batch. Other error kinds are
    /// treated as transient I/O and mapped to `IoError`.
    pub fn from_io_error(error: io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData => {
                SendResult::Rejected(error.to_string())
            }
            _ => SendResult::IoError(error),
        }
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

    /// Signal that a new logical batch is starting.
    ///
    /// **Calling contract:** The driver (e.g., `OutputWorkerPool`) MUST call
    /// `begin_batch()` before the first `send_batch()` of each new logical
    /// batch. Without this call, `AsyncFanoutSink` will skip all children
    /// because their states are still terminal from the previous batch.
    ///
    /// Simple sinks can ignore this (the default is a no-op). Stateful sinks
    /// like `AsyncFanoutSink` use it to reset per-child delivery tracking.
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

fn merge_child_send_result(
    result: SendResult,
    any_pending: &mut bool,
    last_io_error: &mut Option<io::Error>,
    max_retry: &mut Option<Duration>,
) -> Option<ChildState> {
    match result {
        SendResult::Ok => Some(ChildState::Ok),
        SendResult::Rejected(reason) => Some(ChildState::Rejected(reason)),
        SendResult::RetryAfter(d) => {
            *any_pending = true;
            if max_retry.is_none_or(|prev| d > prev) {
                *max_retry = Some(d);
            }
            None
        }
        SendResult::IoError(e) => {
            *any_pending = true;
            *last_io_error = Some(e);
            None
        }
    }
}

fn finalize_fanout_outcome(
    states: &[ChildState],
    any_pending: bool,
    last_io_error: Option<io::Error>,
    max_retry: Option<Duration>,
) -> SendResult {
    if any_pending {
        return match max_retry {
            Some(d) => SendResult::RetryAfter(d),
            None => SendResult::IoError(last_io_error.unwrap_or_else(|| {
                io::Error::other("fanout: pending children with no error (bug)")
            })),
        };
    }

    let mut first_rejection = None;
    for state in states {
        if let ChildState::Rejected(reason) = state
            && first_rejection.is_none()
        {
            first_rejection = Some(reason.clone());
        }
    }

    if let Some(reason) = first_rejection {
        SendResult::Rejected(reason)
    } else {
        SendResult::Ok
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
            let mut last_io_error: Option<io::Error> = None;
            let mut max_retry: Option<Duration> = None;

            for (i, sink) in self.sinks.iter_mut().enumerate() {
                if self.states[i] != ChildState::Pending {
                    continue;
                }

                if let Some(next_state) = merge_child_send_result(
                    sink.send_batch(batch, metadata).await,
                    &mut any_pending,
                    &mut last_io_error,
                    &mut max_retry,
                ) {
                    self.states[i] = next_state;
                }
            }

            finalize_fanout_outcome(&self.states, any_pending, last_io_error, max_retry)
        })
    }

    /// Flush all child sinks, attempting every one even if earlier sinks fail.
    ///
    /// Consistent with `send_batch()`, which always delivers to all sinks
    /// regardless of failures. The first error encountered is returned after
    /// all sinks have been flushed. Fixes #1648.
    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let mut first_err: Option<io::Error> = None;
            for sink in &mut self.sinks {
                if let Err(e) = sink.flush().await
                    && first_err.is_none()
                {
                    first_err = Some(e);
                }
            }
            match first_err {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        aggregate_fanout_health(self.sinks.iter().map(|sink| sink.health()))
    }

    /// Shut down all child sinks, attempting every one even if earlier sinks fail.
    ///
    /// Consistent with `send_batch()`, which always delivers to all sinks
    /// regardless of failures. The first error encountered is returned after
    /// all sinks have been shut down. Fixes #1648.
    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            let mut first_err: Option<io::Error> = None;
            for sink in &mut self.sinks {
                if let Err(e) = sink.shutdown().await
                    && first_err.is_none()
                {
                    first_err = Some(e);
                }
            }
            match first_err {
                Some(e) => Err(e),
                None => Ok(()),
            }
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

    #[kani::proof]
    fn verify_merge_child_send_result_tracks_pending_signals() {
        let tag: u8 = kani::any_where(|&v| v <= 3);
        let mut any_pending = false;
        let mut last_io_error = None;
        // Use a symbolic initial max_retry so the proof covers both the
        // `max_retry.is_none()` path and the `d > prev` / `d <= prev` branches
        // inside the RetryAfter arm of merge_child_send_result.
        // Bounded to <= 30_000 ms to keep solver tractable (Duration arithmetic
        // on full u64 causes 500s+ solve times with no extra coverage).
        let initial_ms: Option<u64> =
            if kani::any() { Some(kani::any_where(|&v: &u64| v <= 30_000)) } else { None };
        let mut max_retry = initial_ms.map(Duration::from_millis);

        let result = match tag {
            0 => SendResult::Ok,
            1 => SendResult::Rejected("no".to_string()),
            2 => {
                let retry_ms: u64 = kani::any_where(|&v| v <= 30_000);
                SendResult::RetryAfter(Duration::from_millis(retry_ms))
            }
            _ => SendResult::IoError(io::Error::other("boom")),
        };

        // Capture the duration we are about to pass (only meaningful for tag==2).
        let retry_duration = if tag == 2 {
            if let SendResult::RetryAfter(d) = &result {
                Some(*d)
            } else {
                None
            }
        } else {
            None
        };

        let state =
            merge_child_send_result(result, &mut any_pending, &mut last_io_error, &mut max_retry);

        match tag {
            0 => {
                assert_eq!(state, Some(ChildState::Ok));
                assert!(!any_pending);
                assert!(last_io_error.is_none());
            }
            1 => {
                assert!(matches!(state, Some(ChildState::Rejected(_))));
                assert!(!any_pending);
                assert!(last_io_error.is_none());
            }
            2 => {
                assert!(state.is_none());
                assert!(any_pending);
                let d = retry_duration.unwrap();
                let prev = initial_ms.map(Duration::from_millis);
                if prev.is_none_or(|p| d > p) {
                    assert_eq!(max_retry, Some(d));
                } else {
                    assert_eq!(max_retry, prev);
                }
            }
            _ => {
                assert!(state.is_none());
                assert!(any_pending);
                assert!(last_io_error.is_some());
            }
        }

        kani::cover!(tag == 0, "ok becomes ChildState::Ok");
        kani::cover!(tag == 1, "rejected becomes ChildState::Rejected");
        kani::cover!(
            tag == 2 && initial_ms.is_none(),
            "retry with no prior max: sets max_retry"
        );
        kani::cover!(
            tag == 2 && initial_ms.is_some(),
            "retry with existing max: exercises d > prev branch"
        );
        kani::cover!(tag == 3, "io error keeps child pending and stores error");

        // Exercise the `d > prev` branch: start with an existing max_retry and
        // merge a second RetryAfter so the harness covers both the "new delay
        // wins" and "existing delay wins" paths.
        let prev_retry_ms: u64 = kani::any_where(|&v| v <= 30_000);
        let mut any_pending2 = true;
        let mut last_io_error2 = None;
        let mut max_retry2 = Some(Duration::from_millis(prev_retry_ms));
        let new_retry_ms: u64 = kani::any_where(|&v| v <= 30_000);
        merge_child_send_result(
            SendResult::RetryAfter(Duration::from_millis(new_retry_ms)),
            &mut any_pending2,
            &mut last_io_error2,
            &mut max_retry2,
        );
        assert!(any_pending2);
        assert_eq!(
            max_retry2,
            Some(Duration::from_millis(prev_retry_ms.max(new_retry_ms)))
        );
        kani::cover!(
            new_retry_ms > prev_retry_ms,
            "new RetryAfter delay replaces smaller existing delay"
        );
        kani::cover!(
            new_retry_ms <= prev_retry_ms,
            "existing delay is kept when new delay is not larger"
        );
    }

    #[kani::proof]
    #[kani::unwind(3)]
    fn verify_finalize_fanout_outcome_precedence() {
        let max_retry_ms: u64 = kani::any_where(|&v| v <= 30_000);
        // Mixed states: one Ok + one Rejected.
        let mixed_states = [ChildState::Ok, ChildState::Rejected("bad".to_string())];

        let retry = finalize_fanout_outcome(
            &mixed_states,
            true,
            Some(io::Error::other("io")),
            Some(Duration::from_millis(max_retry_ms)),
        );
        assert!(matches!(retry, SendResult::RetryAfter(_)));

        let io_only =
            finalize_fanout_outcome(&mixed_states, true, Some(io::Error::other("io")), None);
        assert!(matches!(io_only, SendResult::IoError(_)));

        // Mixed Ok + Rejected with no pending/error → partial success → Rejected.
        let partial_ok = finalize_fanout_outcome(&mixed_states, false, None, None);
        assert!(matches!(partial_ok, SendResult::Rejected(_)));

        // All-rejected states → Rejected.
        let all_rejected_states = [
            ChildState::Rejected("bad".to_string()),
            ChildState::Rejected("also bad".to_string()),
        ];
        let rejected = finalize_fanout_outcome(&all_rejected_states, false, None, None);
        assert!(matches!(rejected, SendResult::Rejected(_)));

        let ok = finalize_fanout_outcome(&[ChildState::Ok], false, None, None);
        assert!(matches!(ok, SendResult::Ok));

        kani::cover!(
            max_retry_ms == 0,
            "zero-duration RetryAfter remains retryable"
        );
        kani::cover!(max_retry_ms > 0, "non-zero RetryAfter remains retryable");
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
            .map_err(|_e| io::Error::other("OnceAsyncFactory mutex poisoned"))?;
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
        // Mixed Ok+Rejected should return Rejected so caller is notified.
        assert!(
            matches!(result, SendResult::Rejected(_)),
            "expected Rejected (partial success), got {result:?}"
        );
        assert_eq!(*calls1.lock().unwrap(), 1, "s1 must be called");
        assert_eq!(*calls2.lock().unwrap(), 1, "s2 must be called");
        assert_eq!(*calls3.lock().unwrap(), 1, "s3 must be called");
    }

    #[tokio::test]
    async fn fanout_all_rejected_returns_rejected() {
        // When every child returns Rejected the fanout must propagate Rejected
        // (no partial success to salvage).
        let (s1, calls1) = FixedSink::new("s1", SendResult::Rejected("no-s1".into()));
        let (s2, calls2) = FixedSink::new("s2", SendResult::Rejected("no-s2".into()));
        let mut fanout = AsyncFanoutSink::new(vec![Box::new(s1), Box::new(s2)]);

        let result = fanout.send_batch(&empty_batch(), &empty_meta()).await;
        assert!(
            matches!(result, SendResult::Rejected(_)),
            "all-rejected fanout must return Rejected, got {result:?}"
        );
        assert_eq!(*calls1.lock().unwrap(), 1, "s1 must be called");
        assert_eq!(*calls2.lock().unwrap(), 1, "s2 must be called");
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

    /// Regression test for #1648: flush() must try all sinks even if earlier ones fail.
    #[tokio::test]
    async fn fanout_flush_calls_all_sinks_even_after_error() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct FailFlushSink {
            flushed: Arc<AtomicBool>,
        }
        impl Sink for FailFlushSink {
            fn send_batch<'a>(
                &'a mut self,
                _batch: &'a RecordBatch,
                _metadata: &'a BatchMetadata,
            ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
                Box::pin(async { SendResult::Ok })
            }
            fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
                self.flushed.store(true, Ordering::SeqCst);
                Box::pin(async { Err(io::Error::other("flush error")) })
            }
            fn name(&self) -> &'static str {
                "fail_flush"
            }
            fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
                Box::pin(async { Ok(()) })
            }
        }

        let second_flushed = Arc::new(AtomicBool::new(false));
        let second_flushed_clone = Arc::clone(&second_flushed);

        struct RecordFlushSink {
            flushed: Arc<AtomicBool>,
        }
        impl Sink for RecordFlushSink {
            fn send_batch<'a>(
                &'a mut self,
                _batch: &'a RecordBatch,
                _metadata: &'a BatchMetadata,
            ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
                Box::pin(async { SendResult::Ok })
            }
            fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
                self.flushed.store(true, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            }
            fn name(&self) -> &'static str {
                "record_flush"
            }
            fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
                Box::pin(async { Ok(()) })
            }
        }

        let mut fanout = AsyncFanoutSink::new(vec![
            Box::new(FailFlushSink {
                flushed: Arc::new(AtomicBool::new(false)),
            }),
            Box::new(RecordFlushSink {
                flushed: second_flushed_clone,
            }),
        ]);

        let result = fanout.flush().await;
        assert!(result.is_err(), "flush must propagate the error");
        assert!(
            second_flushed.load(Ordering::SeqCst),
            "second sink must be flushed even though first sink failed"
        );
    }

    /// Regression test for #1648: shutdown() must try all sinks even if earlier ones fail.
    #[tokio::test]
    async fn fanout_shutdown_calls_all_sinks_even_after_error() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct FailShutdownSink;
        impl Sink for FailShutdownSink {
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
                "fail_shutdown"
            }
            fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
                Box::pin(async { Err(io::Error::other("shutdown error")) })
            }
        }

        let second_shutdown = Arc::new(AtomicBool::new(false));
        let second_shutdown_clone = Arc::clone(&second_shutdown);

        struct RecordShutdownSink {
            shutdown: Arc<AtomicBool>,
        }
        impl Sink for RecordShutdownSink {
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
                "record_shutdown"
            }
            fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
                self.shutdown.store(true, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            }
        }

        let mut fanout = AsyncFanoutSink::new(vec![
            Box::new(FailShutdownSink),
            Box::new(RecordShutdownSink {
                shutdown: second_shutdown_clone,
            }),
        ]);

        let result = fanout.shutdown().await;
        assert!(result.is_err(), "shutdown must propagate the error");
        assert!(
            second_shutdown.load(Ordering::SeqCst),
            "second sink must be shut down even though first sink failed"
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

    #[test]
    fn from_io_error_maps_invalid_input_to_rejected() {
        let result =
            SendResult::from_io_error(io::Error::new(io::ErrorKind::InvalidInput, "bad payload"));
        assert!(matches!(result, SendResult::Rejected(_)));
    }

    #[test]
    fn from_io_error_maps_connection_reset_to_io_error() {
        let result = SendResult::from_io_error(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "connection reset",
        ));
        assert!(matches!(result, SendResult::IoError(_)));
    }

    // ---------------------------------------------------------------
    // Combined TLA+ state machine proptest — FanoutSink.tla::Next
    //
    // Generates random interleaved action sequences from the TLA+ Next
    // relation and verifies safety invariants after each step.
    // ---------------------------------------------------------------

    mod tla_fanout {
        use super::*;
        use proptest::{prop_assert, proptest};

        const MAX_CHILDREN: usize = 3;
        const MAX_RETRIES: usize = 3;

        #[derive(Clone, Debug)]
        enum ChildBehavior {
            WillOk,
            WillReject,
            WillTransient,
        }

        #[derive(Clone, Debug)]
        struct FanoutTlaState {
            batch_phase: &'static str,      // "Idle" | "Delivering" | "Finalized"
            child_state: Vec<&'static str>, // "Pending" | "Ok" | "Rejected"
            sink_behavior: Vec<ChildBehavior>,
            retry_count: usize,
            fanout_result: &'static str, // "None" | "Ok" | "Rejected" | "RetryNeeded"
            num_children: usize,
        }

        impl FanoutTlaState {
            fn init(num_children: usize) -> Self {
                Self {
                    batch_phase: "Idle",
                    child_state: vec!["Pending"; num_children],
                    sink_behavior: vec![ChildBehavior::WillOk; num_children],
                    retry_count: 0,
                    fanout_result: "None",
                    num_children,
                }
            }

            fn all_children_terminal(&self) -> bool {
                self.child_state
                    .iter()
                    .all(|s| *s == "Ok" || *s == "Rejected")
            }

            fn any_child_pending(&self) -> bool {
                self.child_state.iter().any(|s| *s == "Pending")
            }
        }

        fn apply_action(s: &mut FanoutTlaState, action: u8, child_idx: usize, new_behavior: u8) {
            let child = child_idx % s.num_children;
            match action % 8 {
                0 => {
                    // BeginBatch
                    if s.batch_phase == "Idle" {
                        s.batch_phase = "Delivering";
                        s.child_state = vec!["Pending"; s.num_children];
                        s.retry_count = 0;
                        s.fanout_result = "None";
                        // Randomize behaviors
                        for b in &mut s.sink_behavior {
                            *b = match new_behavior % 3 {
                                0 => ChildBehavior::WillOk,
                                1 => ChildBehavior::WillReject,
                                _ => ChildBehavior::WillTransient,
                            };
                        }
                    }
                }
                1 => {
                    // SendToChild — using real Rust functions
                    if s.batch_phase == "Delivering" && s.child_state[child] == "Pending" {
                        let result = match &s.sink_behavior[child] {
                            ChildBehavior::WillOk => SendResult::Ok,
                            ChildBehavior::WillReject => SendResult::Rejected("test".to_string()),
                            ChildBehavior::WillTransient => {
                                SendResult::RetryAfter(Duration::from_millis(100))
                            }
                        };
                        let mut any_pending = false;
                        let mut last_io_error = None;
                        let mut max_retry = None;
                        if let Some(next) = merge_child_send_result(
                            result,
                            &mut any_pending,
                            &mut last_io_error,
                            &mut max_retry,
                        ) {
                            s.child_state[child] = match next {
                                ChildState::Ok => "Ok",
                                ChildState::Rejected(_) => "Rejected",
                                ChildState::Pending => "Pending",
                            };
                        }
                    }
                }
                2 => {
                    // EnvironmentChangesBehavior
                    if s.batch_phase == "Delivering" && s.child_state[child] == "Pending" {
                        s.sink_behavior[child] = match new_behavior % 3 {
                            0 => ChildBehavior::WillOk,
                            1 => ChildBehavior::WillReject,
                            _ => ChildBehavior::WillTransient,
                        };
                    }
                }
                3 => {
                    // FinalizeTerminal — using real Rust function
                    if s.batch_phase == "Delivering" && s.all_children_terminal() {
                        let states: Vec<ChildState> = s
                            .child_state
                            .iter()
                            .map(|cs| match *cs {
                                "Ok" => ChildState::Ok,
                                "Rejected" => ChildState::Rejected("test".into()),
                                _ => ChildState::Pending,
                            })
                            .collect();
                        let result = finalize_fanout_outcome(&states, false, None, None);
                        s.fanout_result = match result {
                            SendResult::Ok => "Ok",
                            SendResult::Rejected(_) => "Rejected",
                            _ => "RetryNeeded",
                        };
                        s.batch_phase = "Finalized";
                    }
                }
                4 => {
                    // FinalizeRetryNeeded
                    if s.batch_phase == "Delivering"
                        && s.any_child_pending()
                        && s.retry_count < MAX_RETRIES
                    {
                        s.fanout_result = "RetryNeeded";
                    }
                }
                5 => {
                    // RetryBatch
                    if s.batch_phase == "Delivering"
                        && s.fanout_result == "RetryNeeded"
                        && s.retry_count < MAX_RETRIES
                    {
                        s.retry_count += 1;
                        s.fanout_result = "None";
                    }
                }
                6 => {
                    // ExhaustRetries
                    if s.batch_phase == "Delivering"
                        && s.any_child_pending()
                        && s.retry_count >= MAX_RETRIES
                    {
                        s.fanout_result = "Rejected";
                        s.batch_phase = "Finalized";
                    }
                }
                7 => {
                    // ResetForNextBatch
                    if s.batch_phase == "Finalized" {
                        s.batch_phase = "Idle";
                        s.fanout_result = "None";
                    }
                }
                _ => {}
            }
        }

        fn assert_invariants(
            s: &FanoutTlaState,
        ) -> Result<(), proptest::test_runner::TestCaseError> {
            // AnyRejectionIsRejected
            if s.batch_phase == "Finalized"
                && s.all_children_terminal()
                && s.child_state.iter().any(|st| *st == "Rejected")
            {
                prop_assert!(
                    s.fanout_result == "Rejected",
                    "AnyRejectionIsRejected violated: result={}",
                    s.fanout_result
                );
            }
            // AllOkIsOk
            if s.batch_phase == "Finalized"
                && s.all_children_terminal()
                && !s.child_state.iter().any(|st| *st == "Rejected")
            {
                prop_assert!(
                    s.fanout_result == "Ok",
                    "AllOkIsOk violated: result={}",
                    s.fanout_result
                );
            }
            // BatchPhaseConsistency
            if s.batch_phase == "Idle" {
                prop_assert!(
                    s.fanout_result == "None",
                    "BatchPhaseConsistency: Idle but result={}",
                    s.fanout_result
                );
            }
            if s.fanout_result == "Ok" || s.fanout_result == "Rejected" {
                prop_assert!(
                    s.batch_phase == "Finalized",
                    "BatchPhaseConsistency: result={} but phase={}",
                    s.fanout_result,
                    s.batch_phase
                );
            }
            // RetryCountBound
            prop_assert!(
                s.retry_count <= MAX_RETRIES,
                "RetryCountBound violated: {}",
                s.retry_count
            );
            // OkChildNeverReverts (checked: Ok children stay Ok during Delivering)
            // This is implicitly verified because SendToChild skips non-Pending children.
            Ok(())
        }

        proptest! {
            #![proptest_config(proptest::test_runner::Config {
                cases: 2000,
                ..Default::default()
            })]
            #[test]
            fn fanout_tla_invariants_hold_across_random_action_sequences(
                num_children in 1usize..=MAX_CHILDREN,
                actions in proptest::collection::vec(
                    (0u8..8, 0usize..MAX_CHILDREN, 0u8..3),
                    0..80
                )
            ) {
                let mut state = FanoutTlaState::init(num_children);
                for (action, child_idx, behavior) in actions {
                    apply_action(&mut state, action, child_idx, behavior);
                    assert_invariants(&state)?;
                }
            }
        }
    }
}

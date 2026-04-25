//! Reusable test sink implementations for the ffwd pipeline.
//!
//! These sinks implement the async [`Sink`] trait and simulate various output
//! behaviors: discarding, slow I/O, frozen connections, and transient failures.

// The `Sink` trait requires `fn name(&self) -> &str`, but all test sinks
// return string literals.  Clippy's `unnecessary_literal_bound` fires on
// every impl because it wants `&'static str`, which is incompatible with
// the trait signature.
#![allow(clippy::unnecessary_literal_bound)]

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use tokio_util::sync::CancellationToken;

use ffwd_output::BatchMetadata;
use ffwd_output::sink::{SendResult, Sink};

/// A sink that discards all data.
pub struct DevNullSink;

impl Sink for DevNullSink {
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

    fn name(&self) -> &str {
        "devnull"
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

/// A sink that sleeps on each `send_batch` to simulate slow output.
pub struct SlowSink {
    /// How long to sleep on each `send_batch` call.
    pub delay: Duration,
}

impl Sink for SlowSink {
    fn send_batch<'a>(
        &'a mut self,
        _batch: &'a RecordBatch,
        _metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            SendResult::Ok
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn name(&self) -> &str {
        "slow"
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

/// A sink that blocks until a [`CancellationToken`] is cancelled.
/// Simulates a frozen output (hung network connection, deadlock).
pub struct FrozenSink {
    /// Token that must be cancelled to unblock the sink.
    pub release: CancellationToken,
}

impl Sink for FrozenSink {
    fn send_batch<'a>(
        &'a mut self,
        _batch: &'a RecordBatch,
        _metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        Box::pin(async move {
            self.release.cancelled().await;
            SendResult::Ok
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn name(&self) -> &str {
        "frozen"
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

/// A sink that fails the first N calls, then succeeds.
pub struct FailingSink {
    /// Number of initial calls that should fail.
    pub fail_count: u32,
    /// Internal call counter.
    pub calls: u32,
}

impl FailingSink {
    /// Create a new `FailingSink` that fails the first `fail_count` calls.
    pub fn new(fail_count: u32) -> Self {
        Self {
            fail_count,
            calls: 0,
        }
    }
}

impl Sink for FailingSink {
    fn send_batch<'a>(
        &'a mut self,
        _batch: &'a RecordBatch,
        _metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        self.calls += 1;
        let calls = self.calls;
        let fail_count = self.fail_count;
        Box::pin(async move {
            if calls <= fail_count {
                return SendResult::IoError(io::Error::other(format!(
                    "simulated output failure {calls}/{fail_count}",
                )));
            }
            SendResult::Ok
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn name(&self) -> &str {
        "failing"
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

/// A sink that counts the total number of rows received.
/// The counter is shared via an `Arc<AtomicU64>` so tests can inspect it
/// after the pipeline shuts down.
pub struct CountingSink {
    counter: Arc<AtomicU64>,
}

impl CountingSink {
    pub fn new(counter: Arc<AtomicU64>) -> Self {
        Self { counter }
    }
}

impl Sink for CountingSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        _metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        self.counter
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        Box::pin(async { SendResult::Ok })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn name(&self) -> &str {
        "counting"
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn dummy_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap()
    }

    fn dummy_metadata() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 0,
        }
    }

    #[tokio::test]
    async fn devnull_accepts_all() {
        let mut sink = DevNullSink;
        let batch = dummy_batch();
        let meta = dummy_metadata();
        assert!(matches!(
            sink.send_batch(&batch, &meta).await,
            SendResult::Ok
        ));
        assert!(sink.flush().await.is_ok());
        assert_eq!(sink.name(), "devnull");
    }

    #[tokio::test]
    async fn failing_sink_fails_then_succeeds() {
        let mut sink = FailingSink::new(2);
        let batch = dummy_batch();
        let meta = dummy_metadata();

        assert!(matches!(
            sink.send_batch(&batch, &meta).await,
            SendResult::IoError(_)
        )); // call 1
        assert!(matches!(
            sink.send_batch(&batch, &meta).await,
            SendResult::IoError(_)
        )); // call 2
        assert!(matches!(
            sink.send_batch(&batch, &meta).await,
            SendResult::Ok
        )); // call 3 — success
        assert!(matches!(
            sink.send_batch(&batch, &meta).await,
            SendResult::Ok
        )); // call 4 — success
    }

    #[tokio::test]
    async fn slow_sink_delays_send() {
        let delay = Duration::from_millis(50);
        let mut sink = SlowSink { delay };
        let batch = dummy_batch();
        let meta = dummy_metadata();

        let start = std::time::Instant::now();
        assert!(matches!(
            sink.send_batch(&batch, &meta).await,
            SendResult::Ok
        ));
        assert!(
            start.elapsed() >= delay,
            "SlowSink should sleep at least {:?}, but only {:?} elapsed",
            delay,
            start.elapsed()
        );
        assert!(sink.flush().await.is_ok());
        assert_eq!(sink.name(), "slow");
    }

    #[tokio::test]
    async fn frozen_sink_blocks_until_released() {
        let token = CancellationToken::new();
        let mut sink = FrozenSink {
            release: token.clone(),
        };
        let batch = dummy_batch();
        let meta = dummy_metadata();

        // Release the token from a spawned task after a short delay.
        let release_delay = Duration::from_millis(100);
        let t = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(release_delay).await;
            t.cancel();
        });

        let start = std::time::Instant::now();
        assert!(matches!(
            sink.send_batch(&batch, &meta).await,
            SendResult::Ok
        ));
        // Allow 20% scheduling jitter below the release delay.
        let min_expected = release_delay * 80 / 100;
        assert!(
            start.elapsed() >= min_expected,
            "FrozenSink should block until released, but only {:?} elapsed (expected >= {min_expected:?})",
            start.elapsed()
        );
        assert!(sink.flush().await.is_ok());
        assert_eq!(sink.name(), "frozen");
    }

    #[tokio::test]
    async fn counting_sink_counts_rows() {
        let counter = Arc::new(AtomicU64::new(0));
        let mut sink = CountingSink::new(Arc::clone(&counter));
        let batch = dummy_batch();
        let meta = dummy_metadata();

        assert!(matches!(
            sink.send_batch(&batch, &meta).await,
            SendResult::Ok
        ));
        assert!(matches!(
            sink.send_batch(&batch, &meta).await,
            SendResult::Ok
        ));
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }
}

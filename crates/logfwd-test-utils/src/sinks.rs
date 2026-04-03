//! Reusable test sink implementations for the logfwd pipeline.
//!
//! These sinks implement [`OutputSink`] and simulate various output behaviors:
//! discarding, slow I/O, frozen connections, and transient failures.

use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use tokio_util::sync::CancellationToken;

use logfwd_output::{BatchMetadata, OutputSink};

/// A sink that discards all data.
pub struct DevNullSink;

impl OutputSink for DevNullSink {
    fn send_batch(&mut self, _batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn name(&self) -> &'static str {
        "devnull"
    }
}

/// A sink that sleeps on each `send_batch` to simulate slow output.
pub struct SlowSink {
    /// How long to sleep on each `send_batch` call.
    pub delay: Duration,
}

impl OutputSink for SlowSink {
    fn send_batch(&mut self, _batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        std::thread::sleep(self.delay);
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn name(&self) -> &'static str {
        "slow"
    }
}

/// A sink that blocks until a [`CancellationToken`] is cancelled.
/// Simulates a frozen output (hung network connection, deadlock).
pub struct FrozenSink {
    /// Token that must be cancelled to unblock the sink.
    pub release: CancellationToken,
}

impl OutputSink for FrozenSink {
    fn send_batch(&mut self, _batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        // Block until released. In a real scenario this would be a
        // hung HTTP connection or a deadlocked mutex.
        while !self.release.is_cancelled() {
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn name(&self) -> &'static str {
        "frozen"
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

impl OutputSink for FailingSink {
    fn send_batch(&mut self, _batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        self.calls += 1;
        if self.calls <= self.fail_count {
            return Err(io::Error::other(format!(
                "simulated output failure {}/{}",
                self.calls, self.fail_count
            )));
        }
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn name(&self) -> &'static str {
        "failing"
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

impl OutputSink for CountingSink {
    fn send_batch(&mut self, batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        self.counter
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        Ok(())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn name(&self) -> &'static str {
        "counting"
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
            resource_attrs: Arc::new(vec![]),
            observed_time_ns: 0,
        }
    }

    #[test]
    fn devnull_accepts_all() {
        let mut sink = DevNullSink;
        assert!(sink.send_batch(&dummy_batch(), &dummy_metadata()).is_ok());
        assert!(sink.flush().is_ok());
        assert_eq!(sink.name(), "devnull");
    }

    #[test]
    fn failing_sink_fails_then_succeeds() {
        let mut sink = FailingSink::new(2);
        let batch = dummy_batch();
        let meta = dummy_metadata();

        assert!(sink.send_batch(&batch, &meta).is_err()); // call 1
        assert!(sink.send_batch(&batch, &meta).is_err()); // call 2
        assert!(sink.send_batch(&batch, &meta).is_ok()); // call 3 — success
        assert!(sink.send_batch(&batch, &meta).is_ok()); // call 4 — success
    }

    #[test]
    fn slow_sink_delays_send() {
        let delay = Duration::from_millis(50);
        let mut sink = SlowSink { delay };
        let batch = dummy_batch();
        let meta = dummy_metadata();

        let start = std::time::Instant::now();
        assert!(sink.send_batch(&batch, &meta).is_ok());
        assert!(
            start.elapsed() >= delay,
            "SlowSink should sleep at least {:?}, but only {:?} elapsed",
            delay,
            start.elapsed()
        );
        assert!(sink.flush().is_ok());
        assert_eq!(sink.name(), "slow");
    }

    #[test]
    fn frozen_sink_blocks_until_released() {
        let token = CancellationToken::new();
        let mut sink = FrozenSink {
            release: token.clone(),
        };
        let batch = dummy_batch();
        let meta = dummy_metadata();

        // Release the token from another thread after a short delay.
        let release_delay = Duration::from_millis(100);
        let t = token.clone();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(release_delay);
            t.cancel();
        });

        let start = std::time::Instant::now();
        assert!(sink.send_batch(&batch, &meta).is_ok());
        // Allow 20% scheduling jitter below the release delay.
        let min_expected = release_delay * 80 / 100;
        assert!(
            start.elapsed() >= min_expected,
            "FrozenSink should block until released, but only {:?} elapsed (expected >= {min_expected:?})",
            start.elapsed()
        );
        handle.join().expect("release thread should not panic");
        assert!(sink.flush().is_ok());
        assert_eq!(sink.name(), "frozen");
    }
}

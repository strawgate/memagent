//! Instrumented sink with programmable failure scripts for simulation testing.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use logfwd_output::BatchMetadata;
use logfwd_output::sink::{SendResult, Sink};

/// What the sink should do on a given call.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum FailureAction {
    /// Succeed immediately.
    Succeed,
    /// Return RetryAfter with the given duration.
    RetryAfter(Duration),
    /// Return an IO error with the given kind.
    IoError(io::ErrorKind),
    /// Reject with the given reason.
    Reject(String),
    /// Succeed after a delay (simulates slow delivery).
    Delay(Duration),
    /// Panic during send (simulates buggy sink implementation).
    Panic,
}

/// Sink that follows a script of actions and records all calls.
///
/// Each `send_batch` call consumes the next action from the script.
/// When the script is exhausted, subsequent calls succeed immediately.
/// All calls are counted via shared atomic counters for test assertions.
pub struct InstrumentedSink {
    script: Vec<FailureAction>,
    call_index: usize,
    delivered_rows: Arc<AtomicU64>,
    call_count: Arc<AtomicU64>,
}

#[allow(dead_code)]
impl InstrumentedSink {
    pub fn new(script: Vec<FailureAction>) -> Self {
        Self {
            script,
            call_index: 0,
            delivered_rows: Arc::new(AtomicU64::new(0)),
            call_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create a sink that always succeeds.
    pub fn always_succeed() -> Self {
        Self::new(vec![])
    }

    /// Get shared counter for delivered rows.
    pub fn delivered_counter(&self) -> Arc<AtomicU64> {
        self.delivered_rows.clone()
    }

    /// Get shared counter for total calls.
    pub fn call_counter(&self) -> Arc<AtomicU64> {
        self.call_count.clone()
    }

    fn next_action(&mut self) -> FailureAction {
        let idx = self.call_index;
        self.call_index += 1;
        if idx < self.script.len() {
            self.script[idx].clone()
        } else {
            FailureAction::Succeed
        }
    }
}

/// Factory that creates InstrumentedSink instances for multi-worker pool testing.
///
/// Each worker gets its own sink with an independent script, but all share
/// the same delivery and call counters. The factory pops scripts from a queue;
/// when exhausted, new workers get an always-succeed sink.
pub struct InstrumentedSinkFactory {
    scripts: std::sync::Mutex<Vec<Vec<FailureAction>>>,
    delivered_rows: Arc<AtomicU64>,
    call_count: Arc<AtomicU64>,
}

#[allow(dead_code)]
impl InstrumentedSinkFactory {
    pub fn new(per_worker_scripts: Vec<Vec<FailureAction>>) -> Self {
        let delivered_rows = Arc::new(AtomicU64::new(0));
        let call_count = Arc::new(AtomicU64::new(0));
        Self {
            scripts: std::sync::Mutex::new(per_worker_scripts),
            delivered_rows,
            call_count,
        }
    }

    pub fn delivered_counter(&self) -> Arc<AtomicU64> {
        self.delivered_rows.clone()
    }

    pub fn call_counter(&self) -> Arc<AtomicU64> {
        self.call_count.clone()
    }
}

impl logfwd_output::SinkFactory for InstrumentedSinkFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
        let script = self.scripts.lock().unwrap().pop().unwrap_or_default();
        let mut sink = InstrumentedSink::new(script);
        sink.delivered_rows = self.delivered_rows.clone();
        sink.call_count = self.call_count.clone();
        Ok(Box::new(sink))
    }

    fn name(&self) -> &str {
        "instrumented-factory"
    }

    fn is_single_use(&self) -> bool {
        false
    }
}

impl Sink for InstrumentedSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        _metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = io::Result<SendResult>> + Send + 'a>> {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        let action = self.next_action();
        let rows = batch.num_rows() as u64;
        let delivered = self.delivered_rows.clone();

        Box::pin(async move {
            match action {
                FailureAction::Succeed => {
                    delivered.fetch_add(rows, Ordering::Relaxed);
                    Ok(SendResult::Ok)
                }
                FailureAction::RetryAfter(dur) => Ok(SendResult::RetryAfter(dur)),
                FailureAction::IoError(kind) => Err(io::Error::new(kind, "simulated failure")),
                FailureAction::Reject(reason) => Ok(SendResult::Rejected(reason)),
                FailureAction::Delay(dur) => {
                    tokio::time::sleep(dur).await;
                    delivered.fetch_add(rows, Ordering::Relaxed);
                    Ok(SendResult::Ok)
                }
                FailureAction::Panic => {
                    panic!("simulated sink panic for testing");
                }
            }
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn name(&self) -> &str {
        "instrumented"
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

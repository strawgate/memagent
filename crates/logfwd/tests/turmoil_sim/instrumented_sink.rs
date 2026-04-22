//! Instrumented sink with programmable failure scripts for simulation testing.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use logfwd_output::BatchMetadata;
use logfwd_output::sink::{SendResult, Sink};

use super::trace_bridge::{SinkOutcome, TraceEvent, TraceRecorder};

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
    /// Return an IO error forever (does not consume script position).
    RepeatIoError(io::ErrorKind),
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
    outcomes: Arc<Mutex<Vec<SinkOutcome>>>,
    trace: Option<TraceRecorder>,
}

#[allow(dead_code)]
impl InstrumentedSink {
    /// Create a sink with the supplied failure script.
    pub fn new(script: Vec<FailureAction>) -> Self {
        Self {
            script,
            call_index: 0,
            delivered_rows: Arc::new(AtomicU64::new(0)),
            call_count: Arc::new(AtomicU64::new(0)),
            outcomes: Arc::new(Mutex::new(Vec::new())),
            trace: None,
        }
    }

    /// Create a sink that always succeeds.
    pub fn always_succeed() -> Self {
        Self::new(vec![])
    }

    /// Get the shared counter for delivered rows.
    pub fn delivered_counter(&self) -> Arc<AtomicU64> {
        self.delivered_rows.clone()
    }

    /// Get the shared counter for total calls.
    pub fn call_counter(&self) -> Arc<AtomicU64> {
        self.call_count.clone()
    }

    /// Get the shared sink outcome log in observed call order.
    pub fn outcome_log(&self) -> Arc<Mutex<Vec<SinkOutcome>>> {
        self.outcomes.clone()
    }

    /// Attach a trace recorder that receives sink result events.
    ///
    /// The recorder is cloned into the sink and records `SinkResult` events
    /// in the same order `send_batch` processes them.
    pub fn with_trace_recorder(mut self, trace: TraceRecorder) -> Self {
        self.trace = Some(trace);
        self
    }

    fn next_action(&mut self) -> FailureAction {
        let idx = self.call_index;
        self.call_index += 1;
        if idx < self.script.len() {
            let action = self.script[idx].clone();
            if matches!(action, FailureAction::RepeatIoError(_)) {
                // Keep repeating this script slot forever.
                self.call_index = idx;
            }
            action
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
    scripts: Mutex<Vec<Vec<FailureAction>>>,
    delivered_rows: Arc<AtomicU64>,
    call_count: Arc<AtomicU64>,
    outcomes: Arc<Mutex<Vec<SinkOutcome>>>,
    trace: Option<TraceRecorder>,
}

#[allow(dead_code)]
impl InstrumentedSinkFactory {
    /// Create a sink factory with one script queue per worker.
    pub fn new(per_worker_scripts: Vec<Vec<FailureAction>>) -> Self {
        let delivered_rows = Arc::new(AtomicU64::new(0));
        let call_count = Arc::new(AtomicU64::new(0));
        Self {
            scripts: Mutex::new(per_worker_scripts),
            delivered_rows,
            call_count,
            outcomes: Arc::new(Mutex::new(Vec::new())),
            trace: None,
        }
    }

    /// Get the shared counter for delivered rows.
    pub fn delivered_counter(&self) -> Arc<AtomicU64> {
        self.delivered_rows.clone()
    }

    /// Get the shared counter for total calls.
    pub fn call_counter(&self) -> Arc<AtomicU64> {
        self.call_count.clone()
    }

    /// Get the shared outcome log across all sinks created by this factory.
    pub fn outcome_log(&self) -> Arc<Mutex<Vec<SinkOutcome>>> {
        self.outcomes.clone()
    }

    /// Attach a trace recorder that receives sink result events.
    ///
    /// Each created sink clones the recorder and emits `SinkResult` events in
    /// call order, so the factory can share one trace stream across workers.
    pub fn with_trace_recorder(mut self, trace: TraceRecorder) -> Self {
        self.trace = Some(trace);
        self
    }
}

impl logfwd_output::SinkFactory for InstrumentedSinkFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
        let script = self.scripts.lock().unwrap().pop().unwrap_or_default();
        let mut sink = InstrumentedSink::new(script);
        sink.delivered_rows = self.delivered_rows.clone();
        sink.call_count = self.call_count.clone();
        sink.outcomes = self.outcomes.clone();
        sink.trace = self.trace.clone();
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
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        let action = self.next_action();
        let rows = batch.num_rows() as u64;
        let delivered = self.delivered_rows.clone();
        let outcomes = self.outcomes.clone();
        let trace = self.trace.clone();

        Box::pin(async move {
            match action {
                FailureAction::Succeed => {
                    outcomes
                        .lock()
                        .expect("outcomes mutex poisoned")
                        .push(SinkOutcome::Ok);
                    delivered.fetch_add(rows, Ordering::Relaxed);
                    if let Some(trace) = &trace {
                        trace.record(TraceEvent::SinkResult { worker_id: 0,
                            outcome: SinkOutcome::Ok,
                            rows,
                        });
                    }
                    SendResult::Ok
                }
                FailureAction::RetryAfter(dur) => {
                    outcomes
                        .lock()
                        .expect("outcomes mutex poisoned")
                        .push(SinkOutcome::RetryAfter);
                    if let Some(trace) = &trace {
                        trace.record(TraceEvent::SinkResult { worker_id: 0,
                            outcome: SinkOutcome::RetryAfter,
                            rows,
                        });
                    }
                    SendResult::RetryAfter(dur)
                }
                FailureAction::IoError(kind) => {
                    outcomes
                        .lock()
                        .expect("outcomes mutex poisoned")
                        .push(SinkOutcome::IoError);
                    if let Some(trace) = &trace {
                        trace.record(TraceEvent::SinkResult { worker_id: 0,
                            outcome: SinkOutcome::IoError,
                            rows,
                        });
                    }
                    SendResult::IoError(io::Error::new(kind, "simulated failure"))
                }
                FailureAction::RepeatIoError(kind) => {
                    outcomes
                        .lock()
                        .expect("outcomes mutex poisoned")
                        .push(SinkOutcome::IoError);
                    if let Some(trace) = &trace {
                        trace.record(TraceEvent::SinkResult { worker_id: 0,
                            outcome: SinkOutcome::IoError,
                            rows,
                        });
                    }
                    SendResult::IoError(io::Error::new(kind, "simulated repeated failure"))
                }
                FailureAction::Reject(reason) => {
                    outcomes
                        .lock()
                        .expect("outcomes mutex poisoned")
                        .push(SinkOutcome::Rejected);
                    if let Some(trace) = &trace {
                        trace.record(TraceEvent::SinkResult { worker_id: 0,
                            outcome: SinkOutcome::Rejected,
                            rows,
                        });
                    }
                    SendResult::Rejected(reason)
                }
                FailureAction::Delay(dur) => {
                    tokio::time::sleep(dur).await;
                    outcomes
                        .lock()
                        .expect("outcomes mutex poisoned")
                        .push(SinkOutcome::Ok);
                    delivered.fetch_add(rows, Ordering::Relaxed);
                    if let Some(trace) = &trace {
                        trace.record(TraceEvent::SinkResult { worker_id: 0,
                            outcome: SinkOutcome::Ok,
                            rows,
                        });
                    }
                    SendResult::Ok
                }
                FailureAction::Panic => {
                    outcomes
                        .lock()
                        .expect("outcomes mutex poisoned")
                        .push(SinkOutcome::Panic);
                    if let Some(trace) = &trace {
                        trace.record(TraceEvent::SinkResult { worker_id: 0,
                            outcome: SinkOutcome::Panic,
                            rows,
                        });
                    }
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

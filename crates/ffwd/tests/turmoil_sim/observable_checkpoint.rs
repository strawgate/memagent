//! Checkpoint store with Arc-shared state for post-test invariant verification.
//!
//! The store is moved into the Pipeline, but its inner state is Arc-shared,
//! so tests can inspect checkpoint history and durable offsets AFTER the
//! simulation completes.

use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use ffwd_io::checkpoint::{CheckpointStore, SourceCheckpoint};

use super::trace_bridge::{TraceEvent, TraceRecorder};

/// Entry in the checkpoint update history.
#[derive(Debug, Clone)]
pub struct CheckpointEvent {
    pub source_id: u64,
    pub offset: u64,
}

/// Shared inner state — survives the store being moved into the Pipeline.
#[derive(Default)]
pub struct CheckpointState {
    pub durable: BTreeMap<u64, SourceCheckpoint>,
    pub update_history: Vec<CheckpointEvent>,
    pub flush_count: u64,
    pub crash_count: u64,
}

/// Handle for inspecting and asserting on checkpoint state from outside
/// the Pipeline (after the ObservableCheckpointStore has been moved in).
#[derive(Clone)]
pub struct CheckpointHandle {
    state: Arc<Mutex<CheckpointState>>,
    crash_armed: Arc<AtomicBool>,
}

#[allow(dead_code)]
impl CheckpointHandle {
    /// Get the durable checkpoint offset for a source.
    pub fn durable_offset(&self, source_id: u64) -> Option<u64> {
        let s = self.state.lock().unwrap();
        s.durable.get(&source_id).map(|c| c.offset)
    }

    /// Number of successful flushes.
    pub fn flush_count(&self) -> u64 {
        self.state.lock().unwrap().flush_count
    }

    /// Number of crash-simulated flushes.
    pub fn crash_count(&self) -> u64 {
        self.state.lock().unwrap().crash_count
    }

    /// Arm a crash — next flush() will fail and lose pending data.
    pub fn arm_crash(&self) {
        self.crash_armed.store(true, Ordering::SeqCst);
    }

    /// Verify that offsets for a given source never decrease in update history.
    /// Panics with descriptive message if regression found.
    pub fn assert_monotonic(&self, source_id: u64) {
        let s = self.state.lock().unwrap();
        let mut last_offset: Option<u64> = None;
        for (i, event) in s.update_history.iter().enumerate() {
            if event.source_id != source_id {
                continue;
            }
            if let Some(prev) = last_offset {
                assert!(
                    event.offset >= prev,
                    "checkpoint regression for source {source_id} at history index {i}: \
                     offset went from {prev} to {}",
                    event.offset
                );
            }
            last_offset = Some(event.offset);
        }
    }

    /// Verify durable checkpoint for source does not exceed max_offset.
    pub fn assert_checkpoint_le(&self, source_id: u64, max_offset: u64) {
        if let Some(offset) = self.durable_offset(source_id) {
            assert!(
                offset <= max_offset,
                "durable checkpoint for source {source_id} is {offset}, \
                 expected <= {max_offset}"
            );
        }
    }

    /// Number of update events for a source.
    pub fn update_count(&self, source_id: u64) -> usize {
        let s = self.state.lock().unwrap();
        s.update_history
            .iter()
            .filter(|e| e.source_id == source_id)
            .count()
    }

    /// Highest checkpoint update observed for a source.
    pub fn max_update_offset(&self, source_id: u64) -> Option<u64> {
        let s = self.state.lock().unwrap();
        s.update_history
            .iter()
            .filter(|e| e.source_id == source_id)
            .map(|e| e.offset)
            .max()
    }

    /// Assert durable offset, if present, does not exceed update history.
    pub fn assert_durable_not_ahead_of_updates(&self, source_id: u64) {
        if let Some(durable) = self.durable_offset(source_id) {
            let Some(max_update) = self.max_update_offset(source_id) else {
                panic!(
                    "durable checkpoint exists for source {source_id} ({durable}) \
                     but no checkpoint updates were recorded"
                );
            };
            assert!(
                durable <= max_update,
                "durable checkpoint for source {source_id} is {durable}, \
                 ahead of max update {max_update}"
            );
        }
    }
}

/// A `CheckpointStore` with Arc-shared inner state for post-test inspection.
pub struct ObservableCheckpointStore {
    state: Arc<Mutex<CheckpointState>>,
    pending: BTreeMap<u64, SourceCheckpoint>,
    crash_armed: Arc<AtomicBool>,
    crash_on_flush_number: Arc<AtomicU64>,
    trace: Option<TraceRecorder>,
}

impl ObservableCheckpointStore {
    /// Create a checkpoint store and its shared inspection handle.
    pub fn new() -> (Self, CheckpointHandle) {
        let state = Arc::new(Mutex::new(CheckpointState::default()));
        let crash_armed = Arc::new(AtomicBool::new(false));
        let crash_on_flush_number = Arc::new(AtomicU64::new(0));
        let handle = CheckpointHandle {
            state: state.clone(),
            crash_armed: crash_armed.clone(),
        };
        let store = Self {
            state,
            pending: BTreeMap::new(),
            crash_armed,
            crash_on_flush_number,
            trace: None,
        };
        (store, handle)
    }

    /// Attach a trace recorder that receives checkpoint events.
    ///
    /// The recorder is moved into the store and emits `CheckpointUpdate` and
    /// `CheckpointFlush` events as the pipeline updates and flushes state.
    pub fn with_trace_recorder(mut self, trace: TraceRecorder) -> Self {
        self.trace = Some(trace);
        self
    }

    /// Configure the store to fail exactly on the Nth `flush` call (1-indexed).
    pub fn with_crash_on_nth_flush(self, n: u64) -> Self {
        assert!(n > 0, "crash trigger is 1-indexed; 0 disables crash path");
        self.crash_on_flush_number.store(n, Ordering::SeqCst);
        self
    }
}

impl CheckpointStore for ObservableCheckpointStore {
    fn update(&mut self, checkpoint: SourceCheckpoint) {
        let mut s = self.state.lock().unwrap();
        s.update_history.push(CheckpointEvent {
            source_id: checkpoint.source_id,
            offset: checkpoint.offset,
        });
        drop(s);
        if let Some(trace) = &self.trace {
            trace.record(TraceEvent::CheckpointUpdate {
                source_id: checkpoint.source_id,
                offset: checkpoint.offset,
            });
        }
        self.pending.insert(checkpoint.source_id, checkpoint);
    }

    fn flush(&mut self) -> io::Result<()> {
        let next_flush_count = {
            let s = self.state.lock().unwrap();
            s.flush_count + s.crash_count + 1
        };
        let crash_on_n = self.crash_on_flush_number.load(Ordering::SeqCst);
        let crash_on_count = crash_on_n > 0 && next_flush_count == crash_on_n;
        if self.crash_armed.swap(false, Ordering::SeqCst) || crash_on_count {
            if crash_on_count {
                self.crash_on_flush_number.store(0, Ordering::SeqCst);
            }
            self.pending.clear();
            let mut s = self.state.lock().unwrap();
            s.crash_count += 1;
            if let Some(trace) = &self.trace {
                trace.record(TraceEvent::CheckpointFlush { success: false });
            }
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "simulated crash during checkpoint flush",
            ));
        }
        let mut s = self.state.lock().unwrap();
        let pending = std::mem::take(&mut self.pending);
        for (k, v) in pending {
            s.durable.insert(k, v);
        }
        s.flush_count += 1;
        if let Some(trace) = &self.trace {
            trace.record(TraceEvent::CheckpointFlush { success: true });
        }
        Ok(())
    }

    fn load(&self, source_id: u64) -> Option<SourceCheckpoint> {
        let s = self.state.lock().unwrap();
        s.durable.get(&source_id).cloned()
    }

    fn load_all(&self) -> Vec<SourceCheckpoint> {
        let s = self.state.lock().unwrap();
        s.durable.values().cloned().collect()
    }
}

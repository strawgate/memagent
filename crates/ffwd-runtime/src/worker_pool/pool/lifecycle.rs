use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use ffwd_diagnostics::diagnostics::{ComponentHealth, ComponentStats, PipelineMetrics};
use ffwd_output::sink::{OutputHealthEvent, SinkFactory};

use super::health::{
    aggregate_output_health, idle_health_after_worker_insert, reduce_worker_slot_health,
};
use super::types::{AckItem, DeliveryOutcome, WorkItem, WorkerMsg};
use super::worker::worker_task;

#[cfg(test)]
use super::dispatch::{ChannelState, DispatchOutcome, dispatch_step};
#[cfg(test)]
use super::types::{MAX_REJECTION_REASON_BYTES, bound_rejection_reason};

#[cfg(not(test))]
const DRAIN_CANCEL_GRACE: Duration = Duration::from_secs(5);
#[cfg(test)]
const DRAIN_CANCEL_GRACE: Duration = Duration::from_millis(50);

// Worker config (shared across all workers in a pool)
// ---------------------------------------------------------------------------

pub(super) struct WorkerConfig {
    pub(super) idle_timeout: Duration,
    pub(super) cancel: CancellationToken,
    pub(super) max_retry_delay: Duration,
    pub(super) metrics: Arc<PipelineMetrics>,
    pub(super) output_health: Arc<OutputHealthTracker>,
}

pub(super) struct WorkerSlotCleanup {
    pub(super) output_health: Arc<OutputHealthTracker>,
    pub(super) worker_id: usize,
}

impl Drop for WorkerSlotCleanup {
    fn drop(&mut self) {
        self.output_health.remove_worker(self.worker_id);
    }
}

// Worker handle (held by pool)
// ---------------------------------------------------------------------------

struct WorkerHandle {
    /// Channel to send work items to this worker.
    tx: mpsc::Sender<WorkerMsg>,
}

pub(super) struct OutputHealthTracker {
    outputs: Vec<Arc<ComponentStats>>,
    state: std::sync::Mutex<OutputHealthState>,
}

struct OutputHealthState {
    worker_slots: HashMap<usize, ComponentHealth>,
    idle_health: ComponentHealth,
}

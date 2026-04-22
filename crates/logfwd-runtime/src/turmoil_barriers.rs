//! Turmoil barrier trigger types for deterministic seam interleaving tests.
//!
//! These events are only compiled when `logfwd-runtime` is built with
//! `feature = "turmoil"`. Production builds are unaffected.
// xtask-verify: allow(pub_module_needs_tests) reason: turmoil-only seam event enum; behavior verified by turmoil integration suites

use crate::worker_pool::DeliveryOutcome;

/// Pipeline lifecycle phases emitted by runtime seam hooks.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PipelinePhase {
    Running,
    Draining,
    Stopped,
}

/// Terminal state a batch can reach.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BatchTerminalState {
    /// Batch was successfully delivered and acknowledged.
    Acked,
    /// Batch was permanently rejected by the sink.
    Rejected,
    /// Batch was abandoned (force-stop or unrecoverable failure).
    Abandoned,
}

/// Barrier events emitted by runtime seam hooks.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RuntimeBarrierEvent {
    /// Emitted when the pipeline transitions between lifecycle phases.
    PipelinePhase { phase: PipelinePhase },
    /// Emitted when a batch is submitted to the output worker pool.
    BatchSubmitted {
        batch_id: u64,
        checkpoints: Vec<(u64, u64)>,
    },
    /// Emitted by worker tasks immediately before sending an ack item.
    BeforeWorkerAckSend {
        worker_id: usize,
        batch_id: u64,
        outcome: DeliveryOutcome,
        retries: usize,
        num_rows: u64,
    },
    /// Emitted after pipeline ticket/application logic applies an ack.
    AckApplied {
        batch_id: u64,
        outcome: DeliveryOutcome,
        checkpoint_advances: Vec<(u64, u64)>,
    },
    /// Emitted when a batch reaches a terminal disposition.
    /// All tickets in the batch receive the same disposition.
    BatchTerminalized {
        batch_id: u64,
        terminal_state: BatchTerminalState,
    },
    /// Emitted when a batch is held (non-terminal failure).
    BatchHeld { batch_id: u64 },
    /// Emitted when the worker pool begins its drain sequence.
    PoolDrainBegin,
    /// Emitted when the worker pool drain completes.
    PoolDrainComplete { forced_abort: bool },
    /// Emitted by checkpoint I/O immediately before each flush attempt.
    BeforeCheckpointFlushAttempt { attempt: u32 },
    /// Emitted by checkpoint I/O after a flush attempt resolves.
    CheckpointFlush { success: bool },
}

/// Trigger a Turmoil barrier event.
pub async fn trigger(event: RuntimeBarrierEvent) {
    turmoil::barriers::trigger(event).await;
}

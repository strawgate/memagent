//! Prototype bridge: runtime trace events -> transition-contract validator.
//!
//! This stays test-only (`turmoil_sim`) and emits JSONL for easy human inspection.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use logfwd_runtime::turmoil_barriers::{
    BatchTerminalState, PipelinePhase, RetryReason, RuntimeBarrierEvent,
};
use logfwd_runtime::worker_pool::DeliveryOutcome;
use serde_json::{Value, json};

/// Runtime trace event schema (JSONL row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceEvent {
    Phase {
        phase: TracePhase,
    },
    BatchBegin {
        batch_id: u64,
        source_id: u64,
        checkpoint: u64,
    },
    SinkResult {
        worker_id: usize,
        outcome: SinkOutcome,
        rows: u64,
    },
    CheckpointUpdate {
        source_id: u64,
        offset: u64,
    },
    CheckpointFlush {
        success: bool,
    },
    /// Per-batch terminal state (acked, rejected, or abandoned).
    BatchTerminal {
        batch_id: u64,
        source_id: u64,
        terminal: BatchTerminal,
    },
    /// Batch was held (non-terminal failure, will retry or abandon).
    BatchHold {
        batch_id: u64,
        source_id: u64,
    },
    /// Retry attempt inside the worker delivery loop.
    RetryAttempt {
        worker_id: usize,
        batch_id: u64,
        attempt: usize,
        backoff_ms: u64,
        reason: String,
    },
    /// Worker pool drain sequence started.
    PoolDrainBegin,
    /// Worker pool drain sequence completed.
    PoolDrainComplete {
        forced_abort: bool,
    },
}

/// Terminal disposition of a batch as observed in the trace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchTerminal {
    Acked,
    Rejected,
    Abandoned,
}

impl BatchTerminal {
    fn as_str(self) -> &'static str {
        match self {
            Self::Acked => "acked",
            Self::Rejected => "rejected",
            Self::Abandoned => "abandoned",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "acked" => Some(Self::Acked),
            "rejected" => Some(Self::Rejected),
            "abandoned" => Some(Self::Abandoned),
            _ => None,
        }
    }
}

impl TraceEvent {
    fn kind(&self) -> &'static str {
        match self {
            Self::Phase { .. } => "phase",
            Self::BatchBegin { .. } => "batch_begin",
            Self::SinkResult { .. } => "sink_result",
            Self::CheckpointUpdate { .. } => "checkpoint_update",
            Self::CheckpointFlush { .. } => "checkpoint_flush",
            Self::BatchTerminal { .. } => "batch_terminal",
            Self::BatchHold { .. } => "batch_hold",
            Self::RetryAttempt { .. } => "retry_attempt",
            Self::PoolDrainBegin => "pool_drain_begin",
            Self::PoolDrainComplete { .. } => "pool_drain_complete",
        }
    }

    fn summary(&self) -> String {
        match self {
            Self::Phase { phase } => format!("phase={}", phase.as_str()),
            Self::BatchBegin {
                batch_id,
                source_id,
                checkpoint,
            } => {
                format!("batch_begin batch={batch_id} source={source_id} checkpoint={checkpoint}")
            }
            Self::SinkResult {
                worker_id,
                outcome,
                rows,
            } => {
                format!(
                    "sink_result worker={worker_id} outcome={} rows={rows}",
                    outcome.as_str()
                )
            }
            Self::CheckpointUpdate { source_id, offset } => {
                format!("checkpoint_update source_id={source_id} offset={offset}")
            }
            Self::CheckpointFlush { success } => format!("checkpoint_flush success={success}"),
            Self::BatchTerminal {
                batch_id,
                source_id,
                terminal,
            } => {
                format!(
                    "batch_terminal batch={batch_id} source={source_id} terminal={}",
                    terminal.as_str()
                )
            }
            Self::BatchHold {
                batch_id,
                source_id,
            } => format!("batch_hold batch={batch_id} source={source_id}"),
            Self::RetryAttempt {
                worker_id,
                batch_id,
                attempt,
                backoff_ms,
                reason,
            } => {
                format!(
                    "retry_attempt worker={worker_id} batch={batch_id} attempt={attempt} \
                     backoff_ms={backoff_ms} reason={reason}"
                )
            }
            Self::PoolDrainBegin => "pool_drain_begin".to_string(),
            Self::PoolDrainComplete { forced_abort } => {
                format!("pool_drain_complete forced_abort={forced_abort}")
            }
        }
    }
}

/// Convert runtime-emitted turmoil barrier events into contract trace events.
#[must_use]
pub fn trace_event_from_runtime_barrier(event: &RuntimeBarrierEvent) -> Vec<TraceEvent> {
    match event {
        RuntimeBarrierEvent::PipelinePhase { phase } => {
            let phase = match phase {
                PipelinePhase::Running => TracePhase::Running,
                PipelinePhase::Draining => TracePhase::Draining,
                PipelinePhase::Stopped => TracePhase::Stopped,
            };
            vec![TraceEvent::Phase { phase }]
        }
        RuntimeBarrierEvent::BeforeWorkerAckSend {
            worker_id,
            outcome,
            num_rows,
            ..
        } => {
            let outcome = match outcome {
                DeliveryOutcome::Delivered => SinkOutcome::Ok,
                DeliveryOutcome::Rejected { .. } => SinkOutcome::Rejected,
                DeliveryOutcome::InternalFailure => SinkOutcome::Panic,
                DeliveryOutcome::RetryExhausted
                | DeliveryOutcome::TimedOut
                | DeliveryOutcome::PoolClosed
                | DeliveryOutcome::WorkerChannelClosed
                | DeliveryOutcome::NoWorkersAvailable => SinkOutcome::IoError,
            };
            vec![TraceEvent::SinkResult {
                worker_id: *worker_id,
                outcome,
                rows: *num_rows,
            }]
        }
        RuntimeBarrierEvent::CheckpointFlush { success } => {
            vec![TraceEvent::CheckpointFlush { success: *success }]
        }
        RuntimeBarrierEvent::AckApplied {
            checkpoint_advances,
            ..
        } => checkpoint_advances
            .iter()
            .map(|(source_id, offset)| TraceEvent::CheckpointUpdate {
                source_id: *source_id,
                offset: *offset,
            })
            .collect(),
        RuntimeBarrierEvent::BatchSubmitted {
            batch_id,
            checkpoints,
        } => checkpoints
            .iter()
            .map(|(source_id, offset)| TraceEvent::BatchBegin {
                batch_id: *batch_id,
                source_id: *source_id,
                checkpoint: *offset,
            })
            .collect(),
        RuntimeBarrierEvent::BatchTerminalized {
            batch_id,
            terminal_state,
        } => {
            let terminal = match terminal_state {
                BatchTerminalState::Acked => BatchTerminal::Acked,
                BatchTerminalState::Rejected => BatchTerminal::Rejected,
                BatchTerminalState::Abandoned => BatchTerminal::Abandoned,
            };
            vec![TraceEvent::BatchTerminal {
                batch_id: *batch_id,
                source_id: 0, // source correlation via batch_begin events
                terminal,
            }]
        }
        RuntimeBarrierEvent::BatchHeld { batch_id } => vec![TraceEvent::BatchHold {
            batch_id: *batch_id,
            source_id: 0, // source correlation via batch_begin events
        }],
        RuntimeBarrierEvent::RetryAttempt {
            worker_id,
            batch_id,
            attempt,
            backoff_ms,
            reason,
        } => {
            let reason_str = match reason {
                RetryReason::Timeout => "timeout",
                RetryReason::RetryAfter => "retry_after",
                RetryReason::IoError => "io_error",
            };
            vec![TraceEvent::RetryAttempt {
                worker_id: *worker_id,
                batch_id: *batch_id,
                attempt: *attempt,
                backoff_ms: *backoff_ms,
                reason: reason_str.to_string(),
            }]
        }
        RuntimeBarrierEvent::PoolDrainBegin => vec![TraceEvent::PoolDrainBegin],
        RuntimeBarrierEvent::PoolDrainComplete { forced_abort } => {
            vec![TraceEvent::PoolDrainComplete {
                forced_abort: *forced_abort,
            }]
        }
        RuntimeBarrierEvent::BeforeCheckpointFlushAttempt { .. } => Vec::new(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TracePhase {
    Running,
    Draining,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkOutcome {
    Ok,
    RetryAfter,
    IoError,
    Rejected,
    Panic,
}

impl TracePhase {
    fn as_str(self) -> &'static str {
        match self {
            TracePhase::Running => "running",
            TracePhase::Draining => "draining",
            TracePhase::Stopped => "stopped",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "running" => Some(Self::Running),
            "draining" => Some(Self::Draining),
            "stopped" => Some(Self::Stopped),
            _ => None,
        }
    }
}

impl SinkOutcome {
    fn as_str(self) -> &'static str {
        match self {
            SinkOutcome::Ok => "ok",
            SinkOutcome::RetryAfter => "retry_after",
            SinkOutcome::IoError => "io_error",
            SinkOutcome::Rejected => "rejected",
            SinkOutcome::Panic => "panic",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "ok" => Some(Self::Ok),
            "retry_after" => Some(Self::RetryAfter),
            "io_error" => Some(Self::IoError),
            "rejected" => Some(Self::Rejected),
            "panic" => Some(Self::Panic),
            _ => None,
        }
    }
}

impl TraceEvent {
    pub fn to_json(&self) -> Value {
        match self {
            TraceEvent::Phase { phase } => {
                json!({"event": "phase", "phase": phase.as_str()})
            }
            TraceEvent::BatchBegin {
                batch_id,
                source_id,
                checkpoint,
            } => {
                json!({"event": "batch_begin", "batch_id": batch_id, "source_id": source_id, "checkpoint": checkpoint})
            }
            TraceEvent::SinkResult {
                worker_id,
                outcome,
                rows,
            } => {
                json!({"event": "sink_result", "worker_id": worker_id, "outcome": outcome.as_str(), "rows": rows})
            }
            TraceEvent::CheckpointUpdate { source_id, offset } => {
                json!({"event": "checkpoint_update", "source_id": source_id, "offset": offset})
            }
            TraceEvent::CheckpointFlush { success } => {
                json!({"event": "checkpoint_flush", "success": success})
            }
            TraceEvent::BatchTerminal {
                batch_id,
                source_id,
                terminal,
            } => {
                json!({"event": "batch_terminal", "batch_id": batch_id, "source_id": source_id, "terminal": terminal.as_str()})
            }
            TraceEvent::BatchHold {
                batch_id,
                source_id,
            } => {
                json!({"event": "batch_hold", "batch_id": batch_id, "source_id": source_id})
            }
            TraceEvent::RetryAttempt {
                worker_id,
                batch_id,
                attempt,
                backoff_ms,
                reason,
            } => {
                json!({"event": "retry_attempt", "worker_id": worker_id, "batch_id": batch_id, "attempt": attempt, "backoff_ms": backoff_ms, "reason": reason})
            }
            TraceEvent::PoolDrainBegin => {
                json!({"event": "pool_drain_begin"})
            }
            TraceEvent::PoolDrainComplete { forced_abort } => {
                json!({"event": "pool_drain_complete", "forced_abort": forced_abort})
            }
        }
    }

    pub fn from_json(v: &Value) -> Result<Self, String> {
        let Some(event) = v.get("event").and_then(Value::as_str) else {
            return Err("missing string field: event".to_string());
        };

        match event {
            "phase" => {
                let Some(raw_phase) = v.get("phase").and_then(Value::as_str) else {
                    return Err("phase event missing string field: phase".to_string());
                };
                let Some(phase) = TracePhase::from_str(raw_phase) else {
                    return Err(format!("unknown phase '{raw_phase}'"));
                };
                Ok(Self::Phase { phase })
            }
            "sink_result" => {
                let Some(raw_outcome) = v.get("outcome").and_then(Value::as_str) else {
                    return Err("sink_result missing string field: outcome".to_string());
                };
                let Some(outcome) = SinkOutcome::from_str(raw_outcome) else {
                    return Err(format!("unknown sink outcome '{raw_outcome}'"));
                };
                let Some(rows) = v.get("rows").and_then(Value::as_u64) else {
                    return Err("sink_result missing u64 field: rows".to_string());
                };
                let worker_id = v.get("worker_id").and_then(Value::as_u64).unwrap_or(0) as usize;
                Ok(Self::SinkResult {
                    worker_id,
                    outcome,
                    rows,
                })
            }
            "batch_begin" => {
                let Some(batch_id) = v.get("batch_id").and_then(Value::as_u64) else {
                    return Err("batch_begin missing u64 field: batch_id".to_string());
                };
                let Some(source_id) = v.get("source_id").and_then(Value::as_u64) else {
                    return Err("batch_begin missing u64 field: source_id".to_string());
                };
                let Some(checkpoint) = v.get("checkpoint").and_then(Value::as_u64) else {
                    return Err("batch_begin missing u64 field: checkpoint".to_string());
                };
                Ok(Self::BatchBegin {
                    batch_id,
                    source_id,
                    checkpoint,
                })
            }
            "checkpoint_update" => {
                let Some(source_id) = v.get("source_id").and_then(Value::as_u64) else {
                    return Err("checkpoint_update missing u64 field: source_id".to_string());
                };
                let Some(offset) = v.get("offset").and_then(Value::as_u64) else {
                    return Err("checkpoint_update missing u64 field: offset".to_string());
                };
                Ok(Self::CheckpointUpdate { source_id, offset })
            }
            "checkpoint_flush" => {
                let Some(success) = v.get("success").and_then(Value::as_bool) else {
                    return Err("checkpoint_flush missing bool field: success".to_string());
                };
                Ok(Self::CheckpointFlush { success })
            }
            "batch_terminal" => {
                let Some(batch_id) = v.get("batch_id").and_then(Value::as_u64) else {
                    return Err("batch_terminal missing u64 field: batch_id".to_string());
                };
                let Some(source_id) = v.get("source_id").and_then(Value::as_u64) else {
                    return Err("batch_terminal missing u64 field: source_id".to_string());
                };
                let Some(raw_terminal) = v.get("terminal").and_then(Value::as_str) else {
                    return Err("batch_terminal missing string field: terminal".to_string());
                };
                let Some(terminal) = BatchTerminal::from_str(raw_terminal) else {
                    return Err(format!("unknown batch terminal '{raw_terminal}'"));
                };
                Ok(Self::BatchTerminal {
                    batch_id,
                    source_id,
                    terminal,
                })
            }
            "batch_hold" => {
                let Some(batch_id) = v.get("batch_id").and_then(Value::as_u64) else {
                    return Err("batch_hold missing u64 field: batch_id".to_string());
                };
                let Some(source_id) = v.get("source_id").and_then(Value::as_u64) else {
                    return Err("batch_hold missing u64 field: source_id".to_string());
                };
                Ok(Self::BatchHold {
                    batch_id,
                    source_id,
                })
            }
            "retry_attempt" => {
                let worker_id = v.get("worker_id").and_then(Value::as_u64).unwrap_or(0) as usize;
                let Some(batch_id) = v.get("batch_id").and_then(Value::as_u64) else {
                    return Err("retry_attempt missing u64 field: batch_id".to_string());
                };
                let Some(attempt) = v.get("attempt").and_then(Value::as_u64) else {
                    return Err("retry_attempt missing u64 field: attempt".to_string());
                };
                let Some(backoff_ms) = v.get("backoff_ms").and_then(Value::as_u64) else {
                    return Err("retry_attempt missing u64 field: backoff_ms".to_string());
                };
                let Some(reason) = v.get("reason").and_then(Value::as_str) else {
                    return Err("retry_attempt missing string field: reason".to_string());
                };
                Ok(Self::RetryAttempt {
                    worker_id,
                    batch_id,
                    attempt: attempt as usize,
                    backoff_ms,
                    reason: reason.to_string(),
                })
            }
            "pool_drain_begin" => Ok(Self::PoolDrainBegin),
            "pool_drain_complete" => {
                let Some(forced_abort) = v.get("forced_abort").and_then(Value::as_bool) else {
                    return Err("pool_drain_complete missing bool field: forced_abort".to_string());
                };
                Ok(Self::PoolDrainComplete { forced_abort })
            }
            other => Err(format!("unknown event kind '{other}'")),
        }
    }
}

/// Simple JSONL recorder used by simulation-only codepaths.
#[derive(Clone)]
pub struct TraceRecorder {
    file: Arc<Mutex<File>>,
}

impl TraceRecorder {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn record(&self, event: TraceEvent) {
        let line = event.to_json().to_string();
        let mut file = self
            .file
            .lock()
            .expect("trace recorder mutex poisoned while writing event");
        writeln!(file, "{line}").expect("failed to write trace event");
        file.flush().expect("failed to flush trace event");
    }
}

pub fn load_trace(path: impl AsRef<Path>) -> io::Result<Vec<TraceEvent>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut events = Vec::new();
    for (line_no, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("trace parse error at line {}: {e}", line_no + 1),
            )
        })?;
        let event = TraceEvent::from_json(&value).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid trace event at line {}: {e}", line_no + 1),
            )
        })?;
        events.push(event);
    }
    Ok(events)
}

/// Build a deterministic, human-readable contract trace.
///
/// This normalizes events into stable strings so replay tests can compare
/// outcomes across runs with the same seed.
pub fn normalized_contract_trace(events: &[TraceEvent]) -> Vec<String> {
    events
        .iter()
        .map(|event| match event {
            TraceEvent::Phase { phase } => format!("phase:{}", phase.as_str()),
            TraceEvent::BatchBegin {
                batch_id,
                source_id,
                checkpoint,
            } => format!("batch_begin:{batch_id}:{source_id}:{checkpoint}"),
            TraceEvent::SinkResult {
                worker_id,
                outcome,
                rows,
            } => {
                format!("sink:{worker_id}:{}:{rows}", outcome.as_str())
            }
            TraceEvent::CheckpointUpdate { source_id, offset } => {
                format!("ckpt_update:{source_id}:{offset}")
            }
            TraceEvent::CheckpointFlush { success } => format!("ckpt_flush:{success}"),
            TraceEvent::BatchTerminal {
                batch_id,
                source_id,
                terminal,
            } => format!(
                "batch_terminal:{batch_id}:{source_id}:{}",
                terminal.as_str()
            ),
            TraceEvent::BatchHold {
                batch_id,
                source_id,
            } => format!("batch_hold:{batch_id}:{source_id}"),
            TraceEvent::RetryAttempt {
                worker_id,
                attempt,
                backoff_ms,
                reason,
                ..
            } => format!("retry:{worker_id}:{attempt}:{backoff_ms}:{reason}"),
            TraceEvent::PoolDrainBegin => "pool_drain_begin".to_string(),
            TraceEvent::PoolDrainComplete { forced_abort } => {
                format!("pool_drain_complete:{forced_abort}")
            }
        })
        .collect()
}

/// Validate transition history across multiple replay runs.
///
/// Ensures every run satisfies `TransitionValidator` and that all runs produce
/// the same normalized contract trace as run 0.
pub fn validate_replay_history_equivalence(
    runs: &[Vec<TraceEvent>],
) -> Result<Vec<String>, String> {
    let Some(first) = runs.first() else {
        return Err("no runs supplied for replay equivalence validation".to_string());
    };

    let validator = TransitionValidator::default();
    validator
        .validate(first)
        .map_err(|err| format!("run 0 transition contract failed: {err}"))?;
    let baseline = normalized_contract_trace(first);

    for (idx, run) in runs.iter().enumerate().skip(1) {
        validator
            .validate(run)
            .map_err(|err| format!("run {idx} transition contract failed: {err}"))?;
        let normalized = normalized_contract_trace(run);
        if normalized != baseline {
            return Err(format!(
                "run {idx} diverged from run 0 normalized contract trace"
            ));
        }
    }

    Ok(baseline)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PhaseState {
    Running,
    Draining,
    Stopped,
}

impl PhaseState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Draining => "draining",
            Self::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Structured validator failure for a single trace event position.
///
/// This is returned by `TransitionValidator::validate_detailed` so callers can
/// inspect stable machine-readable fields without parsing formatted text.
pub struct ValidationError {
    index: usize,
    code: &'static str,
    message: String,
    event_summary: String,
    previous_event_summary: Option<String>,
}

impl ValidationError {
    fn at(
        events: &[TraceEvent],
        index: usize,
        code: &'static str,
        message: impl Into<String>,
    ) -> Self {
        let event_summary = events
            .get(index)
            .map(TraceEvent::summary)
            .unwrap_or_else(|| "<missing>".to_string());
        let previous_event_summary = index
            .checked_sub(1)
            .and_then(|idx| events.get(idx))
            .map(TraceEvent::summary);
        Self {
            index,
            code,
            message: message.into(),
            event_summary,
            previous_event_summary,
        }
    }

    /// Index of the failing event in the trace.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Stable short code for programmatic matching.
    pub fn code(&self) -> &'static str {
        self.code
    }

    /// Human-readable explanation of the violated condition.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Summary of the event at `index`.
    pub fn event_summary(&self) -> &str {
        &self.event_summary
    }

    /// Summary of the event immediately before `index`, when present.
    pub fn previous_event_summary(&self) -> Option<&str> {
        self.previous_event_summary.as_deref()
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.previous_event_summary {
            Some(prev) => write!(
                f,
                "[{}] event #{}: {}; event={}; previous={}",
                self.code, self.index, self.message, self.event_summary, prev
            ),
            None => write!(
                f,
                "[{}] event #{}: {}; event={}",
                self.code, self.index, self.message, self.event_summary
            ),
        }
    }
}

/// Strategy-B prototype: normalized event-log IR with validator plug-ins.
///
/// This is intentionally partial (no source-offset state), and exists as a
/// contrast point for testing extension ergonomics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedTrace {
    pub entries: Vec<NormalizedTraceEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Normalized event record used by pluggable validators.
pub struct NormalizedTraceEvent {
    pub index: usize,
    pub kind: &'static str,
    pub attributes: BTreeMap<&'static str, String>,
}

impl NormalizedTrace {
    pub fn from_events(events: &[TraceEvent]) -> Self {
        let entries = events
            .iter()
            .enumerate()
            .map(|(index, event)| {
                let mut attributes: BTreeMap<&'static str, String> = BTreeMap::new();
                match event {
                    TraceEvent::Phase { phase } => {
                        attributes.insert("phase", phase.as_str().to_string());
                    }
                    TraceEvent::BatchBegin {
                        batch_id,
                        source_id,
                        checkpoint,
                    } => {
                        attributes.insert("batch_id", batch_id.to_string());
                        attributes.insert("source_id", source_id.to_string());
                        attributes.insert("checkpoint", checkpoint.to_string());
                    }
                    TraceEvent::SinkResult {
                        worker_id,
                        outcome,
                        rows,
                    } => {
                        attributes.insert("worker_id", worker_id.to_string());
                        attributes.insert("outcome", outcome.as_str().to_string());
                        attributes.insert("rows", rows.to_string());
                    }
                    TraceEvent::CheckpointUpdate { source_id, offset } => {
                        attributes.insert("source_id", source_id.to_string());
                        attributes.insert("offset", offset.to_string());
                    }
                    TraceEvent::CheckpointFlush { success } => {
                        attributes.insert("success", success.to_string());
                    }
                    TraceEvent::BatchTerminal {
                        batch_id,
                        source_id,
                        terminal,
                    } => {
                        attributes.insert("batch_id", batch_id.to_string());
                        attributes.insert("source_id", source_id.to_string());
                        attributes.insert("terminal", terminal.as_str().to_string());
                    }
                    TraceEvent::BatchHold {
                        batch_id,
                        source_id,
                    } => {
                        attributes.insert("batch_id", batch_id.to_string());
                        attributes.insert("source_id", source_id.to_string());
                    }
                    TraceEvent::RetryAttempt {
                        worker_id,
                        batch_id,
                        attempt,
                        backoff_ms,
                        reason,
                    } => {
                        attributes.insert("worker_id", worker_id.to_string());
                        attributes.insert("batch_id", batch_id.to_string());
                        attributes.insert("attempt", attempt.to_string());
                        attributes.insert("backoff_ms", backoff_ms.to_string());
                        attributes.insert("reason", reason.clone());
                    }
                    TraceEvent::PoolDrainBegin => {}
                    TraceEvent::PoolDrainComplete { forced_abort } => {
                        attributes.insert("forced_abort", forced_abort.to_string());
                    }
                }
                NormalizedTraceEvent {
                    index,
                    kind: event.kind(),
                    attributes,
                }
            })
            .collect();
        Self { entries }
    }
}

/// Validator plug-in interface for normalized trace analysis.
pub trait EventValidator {
    fn validate(&self, trace: &NormalizedTrace) -> Result<(), String>;
}

/// Minimal validator that enforces `running -> draining -> stopped` order.
pub struct PhaseOrderValidator;

impl EventValidator for PhaseOrderValidator {
    fn validate(&self, trace: &NormalizedTrace) -> Result<(), String> {
        let mut expected = "running";
        for entry in &trace.entries {
            if entry.kind != "phase" {
                continue;
            }
            let Some(phase) = entry.attributes.get("phase") else {
                return Err(format!(
                    "phase event missing normalized 'phase' at index {}",
                    entry.index
                ));
            };
            match (expected, phase.as_str()) {
                ("running", "running") => expected = "draining",
                ("draining", "draining") => expected = "stopped",
                ("stopped", "stopped") => expected = "done",
                (want, got) => {
                    return Err(format!(
                        "phase order mismatch at index {}: expected {want}, got {got}",
                        entry.index
                    ));
                }
            }
        }
        if expected != "done" {
            return Err(format!(
                "phase order incomplete in normalized trace: next_expected={expected}"
            ));
        }
        Ok(())
    }
}

/// Declared transition contract for replay validation.
#[derive(Debug, Default)]
pub struct TransitionValidator {
    source_offsets: BTreeMap<u64, u64>,
}

impl TransitionValidator {
    pub fn validate(&self, events: &[TraceEvent]) -> Result<(), String> {
        self.validate_detailed(events)
            .map_err(|err| err.to_string())
    }

    /// Validate transition rules and return structured failure context.
    ///
    /// On success returns `Ok(())`. On failure returns `ValidationError`
    /// with a stable code, failing event index, and neighboring summaries.
    pub fn validate_detailed(&self, events: &[TraceEvent]) -> Result<(), ValidationError> {
        if events.is_empty() {
            return Err(ValidationError::at(
                events,
                0,
                "empty_trace",
                "trace is empty",
            ));
        }
        if !matches!(
            events.first(),
            Some(TraceEvent::Phase {
                phase: TracePhase::Running
            })
        ) {
            return Err(ValidationError::at(
                events,
                0,
                "missing_running_start",
                "trace must start with running phase marker",
            ));
        }

        let mut phase = PhaseState::Running;
        let mut source_offsets = self.source_offsets.clone();

        for (idx, event) in events.iter().enumerate() {
            match event {
                TraceEvent::Phase {
                    phase: TracePhase::Running,
                } => {
                    if idx == 0 {
                        phase = PhaseState::Running;
                    } else {
                        return Err(ValidationError::at(
                            events,
                            idx,
                            "duplicate_running",
                            "running phase marker may only appear first",
                        ));
                    }
                }
                TraceEvent::Phase {
                    phase: TracePhase::Draining,
                } => {
                    if phase != PhaseState::Running {
                        return Err(ValidationError::at(
                            events,
                            idx,
                            "invalid_phase_transition",
                            format!("invalid phase transition {} -> draining", phase.as_str()),
                        ));
                    }
                    phase = PhaseState::Draining;
                }
                TraceEvent::Phase {
                    phase: TracePhase::Stopped,
                } => {
                    if phase != PhaseState::Draining {
                        return Err(ValidationError::at(
                            events,
                            idx,
                            "invalid_phase_transition",
                            format!("invalid phase transition {} -> stopped", phase.as_str()),
                        ));
                    }
                    phase = PhaseState::Stopped;
                }
                TraceEvent::CheckpointUpdate { source_id, offset } => {
                    if phase == PhaseState::Stopped {
                        return Err(ValidationError::at(
                            events,
                            idx,
                            "event_after_stopped",
                            format!(
                                "checkpoint update after stopped (source_id={source_id}, offset={offset})"
                            ),
                        ));
                    }
                    let last = source_offsets.get(source_id).copied().unwrap_or(0);
                    if *offset < last {
                        return Err(ValidationError::at(
                            events,
                            idx,
                            "checkpoint_regression",
                            format!(
                                "checkpoint regression for source {source_id}: {offset} < {last}"
                            ),
                        ));
                    }
                    source_offsets.insert(*source_id, *offset);
                }
                TraceEvent::CheckpointFlush { .. } => {
                    if phase == PhaseState::Stopped {
                        return Err(ValidationError::at(
                            events,
                            idx,
                            "event_after_stopped",
                            "checkpoint flush after stopped",
                        ));
                    }
                }
                TraceEvent::BatchBegin { .. } => {
                    if phase == PhaseState::Stopped {
                        return Err(ValidationError::at(
                            events,
                            idx,
                            "event_after_stopped",
                            "batch begin after stopped",
                        ));
                    }
                }
                TraceEvent::SinkResult { .. } => {
                    if phase == PhaseState::Stopped {
                        return Err(ValidationError::at(
                            events,
                            idx,
                            "event_after_stopped",
                            "sink activity after stopped",
                        ));
                    }
                }
                // Batch terminal/hold and pool drain events are valid in any non-stopped
                // phase. Detailed validation is handled by pluggable EventValidator
                // implementations (NoDoubleComplete, ForceAbortAccountsForAll, etc.).
                TraceEvent::BatchTerminal { .. }
                | TraceEvent::BatchHold { .. }
                | TraceEvent::RetryAttempt { .. }
                | TraceEvent::PoolDrainBegin
                | TraceEvent::PoolDrainComplete { .. } => {}
            }
        }

        if phase != PhaseState::Stopped {
            return Err(ValidationError::at(
                events,
                events.len().saturating_sub(1),
                "terminal_phase_missing",
                format!(
                    "trace ended before stopped transition (last phase: {})",
                    phase.as_str()
                ),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_event_json_roundtrip() {
        let event = TraceEvent::CheckpointUpdate {
            source_id: 7,
            offset: 123,
        };
        let value = event.to_json();
        let decoded = TraceEvent::from_json(&value).expect("decode should work");
        assert_eq!(event, decoded);
    }

    #[test]
    fn strategy_b_normalized_trace_builds_event_entries() {
        let events = vec![
            TraceEvent::Phase {
                phase: TracePhase::Running,
            },
            TraceEvent::SinkResult {
                worker_id: 0,
                outcome: SinkOutcome::Ok,
                rows: 2,
            },
        ];
        let normalized = NormalizedTrace::from_events(&events);
        assert_eq!(normalized.entries.len(), 2);
        assert_eq!(normalized.entries[0].kind, "phase");
        assert_eq!(
            normalized.entries[0]
                .attributes
                .get("phase")
                .expect("phase key present"),
            "running"
        );
    }

    #[test]
    fn strategy_b_phase_validator_detects_order_mismatch() {
        let normalized = NormalizedTrace::from_events(&[
            TraceEvent::Phase {
                phase: TracePhase::Running,
            },
            TraceEvent::Phase {
                phase: TracePhase::Stopped,
            },
        ]);
        let err = PhaseOrderValidator
            .validate(&normalized)
            .expect_err("phase mismatch should fail");
        assert!(
            err.contains("phase order mismatch"),
            "unexpected error: {err}"
        );
    }
}

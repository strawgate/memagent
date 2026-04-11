//! Prototype bridge: runtime trace events -> transition-contract validator.
//!
//! This stays test-only (`turmoil_sim`) and emits JSONL for easy human inspection.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use logfwd::pipeline::{
    TransitionAction as RuntimeTransitionAction,
    TransitionDisposition as RuntimeTransitionDisposition,
    TransitionEvent as RuntimeTransitionEvent, TransitionOutcome as RuntimeTransitionOutcome,
};
use logfwd_runtime::turmoil_barriers::{PipelinePhase, RuntimeBarrierEvent};
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
    BatchTerminal {
        batch_id: Option<u64>,
        source_id: Option<u64>,
        checkpoint: Option<u64>,
        outcome: TraceOutcome,
        disposition: TraceDisposition,
        rows: u64,
    },
    SinkAttempt {
        outcome: SinkOutcome,
        rows: u64,
    },
    SinkResult {
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
}

impl TraceEvent {
    fn kind(&self) -> &'static str {
        match self {
            Self::Phase { .. } => "phase",
            Self::BatchBegin { .. } => "batch_begin",
            Self::BatchTerminal { .. } => "batch_terminal",
            Self::SinkAttempt { .. } => "sink_attempt",
            Self::SinkResult { .. } => "sink_result",
            Self::CheckpointUpdate { .. } => "checkpoint_update",
            Self::CheckpointFlush { .. } => "checkpoint_flush",
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
                format!(
                    "batch_begin batch={batch_id} source={source_id} checkpoint={checkpoint}"
                )
            }
            Self::BatchTerminal {
                batch_id,
                source_id,
                checkpoint,
                outcome,
                disposition,
                rows,
            } => {
                format!(
                    "batch_terminal batch={batch_id:?} source={source_id:?} checkpoint={checkpoint:?} outcome={} disposition={} rows={rows}",
                    outcome.as_str(),
                    disposition.as_str()
                )
            }
            Self::SinkAttempt { outcome, rows } => {
                format!("sink_attempt outcome={} rows={rows}", outcome.as_str())
            }
            Self::SinkResult { outcome, rows } => {
                format!("sink_result outcome={} rows={rows}", outcome.as_str())
            }
            Self::CheckpointUpdate { source_id, offset } => {
                format!("checkpoint_update source_id={source_id} offset={offset}")
            }
            Self::CheckpointFlush { success } => format!("checkpoint_flush success={success}"),
        }
    }
}

/// Convert runtime-emitted turmoil barrier events into contract trace events.
#[must_use]
pub fn trace_event_from_runtime_barrier(event: &RuntimeBarrierEvent) -> Option<TraceEvent> {
    match event {
        RuntimeBarrierEvent::PipelinePhase { phase } => {
            let phase = match phase {
                PipelinePhase::Running => TracePhase::Running,
                PipelinePhase::Draining => TracePhase::Draining,
                PipelinePhase::Stopped => TracePhase::Stopped,
            };
            Some(TraceEvent::Phase { phase })
        }
        RuntimeBarrierEvent::BeforeWorkerAckSend {
            outcome, num_rows, ..
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
            Some(TraceEvent::SinkResult {
                outcome,
                rows: *num_rows,
            })
        }
        RuntimeBarrierEvent::CheckpointFlush { success } => {
            Some(TraceEvent::CheckpointFlush { success: *success })
        }
        RuntimeBarrierEvent::BeforeCheckpointFlushAttempt { .. } => None,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceOutcome {
    Delivered,
    Rejected,
    RetryExhausted,
    TimedOut,
    PoolClosed,
    WorkerChannelClosed,
    NoWorkersAvailable,
    InternalFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceDisposition {
    Ack,
    Reject,
    Hold,
    Abandon,
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

impl TraceOutcome {
    fn as_str(self) -> &'static str {
        match self {
            TraceOutcome::Delivered => "delivered",
            TraceOutcome::Rejected => "rejected",
            TraceOutcome::RetryExhausted => "retry_exhausted",
            TraceOutcome::TimedOut => "timed_out",
            TraceOutcome::PoolClosed => "pool_closed",
            TraceOutcome::WorkerChannelClosed => "worker_channel_closed",
            TraceOutcome::NoWorkersAvailable => "no_workers_available",
            TraceOutcome::InternalFailure => "internal_failure",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "delivered" | "ok" => Some(Self::Delivered),
            "rejected" => Some(Self::Rejected),
            "retry_exhausted" => Some(Self::RetryExhausted),
            "timed_out" => Some(Self::TimedOut),
            "pool_closed" => Some(Self::PoolClosed),
            "worker_channel_closed" => Some(Self::WorkerChannelClosed),
            "no_workers_available" => Some(Self::NoWorkersAvailable),
            "internal_failure" | "panic" => Some(Self::InternalFailure),
            _ => None,
        }
    }
}

impl TraceDisposition {
    fn as_str(self) -> &'static str {
        match self {
            TraceDisposition::Ack => "ack",
            TraceDisposition::Reject => "reject",
            TraceDisposition::Hold => "hold",
            TraceDisposition::Abandon => "abandon",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "ack" => Some(Self::Ack),
            "reject" => Some(Self::Reject),
            "hold" => Some(Self::Hold),
            "abandon" => Some(Self::Abandon),
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
                json!({
                    "event": "transition",
                    "phase": "begin_send",
                    "batch": batch_id,
                    "source": source_id,
                    "checkpoint": checkpoint
                })
            }
            TraceEvent::BatchTerminal {
                batch_id,
                source_id,
                checkpoint,
                outcome,
                disposition,
                rows,
            } => {
                json!({
                    "event": "transition",
                    "phase": "terminal",
                    "batch": batch_id,
                    "source": source_id,
                    "checkpoint": checkpoint,
                    "outcome": outcome.as_str(),
                    "disposition": disposition.as_str(),
                    "rows": rows
                })
            }
            TraceEvent::SinkAttempt { outcome, rows } => {
                json!({"event": "sink_attempt", "outcome": outcome.as_str(), "rows": rows})
            }
            TraceEvent::SinkResult { outcome, rows } => {
                json!({"event": "sink_result", "outcome": outcome.as_str(), "rows": rows})
            }
            TraceEvent::CheckpointUpdate { source_id, offset } => {
                json!({
                    "event": "checkpoint_update",
                    "source_id": source_id,
                    "source": source_id,
                    "offset": offset,
                    "checkpoint": offset
                })
            }
            TraceEvent::CheckpointFlush { success } => {
                json!({"event": "checkpoint_flush", "success": success})
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
            "transition" | "batch_transition" => {
                let Some(raw_phase) = v.get("phase").and_then(Value::as_str) else {
                    return Err("transition event missing string field: phase".to_string());
                };
                match raw_phase {
                    "begin_send" => {
                        let batch_id = required_u64_any(v, &["batch", "batch_id"])?;
                        let source_id = required_u64_any(v, &["source", "source_id"])?;
                        let checkpoint = required_u64_any(v, &["checkpoint", "offset"])?;
                        Ok(Self::BatchBegin {
                            batch_id,
                            source_id,
                            checkpoint,
                        })
                    }
                    "terminal" => {
                        let Some(raw_outcome) = v.get("outcome").and_then(Value::as_str) else {
                            return Err(
                                "terminal transition missing string field: outcome".to_string()
                            );
                        };
                        let Some(outcome) = TraceOutcome::from_str(raw_outcome) else {
                            return Err(format!("unknown trace outcome '{raw_outcome}'"));
                        };
                        let Some(raw_disposition) = v.get("disposition").and_then(Value::as_str)
                        else {
                            return Err(
                                "terminal transition missing string field: disposition".to_string()
                            );
                        };
                        let Some(disposition) = TraceDisposition::from_str(raw_disposition) else {
                            return Err(format!("unknown trace disposition '{raw_disposition}'"));
                        };
                        let rows = v.get("rows").and_then(Value::as_u64).unwrap_or(0);
                        Ok(Self::BatchTerminal {
                            batch_id: optional_u64_any(v, &["batch", "batch_id"]),
                            source_id: optional_u64_any(v, &["source", "source_id"]),
                            checkpoint: optional_u64_any(v, &["checkpoint", "offset"]),
                            outcome,
                            disposition,
                            rows,
                        })
                    }
                    other => Err(format!("unknown transition phase '{other}'")),
                }
            }
            "batch_begin" => {
                let batch_id = required_u64_any(v, &["batch", "batch_id"])?;
                let source_id = required_u64_any(v, &["source", "source_id"])?;
                let checkpoint = required_u64_any(v, &["checkpoint", "offset"])?;
                Ok(Self::BatchBegin {
                    batch_id,
                    source_id,
                    checkpoint,
                })
            }
            "batch_terminal" => {
                let Some(raw_outcome) = v.get("outcome").and_then(Value::as_str) else {
                    return Err("batch_terminal missing string field: outcome".to_string());
                };
                let Some(outcome) = TraceOutcome::from_str(raw_outcome) else {
                    return Err(format!("unknown trace outcome '{raw_outcome}'"));
                };
                let Some(raw_disposition) = v.get("disposition").and_then(Value::as_str) else {
                    return Err("batch_terminal missing string field: disposition".to_string());
                };
                let Some(disposition) = TraceDisposition::from_str(raw_disposition) else {
                    return Err(format!("unknown trace disposition '{raw_disposition}'"));
                };
                let rows = v.get("rows").and_then(Value::as_u64).unwrap_or(0);
                Ok(Self::BatchTerminal {
                    batch_id: optional_u64_any(v, &["batch", "batch_id"]),
                    source_id: optional_u64_any(v, &["source", "source_id"]),
                    checkpoint: optional_u64_any(v, &["checkpoint", "offset"]),
                    outcome,
                    disposition,
                    rows,
                })
            }
            "sink_attempt" => {
                let Some(raw_outcome) = v.get("outcome").and_then(Value::as_str) else {
                    return Err("sink_attempt missing string field: outcome".to_string());
                };
                let Some(outcome) = SinkOutcome::from_str(raw_outcome) else {
                    return Err(format!("unknown sink outcome '{raw_outcome}'"));
                };
                let Some(rows) = v.get("rows").and_then(Value::as_u64) else {
                    return Err("sink_attempt missing u64 field: rows".to_string());
                };
                Ok(Self::SinkAttempt { outcome, rows })
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
                Ok(Self::SinkResult { outcome, rows })
            }
            "checkpoint_update" => {
                let source_id = required_u64_any(v, &["source_id", "source"])?;
                let offset = required_u64_any(v, &["offset", "checkpoint"])?;
                Ok(Self::CheckpointUpdate { source_id, offset })
            }
            "checkpoint_flush" => {
                let Some(success) = v.get("success").and_then(Value::as_bool) else {
                    return Err("checkpoint_flush missing bool field: success".to_string());
                };
                Ok(Self::CheckpointFlush { success })
            }
            other => Err(format!("unknown event kind '{other}'")),
        }
    }
}

fn required_u64_any(v: &Value, fields: &[&str]) -> Result<u64, String> {
    optional_u64_any(v, fields).ok_or_else(|| {
        format!(
            "missing u64 field: {}",
            fields.iter().copied().collect::<Vec<_>>().join(" or ")
        )
    })
}

fn optional_u64_any(v: &Value, fields: &[&str]) -> Option<u64> {
    fields
        .iter()
        .find_map(|field| v.get(*field).and_then(Value::as_u64))
}

/// Simple JSONL recorder used by simulation-only codepaths.
#[derive(Clone)]
pub struct TraceRecorder {
    file: Arc<Mutex<File>>,
    next_batch_id: Arc<AtomicU64>,
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
            next_batch_id: Arc::new(AtomicU64::new(0)),
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

    pub fn record_batch_begin(&self, source_id: u64, checkpoint: u64) -> u64 {
        let batch_id = self.next_batch_id.fetch_add(1, Ordering::SeqCst);
        self.record(TraceEvent::BatchBegin {
            batch_id,
            source_id,
            checkpoint,
        });
        batch_id
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
        .filter_map(|event| match event {
            TraceEvent::Phase { phase } => Some(format!("phase:{}", phase.as_str())),
            TraceEvent::BatchBegin {
                batch_id,
                source_id,
                checkpoint,
            } => Some(format!("batch_begin:{batch_id}:{source_id}:{checkpoint}")),
            TraceEvent::BatchTerminal {
                outcome,
                disposition,
                rows,
                ..
            } => Some(format!(
                "batch_terminal:{}:{}:{rows}",
                outcome.as_str(),
                disposition.as_str()
            )),
            // SinkAttempt is an intermediate event (pre-result); excluded from the contract trace.
            TraceEvent::SinkAttempt { .. } => None,
            TraceEvent::SinkResult { outcome, rows } => {
                Some(format!("sink:{}:{rows}", outcome.as_str()))
            }
            TraceEvent::CheckpointUpdate { source_id, offset } => {
                Some(format!("ckpt_update:{source_id}:{offset}"))
            }
            TraceEvent::CheckpointFlush { success } => Some(format!("ckpt_flush:{success}")),
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
                    TraceEvent::BatchTerminal {
                        batch_id,
                        source_id,
                        checkpoint,
                        outcome,
                        disposition,
                        rows,
                    } => {
                        if let Some(batch_id) = batch_id {
                            attributes.insert("batch_id", batch_id.to_string());
                        }
                        if let Some(source_id) = source_id {
                            attributes.insert("source_id", source_id.to_string());
                        }
                        if let Some(checkpoint) = checkpoint {
                            attributes.insert("checkpoint", checkpoint.to_string());
                        }
                        attributes.insert("outcome", outcome.as_str().to_string());
                        attributes.insert("disposition", disposition.as_str().to_string());
                        attributes.insert("rows", rows.to_string());
                    }
                    TraceEvent::SinkAttempt { outcome, rows } => {
                        attributes.insert("outcome", outcome.as_str().to_string());
                        attributes.insert("rows", rows.to_string());
                    }
                    TraceEvent::SinkResult { outcome, rows } => {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchStatus {
    Open,
    Held,
    Acked,
    Rejected,
    Abandoned,
}

#[derive(Debug, Clone)]
struct BatchState {
    source_id: u64,
    checkpoint: u64,
    status: BatchStatus,
    saw_failure_attempt: bool,
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
        let mut phase: Option<PhaseState> = None;
        let mut saw_stopped = false;
        let mut source_offsets = self.source_offsets.clone();
        let mut batches = BTreeMap::<u64, BatchState>::new();
        let mut source_batches = BTreeMap::<u64, VecDeque<u64>>::new();
        let saw_phase_marker = events
            .iter()
            .any(|event| matches!(event, TraceEvent::Phase { .. }));
        let saw_batch_marker = events
            .iter()
            .any(|event| matches!(event, TraceEvent::BatchBegin { .. }));

        for (idx, event) in events.iter().enumerate() {
            match event {
                TraceEvent::Phase {
                    phase: TracePhase::Running,
                } => {
                    if idx != 0 || phase.is_some() {
                        return Err(ValidationError::at(
                            events,
                            idx,
                            "duplicate_running",
                            "running phase marker may only appear first",
                        ));
                    }
                    phase = Some(PhaseState::Running);
                }
                TraceEvent::Phase {
                    phase: TracePhase::Draining,
                } => {
                    if phase != Some(PhaseState::Running) {
                        return Err(ValidationError::at(
                            events,
                            idx,
                            "invalid_phase_transition",
                            format!("invalid phase transition {phase:?} -> Draining"),
                        ));
                    }
                    phase = Some(PhaseState::Draining);
                }
                TraceEvent::Phase {
                    phase: TracePhase::Stopped,
                } => {
                    if phase != Some(PhaseState::Draining) {
                        return Err(ValidationError::at(
                            events,
                            idx,
                            "invalid_phase_transition",
                            format!("invalid phase transition {phase:?} -> Stopped"),
                        ));
                    }
                    phase = Some(PhaseState::Stopped);
                    saw_stopped = true;
                }
                TraceEvent::BatchBegin {
                    batch_id,
                    source_id,
                    checkpoint,
                } => {
                    reject_after_stopped(events, idx, saw_stopped, "begin_send")?;
                    if batches.contains_key(batch_id) {
                        return Err(ValidationError::at(
                            events,
                            idx,
                            "duplicate_batch_id",
                            format!("duplicate batch id {batch_id}"),
                        ));
                    }
                    batches.insert(
                        *batch_id,
                        BatchState {
                            source_id: *source_id,
                            checkpoint: *checkpoint,
                            status: BatchStatus::Open,
                            saw_failure_attempt: false,
                        },
                    );
                    source_batches
                        .entry(*source_id)
                        .or_default()
                        .push_back(*batch_id);
                }
                TraceEvent::BatchTerminal {
                    batch_id,
                    source_id,
                    checkpoint,
                    outcome,
                    disposition,
                    rows: _,
                } => {
                    reject_after_stopped(events, idx, saw_stopped, "sink/disposition activity")?;
                    apply_terminal_event(
                        idx,
                        &mut batches,
                        &mut source_batches,
                        saw_batch_marker,
                        *batch_id,
                        *source_id,
                        *checkpoint,
                        *outcome,
                        *disposition,
                    )
                    .map_err(|msg| ValidationError::at(events, idx, "terminal_event", msg))?;
                }
                TraceEvent::CheckpointUpdate { source_id, offset } => {
                    reject_after_stopped(events, idx, saw_stopped, "checkpoint update")?;
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
                    for (batch_id, batch) in &batches {
                        if batch.source_id == *source_id
                            && matches!(batch.status, BatchStatus::Open | BatchStatus::Held)
                            && *offset >= batch.checkpoint
                        {
                            return Err(ValidationError::at(
                                events,
                                idx,
                                "checkpoint_past_unresolved_batch",
                                format!(
                                    "checkpoint advanced to {offset} past unresolved gap \
                                     batch {batch_id} for source {source_id} at checkpoint {}",
                                    batch.checkpoint
                                ),
                            ));
                        }
                    }
                    source_offsets.insert(*source_id, *offset);
                }
                TraceEvent::CheckpointFlush { .. } => {
                    reject_after_stopped(events, idx, saw_stopped, "flush")?;
                }
                TraceEvent::SinkAttempt { outcome, .. } => {
                    reject_after_stopped(events, idx, saw_stopped, "sink activity")?;
                    if matches!(
                        outcome,
                        SinkOutcome::RetryAfter | SinkOutcome::IoError | SinkOutcome::Panic
                    ) {
                        if let Some(batch_id) = first_open_batch(&batches, None) {
                            if let Some(batch) = batches.get_mut(&batch_id) {
                                batch.saw_failure_attempt = true;
                            }
                        }
                    }
                }
                TraceEvent::SinkResult { outcome, rows } => {
                    reject_after_stopped(events, idx, saw_stopped, "sink activity")?;
                    apply_legacy_sink_result(
                        idx,
                        &mut batches,
                        &mut source_batches,
                        saw_batch_marker,
                        *outcome,
                        *rows,
                    )
                    .map_err(|msg| ValidationError::at(events, idx, "legacy_sink", msg))?;
                }
            }
        }

        if saw_phase_marker && phase != Some(PhaseState::Stopped) {
            return Err(ValidationError::at(
                events,
                events.len().saturating_sub(1),
                "terminal_phase_missing",
                format!("trace ended before stopped transition (last phase: {phase:?})"),
            ));
        }
        for (batch_id, batch) in &batches {
            if batch.status == BatchStatus::Open {
                return Err(ValidationError::at(
                    events,
                    events.len().saturating_sub(1),
                    "unterminated_batch",
                    format!(
                        "batch {batch_id}: begin_send was not terminalized or explicitly held \
                         (source_id={}, checkpoint={}, saw_failure_attempt={})",
                        batch.source_id, batch.checkpoint, batch.saw_failure_attempt
                    ),
                ));
            }
        }
        Ok(())
    }
}

/// Project runtime-emitted transition events into the turmoil trace contract.
pub fn trace_events_from_runtime(events: &[RuntimeTransitionEvent]) -> Vec<TraceEvent> {
    if events.is_empty() {
        return Vec::new();
    }

    let mut trace = Vec::with_capacity(events.len() + 2);
    let mut current_phase = Some(TracePhase::Running);
    let mut sink_outcomes_by_batch_source: BTreeMap<(u64, u64), TraceOutcome> = BTreeMap::new();

    trace.push(TraceEvent::Phase {
        phase: TracePhase::Running,
    });

    for event in events {
        match event.action {
            RuntimeTransitionAction::EnterSending => {
                if let (Some(batch_id), Some(source_id), Some(checkpoint)) =
                    (event.batch_id, event.source_id, event.checkpoint_offset)
                {
                    trace.push(TraceEvent::BatchBegin {
                        batch_id,
                        source_id,
                        checkpoint,
                    });
                }
            }
            RuntimeTransitionAction::SinkOutcome => {
                if let Some(outcome) = event.outcome.and_then(trace_outcome_from_runtime) {
                    if let (Some(batch_id), Some(source_id)) = (event.batch_id, event.source_id) {
                        sink_outcomes_by_batch_source.insert((batch_id, source_id), outcome);
                    }
                }
            }
            RuntimeTransitionAction::ApplyDisposition => {
                if let Some(disposition) =
                    event.disposition.and_then(trace_disposition_from_runtime)
                {
                    let outcome = match disposition {
                        TraceDisposition::Ack => TraceOutcome::Delivered,
                        TraceDisposition::Reject => TraceOutcome::Rejected,
                        TraceDisposition::Abandon => TraceOutcome::InternalFailure,
                        TraceDisposition::Hold => {
                            if let (Some(batch_id), Some(source_id)) =
                                (event.batch_id, event.source_id)
                            {
                                sink_outcomes_by_batch_source
                                    .get(&(batch_id, source_id))
                                    .copied()
                                    .or_else(|| event.outcome.and_then(trace_outcome_from_runtime))
                                    .unwrap_or(TraceOutcome::InternalFailure)
                            } else {
                                event
                                    .outcome
                                    .and_then(trace_outcome_from_runtime)
                                    .unwrap_or(TraceOutcome::InternalFailure)
                            }
                        }
                    };

                    trace.push(TraceEvent::BatchTerminal {
                        batch_id: event.batch_id,
                        source_id: event.source_id,
                        checkpoint: event.checkpoint_offset,
                        outcome,
                        disposition,
                        rows: 0,
                    });
                }
            }
            RuntimeTransitionAction::UpdateCheckpoint => {
                if let (Some(source_id), Some(offset)) = (event.source_id, event.checkpoint_offset)
                {
                    trace.push(TraceEvent::CheckpointUpdate { source_id, offset });
                }
            }
            RuntimeTransitionAction::FlushCheckpoint => {
                if let Some(outcome) = event.outcome {
                    let success = matches!(outcome, RuntimeTransitionOutcome::FlushSucceeded);
                    let failure = matches!(outcome, RuntimeTransitionOutcome::FlushFailed);
                    if success || failure {
                        trace.push(TraceEvent::CheckpointFlush { success });
                    }
                }
            }
            RuntimeTransitionAction::BeginDrain => {
                if current_phase != Some(TracePhase::Draining) {
                    current_phase = Some(TracePhase::Draining);
                    trace.push(TraceEvent::Phase {
                        phase: TracePhase::Draining,
                    });
                }
            }
            RuntimeTransitionAction::Stop | RuntimeTransitionAction::ForceStop => {
                if matches!(
                    event.outcome,
                    Some(RuntimeTransitionOutcome::Completed | RuntimeTransitionOutcome::Forced)
                ) && current_phase != Some(TracePhase::Stopped)
                {
                    current_phase = Some(TracePhase::Stopped);
                    trace.push(TraceEvent::Phase {
                        phase: TracePhase::Stopped,
                    });
                }
            }
        }
    }

    trace
}

fn trace_outcome_from_runtime(outcome: RuntimeTransitionOutcome) -> Option<TraceOutcome> {
    match outcome {
        RuntimeTransitionOutcome::Delivered => Some(TraceOutcome::Delivered),
        RuntimeTransitionOutcome::Rejected => Some(TraceOutcome::Rejected),
        RuntimeTransitionOutcome::RetryExhausted => Some(TraceOutcome::RetryExhausted),
        RuntimeTransitionOutcome::TimedOut => Some(TraceOutcome::TimedOut),
        RuntimeTransitionOutcome::PoolClosed => Some(TraceOutcome::PoolClosed),
        RuntimeTransitionOutcome::WorkerChannelClosed => Some(TraceOutcome::WorkerChannelClosed),
        RuntimeTransitionOutcome::NoWorkersAvailable => Some(TraceOutcome::NoWorkersAvailable),
        RuntimeTransitionOutcome::InternalFailure => Some(TraceOutcome::InternalFailure),
        _ => None,
    }
}

fn trace_disposition_from_runtime(
    disposition: RuntimeTransitionDisposition,
) -> Option<TraceDisposition> {
    match disposition {
        RuntimeTransitionDisposition::Ack => Some(TraceDisposition::Ack),
        RuntimeTransitionDisposition::Reject => Some(TraceDisposition::Reject),
        RuntimeTransitionDisposition::Hold => Some(TraceDisposition::Hold),
    }
}

fn reject_after_stopped(
    events: &[TraceEvent],
    idx: usize,
    saw_stopped: bool,
    activity: &str,
) -> Result<(), ValidationError> {
    if saw_stopped {
        Err(ValidationError::at(
            events,
            idx,
            "activity_after_stopped",
            format!("{activity} after Stopped"),
        ))
    } else {
        Ok(())
    }
}

fn apply_legacy_sink_result(
    idx: usize,
    batches: &mut BTreeMap<u64, BatchState>,
    source_batches: &mut BTreeMap<u64, VecDeque<u64>>,
    saw_batch_marker: bool,
    outcome: SinkOutcome,
    rows: u64,
) -> Result<(), String> {
    match outcome {
        SinkOutcome::Ok => apply_terminal_event(
            idx,
            batches,
            source_batches,
            saw_batch_marker,
            None,
            None,
            None,
            TraceOutcome::Delivered,
            TraceDisposition::Ack,
        ),
        SinkOutcome::Rejected => apply_terminal_event(
            idx,
            batches,
            source_batches,
            saw_batch_marker,
            None,
            None,
            None,
            TraceOutcome::Rejected,
            TraceDisposition::Reject,
        ),
        SinkOutcome::Panic => apply_terminal_event(
            idx,
            batches,
            source_batches,
            saw_batch_marker,
            None,
            None,
            None,
            TraceOutcome::InternalFailure,
            TraceDisposition::Hold,
        ),
        SinkOutcome::RetryAfter | SinkOutcome::IoError => {
            if rows > 0 {
                if let Some(batch_id) = first_open_batch(batches, None) {
                    if let Some(batch) = batches.get_mut(&batch_id) {
                        batch.saw_failure_attempt = true;
                    }
                }
            }
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_terminal_event(
    idx: usize,
    batches: &mut BTreeMap<u64, BatchState>,
    source_batches: &mut BTreeMap<u64, VecDeque<u64>>,
    saw_batch_marker: bool,
    batch_id: Option<u64>,
    source_id: Option<u64>,
    checkpoint: Option<u64>,
    outcome: TraceOutcome,
    disposition: TraceDisposition,
) -> Result<(), String> {
    let Some(batch_id) = batch_id.or_else(|| first_open_batch(batches, source_id)) else {
        if saw_batch_marker {
            return Err(format!(
                "event {idx}: terminal outcome {outcome:?}/{disposition:?} without open begin_send"
            ));
        }
        return Ok(());
    };

    let Some(batch) = batches.get_mut(&batch_id) else {
        return Err(format!(
            "event {idx}: terminal outcome references unknown batch {batch_id}"
        ));
    };
    if batch.status != BatchStatus::Open {
        return Err(format!(
            "event {idx}: terminal outcome for batch {batch_id} after prior {:?}",
            batch.status
        ));
    }
    if let Some(source_id) = source_id {
        if source_id != batch.source_id {
            return Err(format!(
                "event {idx}: terminal outcome source mismatch for batch {batch_id}: \
                 {source_id} != {}",
                batch.source_id
            ));
        }
    }
    if let Some(checkpoint) = checkpoint {
        if checkpoint != batch.checkpoint {
            return Err(format!(
                "event {idx}: terminal outcome checkpoint mismatch for batch {batch_id}: \
                 {checkpoint} != {}",
                batch.checkpoint
            ));
        }
    }

    batch.status = match disposition {
        TraceDisposition::Ack => {
            if outcome != TraceOutcome::Delivered {
                return Err(format!(
                    "event {idx}: ack disposition must carry delivered outcome, got {outcome:?}"
                ));
            }
            BatchStatus::Acked
        }
        TraceDisposition::Reject => {
            if outcome != TraceOutcome::Rejected {
                return Err(format!(
                    "event {idx}: reject disposition must carry rejected outcome, got {outcome:?}"
                ));
            }
            BatchStatus::Rejected
        }
        TraceDisposition::Hold => BatchStatus::Held,
        TraceDisposition::Abandon => BatchStatus::Abandoned,
    };

    if !matches!(batch.status, BatchStatus::Held) {
        remove_from_source_queue(source_batches, batch.source_id, batch_id);
    }
    Ok(())
}

fn first_open_batch(batches: &BTreeMap<u64, BatchState>, source_id: Option<u64>) -> Option<u64> {
    batches
        .iter()
        .find(|(_, batch)| {
            batch.status == BatchStatus::Open && source_id.is_none_or(|sid| sid == batch.source_id)
        })
        .map(|(batch_id, _)| *batch_id)
}

fn remove_from_source_queue(
    source_batches: &mut BTreeMap<u64, VecDeque<u64>>,
    source_id: u64,
    batch_id: u64,
) {
    if let Some(queue) = source_batches.get_mut(&source_id) {
        if let Some(pos) = queue.iter().position(|candidate| *candidate == batch_id) {
            queue.remove(pos);
        }
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

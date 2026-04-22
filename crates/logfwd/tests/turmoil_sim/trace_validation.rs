//! End-to-end trace validation prototype tests.

use std::sync::atomic::Ordering;
use std::time::Duration;

use logfwd::pipeline::Pipeline;
use logfwd_types::pipeline::SourceId;
use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;

use super::channel_input::ChannelInputSource;
use super::instrumented_sink::{FailureAction, InstrumentedSink};
use super::observable_checkpoint::ObservableCheckpointStore;
use super::trace_bridge::{
    NormalizedTrace, TraceEvent, TracePhase, TraceRecorder, TransitionValidator, load_trace,
};
use super::validators;

fn generate_json_lines(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("{{\"msg\":\"line {i}\",\"num\":{i}}}\n").into_bytes())
        .collect()
}

#[test]
fn trace_bridge_end_to_end_validates_runtime_transitions() {
    let mut sim = super::build_sim(40, 1);

    let trace_path = NamedTempFile::new()
        .expect("create temp trace file")
        .into_temp_path();
    let trace = TraceRecorder::new(&trace_path).expect("create trace recorder");

    let sink = InstrumentedSink::always_succeed().with_trace_recorder(trace.clone());
    let delivered_counter = sink.delivered_counter();

    let (store, handle) = ObservableCheckpointStore::new();
    let store = store.with_trace_recorder(trace.clone());

    sim.client("pipeline", async move {
        trace.record(TraceEvent::Phase {
            phase: TracePhase::Running,
        });

        let lines = generate_json_lines(20);
        let input = ChannelInputSource::new("test", SourceId(1), lines);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(20));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(50));
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let trace_for_shutdown = trace.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            trace_for_shutdown.record(TraceEvent::Phase {
                phase: TracePhase::Draining,
            });
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();

        trace.record(TraceEvent::Phase {
            phase: TracePhase::Stopped,
        });

        Ok(())
    });

    sim.run().unwrap();

    assert_eq!(
        delivered_counter.load(Ordering::Relaxed),
        20,
        "expected all rows delivered"
    );
    assert!(
        handle.durable_offset(1).is_some(),
        "expected durable checkpoint for source 1"
    );

    let events = load_trace(&trace_path).expect("load trace file");
    assert!(!events.is_empty(), "trace must contain events");

    let validator = TransitionValidator::default();
    validator
        .validate(&events)
        .expect("runtime event stream should satisfy declared trace contract");
}

/// End-to-end test: runs all PipelineMachine validators against a real
/// turmoil simulation trace. Verifies NoDoubleComplete, DrainCompleteness,
/// NoCreateAfterDrain, CheckpointOrdering, and CommittedMonotonic on a
/// normal (no-failure) pipeline run with 20 JSON lines.
#[test]
fn trace_validates_pipeline_machine_properties_normal_run() {
    let mut sim = super::build_sim(40, 1);

    let trace_path = NamedTempFile::new()
        .expect("create temp trace file")
        .into_temp_path();
    let trace = TraceRecorder::new(&trace_path).expect("create trace recorder");

    let sink = InstrumentedSink::always_succeed().with_trace_recorder(trace.clone());
    let delivered_counter = sink.delivered_counter();

    let (store, handle) = ObservableCheckpointStore::new();
    let store = store.with_trace_recorder(trace.clone());

    sim.client("pipeline", async move {
        trace.record(TraceEvent::Phase {
            phase: TracePhase::Running,
        });

        let lines = generate_json_lines(20);
        let input = ChannelInputSource::new("test", SourceId(1), lines);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(20));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(50));
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let trace_for_shutdown = trace.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            trace_for_shutdown.record(TraceEvent::Phase {
                phase: TracePhase::Draining,
            });
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();

        trace.record(TraceEvent::Phase {
            phase: TracePhase::Stopped,
        });

        Ok(())
    });

    sim.run().unwrap();

    assert!(
        delivered_counter.load(Ordering::Relaxed) > 0,
        "expected rows delivered"
    );
    assert!(
        handle.durable_offset(1).is_some(),
        "expected durable checkpoint for source 1"
    );

    let events = load_trace(&trace_path).expect("load trace file");
    assert!(!events.is_empty(), "trace must contain events");

    // Run baseline TransitionValidator.
    let validator = TransitionValidator::default();
    validator
        .validate(&events)
        .expect("baseline transition contract");

    // Run all PipelineMachine validators.
    let normalized = NormalizedTrace::from_events(&events);
    validators::run_all(&normalized).expect("all PipelineMachine TLA+ properties should hold");
}

#[test]
fn trace_validator_rejects_phase_regression() {
    let events = vec![
        TraceEvent::Phase {
            phase: TracePhase::Running,
        },
        TraceEvent::Phase {
            phase: TracePhase::Draining,
        },
        TraceEvent::Phase {
            phase: TracePhase::Running,
        },
        TraceEvent::Phase {
            phase: TracePhase::Stopped,
        },
    ];

    let validator = TransitionValidator::default();
    let err = validator
        .validate(&events)
        .expect_err("phase regression must be rejected");
    assert!(
        err.contains("running phase marker may only appear first")
            || err.contains("invalid phase transition"),
        "unexpected validator error: {err}"
    );
}

#[test]
fn trace_validator_rejects_checkpoint_regression() {
    let events = vec![
        TraceEvent::Phase {
            phase: TracePhase::Running,
        },
        TraceEvent::CheckpointUpdate {
            source_id: 99,
            offset: 120,
        },
        TraceEvent::CheckpointUpdate {
            source_id: 99,
            offset: 119,
        },
        TraceEvent::Phase {
            phase: TracePhase::Draining,
        },
        TraceEvent::Phase {
            phase: TracePhase::Stopped,
        },
    ];

    let validator = TransitionValidator::default();
    let err = validator
        .validate(&events)
        .expect_err("checkpoint regression must be rejected");
    assert!(
        err.contains("checkpoint regression"),
        "unexpected validator error: {err}"
    );
}

#[test]
fn trace_validator_rejects_missing_running_marker() {
    let events = vec![
        TraceEvent::CheckpointUpdate {
            source_id: 1,
            offset: 1,
        },
        TraceEvent::Phase {
            phase: TracePhase::Draining,
        },
        TraceEvent::Phase {
            phase: TracePhase::Stopped,
        },
    ];

    let validator = TransitionValidator::default();
    let err = validator
        .validate(&events)
        .expect_err("trace without initial running marker must be rejected");
    assert!(
        err.contains("start with running phase marker"),
        "unexpected validator error: {err}"
    );
}

#[test]
fn trace_validator_rejects_sink_activity_after_stopped() {
    let events = vec![
        TraceEvent::Phase {
            phase: TracePhase::Running,
        },
        TraceEvent::Phase {
            phase: TracePhase::Draining,
        },
        TraceEvent::Phase {
            phase: TracePhase::Stopped,
        },
        TraceEvent::SinkResult {
            worker_id: 0,
            outcome: super::trace_bridge::SinkOutcome::Ok,
            rows: 1,
        },
    ];

    let validator = TransitionValidator::default();
    let err = validator
        .validate(&events)
        .expect_err("sink activity after stopped must be rejected");
    assert!(
        err.contains("sink activity after stopped"),
        "unexpected validator error: {err}"
    );
}

#[test]
fn trace_validator_rejects_checkpoint_flush_after_stopped() {
    let events = vec![
        TraceEvent::Phase {
            phase: TracePhase::Running,
        },
        TraceEvent::Phase {
            phase: TracePhase::Draining,
        },
        TraceEvent::Phase {
            phase: TracePhase::Stopped,
        },
        TraceEvent::CheckpointFlush { success: true },
    ];

    let validator = TransitionValidator::default();
    let err = validator
        .validate(&events)
        .expect_err("flush activity after stopped must be rejected");
    assert!(
        err.contains("checkpoint flush after stopped"),
        "unexpected validator error: {err}"
    );
}

#[test]
fn trace_validator_rejects_trace_without_stopped_terminalization() {
    let events = vec![
        TraceEvent::Phase {
            phase: TracePhase::Running,
        },
        TraceEvent::SinkResult {
            worker_id: 0,
            outcome: super::trace_bridge::SinkOutcome::Ok,
            rows: 4,
        },
        TraceEvent::Phase {
            phase: TracePhase::Draining,
        },
    ];

    let validator = TransitionValidator::default();
    let err = validator
        .validate(&events)
        .expect_err("missing stopped phase must be rejected");
    assert!(
        err.contains("terminal_phase_missing") && err.contains("last phase: draining"),
        "unexpected validator error: {err}"
    );
}

#[test]
fn trace_validator_detailed_error_contains_operator_context() {
    let events = vec![
        TraceEvent::Phase {
            phase: TracePhase::Running,
        },
        TraceEvent::CheckpointUpdate {
            source_id: 12,
            offset: 42,
        },
        TraceEvent::CheckpointUpdate {
            source_id: 12,
            offset: 3,
        },
        TraceEvent::Phase {
            phase: TracePhase::Draining,
        },
        TraceEvent::Phase {
            phase: TracePhase::Stopped,
        },
    ];
    let validator = TransitionValidator::default();
    let err = validator
        .validate_detailed(&events)
        .expect_err("checkpoint regression must fail with detailed diagnostics");

    assert_eq!(err.code(), "checkpoint_regression");
    assert_eq!(err.index(), 2);
    assert!(
        err.message().contains("checkpoint regression"),
        "unexpected detailed message: {}",
        err.message()
    );
    assert_eq!(
        err.event_summary(),
        "checkpoint_update source_id=12 offset=3"
    );
    assert_eq!(
        err.previous_event_summary(),
        Some("checkpoint_update source_id=12 offset=42")
    );

    let rendered = err.to_string();

    assert!(
        rendered.contains("[checkpoint_regression]"),
        "expected stable machine-readable code: {rendered}"
    );
    assert!(
        rendered.contains("event #2"),
        "expected failing index context: {rendered}"
    );
    assert!(
        rendered.contains("previous=checkpoint_update source_id=12 offset=42"),
        "expected previous event context for operator debugging: {rendered}"
    );
}

// ---------------------------------------------------------------------------
// Failure scenario integration tests
//
// Exercise the trace validators against non-happy-path pipeline runs:
// rejection, IO errors, and mixed outcomes.
// ---------------------------------------------------------------------------

/// Sink rejects a batch. Validators must still pass: the rejected batch
/// should appear as BatchTerminal{Rejected} and conservation holds.
#[test]
fn trace_validates_properties_with_sink_rejection() {
    let mut sim = super::build_sim(40, 1);

    let trace_path = NamedTempFile::new()
        .expect("create temp trace file")
        .into_temp_path();
    let trace = TraceRecorder::new(&trace_path).expect("create trace recorder");

    // First batch succeeds, second is rejected, rest succeed.
    let script = vec![
        FailureAction::Succeed,
        FailureAction::Reject("test rejection".to_string()),
        FailureAction::Succeed,
        FailureAction::Succeed,
        FailureAction::Succeed,
        FailureAction::Succeed,
        FailureAction::Succeed,
        FailureAction::Succeed,
        FailureAction::Succeed,
        FailureAction::Succeed,
    ];
    let sink = InstrumentedSink::new(script).with_trace_recorder(trace.clone());

    let (store, _handle) = ObservableCheckpointStore::new();
    let store = store.with_trace_recorder(trace.clone());

    sim.client("pipeline", async move {
        trace.record(TraceEvent::Phase {
            phase: TracePhase::Running,
        });

        let lines = generate_json_lines(20);
        let input = ChannelInputSource::new("test", SourceId(1), lines);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(20));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(50));
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let trace_for_shutdown = trace.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            trace_for_shutdown.record(TraceEvent::Phase {
                phase: TracePhase::Draining,
            });
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();

        trace.record(TraceEvent::Phase {
            phase: TracePhase::Stopped,
        });

        Ok(())
    });

    sim.run().unwrap();

    let events = load_trace(&trace_path).expect("load trace file");
    assert!(!events.is_empty(), "trace must contain events");

    let validator = TransitionValidator::default();
    validator
        .validate(&events)
        .expect("baseline transition contract");

    let normalized = NormalizedTrace::from_events(&events);
    validators::run_all(&normalized).expect("all TLA+ properties should hold with rejection");
}

/// Sink returns transient IO errors on first attempt, then succeeds on retry.
/// Validators must pass: retried batches still reach terminal state.
#[test]
fn trace_validates_properties_with_transient_errors() {
    let mut sim = super::build_sim(40, 1);

    let trace_path = NamedTempFile::new()
        .expect("create temp trace file")
        .into_temp_path();
    let trace = TraceRecorder::new(&trace_path).expect("create trace recorder");

    // First attempt fails with IO error, retry succeeds.
    let script = vec![
        FailureAction::IoError(std::io::ErrorKind::ConnectionReset),
        FailureAction::Succeed,
        FailureAction::IoError(std::io::ErrorKind::ConnectionReset),
        FailureAction::Succeed,
        FailureAction::Succeed,
        FailureAction::Succeed,
        FailureAction::Succeed,
        FailureAction::Succeed,
        FailureAction::Succeed,
        FailureAction::Succeed,
    ];
    let sink = InstrumentedSink::new(script).with_trace_recorder(trace.clone());

    let (store, _handle) = ObservableCheckpointStore::new();
    let store = store.with_trace_recorder(trace.clone());

    sim.client("pipeline", async move {
        trace.record(TraceEvent::Phase {
            phase: TracePhase::Running,
        });

        let lines = generate_json_lines(20);
        let input = ChannelInputSource::new("test", SourceId(1), lines);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(20));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(50));
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let trace_for_shutdown = trace.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            trace_for_shutdown.record(TraceEvent::Phase {
                phase: TracePhase::Draining,
            });
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();

        trace.record(TraceEvent::Phase {
            phase: TracePhase::Stopped,
        });

        Ok(())
    });

    sim.run().unwrap();

    let events = load_trace(&trace_path).expect("load trace file");
    assert!(!events.is_empty(), "trace must contain events");

    let validator = TransitionValidator::default();
    validator
        .validate(&events)
        .expect("baseline transition contract");

    let normalized = NormalizedTrace::from_events(&events);
    validators::run_all(&normalized)
        .expect("all TLA+ properties should hold with transient errors and retry");
}

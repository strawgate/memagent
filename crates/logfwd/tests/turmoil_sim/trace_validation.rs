//! End-to-end trace validation prototype tests.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use logfwd::pipeline::{Pipeline, TransitionAction, TransitionEventRecorder};
use logfwd_types::pipeline::SourceId;
use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;

use super::channel_input::ChannelInputSource;
use super::instrumented_sink::{FailureAction, InstrumentedSink, InstrumentedSinkFactory};
use super::observable_checkpoint::ObservableCheckpointStore;
use super::trace_bridge::{
    TraceDisposition, TraceEvent, TraceOutcome, TracePhase, TraceRecorder, TransitionValidator,
    load_trace, trace_events_from_runtime,
};

fn generate_json_lines(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("{{\"msg\":\"line {i}\",\"num\":{i}}}\n").into_bytes())
        .collect()
}

fn total_bytes(lines: &[Vec<u8>]) -> u64 {
    lines.iter().map(|line| line.len() as u64).sum()
}

#[test]
fn trace_bridge_end_to_end_validates_runtime_event_contracts() {
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
        let lines = generate_json_lines(20);
        let input =
            ChannelInputSource::new("test", SourceId(1), lines).with_trace_recorder(trace.clone());

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(20));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(50));
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let phase_trace = trace.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            phase_trace.record(TraceEvent::Phase {
                phase: TracePhase::Draining,
            });
            sd.cancel();
        });

        trace.record(TraceEvent::Phase {
            phase: TracePhase::Running,
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
    assert!(
        events
            .iter()
            .any(|event| matches!(event, TraceEvent::BatchBegin { .. })),
        "trace must contain runtime-originated begin_send events"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            TraceEvent::BatchTerminal {
                disposition: TraceDisposition::Ack,
                outcome: TraceOutcome::Delivered,
                ..
            }
        )),
        "trace must contain delivered/ack terminal evidence"
    );

    let validator = TransitionValidator::default();
    validator
        .validate(&events)
        .expect("runtime event stream should satisfy declared trace contract");
}

#[test]
fn runtime_transition_emitter_end_to_end_validates_contracts() {
    let mut sim = super::build_sim(40, 1);
    let runtime_events = Arc::new(TransitionEventRecorder::default());
    let runtime_events_for_task = runtime_events.clone();

    let sink = InstrumentedSink::always_succeed();
    let delivered_counter = sink.delivered_counter();

    let (store, handle) = ObservableCheckpointStore::new();

    sim.client("pipeline", async move {
        let lines = generate_json_lines(20);
        let input = ChannelInputSource::new("runtime", SourceId(1), lines);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink))
            .with_transition_event_emitter(runtime_events_for_task.clone());
        pipeline.set_batch_timeout(Duration::from_millis(20));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(50));
        let mut pipeline = pipeline
            .with_input("runtime", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
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

    let runtime_snapshot = runtime_events.snapshot();
    assert!(
        runtime_snapshot
            .iter()
            .any(|event| matches!(event.action, TransitionAction::EnterSending)),
        "runtime transition stream must include begin_send events"
    );

    let trace_events = trace_events_from_runtime(&runtime_snapshot);
    assert!(
        trace_events
            .iter()
            .any(|event| matches!(event, TraceEvent::BatchBegin { .. })),
        "converted runtime trace must include begin_send evidence"
    );

    let validator = TransitionValidator::default();
    validator
        .validate(&trace_events)
        .expect("runtime transition stream should satisfy the declared contract");
}

#[test]
fn trace_bridge_e2e_panic_holds_gap_and_shutdown_stops_activity() {
    let mut sim = super::build_sim(120, 1);

    let trace_path = NamedTempFile::new()
        .expect("create temp trace file")
        .into_temp_path();
    let trace = TraceRecorder::new(&trace_path).expect("create trace recorder");

    let factory =
        InstrumentedSinkFactory::new(vec![vec![FailureAction::Succeed, FailureAction::Panic]])
            .with_trace_recorder(trace.clone());
    let delivered_counter = factory.delivered_counter();
    let call_counter = factory.call_counter();
    let factory = Arc::new(factory);

    let (store, handle) = ObservableCheckpointStore::new();
    let store = store.with_trace_recorder(trace.clone());
    let lines = generate_json_lines(8);
    let input_total_bytes = total_bytes(&lines);

    sim.client("pipeline", async move {
        let input = ChannelInputSource::new("panic-midstream", SourceId(1), lines)
            .with_trace_recorder(trace.clone());

        let mut pipeline = Pipeline::for_simulation_with_factory("sim", factory, 1);
        pipeline.set_batch_target_bytes(128);
        pipeline.set_batch_timeout(Duration::from_millis(20));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(20));
        let mut pipeline = pipeline
            .with_input("panic-midstream", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            sd.cancel();
        });

        trace.record(TraceEvent::Phase {
            phase: TracePhase::Running,
        });
        pipeline.run_async(&shutdown).await.unwrap();
        // Under the terminal-hold model, the pipeline shuts down when the
        // panic ack is received. Emit Draining + Stopped in sequence to
        // reflect the actual drain path that run_async executes internally.
        trace.record(TraceEvent::Phase {
            phase: TracePhase::Draining,
        });
        trace.record(TraceEvent::Phase {
            phase: TracePhase::Stopped,
        });
        Ok(())
    });

    sim.run().unwrap();

    assert!(
        call_counter.load(Ordering::Relaxed) >= 2,
        "expected at least success + panic send attempts"
    );
    assert!(
        delivered_counter.load(Ordering::Relaxed) > 0,
        "expected initial successful delivery before panic"
    );
    if let Some(durable) = handle.durable_offset(1) {
        assert!(
            durable < input_total_bytes,
            "held panic gap must keep durable checkpoint behind full input: \
             durable={durable}, input_total_bytes={input_total_bytes}"
        );
    }

    let events = load_trace(&trace_path).expect("load trace file");
    assert!(
        events.iter().any(|event| matches!(
            event,
            TraceEvent::BatchTerminal {
                outcome: TraceOutcome::InternalFailure,
                disposition: TraceDisposition::Hold,
                ..
            }
        )),
        "panic path must produce internal_failure/hold trace evidence"
    );
    assert!(
        matches!(
            events.last(),
            Some(TraceEvent::Phase {
                phase: TracePhase::Stopped
            })
        ),
        "Stopped must be the final trace event"
    );

    let validator = TransitionValidator::default();
    validator
        .validate(&events)
        .expect("panic/hold trace should satisfy shutdown and gap invariants");
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
        err.contains("invalid phase transition"),
        "unexpected validator error: {err}"
    );
}

#[test]
fn trace_validator_accepts_phase_less_runtime_event_streams() {
    let events = vec![
        TraceEvent::BatchBegin {
            batch_id: 0,
            source_id: 1,
            checkpoint: 10,
        },
        TraceEvent::BatchTerminal {
            batch_id: Some(0),
            source_id: Some(1),
            checkpoint: Some(10),
            outcome: TraceOutcome::Delivered,
            disposition: TraceDisposition::Ack,
            rows: 5,
        },
        TraceEvent::CheckpointUpdate {
            source_id: 1,
            offset: 10,
        },
        TraceEvent::CheckpointFlush { success: true },
        TraceEvent::BatchBegin {
            batch_id: 1,
            source_id: 1,
            checkpoint: 20,
        },
        TraceEvent::BatchTerminal {
            batch_id: Some(1),
            source_id: Some(1),
            checkpoint: Some(20),
            outcome: TraceOutcome::Delivered,
            disposition: TraceDisposition::Ack,
            rows: 5,
        },
        TraceEvent::CheckpointUpdate {
            source_id: 1,
            offset: 20,
        },
    ];

    let validator = TransitionValidator::default();
    validator
        .validate(&events)
        .expect("phase-less streams should validate when checkpoint monotonicity holds");
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
        TraceEvent::BatchTerminal {
            batch_id: None,
            source_id: None,
            checkpoint: None,
            outcome: TraceOutcome::Delivered,
            disposition: TraceDisposition::Ack,
            rows: 1,
        },
    ];

    let validator = TransitionValidator::default();
    let err = validator
        .validate(&events)
        .expect_err("sink activity after stopped must be rejected");
    assert!(
        err.contains("activity after Stopped"),
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
        err.contains("checkpoint flush after stopped") || err.contains("after Stopped"),
        "unexpected validator error: {err}"
    );
}

#[test]
fn trace_validator_rejects_unterminalized_begin_send() {
    let events = vec![TraceEvent::BatchBegin {
        batch_id: 0,
        source_id: 1,
        checkpoint: 100,
    }];

    let validator = TransitionValidator::default();
    let err = validator
        .validate(&events)
        .expect_err("begin_send without terminal/hold evidence must be rejected");
    assert!(
        err.contains("begin_send was not terminalized"),
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
        err.contains("terminal_phase_missing")
            || err.contains("trace ended before stopped transition"),
        "unexpected validator error: {err}"
    );
}

#[test]
fn trace_validator_accepts_explicit_hold_without_checkpoint_advance() {
    let events = vec![
        TraceEvent::BatchBegin {
            batch_id: 0,
            source_id: 1,
            checkpoint: 100,
        },
        TraceEvent::BatchTerminal {
            batch_id: Some(0),
            source_id: Some(1),
            checkpoint: Some(100),
            outcome: TraceOutcome::InternalFailure,
            disposition: TraceDisposition::Hold,
            rows: 10,
        },
        TraceEvent::CheckpointUpdate {
            source_id: 1,
            offset: 99,
        },
    ];

    let validator = TransitionValidator::default();
    validator
        .validate(&events)
        .expect("held gap should validate while checkpoint remains before it");
}

#[test]
fn trace_validator_rejects_checkpoint_advance_past_held_gap() {
    let events = vec![
        TraceEvent::BatchBegin {
            batch_id: 0,
            source_id: 1,
            checkpoint: 100,
        },
        TraceEvent::BatchTerminal {
            batch_id: Some(0),
            source_id: Some(1),
            checkpoint: Some(100),
            outcome: TraceOutcome::InternalFailure,
            disposition: TraceDisposition::Hold,
            rows: 10,
        },
        TraceEvent::BatchBegin {
            batch_id: 1,
            source_id: 1,
            checkpoint: 200,
        },
        TraceEvent::BatchTerminal {
            batch_id: Some(1),
            source_id: Some(1),
            checkpoint: Some(200),
            outcome: TraceOutcome::Delivered,
            disposition: TraceDisposition::Ack,
            rows: 10,
        },
        TraceEvent::CheckpointUpdate {
            source_id: 1,
            offset: 200,
        },
    ];

    let validator = TransitionValidator::default();
    let err = validator
        .validate(&events)
        .expect_err("checkpoint must not advance past an unresolved held gap");
    assert!(
        err.contains("unresolved gap"),
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

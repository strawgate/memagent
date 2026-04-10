//! End-to-end trace validation prototype tests.

use std::sync::atomic::Ordering;
use std::time::Duration;

use logfwd::pipeline::Pipeline;
use logfwd_types::pipeline::SourceId;
use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;

use super::channel_input::ChannelInputSource;
use super::instrumented_sink::InstrumentedSink;
use super::observable_checkpoint::ObservableCheckpointStore;
use super::trace_bridge::{TraceEvent, TracePhase, TraceRecorder, TransitionValidator, load_trace};

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
        .expect("runtime trace should satisfy declared transition contract");
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
            outcome: super::trace_bridge::SinkOutcome::Ok,
            rows: 1,
        },
    ];

    let validator = TransitionValidator::default();
    let err = validator
        .validate(&events)
        .expect_err("sink activity after stopped must be rejected");
    assert!(
        err.contains("after Stopped"),
        "unexpected validator error: {err}"
    );
}

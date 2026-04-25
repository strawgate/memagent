//! Barrier-driven turmoil seam tests.
//!
//! These tests avoid timing sleeps for critical interleavings by suspending
//! execution exactly at runtime seam hooks.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ffwd::pipeline::Pipeline;
use ffwd_runtime::turmoil_barriers::RuntimeBarrierEvent;
use ffwd_test_utils::sinks::CountingSink;
use ffwd_types::pipeline::SourceId;
use futures_util::FutureExt;
use tokio_util::sync::CancellationToken;
use turmoil::barriers::{Barrier, Reaction};

use super::channel_input::ChannelInputSource;
use super::observable_checkpoint::ObservableCheckpointStore;

fn generate_json_lines(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("{{\"msg\":\"line {i}\",\"num\":{i}}}\n").into_bytes())
        .collect()
}

#[test]
fn barrier_before_worker_ack_send_suspends_checkpoint_advance() {
    let mut sim = super::build_sim(120, 1);
    let mut barrier = Barrier::build(Reaction::Suspend, |event: &RuntimeBarrierEvent| {
        matches!(event, RuntimeBarrierEvent::BeforeWorkerAckSend { .. })
    });

    let delivered = Arc::new(AtomicU64::new(0));
    let delivered_clone = Arc::clone(&delivered);
    let (store, handle) = ObservableCheckpointStore::new();

    sim.client("pipeline", async move {
        let lines = generate_json_lines(20);
        let input = ChannelInputSource::new("test", SourceId(1), lines);
        let sink = CountingSink::new(delivered_clone);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(20));
        pipeline.set_batch_target_bytes(1024 * 1024);
        pipeline.set_checkpoint_flush_interval(Duration::from_secs(60));
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    let mut wait = Box::pin(barrier.wait());
    let triggered = loop {
        let finished = sim.step().expect("simulation step should succeed");
        if let Some(triggered) = wait.as_mut().now_or_never().flatten() {
            break triggered;
        }
        assert!(
            !finished,
            "simulation finished before before-ack barrier triggered"
        );
    };

    assert_eq!(
        handle.update_count(1),
        0,
        "checkpoint must not advance while ack-send seam is suspended"
    );
    for _ in 0..100 {
        sim.step().expect("simulation step should succeed");
    }
    assert_eq!(
        handle.update_count(1),
        0,
        "checkpoint must remain stalled while barrier is held"
    );

    drop(triggered);
    sim.run()
        .expect("simulation should complete after barrier release");

    assert!(
        handle.update_count(1) > 0,
        "checkpoint should advance after ack-send barrier is released"
    );
    assert_eq!(
        delivered.load(Ordering::Relaxed),
        20,
        "all rows should be delivered after releasing the seam barrier"
    );
}

#[test]
fn barrier_before_checkpoint_flush_suspends_durability_commit() {
    let mut sim = super::build_sim(120, 1);
    let mut barrier = Barrier::build(Reaction::Suspend, |event: &RuntimeBarrierEvent| {
        matches!(
            event,
            RuntimeBarrierEvent::BeforeCheckpointFlushAttempt { attempt: 0 }
        )
    });

    let delivered = Arc::new(AtomicU64::new(0));
    let delivered_clone = Arc::clone(&delivered);
    let (store, handle) = ObservableCheckpointStore::new();

    sim.client("pipeline", async move {
        let lines = generate_json_lines(16);
        let input = ChannelInputSource::new("test", SourceId(1), lines);
        let sink = CountingSink::new(delivered_clone);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(10));
        pipeline.set_batch_target_bytes(1024 * 1024);
        pipeline.set_checkpoint_flush_interval(Duration::from_secs(60));
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(250)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    let mut wait = Box::pin(barrier.wait());
    let triggered = loop {
        let finished = sim.step().expect("simulation step should succeed");
        if let Some(triggered) = wait.as_mut().now_or_never().flatten() {
            break triggered;
        }
        assert!(
            !finished,
            "simulation finished before before-flush barrier triggered"
        );
    };

    assert_eq!(
        handle.durable_offset(1),
        None,
        "durable checkpoint must remain unset while flush seam is suspended"
    );
    for _ in 0..50 {
        sim.step().expect("simulation step should succeed");
    }
    assert_eq!(
        handle.durable_offset(1),
        None,
        "durability state should remain frozen until barrier release"
    );

    drop(triggered);
    sim.run()
        .expect("simulation should complete after barrier release");
    assert_eq!(
        delivered.load(Ordering::Relaxed),
        16,
        "rows should still deliver while durability commit is suspended"
    );
    assert!(
        handle.durable_offset(1).is_some(),
        "durable checkpoint should be persisted after barrier release"
    );
}

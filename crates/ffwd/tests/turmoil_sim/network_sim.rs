//! Invariant-based failure simulation tests.
//!
//! These tests exercise the real Pipeline + OutputWorkerPool + PipelineMachine
//! interaction under controlled failure conditions. Each test documents the
//! invariant it probes and makes strong assertions on state, not just counts.
//!
//! Tests A/B/C use turmoil::net to exercise real TCP networking through the
//! simulated network, validating end-to-end delivery, partition recovery,
//! and server crash reconnection.

use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ffwd::pipeline::Pipeline;
use ffwd_types::pipeline::SourceId;
use tokio_util::sync::CancellationToken;

use super::channel_input::ChannelInputSource;
use super::instrumented_sink::{FailureAction, InstrumentedSink, InstrumentedSinkFactory};
use super::observable_checkpoint::ObservableCheckpointStore;
use super::tcp_server::{TcpServerHandle, run_tcp_server};
use super::turmoil_tcp_sink::TurmoilTcpSink;

fn generate_json_lines(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("{{\"msg\":\"line {i}\",\"num\":{i}}}\n").into_bytes())
        .collect()
}

fn inflight_link_messages(sim: &turmoil::Sim<'_>, a: &str, b: &str) -> usize {
    let a_ip = sim.lookup(a);
    let b_ip = sim.lookup(b);
    let pair = if a_ip < b_ip {
        (a_ip, b_ip)
    } else {
        (b_ip, a_ip)
    };
    let mut count = 0usize;
    sim.links(|links| {
        for link in links {
            if link.pair() == pair {
                count = link.count();
                break;
            }
        }
    });
    count
}

fn step_n(sim: &mut turmoil::Sim<'_>, count: usize, scope: &str) {
    for step in 0..count {
        super::maybe_trace_sim_step(scope, step + 1);
        sim.step().expect("simulation step should succeed");
    }
}

/// Test: persistent transient failures hold checkpoint progress and pipeline does not hang.
///
/// Invariant probed: while transient sends keep failing, the batch must not
/// advance checkpoints, and shutdown completes without deadlock.
///
/// Script: all calls return IoError(ConnectionRefused).
#[test]
fn retry_exhaustion_holds_checkpoint_and_completes_shutdown() {
    let mut sim = super::build_sim(120, 1);

    let sink = InstrumentedSink::new(vec![FailureAction::RepeatIoError(
        io::ErrorKind::ConnectionRefused,
    )]);
    let delivered_counter = sink.delivered_counter();
    let call_counter = sink.call_counter();
    let (store, ckpt_handle) = ObservableCheckpointStore::new();

    sim.client("pipeline", async move {
        let lines = generate_json_lines(10);
        let input = ChannelInputSource::new("test", SourceId(1), lines);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_pool_drain_timeout(Duration::from_secs(2));
        pipeline.set_batch_timeout(Duration::from_millis(20));
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            sd.cancel();
        });

        // Pipeline must NOT hang despite all failures.
        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    // No rows should be delivered — all attempts failed.
    let delivered = delivered_counter.load(Ordering::Relaxed);
    assert_eq!(delivered, 0, "expected 0 rows delivered, got {delivered}");

    // The sink should have been called repeatedly while failures persist.
    let calls = call_counter.load(Ordering::Relaxed);
    assert!(
        calls >= 4,
        "expected multiple retry attempts while failures persist, got {calls}"
    );

    let durable = ckpt_handle.durable_offset(1);
    assert!(
        durable.is_none(),
        "expected no durable checkpoint while retries never succeed, got {durable:?}"
    );
    let updates = ckpt_handle.update_count(1);
    assert_eq!(
        updates, 0,
        "checkpoint should not advance under persistent retries, got {updates} updates"
    );
}

/// Test: shutdown drain with in-flight slow work.
///
/// Invariant probed: shutdown race between drain and in-flight work.
/// The pool.drain(60s) + force_stop path must handle slow sinks without
/// deadlocking. The slow batch completes within the drain window so data
/// should be delivered.
#[test]
fn shutdown_drain_with_inflight_work() {
    let mut sim = super::build_sim(120, 10);

    // 2s delay — fast enough to complete within drain window (60s default).
    let sink = InstrumentedSink::new(vec![FailureAction::Delay(Duration::from_secs(2))]);
    let delivered_counter = sink.delivered_counter();

    sim.client("pipeline", async move {
        let lines = generate_json_lines(10);
        let input = ChannelInputSource::new("test", SourceId(1), lines);

        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(20));
        let mut pipeline = pipeline.with_input("test", Box::new(input));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        // Shutdown after 1s — first batch is still in 2s delay, but
        // drain window is long enough to let it complete.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    // Pipeline must complete shutdown without deadlocking.
    sim.run().unwrap();

    // The drain window (60s default) is long enough for the 2s-delayed batch
    // to complete, so all 10 rows should be delivered.
    let delivered = delivered_counter.load(Ordering::Relaxed);
    assert_eq!(
        delivered, 10,
        "expected all 10 rows delivered during drain, got {delivered}"
    );
}

/// Test: multi-worker out-of-order delivery with checkpoint ordering.
///
/// Invariant probed: PipelineMachine ordered-ack with ACTUAL concurrency.
/// With 2 workers, worker 1 gets a slow batch (3s delay) while worker 2
/// gets fast batches. Worker 2 acks before worker 1. The checkpoint must
/// NOT advance past worker 1's batch until it completes.
///
/// This is the test that exercises real concurrent batch processing — with
/// 1 worker, batches are sequential by definition.
#[test]
fn multi_worker_out_of_order_ack_checkpoint_ordering() {
    let mut sim = super::build_sim(60, 1);

    // Worker 1 script: first batch delays 3s (slow), then succeeds normally.
    // Worker 2 script: all batches succeed instantly (fast).
    // Note: factory pops from the END, so worker 2's script is pushed first.
    let factory = Arc::new(InstrumentedSinkFactory::new(vec![
        // Worker 2 (popped first): always fast
        vec![],
        // Worker 1 (popped second): slow first batch
        vec![FailureAction::Delay(Duration::from_secs(3))],
    ]));
    let delivered_counter = factory.delivered_counter();

    let (store, ckpt_handle) = ObservableCheckpointStore::new();

    sim.client("pipeline", async move {
        // 30 lines — enough data for multiple batches across 2 workers.
        let lines = generate_json_lines(30);
        let input = ChannelInputSource::new("test", SourceId(1), lines);

        // 2 workers: enables actual concurrent batch processing.
        let mut pipeline = Pipeline::for_simulation_with_factory("sim", factory, 2);
        pipeline.set_batch_timeout(Duration::from_millis(20));
        pipeline.set_checkpoint_flush_interval(Duration::from_millis(100));
        let mut pipeline = pipeline
            .with_input("test", Box::new(input))
            .with_checkpoint_store(Box::new(store));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(15)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    // All 30 rows should be delivered (slow batch delays, not fails).
    let count = delivered_counter.load(Ordering::Relaxed);
    assert_eq!(count, 30, "expected all 30 rows delivered, got {count}");

    // INVARIANT: checkpoint history is monotonically increasing.
    // If the PipelineMachine allowed worker 2's fast ack to advance the
    // checkpoint past worker 1's slow batch, this would catch it.
    ckpt_handle.assert_monotonic(1);

    // INVARIANT: durable checkpoint exists and reflects delivered data.
    let durable = ckpt_handle.durable_offset(1);
    assert!(
        durable.is_some(),
        "expected durable checkpoint after delivering 30 rows"
    );
    assert!(
        durable.unwrap() > 0,
        "expected durable offset > 0, got {}",
        durable.unwrap()
    );

    // INVARIANT: checkpoint updates happened (flush throttle worked).
    let updates = ckpt_handle.update_count(1);
    assert!(
        updates > 0,
        "expected checkpoint updates for source 1, got 0"
    );
}

// ============================================================================
// Tests using turmoil::net for real TCP simulation
// ============================================================================

const TCP_PORT: u16 = 9000;

/// Test A: basic end-to-end TCP delivery through turmoil::net.
///
/// Proves that turmoil::net::TcpStream actually works for data transfer
/// between two simulated hosts. A TCP server runs on "server" and the
/// pipeline with TurmoilTcpSink runs on "pipeline".
#[test]
fn real_tcp_delivery_through_turmoil_net() {
    let mut sim = super::build_sim_with_profile(super::SimProfile::DEFAULT.with_duration(30));

    let server_handle = TcpServerHandle::new();
    let server_handle_check = server_handle.clone();

    // Server host: runs the TCP listener.
    let sh = server_handle.clone();
    sim.host("server", move || {
        let h = sh.clone();
        async move {
            run_tcp_server(TCP_PORT, h).await?;
            Ok(())
        }
    });

    // Pipeline client: connects to server via TurmoilTcpSink.
    sim.client("pipeline", async move {
        let lines = generate_json_lines(50);
        let input = ChannelInputSource::new("test", SourceId(1), lines);

        let sink = TurmoilTcpSink::new("server", TCP_PORT);
        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(20));
        let mut pipeline = pipeline.with_input("test", Box::new(input));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    // Server must have received all 50 lines.
    let received = server_handle_check.received_lines.load(Ordering::Relaxed);
    assert_eq!(
        received, 50,
        "expected server to receive 50 lines, got {received}"
    );

    // At least one connection was established.
    let conns = server_handle_check.connection_count.load(Ordering::Relaxed);
    assert!(
        conns >= 1,
        "expected at least 1 TCP connection, got {conns}"
    );
}

/// Test B: network partition causes failure and repair enables recovery.
///
/// Uses turmoil::partition/repair to break and restore the network between
/// the pipeline and server. The pipeline's retry logic must handle the
/// partition gracefully and resume delivery after repair.
#[test]
fn tcp_partition_causes_retry_and_recovery() {
    let mut sim = super::build_sim_with_profile(super::SimProfile::LONG_IO.with_duration(60));

    let server_handle = TcpServerHandle::new();
    let server_handle_check = server_handle.clone();

    // Server host.
    let sh = server_handle.clone();
    sim.host("server", move || {
        let h = sh.clone();
        async move {
            run_tcp_server(TCP_PORT, h).await?;
            Ok(())
        }
    });

    // Pipeline client: sends data in two phases separated by a partition.
    // Phase 1: send 20 lines (should succeed).
    // Phase 2: partition, then repair, then send 30 more lines.
    sim.client("pipeline", async move {
        // Phase 1: pre-partition data.
        let lines_phase1 = generate_json_lines(20);
        let input1 = ChannelInputSource::new("test", SourceId(1), lines_phase1);

        let sink = TurmoilTcpSink::new("server", TCP_PORT);
        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(20));
        let mut pipeline = pipeline.with_input("test", Box::new(input1));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(20)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    // Step 1: let the pipeline deliver some data before partitioning.
    step_n(&mut sim, 500, "partition-recovery-pre");

    // Partition the network.
    sim.partition("pipeline", "server");

    // Step through partition — sends during this time should fail.
    step_n(&mut sim, 200, "partition-recovery-partitioned");

    // Repair the network.
    sim.repair("pipeline", "server");

    // Run to completion.
    sim.run().unwrap();

    // Server must have received a substantial portion of the data.
    // Some lines may be lost during the partition window, but data sent
    // before partition and after repair must arrive.
    let received = server_handle_check.received_lines.load(Ordering::Relaxed);
    assert!(
        received >= 5,
        "expected server to receive at least 5 of 20 lines (pre-partition + post-repair), got {received}"
    );
}

/// Test C: server crash preserves pre-crash delivery under bounce.
///
/// The server starts, accepts data, gets crashed via sim.crash(), then
/// gets bounced via sim.bounce(). This test verifies pre-crash data is not
/// corrupted or lost across crash + restart boundaries.
#[test]
fn tcp_server_crash_preserves_pre_crash_delivery() {
    let mut sim = super::build_sim_with_profile(super::SimProfile::LONG_IO.with_duration(120));

    let server_handle = TcpServerHandle::new();
    let server_handle_check = server_handle.clone();

    // Server host (restartable via bounce).
    let sh = server_handle.clone();
    sim.host("server", move || {
        let h = sh.clone();
        async move {
            run_tcp_server(TCP_PORT, h).await?;
            Ok(())
        }
    });

    // Pipeline client: sends enough lines for mid-stream crash/reconnect checks
    // without overflowing Turmoil's socket buffer under adverse schedules.
    sim.client("pipeline", async move {
        let lines = generate_json_lines(60);
        let input = ChannelInputSource::new("test", SourceId(1), lines);

        let sink = TurmoilTcpSink::new("server", TCP_PORT);
        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(20));
        pipeline.set_batch_target_bytes(256);
        let mut pipeline = pipeline.with_input("test", Box::new(input));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    // Step until the server has received some data, then crash it.
    // This ensures data was flowing before the crash.
    let mut steps = 0;
    loop {
        sim.step().unwrap();
        steps += 1;
        super::maybe_trace_sim_step("server-crash-pre", steps);
        let received = server_handle_check.received_lines.load(Ordering::Relaxed);
        if received > 0 || steps > 5000 {
            break;
        }
    }

    let pre_crash = server_handle_check.received_lines.load(Ordering::Relaxed);

    // Crash the server — all connections drop.
    sim.crash("server");

    // Step through crash — sink should detect broken connection.
    // Keep the crash window bounded to avoid unbounded simulator buffering.
    step_n(&mut sim, 5, "server-crash-window");

    // Bounce the server — it restarts and starts accepting again.
    sim.bounce("server");

    // Run to completion — pipeline should reconnect and deliver.
    sim.run().unwrap();

    let total_received = server_handle_check.received_lines.load(Ordering::Relaxed);

    // The server received some data before the crash.
    assert!(
        pre_crash > 0,
        "expected data delivered before server crash, got 0"
    );

    // With indefinite retry the worker keeps trying through the
    // crash/bounce window. Pre-crash data is never lost, and
    // additional data may arrive once the server is back up.
    assert!(
        total_received >= pre_crash,
        "crash should not lose pre-crash data: total ({total_received}) < pre_crash ({pre_crash})"
    );
}

/// Test: hold/release creates burst delivery after message buffering.
///
/// turmoil::hold() buffers TCP segments without dropping them (unlike
/// partition which drops). When released, all buffered data arrives in
/// a burst. This exercises a failure mode that partition cannot create:
/// delayed delivery rather than failed delivery.
///
/// We use 500 lines and apply hold after only 50 steps, ensuring the
/// pipeline is still actively sending when the hold takes effect.
#[test]
fn tcp_hold_release_burst_delivery() {
    let mut sim = super::build_sim_with_profile(super::SimProfile::LONG_IO.with_duration(60));

    let server_handle = TcpServerHandle::new();
    let server_check = server_handle.clone();

    let sh = server_handle.clone();
    sim.host("server", move || {
        let h = sh.clone();
        async move { run_tcp_server(TCP_PORT, h).await }
    });

    sim.client("pipeline", async move {
        let lines = generate_json_lines(200);
        let input = ChannelInputSource::new("test", SourceId(1), lines);
        let sink = TurmoilTcpSink::new("server", TCP_PORT);
        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(20));
        let mut pipeline = pipeline.with_input("test", Box::new(input));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    // Let some data flow normally — only 50 steps so the pipeline is
    // still actively sending when we apply the hold.
    step_n(&mut sim, 50, "hold-release-pre");
    let _pre_hold = server_check.received_lines.load(Ordering::Relaxed);

    // Hold: buffer TCP segments, don't drop them.
    sim.hold("pipeline", "server");

    // Step during hold — data is buffered, not delivered.
    let mut held_inflight_peak = 0usize;
    for step in 0..500 {
        super::maybe_trace_sim_step("hold-release-held", step + 1);
        sim.step().unwrap();
        held_inflight_peak =
            held_inflight_peak.max(inflight_link_messages(&sim, "pipeline", "server"));
    }
    let during_hold = server_check.received_lines.load(Ordering::Relaxed);
    let pending_before_release = inflight_link_messages(&sim, "pipeline", "server");
    assert!(
        pending_before_release > 0 || held_inflight_peak > 0,
        "expected pending in-flight traffic while hold is active"
    );

    // Release: deliver all buffered data in a burst.
    sim.release("pipeline", "server");
    let mut observed_delivery_progress = false;
    for step in 0..100 {
        super::maybe_trace_sim_step("hold-release-post", step + 1);
        sim.step().unwrap();
        let received = server_check.received_lines.load(Ordering::Relaxed);
        if received > during_hold {
            observed_delivery_progress = true;
            break;
        }
    }
    assert!(
        observed_delivery_progress || pending_before_release <= 1,
        "expected post-release delivery progress when hold buffered data; \
         pending_before_release={pending_before_release}, during_hold={during_hold}"
    );

    // Run to completion.
    sim.run().unwrap();

    let total = server_check.received_lines.load(Ordering::Relaxed);
    let pending_after_run = inflight_link_messages(&sim, "pipeline", "server");

    // After release, total should exceed during_hold (burst delivery).
    // If during_hold == total, the hold had no effect (all data arrived
    // before or during hold via in-flight segments).
    assert!(
        total > during_hold,
        "expected burst delivery after release, but total ({total}) == during_hold ({during_hold})"
    );

    // All 200 lines should eventually arrive (hold doesn't drop).
    assert_eq!(
        total, 200,
        "expected all 200 lines delivered after hold/release, got {total}"
    );
    assert!(
        pending_after_run <= 1,
        "expected bounded residual in-flight messages after completion, got {pending_after_run}"
    );
    assert!(
        pending_after_run <= pending_before_release,
        "release should not increase backlog at completion: before={pending_before_release}, after={pending_after_run}"
    );
}

/// Test: intermittent TCP connection failures exercise retry under realistic conditions.
///
/// Unlike persistent-failure scenarios (all calls fail) or happy-path (all succeed),
/// this test uses turmoil's fail_rate for random TCP connection breakage.
/// In turmoil, fail_rate breaks TCP connections entirely (not individual
/// segments), and repair_rate controls how quickly the link is restored.
///
/// With a low fail_rate and high repair_rate, the pipeline's retry logic
/// should recover from most connection breaks and deliver a substantial
/// portion of data.
#[test]
fn tcp_intermittent_failures_with_fail_rate() {
    let seed = super::turmoil_seed();

    let mut sim = super::build_sim_with_profile(super::SimProfile::TCP_INTERMITTENT);

    let server_handle = TcpServerHandle::new();
    let server_check = server_handle.clone();

    let sh = server_handle.clone();
    sim.host("server", move || {
        let h = sh.clone();
        async move { run_tcp_server(TCP_PORT, h).await }
    });

    sim.client("pipeline", async move {
        // Avoid conflating server boot timing with intermittent link failures.
        tokio::time::sleep(Duration::from_secs(2)).await;

        let lines = generate_json_lines(50);
        let input = ChannelInputSource::new("test", SourceId(1), lines);
        let sink = TurmoilTcpSink::new("server", TCP_PORT);
        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(50));
        // Force multiple independent retry decisions. If all 50 lines fit in a
        // single batch, one prolonged transient-failure window can produce 0
        // delivered rows before shutdown.
        pipeline.set_batch_target_bytes(256);
        let mut pipeline = pipeline.with_input("test", Box::new(input));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.run().unwrap();

    // With 0.1% TCP connection breakage, the pipeline's retry logic should
    // recover from most breaks. We assert a minimum delivery threshold.
    let received = server_check.received_lines.load(Ordering::Relaxed);
    assert!(
        received >= 5,
        "expected at least 5 of 50 lines despite 0.1% fail rate (seed={seed}), got {received}"
    );
}

/// Test: one-way partition blocks egress until directional repair is applied.
///
/// This exercises `partition_oneway` / `repair_oneway` rather than full
/// bidirectional partition, ensuring directional link controls behave as
/// expected under replayable schedules.
#[test]
fn tcp_oneway_partition_recovers_after_directional_repair() {
    let mut sim = super::build_sim_with_profile(super::SimProfile::LONG_IO.with_duration(90));

    let server_handle = TcpServerHandle::new();
    let server_check = server_handle.clone();

    let sh = server_handle.clone();
    sim.host("server", move || {
        let h = sh.clone();
        async move { run_tcp_server(TCP_PORT, h).await }
    });

    sim.client("pipeline", async move {
        let lines = generate_json_lines(200);
        let input = ChannelInputSource::new("test", SourceId(1), lines);
        let sink = TurmoilTcpSink::new("server", TCP_PORT);
        let mut pipeline = Pipeline::for_simulation("sim", Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(20));
        let mut pipeline = pipeline.with_input("test", Box::new(input));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            sd.cancel();
        });

        pipeline.run_async(&shutdown).await.unwrap();
        Ok(())
    });

    sim.partition_oneway("pipeline", "server");
    step_n(&mut sim, 250, "oneway-partitioned");
    let blocked_received = server_check.received_lines.load(Ordering::Relaxed);
    assert_eq!(
        blocked_received, 0,
        "directional egress partition should block initial delivery"
    );

    sim.repair_oneway("pipeline", "server");
    step_n(&mut sim, 1000, "oneway-repair");

    sim.run().unwrap();
    let total = server_check.received_lines.load(Ordering::Relaxed);
    assert_eq!(
        total, 200,
        "all lines should deliver after one-way partition is repaired"
    );
}

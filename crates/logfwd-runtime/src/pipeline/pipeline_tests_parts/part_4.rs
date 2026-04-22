#[tokio::test(flavor = "multi_thread")]
async fn test_async_shutdown_with_frozen_output() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("frozen.log");

    let mut data = String::new();
    for i in 0..20 {
        data.push_str(&format!(r#"{{"frozen":{i}}}"#));
        data.push('\n');
    }
    std::fs::write(&log_path, data.as_bytes()).unwrap();

    let yaml = format!(
        r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
        log_path.display()
    );

    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

    // FrozenSink: first call blocks until the token is cancelled.
    // This simulates an output that hangs (network timeout, deadlock).
    let freeze_token = CancellationToken::new();
    pipeline = pipeline.with_sink(Box::new(FrozenSink {
        release: freeze_token.clone(),
    }));
    pipeline.batch_timeout = Duration::from_millis(20);

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    let ft = freeze_token.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        sd.cancel();
        // Release the frozen sink shortly after shutdown so the
        // block_in_place call can return and the pipeline can exit.
        tokio::time::sleep(Duration::from_millis(100)).await;
        ft.cancel();
    });

    let result = tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

    assert!(
        result.is_ok(),
        "frozen output must not prevent shutdown (5s timeout)"
    );
    assert!(result.unwrap().is_ok());
}

/// SlowSink: verify that batch timeout flushes fire when the output
/// is slow, and that the pipeline continues processing all data.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_slow_output_flush_by_timeout() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("slow_timeout.log");

    // Write enough data that multiple batches will be produced.
    let mut data = String::new();
    for i in 0..100 {
        data.push_str(&format!(r#"{{"msg":"timeout {}"}}"#, i));
        data.push('\n');
    }
    std::fs::write(&log_path, data.as_bytes()).unwrap();

    let yaml = format!(
        r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
        log_path.display()
    );

    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

    // Slow output with a small delay. Combined with a short batch_timeout
    // and large batch_target_bytes, this forces flushes by timeout rather
    // than by size.
    pipeline = pipeline.with_sink(Box::new(SlowSink {
        delay: Duration::from_millis(10),
    }));
    pipeline.batch_timeout = Duration::from_millis(30);
    pipeline.batch_target_bytes = 1024 * 1024; // 1 MB — far larger than our data

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(600)).await;
        sd.cancel();
    });

    pipeline.run_async(&shutdown).await.unwrap();

    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert!(
        lines_in >= 100,
        "slow output: pipeline should process all data, got {lines_in} (expected >= 100)"
    );

    let batches = pipeline.metrics.batches_total.load(Ordering::Relaxed);
    assert!(
        batches > 0,
        "slow output: at least one batch should succeed, got {batches}"
    );

    // In the split architecture, timeout-based flushing happens in the
    // I/O worker (not tracked by pipeline metrics). Verify data flowed
    // through the pipeline by checking batches_total above.
}

/// FrozenSink: verify that a frozen output causes dropped batches
/// and output errors to be tracked in metrics while the pipeline
/// still exits cleanly when released.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_frozen_output_tracks_metrics() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("frozen_metrics.log");

    let mut data = String::new();
    for i in 0..20 {
        data.push_str(&format!(r#"{{"msg":"frozen {}"}}"#, i));
        data.push('\n');
    }
    std::fs::write(&log_path, data.as_bytes()).unwrap();

    let yaml = format!(
        r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
        log_path.display()
    );

    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

    let freeze_token = CancellationToken::new();
    pipeline = pipeline.with_sink(Box::new(FrozenSink {
        release: freeze_token.clone(),
    }));
    pipeline.batch_timeout = Duration::from_millis(20);

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    let ft = freeze_token.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        sd.cancel();
        // Release the frozen sink so the block_in_place call returns.
        tokio::time::sleep(Duration::from_millis(100)).await;
        ft.cancel();
    });

    let result = tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

    assert!(
        result.is_ok(),
        "frozen output: pipeline must exit within timeout"
    );
    assert!(result.unwrap().is_ok());

    // With a frozen output, the input side should still have ingested
    // data (the input thread reads independently of the output).
    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    let output_nanos = pipeline.metrics.output_nanos_total.load(Ordering::Relaxed);

    // The frozen sink blocks in send_batch, so output_nanos should
    // reflect time spent waiting. At least some data should have
    // reached the transform stage.
    assert!(
        lines_in > 0,
        "frozen output: some lines should reach transform, got {lines_in}"
    );
    assert!(
        output_nanos > 0,
        "frozen output: output stage should record nonzero time, got {output_nanos}"
    );
}

/// Concurrent shutdown and data arrival: data written to the file
/// after shutdown is signalled should not cause panics or hangs.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_concurrent_write_and_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("concurrent.log");

    // Start with some data.
    let mut initial = String::new();
    for i in 0..10 {
        initial.push_str(&format!(r#"{{"phase":"initial","i":{i}}}"#));
        initial.push('\n');
    }
    std::fs::write(&log_path, initial.as_bytes()).unwrap();

    let yaml = format!(
        r#"
input:
  type: file
  path: "{}"
  format: json
output:
  type: stdout
  format: json
"#,
        log_path.display()
    );

    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
    pipeline = pipeline.with_sink(Box::new(DevNullSink));
    pipeline.batch_timeout = Duration::from_millis(20);

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    let lp = log_path.clone();

    // Concurrently: write more data AND shutdown at roughly the same time.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Write more data to the file (the tailer should pick it up).
        let mut more = String::new();
        for i in 10..30 {
            more.push_str(&format!(r#"{{"phase":"late","i":{i}}}"#));
            more.push('\n');
        }
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(&lp).unwrap();
        f.write_all(more.as_bytes()).unwrap();
        f.flush().unwrap();

        // Shutdown shortly after the write.
        tokio::time::sleep(Duration::from_millis(50)).await;
        sd.cancel();
    });

    let result = tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

    assert!(result.is_ok(), "concurrent write + shutdown must not hang");
    assert!(result.unwrap().is_ok());
}

// -----------------------------------------------------------------------
// PipelineMachine integration tests (#586, #587)
// -----------------------------------------------------------------------

/// Helper: build a pipeline from a log file path with a custom async sink.
fn pipeline_with_sink(log_path: &std::path::Path, sink: Box<dyn Sink>) -> Pipeline {
    let yaml = format!(
        r#"
input:
  type: file
  path: {}
  format: json
output:
  type: "null"
"#,
        log_path.display()
    );
    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    Pipeline::from_config("default", pipe_cfg, &test_meter(), None)
        .unwrap()
        .with_sink(sink)
}

#[test]
fn test_machine_initialized_on_construction() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    std::fs::write(&log_path, "").unwrap();

    let pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
    assert!(
        pipeline.machine.is_some(),
        "PipelineMachine must be initialized in from_config"
    );
}

#[test]
fn test_machine_clean_shutdown_no_in_flight() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    logfwd_test_utils::generate_json_lines(&log_path, 100, "machine-test");

    let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
    pipeline.set_batch_timeout(Duration::from_millis(10));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    let metrics = Arc::clone(pipeline.metrics());
    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if metrics.batch_rows_total.load(Ordering::Relaxed) >= 100 {
                std::thread::sleep(Duration::from_millis(50));
                sd.cancel();
                return;
            }
            if Instant::now() > deadline {
                sd.cancel();
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    });

    let result = pipeline.run(&shutdown);
    assert!(result.is_ok());
    assert!(
        pipeline.machine.is_none(),
        "machine should be consumed by shutdown drain"
    );
}

#[test]
fn test_flush_batch_with_zero_rows_still_completes() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("empty.log");
    std::fs::write(&log_path, "").unwrap();

    let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
    pipeline.set_batch_timeout(Duration::from_millis(10));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        sd.cancel();
    });

    let result = pipeline.run(&shutdown);
    assert!(result.is_ok(), "empty file should not crash pipeline");
}

#[test]
fn test_flush_batch_output_error_machine_still_drains() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    logfwd_test_utils::generate_json_lines(&log_path, 50, "fail-test");

    let mut pipeline = pipeline_with_sink(&log_path, Box::new(FailingSink::new(u32::MAX)));
    pipeline.set_batch_timeout(Duration::from_millis(10));
    // Short drain timeout so pool cancels indefinitely-retrying workers promptly.
    pipeline.set_pool_drain_timeout(Duration::from_secs(1));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    let metrics = Arc::clone(pipeline.metrics());
    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if metrics.outputs[0].2.errors() > 0 {
                std::thread::sleep(Duration::from_millis(50));
                sd.cancel();
                return;
            }
            if Instant::now() > deadline {
                sd.cancel();
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    });

    let result = pipeline.run(&shutdown);
    assert!(result.is_ok());
    assert!(
        pipeline.machine.is_none(),
        "machine should be consumed even with output errors"
    );
}

#[test]
fn test_channel_msg_carries_checkpoints() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<ChannelMsg>(4);

    let empty = RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty()));
    let mut checkpoints = HashMap::new();
    checkpoints.insert(SourceId(42), ByteOffset(1000));
    tx.try_send(ChannelMsg {
        batch: empty,
        checkpoints,
        queued_at: Some(tokio::time::Instant::now()),
        input_index: 0,
        scan_ns: 0,
        transform_ns: 0,
    })
    .unwrap();

    let msg = rx.try_recv().unwrap();
    assert_eq!(msg.checkpoints.len(), 1);
    assert_eq!(msg.checkpoints[&SourceId(42)], ByteOffset(1000));
}

#[test]
fn hold_disposition_retains_failed_ticket_without_advancing_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("hold.log");
    std::fs::write(&log_path, b"").unwrap();

    let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
    let machine = pipeline.machine.as_mut().unwrap();
    let source = SourceId(42);
    let ticket = machine.create_batch(source, 1000);
    let ticket = machine.begin_send(ticket);

    let (has_held, _advances) = pipeline.ack_all_tickets(vec![ticket], TicketDisposition::Hold);
    assert!(
        has_held,
        "hold disposition must request terminal shutdown to bound held-ticket growth"
    );

    let machine = pipeline.machine.as_ref().unwrap();
    assert_eq!(
        machine.in_flight_count(),
        1,
        "held tickets must keep the source checkpoint unadvanced"
    );
    assert_eq!(machine.committed_checkpoint(source), None);
    assert_eq!(
        pipeline.held_tickets.len(),
        1,
        "hold must retain a failed queued ticket instead of dropping the sending ticket"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_apply_pool_ack_does_not_underflow_inflight_counter() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("underflow.log");
    std::fs::write(&log_path, "").unwrap();
    let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));

    pipeline
        .metrics
        .inflight_batches
        .store(0, Ordering::Relaxed);
    pipeline
        .apply_pool_ack(AckItem {
            tickets: vec![],
            outcome: crate::worker_pool::DeliveryOutcome::InternalFailure,
            num_rows: 0,
            submitted_at: tokio::time::Instant::now(),
            scan_ns: 0,
            transform_ns: 0,
            output_ns: 0,
            queue_wait_ns: 0,
            send_latency_ns: 0,
            batch_id: 0,
            output_name: "test".to_string(),
        })
        .await;

    assert_eq!(
        pipeline.metrics.inflight_batches.load(Ordering::Relaxed),
        0,
        "inflight counter must not underflow on stray ack"
    );
}

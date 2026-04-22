#[tokio::test(flavor = "multi_thread")]
async fn held_ticket_shutdown_does_not_deadlock_on_full_channel() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("held_ticket_full_channel.log");

    let mut data = String::new();
    for i in 0..200 {
        data.push_str(&format!(r#"{{"i":{i}}}"#));
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
    // A panic in the sink is converted into InternalFailure by the worker,
    // which maps to Hold and triggers terminal shutdown.
    pipeline = pipeline.with_sink(Box::new(PanicSink));
    pipeline.batch_target_bytes = 64;
    pipeline.batch_timeout = Duration::from_millis(10);
    pipeline.set_pool_drain_timeout(Duration::from_secs(1));

    let shutdown = CancellationToken::new();
    let result = tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

    assert!(
        result.is_ok(),
        "held-ticket shutdown should close the channel and not deadlock"
    );
    assert!(
        result.unwrap().is_ok(),
        "pipeline should complete terminal held-ticket shutdown cleanly"
    );
}

/// Shutdown with a slow output must still drain all buffered data.
/// The output takes 50ms per batch, but shutdown should wait for
/// the drain to complete rather than dropping data.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_shutdown_drains_with_slow_output() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("slow_output.log");

    let mut data = String::new();
    for i in 0..50 {
        data.push_str(&format!(r#"{{"msg":"slow {}"}}"#, i));
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
    pipeline = pipeline.with_sink(Box::new(SlowSink {
        delay: Duration::from_millis(20),
    }));
    pipeline.batch_timeout = Duration::from_millis(30);

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        sd.cancel();
    });

    pipeline.run_async(&shutdown).await.unwrap();

    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert!(
        lines_in >= 50,
        "slow output: all data should drain on shutdown, got {lines_in} (expected >= 50)"
    );
}

/// After all file data is read, shutdown must still drain everything.
/// The file tailer keeps polling after EOF (it's a tailer, not a
/// reader), so the pipeline only exits via shutdown signal.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_pipeline_processes_all_data_before_shutdown() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("eof_test.log");

    let mut data = String::new();
    for i in 0..30 {
        data.push_str(&format!(r#"{{"eof":{i}}}"#));
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
    pipeline = pipeline.with_sink(Box::new(DevNullSink));
    pipeline.batch_timeout = Duration::from_millis(20);

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    // Wait for data to be processed, then shut down.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        sd.cancel();
    });

    pipeline.run_async(&shutdown).await.unwrap();

    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert!(
        lines_in >= 30,
        "all file data should be processed before shutdown, got {lines_in} (expected >= 30)"
    );
}

/// Shutdown with zero data should complete instantly without errors.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_shutdown_empty_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("empty.log");
    std::fs::write(&log_path, b"").unwrap();

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

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        sd.cancel();
    });

    let result = tokio::time::timeout(Duration::from_secs(2), pipeline.run_async(&shutdown)).await;

    assert!(result.is_ok(), "empty pipeline should shut down cleanly");
    assert!(result.unwrap().is_ok());
}

/// Immediate shutdown (cancel before run_async) must not panic or hang.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_immediate_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("immediate.log");
    std::fs::write(&log_path, b"{\"a\":1}\n").unwrap();

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

    let shutdown = CancellationToken::new();
    // Cancel immediately, before even starting.
    shutdown.cancel();

    let result = tokio::time::timeout(Duration::from_secs(2), pipeline.run_async(&shutdown)).await;

    assert!(result.is_ok(), "immediate shutdown must not hang");
    assert!(result.unwrap().is_ok(), "immediate shutdown must not error");
}

/// Output errors must not crash the pipeline. Retry/control-plane
/// failures are surfaced as output errors without advancing checkpoints.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_output_errors_do_not_crash() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("output_err.log");

    let mut data = String::new();
    for i in 0..100 {
        data.push_str(&format!(r#"{{"err":{i}}}"#));
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
    // FailingSink fails the first N batches, then succeeds.
    pipeline = pipeline.with_sink(Box::new(FailingSink {
        fail_count: 3,
        calls: 0,
    }));
    pipeline.batch_timeout = Duration::from_millis(20);

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        sd.cancel();
    });

    let result = pipeline.run_async(&shutdown).await;
    assert!(result.is_ok(), "output errors should not crash pipeline");

    // Data should still flow through transform even though output
    // failed on some batches.
    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert!(
        lines_in > 0,
        "data should flow through transform despite output errors"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_async_fanout_output_errors_do_not_count_as_permanent_drops() {
    // This test verifies that a persistently-failing output does not
    // crash the pipeline and is not misclassified as a permanent drop.
    //
    // With the async worker pool, retries are built into the pool. An
    // always-failing sink causes the pool to exhaust retries and return a
    // non-delivered AckItem outcome that should HOLD the checkpoint seam.
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("fanout_output_err.log");

    let mut data = String::new();
    for i in 0..100 {
        data.push_str(&format!(r#"{{"err":{i}}}"#));
        data.push('\n');
    }
    std::fs::write(&log_path, data.as_bytes()).unwrap();

    let yaml = format!(
        r#"
pipelines:
  default:
    inputs:
      - type: file
        path: "{}"
        format: json
    outputs:
      - type: stdout
        format: json
        name: always-failing
"#,
        log_path.display()
    );

    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
    // always-failing: fails every call — workers retry indefinitely and the
    // pipeline must hold checkpoint progress instead of rejecting it.
    pipeline = pipeline.with_sink(Box::new(FailingSink::new(u32::MAX)));
    pipeline.batch_timeout = Duration::from_millis(20);
    // Short drain timeout so pool cancels workers promptly on shutdown.
    pipeline.set_pool_drain_timeout(Duration::from_secs(1));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    tokio::spawn(async move {
        // Give workers time to start retrying.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        sd.cancel();
    });

    let result = pipeline.run_async(&shutdown).await;
    assert!(
        result.is_ok(),
        "fanout output errors should not crash pipeline"
    );

    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert!(
        lines_in > 0,
        "data should still reach transform despite output failure"
    );

    let dropped = pipeline
        .metrics
        .dropped_batches_total
        .load(Ordering::Relaxed);
    assert_eq!(
        dropped, 0,
        "retry/control-plane failures should not count as permanent drops"
    );
}

/// Rapid repeated shutdown signals must not cause panics.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_repeated_shutdown_signals() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("repeat_shutdown.log");

    let mut data = String::new();
    for i in 0..50 {
        data.push_str(&format!(r#"{{"rep":{i}}}"#));
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
    pipeline = pipeline.with_sink(Box::new(DevNullSink));
    pipeline.batch_timeout = Duration::from_millis(20);

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    // Cancel many times — CancellationToken should handle this gracefully.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        sd.cancel();
        sd.cancel();
        sd.cancel();
    });

    let result = tokio::time::timeout(Duration::from_secs(2), pipeline.run_async(&shutdown)).await;

    assert!(result.is_ok(), "repeated shutdown must not hang");
    assert!(result.unwrap().is_ok());
}

/// Large batch that exceeds batch_target_bytes on a single line
/// must still be processed and not cause channel issues.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_oversized_single_line() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("big_line.log");

    // Single line with a large value (~10KB).
    let big_value = "x".repeat(10_000);
    let data = format!(r#"{{"big":"{}"}}"#, big_value);
    std::fs::write(&log_path, format!("{data}\n")).unwrap();

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
    pipeline.batch_target_bytes = 1024; // Much smaller than the line
    pipeline.batch_timeout = Duration::from_millis(20);

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        sd.cancel();
    });

    pipeline.run_async(&shutdown).await.unwrap();

    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert!(
        lines_in >= 1,
        "oversized line should still be processed, got {lines_in}"
    );
}

/// A frozen output (blocks indefinitely on send_batch) must not
/// prevent shutdown. The pipeline should still exit because shutdown
/// cancellation breaks the select! loop, and the drain completes
/// when input threads exit.

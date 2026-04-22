#[test]
fn test_pipeline_with_processor() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");

    // Write 4 lines.
    let mut data = String::new();
    for i in 0..4 {
        data.push_str(&format!(r#"{{"level":"INFO","seq":{}}}"#, i));
        data.push('\n');
    }
    std::fs::write(&log_path, data.as_bytes()).unwrap();

    let yaml = format!(
        r"
input:
  type: file
  path: {}
  format: json
output:
  type: stdout
  format: json
",
        log_path.display()
    );
    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

    // Custom processor that drops rows where seq is odd.
    #[derive(Debug)]
    struct DropOddProcessor;
    impl Processor for DropOddProcessor {
        fn process(
            &mut self,
            batch: RecordBatch,
            _meta: &BatchMetadata,
        ) -> Result<smallvec::SmallVec<[RecordBatch; 1]>, crate::processor::ProcessorError>
        {
            // Drop rows where seq is odd
            let seq_array = batch
                .column_by_name("seq")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            let mut indices = vec![];
            for i in 0..seq_array.len() {
                if seq_array.value(i) % 2 == 0 {
                    indices.push(i as u32);
                }
            }
            let indices_array = arrow::array::UInt32Array::from(indices);
            let filtered = arrow::compute::take_record_batch(&batch, &indices_array).unwrap();
            Ok(smallvec::smallvec![filtered])
        }

        fn flush(&mut self) -> smallvec::SmallVec<[RecordBatch; 1]> {
            smallvec::SmallVec::new()
        }

        fn name(&self) -> &'static str {
            "drop_odd"
        }

        fn is_stateful(&self) -> bool {
            false
        }
    }

    pipeline = pipeline.with_processor(Box::new(DropOddProcessor));

    pipeline.batch_timeout = Duration::from_millis(10);
    pipeline.poll_interval = Duration::from_millis(5);

    let shutdown = CancellationToken::new();
    let sd_clone = shutdown.clone();
    let metrics = Arc::clone(&pipeline.metrics);

    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let errors = metrics.transform_errors.load(Ordering::Relaxed);
            let dropped = metrics.dropped_batches_total.load(Ordering::Relaxed);
            if errors > 0 || dropped > 0 || Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        sd_clone.cancel();
    });

    let result = pipeline.run(&shutdown);
    assert!(result.is_ok(), "got: {:?}", result.err());

    // Input reads 4 lines, but transform output drops 2 lines via processor.
    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert_eq!(
        lines_in, 4,
        "expected transform_in to be 4, got {}",
        lines_in
    );

    let lines_out = pipeline
        .metrics
        .transform_out
        .lines_total
        .load(Ordering::Relaxed);
    assert_eq!(
        lines_out, 2,
        "expected transform_out to be 2, got {}",
        lines_out
    );
}

#[test]
fn test_pipeline_run_one_batch() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");

    // Write JSON lines.
    let mut data = String::new();
    for i in 0..10 {
        data.push_str(&format!(
            r#"{{"level":"INFO","msg":"hello {}","status":{}}}"#,
            i,
            200 + i
        ));
        data.push('\n');
    }
    std::fs::write(&log_path, data.as_bytes()).unwrap();

    let yaml = format!(
        r"
input:
  type: file
  path: {}
  format: json
output:
  type: stdout
  format: json
",
        log_path.display()
    );
    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

    // Use a very short batch timeout so the test flushes quickly.
    pipeline.batch_timeout = Duration::from_millis(10);
    pipeline.poll_interval = Duration::from_millis(5);

    let shutdown = CancellationToken::new();
    let sd_clone = shutdown.clone();
    let metrics = Arc::clone(&pipeline.metrics);

    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let errors = metrics.transform_errors.load(Ordering::Relaxed);
            let dropped = metrics.dropped_batches_total.load(Ordering::Relaxed);
            if errors > 0 || dropped > 0 || Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        sd_clone.cancel();
    });

    let result = pipeline.run(&shutdown);
    assert!(result.is_ok(), "got: {:?}", result.err());

    // Verify metrics show data was processed.
    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert!(lines_in > 0, "expected transform_in > 0, got {lines_in}");
}

/// Data buffered in `buf` at the moment the shutdown token fires must
/// not be silently discarded.  The final flush-on-shutdown should process
/// any pending data before the pipeline exits.
#[test]
fn test_pipeline_flush_on_shutdown() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");

    let mut data = String::new();
    for i in 0..5 {
        data.push_str(&format!(r#"{{"level":"INFO","msg":"hello {}"}}"#, i));
        data.push('\n');
    }
    std::fs::write(&log_path, data.as_bytes()).unwrap();

    let yaml = format!(
        r"
input:
  type: file
  path: {}
  format: json
output:
  type: stdout
  format: json
",
        log_path.display()
    );
    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

    // Set batch_timeout large enough that the normal time-based flush will
    // never fire during the test — data stays in buf until shutdown.
    pipeline.batch_timeout = Duration::from_secs(60);
    pipeline.poll_interval = Duration::from_millis(5);

    let shutdown = CancellationToken::new();
    let sd_clone = shutdown.clone();

    // Cancel after enough time for the file to be polled and data to land
    // in buf, but well before the 60-second batch_timeout would fire.
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        sd_clone.cancel();
    });

    let result = pipeline.run(&shutdown);
    assert!(result.is_ok(), "got: {:?}", result.err());

    // The shutdown flush must have forwarded the buffered lines.
    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert!(lines_in > 0, "expected transform_in > 0, got {lines_in}");
}

/// A SQL that references a column that does not exist in the batch should
/// cause the transform to return an error.  The pipeline must log the error,
/// drop that batch, and continue running — it must NOT return `Err`.
#[test]
fn test_pipeline_transform_error_skips_batch_continues() {
    use std::sync::atomic::Ordering;

    // SQL references a column that will never be present. Use generator
    // input so this test only exercises pipeline transform behavior and is
    // not sensitive to file-tail timing under heavy coverage.
    let yaml = r#"
input:
  type: generator
  generator:
    events_per_sec: 10000
    batch_size: 64
    profile: record
    attributes:
      benchmark_id: run-123
      pod_name: emitter-0
      stream_id: emitter-0
transform: "SELECT nonexistent_col FROM logs"
output:
  type: "null"
"#;
    let config = logfwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();

    pipeline.batch_timeout = Duration::from_millis(10);
    pipeline.poll_interval = Duration::from_millis(5);

    let shutdown = CancellationToken::new();
    let sd_clone = shutdown.clone();

    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(500));
        sd_clone.cancel();
    });

    // Pipeline must not return Err even though every batch will fail the
    // transform step.
    let result = pipeline.run(&shutdown);
    assert!(
        result.is_ok(),
        "pipeline should survive transform errors but got: {:?}",
        result.err()
    );

    // At least one transform error must have been counted.
    let errors = pipeline.metrics.transform_errors.load(Ordering::Relaxed);
    assert!(errors > 0, "expected transform_errors > 0, got {errors}");

    // Every failed transform must also increment the dropped-batches counter.
    let dropped = pipeline
        .metrics
        .dropped_batches_total
        .load(Ordering::Relaxed);
    assert!(
        dropped > 0,
        "expected dropped_batches_total > 0, got {dropped}"
    );
}

// -----------------------------------------------------------------------
// Async pipeline tests
// -----------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_async_pipeline_end_to_end() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("async_test.log");
    let mut data = String::new();
    for i in 0..100 {
        data.push_str(&format!(
            r#"{{"level":"INFO","msg":"async event {}","val":{}}}"#,
            i, i
        ));
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

    // Use devnull output to avoid stdout noise in test.
    pipeline = pipeline.with_sink(Box::new(DevNullSink));

    pipeline.batch_timeout = Duration::from_millis(50);

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        sd.cancel();
    });

    let result = pipeline.run_async(&shutdown).await;
    assert!(
        result.is_ok(),
        "async pipeline should succeed: {:?}",
        result.err()
    );

    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert!(
        lines_in >= 100,
        "expected at least 100 lines through transform, got {lines_in}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_async_pipeline_shutdown_drains_data() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("drain_test.log");

    // Write data
    let mut data = String::new();
    for i in 0..50 {
        data.push_str(&format!(r#"{{"msg":"drain {}"}}"#, i));
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

    // Give enough time for data to be read and processed, then shutdown.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        sd.cancel();
    });

    pipeline.run_async(&shutdown).await.unwrap();

    // After shutdown, all data should have been drained.
    let lines_in = pipeline
        .metrics
        .transform_in
        .lines_total
        .load(Ordering::Relaxed);
    assert!(
        lines_in >= 50,
        "shutdown should drain all data, got {lines_in} lines (expected >= 50)"
    );
}

// -------------------------------------------------------------------
// Shutdown stress tests
// -------------------------------------------------------------------

/// Shutdown must not deadlock when the bounded channel is full and
/// input threads are blocked in blocking_send. This was a real bug:
/// if the pipeline joins threads before draining the channel, the
/// threads can't make progress and the join hangs forever.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_shutdown_does_not_deadlock_on_full_channel() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("full_channel.log");

    // Write enough data to fill the channel (16 slots). Use a tiny
    // batch target so each line becomes its own channel message.
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

    // SlowSink blocks the consumer, causing the channel to fill up.
    pipeline = pipeline.with_sink(Box::new(SlowSink {
        delay: Duration::from_millis(50),
    }));
    pipeline.batch_target_bytes = 64; // tiny batches -> many channel sends
    pipeline.batch_timeout = Duration::from_millis(10);

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    // Cancel quickly — while input threads are likely blocked on a full channel.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        sd.cancel();
    });

    // This must complete without deadlock. If the drain/join order is
    // wrong, this will hang and the test will time out.
    let result = tokio::time::timeout(Duration::from_secs(5), pipeline.run_async(&shutdown)).await;

    assert!(
        result.is_ok(),
        "pipeline should not deadlock on shutdown with full channel"
    );
    assert!(
        result.unwrap().is_ok(),
        "pipeline should complete successfully"
    );
}

/// Terminal held-ticket shutdown must close the input channel before
/// joining producers. Otherwise producers blocked on a full bounded channel
/// can keep shutdown stuck forever.

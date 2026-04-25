#[tokio::test(flavor = "current_thread")]
async fn held_pool_ack_requests_terminal_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("held-pool-ack.log");
    std::fs::write(&log_path, "").unwrap();
    let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
    let machine = pipeline.machine.as_mut().unwrap();
    let ticket = machine.create_batch(SourceId(42), 1000);
    let ticket = machine.begin_send(ticket);

    let should_stop = pipeline
        .apply_pool_ack(AckItem {
            tickets: vec![ticket],
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

    assert!(
        should_stop,
        "held worker outcomes must terminalize ingestion instead of accumulating tickets"
    );
    assert_eq!(pipeline.held_tickets.len(), 1);
    assert_eq!(
        pipeline
            .machine
            .as_ref()
            .unwrap()
            .committed_checkpoint(SourceId(42)),
        None
    );
}

#[cfg(feature = "internal-failpoints")]
#[test]
#[serial]
fn test_failpoint_submit_before_pool_triggers_hold_and_shutdown() {
    let scenario = fail::FailScenario::setup();
    fail::cfg(
        "runtime::pipeline::submit::before_pool_submit",
        "1*return->off",
    )
    .expect("configure failpoint");

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("submit-failpoint.log");
    ffwd_test_utils::generate_json_lines(&log_path, 20, "submit-failpoint");

    let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
    pipeline.set_batch_timeout(Duration::from_millis(10));

    let shutdown = CancellationToken::new();
    let result = pipeline.run(&shutdown);
    assert!(
        result.is_ok(),
        "submit failpoint should trigger graceful shutdown without panic"
    );
    assert!(
        pipeline.machine.is_none(),
        "pipeline machine should still reach drained terminal state"
    );

    fail::remove("runtime::pipeline::submit::before_pool_submit");
    scenario.teardown();
}

#[cfg(feature = "internal-failpoints")]
#[test]
#[serial]
fn test_failpoint_shutdown_skip_channel_drain_remains_safe() {
    let scenario = fail::FailScenario::setup();
    fail::cfg("runtime::pipeline::run_async::skip_channel_drain", "return")
        .expect("configure failpoint");

    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("shutdown-skip-drain.log");
    ffwd_test_utils::generate_json_lines(&log_path, 100, "skip-drain");

    let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
    pipeline.set_batch_timeout(Duration::from_millis(10));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        sd.cancel();
    });

    let result = pipeline.run(&shutdown);
    assert!(
        result.is_ok(),
        "skip-drain failpoint must not deadlock or crash shutdown"
    );

    fail::remove("runtime::pipeline::run_async::skip_channel_drain");
    scenario.teardown();
}

#[test]
fn test_transform_filter_all_rows_does_not_crash() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    // Write data that will be filtered by the transform
    ffwd_test_utils::generate_json_lines(&log_path, 50, "filter-test");

    let yaml = single_pipeline_yaml_with_transform(
        &format!("type: file\npath: {}\nformat: json", log_path.display()),
        Some("\"SELECT * FROM logs WHERE upper(level) = 'NONEXISTENT'\""),
        "type: \"null\"",
    );
    let config = ffwd_config::Config::load_str(&yaml).unwrap();
    let pipe_cfg = &config.pipelines["default"];
    let mut pipeline = Pipeline::from_config("default", pipe_cfg, &test_meter(), None).unwrap();
    pipeline.set_batch_timeout(Duration::from_millis(10));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();

    let metrics = Arc::clone(pipeline.metrics());
    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if metrics.batches_total.load(Ordering::Relaxed) > 0 {
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
    assert!(
        result.is_ok(),
        "pipeline with filtering transform must not crash"
    );
    assert!(
        pipeline.machine.is_none(),
        "machine should drain cleanly even when transform filters all rows"
    );

    // Bug #700 fix: batch_rows_total must be 0 when all rows are filtered
    // by the SQL WHERE clause. batches_total must still be incremented.
    let batches = pipeline.metrics.batches_total.load(Ordering::Relaxed);
    assert!(
        batches > 0,
        "batches_total must be incremented even when transform filters all rows, got {batches}"
    );
    let batch_rows = pipeline.metrics.batch_rows_total.load(Ordering::Relaxed);
    assert_eq!(
        batch_rows, 0,
        "batch_rows_total must be 0 when transform filters all rows, got {batch_rows}"
    );
}

/// Write N lines, run the pipeline, shut down cleanly. Verify that
/// checkpoints.json exists and contains a non-zero offset.
#[test]
#[serial]
fn test_checkpoint_persisted_after_clean_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("test.log");
    ffwd_test_utils::generate_json_lines(&log_path, 50, "cp-test");

    // Override data dir so checkpoints land in our temp dir.
    // SAFETY: serialised by CHECKPOINT_ENV_MUTEX; spawned thread only
    // accesses metrics/shutdown, not environment variables.
    unsafe {
        std::env::set_var("FFWD_DATA_DIR", dir.path());
    }

    let mut pipeline = pipeline_with_sink(&log_path, Box::new(DevNullSink));
    pipeline.set_batch_timeout(Duration::from_millis(10));

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    let metrics = Arc::clone(pipeline.metrics());
    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if metrics.batch_rows_total.load(Ordering::Relaxed) >= 50 {
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

    pipeline.run(&shutdown).unwrap();

    // checkpoints.json must exist under the pipeline-scoped subdirectory.
    let cp_dir = dir.path().join("default");
    assert!(
        cp_dir.join("checkpoints.json").exists(),
        "checkpoints.json must exist after clean shutdown"
    );
    // Re-open the store to verify the checkpoints are readable.
    let store = FileCheckpointStore::open(&cp_dir).unwrap();
    let cps = store.load_all();
    assert!(!cps.is_empty(), "at least one checkpoint must be written");
    assert!(cps[0].offset > 0, "checkpoint offset must be non-zero");

    // SAFETY: serialised by CHECKPOINT_ENV_MUTEX; spawned thread only
    // accesses metrics/shutdown, not environment variables.
    unsafe {
        std::env::remove_var("FFWD_DATA_DIR");
    }
}

/// Write N lines, run pipeline, shut down, then write M more lines, run
/// again. The second run must process only the M new lines.
#[cfg(not(feature = "turmoil"))]
#[test]
#[serial]
fn test_pipeline_resumes_from_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("resume.log");
    // SAFETY: serialised by CHECKPOINT_ENV_MUTEX; spawned thread only
    // accesses metrics/shutdown, not environment variables.
    unsafe {
        std::env::set_var("FFWD_DATA_DIR", dir.path());
    }

    // First run: write 20 lines, process them.
    ffwd_test_utils::generate_json_lines(&log_path, 20, "resume-first");
    {
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let counter_clone = Arc::clone(&counter);
        let sink = ffwd_test_utils::CountingSink::new(counter_clone);

        let mut pipeline = pipeline_with_sink(&log_path, Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let metrics = Arc::clone(pipeline.metrics());
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                if metrics.batch_rows_total.load(Ordering::Relaxed) >= 20 {
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
        pipeline.run(&shutdown).unwrap();
        assert_eq!(
            counter.load(Ordering::Relaxed),
            20,
            "first run must process 20 rows"
        );
    }

    // Append 10 more lines.
    ffwd_test_utils::append_json_lines(&log_path, 10, "resume-second");

    // Second run: must process only the 10 new lines.
    {
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let counter_clone = Arc::clone(&counter);
        let sink = ffwd_test_utils::CountingSink::new(counter_clone);

        let mut pipeline = pipeline_with_sink(&log_path, Box::new(sink));
        pipeline.set_batch_timeout(Duration::from_millis(10));

        let shutdown = CancellationToken::new();
        let sd = shutdown.clone();
        let metrics = Arc::clone(pipeline.metrics());
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                if metrics.batch_rows_total.load(Ordering::Relaxed) >= 10 {
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
        pipeline.run(&shutdown).unwrap();
        assert_eq!(
            counter.load(Ordering::Relaxed),
            10,
            "second run must process only the 10 new lines, not re-read the first 20"
        );
    }

    // SAFETY: serialised by CHECKPOINT_ENV_MUTEX; spawned thread only
    // accesses metrics/shutdown, not environment variables.
    unsafe {
        std::env::remove_var("FFWD_DATA_DIR");
    }
}

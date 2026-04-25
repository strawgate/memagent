    #[tokio::test]
    async fn pool_acks_flow_back_to_receiver() {
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: false,
            fail_shutdown: false,
        });
        let mut pool = OutputWorkerPool::new(factory, 2, Duration::from_secs(60), test_metrics());

        pool.submit(empty_work_item()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let ack = pool.ack_rx_mut().try_recv();
        assert!(ack.is_ok(), "expected an ack item");
        assert_eq!(ack.unwrap().outcome, DeliveryOutcome::Delivered);

        pool.drain(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn pool_transient_sink_errors_hold_until_pool_cancel() {
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: true,
            fail_shutdown: false,
        });
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), test_metrics());

        pool.submit(empty_work_item()).await;
        // Transient errors should keep the batch in-flight; no terminal ack yet.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            pool.ack_rx_mut().try_recv().is_err(),
            "transient failures should not terminalize before cancellation"
        );

        // Drain with a short graceful window to trigger cancellation.
        pool.drain(Duration::from_millis(50)).await;

        let ack = pool.ack_rx_mut().try_recv();
        assert!(ack.is_ok(), "expected an ack item after cancellation");
        assert_eq!(
            ack.unwrap().outcome,
            DeliveryOutcome::PoolClosed,
            "ack should report pool-closed cancellation for in-flight transient failures"
        );
    }

    #[tokio::test]
    async fn pool_respects_max_workers() {
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: false,
            fail_shutdown: false,
        });
        // max 2 workers
        let mut pool = OutputWorkerPool::new(factory, 2, Duration::from_secs(60), test_metrics());

        // Submit many items quickly to saturate channels.
        for _ in 0..10 {
            pool.submit(empty_work_item()).await;
        }

        assert!(
            pool.worker_count() <= 2,
            "worker count exceeded max_workers"
        );

        pool.drain(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn pool_drain_propagates_shutdown_error() {
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: false,
            fail_shutdown: true,
        });

        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("test", "", &meter);
        let out_stats = pm.add_output("counting", "counting");
        let metrics = Arc::new(pm);

        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), metrics);

        pool.submit(empty_work_item()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        pool.drain(Duration::from_secs(5)).await;

        assert_eq!(
            out_stats.errors(),
            1,
            "expected output_error to increment stats when shutdown fails"
        );
        assert_eq!(
            out_stats.health(),
            ComponentHealth::Stopped,
            "shutdown failure should not make the output look permanently failed"
        );
    }

    #[tokio::test]
    async fn pool_drain_waits_for_in_flight() {
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: false,
            fail_shutdown: false,
        });
        let mut pool = OutputWorkerPool::new(factory, 2, Duration::from_secs(60), test_metrics());

        for _ in 0..5 {
            pool.submit(empty_work_item()).await;
        }

        // drain should wait until all workers finish.
        pool.drain(Duration::from_secs(5)).await;

        // All 5 items should have been processed.
        assert_eq!(calls.load(Ordering::Relaxed), 5);
    }

    #[tokio::test]
    async fn pool_retrying_output_marks_degraded_then_recovers() {
        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("test", "", &meter);
        let out_stats = pm.add_output("scripted", "http");
        let metrics = Arc::new(pm);
        let factory = Arc::new(ScriptedSinkFactory::new(
            "scripted",
            vec![
                ScriptedResult::RetryAfter(Duration::from_millis(40)),
                ScriptedResult::Ok,
            ],
        ));
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), metrics);

        pool.submit(empty_work_item()).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(out_stats.health(), ComponentHealth::Degraded);

        tokio::time::sleep(Duration::from_millis(80)).await;
        let ack = pool.ack_rx_mut().try_recv().expect("expected ack item");
        assert_eq!(ack.outcome, DeliveryOutcome::Delivered);
        assert_eq!(out_stats.health(), ComponentHealth::Healthy);

        pool.drain(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn pool_rejected_output_marks_failed() {
        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("test", "", &meter);
        let out_stats = pm.add_output("scripted", "http");
        let metrics = Arc::new(pm);
        let factory = Arc::new(ScriptedSinkFactory::new(
            "scripted",
            vec![ScriptedResult::Rejected("bad request")],
        ));
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), metrics);

        pool.submit(empty_work_item()).await;
        tokio::time::sleep(Duration::from_millis(30)).await;

        let ack = pool.ack_rx_mut().try_recv().expect("expected ack item");
        assert_eq!(
            ack.outcome,
            DeliveryOutcome::Rejected {
                reason: "bad request".to_string()
            }
        );
        assert_eq!(out_stats.health(), ComponentHealth::Failed);

        pool.drain(Duration::from_secs(5)).await;
    }

    #[test]
    fn elasticsearch_400_records_status_code_and_batch_rejected_log() {
        let capture = TraceCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        runtime.block_on(async {
            let (endpoint, mock_handle) = start_elasticsearch_mock(
                400,
                r#"{"error":{"type":"mapper_parsing_exception","reason":"bad field"}}"#,
                1,
            );
            let meter = ffwd_test_utils::test_meter();
            let mut pm = PipelineMetrics::new("test", "", &meter);
            let out_stats = pm.add_output("test_es", "elasticsearch");
            let factory = Arc::new(
                ElasticsearchSinkFactory::new(
                    "test_es".to_string(),
                    endpoint,
                    "logs".to_string(),
                    vec![],
                    false,
                    ElasticsearchRequestMode::Buffered,
                    Arc::clone(&out_stats),
                )
                .expect("factory creation"),
            );
            let metrics = Arc::new(pm);
            let mut pool =
                OutputWorkerPool::new(factory, 1, Duration::from_secs(60), Arc::clone(&metrics));

            pool.submit(empty_work_item()).await;

            let ack = tokio::time::timeout(Duration::from_secs(5), pool.ack_rx_mut().recv())
                .await
                .expect("timed out waiting for ack")
                .expect("ack channel closed");
            assert!(
                matches!(ack.outcome, DeliveryOutcome::Rejected { .. }),
                "expected rejected outcome, got {:?}",
                ack.outcome
            );

            pool.drain(Duration::from_secs(5)).await;
            mock_handle.join().expect("mock thread panicked");
        });

        let output_span = capture.closed_output_span().expect("closed output span");
        assert_eq!(
            output_span.fields.get("status_code"),
            Some(&"400".to_string())
        );
        assert!(
            output_span
                .fields
                .get("resp_bytes")
                .and_then(|v| v.parse::<u64>().ok())
                .is_some_and(|v| v > 0),
            "expected response bytes on failed Elasticsearch request: {output_span:?}"
        );
        assert!(
            capture.contains_event_message("worker_pool: batch rejected"),
            "expected rejection log event"
        );
        assert!(
            capture.contains_event_field("reason", "HTTP 400"),
            "expected rejection reason to include HTTP 400"
        );
    }

    #[test]
    fn elasticsearch_503_records_status_code_and_retry_logs() {
        let capture = TraceCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        runtime.block_on(async {
            let (endpoint, mock_handle) = start_elasticsearch_mock(
                503,
                r#"{"error":{"type":"unavailable_shards_exception","reason":"busy"}}"#,
                4,
            );
            let meter = ffwd_test_utils::test_meter();
            let mut pm = PipelineMetrics::new("test", "", &meter);
            let out_stats = pm.add_output("test_es", "elasticsearch");
            let factory = Arc::new(
                ElasticsearchSinkFactory::new(
                    "test_es".to_string(),
                    endpoint,
                    "logs".to_string(),
                    vec![],
                    false,
                    ElasticsearchRequestMode::Buffered,
                    Arc::clone(&out_stats),
                )
                .expect("factory creation"),
            );
            let metrics = Arc::new(pm);
            let mut pool =
                OutputWorkerPool::new(factory, 1, Duration::from_secs(60), Arc::clone(&metrics));

            pool.submit(empty_work_item()).await;

            // 503 responses are transient. They should keep retrying until
            // cancellation instead of producing RetryExhausted.
            tokio::time::sleep(Duration::from_millis(500)).await;
            assert!(
                pool.ack_rx_mut().try_recv().is_err(),
                "transient 503 path should remain in-flight before cancellation"
            );

            pool.drain(Duration::from_millis(50)).await;

            let ack = tokio::time::timeout(Duration::from_secs(5), pool.ack_rx_mut().recv())
                .await
                .expect("timed out waiting for ack")
                .expect("ack channel closed");
            assert_eq!(ack.outcome, DeliveryOutcome::PoolClosed);

            mock_handle.join().expect("mock thread panicked");
        });

        let output_span = capture.closed_output_span().expect("closed output span");
        assert_eq!(
            output_span.fields.get("status_code"),
            Some(&"503".to_string())
        );
        assert!(
            capture.contains_event_message("worker_pool: transient error, retrying"),
            "expected retry log event"
        );
        assert!(
            capture.contains_event_field("error", "503"),
            "expected logged error to include HTTP 503 detail"
        );
    }

    #[tokio::test]
    async fn pool_create_failure_marks_output_failed() {
        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("test", "", &meter);
        let out_stats = pm.add_output("broken", "http");
        let metrics = Arc::new(pm);
        let factory = Arc::new(FailingCreateFactory { name: "broken" });
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), metrics);

        pool.submit(empty_work_item()).await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        let ack = pool.ack_rx_mut().try_recv().expect("expected rejected ack");
        assert_eq!(ack.outcome, DeliveryOutcome::NoWorkersAvailable);
        assert_eq!(out_stats.health(), ComponentHealth::Failed);
    }

    #[tokio::test]
    async fn pool_drain_marks_output_stopped() {
        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("test", "", &meter);
        let out_stats = pm.add_output("counting", "counting");
        let metrics = Arc::new(pm);
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: false,
            fail_shutdown: false,
        });
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), metrics);

        pool.submit(empty_work_item()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        pool.drain(Duration::from_secs(5)).await;

        assert_eq!(out_stats.health(), ComponentHealth::Stopped);
    }

    #[tokio::test]
    async fn idle_worker_exit_keeps_output_ready() {
        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("test", "", &meter);
        let out_stats = pm.add_output("counting", "counting");
        let metrics = Arc::new(pm);
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: false,
            fail_shutdown: false,
        });
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_millis(20), metrics);

        pool.submit(empty_work_item()).await;
        tokio::time::sleep(Duration::from_millis(80)).await;

        assert_eq!(out_stats.health(), ComponentHealth::Healthy);

        pool.drain(Duration::from_secs(5)).await;
    }

    #[test]
    fn output_health_tracker_keeps_worst_live_worker_state() {
        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_a = pm.add_output("output_0", "http");
        let out_b = pm.add_output("output_1", "stdout");
        let tracker = OutputHealthTracker::new(vec![Arc::clone(&out_a), Arc::clone(&out_b)]);

        tracker.insert_worker(1, ComponentHealth::Healthy);
        tracker.insert_worker(2, ComponentHealth::Healthy);
        tracker.apply_worker_event(1, OutputHealthEvent::Retrying);

        assert_eq!(out_a.health(), ComponentHealth::Degraded);
        assert_eq!(out_b.health(), ComponentHealth::Degraded);
        assert_eq!(tracker.slot_health(1), Some(ComponentHealth::Degraded));
        assert_eq!(tracker.slot_health(2), Some(ComponentHealth::Healthy));

        tracker.apply_worker_event(2, OutputHealthEvent::DeliverySucceeded);

        assert_eq!(out_a.health(), ComponentHealth::Degraded);
        assert_eq!(out_b.health(), ComponentHealth::Degraded);

        tracker.remove_worker(1);

        assert_eq!(out_a.health(), ComponentHealth::Healthy);
        assert_eq!(out_b.health(), ComponentHealth::Healthy);
        assert_eq!(tracker.slot_health(1), None);
        assert_eq!(tracker.slot_health(2), Some(ComponentHealth::Healthy));
    }

    #[test]
    fn output_health_tracker_keeps_pool_phase_over_late_worker_events() {
        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_stats = pm.add_output("output_0", "http");
        let tracker = OutputHealthTracker::new(vec![Arc::clone(&out_stats)]);

        tracker.insert_worker(1, ComponentHealth::Healthy);
        tracker.set_pool_health(ComponentHealth::Stopping);
        assert_eq!(out_stats.health(), ComponentHealth::Stopping);

        tracker.apply_worker_event(1, OutputHealthEvent::DeliverySucceeded);
        assert_eq!(out_stats.health(), ComponentHealth::Stopping);

        tracker.remove_worker(1);
        assert_eq!(out_stats.health(), ComponentHealth::Stopping);

        tracker.set_pool_health(ComponentHealth::Stopped);
        assert_eq!(out_stats.health(), ComponentHealth::Stopped);
    }

    #[test]
    fn output_health_tracker_keeps_failed_pool_phase_on_new_worker() {
        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_stats = pm.add_output("output_0", "http");
        let tracker = OutputHealthTracker::new(vec![Arc::clone(&out_stats)]);

        tracker.set_pool_health(ComponentHealth::Failed);
        assert_eq!(out_stats.health(), ComponentHealth::Failed);

        tracker.insert_worker(1, ComponentHealth::Starting);
        assert_eq!(out_stats.health(), ComponentHealth::Failed);
        assert_eq!(tracker.slot_health(1), Some(ComponentHealth::Starting));
    }

    #[test]
    fn output_health_tracker_ignores_unknown_worker_events() {
        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_stats = pm.add_output("output_0", "http");
        let tracker = OutputHealthTracker::new(vec![Arc::clone(&out_stats)]);

        tracker.insert_worker(1, ComponentHealth::Healthy);
        let aggregate = tracker.apply_worker_event(999, OutputHealthEvent::FatalFailure);

        assert_eq!(aggregate, ComponentHealth::Healthy);
        assert_eq!(out_stats.health(), ComponentHealth::Healthy);
        assert_eq!(tracker.slot_health(1), Some(ComponentHealth::Healthy));
        assert_eq!(tracker.slot_health(999), None);
    }

    #[test]
    fn output_health_tracker_force_clear_drops_stale_worker_slots() {
        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_stats = pm.add_output("output_0", "http");
        let tracker = OutputHealthTracker::new(vec![Arc::clone(&out_stats)]);

        tracker.insert_worker(1, ComponentHealth::Healthy);
        tracker.insert_worker(2, ComponentHealth::Healthy);
        tracker.apply_worker_event(2, OutputHealthEvent::FatalFailure);
        assert!(tracker.has_active_workers());
        assert_eq!(out_stats.health(), ComponentHealth::Failed);

        let aggregate = tracker.clear_workers_and_set_pool_health(ComponentHealth::Stopped);

        assert_eq!(aggregate, ComponentHealth::Stopped);
        assert_eq!(out_stats.health(), ComponentHealth::Stopped);
        assert!(!tracker.has_active_workers());
        assert_eq!(tracker.slot_health(1), None);
        assert_eq!(tracker.slot_health(2), None);
    }

    #[tokio::test]
    async fn create_failure_with_live_worker_does_not_mark_output_failed() {
        #[derive(Default)]
        struct OneShotFactory {
            created: AtomicBool,
        }

        impl SinkFactory for OneShotFactory {
            fn create(&self) -> io::Result<Box<dyn Sink>> {
                if self.created.swap(true, Ordering::Relaxed) {
                    Err(io::Error::other("create failed"))
                } else {
                    Ok(Box::new(CountingSink {
                        name: "oneshot".into(),
                        calls: Arc::new(AtomicU32::new(0)),
                        fail: false,
                        fail_shutdown: false,
                    }))
                }
            }

            fn name(&self) -> &'static str {
                "oneshot"
            }
        }

        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_stats = pm.add_output("oneshot", "http");
        let metrics = Arc::new(pm);
        let factory = Arc::new(OneShotFactory::default());
        let mut pool = OutputWorkerPool::new(factory, 2, Duration::from_secs(60), metrics);

        let handle = pool.spawn_worker().expect("first worker should spawn");
        pool.workers.push_front(handle);
        assert_eq!(out_stats.health(), ComponentHealth::Healthy);

        let err = pool.spawn_worker().err().expect("second spawn should fail");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert_eq!(out_stats.health(), ComponentHealth::Healthy);

        pool.drain(Duration::from_secs(5)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_worker_reports_starting_while_create_is_in_flight() {
        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_stats = pm.add_output("slow-create", "http");
        let metrics = Arc::new(pm);
        let entered_create = Arc::new(AtomicBool::new(false));
        let release_create = Arc::new(AtomicBool::new(false));
        let factory = Arc::new(SlowCreateFactory {
            entered_create: Arc::clone(&entered_create),
            release_create: Arc::clone(&release_create),
        });
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), metrics);

        let submit = tokio::spawn(async move {
            pool.submit(empty_work_item()).await;
            pool
        });

        wait_for_flag(&entered_create).await;
        assert_eq!(out_stats.health(), ComponentHealth::Starting);

        release_create.store(true, Ordering::Release);
        let mut pool = submit.await.expect("submit task should complete");
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(out_stats.health(), ComponentHealth::Healthy);

        pool.drain(Duration::from_secs(5)).await;
    }

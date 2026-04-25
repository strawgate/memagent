    #[tokio::test]
    async fn pool_forced_abort_clears_stale_worker_slots() {
        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_stats = pm.add_output("hanging", "http");
        let metrics = Arc::new(pm);
        let entered_send = Arc::new(AtomicBool::new(false));
        let factory = Arc::new(HangingSinkFactory {
            entered_send: Arc::clone(&entered_send),
        });
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), metrics);

        pool.submit(empty_work_item()).await;
        wait_for_flag(&entered_send).await;
        assert!(pool.output_health.has_active_workers());
        assert_eq!(
            pool.output_health.slot_health(0),
            Some(ComponentHealth::Healthy)
        );

        pool.drain(Duration::from_millis(10)).await;

        assert_eq!(out_stats.health(), ComponentHealth::Stopped);
        assert!(!pool.output_health.has_active_workers());
        assert_eq!(pool.output_health.slot_health(0), None);
        if let Ok(ack) = pool.ack_rx_mut().try_recv() {
            assert_ne!(
                ack.outcome,
                DeliveryOutcome::Delivered,
                "forced-abort path must never emit a delivered ack for the hung batch"
            );
        }
    }

    #[tokio::test]
    async fn worker_panic_does_not_leave_stale_output_slot() {
        let meter = ffwd_test_utils::test_meter();
        let mut pm = PipelineMetrics::new("pipe", "", &meter);
        let out_stats = pm.add_output("panic", "http");
        let metrics = Arc::new(pm);
        let entered_send = Arc::new(AtomicBool::new(false));
        let factory = Arc::new(PanicSinkFactory {
            entered_send: Arc::clone(&entered_send),
        });
        let mut pool = OutputWorkerPool::new(factory, 1, Duration::from_secs(60), metrics);

        pool.submit(empty_work_item()).await;
        wait_for_flag(&entered_send).await;
        wait_for_no_active_workers(&pool).await;

        let ack = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Ok(ack) = pool.ack_rx_mut().try_recv() {
                    break ack;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("panicking worker must still emit an ack");
        assert_eq!(
            ack.outcome,
            DeliveryOutcome::InternalFailure,
            "panic path must surface InternalFailure to hold checkpoint seam"
        );

        assert_eq!(pool.output_health.slot_health(0), None);
        assert_eq!(out_stats.health(), ComponentHealth::Healthy);

        pool.drain(Duration::from_secs(5)).await;
    }

    #[cfg(feature = "loom-tests")]
    #[test]
    fn loom_worker_removal_vs_late_event_model_never_resurrects_slot() {
        struct LoomState {
            worker_present: bool,
            slot_gen: u64,
            slot_health: ComponentHealth,
        }

        loom::model(|| {
            let state = loom::sync::Arc::new(loom::sync::Mutex::new(LoomState {
                worker_present: true,
                slot_gen: 0,
                slot_health: ComponentHealth::Starting,
            }));

            let remove_state = loom::sync::Arc::clone(&state);
            let remove = loom::thread::spawn(move || {
                let mut state = remove_state
                    .lock()
                    .expect("loom mutex poisoned while removing worker");
                state.slot_gen += 1;
                state.worker_present = false;
            });

            let event_state = loom::sync::Arc::clone(&state);
            let event = loom::thread::spawn(move || {
                let observed_gen = {
                    let state = event_state
                        .lock()
                        .expect("loom mutex poisoned while observing worker slot");
                    state.slot_gen
                };
                let mut state = event_state
                    .lock()
                    .expect("loom mutex poisoned while applying event");
                if state.worker_present && observed_gen == state.slot_gen {
                    state.slot_health = reduce_worker_slot_health(
                        state.slot_health,
                        OutputHealthEvent::FatalFailure,
                    );
                    true
                } else {
                    false
                }
            });

            remove.join().expect("remove thread should not panic");
            let event_applied = event.join().expect("event thread should not panic");

            let state = state
                .lock()
                .expect("loom mutex poisoned while validating state");
            assert!(
                !state.worker_present,
                "late events must not resurrect removed worker state"
            );
            assert_eq!(
                state.slot_gen, 1,
                "worker removal must advance the slot generation"
            );
            let expected_health = if event_applied {
                reduce_worker_slot_health(
                    ComponentHealth::Starting,
                    OutputHealthEvent::FatalFailure,
                )
            } else {
                ComponentHealth::Starting
            };
            assert_eq!(
                state.slot_health, expected_health,
                "stale events must not mutate the resurrectable slot state"
            );
        });
    }

    #[test]
    fn deterministic_worker_removal_late_event_orders_match_expected_health() {
        #[derive(Clone, Copy)]
        enum Step {
            Observe,
            Remove,
            Apply,
        }

        struct DeterministicState {
            worker_present: bool,
            slot_gen: u64,
            slot_health: ComponentHealth,
            observed_gen: Option<u64>,
        }

        fn run_order(order: [Step; 3]) -> (bool, ComponentHealth) {
            let mut state = DeterministicState {
                worker_present: true,
                slot_gen: 0,
                slot_health: ComponentHealth::Starting,
                observed_gen: None,
            };
            let mut event_applied = false;

            for step in order {
                match step {
                    Step::Observe => {
                        state.observed_gen = Some(state.slot_gen);
                    }
                    Step::Remove => {
                        state.slot_gen = state.slot_gen.saturating_add(1);
                        state.worker_present = false;
                    }
                    Step::Apply => {
                        if state.worker_present && state.observed_gen == Some(state.slot_gen) {
                            state.slot_health = reduce_worker_slot_health(
                                state.slot_health,
                                OutputHealthEvent::FatalFailure,
                            );
                            event_applied = true;
                        }
                    }
                }
            }

            (event_applied, state.slot_health)
        }

        let expected_degraded =
            reduce_worker_slot_health(ComponentHealth::Starting, OutputHealthEvent::FatalFailure);

        // Order 1: event fully completes before removal. Health can degrade.
        let (event_applied_before_removal, health_before_removal) =
            run_order([Step::Observe, Step::Apply, Step::Remove]);
        assert!(event_applied_before_removal);
        assert_eq!(health_before_removal, expected_degraded);

        // Order 2: remove happens before event apply. Stale event must be ignored.
        let (event_applied_after_removal, health_after_removal) =
            run_order([Step::Observe, Step::Remove, Step::Apply]);
        assert!(!event_applied_after_removal);
        assert_eq!(health_after_removal, ComponentHealth::Starting);
    }

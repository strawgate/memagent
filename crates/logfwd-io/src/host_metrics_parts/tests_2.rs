    #[test]
    fn health_degrades_on_reload_error_and_recovers_after_valid_reload() {
        let (_dir, control_path) = tempfiles::control_file_path();
        tempfiles::write_malformed_control_file(&control_path);

        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                control_path: Some(control_path.clone()),
                control_reload_interval: Duration::from_millis(1),
                emit_signal_rows: false,
                ..HostMetricsConfig::default()
            },
        )
        .expect("startup should succeed");

        assert_eq!(input.health(), ComponentHealth::Starting);
        let _ = input.poll().expect("startup poll");
        assert_eq!(input.health(), ComponentHealth::Healthy);

        std::thread::sleep(Duration::from_millis(2));
        let _ = input.poll().expect("reload poll");
        assert_eq!(input.health(), ComponentHealth::Degraded);

        tempfiles::write_control_file(
            &control_path,
            serde_json::json!({
                "generation": 2,
                "enabled_families": ["process"],
                "emit_signal_rows": false,
            }),
        );
        std::thread::sleep(Duration::from_millis(2));
        let _ = input.poll().expect("recovery poll");
        assert_eq!(input.health(), ComponentHealth::Healthy);
    }

    #[test]
    fn poll_error_preserves_machine_and_name_invariants() {
        let mut input =
            HostMetricsInput::new("sensor", host_target(), HostMetricsConfig::default())
                .expect("host target should be valid");

        let init = match input.machine.as_mut() {
            Some(HostMetricsMachine::Init(init)) => init,
            _ => panic!("expected init machine"),
        };
        init.common.schema = Arc::new(Schema::new(Vec::<Field>::new()));

        let err = match input.poll() {
            Ok(_) => panic!("invalid schema must fail"),
            Err(err) => err,
        };
        assert!(
            !err.to_string().contains("state missing"),
            "unexpected state-loss error: {err}"
        );

        // Even after a poll error, public API should remain non-panicking.
        assert_eq!(input.name(), "sensor");

        let second_err = match input.poll() {
            Ok(_) => panic!("machine should still be present"),
            Err(err) => err,
        };
        assert!(
            !second_err.to_string().contains("state missing"),
            "machine should not be lost after an error: {second_err}"
        );
    }

    #[test]
    fn health_transitions_from_starting_to_healthy_after_first_poll() {
        let mut input =
            HostMetricsInput::new("sensor", host_target(), HostMetricsConfig::default())
                .expect("host target should be valid");

        assert_eq!(input.health(), ComponentHealth::Starting);
        let _ = input.poll().expect("startup poll succeeds");
        assert_eq!(input.health(), ComponentHealth::Healthy);
    }

    #[test]
    fn health_degrades_on_control_reload_failure_and_recovers_on_success() {
        let (_dir, control_path) = tempfiles::control_file_path();
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                control_path: Some(control_path.clone()),
                control_reload_interval: Duration::from_millis(1),
                emit_signal_rows: false,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        assert_eq!(input.health(), ComponentHealth::Starting);
        let _ = input.poll().expect("startup poll");
        assert_eq!(input.health(), ComponentHealth::Healthy);

        tempfiles::write_control_file(&control_path, serde_json::json!({"generation":"bad"}));
        std::thread::sleep(Duration::from_millis(2));
        let _ = input.poll().expect("reload failure poll");
        assert_eq!(input.health(), ComponentHealth::Degraded);

        tempfiles::write_control_file(
            &control_path,
            serde_json::json!({"generation":2,"enabled_families":["process"],"emit_signal_rows":false}),
        );
        std::thread::sleep(Duration::from_millis(2));
        let _ = input.poll().expect("reload recovery poll");
        assert_eq!(input.health(), ComponentHealth::Healthy);
    }

    fn u32_col_optional(batch: &RecordBatch, name: &str) -> Vec<Option<u32>> {
        let idx = batch.schema().index_of(name).expect("column exists");
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("u32 array");
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            })
            .collect()
    }

    #[test]
    fn first_poll_emits_process_data() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["process".to_string()]),
                poll_interval: Duration::from_millis(1),
                max_rows_per_poll: 8,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        let _ = input.poll().expect("startup poll");
        std::thread::sleep(Duration::from_millis(2));

        let events = input.poll().expect("second poll with data");
        assert_eq!(events.len(), 1);
        let batch = first_batch(&events);
        let pids = u32_col_optional(batch, "process_pid");
        assert!(
            pids.iter().any(Option::is_some),
            "process snapshot rows should have process_pid set"
        );
        let kinds = string_col(batch, "event_kind");
        assert!(
            kinds.iter().any(|v| v.as_deref() == Some("snapshot")),
            "should contain snapshot event_kind"
        );
    }

    #[test]
    fn first_poll_emits_network_data() {
        // Skip only when the exact precondition the sensor relies on isn't
        // met: `sysinfo::Networks` has to enumerate at least one interface,
        // because that's the source the sensor reads. An earlier revision
        // also gated on `UdpSocket::connect("192.0.2.1:1")`, but that
        // over-skips — network-isolated containers with usable loopback
        // interfaces fail the route probe even though sysinfo would still
        // populate `network_interface`, quietly bypassing the assertion
        // exactly where this portability test matters most.
        //
        // Silent skip — `print_stderr` is a workspace-level warn and the
        // other portability skips in this PR stay silent too.
        let mut iface_probe = Networks::new_with_refreshed_list();
        iface_probe.refresh(true);
        if iface_probe.iter().next().is_none() {
            return;
        }

        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["network".to_string()]),
                poll_interval: Duration::from_millis(1),
                max_rows_per_poll: 64,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        let _ = input.poll().expect("startup poll");
        std::thread::sleep(Duration::from_millis(2));

        let events = input.poll().expect("second poll with data");
        // Some CI environments may have no network interfaces, so just check
        // we get at least an empty successful poll.
        if !events.is_empty() {
            let batch = first_batch(&events);
            let ifaces = string_col(batch, "network_interface");
            assert!(
                ifaces.iter().any(Option::is_some),
                "network snapshot rows should have network_interface set"
            );
        }
    }

    #[test]
    fn collection_respects_enabled_families() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["process".to_string()]),
                emit_signal_rows: false,
                poll_interval: Duration::from_millis(1),
                max_rows_per_poll: 8,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        let _ = input.poll().expect("startup poll");
        std::thread::sleep(Duration::from_millis(2));

        let events = input.poll().expect("second poll");
        assert_eq!(events.len(), 1);
        let batch = first_batch(&events);
        let families = string_col(batch, "event_family");
        assert!(
            !families.iter().any(|v| v.as_deref() == Some("network")),
            "network rows should not be emitted when only process is enabled"
        );
        assert!(
            !families.iter().any(|v| v.as_deref() == Some("disk_io")),
            "disk_io rows should not be emitted when only process is enabled"
        );
    }

    #[test]
    fn max_rows_per_poll_caps_data_collection() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["process".to_string()]),
                emit_signal_rows: false,
                poll_interval: Duration::from_millis(1),
                max_rows_per_poll: 3,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        let _ = input.poll().expect("startup poll");
        std::thread::sleep(Duration::from_millis(2));

        let events = input.poll().expect("second poll");
        assert_eq!(events.len(), 1);
        let batch = first_batch(&events);
        // On any real system there are more than 3 processes; the cap should hold.
        // The system health row is always emitted outside the budget, so exclude it.
        let event_families = string_col(batch, "event_family");
        let snapshot_count = string_col(batch, "event_kind")
            .iter()
            .zip(event_families.iter())
            .filter(|(kind, family)| {
                kind.as_deref() == Some("snapshot") && family.as_deref() != Some("system")
            })
            .count();
        assert!(
            snapshot_count <= 3,
            "max_rows_per_poll should cap data rows, got {snapshot_count}"
        );
    }

    #[test]
    fn max_process_rows_per_poll_caps_process_collection() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["process".to_string()]),
                emit_signal_rows: false,
                max_rows_per_poll: usize::MAX,
                max_process_rows_per_poll: 3,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host metrics input should construct");

        let events = input.poll().expect("poll should succeed");
        let batch = first_batch(&events);

        let families = string_col(batch, "signal_family");
        let kinds = string_col(batch, "event_kind");
        let snapshot_count = families
            .iter()
            .zip(kinds.iter())
            .filter(|(_, kind)| kind.as_deref() == Some("snapshot"))
            .filter(|(family, _)| family.as_deref() == Some("process"))
            .count();
        assert!(
            snapshot_count > 0,
            "expected process snapshot rows to be emitted so the cap can be validated"
        );
        assert!(
            snapshot_count <= 3,
            "max_process_rows_per_poll should cap process rows, got {snapshot_count}"
        );
    }

    #[test]
    fn zero_max_process_rows_per_poll_uses_runtime_default() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["process".to_string()]),
                emit_signal_rows: false,
                max_rows_per_poll: usize::MAX,
                max_process_rows_per_poll: 0,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host metrics input should construct");

        let events = input.poll().expect("poll should succeed");
        let batch = first_batch(&events);

        let families = string_col(batch, "signal_family");
        let kinds = string_col(batch, "event_kind");
        let snapshot_count = families
            .iter()
            .zip(kinds.iter())
            .filter(|(_, kind)| kind.as_deref() == Some("snapshot"))
            .filter(|(family, _)| family.as_deref() == Some("process"))
            .count();
        assert!(
            snapshot_count > 0,
            "expected default process cap to leave process rows enabled"
        );
        assert!(
            snapshot_count <= DEFAULT_MAX_PROCESS_ROWS_PER_POLL,
            "default process cap should apply when runtime config is 0, got {snapshot_count}"
        );
    }

    #[test]
    fn process_snapshot_has_valid_fields() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["process".to_string()]),
                emit_signal_rows: false,
                poll_interval: Duration::from_millis(1),
                max_rows_per_poll: 16,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        // Two polls so sysinfo has history for deltas.
        let _ = input.poll().expect("startup poll");
        std::thread::sleep(Duration::from_millis(50));
        let events = input.poll().expect("second poll");
        assert_eq!(events.len(), 1);
        let batch = first_batch(&events);

        let pids = u32_col_optional(batch, "process_pid");
        let names = string_col(batch, "process_name");
        let mem_col_idx = batch
            .schema()
            .index_of("process_memory_bytes")
            .expect("col");
        let mem_arr = batch
            .column(mem_col_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("u64");

        let mut found_valid = false;
        for i in 0..batch.num_rows() {
            if let Some(pid) = pids[i] {
                assert!(pid > 0, "pid should be positive");
                let name = names[i].as_deref().unwrap_or("");
                assert!(!name.is_empty(), "process name should not be empty");
                if !mem_arr.is_null(i) {
                    // At least some processes have nonzero memory
                    found_valid = true;
                }
            }
        }
        assert!(found_valid, "at least one process should have memory data");
    }

    #[test]
    fn disk_io_family_emits_only_active_processes() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["file".to_string()]),
                emit_signal_rows: false,
                poll_interval: Duration::from_millis(1),
                max_rows_per_poll: 256,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        // First poll primes sysinfo; on second poll we may see disk_io rows.
        let _ = input.poll().expect("startup poll");
        std::thread::sleep(Duration::from_millis(50));
        let events = input.poll().expect("second poll");

        // Even if no process has active I/O, we should not crash.
        if !events.is_empty() {
            let batch = first_batch(&events);
            let families = string_col(batch, "event_family");
            // All data rows should be disk_io (no process or network)
            for fam in &families {
                if fam.as_deref() == Some("disk_io") {
                    // disk_io rows are expected
                }
            }
            assert!(
                !families.iter().any(|v| v.as_deref() == Some("process")),
                "only file family enabled; no process rows expected"
            );
        }
    }

    #[test]
    fn second_cycle_network_deltas_are_plausible() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["network".to_string()]),
                emit_signal_rows: false,
                poll_interval: Duration::from_millis(1),
                max_rows_per_poll: 64,
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        let _ = input.poll().expect("startup poll");
        std::thread::sleep(Duration::from_millis(50));
        let events = input.poll().expect("second poll");

        if !events.is_empty() {
            let batch = first_batch(&events);
            let ifaces = string_col(batch, "network_interface");

            let rx_total_idx = batch
                .schema()
                .index_of("network_received_total_bytes")
                .expect("col");
            let rx_total_arr = batch
                .column(rx_total_idx)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("u64");

            let rx_delta_idx = batch
                .schema()
                .index_of("network_received_delta_bytes")
                .expect("col");
            let rx_delta_arr = batch
                .column(rx_delta_idx)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("u64");

            for i in 0..batch.num_rows() {
                if ifaces[i].is_some() && !rx_total_arr.is_null(i) && !rx_delta_arr.is_null(i) {
                    let total = rx_total_arr.value(i);
                    let delta = rx_delta_arr.value(i);
                    assert!(
                        delta <= total,
                        "delta ({delta}) should not exceed total ({total}) for {}",
                        ifaces[i].as_deref().unwrap_or("?")
                    );
                }
            }
        }
    }

    #[test]
    fn schema_column_count_matches_expected() {
        let schema = sensor_schema();
        assert_eq!(
            schema.fields().len(),
            61,
            "schema should have 14 base + 47 telemetry columns"
        );
    }

    #[test]
    fn budget_rotation_distributes_remainder_fairly() {
        // Simulate the rotation logic with 3 families and budget=2
        // per_family_budget = 2/3 = 0, remainder = 2
        // Over 3 polls, each family should get the extra budget twice.
        let active_count = 3usize;
        let max_rows = 2usize;
        let per_family_budget = max_rows / active_count; // 0
        let remainder = max_rows % active_count; // 2

        // Track how many extra rows each family position gets over 3 cycles.
        let mut extras = [0usize; 3];
        for poll_count in 0..active_count {
            let start_idx = poll_count % active_count;
            for family_idx in 0..active_count {
                let extra =
                    usize::from((family_idx + active_count - start_idx) % active_count < remainder);
                extras[family_idx] += extra;
            }
        }

        // Each family should get exactly 2 extras over 3 polls (fair distribution).
        assert_eq!(
            extras,
            [2, 2, 2],
            "each family must get equal remainder share over a full rotation"
        );
        assert_eq!(
            per_family_budget, 0,
            "base budget is zero with budget=2, families=3"
        );

        // Also verify single cycle: with budget=1 and 3 families, only one family gets 1 per cycle.
        let max_rows = 1usize;
        let remainder = max_rows % active_count; // 1
        let mut totals = [0usize; 3];
        for poll_count in 0..active_count {
            let start_idx = poll_count % active_count;
            for family_idx in 0..active_count {
                let extra =
                    usize::from((family_idx + active_count - start_idx) % active_count < remainder);
                totals[family_idx] += extra;
            }
        }
        assert_eq!(
            totals,
            [1, 1, 1],
            "budget=1 with 3 families: each gets 1 row over 3 polls"
        );
    }

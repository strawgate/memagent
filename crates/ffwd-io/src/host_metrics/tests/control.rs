    use super::*;
    use crate::atomic_write::atomic_write_file;
    use arrow::array::Array;
    use std::sync::Arc;

    mod tempfiles {
        use super::*;

        pub(super) fn control_file_path() -> (tempfile::TempDir, PathBuf) {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("sensor-control.json");
            (dir, path)
        }

        pub(super) fn write_control_file(path: &Path, json: serde_json::Value) {
            let bytes = serde_json::to_vec(&json).expect("serialize control file");
            atomic_write_file(path, &bytes).expect("write control file");
        }

        pub(super) fn write_malformed_control_file(path: &Path) {
            atomic_write_file(path, br#"{"generation":"invalid"}"#)
                .expect("write malformed control file");
        }
    }

    fn host_target() -> HostMetricsTarget {
        #[cfg(target_os = "linux")]
        {
            HostMetricsTarget::Linux
        }
        #[cfg(target_os = "macos")]
        {
            HostMetricsTarget::Macos
        }
        #[cfg(target_os = "windows")]
        {
            HostMetricsTarget::Windows
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            HostMetricsTarget::Unsupported
        }
    }

    fn non_host_target() -> HostMetricsTarget {
        #[cfg(target_os = "linux")]
        {
            HostMetricsTarget::Macos
        }
        #[cfg(target_os = "macos")]
        {
            HostMetricsTarget::Windows
        }
        #[cfg(target_os = "windows")]
        {
            HostMetricsTarget::Linux
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            HostMetricsTarget::Unsupported
        }
    }

    fn first_batch(events: &[SourceEvent]) -> &RecordBatch {
        match &events[0] {
            SourceEvent::Batch { batch, .. } => batch,
            _ => panic!("expected batch event"),
        }
    }

    fn string_col(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
        let idx = batch.schema().index_of(name).expect("column exists");
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).to_string())
                }
            })
            .collect()
    }

    fn u64_col(batch: &RecordBatch, name: &str) -> Vec<u64> {
        let idx = batch.schema().index_of(name).expect("column exists");
        let arr = batch
            .column(idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("u64 array");
        (0..arr.len()).map(|i| arr.value(i)).collect()
    }

    fn poll_until(
        input: &mut HostMetricsInput,
        timeout: Duration,
        predicate: impl Fn(&[SourceEvent]) -> bool,
    ) -> Vec<SourceEvent> {
        let start = Instant::now();
        loop {
            let events = input.poll().expect("poll should succeed");
            if predicate(&events) {
                return events;
            }
            assert!(
                start.elapsed() < timeout,
                "timed out waiting for host-metrics condition"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn rejects_non_matching_platform_target() {
        let err = HostMetricsInput::new("sensor", non_host_target(), HostMetricsConfig::default())
            .expect_err("non-matching target must fail");
        assert!(err.to_string().contains("can only run on"));
    }

    #[test]
    fn rejects_unknown_enabled_family() {
        let err = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["not_a_family".to_string()]),
                ..HostMetricsConfig::default()
            },
        )
        .expect_err("unknown family must fail");
        assert!(err.to_string().contains("unknown sensor family"));
    }

    #[test]
    fn emits_startup_batch_on_first_poll() {
        let mut input =
            HostMetricsInput::new("sensor", host_target(), HostMetricsConfig::default())
                .expect("host target should be valid");

        let events = input.poll().expect("poll should succeed");
        assert_eq!(events.len(), 1);

        let batch = first_batch(&events);
        assert!(batch.num_rows() >= 1);
        let kinds = string_col(batch, "event_kind");
        assert!(kinds.iter().any(|v| v.as_deref() == Some("startup")));
        assert!(
            string_col(batch, "event_family")
                .iter()
                .any(|v| v.as_deref() == Some("sensor_control"))
        );
    }

    #[test]
    fn signal_rows_disabled_suppresses_sample_placeholders() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                emit_signal_rows: false,
                poll_interval: Duration::from_millis(1),
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        let startup = input.poll().expect("startup poll");
        assert_eq!(startup.len(), 1);

        // Data collection rows may still appear, but "sample" placeholders must not.
        let events = poll_until(&mut input, Duration::from_millis(250), |_| true);
        if !events.is_empty() {
            let batch = first_batch(&events);
            let kinds = string_col(batch, "event_kind");
            assert!(
                !kinds.iter().any(|v| v.as_deref() == Some("sample")),
                "signal sample placeholders should be suppressed when emit_signal_rows is false"
            );
        }
    }

    #[test]
    fn periodic_signal_rows_emit_when_enabled() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                emit_signal_rows: true,
                poll_interval: Duration::from_millis(1),
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        let _ = input.poll().expect("startup poll");

        let events = poll_until(&mut input, Duration::from_millis(250), |events| !events.is_empty());
        assert_eq!(events.len(), 1, "second poll should emit sample rows");
        let batch = first_batch(&events);
        let kinds = string_col(batch, "event_kind");
        assert!(
            kinds.iter().any(|v| v.as_deref() == Some("sample")),
            "periodic sample rows should be emitted when signal rows are enabled"
        );
    }

    #[test]
    fn enabled_families_filter_signal_samples() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(vec!["process".to_string(), "dns".to_string()]),
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        let events = input.poll().expect("poll should succeed");
        let batch = first_batch(&events);
        let kinds = string_col(batch, "event_kind");
        let families = string_col(batch, "signal_family");

        // Process family now has real data collection (snapshot rows) instead of
        // signal sample placeholders. DNS still gets signal sample rows.
        let sample_families: Vec<String> = kinds
            .iter()
            .zip(families.iter())
            .filter_map(|(kind, family)| {
                if kind.as_deref() == Some("startup_sample") {
                    family.clone()
                } else {
                    None
                }
            })
            .collect();

        // dns is a placeholder-only family, so it should appear as startup_sample
        assert!(sample_families.iter().any(|f| f == "dns"));
        // process now uses real collection (snapshot), not startup_sample
        assert!(!sample_families.iter().any(|f| f == "process"));
        // network is not enabled, so it should not appear
        assert!(!sample_families.iter().any(|f| f == "network"));

        // Process data should appear as snapshot rows
        let event_families = string_col(batch, "event_family");
        assert!(
            event_families
                .iter()
                .any(|v| v.as_deref() == Some("process")),
            "process data collection rows should be in the startup batch"
        );
    }

    #[test]
    fn explicit_empty_enabled_families_disables_signal_samples() {
        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                enabled_families: Some(Vec::new()),
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        let events = input.poll().expect("poll should succeed");
        let batch = first_batch(&events);
        let kinds = string_col(batch, "event_kind");
        assert!(
            !kinds
                .iter()
                .any(|kind| kind.as_deref() == Some("startup_sample")),
            "empty enabled_families should disable signal sample rows",
        );
    }

    #[test]
    fn control_file_reload_updates_generation_and_families() {
        let (_dir, control_path) = tempfiles::control_file_path();

        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                control_path: Some(control_path.clone()),
                control_reload_interval: Duration::from_millis(1),
                poll_interval: Duration::from_secs(60),
                enabled_families: Some(vec!["process".to_string()]),
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        // startup
        assert_eq!(input.poll().expect("startup poll").len(), 1);

        tempfiles::write_control_file(
            &control_path,
            serde_json::json!({
                "generation": 42,
                "enabled_families": ["dns"],
                "emit_signal_rows": true,
            }),
        );
        let events = poll_until(&mut input, Duration::from_millis(250), |events| !events.is_empty());
        assert_eq!(events.len(), 1);
        let batch = first_batch(&events);

        let kinds = string_col(batch, "event_kind");
        assert!(
            kinds
                .iter()
                .any(|v| v.as_deref() == Some("control_reload_applied"))
        );

        let generations = u64_col(batch, "control_generation");
        assert!(generations.iter().all(|g| *g == 42));

        let families = string_col(batch, "signal_family");
        let reloaded_sample_families: Vec<String> = kinds
            .iter()
            .zip(families.iter())
            .filter_map(|(kind, family)| {
                if kind.as_deref() == Some("control_reload_sample") {
                    family.clone()
                } else {
                    None
                }
            })
            .collect();
        assert!(reloaded_sample_families.iter().any(|f| f == "dns"));
        assert!(!reloaded_sample_families.iter().any(|f| f == "process"));
    }

    #[test]
    fn control_file_ignores_unknown_keys() {
        let (_dir, control_path) = tempfiles::control_file_path();

        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                control_path: Some(control_path.clone()),
                control_reload_interval: Duration::from_millis(1),
                poll_interval: Duration::from_secs(60),
                enabled_families: Some(vec!["process".to_string()]),
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        // Startup poll primes the reload loop.
        assert_eq!(input.poll().expect("startup poll").len(), 1);

        // Control file carries a stale key (emit_heartbeat became a no-op) alongside
        // live keys — the reader must ignore the unknown key rather than abort.
        tempfiles::write_control_file(
            &control_path,
            serde_json::json!({
                "generation": 7,
                "enabled_families": ["process"],
                "emit_signal_rows": true,
                "emit_heartbeat": true,
            }),
        );
        let events = poll_until(&mut input, Duration::from_millis(250), |events| !events.is_empty());
        assert_eq!(events.len(), 1);
        let batch = first_batch(&events);
        let kinds = string_col(batch, "event_kind");
        assert!(
            kinds
                .iter()
                .any(|v| v.as_deref() == Some("control_reload_applied")),
            "stale control-file key must not block reload"
        );
        let generations = u64_col(batch, "control_generation");
        assert!(generations.iter().all(|g| *g == 7));
    }

    #[test]
    fn control_reload_same_generation_and_values_is_noop() {
        let (_dir, control_path) = tempfiles::control_file_path();

        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                control_path: Some(control_path.clone()),
                control_reload_interval: Duration::from_millis(1),
                poll_interval: Duration::from_secs(60),
                enabled_families: Some(vec!["process".to_string()]),
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        // startup
        assert_eq!(input.poll().expect("startup poll").len(), 1);

        tempfiles::write_control_file(
            &control_path,
            serde_json::json!({
                "generation": 1,
                "enabled_families": ["process"],
                "emit_signal_rows": true,
            }),
        );
        let start = Instant::now();
        loop {
            let events = input.poll().expect("poll should succeed");
            assert!(
                events.is_empty(),
                "unchanged control file should not emit reload-applied rows"
            );
            let Some(HostMetricsMachine::Running(state)) = input.machine.as_ref() else {
                panic!("input should remain in running state after no-op reload");
            };
            if state.state.control.source == ControlSource::ControlFile {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_millis(250),
                "timed out waiting for no-op reload bookkeeping"
            );
            std::thread::sleep(Duration::from_millis(5));
        }

        let running = match input.machine.as_ref() {
            Some(HostMetricsMachine::Running(state)) => state,
            _ => panic!("input should remain in running state after no-op reload"),
        };
        assert_eq!(
            running.state.control.source,
            ControlSource::ControlFile,
            "no-op reload should still record that control came from the file"
        );
    }

    #[test]
    fn malformed_control_file_does_not_fail_startup() {
        let (_dir, control_path) = tempfiles::control_file_path();
        tempfiles::write_malformed_control_file(&control_path);

        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                control_path: Some(control_path),
                control_reload_interval: Duration::from_millis(1),
                emit_signal_rows: false,
                ..HostMetricsConfig::default()
            },
        )
        .expect("startup should not fail on malformed optional control file");

        assert_eq!(input.poll().expect("startup poll").len(), 1);

        let events = poll_until(&mut input, Duration::from_millis(250), |events| !events.is_empty());
        assert_eq!(events.len(), 1);
        let batch = first_batch(&events);
        let kinds = string_col(batch, "event_kind");
        assert!(
            kinds
                .iter()
                .any(|v| v.as_deref() == Some("control_reload_failed")),
            "malformed control file should surface through reload failure rows"
        );
    }

    #[test]
    fn omitted_generation_advances_from_current_control_state() {
        let (_dir, control_path) = tempfiles::control_file_path();

        let mut input = HostMetricsInput::new(
            "sensor",
            host_target(),
            HostMetricsConfig {
                control_path: Some(control_path.clone()),
                control_reload_interval: Duration::from_millis(1),
                poll_interval: Duration::from_secs(60),
                enabled_families: Some(vec!["process".to_string()]),
                ..HostMetricsConfig::default()
            },
        )
        .expect("host target should be valid");

        assert_eq!(input.poll().expect("startup poll").len(), 1);

        tempfiles::write_control_file(
            &control_path,
            serde_json::json!({
                "generation": 5,
                "enabled_families": ["dns"],
                "emit_signal_rows": true,
            }),
        );
        let events =
            poll_until(&mut input, Duration::from_millis(250), |events| !events.is_empty());
        assert_eq!(events.len(), 1);

        tempfiles::write_control_file(
            &control_path,
            serde_json::json!({
                "enabled_families": ["process"],
                "emit_signal_rows": false,
            }),
        );
        let events =
            poll_until(&mut input, Duration::from_millis(250), |events| !events.is_empty());
        let batch = first_batch(&events);
        let generations = u64_col(batch, "control_generation");
        assert!(
            generations.iter().all(|generation| *generation == 6),
            "missing generation should advance from current state"
        );
    }

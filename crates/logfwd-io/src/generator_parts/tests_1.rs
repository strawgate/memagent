    use super::*;

    #[test]
    fn generator_health_is_explicitly_healthy() {
        let input = GeneratorInput::new("test", GeneratorConfig::default());
        assert_eq!(input.health(), ComponentHealth::Healthy);
    }

    /// Prove that the Arrow-direct generator produces identical data to the
    /// JSON → Scanner path across randomized inputs.
    ///
    /// Proptest varies counter offset, batch size, timestamp config, and
    /// message template. For each combination the test:
    /// 1. Generates N events via `generate_arrow_batch()`
    /// 2. Generates the same events as JSON via `write_log_fields_json()`
    /// 3. Scans the JSON through `Scanner::scan_detached()`
    /// 4. Asserts both RecordBatches are column-for-column identical
    /// 5. Asserts the JSON batch has no extra columns the Arrow batch lacks
    #[test]
    fn arrow_matches_json_path() {
        use arrow::array::AsArray;
        use logfwd_arrow::Scanner;
        use logfwd_core::cri::json_escape_bytes;
        use logfwd_core::scan_config::ScanConfig;
        use proptest::prelude::*;

        fn assert_batches_identical(
            arrow_batch: &RecordBatch,
            json_batch: &RecordBatch,
            label: &str,
        ) {
            assert_eq!(
                arrow_batch.num_rows(),
                json_batch.num_rows(),
                "{label}: row count mismatch"
            );
            let n = arrow_batch.num_rows();

            // Every column in the Arrow batch must exist and match in the JSON batch.
            let string_cols = ["timestamp", "level", "message", "request_id", "service"];
            let int_cols = ["duration_ms", "status"];

            for col_name in &string_cols {
                let arrow_col = arrow_batch
                    .column_by_name(col_name)
                    .unwrap_or_else(|| panic!("{label}: arrow batch missing {col_name}"));
                let json_col = json_batch
                    .column_by_name(col_name)
                    .unwrap_or_else(|| panic!("{label}: json batch missing {col_name}"));

                let arrow_strs = arrow_col.as_string::<i32>();
                let json_strs = json_col
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap_or_else(|| panic!("{label}: {col_name} not StringArray"));

                for row in 0..n {
                    assert_eq!(
                        arrow_strs.value(row),
                        json_strs.value(row),
                        "{label}: row={row} col={col_name}"
                    );
                }
            }

            for col_name in &int_cols {
                let arrow_col = arrow_batch
                    .column_by_name(col_name)
                    .unwrap_or_else(|| panic!("{label}: arrow batch missing {col_name}"));
                let json_col = json_batch
                    .column_by_name(col_name)
                    .unwrap_or_else(|| panic!("{label}: json batch missing {col_name}"));

                let arrow_ints = arrow_col
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap_or_else(|| panic!("{label}: {col_name} not Int64Array in arrow"));
                let json_ints = json_col
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap_or_else(|| panic!("{label}: {col_name} not Int64Array in json"));

                for row in 0..n {
                    assert_eq!(
                        arrow_ints.value(row),
                        json_ints.value(row),
                        "{label}: row={row} col={col_name}"
                    );
                }
            }

            // The JSON batch must not have extra columns beyond what Arrow produces.
            let arrow_schema = arrow_batch.schema();
            let arrow_names: std::collections::HashSet<&str> = arrow_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();
            for field in json_batch.schema().fields() {
                assert!(
                    arrow_names.contains(field.name().as_str()),
                    "{label}: json batch has extra column '{}' not in arrow batch",
                    field.name()
                );
            }
        }

        /// Compare Arrow-direct output against manually-constructed JSON→Scanner output.
        fn run_comparison(
            start: u64,
            n: usize,
            ts_config: &GeneratorTimestamp,
            raw_template: Option<&str>,
            label: &str,
        ) {
            // Arrow path: raw (un-escaped) template
            let arrow_batch = generate_arrow_batch(n, start, ts_config, raw_template);

            // JSON path: needs pre-escaped template for JSON embedding
            let escaped_template: Option<Vec<u8>> = raw_template.map(|raw| {
                let mut buf = Vec::with_capacity(raw.len() + 4);
                json_escape_bytes(raw.as_bytes(), &mut buf);
                buf
            });
            let escaped_slice = escaped_template.as_deref();

            let mut json_buf = Vec::with_capacity(n * 256);
            let mut json_count = 0;
            for i in 0..n {
                let counter = start + i as u64;
                let Some(fields) = compute_log_fields(
                    counter,
                    ts_config,
                    GeneratorComplexity::Simple,
                    escaped_slice,
                ) else {
                    break;
                };
                write_log_fields_json(&mut json_buf, &fields);
                json_buf.push(b'\n');
                json_count += 1;
            }

            if json_count == 0 {
                assert_eq!(
                    arrow_batch.num_rows(),
                    0,
                    "{label}: expected 0 rows when fields overflow"
                );
                return;
            }

            let config = ScanConfig {
                extract_all: true,
                ..Default::default()
            };
            let mut scanner = Scanner::new(config);
            let json_batch = scanner
                .scan_detached(Bytes::from(json_buf))
                .expect("valid JSON");

            assert_batches_identical(&arrow_batch, &json_batch, label);
        }

        // Proptest: randomized counter, timestamp config, and message template.
        proptest!(ProptestConfig::with_cases(200), |(
            start in 0u64..1_000_000,
            n in 1usize..500,
            start_epoch_ms in 0i64..2_000_000_000_000i64,
            step_ms in -100i64..100,
            use_template in proptest::bool::ANY,
        )| {
            let ts_config = GeneratorTimestamp { start_epoch_ms, step_ms };
            let template_str = "GET /api/test 200";
            let template = if use_template { Some(template_str) } else { None };
            let label = format!("proptest(start={start},n={n},epoch={start_epoch_ms},step={step_ms},tmpl={use_template})");
            run_comparison(start, n, &ts_config, template, &label);
        });

        // Also run deterministic edge cases: counter 0, max rotation coverage,
        // negative timestamp step, and large offsets.
        for (start, n, label) in [
            (0, 420, "all_rotations"), // LCM(4,5,4,3)=60 × 7 = covers all combos
            (u64::MAX - 10, 5, "near_u64_max"),
            (999_999, 200, "high_counter"),
        ] {
            run_comparison(start, n, &GeneratorTimestamp::default(), None, label);
        }

        // Negative step timestamp
        run_comparison(
            0,
            100,
            &GeneratorTimestamp {
                start_epoch_ms: 1_705_276_800_000,
                step_ms: -1,
            },
            None,
            "negative_step",
        );

        // Template with JSON-special characters (quotes, backslashes, newlines)
        run_comparison(
            0,
            50,
            &GeneratorTimestamp::default(),
            Some("hello \"world\" \n\t\\end"),
            "special_chars_template",
        );

        // Years outside the four-digit formatter range are intentionally not
        // generated by the fixed-width timestamp path.
        assert!(
            compute_log_fields(
                0,
                &GeneratorTimestamp {
                    start_epoch_ms: -100_000_000_000_000,
                    step_ms: 1,
                },
                GeneratorComplexity::Simple,
                None,
            )
            .is_none(),
            "generator should reject timestamps whose rendered year would be negative"
        );
    }

    /// Oracle test: use the real `GeneratorInput::poll()` → `Scanner` pipeline
    /// as ground truth, and verify `generate_arrow_batch()` matches it exactly.
    ///
    /// Unlike `arrow_matches_json_path` which reconstructs JSON via
    /// `write_log_fields_json()`, this test exercises the actual production
    /// JSON code path (`GeneratorInput::generate_batch` → `write_logs_event`).
    #[test]
    fn arrow_matches_real_generator_oracle() {
        use arrow::array::AsArray;
        use logfwd_arrow::Scanner;
        use logfwd_core::scan_config::ScanConfig;

        for (batch_size, total, label) in [
            (100, 100, "single_batch"),
            (50, 200, "multi_batch"),
            (1, 10, "single_event_batches"),
            (500, 500, "large_batch"),
        ] {
            let ts_config = GeneratorTimestamp::default();
            let config = GeneratorConfig {
                batch_size,
                total_events: total,
                events_per_sec: 0, // unlimited
                timestamp: ts_config,
                profile: GeneratorProfile::Logs,
                complexity: GeneratorComplexity::Simple,
                message_template: None,
                ..Default::default()
            };
            let mut generator = GeneratorInput::new("oracle-test", config);

            // Collect all JSON bytes from the real generator
            let mut all_json = Vec::new();
            loop {
                let events = generator.poll().expect("poll succeeded");
                if events.is_empty() {
                    break;
                }
                for event in events {
                    if let InputEvent::Data { bytes, .. } = event {
                        all_json.extend_from_slice(&bytes);
                    }
                }
            }
            assert!(!all_json.is_empty(), "{label}: generator produced no bytes");

            // Scan JSON through the real scanner
            let scan_config = ScanConfig {
                extract_all: true,
                ..Default::default()
            };
            let mut scanner = Scanner::new(scan_config);
            let json_batch = scanner
                .scan_detached(Bytes::from(all_json))
                .expect("valid JSON");

            // Generate Arrow-direct batch for the same events
            let arrow_batch = generate_arrow_batch(total as usize, 0, &ts_config, None);

            assert_eq!(
                arrow_batch.num_rows(),
                json_batch.num_rows(),
                "{label}: row count mismatch (arrow={} json={})",
                arrow_batch.num_rows(),
                json_batch.num_rows(),
            );

            let n = arrow_batch.num_rows();

            // Compare all string columns
            for col_name in &["timestamp", "level", "message", "request_id", "service"] {
                let arrow_col = arrow_batch
                    .column_by_name(col_name)
                    .unwrap_or_else(|| panic!("{label}: arrow missing {col_name}"));
                let json_col = json_batch
                    .column_by_name(col_name)
                    .unwrap_or_else(|| panic!("{label}: json missing {col_name}"));

                let arrow_strs = arrow_col.as_string::<i32>();
                let json_strs = json_col
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap_or_else(|| panic!("{label}: {col_name} not StringArray"));

                for row in 0..n {
                    assert_eq!(
                        arrow_strs.value(row),
                        json_strs.value(row),
                        "{label}: row={row} col={col_name}"
                    );
                }
            }

            // Compare all int columns
            for col_name in &["duration_ms", "status"] {
                let arrow_col = arrow_batch
                    .column_by_name(col_name)
                    .unwrap_or_else(|| panic!("{label}: arrow missing {col_name}"));
                let json_col = json_batch
                    .column_by_name(col_name)
                    .unwrap_or_else(|| panic!("{label}: json missing {col_name}"));

                let arrow_ints = arrow_col
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap_or_else(|| panic!("{label}: {col_name} not Int64Array in arrow"));
                let json_ints = json_col
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap_or_else(|| panic!("{label}: {col_name} not Int64Array in json"));

                for row in 0..n {
                    assert_eq!(
                        arrow_ints.value(row),
                        json_ints.value(row),
                        "{label}: row={row} col={col_name}"
                    );
                }
            }
        }
    }

    #[test]
    fn generates_valid_json_lines() {
        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 20,
                total_events: 20,
                ..Default::default()
            },
        );

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);

        if let InputEvent::Data { bytes, .. } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            let lines: Vec<&str> = text.trim().lines().collect();
            assert_eq!(lines.len(), 20);
            // Every line must parse as valid JSON.
            for (i, line) in lines.iter().enumerate() {
                assert!(
                    serde_json::from_str::<serde_json::Value>(line).is_ok(),
                    "line {i} is not valid JSON: {line}"
                );
            }
            assert!(lines[0].contains("\"level\":\"INFO\""));
            assert!(lines[0].contains("\"service\":"));
        } else {
            panic!("expected Data event");
        }

        // Should be done after total_events.
        let events = input.poll().unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn complex_generates_valid_json_lines() {
        let mut input = GeneratorInput::new(
            "test-complex",
            GeneratorConfig {
                batch_size: 50,
                total_events: 50,
                complexity: GeneratorComplexity::Complex,
                ..Default::default()
            },
        );

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);

        if let InputEvent::Data { bytes, .. } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            let lines: Vec<&str> = text.trim().lines().collect();
            assert_eq!(lines.len(), 50);
            let mut saw_nested = false;
            for (i, line) in lines.iter().enumerate() {
                let val: serde_json::Value = serde_json::from_str(line)
                    .unwrap_or_else(|e| panic!("line {i} invalid JSON: {e}\n{line}"));
                if val.get("headers").is_some() || val.get("upstream").is_some() {
                    saw_nested = true;
                }
            }
            assert!(saw_nested, "complex mode should produce nested objects");
        } else {
            panic!("expected Data event");
        }
    }

    #[test]
    fn rate_limited_returns_empty_when_called_too_soon() {
        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 10,
                events_per_sec: 1, // 1 event/sec => ~10s per batch of 10
                total_events: 0,
                ..Default::default()
            },
        );

        // First call succeeds.
        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);

        // Immediate second call should return empty (not block).
        let events = input.poll().unwrap();
        assert!(events.is_empty());
    }

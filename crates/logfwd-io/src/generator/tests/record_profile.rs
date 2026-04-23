use super::common::extract_timestamp;
use super::*;

#[test]
fn record_profile_emits_attributes_and_generated_fields() {
    let mut input = GeneratorInput::new(
        "bench",
        GeneratorConfig {
            batch_size: 3,
            total_events: 3,
            profile: GeneratorProfile::Record,
            attributes: HashMap::from([
                (
                    "benchmark_id".to_string(),
                    GeneratorAttributeValue::String("run-123".to_string()),
                ),
                (
                    "pod_name".to_string(),
                    GeneratorAttributeValue::String("emitter-0".to_string()),
                ),
                (
                    "stream_id".to_string(),
                    GeneratorAttributeValue::String("emitter-0".to_string()),
                ),
                (
                    "service".to_string(),
                    GeneratorAttributeValue::String("bench-emitter".to_string()),
                ),
                ("status".to_string(), GeneratorAttributeValue::Integer(200)),
                ("sampled".to_string(), GeneratorAttributeValue::Bool(true)),
            ]),
            sequence: Some(GeneratorGeneratedField {
                field: "seq".to_string(),
                start: 1,
            }),
            event_created_unix_nano_field: Some("event_created_unix_nano".to_string()),
            ..Default::default()
        },
    );

    let events = input.poll().unwrap();
    assert_eq!(events.len(), 1);
    let InputEvent::Data { bytes, .. } = &events[0] else {
        panic!("expected Data event");
    };
    let text = String::from_utf8_lossy(bytes);
    let lines: Vec<&str> = text.trim().lines().collect();
    assert_eq!(lines.len(), 3);
    let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(first["benchmark_id"], "run-123");
    assert_eq!(first["pod_name"], "emitter-0");
    assert_eq!(first["stream_id"], "emitter-0");
    assert_eq!(first["seq"], 1);
    assert_eq!(first["service"], "bench-emitter");
    assert_eq!(first["status"], 200);
    assert_eq!(first["sampled"], true);
    assert!(first.get("event_created_unix_nano").is_some());
}

#[test]
fn record_profile_supports_custom_sequence_start() {
    let mut input = GeneratorInput::new(
        "bench-input",
        GeneratorConfig {
            batch_size: 2,
            total_events: 2,
            profile: GeneratorProfile::Record,
            sequence: Some(GeneratorGeneratedField {
                field: "seq".to_string(),
                start: 10,
            }),
            ..Default::default()
        },
    );

    let events = input.poll().unwrap();
    let InputEvent::Data { bytes, .. } = &events[0] else {
        panic!("expected Data event");
    };
    let rows: Vec<serde_json::Value> = bytes
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    assert_eq!(rows[0]["seq"], 10);
    assert_eq!(rows[1]["seq"], 11);
}

#[test]
fn record_profile_stops_on_sequence_overflow() {
    let mut input = GeneratorInput::new(
        "bench-input",
        GeneratorConfig {
            batch_size: 2,
            total_events: 2,
            profile: GeneratorProfile::Record,
            sequence: Some(GeneratorGeneratedField {
                field: "seq".to_string(),
                start: u64::MAX,
            }),
            ..Default::default()
        },
    );

    let events = input.poll().unwrap();
    let InputEvent::Data { bytes, .. } = &events[0] else {
        panic!("expected Data event");
    };
    let rows: Vec<serde_json::Value> = bytes
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["seq"], u64::MAX);
    assert!(input.poll().unwrap().is_empty());
}

#[test]
fn record_profile_escapes_string_fields() {
    let mut input = GeneratorInput::new(
        "bench",
        GeneratorConfig {
            batch_size: 1,
            total_events: 1,
            profile: GeneratorProfile::Record,
            attributes: HashMap::from([
                (
                    "benchmark_id".to_string(),
                    GeneratorAttributeValue::String("run-\"quoted\"\nline".to_string()),
                ),
                (
                    "pod_name".to_string(),
                    GeneratorAttributeValue::String("pod-\\name".to_string()),
                ),
                (
                    "stream_id".to_string(),
                    GeneratorAttributeValue::String("stream-\"id\"".to_string()),
                ),
                (
                    "service".to_string(),
                    GeneratorAttributeValue::String("svc\tname".to_string()),
                ),
                ("ratio".to_string(), GeneratorAttributeValue::Float(1.25)),
                ("sampled".to_string(), GeneratorAttributeValue::Bool(false)),
            ]),
            ..Default::default()
        },
    );

    let events = input.poll().unwrap();
    let InputEvent::Data { bytes, .. } = &events[0] else {
        panic!("expected Data event");
    };
    let row: serde_json::Value =
        serde_json::from_slice(bytes.split(|b| *b == b'\n').next().unwrap()).unwrap();
    assert_eq!(row["benchmark_id"], "run-\"quoted\"\nline");
    assert_eq!(row["pod_name"], "pod-\\name");
    assert_eq!(row["stream_id"], "stream-\"id\"");
    assert_eq!(row["service"], "svc\tname");
    assert_eq!(row["ratio"], 1.25);
    assert_eq!(row["sampled"], false);
}

#[test]
fn encode_static_field_serializes_floats_as_json_numbers() {
    let encoded = encode_static_field("ratio", &GeneratorAttributeValue::Float(1.0));
    assert_eq!(std::str::from_utf8(&encoded).unwrap(), "\"ratio\":1.0");
}

#[test]
fn encode_static_field_serializes_non_finite_floats_as_null() {
    let encoded = encode_static_field("ratio", &GeneratorAttributeValue::Float(f64::NAN));
    assert_eq!(std::str::from_utf8(&encoded).unwrap(), "\"ratio\":null");
}

#[test]
fn encode_static_field_serializes_null_attribute() {
    let encoded = encode_static_field("deleted_at", &GeneratorAttributeValue::Null);
    assert_eq!(
        std::str::from_utf8(&encoded).unwrap(),
        "\"deleted_at\":null"
    );
}

// -----------------------------------------------------------------------
// Oracle & property-based tests for date math
// -----------------------------------------------------------------------

#[test]
fn epoch_ms_to_parts_matches_chrono() {
    use chrono::{DateTime, Datelike, Timelike};
    use proptest::prelude::*;

    let max_ms = 7_258_118_400_000_i64;
    proptest!(|(ms in 0_i64..max_ms)| {
        let (year, month, day, hour, min, sec, millis) = epoch_ms_to_parts(ms);
        let dt = DateTime::from_timestamp_millis(ms).unwrap();
        prop_assert_eq!(year, dt.year());
        prop_assert_eq!(month, dt.month());
        prop_assert_eq!(day, dt.day());
        prop_assert_eq!(hour, dt.hour());
        prop_assert_eq!(min, dt.minute());
        prop_assert_eq!(sec, dt.second());
        prop_assert_eq!(millis, dt.nanosecond() / 1_000_000);
    });
}

#[test]
fn epoch_ms_to_parts_negative_matches_chrono() {
    use chrono::{DateTime, Datelike, Timelike};
    use proptest::prelude::*;

    let min_ms = -2_208_988_800_000_i64;
    proptest!(|(ms in min_ms..0_i64)| {
        let (year, month, day, hour, min, sec, millis) = epoch_ms_to_parts(ms);
        let dt = DateTime::from_timestamp_millis(ms).unwrap();
        prop_assert_eq!(year, dt.year());
        prop_assert_eq!(month, dt.month());
        prop_assert_eq!(day, dt.day());
        prop_assert_eq!(hour, dt.hour());
        prop_assert_eq!(min, dt.minute());
        prop_assert_eq!(sec, dt.second());
        prop_assert_eq!(millis, dt.nanosecond() / 1_000_000);
    });
}

#[test]
fn civil_days_roundtrip() {
    use proptest::prelude::*;

    proptest!(|(y in 1_i32..3000, m in 1_u32..=12, d in 1_u32..=28)| {
        let days = days_from_civil(y, m, d);
        let (y2, m2, d2) = civil_from_days(days);
        prop_assert_eq!((y, m, d), (y2, m2, d2));
    });
}

#[test]
fn parse_iso8601_roundtrip_proptest() {
    use proptest::prelude::*;

    proptest!(|(
        y in 1970_i32..2200,
        m in 1_u32..=12,
        d in 1_u32..=28,
        h in 0_u32..=23,
        mi in 0_u32..=59,
        s in 0_u32..=59,
    )| {
        let input = format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z");
        let ms = parse_iso8601_to_epoch_ms(&input).unwrap();
        let (y2, m2, d2, h2, mi2, s2, ms2) = epoch_ms_to_parts(ms);
        prop_assert_eq!((y, m, d, h, mi, s, 0_u32), (y2, m2, d2, h2, mi2, s2, ms2));
    });
}

#[test]
fn timestamp_boundary_crossings() {
    use chrono::{DateTime, Datelike, Timelike};

    struct Case {
        label: &'static str,
        start: &'static str,
        step_ms: i64,
        count: usize,
        checks: Vec<(usize, &'static str)>,
    }

    let cases = [
        Case {
            label: "midnight crossing",
            start: "2024-01-15T23:59:59Z",
            step_ms: 1000,
            count: 5,
            checks: vec![(0, "2024-01-15T23:59:59"), (1, "2024-01-16T00:00:00")],
        },
        Case {
            label: "leap day crossing (2024)",
            start: "2024-02-28T23:59:58Z",
            step_ms: 1000,
            count: 5,
            checks: vec![(0, "2024-02-28T23:59:58"), (2, "2024-02-29T00:00:00")],
        },
        Case {
            label: "leap day to March",
            start: "2024-02-29T23:59:58Z",
            step_ms: 1000,
            count: 5,
            checks: vec![(0, "2024-02-29T23:59:58"), (2, "2024-03-01T00:00:00")],
        },
        Case {
            label: "non-leap Feb to March",
            start: "2023-02-28T23:59:58Z",
            step_ms: 1000,
            count: 5,
            checks: vec![(0, "2023-02-28T23:59:58"), (2, "2023-03-01T00:00:00")],
        },
        Case {
            label: "year-end crossing",
            start: "2024-12-31T23:59:58Z",
            step_ms: 1000,
            count: 5,
            checks: vec![(0, "2024-12-31T23:59:58"), (2, "2025-01-01T00:00:00")],
        },
        Case {
            label: "backward across midnight",
            start: "2024-01-16T00:00:02Z",
            step_ms: -1000,
            count: 5,
            checks: vec![(0, "2024-01-16T00:00:02"), (3, "2024-01-15T23:59:59")],
        },
    ];

    for case in &cases {
        let start_ms = parse_iso8601_to_epoch_ms(case.start).unwrap();
        let mut input = GeneratorInput::new(
            "boundary",
            GeneratorConfig {
                batch_size: case.count,
                total_events: case.count as u64,
                timestamp: GeneratorTimestamp {
                    start_epoch_ms: start_ms,
                    step_ms: case.step_ms,
                },
                ..Default::default()
            },
        );
        let events = input.poll().unwrap();
        let InputEvent::Data { bytes, .. } = &events[0] else {
            panic!("{}: expected Data event", case.label);
        };
        let text = String::from_utf8_lossy(bytes);
        let lines: Vec<&str> = text.trim().lines().collect();

        for &(idx, expected_prefix) in &case.checks {
            let ts = extract_timestamp(lines[idx]);
            assert!(
                ts.starts_with(expected_prefix),
                "{}: event {idx} timestamp {ts} does not start with {expected_prefix}",
                case.label
            );
        }

        // Verify every event against chrono as oracle
        for (i, line) in lines.iter().enumerate() {
            let ts = extract_timestamp(line);
            let event_ms = start_ms + (i as i64) * case.step_ms;
            let dt = DateTime::from_timestamp_millis(event_ms).unwrap();
            let expected = format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
                dt.year(),
                dt.month(),
                dt.day(),
                dt.hour(),
                dt.minute(),
                dt.second(),
                dt.nanosecond() / 1_000_000,
            );
            assert_eq!(ts, expected, "{}: event {i} mismatch", case.label);
        }
    }
}

#[test]
fn prop_positive_step_always_monotonic() {
    use proptest::prelude::*;

    proptest!(|(
        start_ms in 0_i64..4_102_444_800_000_i64,
        step_ms in 1_i64..10_000,
    )| {
        let mut input = GeneratorInput::new(
            "prop",
            GeneratorConfig {
                batch_size: 100,
                total_events: 100,
                timestamp: GeneratorTimestamp { start_epoch_ms: start_ms, step_ms },
                ..Default::default()
            },
        );
        let events = input.poll().unwrap();
        let InputEvent::Data { bytes, .. } = &events[0] else { panic!("expected Data"); };
        let text = String::from_utf8_lossy(bytes);
        let lines: Vec<&str> = text.trim().lines().collect();
        let timestamps: Vec<&str> = lines.iter().map(|l| extract_timestamp(l)).collect();
        for i in 1..timestamps.len() {
            prop_assert!(timestamps[i] > timestamps[i - 1]);
        }
    });
}

#[test]
fn prop_negative_step_always_decreasing() {
    use proptest::prelude::*;

    proptest!(|(
        start_ms in 1_000_000_000_i64..4_102_444_800_000_i64,
        step_ms in -10_000_i64..=-1,
    )| {
        let mut input = GeneratorInput::new(
            "prop",
            GeneratorConfig {
                batch_size: 100,
                total_events: 100,
                timestamp: GeneratorTimestamp { start_epoch_ms: start_ms, step_ms },
                ..Default::default()
            },
        );
        let events = input.poll().unwrap();
        let InputEvent::Data { bytes, .. } = &events[0] else { panic!("expected Data"); };
        let text = String::from_utf8_lossy(bytes);
        let lines: Vec<&str> = text.trim().lines().collect();
        let timestamps: Vec<&str> = lines.iter().map(|l| extract_timestamp(l)).collect();
        for i in 1..timestamps.len() {
            prop_assert!(timestamps[i] < timestamps[i - 1]);
        }
    });
}

#[test]
fn prop_json_valid_with_any_timestamp_config() {
    use proptest::prelude::*;

    proptest!(|(
        start_ms in 0_i64..4_102_444_800_000_i64,
        step_ms in -1000_i64..1000,
        count in 1_u64..50,
    )| {
        prop_assume!(step_ms != 0);
        let mut generator = GeneratorInput::new(
            "prop",
            GeneratorConfig {
                batch_size: count as usize,
                total_events: count,
                timestamp: GeneratorTimestamp { start_epoch_ms: start_ms, step_ms },
                ..Default::default()
            },
        );
        let events = generator.poll().unwrap();
        prop_assert_eq!(events.len(), 1);
        let InputEvent::Data { bytes, .. } = &events[0] else { panic!("expected Data"); };
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        for (i, line) in text.trim().lines().enumerate() {
            prop_assert!(
                serde_json::from_str::<serde_json::Value>(line).is_ok(),
                "invalid JSON at event {i}: {line}"
            );
        }
    });
}

/// Throughput comparison: Arrow-direct vs JSON→Scanner.
///
/// Not a Criterion bench — just a quick sanity check that the Arrow path
/// is significantly faster. Run with `--release --nocapture -- --ignored` to see numbers.
///
/// Ignored by default: wall-clock assertions are inherently flaky under CI
/// load. Run manually to verify performance claims.
#[test]
#[ignore = "wall-clock speedup assertion is flaky under CI load"]
fn arrow_vs_json_throughput() {
    use logfwd_arrow::Scanner;
    use logfwd_core::scan_config::ScanConfig;
    use std::time::Instant;

    let ts_config = GeneratorTimestamp::default();
    let message_template: Option<&str> = None;
    let n = 10_000;
    let rounds = 5;

    // Arrow-direct path
    let start = Instant::now();
    for r in 0..rounds {
        let batch = generate_arrow_batch(n, (r * n) as u64, &ts_config, message_template);
        std::hint::black_box(&batch);
    }
    let arrow_elapsed = start.elapsed();

    // JSON→Scanner path
    let config = ScanConfig {
        extract_all: true,
        ..Default::default()
    };
    let mut scanner = Scanner::new(config);
    let start = Instant::now();
    for r in 0..rounds {
        let mut json_buf = Vec::with_capacity(n * 256);
        for i in 0..n {
            let counter = (r * n) as u64 + i as u64;
            let fields = compute_log_fields(
                counter,
                &ts_config,
                GeneratorComplexity::Simple,
                message_template.map(str::as_bytes),
            )
            .expect("valid counter");
            write_log_fields_json(&mut json_buf, &fields);
            json_buf.push(b'\n');
        }
        let batch = scanner
            .scan_detached(Bytes::from(json_buf))
            .expect("valid JSON");
        std::hint::black_box(&batch);
    }
    let json_elapsed = start.elapsed();

    let total = (n * rounds) as f64;
    let arrow_eps = total / arrow_elapsed.as_secs_f64();
    let json_eps = total / json_elapsed.as_secs_f64();
    let speedup = arrow_eps / json_eps;

    eprintln!(
        "\n  Arrow direct: {:.2}M EPS ({:.1}ms)\n  JSON→Scanner: {:.2}M EPS ({:.1}ms)\n  Speedup:      {:.1}x\n",
        arrow_eps / 1e6,
        arrow_elapsed.as_secs_f64() * 1000.0,
        json_eps / 1e6,
        json_elapsed.as_secs_f64() * 1000.0,
        speedup,
    );

    // Arrow should be at least 2x faster in debug, much more in release
    assert!(
        speedup > 1.5,
        "Arrow path should be significantly faster (got {speedup:.1}x)"
    );
}

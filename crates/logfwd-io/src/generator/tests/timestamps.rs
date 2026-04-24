use super::common::extract_timestamp;
use super::*;

#[test]
fn rate_limited_can_emit_multiple_batches_per_poll() {
    let mut input = GeneratorInput::new(
        "test",
        GeneratorConfig {
            batch_size: 1_000,
            events_per_sec: 200_000,
            total_events: 5_000,
            ..Default::default()
        },
    );

    // Initial seeded credits emit one batch immediately.
    let first = input.poll().unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(input.events_generated(), 1_000);

    // Simulate a 20ms scheduler interval: 4k events should be due.
    input.last_refill = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_millis(20))
        .unwrap_or_else(std::time::Instant::now);

    let second = input.poll().unwrap();
    let emitted_rows: usize = second
        .iter()
        .map(|event| match event {
            InputEvent::Data { bytes, .. } => memchr::memchr_iter(b'\n', bytes).count(),
            _ => 0,
        })
        .sum();

    assert!(
        second.len() >= 2,
        "expected multiple batches in one poll at high EPS, got {}",
        second.len()
    );
    assert_eq!(emitted_rows, 4_000);
    assert_eq!(input.events_generated(), 5_000);
    assert!(input.poll().unwrap().is_empty());
}

#[test]
fn rate_limited_waits_for_a_full_batch_of_credit() {
    let mut input = GeneratorInput::new(
        "test",
        GeneratorConfig {
            batch_size: 1_000,
            events_per_sec: 500,
            total_events: 0,
            ..Default::default()
        },
    );

    let first = input.poll().unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(input.events_generated(), 1_000);

    input.last_refill = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_millis(500))
        .unwrap_or_else(std::time::Instant::now);
    assert!(input.poll().unwrap().is_empty());
    assert_eq!(input.events_generated(), 1_000);

    input.last_refill = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(2))
        .unwrap_or_else(std::time::Instant::now);
    let second = input.poll().unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(input.events_generated(), 2_000);
}

#[test]
fn rate_limited_discards_credit_beyond_burst_cap() {
    let mut input = GeneratorInput::new(
        "test",
        GeneratorConfig {
            batch_size: 1_000,
            events_per_sec: 5_000,
            total_events: 20_000,
            ..Default::default()
        },
    );

    let first = input.poll().unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(input.events_generated(), 1_000);

    input.last_refill = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(10))
        .unwrap_or_else(std::time::Instant::now);
    let second = input.poll().unwrap();
    let emitted_rows: usize = second
        .iter()
        .map(|event| match event {
            InputEvent::Data { bytes, .. } => memchr::memchr_iter(b'\n', bytes).count(),
            _ => 0,
        })
        .sum();
    assert_eq!(second.len(), 5);
    assert_eq!(emitted_rows, 5_000);
    assert_eq!(input.events_generated(), 6_000);

    assert!(input.poll().unwrap().is_empty());
    assert_eq!(input.events_generated(), 6_000);
}

#[test]
fn rate_limited_allows_final_partial_batch_for_finite_total_events() {
    let mut input = GeneratorInput::new(
        "test",
        GeneratorConfig {
            batch_size: 1_000,
            events_per_sec: 100,
            total_events: 1_500,
            ..Default::default()
        },
    );

    let first = input.poll().unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(input.events_generated(), 1_000);

    input.last_refill = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(5))
        .unwrap_or_else(std::time::Instant::now);
    let second = input.poll().unwrap();
    assert_eq!(second.len(), 1);
    let emitted_rows: usize = second
        .iter()
        .map(|event| match event {
            InputEvent::Data { bytes, .. } => memchr::memchr_iter(b'\n', bytes).count(),
            _ => 0,
        })
        .sum();
    assert_eq!(emitted_rows, 500);
    assert_eq!(input.events_generated(), 1_500);
    assert!(input.poll().unwrap().is_empty());
}

#[test]
fn unlimited_keeps_going() {
    let mut input = GeneratorInput::new(
        "test",
        GeneratorConfig {
            batch_size: 100,
            total_events: 0, // infinite
            ..Default::default()
        },
    );

    for _ in 0..5 {
        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);
    }
}

#[test]
fn timestamps_are_monotonically_increasing() {
    let mut input = GeneratorInput::new(
        "test",
        GeneratorConfig {
            batch_size: 2000,
            total_events: 2000,
            ..Default::default()
        },
    );

    let events = input.poll().unwrap();
    let InputEvent::Data { bytes, .. } = &events[0] else {
        panic!("expected Data event");
    };
    let text = String::from_utf8_lossy(bytes);
    let lines: Vec<&str> = text.trim().lines().collect();
    assert_eq!(lines.len(), 2000);

    let mut prev = String::new();
    for (i, line) in lines.iter().enumerate() {
        let ts = extract_timestamp(line);
        if i > 0 {
            assert!(
                ts > prev.as_str(),
                "timestamp at event {i} ({ts}) must be > previous ({prev})"
            );
        }
        prev = ts.to_string();
    }
}

#[test]
fn negative_step_produces_decreasing_timestamps() {
    let mut input = GeneratorInput::new(
        "test",
        GeneratorConfig {
            batch_size: 100,
            total_events: 100,
            timestamp: GeneratorTimestamp {
                start_epoch_ms: 1_705_276_800_000,
                step_ms: -1,
            },
            ..Default::default()
        },
    );

    let events = input.poll().unwrap();
    let InputEvent::Data { bytes, .. } = &events[0] else {
        panic!("expected Data event");
    };
    let text = String::from_utf8_lossy(bytes);
    let lines: Vec<&str> = text.trim().lines().collect();

    let first_ts = extract_timestamp(lines[0]);
    let last_ts = extract_timestamp(lines[99]);
    assert!(
        first_ts > last_ts,
        "negative step should decrease: {first_ts} vs {last_ts}"
    );
}

#[test]
fn custom_start_timestamp() {
    let mut input = GeneratorInput::new(
        "test",
        GeneratorConfig {
            batch_size: 1,
            total_events: 1,
            timestamp: GeneratorTimestamp {
                start_epoch_ms: 0,
                step_ms: 1,
            },
            ..Default::default()
        },
    );

    let events = input.poll().unwrap();
    let InputEvent::Data { bytes, .. } = &events[0] else {
        panic!("expected Data event");
    };
    let text = String::from_utf8_lossy(bytes);
    let line = text.trim().lines().next().unwrap();
    let ts = extract_timestamp(line);
    assert_eq!(ts, "1970-01-01T00:00:00.000Z");
}

#[test]
fn epoch_ms_to_parts_known_values() {
    let (y, mo, d, h, mi, s, ms) = epoch_ms_to_parts(1_705_276_800_000);
    assert_eq!((y, mo, d, h, mi, s, ms), (2024, 1, 15, 0, 0, 0, 0));

    let (y, mo, d, h, mi, s, ms) = epoch_ms_to_parts(0);
    assert_eq!((y, mo, d, h, mi, s, ms), (1970, 1, 1, 0, 0, 0, 0));

    let leap_ms =
        days_from_civil(2000, 2, 29) * 86_400_000 + 23 * 3_600_000 + 59 * 60_000 + 59 * 1000 + 999;
    let (y, mo, d, h, mi, s, ms) = epoch_ms_to_parts(leap_ms);
    assert_eq!((y, mo, d, h, mi, s, ms), (2000, 2, 29, 23, 59, 59, 999));
}

#[test]
fn parse_iso8601_roundtrip() {
    let ms = parse_iso8601_to_epoch_ms("2024-01-15T00:00:00Z").unwrap();
    assert_eq!(ms, 1_705_276_800_000);

    let ms = parse_iso8601_to_epoch_ms("1970-01-01T00:00:00Z").unwrap();
    assert_eq!(ms, 0);

    assert!(parse_iso8601_to_epoch_ms("not-a-date").is_err());
    assert!(parse_iso8601_to_epoch_ms("2024-13-01T00:00:00Z").is_err());
    assert!(parse_iso8601_to_epoch_ms("2024-02-31T00:00:00Z").is_err());
    assert!(parse_iso8601_to_epoch_ms("2023-02-29T00:00:00Z").is_err());
    assert!(parse_iso8601_to_epoch_ms("2024-02-29T00:00:00Z").is_ok());
}

#[test]
fn proptest_generated_json_always_valid() {
    // Inline proptest runner: validate JSON for a range of counter offsets
    // by skipping past initial batches to reach different counter values.
    use proptest::prelude::*;

    proptest!(|(offset in 0u64..1000)| {
        // We generate (offset + 1) events and check the last one.
        let total = offset + 1;
        let mut generator = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: total as usize,
                total_events: total,
                ..Default::default()
            },
        );
        let events = generator.poll().unwrap();
        assert_eq!(events.len(), 1, "poll() must produce exactly one Data event (offset={offset})");
        match &events[0] {
            InputEvent::Data { bytes, .. } => {
                assert!(!bytes.is_empty(), "generator produced empty data (offset={offset})");
                let text = String::from_utf8(bytes.to_vec()).unwrap();
                let line_count = text.trim().lines().count();
                assert!(line_count >= 1, "expected at least 1 JSON line, got 0 (offset={offset})");
                for (i, line) in text.trim().lines().enumerate() {
                    serde_json::from_str::<serde_json::Value>(line)
                        .unwrap_or_else(|e| panic!("invalid JSON at event {i} (offset={offset}): {e}\n{line}"));
                }
            }
            _ => panic!("unexpected event variant"),
        }
    });
}

#[test]
fn proptest_complex_json_always_valid() {
    use proptest::prelude::*;

    proptest!(|(offset in 0u64..500)| {
        let total = offset + 1;
        let mut generator = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: total as usize,
                total_events: total,
                complexity: GeneratorComplexity::Complex,
                ..Default::default()
            },
        );
        let events = generator.poll().unwrap();
        assert_eq!(events.len(), 1, "poll() must produce exactly one Data event (offset={offset})");
        match &events[0] {
            InputEvent::Data { bytes, .. } => {
                assert!(!bytes.is_empty(), "generator produced empty data (offset={offset})");
                let text = String::from_utf8(bytes.to_vec()).unwrap();
                let line_count = text.trim().lines().count();
                assert!(line_count >= 1, "expected at least 1 JSON line, got 0 (offset={offset})");
                for (i, line) in text.trim().lines().enumerate() {
                    serde_json::from_str::<serde_json::Value>(line)
                        .unwrap_or_else(|e| panic!("invalid JSON at event {i} (offset={offset}): {e}\n{line}"));
                }
            }
            _ => panic!("unexpected event variant"),
        }
    });
}

#[test]
fn message_template_with_special_chars_produces_valid_json() {
    // Regression test: message_template with quotes, backslashes, newlines,
    // and control chars must still produce valid JSON.
    let templates = [
        r#"quotes "here" and 'there'"#,
        "backslash \\ and tab \t inside",
        "newline\ninside template",
        "carriage\r\nreturn",
        "control\x01chars\x1f",
        r#"mixed "all" \types\ of 'special' chars"#,
    ];
    for tmpl in &templates {
        for complexity in [GeneratorComplexity::Simple, GeneratorComplexity::Complex] {
            let mut generator = GeneratorInput::new(
                "test",
                GeneratorConfig {
                    batch_size: 10,
                    total_events: 10,
                    complexity,
                    message_template: Some(tmpl.to_string()),
                    ..Default::default()
                },
            );
            let events = generator.poll().unwrap();
            let InputEvent::Data { bytes, .. } = &events[0] else {
                panic!("expected Data event");
            };
            let text = String::from_utf8(bytes.to_vec()).unwrap();
            for (i, line) in text.trim().lines().enumerate() {
                let parsed: serde_json::Value = serde_json::from_str(line).unwrap_or_else(
                        |e| {
                            panic!(
                                "invalid JSON at event {i} with template {tmpl:?} ({complexity:?}): {e}\n{line}"
                            )
                        },
                    );
                let msg = parsed["message"]
                    .as_str()
                    .expect("message field should be a string");
                assert_eq!(msg, *tmpl, "message content should match template");
            }
        }
    }
}

#[test]
fn events_generated_counter() {
    let mut input = GeneratorInput::new(
        "test",
        GeneratorConfig {
            batch_size: 10,
            total_events: 25,
            ..Default::default()
        },
    );
    assert_eq!(input.events_generated(), 0);
    let _ = input.poll().unwrap();
    assert_eq!(input.events_generated(), 10);
    let _ = input.poll().unwrap();
    assert_eq!(input.events_generated(), 20);
    let _ = input.poll().unwrap();
    assert_eq!(input.events_generated(), 25);
}

#[test]
fn generator_respects_total_events() {
    let mut input = GeneratorInput::new(
        "test",
        GeneratorConfig {
            batch_size: 7, // not a divisor of 50 — exercises partial-batch logic
            total_events: 50,
            ..Default::default()
        },
    );

    let mut total_lines = 0u64;
    loop {
        let events = input.poll().unwrap();
        if events.is_empty() {
            break;
        }
        for event in &events {
            if let InputEvent::Data { bytes, .. } = event {
                let text = String::from_utf8_lossy(bytes);
                total_lines += text.trim().lines().count() as u64;
            }
        }
    }

    assert_eq!(
        total_lines, 50,
        "expected exactly 50 events, got {total_lines}"
    );
    assert_eq!(input.events_generated(), 50);
    assert!(
        input.is_finished(),
        "finite generator should report completion"
    );

    // Subsequent polls must return empty.
    let events = input.poll().unwrap();
    assert!(events.is_empty(), "poll after completion must be empty");
}

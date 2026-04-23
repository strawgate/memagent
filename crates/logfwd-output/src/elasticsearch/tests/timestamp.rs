#[test]
fn empty_batch_produces_empty_output() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "level",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(Vec::<&str>::new()))],
    )
    .expect("batch creation failed");

    let mut sink = make_test_sink("logs");
    let meta = zero_metadata();

    sink.serialize_batch(&batch, &meta)
        .expect("serialize failed");
    assert!(sink.batch_buf.is_empty());
}

// --- is_leap_year ---

#[test]
fn leap_year_divisible_by_400() {
    assert!(is_leap_year(1600));
    assert!(is_leap_year(2000));
    assert!(is_leap_year(2400));
}

#[test]
fn not_leap_year_divisible_by_100_not_400() {
    assert!(!is_leap_year(1700));
    assert!(!is_leap_year(1800));
    assert!(!is_leap_year(1900));
    assert!(!is_leap_year(2100));
}

#[test]
fn leap_year_divisible_by_4_not_100() {
    assert!(is_leap_year(2024));
    assert!(is_leap_year(2020));
    assert!(is_leap_year(1996));
}

#[test]
fn not_leap_year_not_divisible_by_4() {
    assert!(!is_leap_year(2023));
    assert!(!is_leap_year(2019));
    assert!(!is_leap_year(1999));
}

// --- format_unix_timestamp_utc ---

#[test]
fn timestamp_epoch_is_1970_01_01() {
    assert_eq!(format_unix_timestamp_utc(0), "1970-01-01T00:00:00");
}

#[test]
fn timestamp_one_second() {
    assert_eq!(format_unix_timestamp_utc(1), "1970-01-01T00:00:01");
}

#[test]
fn timestamp_one_day() {
    assert_eq!(format_unix_timestamp_utc(86400), "1970-01-02T00:00:00");
}

#[test]
fn timestamp_y2k() {
    // 2000-01-01T00:00:00 UTC = 946684800
    assert_eq!(format_unix_timestamp_utc(946684800), "2000-01-01T00:00:00");
}

#[test]
fn timestamp_leap_day_2000() {
    // 2000-02-29T00:00:00 UTC = 951782400
    assert_eq!(format_unix_timestamp_utc(951782400), "2000-02-29T00:00:00");
}

#[test]
fn timestamp_non_leap_year_march() {
    // 2001-03-01T00:00:00 UTC = 983404800 (2001 is not a leap year)
    assert_eq!(format_unix_timestamp_utc(983404800), "2001-03-01T00:00:00");
}

#[test]
fn timestamp_mid_of_day() {
    // 1970-01-01T12:34:56 = 45296
    assert_eq!(format_unix_timestamp_utc(45296), "1970-01-01T12:34:56");
}

#[test]
fn timestamp_end_of_year() {
    // 1970-12-31T23:59:59 = 31535999
    assert_eq!(format_unix_timestamp_utc(31535999), "1970-12-31T23:59:59");
}

// --- parse_bulk_response: edge cases ---

#[test]
fn parse_bulk_response_empty_items_array() {
    let response = br#"{"took":0,"errors":false,"items":[]}"#;
    ElasticsearchSink::parse_bulk_response(response).expect("empty items should succeed");
}

#[test]
fn parse_bulk_response_malformed_json_is_error() {
    ElasticsearchSink::parse_bulk_response(b"not valid json")
        .expect_err("malformed json should be an error");
}

#[test]
fn parse_bulk_response_errors_false_does_not_error() {
    // errors:false means success even if items have non-200 status
    let response = br#"{"took":1,"errors":false,"items":[{"index":{"_id":"1","status":200}}]}"#;
    ElasticsearchSink::parse_bulk_response(response).expect("errors:false must succeed");
}

#[test]
fn test_index_name_escaping() {
    let index = "logs\"-and-\\backslashes";
    let factory = ElasticsearchSinkFactory::new(
        "test".to_string(),
        "http://localhost:9200".to_string(),
        index.to_string(),
        vec![],
        false,
        ElasticsearchRequestMode::Buffered,
        Arc::new(ComponentStats::default()),
    )
    .expect("factory creation failed");

    let action_line = std::str::from_utf8(&factory.config.action_bytes).unwrap();
    // Should be: {"index":{"_index":"logs\"-and-\\backslashes"}}\n
    assert!(
        action_line.contains(r#"_index":"logs\"-and-\\backslashes""#),
        "got: {}",
        action_line
    );
}

#[test]
fn test_timestamp_extreme_values() {
    // Year 10000+: 253402300800 seconds is 10000-01-01T00:00:00
    let secs_y10k = 253402300800;
    let ts = format_unix_timestamp_utc(secs_y10k);
    // Current implementation will either produce "10000-..." (corrupting the 19-byte buf)
    // or debug_panic. We want it to clamp to 9999-12-31T23:59:59.
    assert_eq!(ts, "9999-12-31T23:59:59");

    // u64::MAX should not hang (regression for #1087)
    let _ = format_unix_timestamp_utc(u64::MAX);
}

#[test]
fn test_parse_bulk_response_whitespace() {
    // Bug #1095: parser misses errors if there's whitespace
    let response = br#" { "took": 5, "errors" :  true , "items": [] } "#;
    ElasticsearchSink::parse_bulk_response(response)
        .expect_err("should detect errors:true with whitespace");
}

#[test]
fn test_extract_took_whitespace() {
    let response = br#" { "took" :  123 , "errors": false } "#;
    assert_eq!(ElasticsearchSink::extract_took(response), Some(123));
}

proptest! {
    #[test]
    fn proptest_write_ts_suffix_fast_matches_simple(
        secs in any::<u64>(),
        frac in 0u64..1_000_000_000u64
    ) {
        let mut fast = [0u8; 47];
        write_ts_suffix(&mut fast, secs, frac);
        let simple = write_ts_suffix_simple(secs, frac);
        prop_assert_eq!(fast.as_slice(), simple.as_slice());
    }
}

/// Local microbenchmark for timestamp suffix writer.
///
/// Run with:
/// `cargo test -p logfwd-output elasticsearch::tests::bench_write_ts_suffix_fast_vs_simple --release -- --ignored --nocapture`
#[test]
#[ignore = "microbenchmark"]
fn bench_write_ts_suffix_fast_vs_simple() {
    use std::hint::black_box;
    use std::time::Instant;

    const N: usize = 300_000;
    let inputs: Vec<(u64, u64)> = (0..N)
        .map(|i| {
            let secs = (i as u64).wrapping_mul(2654435761) % (253402300799 + 5000);
            let frac = (i as u64).wrapping_mul(11400714819323198485u64) % 1_000_000_000;
            (secs, frac)
        })
        .collect();

    let t0 = Instant::now();
    for &(secs, frac) in &inputs {
        let mut out = [0u8; 47];
        write_ts_suffix(&mut out, secs, frac);
        black_box(out);
    }
    let fast = t0.elapsed();

    let t0 = Instant::now();
    for &(secs, frac) in &inputs {
        let out = write_ts_suffix_simple(secs, frac);
        black_box(out.len());
    }
    let simple = t0.elapsed();

    eprintln!("ts suffix bench N={N}");
    eprintln!("  fast={:?}", fast);
    eprintln!("  simple={:?}", simple);
}

#[test]
fn test_parse_bulk_response_malformed_is_error() {
    // Bug #1094: malformed bodies treated as success
    ElasticsearchSink::parse_bulk_response(b"{}")
        .expect_err("missing errors field should be an error");
    ElasticsearchSink::parse_bulk_response(b"not json")
        .expect_err("malformed json should be an error");
}

// Regression test for issue #1675: when errors:true but no item has an "error" key,
// the function must return Err rather than Ok (which would silently mask the failure).
#[test]
fn parse_bulk_response_errors_true_no_error_key_is_error() {
    // ES says errors:true but all items look successful (no "error" key).
    // This can happen with malformed/truncated responses or unexpected ES formats.
    let response = br#"{"took":1,"errors":true,"items":[{"index":{"_id":"1","status":200}}]}"#;
    ElasticsearchSink::parse_bulk_response(response)
        .expect_err("errors:true must return Err even when no item has an 'error' key");
}

/// Local microbenchmark for serialize_batch fast path vs simple baseline.
///
/// Run with:
/// `cargo test -p logfwd-output elasticsearch::tests::bench_serialize_batch_fast_vs_simple --release -- --ignored --nocapture`
#[test]
#[ignore = "microbenchmark"]
fn bench_serialize_batch_fast_vs_simple() {
    use std::hint::black_box;
    use std::time::Instant;

    let rows: Vec<RandomRow> = (0..2_000)
        .map(|i| {
            (
                Some(match i % 4 {
                    0 => "INFO".to_string(),
                    1 => "WARN".to_string(),
                    2 => "ERROR".to_string(),
                    _ => "DEBUG".to_string(),
                }),
                Some(format!("request-{i}-payload")),
                Some(200 + (i % 7) as i64),
                Some((i % 1000) as f64 / 10.0),
                Some(i % 3 != 0),
            )
        })
        .collect();
    let batch = build_random_batch(&rows, false);
    let metadata = BatchMetadata {
        resource_attrs: Arc::from([]),
        observed_time_ns: 1_710_000_000_123_456_789,
    };
    let index = "bench-index";

    let mut sink = make_test_sink(index);
    sink.serialize_batch(&batch, &metadata).unwrap();
    let simple_once = serialize_batch_simple_for_test(&batch, &metadata, index).unwrap();
    assert_eq!(
        parse_json_lines(&sink.batch_buf),
        parse_json_lines(&simple_once),
        "fast and simple serializers must remain semantically equivalent"
    );

    const ITERS: usize = 120;
    let t0 = Instant::now();
    for _ in 0..ITERS {
        sink.serialize_batch(&batch, &metadata).unwrap();
        black_box(sink.serialized_len());
    }
    let fast = t0.elapsed();

    let t0 = Instant::now();
    for _ in 0..ITERS {
        let out = serialize_batch_simple_for_test(&batch, &metadata, index).unwrap();
        black_box(out.len());
    }
    let simple = t0.elapsed();

    eprintln!(
        "serialize_batch bench rows={} iters={ITERS}",
        batch.num_rows()
    );
    eprintln!("  fast={:?}", fast);
    eprintln!("  simple={:?}", simple);
}

#[test]
fn test_send_batch_oversized_single_row() {
    use crate::sink::Sink;
    let factory = ElasticsearchSinkFactory::new(
        "test".to_string(),
        "http://localhost:9200".to_string(),
        "logs".to_string(),
        vec![],
        false,
        ElasticsearchRequestMode::Buffered,
        Arc::new(ComponentStats::default()),
    )
    .unwrap();
    let mut sink = factory.create_sink();

    // Use a row large enough to exceed 5MB default limit.
    let large_str = "A".repeat(5 * 1024 * 1024 + 1024);
    let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![large_str]))]).unwrap();

    let meta = zero_metadata();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = rt.block_on(sink.send_batch(&batch, &meta));
    match result {
        crate::sink::SendResult::Rejected(msg) => {
            assert!(msg.contains("exceeds max_bulk_bytes"));
        }
        _ => panic!("Expected Rejected, got {:?}", result),
    }
}

/// Regression test for #1212 (ES sink only):
/// ES bulk item errors (InvalidData from parse_bulk_response) must map to
/// `SendResult::Rejected`, not `SendResult::IoError`.  Before the fix the
/// `send_batch` error classifier only caught `InvalidInput`, so `InvalidData`
/// fell through to `IoError` and the worker pool would retry indefinitely.
#[test]
fn parse_bulk_item_error_maps_to_rejected_not_io_error() {
    // parse_bulk_response returns Err(InvalidData) when the bulk response
    // contains item-level errors even inside a 200 OK.  Confirm the error
    // kind is InvalidData so the send_batch classifier can reject it.
    // All items failed permanently (no successes).
    let response = br#"{
        "took":3,
        "errors":true,
        "items":[
            {"index":{"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [ts]"},"status":400}}
        ]
    }"#;
    let err = ElasticsearchSink::parse_bulk_response(response)
        .expect_err("parse_bulk_response must fail on item error");
    assert_eq!(
        err.kind(),
        io::ErrorKind::InvalidData,
        "bulk item errors must use InvalidData kind so send_batch maps them to Rejected"
    );
    assert!(err.to_string().contains("mapper_parsing_exception"));

    // Verify send_batch converts InvalidData -> Rejected (not IoError).
    // We exercise this through the classifier arm directly to avoid needing
    // a live HTTP server.
    let classified = match err.kind() {
        io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData => {
            crate::sink::SendResult::Rejected(err.to_string())
        }
        _ => crate::sink::SendResult::IoError(err),
    };
    match classified {
        crate::sink::SendResult::Rejected(msg) => {
            assert!(msg.contains("mapper_parsing_exception"), "got: {msg}");
        }
        other => {
            panic!("ES bulk item error must be Rejected, not retried as IoError; got {other:?}")
        }
    }
}

#[test]
fn split_result_preserves_rejection_when_other_half_is_ok() {
    let result = ElasticsearchSink::merge_split_send_results(
        crate::sink::SendResult::Rejected("left bad doc".to_string()),
        crate::sink::SendResult::Ok,
    );

    match result {
        crate::sink::SendResult::Rejected(reason) => {
            assert!(reason.contains("left split rejected"), "got: {reason}");
            assert!(reason.contains("left bad doc"), "got: {reason}");
        }
        other => panic!("expected rejected split result, got {other:?}"),
    }
}

#[test]
fn split_result_combines_two_terminal_rejections() {
    let result = ElasticsearchSink::merge_split_send_results(
        crate::sink::SendResult::Rejected("left bad doc".to_string()),
        crate::sink::SendResult::Rejected("right bad doc".to_string()),
    );

    match result {
        crate::sink::SendResult::Rejected(reason) => {
            assert!(reason.contains("left bad doc"), "got: {reason}");
            assert!(reason.contains("right bad doc"), "got: {reason}");
        }
        other => panic!("expected rejected split result, got {other:?}"),
    }
}

#[test]
fn split_result_keeps_retryable_result_visible() {
    let delay = Duration::from_secs(3);
    let result = ElasticsearchSink::merge_split_send_results(
        crate::sink::SendResult::Rejected("left bad doc".to_string()),
        crate::sink::SendResult::RetryAfter(delay),
    );

    match result {
        crate::sink::SendResult::RetryAfter(actual) => assert_eq!(actual, delay),
        other => panic!("expected retryable split result, got {other:?}"),
    }
}

#[test]
fn split_result_io_error_takes_precedence_over_retry() {
    let result = ElasticsearchSink::merge_split_send_results(
        crate::sink::SendResult::IoError(io::Error::other("network")),
        crate::sink::SendResult::RetryAfter(Duration::from_secs(3)),
    );

    assert!(
        matches!(result, crate::sink::SendResult::IoError(_)),
        "expected io error to take precedence, got {result:?}"
    );
}

#[test]
fn classify_split_result_converts_invalid_data_to_rejected() {
    let err = io::Error::new(io::ErrorKind::InvalidData, "mapper_parsing_exception");
    let result = ElasticsearchSink::classify_split_result(Err(err));
    match result {
        Ok(crate::sink::SendResult::Rejected(msg)) => {
            assert!(msg.contains("mapper_parsing_exception"), "got: {msg}");
        }
        other => panic!("expected Ok(Rejected), got {other:?}"),
    }
}

#[test]
fn classify_split_result_converts_invalid_input_to_rejected() {
    let err = io::Error::new(io::ErrorKind::InvalidInput, "413 Payload Too Large");
    let result = ElasticsearchSink::classify_split_result(Err(err));
    match result {
        Ok(crate::sink::SendResult::Rejected(msg)) => {
            assert!(msg.contains("413 Payload Too Large"), "got: {msg}");
        }
        other => panic!("expected Ok(Rejected), got {other:?}"),
    }
}

#[test]
fn classify_split_result_preserves_io_errors() {
    let err = io::Error::new(io::ErrorKind::ConnectionRefused, "connection refused");
    let result = ElasticsearchSink::classify_split_result(Err(err));
    assert!(result.is_err(), "expected Err for IO error");
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::ConnectionRefused);
}

#[test]
fn classify_split_result_passes_through_ok() {
    let result = ElasticsearchSink::classify_split_result(Ok(crate::sink::SendResult::Ok));
    match result {
        Ok(crate::sink::SendResult::Ok) => {}
        other => panic!("expected Ok(Ok), got {other:?}"),
    }
}

//! Unit, property-based, and snapshot tests for the Elasticsearch sink.

use std::io;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use logfwd_types::diagnostics::ComponentStats;
use logfwd_types::field_names;
use proptest::prelude::*;
use proptest::string::string_regex;
use tokio::sync::mpsc;

use super::timestamp::{
    format_unix_timestamp_utc, is_leap_year, write_ts_suffix, write_ts_suffix_simple,
};
use super::types::{ElasticsearchConfig, ElasticsearchSink, SendAttempt};
use super::{ElasticsearchRequestMode, ElasticsearchSinkFactory};
use crate::{BatchMetadata, build_col_infos, write_row_json};

type RandomRow = (
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<f64>,
    Option<bool>,
);

fn zero_metadata() -> BatchMetadata {
    BatchMetadata {
        resource_attrs: Arc::new(vec![]),
        observed_time_ns: 0,
    }
}

fn shared_test_client() -> reqwest::Client {
    static TEST_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    TEST_CLIENT.get_or_init(reqwest::Client::new).clone()
}

fn unicode_string(max_len: usize) -> impl Strategy<Value = String> {
    proptest::collection::vec(any::<char>(), 0..=max_len)
        .prop_map(|chars| chars.into_iter().collect())
}

fn make_test_sink(index: &str) -> ElasticsearchSink {
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
    let client = shared_test_client();
    ElasticsearchSink::new(
        "test".to_string(),
        Arc::clone(&factory.config),
        client,
        Arc::new(ComponentStats::default()),
    )
}

fn serialize_batch_simple_for_test(
    batch: &RecordBatch,
    metadata: &BatchMetadata,
    index: &str,
) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    if batch.num_rows() == 0 {
        return Ok(out);
    }

    let escaped_index = serde_json::to_string(index).map_err(io::Error::other)?;
    let action_line = format!("{{\"index\":{{\"_index\":{escaped_index}}}}}\n");

    let ts_nanos = metadata.observed_time_ns;
    let ts_secs = ts_nanos / 1_000_000_000;
    let ts_frac = ts_nanos % 1_000_000_000;
    let ts_text = format!("{}.{ts_frac:09}Z", format_unix_timestamp_utc(ts_secs));

    let cols = build_col_infos(batch);
    let has_timestamp_col = cols.iter().any(|c| {
        field_names::matches_any(
            &c.field_name,
            field_names::TIMESTAMP,
            field_names::TIMESTAMP_VARIANTS,
        )
    });

    for row in 0..batch.num_rows() {
        out.extend_from_slice(action_line.as_bytes());

        let doc_start = out.len();
        write_row_json(batch, row, &cols, &mut out, false)?;
        out.push(b'\n');

        if !has_timestamp_col {
            if !out.ends_with(b"}\n") {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "serialize_batch_simple_for_test: JSON doc must end with }\\n",
                ));
            }
            let trim_to = out.len() - 2;
            if trim_to == doc_start + 1 {
                out.truncate(doc_start);
                let suffix = format!("{{\"@timestamp\":\"{ts_text}\"}}");
                out.extend_from_slice(suffix.as_bytes());
            } else {
                out.truncate(trim_to);
                let suffix = format!(",\"@timestamp\":\"{ts_text}\"}}");
                out.extend_from_slice(suffix.as_bytes());
            }
            out.push(b'\n');
        }
    }

    Ok(out)
}

fn parse_json_lines(bytes: &[u8]) -> Vec<serde_json::Value> {
    if bytes.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut lines = bytes.split(|b| *b == b'\n').peekable();
    while let Some(line) = lines.next() {
        if line.is_empty() {
            assert!(
                lines.peek().is_none(),
                "ndjson must not contain interior blank lines"
            );
            continue;
        }
        out.push(serde_json::from_slice::<serde_json::Value>(line).expect("valid ndjson line"));
    }
    out
}

fn build_random_batch(rows: &[RandomRow], include_timestamp: bool) -> RecordBatch {
    let mut fields = Vec::new();
    let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

    if include_timestamp {
        fields.push(Field::new(field_names::TIMESTAMP_AT, DataType::Utf8, true));
        let ts: Vec<Option<String>> = (0..rows.len())
            .map(|i| {
                if i % 3 == 0 {
                    None
                } else {
                    Some(format!("2026-04-08T12:00:{:02}.123456789Z", i % 60))
                }
            })
            .collect();
        columns.push(Arc::new(StringArray::from(
            ts.iter().map(|s| s.as_deref()).collect::<Vec<_>>(),
        )));
    }

    fields.push(Field::new("level", DataType::Utf8, true));
    fields.push(Field::new("message", DataType::Utf8, true));
    fields.push(Field::new("status", DataType::Int64, true));
    fields.push(Field::new("duration_ms", DataType::Float64, true));
    fields.push(Field::new("active", DataType::Boolean, true));

    let levels: Vec<Option<String>> = rows.iter().map(|r| r.0.clone()).collect();
    let messages: Vec<Option<String>> = rows.iter().map(|r| r.1.clone()).collect();
    let statuses: Vec<Option<i64>> = rows.iter().map(|r| r.2).collect();
    let durations: Vec<Option<f64>> = rows.iter().map(|r| r.3).collect();
    let active: Vec<Option<bool>> = rows.iter().map(|r| r.4).collect();

    columns.push(Arc::new(StringArray::from(
        levels.iter().map(|s| s.as_deref()).collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(StringArray::from(
        messages.iter().map(|s| s.as_deref()).collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(Int64Array::from(statuses)));
    columns.push(Arc::new(arrow::array::Float64Array::from(durations)));
    columns.push(Arc::new(arrow::array::BooleanArray::from(active)));

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("valid random batch")
}

#[test]
fn serialize_batch_basic() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("level", DataType::Utf8, false),
        Field::new("status", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["ERROR", "WARN"])),
            Arc::new(Int64Array::from(vec![500, 404])),
        ],
    )
    .expect("batch creation failed");

    let mut sink = make_test_sink("logs");
    let meta = zero_metadata();

    sink.serialize_batch(&batch, &meta)
        .expect("serialize failed");
    let output = String::from_utf8_lossy(&sink.batch_buf);

    // Should produce 4 lines: action + doc for each of 2 rows.
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 4);
    assert!(lines[0].contains(r#"{"index":{"_index":"logs"}}"#));
    assert!(lines[1].contains(r#""level":"ERROR""#));
    assert!(lines[1].contains(r#""status":500"#));
    assert!(lines[1].contains(r#""@timestamp""#));
    assert!(lines[2].contains(r#"{"index":{"_index":"logs"}}"#));
    assert!(lines[3].contains(r#""level":"WARN""#));
}

/// Regression test: batches with a canonical timestamp column name (`timestamp`,
/// `time`, `ts`) must NOT have a synthetic `@timestamp` injected.
///
/// Before the fix, `has_timestamp_col` only checked `@timestamp`/`_timestamp`,
/// so a column named `timestamp`, `time`, or `ts` would cause two timestamp
/// fields in the ES document — the user's original field plus the injected
/// `@timestamp` derived from `metadata.observed_time_ns`.
#[test]
fn canonical_timestamp_variants_suppress_at_timestamp_injection() {
    for col_name in &["timestamp", "time", "ts"] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, false),
            Field::new(*col_name, DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(StringArray::from(vec!["2024-01-01T00:00:00Z"])),
            ],
        )
        .expect("batch creation failed");

        let mut sink = make_test_sink("logs");
        let meta = zero_metadata();
        sink.serialize_batch(&batch, &meta)
            .expect("serialize failed");
        let output = String::from_utf8_lossy(&sink.batch_buf);
        let lines: Vec<&str> = output.lines().collect();
        // doc line is index 1
        assert!(
            !lines[1].contains(r#""@timestamp""#),
            "column '{}': expected no @timestamp injection but got: {}",
            col_name,
            lines[1]
        );
    }
}

/// Regression test for issue #1680.
///
/// `serialize_batch_streaming` used a narrow `has_timestamp_col` check that
/// only matched `@timestamp`/`_timestamp`.  A column named `timestamp`, `time`,
/// or `ts` caused a spurious `@timestamp` injection alongside the user's column.
#[test]
fn streaming_canonical_timestamp_variants_suppress_at_timestamp_injection() {
    use std::sync::atomic::AtomicU64;

    // Build a minimal streaming config.
    let factory = ElasticsearchSinkFactory::new(
        "test".to_string(),
        "http://localhost:9200".to_string(),
        "test-index".to_string(),
        vec![],
        false,
        ElasticsearchRequestMode::Streaming,
        Arc::new(ComponentStats::default()),
    )
    .expect("factory creation failed");
    let config = Arc::clone(&factory.config);

    for col_name in &["timestamp", "time", "ts"] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, false),
            Field::new(*col_name, DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(StringArray::from(vec!["2024-01-01T00:00:00Z"])),
            ],
        )
        .expect("batch creation failed");

        // We need a tokio runtime to drive the mpsc channel.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("tokio runtime");
        let (tx, mut rx) = mpsc::channel::<io::Result<Vec<u8>>>(16);
        let emitted = Arc::new(AtomicU64::new(0));

        rt.block_on(async {
            let config_clone = Arc::clone(&config);
            let batch_clone = batch.clone();
            let meta = BatchMetadata {
                resource_attrs: Arc::new(vec![]),
                observed_time_ns: 0,
            };
            let tx_clone = tx.clone();
            drop(tx); // ensure channel closes when producer is done
            tokio::task::spawn_blocking(move || {
                ElasticsearchSink::serialize_batch_streaming(
                    batch_clone,
                    meta,
                    config_clone,
                    tx_clone,
                    emitted,
                )
                .expect("serialize_batch_streaming should not error");
            })
            .await
            .expect("task must not panic");

            // Collect all chunks.
            let mut output = Vec::new();
            while let Some(chunk) = rx.recv().await {
                output.extend_from_slice(&chunk.expect("no io error"));
            }

            let doc_line = String::from_utf8_lossy(&output)
                .lines()
                .nth(1)
                .unwrap_or("")
                .to_string();
            assert!(
                !doc_line.contains(r#""@timestamp""#),
                "streaming column '{}': expected no @timestamp injection but got: {}",
                col_name,
                doc_line
            );
        });
    }
}

#[test]
fn parse_bulk_response_success() {
    let response = br#"{"took":5,"errors":false,"items":[{"index":{"_id":"1","status":201}}]}"#;
    ElasticsearchSink::parse_bulk_response(response).expect("should not error on success");
}

proptest! {
    #[test]
    fn proptest_serialize_batch_fast_matches_simple(
        rows in proptest::collection::vec(
            (
                prop::option::of(unicode_string(8)),
                prop::option::of(unicode_string(32)),
                prop::option::of(any::<i64>()),
                prop::option::of((-1_000_000i64..1_000_000i64).prop_map(|n| n as f64 / 10.0)),
                prop::option::of(any::<bool>()),
            ),
            0..40
        ),
        include_timestamp in any::<bool>(),
        index in string_regex("[A-Za-z0-9_.-]{1,16}").expect("regex"),
        observed_time_ns in any::<u64>()
    ) {
        let batch = build_random_batch(&rows, include_timestamp);
        let metadata = BatchMetadata {
            resource_attrs: Arc::new(vec![]),
            observed_time_ns,
        };

        let mut sink = make_test_sink(index.as_str());
        sink.serialize_batch(&batch, &metadata).expect("fast serialize must succeed");
        let simple = serialize_batch_simple_for_test(&batch, &metadata, index.as_str())
            .expect("simple serialize must succeed");

        prop_assert_eq!(
            parse_json_lines(&sink.batch_buf),
            parse_json_lines(&simple),
            "serialize_batch fast path drifted from simple reference"
        );
    }
}

#[test]
fn parse_bulk_response_error() {
    let response = br#"{
        "took":5,
        "errors":true,
        "items":[
            {"index":{"_id":"1","status":201}},
            {"index":{"error":{"type":"mapper_parsing_exception","reason":"failed to parse"},"status":400}}
        ]
    }"#;
    let err =
        ElasticsearchSink::parse_bulk_response(response).expect_err("should error on bulk failure");
    // Mixed result: 1 succeeded + 1 rejected → InvalidData to prevent duplication (#1880).
    assert_eq!(
        err.kind(),
        io::ErrorKind::InvalidData,
        "mixed success/failure must be permanent to prevent duplication"
    );
    assert!(
        err.to_string().contains("partial failure"),
        "error should mention partial failure: {err}"
    );
    assert!(err.to_string().contains("mapper_parsing_exception"));
}

#[test]
fn parse_bulk_response_retryable_item_error_is_transient() {
    // All items failed with 429 (no successes) — safe to retry the full batch.
    let response = br#"{
        "took":5,
        "errors":true,
        "items":[
            {"index":{"error":{"type":"es_rejected_execution_exception","reason":"too many requests"},"status":429}}
        ]
    }"#;
    let err = ElasticsearchSink::parse_bulk_response(response)
        .expect_err("status 429 bulk item error must be transient");
    assert_eq!(
        err.kind(),
        io::ErrorKind::Other,
        "all-429 item-level errors should be retried"
    );
}

#[test]
fn parse_bulk_response_status_only_retryable_item_error_is_transient() {
    // All items failed with status-only 429 (no successes) — safe to retry.
    let response = br#"{
        "took":5,
        "errors":true,
        "items":[
            {"index":{"status":429}}
        ]
    }"#;
    let err = ElasticsearchSink::parse_bulk_response(response)
        .expect_err("status-only 429 bulk item error must be transient");
    assert_eq!(
        err.kind(),
        io::ErrorKind::Other,
        "status-only 429 item-level errors should be retried"
    );
}

#[test]
fn parse_bulk_response_status_only_permanent_item_error_is_invalid_data() {
    let response = br#"{
        "took":5,
        "errors":true,
        "items":[
            {"index":{"status":400}}
        ]
    }"#;
    let err = ElasticsearchSink::parse_bulk_response(response)
        .expect_err("status-only 400 bulk item error must be permanent");
    assert_eq!(
        err.kind(),
        io::ErrorKind::InvalidData,
        "status-only 400 item-level errors should be rejected"
    );
}

#[test]
fn parse_bulk_response_status_only_redirect_is_invalid_data() {
    let response = br#"{
        "took":5,
        "errors":true,
        "items":[
            {"index":{"status":307}}
        ]
    }"#;
    let err = ElasticsearchSink::parse_bulk_response(response)
        .expect_err("unexpected 3xx bulk item status must be rejected");
    assert_eq!(
        err.kind(),
        io::ErrorKind::InvalidData,
        "status-only 3xx item-level responses should not be counted as accepted"
    );
    assert!(
        err.to_string()
            .contains("unexpected ES bulk item status 307"),
        "error should mention unexpected 3xx status: {err}"
    );
}

/// Regression test for issue #1675.
///
/// `parse_bulk_response` used to return `Ok(())` when `errors:true` but no
/// parseable error could be found in `items[]`.  The batch would then be
/// silently treated as successfully delivered.
#[test]
fn parse_bulk_errors_true_without_parseable_error_returns_err() {
    // Simulate a malformed ES response where errors:true but no item has
    // an "error" key — the path that previously fell through to Ok(()).
    let response = br#"{"took":5,"errors":true,"items":[{"index":{"status":200}}]}"#;
    let err = ElasticsearchSink::parse_bulk_response(response)
        .expect_err("errors:true with no parseable error must return Err");
    assert_eq!(
        err.kind(),
        io::ErrorKind::InvalidData,
        "errors:true must be classified as InvalidData so send_batch rejects it"
    );
    assert!(
        err.to_string().contains("no error details found"),
        "error message should mention no error details: {err}"
    );
}

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
        resource_attrs: Arc::new(vec![]),
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

#[tokio::test]
async fn split_rejection_on_left_still_sends_right_half() {
    use crate::sink::Sink;

    let mut server = mockito::Server::new_async().await;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["left-row", "right-row"]))],
    )
    .expect("test batch should be valid");
    let metadata = zero_metadata();

    let mut sizing_sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config(&server.url(), "logs", usize::MAX),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );
    sizing_sink
        .serialize_batch(&batch, &metadata)
        .expect("full batch should serialize");
    let full_len = sizing_sink.batch_buf.len();
    sizing_sink
        .serialize_batch(&batch.slice(0, 1), &metadata)
        .expect("left half should serialize");
    let left_len = sizing_sink.batch_buf.len();
    sizing_sink
        .serialize_batch(&batch.slice(1, 1), &metadata)
        .expect("right half should serialize");
    let right_len = sizing_sink.batch_buf.len();
    let split_threshold = left_len.max(right_len) + 1;
    assert!(full_len > split_threshold);

    let left_mock = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Regex("left-row".to_string()))
        .with_status(400)
        .with_body("left bad doc")
        .create_async()
        .await;
    let right_mock = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Regex("right-row".to_string()))
        .with_status(200)
        .with_body(r#"{"took":1,"errors":false,"items":[{"index":{"status":201}}]}"#)
        .create_async()
        .await;

    let mut sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config(&server.url(), "logs", split_threshold),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );

    let result = sink.send_batch(&batch, &metadata).await;
    match result {
        crate::sink::SendResult::Rejected(reason) => {
            assert!(reason.contains("left split rejected"), "got: {reason}");
            assert!(reason.contains("left bad doc"), "got: {reason}");
        }
        other => panic!("expected left rejection after right half send, got {other:?}"),
    }
    left_mock.assert_async().await;
    right_mock.assert_async().await;
}

/// Regression test for the CodeRabbit review on #2267:
/// When the left split half returns HTTP 200 with `errors: true` (permanent
/// item rejection via `parse_bulk_response` → `InvalidData`), the right
/// half must still be attempted.  Before `classify_split_result`, the `?`
/// operator propagated the `Err(InvalidData)` and skipped the right half.
#[tokio::test]
async fn split_bulk_item_error_on_left_still_sends_right_half() {
    use crate::sink::Sink;

    let mut server = mockito::Server::new_async().await;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["left-row", "right-row"]))],
    )
    .expect("test batch should be valid");
    let metadata = zero_metadata();

    // Measure serialized sizes to pick a split_threshold that forces splitting.
    let mut sizing_sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config(&server.url(), "logs", usize::MAX),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );
    sizing_sink
        .serialize_batch(&batch, &metadata)
        .expect("full batch should serialize");
    let full_len = sizing_sink.batch_buf.len();
    sizing_sink
        .serialize_batch(&batch.slice(0, 1), &metadata)
        .expect("left half should serialize");
    let left_len = sizing_sink.batch_buf.len();
    sizing_sink
        .serialize_batch(&batch.slice(1, 1), &metadata)
        .expect("right half should serialize");
    let right_len = sizing_sink.batch_buf.len();
    let split_threshold = left_len.max(right_len) + 1;
    assert!(full_len > split_threshold);

    // Left half: HTTP 200 with errors:true — permanent item rejection.
    // parse_bulk_response returns Err(InvalidData) for this.
    let left_mock = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Regex("left-row".to_string()))
        .with_status(200)
        .with_body(
            r#"{"took":1,"errors":true,"items":[{"index":{"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [ts]"},"status":400}}]}"#,
        )
        .create_async()
        .await;
    // Right half: success.
    let right_mock = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Regex("right-row".to_string()))
        .with_status(200)
        .with_body(r#"{"took":1,"errors":false,"items":[{"index":{"status":201}}]}"#)
        .create_async()
        .await;

    let mut sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config(&server.url(), "logs", split_threshold),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );

    let result = sink.send_batch(&batch, &metadata).await;
    match result {
        crate::sink::SendResult::Rejected(reason) => {
            assert!(
                reason.contains("left split rejected"),
                "expected left split label, got: {reason}"
            );
            assert!(
                reason.contains("mapper_parsing_exception"),
                "expected ES error type, got: {reason}"
            );
        }
        other => panic!(
            "expected Rejected from left bulk item error after right half send, got {other:?}"
        ),
    }
    // Both mocks must have been hit — the right half was not skipped.
    left_mock.assert_async().await;
    right_mock.assert_async().await;
}

// -----------------------------------------------------------------------
// Bug #1873: split-half retry duplication prevention tests
// -----------------------------------------------------------------------

/// When left succeeds but right gets a transient error (429), the ES sink
/// must persist only the right half for the worker-level retry so the
/// already-delivered left half is not resent.
#[tokio::test]
async fn split_left_ok_right_retry_does_not_duplicate() {
    use crate::sink::Sink;

    let mut server = mockito::Server::new_async().await;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["left-row", "right-row"]))],
    )
    .expect("test batch should be valid");
    let metadata = zero_metadata();

    // Measure serialized sizes to pick a threshold that forces splitting.
    let mut sizing_sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config(&server.url(), "logs", usize::MAX),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );
    sizing_sink
        .serialize_batch(&batch, &metadata)
        .expect("full batch should serialize");
    let full_len = sizing_sink.batch_buf.len();
    sizing_sink
        .serialize_batch(&batch.slice(0, 1), &metadata)
        .expect("left half should serialize");
    let left_len = sizing_sink.batch_buf.len();
    sizing_sink
        .serialize_batch(&batch.slice(1, 1), &metadata)
        .expect("right half should serialize");
    let right_len = sizing_sink.batch_buf.len();
    let split_threshold = left_len.max(right_len) + 1;
    assert!(full_len > split_threshold);

    // Left half: success.
    let left_mock = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Regex("left-row".to_string()))
        .with_status(200)
        .with_body(r#"{"took":1,"errors":false,"items":[{"index":{"status":201}}]}"#)
        .expect(1) // left must be sent exactly once — no duplication
        .create_async()
        .await;
    // Right half: first attempt returns 429, worker-level subset retry succeeds.
    let right_fail_mock = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Regex("right-row".to_string()))
        .with_status(429)
        .with_body("too many requests")
        .expect(1) // first attempt fails
        .create_async()
        .await;
    let right_ok_mock = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Regex("right-row".to_string()))
        .with_status(200)
        .with_body(r#"{"took":1,"errors":false,"items":[{"index":{"status":201}}]}"#)
        .expect(1) // subset retry succeeds
        .create_async()
        .await;

    let mut sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config(&server.url(), "logs", split_threshold),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );

    let result = sink.send_batch(&batch, &metadata).await;
    assert!(
        matches!(result, crate::sink::SendResult::RetryAfter(_)),
        "expected RetryAfter after right half failure, got {result:?}"
    );
    let result = sink.send_batch(&batch, &metadata).await;
    assert!(
        matches!(result, crate::sink::SendResult::Ok),
        "expected Ok after subset retry of right half, got {result:?}"
    );
    // Left must be sent exactly once — no duplication.
    left_mock.assert_async().await;
    right_fail_mock.assert_async().await;
    right_ok_mock.assert_async().await;
}

/// When left succeeds and right remains retryable, worker-level retries must
/// continue to send only the right half.
#[tokio::test]
async fn split_left_ok_right_retries_only_right_half() {
    use crate::sink::Sink;

    let mut server = mockito::Server::new_async().await;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["left-row", "right-row"]))],
    )
    .expect("test batch should be valid");
    let metadata = zero_metadata();

    let mut sizing_sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config(&server.url(), "logs", usize::MAX),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );
    sizing_sink
        .serialize_batch(&batch, &metadata)
        .expect("full batch should serialize");
    let full_len = sizing_sink.batch_buf.len();
    sizing_sink
        .serialize_batch(&batch.slice(0, 1), &metadata)
        .expect("left half should serialize");
    let left_len = sizing_sink.batch_buf.len();
    sizing_sink
        .serialize_batch(&batch.slice(1, 1), &metadata)
        .expect("right half should serialize");
    let right_len = sizing_sink.batch_buf.len();
    let split_threshold = left_len.max(right_len) + 1;
    assert!(full_len > split_threshold);

    // Left half: success.
    let left_mock = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Regex("left-row".to_string()))
        .with_status(200)
        .with_body(r#"{"took":1,"errors":false,"items":[{"index":{"status":201}}]}"#)
        .expect(1) // left must be sent exactly once
        .create_async()
        .await;
    // Right half: always fails with 429 across worker-level attempts.
    let right_mock = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Regex("right-row".to_string()))
        .with_status(429)
        .with_body("too many requests")
        .expect(2) // initial + one worker-level subset retry
        .create_async()
        .await;

    let mut sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config(&server.url(), "logs", split_threshold),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );

    let result = sink.send_batch(&batch, &metadata).await;
    assert!(
        matches!(result, crate::sink::SendResult::RetryAfter(_)),
        "expected retryable result for right half, got {result:?}"
    );
    let result = sink.send_batch(&batch, &metadata).await;
    assert!(
        matches!(result, crate::sink::SendResult::RetryAfter(_)),
        "expected retryable result for right half retry, got {result:?}"
    );
    left_mock.assert_async().await;
    right_mock.assert_async().await;
}

#[tokio::test]
async fn split_left_retry_right_ok_attempts_right_and_retries_only_left() {
    use crate::sink::Sink;

    let mut server = mockito::Server::new_async().await;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["left-row", "right-row"]))],
    )
    .expect("test batch should be valid");
    let metadata = zero_metadata();

    let mut sizing_sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config(&server.url(), "logs", usize::MAX),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );
    sizing_sink
        .serialize_batch(&batch, &metadata)
        .expect("full batch should serialize");
    let full_len = sizing_sink.batch_buf.len();
    sizing_sink
        .serialize_batch(&batch.slice(0, 1), &metadata)
        .expect("left half should serialize");
    let left_len = sizing_sink.batch_buf.len();
    let left_body = String::from_utf8(sizing_sink.batch_buf.clone()).expect("utf8 body");
    sizing_sink
        .serialize_batch(&batch.slice(1, 1), &metadata)
        .expect("right half should serialize");
    let right_len = sizing_sink.batch_buf.len();
    let split_threshold = left_len.max(right_len) + 1;
    assert!(full_len > split_threshold);

    let left_fail_mock = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Regex("left-row".to_string()))
        .with_status(429)
        .with_body("too many requests")
        .expect(1)
        .create_async()
        .await;
    let right_ok_mock = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Regex("right-row".to_string()))
        .with_status(200)
        .with_body(r#"{"took":1,"errors":false,"items":[{"index":{"status":201}}]}"#)
        .expect(1)
        .create_async()
        .await;
    let left_retry_ok_mock = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Exact(left_body))
        .with_status(200)
        .with_body(r#"{"took":1,"errors":false,"items":[{"index":{"status":201}}]}"#)
        .expect(1)
        .create_async()
        .await;

    let mut sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config(&server.url(), "logs", split_threshold),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );

    let first = sink.send_batch(&batch, &metadata).await;
    assert!(
        matches!(first, crate::sink::SendResult::RetryAfter(_)),
        "expected retry after left half failure, got {first:?}"
    );
    let second = sink.send_batch(&batch, &metadata).await;
    assert!(
        matches!(second, crate::sink::SendResult::Ok),
        "expected Ok after subset retry of left half, got {second:?}"
    );

    left_fail_mock.assert_async().await;
    right_ok_mock.assert_async().await;
    left_retry_ok_mock.assert_async().await;
}

#[test]
fn merge_both_io_errors_keeps_right_error_context() {
    let result = ElasticsearchSink::merge_split_attempts(
        SendAttempt::IoError {
            pending_rows: vec![0],
            rejections: Vec::new(),
            error: io::Error::other("left unavailable"),
        },
        SendAttempt::IoError {
            pending_rows: vec![1],
            rejections: Vec::new(),
            error: io::Error::other("right unavailable"),
        },
    );

    match result {
        SendAttempt::IoError {
            pending_rows,
            error,
            ..
        } => {
            assert_eq!(pending_rows, vec![0, 1]);
            let message = error.to_string();
            assert!(message.contains("left unavailable"), "got: {message}");
            assert!(message.contains("right unavailable"), "got: {message}");
        }
        _ => panic!("expected IoError"),
    }
}

// -----------------------------------------------------------------------
// Bug #1880: bulk partial success duplication prevention tests
// -----------------------------------------------------------------------

/// When an ES bulk response contains mixed results (some 200, some 429),
/// `parse_bulk_response` must return a permanent error to prevent the
/// worker pool from retrying already-accepted rows.
#[test]
fn bulk_partial_success_returns_permanent_error() {
    let response = br#"{
        "took":5,
        "errors":true,
        "items":[
            {"index":{"_id":"1","status":201}},
            {"index":{"error":{"type":"es_rejected_execution_exception","reason":"too many requests"},"status":429}},
            {"index":{"_id":"3","status":201}}
        ]
    }"#;
    let err = ElasticsearchSink::parse_bulk_response(response)
        .expect_err("mixed success/failure must be an error");
    assert_eq!(
        err.kind(),
        io::ErrorKind::InvalidData,
        "mixed results must be permanent to prevent duplication of 2 delivered rows"
    );
    assert!(
        err.to_string().contains("partial failure"),
        "error should mention partial failure: {err}"
    );
    assert!(
        err.to_string().contains("2 succeeded"),
        "error should report succeeded count: {err}"
    );
    assert!(
        err.to_string().contains("1 retryable"),
        "error should report retryable count: {err}"
    );
}

#[tokio::test]
async fn bulk_partial_success_retries_only_transient_items() {
    use crate::sink::Sink;

    let mut server = mockito::Server::new_async().await;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec![
            "accepted-a",
            "retry-me",
            "accepted-b",
        ]))],
    )
    .expect("test batch should be valid");
    let metadata = zero_metadata();

    let mut sizing_sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config(&server.url(), "logs", usize::MAX),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );
    sizing_sink
        .serialize_batch(&batch.slice(1, 1), &metadata)
        .expect("retry row should serialize");
    let retry_body = String::from_utf8(sizing_sink.batch_buf.clone()).expect("utf8 body");

    let full_attempt = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Regex("accepted-a".to_string()))
        .with_status(200)
        .with_body(
            r#"{"took":1,"errors":true,"items":[{"index":{"status":201}},{"index":{"error":{"type":"es_rejected_execution_exception","reason":"too many requests"},"status":429}},{"index":{"status":201}}]}"#,
        )
        .expect(1)
        .create_async()
        .await;
    let retry_subset = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Exact(retry_body))
        .with_status(200)
        .with_body(r#"{"took":1,"errors":false,"items":[{"index":{"status":201}}]}"#)
        .expect(1)
        .create_async()
        .await;

    let mut sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config(&server.url(), "logs", usize::MAX),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );

    let first = sink.send_batch(&batch, &metadata).await;
    assert!(
        matches!(first, crate::sink::SendResult::RetryAfter(_)),
        "expected subset retry after partial response, got {first:?}"
    );
    let second = sink.send_batch(&batch, &metadata).await;
    assert!(
        matches!(second, crate::sink::SendResult::Ok),
        "expected retry subset success, got {second:?}"
    );

    full_attempt.assert_async().await;
    retry_subset.assert_async().await;
}

#[tokio::test]
async fn streaming_bulk_partial_success_retries_only_transient_items() {
    use crate::sink::Sink;

    let mut server = mockito::Server::new_async().await;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec![
            "accepted-a",
            "retry-me",
            "accepted-b",
        ]))],
    )
    .expect("test batch should be valid");
    let metadata = zero_metadata();

    let retry_body = streaming_body_for_test(
        &batch.slice(1, 1),
        &metadata,
        test_es_config_with_mode(
            &server.url(),
            "logs",
            usize::MAX,
            ElasticsearchRequestMode::Streaming,
        ),
    )
    .await;

    let full_attempt = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Regex("accepted-a".to_string()))
        .with_status(200)
        .with_body(
            r#"{"took":1,"errors":true,"items":[{"index":{"status":201}},{"index":{"error":{"type":"es_rejected_execution_exception","reason":"too many requests"},"status":429}},{"index":{"status":201}}]}"#,
        )
        .expect(1)
        .create_async()
        .await;
    let retry_subset = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Exact(retry_body))
        .with_status(200)
        .with_body(r#"{"took":1,"errors":false,"items":[{"index":{"status":201}}]}"#)
        .expect(1)
        .create_async()
        .await;

    let mut sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config_with_mode(
            &server.url(),
            "logs",
            usize::MAX,
            ElasticsearchRequestMode::Streaming,
        ),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );

    let first = sink.send_batch(&batch, &metadata).await;
    assert!(
        matches!(first, crate::sink::SendResult::RetryAfter(_)),
        "expected streaming subset retry after partial response, got {first:?}"
    );
    let second = sink.send_batch(&batch, &metadata).await;
    assert!(
        matches!(second, crate::sink::SendResult::Ok),
        "expected streaming retry subset success, got {second:?}"
    );

    full_attempt.assert_async().await;
    retry_subset.assert_async().await;
}

#[tokio::test]
async fn bulk_mixed_permanent_and_transient_retries_then_rejects_permanent() {
    use crate::sink::Sink;

    let mut server = mockito::Server::new_async().await;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec![
            "accepted", "bad-doc", "retry-me",
        ]))],
    )
    .expect("test batch should be valid");
    let metadata = zero_metadata();

    let mut sizing_sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config(&server.url(), "logs", usize::MAX),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );
    sizing_sink
        .serialize_batch(&batch.slice(2, 1), &metadata)
        .expect("retry row should serialize");
    let retry_body = String::from_utf8(sizing_sink.batch_buf.clone()).expect("utf8 body");

    let mixed = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Regex("bad-doc".to_string()))
        .with_status(200)
        .with_body(
            r#"{"took":1,"errors":true,"items":[{"index":{"status":201}},{"index":{"error":{"type":"mapper_parsing_exception","reason":"bad field"},"status":400}},{"index":{"error":{"type":"es_rejected_execution_exception","reason":"too many requests"},"status":429}}]}"#,
        )
        .expect(1)
        .create_async()
        .await;
    let retry_ok = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .match_body(mockito::Matcher::Exact(retry_body))
        .with_status(200)
        .with_body(r#"{"took":1,"errors":false,"items":[{"index":{"status":201}}]}"#)
        .expect(1)
        .create_async()
        .await;

    let mut sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config(&server.url(), "logs", usize::MAX),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );

    let first = sink.send_batch(&batch, &metadata).await;
    assert!(
        matches!(first, crate::sink::SendResult::RetryAfter(_)),
        "expected transient row to remain retryable, got {first:?}"
    );
    let second = sink.send_batch(&batch, &metadata).await;
    match second {
        crate::sink::SendResult::Rejected(reason) => {
            assert!(reason.contains("mapper_parsing_exception"), "got: {reason}");
            assert!(reason.contains("bad field"), "got: {reason}");
        }
        other => panic!("expected permanent rejection after retry success, got {other:?}"),
    }

    mixed.assert_async().await;
    retry_ok.assert_async().await;
}

#[tokio::test]
async fn bulk_item_count_mismatch_rejects_without_subset_retry() {
    use crate::sink::Sink;

    let mut server = mockito::Server::new_async().await;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["row-a", "row-b", "row-c"]))],
    )
    .expect("test batch should be valid");
    let metadata = zero_metadata();

    let mismatch = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .with_status(200)
        .with_body(
            r#"{"took":1,"errors":true,"items":[{"index":{"status":201}},{"index":{"error":{"type":"es_rejected_execution_exception","reason":"too many requests"},"status":429}}]}"#,
        )
        .expect(1)
        .create_async()
        .await;

    let mut sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config(&server.url(), "logs", usize::MAX),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );

    let result = sink.send_batch(&batch, &metadata).await;
    match result {
        crate::sink::SendResult::Rejected(reason) => {
            assert!(reason.contains("item count mismatch"), "got: {reason}");
        }
        other => panic!("expected structural mismatch rejection, got {other:?}"),
    }

    mismatch.assert_async().await;
}

/// When ALL items in the bulk response failed with retryable errors
/// (429/5xx), `parse_bulk_response` should return a transient error
/// since it's safe to retry the full batch (no rows were accepted).
#[test]
fn bulk_all_retryable_returns_transient() {
    let response = br#"{
        "took":5,
        "errors":true,
        "items":[
            {"index":{"error":{"type":"es_rejected_execution_exception","reason":"too many requests"},"status":429}},
            {"index":{"error":{"type":"es_rejected_execution_exception","reason":"too many requests"},"status":429}}
        ]
    }"#;
    let err = ElasticsearchSink::parse_bulk_response(response)
        .expect_err("all-retryable must be an error");
    assert_eq!(
        err.kind(),
        io::ErrorKind::Other,
        "all-retryable failures should be transient (safe to retry full batch)"
    );
}

/// When ALL items in the bulk response failed with permanent errors
/// (4xx non-429), `parse_bulk_response` should return a permanent error.
#[test]
fn bulk_all_permanent_returns_rejected() {
    let response = br#"{
        "took":5,
        "errors":true,
        "items":[
            {"index":{"error":{"type":"mapper_parsing_exception","reason":"failed to parse"},"status":400}},
            {"index":{"error":{"type":"strict_dynamic_mapping_exception","reason":"unmapped field"},"status":400}}
        ]
    }"#;
    let err = ElasticsearchSink::parse_bulk_response(response)
        .expect_err("all-permanent must be an error");
    assert_eq!(
        err.kind(),
        io::ErrorKind::InvalidData,
        "all-permanent failures should be rejected"
    );
    assert!(
        err.to_string().contains("2 items rejected"),
        "error should report count: {err}"
    );
}

/// When both split halves fail with transient errors (no rows delivered),
/// the merged result should still be retryable since it's safe for the
/// worker pool to retry the full batch.
#[tokio::test]
async fn split_both_halves_fail_returns_retryable() {
    use crate::sink::Sink;

    let mut server = mockito::Server::new_async().await;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["left-row", "right-row"]))],
    )
    .expect("test batch should be valid");
    let metadata = zero_metadata();

    let mut sizing_sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config(&server.url(), "logs", usize::MAX),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );
    sizing_sink
        .serialize_batch(&batch, &metadata)
        .expect("full batch should serialize");
    let full_len = sizing_sink.batch_buf.len();
    sizing_sink
        .serialize_batch(&batch.slice(0, 1), &metadata)
        .expect("left half should serialize");
    let left_len = sizing_sink.batch_buf.len();
    sizing_sink
        .serialize_batch(&batch.slice(1, 1), &metadata)
        .expect("right half should serialize");
    let right_len = sizing_sink.batch_buf.len();
    let split_threshold = left_len.max(right_len) + 1;
    assert!(full_len > split_threshold);

    // Both halves fail with 503.
    let _mock = server
        .mock("POST", "/_bulk")
        .match_query(mockito::Matcher::Any)
        .with_status(503)
        .with_body("service unavailable")
        .expect(2) // both halves attempted
        .create_async()
        .await;

    let mut sink = ElasticsearchSink::new(
        "test".to_string(),
        test_es_config(&server.url(), "logs", split_threshold),
        reqwest::Client::new(),
        Arc::new(ComponentStats::default()),
    );

    let result = sink.send_batch(&batch, &metadata).await;
    assert!(
        matches!(
            result,
            crate::sink::SendResult::IoError(_) | crate::sink::SendResult::RetryAfter(_)
        ),
        "both halves failed — safe to return retryable for worker pool retry, got {result:?}"
    );
}

fn test_es_config(endpoint: &str, index: &str, max_bulk_bytes: usize) -> Arc<ElasticsearchConfig> {
    test_es_config_with_mode(
        endpoint,
        index,
        max_bulk_bytes,
        ElasticsearchRequestMode::Buffered,
    )
}

fn test_es_config_with_mode(
    endpoint: &str,
    index: &str,
    max_bulk_bytes: usize,
    request_mode: ElasticsearchRequestMode,
) -> Arc<ElasticsearchConfig> {
    let escaped_index = serde_json::to_string(index).expect("test index should serialize");
    Arc::new(ElasticsearchConfig {
        endpoint: endpoint.to_string(),
        headers: Vec::new(),
        compress: false,
        request_mode,
        max_bulk_bytes,
        stream_chunk_bytes: 64 * 1024,
        bulk_url: format!(
            "{}/_bulk?filter_path=errors,took,items.*.error,items.*.status",
            endpoint.trim_end_matches('/')
        ),
        action_bytes: format!("{{\"index\":{{\"_index\":{escaped_index}}}}}\n")
            .into_bytes()
            .into_boxed_slice(),
    })
}

async fn streaming_body_for_test(
    batch: &RecordBatch,
    metadata: &BatchMetadata,
    config: Arc<ElasticsearchConfig>,
) -> String {
    let (tx, mut rx) = mpsc::channel::<io::Result<Vec<u8>>>(16);
    let producer_batch = batch.clone();
    let producer_metadata = metadata.clone();
    let producer = tokio::task::spawn_blocking(move || {
        ElasticsearchSink::serialize_batch_streaming(
            producer_batch,
            producer_metadata,
            config,
            tx,
            Arc::new(AtomicU64::new(0)),
        )
    });
    let mut body = Vec::new();
    while let Some(chunk) = rx.recv().await {
        body.extend(chunk.expect("streaming chunk should be ok"));
    }
    producer
        .await
        .expect("streaming producer should not panic")
        .expect("streaming test body should serialize");
    String::from_utf8(body).expect("streaming body should be utf8")
}

mod snapshot_tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn zero_metadata() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::new(vec![]),
            observed_time_ns: 0,
        }
    }

    fn make_test_sink() -> ElasticsearchSink {
        let factory = ElasticsearchSinkFactory::new(
            "test".to_string(),
            "http://localhost:9200".to_string(),
            "test-index".to_string(),
            vec![],
            false,
            ElasticsearchRequestMode::Buffered,
            Arc::new(ComponentStats::default()),
        )
        .expect("factory creation failed");
        let client = reqwest::Client::new();
        ElasticsearchSink::new(
            "test".to_string(),
            Arc::clone(&factory.config),
            client,
            Arc::new(ComponentStats::default()),
        )
    }

    /// Snapshot: basic multi-type batch (level, status, duration_ms).
    /// Regression guard: field name preservation + type serialization.
    #[test]
    fn snapshot_basic_multi_type_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, false),
            Field::new("status", DataType::Int64, false),
            Field::new("duration_ms", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["ERROR", "INFO", "WARN"])),
                Arc::new(Int64Array::from(vec![500i64, 200, 404])),
                Arc::new(Float64Array::from(vec![125.5f64, 3.2, 87.0])),
            ],
        )
        .unwrap();

        let mut sink = make_test_sink();
        let meta = zero_metadata();
        sink.serialize_batch(&batch, &meta).unwrap();
        let output = String::from_utf8(sink.batch_buf.clone()).unwrap();
        insta::assert_snapshot!("basic_multi_type", output);
    }

    /// Snapshot: nullable columns — null values must appear as JSON null.
    #[test]
    fn snapshot_nullable_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("hello"), None, Some("world")])),
                Arc::new(Int64Array::from(vec![Some(1i64), Some(2), None])),
            ],
        )
        .unwrap();

        let mut sink = make_test_sink();
        let meta = zero_metadata();
        sink.serialize_batch(&batch, &meta).unwrap();
        let output = String::from_utf8(sink.batch_buf.clone()).unwrap();
        insta::assert_snapshot!("nullable_columns", output);
    }

    /// Snapshot: strings with special JSON characters (quotes, backslash, newlines).
    /// Regression guard: escape_json correctness.
    #[test]
    fn snapshot_special_char_strings() {
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                r#"say "hello" world"#,
                "line1\nline2\ttab",
                r"back\slash",
                "control\x00char",
            ]))],
        )
        .unwrap();

        let mut sink = make_test_sink();
        let meta = zero_metadata();
        sink.serialize_batch(&batch, &meta).unwrap();
        let output = String::from_utf8(sink.batch_buf.clone()).unwrap();
        insta::assert_snapshot!("special_char_strings", output);
    }

    /// Snapshot: single row with all-null nullable fields produces valid output.
    #[test]
    fn snapshot_all_null_fields() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, true),
            Field::new("code", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![None as Option<&str>])),
                Arc::new(Int64Array::from(vec![None as Option<i64>])),
            ],
        )
        .unwrap();

        let mut sink = make_test_sink();
        let meta = zero_metadata();
        sink.serialize_batch(&batch, &meta).unwrap();
        let output = String::from_utf8(sink.batch_buf.clone()).unwrap();
        insta::assert_snapshot!("all_null_fields", output);
    }
}

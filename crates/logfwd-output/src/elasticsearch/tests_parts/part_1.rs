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
        resource_attrs: Arc::from([]),
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
                resource_attrs: Arc::from([]),
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
            resource_attrs: Arc::from([]),
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

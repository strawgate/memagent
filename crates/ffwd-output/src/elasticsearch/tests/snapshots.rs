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
            resource_attrs: Arc::from([]),
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

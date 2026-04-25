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

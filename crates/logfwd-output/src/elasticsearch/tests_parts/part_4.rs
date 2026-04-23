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

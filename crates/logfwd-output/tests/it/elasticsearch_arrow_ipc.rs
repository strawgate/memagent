//! Integration tests for Elasticsearch ES|QL with Arrow IPC format.
//!
//! These tests require a running Elasticsearch instance with ES|QL support (8.11+).
//! Run with: docker-compose up -d in examples/elasticsearch/
//!
//! Skip tests if Elasticsearch is not available using the `#[ignore]` attribute.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use logfwd_output::sink::SinkFactory;
use logfwd_output::{BatchMetadata, ElasticsearchRequestMode, ElasticsearchSinkFactory};
use logfwd_types::diagnostics::ComponentStats;

const ES_ENDPOINT: &str = "http://localhost:9200";
const TEST_INDEX: &str = "logfwd-test-arrow";

/// Check if Elasticsearch is running and accessible.
async fn check_elasticsearch_available() -> bool {
    let client = reqwest::Client::new();
    client
        .get(format!("{}/_cluster/health", ES_ENDPOINT))
        .send()
        .await
        .is_ok()
}

/// Delete test index to clean up after tests.
async fn cleanup_test_index() {
    let client = reqwest::Client::new();
    let _ = client
        .delete(format!("{}/{}", ES_ENDPOINT, TEST_INDEX))
        .send()
        .await;
}

/// Create an empty test index so ES|QL returns a zero-row result instead of a
/// missing-index error.
async fn create_empty_test_index() {
    let client = reqwest::Client::new();
    let response = client
        .put(format!("{}/{}", ES_ENDPOINT, TEST_INDEX))
        .send()
        .await
        .expect("failed to create empty test index");
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        panic!("failed to create empty test index (HTTP {status}): {body}");
    }
}

/// Query Elasticsearch using ES|QL and return Arrow IPC batches.
async fn query_arrow(query: &str) -> std::io::Result<Vec<RecordBatch>> {
    let client = reqwest::Client::new();
    let query_body = serde_json::json!({ "query": query });
    let query_bytes = serde_json::to_vec(&query_body).map_err(std::io::Error::other)?;
    let url = format!("{}/_query", ES_ENDPOINT);
    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/vnd.apache.arrow.stream")
        .body(query_bytes)
        .send()
        .await
        .map_err(std::io::Error::other)?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(std::io::Error::other(format!(
            "ES query failed (HTTP {status}): {body}"
        )));
    }
    let body = response.bytes().await.map_err(std::io::Error::other)?;
    let cursor = std::io::Cursor::new(body);
    let reader = arrow::ipc::reader::StreamReader::try_new(cursor, None)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e}")))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(std::io::Error::other)
}

/// Create test data and index it via the async sink.
async fn setup_test_data(sink: &mut Box<dyn logfwd_output::Sink>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("level_str", DataType::Utf8, false),
        Field::new("message_str", DataType::Utf8, false),
        Field::new("status_int", DataType::Int64, false),
        Field::new("duration_ms_int", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                "ERROR", "WARN", "INFO", "ERROR", "DEBUG",
            ])),
            Arc::new(StringArray::from(vec![
                "database timeout",
                "slow query",
                "request complete",
                "connection failed",
                "cache hit",
            ])),
            Arc::new(Int64Array::from(vec![500, 400, 200, 503, 200])),
            Arc::new(Int64Array::from(vec![1500, 800, 100, 2000, 5])),
        ],
    )
    .expect("batch creation failed");

    let metadata = BatchMetadata {
        resource_attrs: Arc::from([]),
        observed_time_ns: 0,
    };

    // Index the batch
    match sink.send_batch(&batch, &metadata).await {
        logfwd_output::sink::SendResult::Ok => {}
        logfwd_output::sink::SendResult::IoError(e) => {
            panic!("failed to index test data: {e}");
        }
        logfwd_output::sink::SendResult::RetryAfter(delay) => {
            panic!("failed to index test data: retry after {delay:?}");
        }
        logfwd_output::sink::SendResult::Rejected(reason) => {
            panic!("failed to index test data: {reason}");
        }
        other => {
            panic!("failed to index test data: unexpected result {other:?}");
        }
    }

    // Wait for indexing to complete
    tokio::time::sleep(Duration::from_millis(1000)).await;

    batch
}

#[test]
#[ignore = "needs es instance"] // Run with: cargo test --test elasticsearch_arrow_ipc -- --ignored
fn test_query_arrow_all_documents() {
    let rt = tokio::runtime::Runtime::new().expect("failed to create runtime");
    rt.block_on(async {
        if !check_elasticsearch_available().await {
            eprintln!(
                "Skipping test: Elasticsearch not available at {}",
                ES_ENDPOINT
            );
            return;
        }

        cleanup_test_index().await;

        let stats = Arc::new(ComponentStats::default());
        let factory = ElasticsearchSinkFactory::new(
            "test_arrow".to_string(),
            ES_ENDPOINT.to_string(),
            TEST_INDEX.to_string(),
            vec![],
            false,
            ElasticsearchRequestMode::Buffered,
            stats.clone(),
        )
        .expect("factory creation failed");

        let mut sink = factory.create().expect("sink creation failed");

        // Setup test data
        let _original_batch = setup_test_data(&mut sink).await;

        // Query all documents using ES|QL with Arrow IPC format
        let query = format!("FROM {} | SORT status_int | LIMIT 100", TEST_INDEX);
        let result = query_arrow(&query).await;

        match result {
            Ok(batches) => {
                assert!(!batches.is_empty(), "expected at least one batch");

                let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                assert_eq!(total_rows, 5, "expected 5 rows in query result");

                // Verify schema contains expected fields
                if let Some(first_batch) = batches.first() {
                    let schema = first_batch.schema();
                    let field_names: Vec<&str> =
                        schema.fields().iter().map(|f| f.name().as_str()).collect();

                    assert!(
                        field_names.contains(&"level_str"),
                        "schema should contain level_str"
                    );
                    assert!(
                        field_names.contains(&"status_int"),
                        "schema should contain status_int"
                    );
                }

                println!("Successfully queried {} rows via Arrow IPC", total_rows);
            }
            Err(e) => panic!("ES|QL Arrow query failed: {e}"),
        }

        cleanup_test_index().await;
    });
}

#[test]
#[ignore = "needs es instance"]
fn test_query_arrow_with_filter() {
    let rt = tokio::runtime::Runtime::new().expect("failed to create runtime");
    rt.block_on(async {
        if !check_elasticsearch_available().await {
            eprintln!(
                "Skipping test: Elasticsearch not available at {}",
                ES_ENDPOINT
            );
            return;
        }

        cleanup_test_index().await;

        let stats = Arc::new(ComponentStats::default());
        let factory = ElasticsearchSinkFactory::new(
            "test_arrow_filter".to_string(),
            ES_ENDPOINT.to_string(),
            TEST_INDEX.to_string(),
            vec![],
            false,
            ElasticsearchRequestMode::Buffered,
            stats.clone(),
        )
        .expect("factory creation failed");

        let mut sink = factory.create().expect("sink creation failed");

        // Setup test data
        setup_test_data(&mut sink).await;

        // Query with filter using ES|QL
        let query = format!(
            r#"FROM {} | WHERE level_str == "ERROR" | LIMIT 100"#,
            TEST_INDEX
        );
        let result = query_arrow(&query).await;

        match result {
            Ok(batches) => {
                let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                assert_eq!(total_rows, 2, "expected 2 ERROR rows");

                println!(
                    "Successfully filtered to {} ERROR rows via Arrow IPC",
                    total_rows
                );
            }
            Err(e) => panic!("ES|QL Arrow filter query failed: {e}"),
        }

        cleanup_test_index().await;
    });
}

#[test]
#[ignore = "needs es instance"]
fn test_query_arrow_with_projection() {
    let rt = tokio::runtime::Runtime::new().expect("failed to create runtime");
    rt.block_on(async {
        if !check_elasticsearch_available().await {
            eprintln!(
                "Skipping test: Elasticsearch not available at {}",
                ES_ENDPOINT
            );
            return;
        }

        cleanup_test_index().await;

        let stats = Arc::new(ComponentStats::default());
        let factory = ElasticsearchSinkFactory::new(
            "test_arrow_projection".to_string(),
            ES_ENDPOINT.to_string(),
            TEST_INDEX.to_string(),
            vec![],
            false,
            ElasticsearchRequestMode::Buffered,
            stats.clone(),
        )
        .expect("factory creation failed");

        let mut sink = factory.create().expect("sink creation failed");

        // Setup test data
        setup_test_data(&mut sink).await;

        // Query with projection using ES|QL
        let query = format!(
            "FROM {} | KEEP level_str, status_int | LIMIT 100",
            TEST_INDEX
        );
        let result = query_arrow(&query).await;

        match result {
            Ok(batches) => {
                if let Some(first_batch) = batches.first() {
                    let schema = first_batch.schema();
                    assert_eq!(
                        schema.fields().len(),
                        2,
                        "expected only 2 fields in projection"
                    );

                    let field_names: Vec<&str> =
                        schema.fields().iter().map(|f| f.name().as_str()).collect();
                    assert!(field_names.contains(&"level_str"));
                    assert!(field_names.contains(&"status_int"));
                }

                println!("Successfully projected 2 columns via Arrow IPC");
            }
            Err(e) => panic!("ES|QL Arrow projection query failed: {e}"),
        }

        cleanup_test_index().await;
    });
}

#[test]
#[ignore = "needs es instance"]
fn test_query_arrow_empty_result() {
    let rt = tokio::runtime::Runtime::new().expect("failed to create runtime");
    rt.block_on(async {
        if !check_elasticsearch_available().await {
            eprintln!(
                "Skipping test: Elasticsearch not available at {}",
                ES_ENDPOINT
            );
            return;
        }

        cleanup_test_index().await;
        create_empty_test_index().await;

        // Query an existing but empty index to verify the zero-row Arrow path.
        let query = format!("FROM {} | LIMIT 100", TEST_INDEX);
        let result = query_arrow(&query).await;

        match result {
            Ok(batches) => {
                let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                assert_eq!(total_rows, 0, "expected 0 rows from empty index");
                println!("Empty result handled correctly");
            }
            Err(e) => panic!("empty-index ES|QL query should return zero rows, got error: {e}"),
        }

        cleanup_test_index().await;
    });
}

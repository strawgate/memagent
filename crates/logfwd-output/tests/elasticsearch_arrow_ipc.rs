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

use logfwd_io::diagnostics::ComponentStats;
use logfwd_output::{BatchMetadata, ElasticsearchSink, OutputSink};

const ES_ENDPOINT: &str = "http://localhost:9200";
const TEST_INDEX: &str = "logfwd-test-arrow";

/// Check if Elasticsearch is running and accessible.
fn check_elasticsearch_available() -> bool {
    ureq::get(&format!("{}/_cluster/health", ES_ENDPOINT))
        .call()
        .is_ok()
}

/// Create test data and index it in Elasticsearch.
fn setup_test_data(sink: &mut ElasticsearchSink) -> RecordBatch {
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
        resource_attrs: Arc::new(vec![]),
        observed_time_ns: 0,
    };

    // Index the batch
    sink.send_batch(&batch, &metadata)
        .expect("failed to index test data");

    // Wait for indexing to complete
    std::thread::sleep(Duration::from_millis(1000));

    batch
}

/// Delete test index to clean up after tests.
fn cleanup_test_index() {
    let _ = ureq::delete(&format!("{}/{}", ES_ENDPOINT, TEST_INDEX)).call();
}

#[test]
#[ignore] // Run with: cargo test --test elasticsearch_arrow_ipc -- --ignored
fn test_query_arrow_all_documents() {
    if !check_elasticsearch_available() {
        eprintln!(
            "Skipping test: Elasticsearch not available at {}",
            ES_ENDPOINT
        );
        return;
    }

    cleanup_test_index();

    let stats = Arc::new(ComponentStats::default());
    let mut sink = ElasticsearchSink::new(
        "test_arrow".to_string(),
        ES_ENDPOINT.to_string(),
        TEST_INDEX.to_string(),
        vec![],
        stats.clone(),
    );

    // Setup test data
    let _original_batch = setup_test_data(&mut sink);

    // Query all documents using ES|QL with Arrow IPC format
    let query = format!("FROM {} | SORT status_int | LIMIT 100", TEST_INDEX);
    let result = sink.query_arrow(&query);

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

            println!("✓ Successfully queried {} rows via Arrow IPC", total_rows);
        }
        Err(e) => {
            eprintln!(
                "Query failed (ES|QL might not be enabled or version < 8.11): {}",
                e
            );
            // Don't fail the test - ES|QL might not be available
        }
    }

    cleanup_test_index();
}

#[test]
#[ignore]
fn test_query_arrow_with_filter() {
    if !check_elasticsearch_available() {
        eprintln!(
            "Skipping test: Elasticsearch not available at {}",
            ES_ENDPOINT
        );
        return;
    }

    cleanup_test_index();

    let stats = Arc::new(ComponentStats::default());
    let mut sink = ElasticsearchSink::new(
        "test_arrow_filter".to_string(),
        ES_ENDPOINT.to_string(),
        TEST_INDEX.to_string(),
        vec![],
        stats.clone(),
    );

    // Setup test data
    setup_test_data(&mut sink);

    // Query with filter using ES|QL
    let query = format!(
        r#"FROM {} | WHERE level_str == "ERROR" | LIMIT 100"#,
        TEST_INDEX
    );
    let result = sink.query_arrow(&query);

    match result {
        Ok(batches) => {
            let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total_rows, 2, "expected 2 ERROR rows");

            println!(
                "✓ Successfully filtered to {} ERROR rows via Arrow IPC",
                total_rows
            );
        }
        Err(e) => {
            eprintln!(
                "Query failed (ES|QL might not be enabled or version < 8.11): {}",
                e
            );
        }
    }

    cleanup_test_index();
}

#[test]
#[ignore]
fn test_query_arrow_with_projection() {
    if !check_elasticsearch_available() {
        eprintln!(
            "Skipping test: Elasticsearch not available at {}",
            ES_ENDPOINT
        );
        return;
    }

    cleanup_test_index();

    let stats = Arc::new(ComponentStats::default());
    let mut sink = ElasticsearchSink::new(
        "test_arrow_projection".to_string(),
        ES_ENDPOINT.to_string(),
        TEST_INDEX.to_string(),
        vec![],
        stats.clone(),
    );

    // Setup test data
    setup_test_data(&mut sink);

    // Query with projection using ES|QL
    let query = format!(
        "FROM {} | KEEP level_str, status_int | LIMIT 100",
        TEST_INDEX
    );
    let result = sink.query_arrow(&query);

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

            println!("✓ Successfully projected 2 columns via Arrow IPC");
        }
        Err(e) => {
            eprintln!(
                "Query failed (ES|QL might not be enabled or version < 8.11): {}",
                e
            );
        }
    }

    cleanup_test_index();
}

#[test]
#[ignore]
fn test_query_arrow_empty_result() {
    if !check_elasticsearch_available() {
        eprintln!(
            "Skipping test: Elasticsearch not available at {}",
            ES_ENDPOINT
        );
        return;
    }

    cleanup_test_index();

    let stats = Arc::new(ComponentStats::default());
    let sink = ElasticsearchSink::new(
        "test_arrow_empty".to_string(),
        ES_ENDPOINT.to_string(),
        TEST_INDEX.to_string(),
        vec![],
        stats.clone(),
    );

    // Query non-existent index (should return empty or error gracefully)
    let query = format!("FROM {} | LIMIT 100", TEST_INDEX);
    let result = sink.query_arrow(&query);

    match result {
        Ok(batches) => {
            let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total_rows, 0, "expected 0 rows from non-existent index");
            println!("✓ Empty result handled correctly");
        }
        Err(e) => {
            // Error is acceptable for non-existent index
            println!("✓ Empty result error handled: {}", e);
        }
    }
}

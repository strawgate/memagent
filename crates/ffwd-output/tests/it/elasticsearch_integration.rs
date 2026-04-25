//! Integration tests for Elasticsearch output using blackhole receiver.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use ffwd_output::sink::SinkFactory;
use ffwd_output::{BatchMetadata, ElasticsearchRequestMode, ElasticsearchSinkFactory};
use ffwd_types::diagnostics::ComponentStats;

/// Start a blackhole HTTP server that mimics Elasticsearch bulk API.
fn start_blackhole() -> (String, tiny_http::Server) {
    // Find available port
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind failed");
    let port = listener.local_addr().expect("local_addr failed").port();
    drop(listener);

    let addr = format!("127.0.0.1:{port}");
    let server = tiny_http::Server::http(&addr).expect("server failed");

    // Give server time to start
    std::thread::sleep(Duration::from_millis(50));

    (format!("http://{addr}"), server)
}

/// Simple bulk API response handler that tracks lines received.
fn handle_bulk_requests(server: tiny_http::Server, total_lines: Arc<std::sync::atomic::AtomicU64>) {
    let json_hdr =
        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    let bulk_ok = r#"{"took":0,"errors":false,"items":[]}"#;

    std::thread::spawn(move || {
        for mut req in server.incoming_requests() {
            // Read body
            let mut body = Vec::new();
            let _ = req.as_reader().read_to_end(&mut body);

            // Count lines (bulk format: 2 lines per record)
            let lines = memchr::memchr_iter(b'\n', &body).count() as u64 / 2;
            total_lines.fetch_add(lines, std::sync::atomic::Ordering::Relaxed);

            // Respond with success
            let _ = req.respond(
                tiny_http::Response::from_string(bulk_ok)
                    .with_status_code(200)
                    .with_header(json_hdr.clone()),
            );
        }
    });
}

fn handle_bulk_requests_with_transfer_encoding(
    server: tiny_http::Server,
    total_lines: Arc<std::sync::atomic::AtomicU64>,
    saw_chunked: Arc<std::sync::atomic::AtomicBool>,
) {
    let json_hdr =
        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    let bulk_ok = r#"{"took":0,"errors":false,"items":[]}"#;

    std::thread::spawn(move || {
        for mut req in server.incoming_requests() {
            let is_chunked = req.headers().iter().any(|h| {
                h.field.equiv("Transfer-Encoding")
                    && h.value.as_str().eq_ignore_ascii_case("chunked")
            });
            if is_chunked {
                saw_chunked.store(true, std::sync::atomic::Ordering::Relaxed);
            }

            let mut body = Vec::new();
            let _ = req.as_reader().read_to_end(&mut body);
            let lines = memchr::memchr_iter(b'\n', &body).count() as u64 / 2;
            total_lines.fetch_add(lines, std::sync::atomic::Ordering::Relaxed);

            let _ = req.respond(
                tiny_http::Response::from_string(bulk_ok)
                    .with_status_code(200)
                    .with_header(json_hdr.clone()),
            );
        }
    });
}

#[test]
fn elasticsearch_sink_sends_bulk_data() {
    let (endpoint, server) = start_blackhole();
    let total_lines = Arc::new(std::sync::atomic::AtomicU64::new(0));
    handle_bulk_requests(server, total_lines.clone());

    let schema = Arc::new(Schema::new(vec![
        Field::new("level_str", DataType::Utf8, false),
        Field::new("msg_str", DataType::Utf8, false),
        Field::new("status_int", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["ERROR", "WARN", "INFO"])),
            Arc::new(StringArray::from(vec![
                "database timeout",
                "slow query",
                "request complete",
            ])),
            Arc::new(Int64Array::from(vec![500, 400, 200])),
        ],
    )
    .expect("batch creation failed");

    let stats = Arc::new(ComponentStats::default());
    let factory = ElasticsearchSinkFactory::new(
        "test_es".to_string(),
        endpoint,
        "logs".to_string(),
        vec![],
        false,
        ElasticsearchRequestMode::Buffered,
        stats.clone(),
    )
    .unwrap();
    let mut sink = factory.create().unwrap();

    let metadata = BatchMetadata {
        resource_attrs: Arc::from([]),
        observed_time_ns: 0,
    };

    // Send batch
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(sink.send_batch(&batch, &metadata)).unwrap();

    // Give server time to process
    std::thread::sleep(Duration::from_millis(200));

    let lines = total_lines.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(lines, 3, "expected 3 log records");

    // Verify stats were updated
    assert_eq!(
        stats.lines_total.load(std::sync::atomic::Ordering::Relaxed),
        3
    );
    assert!(stats.bytes_total.load(std::sync::atomic::Ordering::Relaxed) > 0);
}

#[test]
fn elasticsearch_sink_handles_empty_batch() {
    let (endpoint, _server) = start_blackhole();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "level_str",
        DataType::Utf8,
        false,
    )]));

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(Vec::<&str>::new()))],
    )
    .expect("batch creation failed");

    let stats = Arc::new(ComponentStats::default());
    let factory = ElasticsearchSinkFactory::new(
        "test_es".to_string(),
        endpoint,
        "logs".to_string(),
        vec![],
        false,
        ElasticsearchRequestMode::Buffered,
        stats.clone(),
    )
    .unwrap();
    let mut sink = factory.create().unwrap();

    let metadata = BatchMetadata {
        resource_attrs: Arc::from([]),
        observed_time_ns: 0,
    };

    // Send empty batch
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(sink.send_batch(&batch, &metadata)).unwrap();

    // Empty batch should not increment stats
    assert_eq!(
        stats.lines_total.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        stats.bytes_total.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[test]
fn elasticsearch_sink_multiple_batches() {
    let (endpoint, server) = start_blackhole();
    let total_lines = Arc::new(std::sync::atomic::AtomicU64::new(0));
    handle_bulk_requests(server, total_lines.clone());

    let schema = Arc::new(Schema::new(vec![
        Field::new("level_str", DataType::Utf8, false),
        Field::new("count_int", DataType::Int64, false),
    ]));

    let stats = Arc::new(ComponentStats::default());
    let factory = ElasticsearchSinkFactory::new(
        "test_es".to_string(),
        endpoint,
        "logs".to_string(),
        vec![],
        false,
        ElasticsearchRequestMode::Buffered,
        stats.clone(),
    )
    .unwrap();
    let mut sink = factory.create().unwrap();

    let metadata = BatchMetadata {
        resource_attrs: Arc::from([]),
        observed_time_ns: 0,
    };

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Send 3 batches
    for i in 0..3 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["INFO", "ERROR"])),
                Arc::new(Int64Array::from(vec![i, i + 100])),
            ],
        )
        .expect("batch creation failed");

        rt.block_on(sink.send_batch(&batch, &metadata)).unwrap();
    }

    // Give server time to process
    std::thread::sleep(Duration::from_millis(200));

    let lines = total_lines.load(std::sync::atomic::Ordering::Relaxed);

    // 3 batches × 2 rows each = 6 total records
    assert_eq!(lines, 6);
    assert_eq!(
        stats.lines_total.load(std::sync::atomic::Ordering::Relaxed),
        6
    );
}

#[test]
fn elasticsearch_streaming_mode_uses_chunked_transfer() {
    let (endpoint, server) = start_blackhole();
    let total_lines = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let saw_chunked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    handle_bulk_requests_with_transfer_encoding(server, total_lines.clone(), saw_chunked.clone());

    let schema = Arc::new(Schema::new(vec![
        Field::new("level_str", DataType::Utf8, false),
        Field::new("msg_str", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["INFO", "WARN", "ERROR"])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .expect("batch creation failed");

    let stats = Arc::new(ComponentStats::default());
    let factory = ElasticsearchSinkFactory::new(
        "test_es_streaming".to_string(),
        endpoint,
        "logs".to_string(),
        vec![],
        false,
        ElasticsearchRequestMode::Streaming,
        stats.clone(),
    )
    .unwrap();
    let mut sink = factory.create().unwrap();

    let metadata = BatchMetadata {
        resource_attrs: Arc::from([]),
        observed_time_ns: 0,
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(sink.send_batch(&batch, &metadata)).unwrap();

    std::thread::sleep(Duration::from_millis(200));

    assert_eq!(
        total_lines.load(std::sync::atomic::Ordering::Relaxed),
        3,
        "expected 3 log records",
    );
    assert!(
        saw_chunked.load(std::sync::atomic::Ordering::Relaxed),
        "streaming mode should use Transfer-Encoding: chunked",
    );
    assert_eq!(
        stats.lines_total.load(std::sync::atomic::Ordering::Relaxed),
        3,
    );
}

//! Fuzz the scanner-to-output-sink pipeline.
//!
//! Feeds arbitrary bytes through `CopyScanner` and then serializes the
//! resulting `RecordBatch` with both `JsonLinesSink` and `OtlpSink`.
//!
//! Verifies that:
//! - Neither serializer panics on any scanner output.
//! - `JsonLinesSink` produces valid UTF-8 (newline-delimited JSON).
//! - `OtlpSink` produces output whose length is consistent with the number
//!   of rows (empty batch → empty buffer).

#![no_main]
use libfuzzer_sys::fuzz_target;
use logfwd_core::scan_config::ScanConfig;
use logfwd_arrow::scanner::CopyScanner;
use logfwd_io::diagnostics::ComponentStats;
use logfwd_output::{BatchMetadata, Compression, JsonLinesSink, OtlpProtocol, OtlpSink};
use std::sync::Arc;

fuzz_target!(|data: &[u8]| {
    let mut scanner = CopyScanner::new(ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        keep_raw: false,
        validate_utf8: false,
    });
    let Ok(batch) = scanner.scan(data) else { return; };

    let metadata = BatchMetadata {
        resource_attrs: Arc::new(vec![]),
        observed_time_ns: 0,
    };

    // --- JSON lines serialization ---
    let mut json_sink = JsonLinesSink::new(
        "fuzz".to_string(),
        "http://localhost/".to_string(),
        vec![],
        Compression::None,
        Arc::new(ComponentStats::new()),
    );
    json_sink.serialize_batch(&batch);

    // Output must be valid UTF-8 (it is JSON).
    assert!(
        std::str::from_utf8(&json_sink.batch_buf).is_ok(),
        "JsonLinesSink produced non-UTF-8 output ({} bytes)",
        json_sink.batch_buf.len()
    );

    // An empty batch must produce an empty buffer.
    if batch.num_rows() == 0 {
        assert!(
            json_sink.batch_buf.is_empty(),
            "JsonLinesSink: non-empty output for 0-row batch"
        );
    }

    // --- OTLP protobuf encoding ---
    let mut otlp_sink = OtlpSink::new(
        "fuzz".to_string(),
        "http://localhost/".to_string(),
        OtlpProtocol::Http,
        Compression::None,
        vec![],
        Arc::new(ComponentStats::new()),
    );
    otlp_sink.encode_batch(&batch, &metadata);

    // An empty batch must produce an empty buffer.
    if batch.num_rows() == 0 {
        assert!(
            otlp_sink.encoder_buf.is_empty(),
            "OtlpSink: non-empty output for 0-row batch"
        );
    }
});

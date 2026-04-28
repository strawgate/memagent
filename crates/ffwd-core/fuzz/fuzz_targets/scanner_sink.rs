//! Fuzz the scanner-to-output-sink pipeline.
//!
//! Feeds arbitrary bytes through `Scanner` and then serializes the
//! resulting `RecordBatch` with both `JsonLinesSink` and `OtlpSink`.
//!
//! Verifies that neither serializer panics on any scanner output.
//! Errors from serialization are expected and ignored.

#![no_main]
use libfuzzer_sys::fuzz_target;
use std::sync::Arc;

use ffwd_arrow::scanner::Scanner;
use ffwd_core::scan_config::ScanConfig;
use ffwd_output::{BatchMetadata, Compression, JsonLinesSink, OtlpProtocol, OtlpSink};
use ffwd_types::diagnostics::ComponentStats;

fn run_sinks(data: &[u8], validate_utf8: bool, line_field_name: Option<&str>) {
    let mut scanner = Scanner::new(ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        line_field_name: line_field_name.map(ToString::to_string),
        validate_utf8,
        row_predicate: None,
    });
    let Ok(batch) = scanner.scan_detached(bytes::Bytes::copy_from_slice(data)) else {
        return;
    };

    let metadata = BatchMetadata {
        resource_attrs: Arc::from([]),
        observed_time_ns: 0,
    };

    let client = Arc::new(reqwest::Client::new());

    // --- JSON lines serialization ---
    let mut json_sink = JsonLinesSink::new(
        "fuzz".to_string(),
        "http://localhost/".to_string(),
        reqwest::header::HeaderMap::new(),
        Compression::None,
        client.clone(),
        Arc::new(ComponentStats::new()),
    );
    // serialize_batch returns io::Result - errors are expected (e.g., schema mismatch)
    let _ = json_sink.serialize_batch(&batch);

    // --- OTLP protobuf encoding ---
    let mut otlp_sink = match OtlpSink::new(
        "fuzz".to_string(),
        "http://localhost/".to_string(),
        OtlpProtocol::Http,
        Compression::None,
        vec![],
        (*client).clone(),
        Arc::new(ComponentStats::new()),
    ) {
        Ok(sink) => sink,
        Err(_) => return, // New can fail (e.g., Zstd init) - expected in fuzzing
    };
    otlp_sink.encode_batch(&batch, &metadata);
}

fuzz_target!(|data: &[u8]| {
    run_sinks(data, false, None);
    run_sinks(data, true, None);
    run_sinks(data, false, Some("body"));
    run_sinks(data, true, Some("body"));
});

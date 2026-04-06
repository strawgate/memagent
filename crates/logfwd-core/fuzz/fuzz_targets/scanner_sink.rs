#![no_main]
use libfuzzer_sys::fuzz_target;
use logfwd_core::scan_config::ScanConfig;
use logfwd_arrow::scanner::Scanner;
use logfwd_types::diagnostics::ComponentStats;
use logfwd_output::{BatchMetadata, Compression, OtlpProtocol, OtlpSink};
use std::sync::Arc;

fn run_sinks(data: &[u8], validate_utf8: bool) {
    let mut scanner = Scanner::new(ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        keep_raw: false,
        validate_utf8,
    });
    let Ok(batch) = scanner.scan_detached(bytes::Bytes::copy_from_slice(data)) else { return; };

    let metadata = BatchMetadata {
        resource_attrs: Arc::new(vec![]),
        observed_time_ns: 0,
    };

    let stats = Arc::new(ComponentStats::new());
    if let Ok(mut otlp_sink) = OtlpSink::new(
        "fuzz".to_string(),
        "http://localhost/".to_string(),
        OtlpProtocol::Http,
        Compression::None,
        vec![],
        reqwest::Client::new(),
        stats,
    ) {
        otlp_sink.encode_batch(&batch, &metadata);
    }
}

fuzz_target!(|data: &[u8]| {
    run_sinks(data, false);
    run_sinks(data, true);
});

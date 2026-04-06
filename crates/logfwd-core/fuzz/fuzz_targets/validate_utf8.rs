#![no_main]
use libfuzzer_sys::fuzz_target;
use logfwd_arrow::scanner::Scanner;
use logfwd_core::scan_config::ScanConfig;

fuzz_target!(|data: &[u8]| {
    // Extract-all mode with UTF-8 validation.
    let config = ScanConfig {
        wanted_fields: vec![],
        extract_all: true,
        keep_raw: true,
        validate_utf8: true,
    };
    let mut scanner = Scanner::new(config);
    let _ = scanner.scan_detached(bytes::Bytes::copy_from_slice(data));
});

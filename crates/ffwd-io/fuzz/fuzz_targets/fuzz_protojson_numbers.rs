#![no_main]

use libfuzzer_sys::fuzz_target;
use ffwd_io::otlp_receiver::{
    fuzz_decode_protojson_bytes, fuzz_parse_protojson_f64, fuzz_parse_protojson_i64,
    fuzz_parse_protojson_u64,
};

const MAX_INPUT: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }

    // Path 1: raw bytes as potential JSON value.
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(data) {
        let _ = fuzz_parse_protojson_i64(&value);
        let _ = fuzz_parse_protojson_u64(&value);
        let _ = fuzz_parse_protojson_f64(&value);

        if let Some(s) = value.as_str() {
            let _ = fuzz_decode_protojson_bytes(s);
        }
    }

    // Path 2: parse as UTF-8 scalar string for numeric edge-cases.
    if let Ok(s) = std::str::from_utf8(data) {
        let as_json_string = serde_json::Value::String(s.to_string());
        let _ = fuzz_parse_protojson_i64(&as_json_string);
        let _ = fuzz_parse_protojson_u64(&as_json_string);
        let _ = fuzz_parse_protojson_f64(&as_json_string);
        let _ = fuzz_decode_protojson_bytes(s);
    }
});

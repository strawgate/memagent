#![no_main]

use libfuzzer_sys::fuzz_target;
use ffwd_io::otlp_receiver::fuzz_decode_otlp_json;
use ffwd_types::field_names;

const MAX_INPUT: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }

    let _ = fuzz_decode_otlp_json(data, field_names::DEFAULT_RESOURCE_PREFIX);
});

#![no_main]

use libfuzzer_sys::fuzz_target;
use ffwd_io::otap_receiver::fuzz_decode_batch_arrow_records;

const MAX_INPUT: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }

    let _ = fuzz_decode_batch_arrow_records(data);
});

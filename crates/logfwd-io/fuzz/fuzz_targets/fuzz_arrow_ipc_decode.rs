#![no_main]

use libfuzzer_sys::fuzz_target;
use logfwd_io::arrow_ipc_receiver::fuzz_decode_arrow_ipc_stream;

const MAX_INPUT: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }

    let _ = fuzz_decode_arrow_ipc_stream(data);
});

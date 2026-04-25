#![no_main]

// Trust boundary: decode_ipc_stream
use libfuzzer_sys::fuzz_target;
use ffwd_io::arrow_ipc_receiver::fuzz_decode_arrow_ipc_stream;

const MAX_INPUT: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }

    let _ = fuzz_decode_arrow_ipc_stream(data);
});

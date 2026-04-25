#![no_main]

use flate2::Compression;
use flate2::write::GzEncoder;
use libfuzzer_sys::fuzz_target;
use ffwd_io::otlp_receiver::{fuzz_decompress_gzip, fuzz_decompress_zstd};
use std::io::Write;

const MAX_INPUT: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }

    let _ = fuzz_decompress_gzip(data, MAX_INPUT);
    let _ = fuzz_decompress_zstd(data, MAX_INPUT);

    // Also exercise valid compressed paths by compressing a bounded prefix.
    let prefix_len = data.len().min(8 * 1024);
    let prefix = &data[..prefix_len];

    let mut gzip_buf = Vec::new();
    let mut encoder = GzEncoder::new(&mut gzip_buf, Compression::default());
    if encoder.write_all(prefix).is_ok() && encoder.finish().is_ok() {
        let _ = fuzz_decompress_gzip(&gzip_buf, MAX_INPUT);
    }

    if let Ok(zstd_buf) = zstd::bulk::compress(prefix, 1) {
        let _ = fuzz_decompress_zstd(&zstd_buf, MAX_INPUT);
    }
});

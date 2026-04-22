//! Targeted fuzz target for streaming structural classification.
//!
//! Exercises the live scanner structural path: SIMD/scalar character
//! detection, `StreamingClassifier` cross-block state, and tail masking.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use logfwd_arrow::scanner::Scanner;
use logfwd_core::scan_config::ScanConfig;
use logfwd_core::structural::{
    StreamingClassifier, find_structural_chars, find_structural_chars_scalar,
};

fuzz_target!(|data: &[u8]| {
    let mut classifier = StreamingClassifier::new();
    let num_blocks = data.len().div_ceil(64);

    for block_idx in 0..num_blocks {
        let offset = block_idx * 64;
        let remaining = data.len() - offset;
        let block_len = remaining.min(64);
        let mut block = [b' '; 64];
        block[..block_len].copy_from_slice(&data[offset..offset + block_len]);

        let scalar = find_structural_chars_scalar(&block);
        let simd = find_structural_chars(&block);
        assert_eq!(scalar, simd, "SIMD structural masks must match scalar");

        let processed = classifier.process_block(&simd, block_len);
        assert_eq!(processed.real_quotes & !simd.quote, 0);
        assert_eq!(processed.space & processed.in_string, 0);
        assert_eq!(processed.comma & processed.in_string, 0);
        assert_eq!(processed.colon & processed.in_string, 0);
        assert_eq!(processed.open_brace & processed.in_string, 0);
        assert_eq!(processed.close_brace & processed.in_string, 0);
        assert_eq!(processed.open_bracket & processed.in_string, 0);
        assert_eq!(processed.close_bracket & processed.in_string, 0);

        if block_len < 64 {
            let tail = !((1u64 << block_len) - 1);
            assert_eq!(processed.newline & tail, 0);
            assert_eq!(processed.space & tail, 0);
            assert_eq!(processed.real_quotes & tail, 0);
            assert_eq!(processed.in_string & tail, 0);
            assert_eq!(processed.comma & tail, 0);
            assert_eq!(processed.colon & tail, 0);
            assert_eq!(processed.open_brace & tail, 0);
            assert_eq!(processed.close_brace & tail, 0);
            assert_eq!(processed.open_bracket & tail, 0);
            assert_eq!(processed.close_bracket & tail, 0);
        }
    }

    let config = ScanConfig {
        validate_utf8: true,
        ..ScanConfig::default()
    };
    let mut scanner = Scanner::new(config);
    let Ok(batch) = scanner.scan_detached(Bytes::copy_from_slice(data)) else {
        return;
    };
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    for col_idx in 0..batch.num_columns() {
        assert_eq!(
            batch.column(col_idx).len(),
            num_rows,
            "column '{}' length {} != num_rows {num_rows}",
            schema.field(col_idx).name(),
            batch.column(col_idx).len(),
        );
    }
});

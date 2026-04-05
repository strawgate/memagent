//! Targeted fuzz target for `StructuralIndex` escape-detection logic.
//!
//! `StructuralIndex` uses portable SIMD (via `wide` crate) and `prefix_xor`-based bitmask
//! operations to locate unescaped quote positions. The escape-detection
//! algorithm has known subtle behaviour at 64-byte block boundaries (e.g. a
//! backslash at byte 63 must carry over into the next block).
//!
//! This target:
//! 1. Constructs a `StructuralIndex` from arbitrary bytes.
//! 2. Exercises all public query methods (`is_in_string`, `next_quote`,
//!    `scan_string`, `skip_nested`) at every byte position to ensure no
//!    out-of-bounds access or panic.
//! 3. Passes the same bytes through `Scanner` to exercise the full
//!    scanner pipeline built on top of `StructuralIndex`.
//!
//! The corpus should include inputs with dense backslash runs near multiples
//! of 64 bytes (e.g. 63 × `\` followed by `"`) to stress the cross-block
//! carry logic.

#![no_main]
use libfuzzer_sys::fuzz_target;
use logfwd_core::structural::StructuralIndex;
use logfwd_core::scan_config::ScanConfig;
use logfwd_arrow::scanner::Scanner;

fuzz_target!(|data: &[u8]| {
    // --- Direct StructuralIndex API exercise ---
    let (index, _line_ranges) = StructuralIndex::new(data);

    for i in 0..data.len() {
        // is_in_string must not panic for any valid byte position.
        let _ = index.is_in_string(i);

        // next_quote must not panic.
        let _ = index.next_quote(i);

        // scan_string is only meaningful when starting at a `"` byte.
        if data[i] == b'"' {
            if let Some((val, after)) = index.scan_string(data, i, data.len()) {
                // Returned slice must be a valid sub-slice of `data`.
                assert!(after <= data.len(), "scan_string: after={after} > len={}", data.len());
                let val_start = val.as_ptr() as usize;
                let data_start = data.as_ptr() as usize;
                assert!(
                    val_start >= data_start && val_start + val.len() <= data_start + data.len(),
                    "scan_string: returned slice out of bounds"
                );
            }
        }

        // skip_nested is only meaningful for `{` or `[`.
        if data[i] == b'{' || data[i] == b'[' {
            let end = index.skip_nested(data, i, data.len());
            assert!(end <= data.len(), "skip_nested: end={end} > len={}", data.len());
        }
    }

    // --- Full scanner pipeline (uses StructuralIndex internally) ---
    for validate_utf8 in [false, true] {
        let config = ScanConfig {
            validate_utf8,
            ..ScanConfig::default()
        };
        let mut scanner = Scanner::new(config);
        let Ok(batch) = scanner.scan_detached(bytes::Bytes::copy_from_slice(data)) else { continue; };
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
    }
});

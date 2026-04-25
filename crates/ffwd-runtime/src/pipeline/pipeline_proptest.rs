//! Property-based tests for pipeline split/scan and CRI P/F sequences.

use super::*;
use ffwd_core::scan_config::ScanConfig;
use proptest::prelude::*;

/// Generate random NDJSON: N lines of {"k":"v"} with random key/value lengths.
fn arb_ndjson(max_lines: usize) -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(("[a-z]{1,8}", "[a-zA-Z0-9 ]{0,50}"), 1..=max_lines).prop_map(
        |fields| {
            let mut buf = Vec::new();
            for (key, val) in fields {
                buf.extend_from_slice(b"{\"");
                buf.extend_from_slice(key.as_bytes());
                buf.extend_from_slice(b"\":\"");
                buf.extend_from_slice(val.as_bytes());
                buf.extend_from_slice(b"\"}\n");
            }
            buf
        },
    )
}

proptest! {
    /// Split arbitrary NDJSON at an arbitrary point. Both halves
    /// processed with remainder handling should produce the same
    /// RecordBatch row count as processing the whole buffer at once.
    #[test]
    #[allow(unused_assignments, unused_variables)]
    fn split_anywhere_same_row_count(
        ndjson in arb_ndjson(20),
        split_pct in 1u8..99u8,
    ) {
        let split_at = (ndjson.len() as u64 * split_pct as u64 / 100) as usize;
        let split_at = split_at.max(1).min(ndjson.len() - 1);

        // Whole buffer at once
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: None,
        };
        let mut scanner_whole = Scanner::new(config);
        let batch_whole = scanner_whole.scan_detached(Bytes::from(ndjson.clone())).unwrap();

        // Split into two chunks with remainder handling
        let chunk1 = &ndjson[..split_at];
        let chunk2 = &ndjson[split_at..];

        let mut buf = Vec::new();
        let mut remainder: Vec<u8> = Vec::new();

        // Process chunk1
        let mut combined = remainder;
        combined.extend_from_slice(chunk1);
        if let Some(pos) = memchr::memrchr(b'\n', &combined) {
            if pos + 1 < combined.len() {
                remainder = combined[pos + 1..].to_vec();
                combined.truncate(pos + 1);
            } else {
                remainder = Vec::new();
            }
            buf.extend_from_slice(&combined);
        } else {
            remainder = combined;
        }

        // Process chunk2
        let mut combined = remainder;
        combined.extend_from_slice(chunk2);
        if let Some(pos) = memchr::memrchr(b'\n', &combined) {
            if pos + 1 < combined.len() {
                combined.truncate(pos + 1);
            }
            buf.extend_from_slice(&combined);
        }

        let config2 = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: None,
        };
        let mut scanner_split = Scanner::new(config2);
        let batch_split = scanner_split.scan_detached(Bytes::from(buf.clone())).unwrap();

        prop_assert_eq!(
            batch_whole.num_rows(),
            batch_split.num_rows(),
            "split at {} of {}: row count mismatch",
            split_at,
            ndjson.len()
        );
    }

    /// CRI P/F sequences with random message content should always
    /// produce the same number of complete messages.
    #[test]
    fn cri_pf_random_sequences(
        messages in proptest::collection::vec("[a-zA-Z0-9]{1,30}", 1..=10),
        partials in proptest::collection::vec(0u8..3u8, 1..=10),
    ) {
        let mut input = Vec::new();
        let mut expected_count = 0usize;

        for (i, msg) in messages.iter().enumerate() {
            let num_partials = if i < partials.len() { partials[i] as usize } else { 0 };
            let msg_bytes = msg.as_bytes();

            if num_partials == 0 || msg_bytes.len() < 2 {
                // Single F line
                input.extend_from_slice(
                    format!("2024-01-15T10:30:{:02}Z stdout F {}\n", i % 60, msg).as_bytes(),
                );
                expected_count += 1;
            } else {
                // Split into P chunks + final F
                let chunk_size = (msg_bytes.len() / (num_partials + 1)).max(1);
                let mut offset = 0;
                for _ in 0..num_partials {
                    let end = (offset + chunk_size).min(msg_bytes.len());
                    let chunk = &msg[offset..end];
                    input.extend_from_slice(
                        format!("2024-01-15T10:30:{:02}Z stdout P {}\n", i % 60, chunk)
                            .as_bytes(),
                    );
                    offset = end;
                }
                let remaining = &msg[offset..];
                input.extend_from_slice(
                    format!("2024-01-15T10:30:{:02}Z stdout F {}\n", i % 60, remaining)
                        .as_bytes(),
                );
                expected_count += 1;
            }
        }

        let mut out = Vec::new();
        let stats = Arc::new(ComponentStats::new());
        let mut fmt = FormatDecoder::cri(1024 * 1024, Arc::clone(&stats));
        fmt.process_lines(&input, &mut out);

        let line_count = out.iter().filter(|&&b| b == b'\n').count();
        prop_assert_eq!(
            line_count,
            expected_count,
            "expected {} complete messages, got {}",
            expected_count,
            line_count
        );
    }
}

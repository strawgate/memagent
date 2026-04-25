//! Fuzz the CRI parser and reassembler with arbitrary bytes.
//!
//! The CRI parser is the first code to touch raw log data in Kubernetes
//! deployments. This target exercises:
//! - `parse_cri_line` — single-line parsing of untrusted container runtime output.
//! - `CriReassembler` — partial line reassembly (P/F flag handling).
//!
//! Verifies that no combination of input bytes causes a panic.

#![no_main]
use libfuzzer_sys::fuzz_target;
use ffwd_core::cri::{AggregateResult, CriReassembler, parse_cri_line};

fuzz_target!(|data: &[u8]| {
    // --- Single-line parsing ---
    if let Some(cri) = parse_cri_line(data) {
        // Exercise field access on successfully parsed lines.
        let _ts = cri.timestamp;
        let _stream = cri.stream;
        let _full = cri.is_full;
        let _msg = cri.message;
    }

    // --- Chunk processing via parse_cri_line + CriReassembler ---
    let mut reassembler = CriReassembler::new(1024 * 1024);
    let mut count = 0usize;
    for line in data.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        if let Some(cri) = parse_cri_line(line) {
            match reassembler.feed(cri.message, cri.is_full) {
                AggregateResult::Complete(msg) => {
                    let _ = msg.len();
                    count += 1;
                    reassembler.reset();
                }
                AggregateResult::Truncated(msg) => {
                    let _ = msg.len();
                    count += 1;
                    reassembler.reset();
                }
                AggregateResult::Pending => {}
            }
        }
    }
    let _ = count;
});

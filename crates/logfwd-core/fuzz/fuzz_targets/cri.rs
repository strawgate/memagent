//! Fuzz the CRI parser and reassembler with arbitrary bytes.
//!
//! The CRI parser is the first code to touch raw log data in Kubernetes
//! deployments. This target exercises:
//! - `parse_cri_line` — single-line parsing of untrusted container runtime output.
//! - `CriReassembler` — partial line reassembly (P/F flag handling).
//! - `process_cri_to_buf` — full chunk processing pipeline including JSON
//!   prefix injection.
//!
//! Verifies that no combination of input bytes causes a panic.

#![no_main]
use libfuzzer_sys::fuzz_target;
use logfwd_core::cri::{parse_cri_line, process_cri_to_buf, CriReassembler};

fuzz_target!(|data: &[u8]| {
    // --- Single-line parsing ---
    if let Some(cri) = parse_cri_line(data) {
        // Exercise field access on successfully parsed lines.
        let _ts = cri.timestamp;
        let _stream = cri.stream;
        let _full = cri.is_full;
        let _msg = cri.message;
    }

    // --- Chunk processing without JSON prefix ---
    let mut reassembler = CriReassembler::new(1024 * 1024);
    let mut out = Vec::new();
    let (_count, _errors) = process_cri_to_buf(data, &mut reassembler, None, &mut out);

    // --- Chunk processing with a JSON prefix ---
    let mut reassembler2 = CriReassembler::new(1024 * 1024);
    let mut out2 = Vec::new();
    let prefix = b"\"k8s.pod\":\"fuzz\",";
    let (_count2, _errors2) =
        process_cri_to_buf(data, &mut reassembler2, Some(prefix), &mut out2);
});

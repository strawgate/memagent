//! Proven pure-logic kernel for logfwd.
//!
//! `#![no_std]` + `#![forbid(unsafe_code)]` — the compiler enforces
//! no IO, no filesystem, no threads, no unsafe. All parsing, encoding,
//! and structural detection logic lives here with Kani proofs.
#![no_std]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

extern crate alloc;

/// CRI partial line aggregator (P/F reassembly).
pub mod aggregator;
/// Proven byte search (alternative to memchr for Kani).
pub mod byte_search;
/// CRI log format parsing.
pub mod cri;
/// Newline framing (Kani-proven).
pub mod framer;
/// OTLP protobuf encoding helpers and parsers.
pub mod otlp;
/// Scanner configuration and field selection.
pub mod scan_config;
/// JSON-to-columnar scan loop.
pub mod scanner;
/// Streaming SIMD structural character detection.
pub mod structural;

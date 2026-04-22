//! Proven pure-logic kernel for logfwd.
//!
//! `#![no_std]` + `#![forbid(unsafe_code)]` — the compiler enforces
//! no IO, no filesystem, no threads, no unsafe. All parsing, encoding,
//! and structural detection logic lives here with Kani proofs.
#![no_std]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::panic)]

extern crate alloc;

/// Proven byte search (alternative to memchr for Kani).
pub mod byte_search;
/// Pure state machine for checkpoint-remainder coordination (Kani-proven).
pub mod checkpoint_tracker;
/// CRI log format parsing.
pub mod cri;
/// Per-crate error types.
pub mod error;
/// Newline framing (Kani-proven).
pub mod framer;
/// Streaming JSON field scanner using StructuralIter.
pub mod json_scanner;
/// OTLP protobuf encoding helpers and parsers.
pub mod otlp;
/// CRI partial line reassembler (P/F reassembly).
pub mod reassembler;
/// Scanner configuration and field selection.
pub mod scan_config;
/// Scanner-to-builder protocol boundary.
pub mod scanner;
/// Streaming SIMD structural character detection.
pub mod structural;
/// Streaming structural position iterator (format-agnostic).
pub mod structural_iter;

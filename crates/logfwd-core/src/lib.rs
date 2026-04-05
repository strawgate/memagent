//! Proven pure-logic kernel for logfwd.
//!
//! `#![no_std]` + `#![forbid(unsafe_code)]` — the compiler enforces
//! no IO, no filesystem, no threads, no unsafe. All parsing, encoding,
//! and structural detection logic lives here with Kani proofs.
#![no_std]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

extern crate alloc;

/// CRI partial line reassembler (P/F reassembly).
pub mod reassembler;
/// Backward-compat re-export so downstream `logfwd_core::aggregator::*` still compiles.
pub use reassembler as aggregator;
/// Proven byte search (alternative to memchr for Kani).
pub mod byte_search;
/// Pure state machine for checkpoint-remainder coordination (Kani-proven).
pub mod checkpoint_tracker;
/// CRI log format parsing.
pub mod cri;
/// Newline framing (Kani-proven).
pub mod framer;
/// Streaming JSON field scanner using StructuralIter.
pub mod json_scanner;
/// OTLP protobuf encoding helpers and parsers.
pub mod otlp;
/// Pipeline state machine — typestate batch lifecycle + ordered offset tracking.
///
/// Re-exported from [`logfwd_types::pipeline`] for backward compatibility.
pub use logfwd_types::pipeline;
/// Per-crate error types.
pub mod error;
/// Scanner configuration and field selection.
pub mod scan_config;
/// JSON-to-columnar scan loop.
pub mod scanner;
/// Streaming SIMD structural character detection.
pub mod structural;
/// Streaming structural position iterator (format-agnostic).
pub mod structural_iter;

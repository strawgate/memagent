//! Stable pipeline types for ffwd.
//!
//! Provides cross-cutting types that multiple crates need without pulling in
//! heavy I/O or transform dependencies. Includes pure state-machine types
//! (pipeline) and lock-free diagnostic counters.
#![warn(missing_docs)]

extern crate alloc;

/// Pipeline state machine — typestate batch lifecycle + ordered offset tracking.
pub mod pipeline;

/// Component-level diagnostic counters (lock-free, hot-path friendly).
pub mod diagnostics;

/// Hints that input sources and parsers can use to filter early.
pub mod filter_hints;

/// Canonical OTLP log field names — single source of truth for receiver ↔ sink column names.
pub mod field_names;

/// Source metadata attachment policy shared by runtime and transform crates.
pub mod source_metadata;

/// Unified I/O action classification — retry, reject, or fatal.
pub mod io_action;

//! Stable pipeline types for logfwd.
//!
//! `#![no_std]` + `#![forbid(unsafe_code)]` — pure types and state machines
//! with no IO, no filesystem, no threads, no unsafe. Extracted from
//! `logfwd-core` so SIMD scanner changes don't cascade rebuilds to
//! downstream crates that only need pipeline types.
#![no_std]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

extern crate alloc;

/// Pipeline state machine — typestate batch lifecycle + ordered offset tracking.
pub mod pipeline;

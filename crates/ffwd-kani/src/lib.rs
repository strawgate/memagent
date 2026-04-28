//! Shared oracle and reference implementations for Kani formal verification.
//!
//! This crate provides mathematically simple reference implementations
//! ("oracles") that Kani proofs use to verify the correctness of optimized
//! production functions. Oracles are intentionally simple and readable—they
//! prioritize clarity over performance.
//!
//! # Architecture
//!
//! ```text
//! production code (ffwd-core)    oracle (ffwd-kani)
//!         │                              │
//!         └──── Kani proof asserts ──────┘
//!               output_prod == output_oracle
//! ```
//!
//! Production functions stay in their original crates. This crate is only
//! called from `#[cfg(kani)]` verification blocks and tests—never from
//! production code paths.

#![no_std]
#![warn(missing_docs)]
// Oracle functions are intentionally simple; some use manual loops for Kani
// compatibility, leading to patterns clippy would normally flag.
#![allow(clippy::manual_is_ascii_check, clippy::indexing_slicing)]

pub mod bytes;
pub mod datetime;
pub mod hex;
pub mod iter;
pub mod numeric;
pub mod proto;

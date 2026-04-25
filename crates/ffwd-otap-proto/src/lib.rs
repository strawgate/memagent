// Generated protobuf code — prost does not derive Eq.
#![allow(clippy::derive_partial_eq_without_eq)]
//! Generated protobuf message types for OTAP transport-edge messages.
//!
//! This crate intentionally keeps codegen at a small protocol boundary so the
//! receiver and sink can share one schema without spreading generation through
//! the rest of the repo.

pub mod otap {
    include!(concat!(env!("OUT_DIR"), "/otap.rs"));
}

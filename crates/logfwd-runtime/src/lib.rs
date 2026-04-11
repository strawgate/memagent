//! Runtime orchestration for logfwd pipelines.
//!
//! This crate holds the long-lived pipeline event loop, output worker pool,
//! and in-process processor chain so the `logfwd` package can focus on CLI
//! entrypoints and compatibility re-exports.

pub mod bootstrap;
pub mod generated_cli;
pub mod pipeline;
pub mod processor;
pub mod transform;
#[cfg(feature = "turmoil")]
pub mod turmoil_barriers;
pub mod worker_pool;

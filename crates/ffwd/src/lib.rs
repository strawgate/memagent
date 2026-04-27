#![allow(clippy::print_stdout, clippy::print_stderr, clippy::indexing_slicing)]
// Binary facade: CLI-adjacent paths may print directly.

pub use ffwd_runtime::{pipeline, processor, worker_pool};
pub mod transform;

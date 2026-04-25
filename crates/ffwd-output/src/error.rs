//! Per-crate error types for the ffwd-output layer.

use thiserror::Error;

/// Errors from output sink construction and factory functions.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OutputError {
    /// Underlying I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialize(String),
    /// Sink construction error (missing config, invalid settings).
    #[error("sink construction error: {0}")]
    Construction(String),
}

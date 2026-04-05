//! Per-crate error types for the logfwd-io layer.

use thiserror::Error;

/// Errors from input sources, enrichment loaders, and protocol receivers.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum InputError {
    /// Underlying I/O error (file, network, compression).
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Configuration or validation error.
    #[error("config error: {0}")]
    Config(String),
    /// Protocol receiver error (protobuf, Arrow IPC, JSON decoding).
    #[error("receiver error: {0}")]
    Receiver(String),
}

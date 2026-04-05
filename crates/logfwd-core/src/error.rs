//! Per-crate error types for the logfwd-core kernel.

use thiserror::Error;

/// Errors from CRI / JSON / structural parsing.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ParseError {
    /// The input does not match the expected format.
    #[error("invalid format: {0}")]
    InvalidFormat(&'static str),
    /// The input buffer was exhausted before a complete record was found.
    #[error("buffer exhausted")]
    BufferExhausted,
    /// The input was truncated mid-record.
    #[error("truncated input")]
    Truncated,
}

/// Errors from protobuf / OTLP encoding.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CodecError {
    /// Protobuf encoding failed.
    #[error("protobuf encoding failed: {0}")]
    Encode(&'static str),
    /// A varint exceeded the maximum representable value.
    #[error("varint overflow")]
    VarintOverflow,
}

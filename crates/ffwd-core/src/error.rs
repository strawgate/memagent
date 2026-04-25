//! Per-crate error types for the ffwd-core kernel.

use core::error::Error;
use core::fmt;

/// Errors from CRI / JSON / structural parsing.
#[derive(Debug)]
#[non_exhaustive]
pub enum ParseError {
    /// The input does not match the expected format.
    InvalidFormat(&'static str),
    /// The input buffer was exhausted before a complete record was found.
    BufferExhausted,
    /// The input was truncated mid-record.
    Truncated,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFormat(message) => write!(f, "invalid format: {message}"),
            Self::BufferExhausted => f.write_str("buffer exhausted"),
            Self::Truncated => f.write_str("truncated input"),
        }
    }
}

impl Error for ParseError {}

/// Errors from protobuf / OTLP encoding.
#[derive(Debug)]
#[non_exhaustive]
pub enum CodecError {
    /// Protobuf encoding failed.
    Encode(&'static str),
    /// A varint exceeded the maximum representable value.
    VarintOverflow,
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode(message) => write!(f, "protobuf encoding failed: {message}"),
            Self::VarintOverflow => f.write_str("varint overflow"),
        }
    }
}

impl Error for CodecError {}

#[cfg(test)]
mod tests {
    use alloc::format;

    use super::{CodecError, ParseError};

    #[test]
    fn parse_error_messages_match_previous_behavior() {
        assert_eq!(
            format!("{}", ParseError::InvalidFormat("oops")),
            "invalid format: oops"
        );
        assert_eq!(
            format!("{}", ParseError::BufferExhausted),
            "buffer exhausted"
        );
        assert_eq!(format!("{}", ParseError::Truncated), "truncated input");
    }

    #[test]
    fn codec_error_messages_match_previous_behavior() {
        assert_eq!(
            format!("{}", CodecError::Encode("bad proto")),
            "protobuf encoding failed: bad proto"
        );
        assert_eq!(format!("{}", CodecError::VarintOverflow), "varint overflow");
    }
}

//! Compression detection and decompression for S3 objects.

use std::io::{self, Read};

use bytes::Bytes;

/// Compression format for an S3 object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// gzip / deflate (RFC 1952).
    Gzip,
    /// Zstandard frame format.
    Zstd,
    /// Snappy framing format.
    Snappy,
    /// No compression — return bytes as-is.
    None,
}

impl Compression {
    /// Parse a user-supplied compression override string.
    ///
    /// Returns `None` if the string is not recognized.
    pub fn from_config_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "gzip" | "gz" => Some(Self::Gzip),
            "zstd" | "zst" => Some(Self::Zstd),
            "snappy" | "sz" => Some(Self::Snappy),
            "none" | "identity" => Some(Self::None),
            _ => None,
        }
    }
}

/// Detect compression from the object key extension, optional `Content-Encoding`,
/// and optional `Content-Type`.
///
/// Priority: content-encoding > key extension > content-type > `Compression::None`.
#[allow(clippy::case_sensitive_file_extension_comparisons)]
pub fn detect_compression(
    key: &str,
    content_encoding: Option<&str>,
    content_type: Option<&str>,
) -> Compression {
    // Content-Encoding is the most authoritative signal.
    if let Some(ce) = content_encoding {
        let ce = ce.to_ascii_lowercase();
        if ce.contains("gzip") {
            return Compression::Gzip;
        }
        if ce.contains("zstd") {
            return Compression::Zstd;
        }
        if ce.contains("snappy") {
            return Compression::Snappy;
        }
    }

    // Check key extension (most reliable for S3 objects).
    let key_lower = key.to_ascii_lowercase();
    if key_lower.ends_with(".gz") || key_lower.ends_with(".gzip") {
        return Compression::Gzip;
    }
    if key_lower.ends_with(".zst") || key_lower.ends_with(".zstd") {
        return Compression::Zstd;
    }
    if key_lower.ends_with(".snappy") || key_lower.ends_with(".sz") {
        return Compression::Snappy;
    }

    // Fall back to Content-Type.
    if let Some(ct) = content_type {
        let ct = ct.to_ascii_lowercase();
        if ct.contains("gzip") || ct.contains("x-gzip") {
            return Compression::Gzip;
        }
        if ct.contains("zstd") || ct.contains("zst") {
            return Compression::Zstd;
        }
        if ct.contains("snappy") {
            return Compression::Snappy;
        }
    }

    Compression::None
}

/// Decompress `data` according to `compression`.
///
/// Reads the full decompressed output into a `Vec<u8>`. For streaming
/// decoders (gzip, zstd, snappy) this avoids materialising the intermediate
/// compressed form twice.
pub fn decompress(data: Bytes, compression: Compression) -> io::Result<Vec<u8>> {
    match compression {
        Compression::None => Ok(data.to_vec()),
        Compression::Gzip => {
            let mut decoder = flate2::read::MultiGzDecoder::new(data.as_ref());
            let mut out = Vec::new();
            decoder.read_to_end(&mut out)?;
            Ok(out)
        }
        Compression::Zstd => {
            let mut decoder = zstd::stream::read::Decoder::new(data.as_ref())
                .map_err(|e| io::Error::other(format!("zstd decoder init: {e}")))?;
            let mut out = Vec::new();
            decoder.read_to_end(&mut out)?;
            Ok(out)
        }
        Compression::Snappy => {
            let mut decoder = snap::read::FrameDecoder::new(data.as_ref());
            let mut out = Vec::new();
            decoder.read_to_end(&mut out)?;
            Ok(out)
        }
    }
}

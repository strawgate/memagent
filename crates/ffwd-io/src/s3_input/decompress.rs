//! Compression detection and decompression for S3 objects.
// xtask-verify: allow(pub_module_needs_tests) reason: covered by higher-level s3 input integration tests; focused unit tests pending

use std::io::{self, Read};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::io::{AsyncBufRead, AsyncRead, BufReader, ReadBuf};

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
        if ce == "identity" {
            return Compression::None;
        }
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

/// Type-erased async reader for streaming decompression.
pub type AsyncDecompressReader = Pin<Box<dyn AsyncRead + Send>>;

/// Wraps a `DuplexStream` reader and checks for a producer error on EOF.
///
/// When the snappy decompression task encounters an error, it stores it in
/// the shared `error` slot and drops the writer half. Without this wrapper
/// the reader would see a clean EOF, causing the caller to treat truncated
/// data as a successful read. With this wrapper the stored error is
/// surfaced on the first zero-length read.
struct ErrorAwareReader {
    inner: tokio::io::DuplexStream,
    error: Arc<Mutex<Option<io::Error>>>,
}

impl AsyncRead for ErrorAwareReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) if buf.filled().len() == before => {
                // Zero-length read (EOF) — check if the producer stored an error.
                if let Some(err) = this
                    .error
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                {
                    return Poll::Ready(Err(err));
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

/// Wrap an `AsyncBufRead` source in the appropriate streaming decompressor.
///
/// For `Compression::None`, returns the reader as-is (no copy).
/// For gzip/zstd, wraps in `async-compression` streaming decoders.
/// For snappy, falls back to buffered sync decompression (no async snappy
/// decoder in `async-compression`). Errors are propagated through an
/// `ErrorAwareReader` wrapper so decode failures surface as read errors.
pub fn wrap_decompressor<R: AsyncBufRead + Send + 'static>(
    reader: R,
    compression: Compression,
) -> AsyncDecompressReader {
    match compression {
        Compression::None => Box::pin(reader),
        Compression::Gzip => Box::pin(async_compression::tokio::bufread::GzipDecoder::new(reader)),
        Compression::Zstd => Box::pin(async_compression::tokio::bufread::ZstdDecoder::new(reader)),
        Compression::Snappy => {
            // async-compression doesn't support snappy framing format.
            // Use a duplex pipe: a spawned task reads compressed data, decodes
            // with snap, and writes decompressed bytes to the writer half.
            // Errors are stored in a shared slot and surfaced via ErrorAwareReader.
            let (async_reader, writer) = tokio::io::duplex(256 * 1024);
            let error_slot: Arc<Mutex<Option<io::Error>>> = Arc::new(Mutex::new(None));
            let task_error = Arc::clone(&error_slot);
            let buf_reader = Box::pin(BufReader::new(reader));

            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf_reader = buf_reader;
                let mut writer = writer;
                let mut compressed = Vec::new();
                if let Err(e) = buf_reader.read_to_end(&mut compressed).await {
                    if let Ok(mut guard) = task_error.lock() {
                        *guard = Some(io::Error::other(format!(
                            "snappy: read compressed data: {e}"
                        )));
                    }
                    return;
                }
                let mut decoder = snap::read::FrameDecoder::new(compressed.as_slice());
                let mut buf = vec![0u8; 256 * 1024];
                loop {
                    match decoder.read(&mut buf) {
                        Ok(0) => break,
                        Ok(n) => {
                            if writer.write_all(&buf[..n]).await.is_err() {
                                break; // Reader dropped — shutting down.
                            }
                        }
                        Err(e) => {
                            if let Ok(mut guard) = task_error.lock() {
                                *guard = Some(io::Error::other(format!("snappy decompress: {e}")));
                            }
                            return;
                        }
                    }
                }
            });

            Box::pin(ErrorAwareReader {
                inner: async_reader,
                error: error_slot,
            })
        }
    }
}

/// Wrap a reqwest streaming response body into an `AsyncBufRead`.
///
/// Converts `bytes_stream()` → `StreamReader` → `BufReader`.
pub fn response_to_async_read(
    resp: reqwest::Response,
) -> BufReader<
    tokio_util::io::StreamReader<impl futures_util::Stream<Item = Result<Bytes, io::Error>>, Bytes>,
> {
    use futures_util::StreamExt;
    let stream = resp
        .bytes_stream()
        .map(|r| r.map_err(|e| io::Error::other(format!("S3 stream read: {e}"))));
    BufReader::with_capacity(256 * 1024, tokio_util::io::StreamReader::new(stream))
}

/// Decompress `data` synchronously (used by benchmarks).
///
/// Reads the full decompressed output into a `Vec<u8>`.
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

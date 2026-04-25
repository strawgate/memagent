//! OTLP payload compression, framing, and HTTP/gRPC transport.

use std::future::Future;
use std::io;
use std::io::Write as _;
use std::pin::Pin;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use ffwd_config::OtlpProtocol;
use flate2::Compression as GzipLevel;
use flate2::write::GzEncoder;

use super::encode::write_grpc_frame;
use super::types::{DEFAULT_GRPC_MAX_MESSAGE_BYTES, OtlpSink};
use crate::http_classify::{self, DEFAULT_RETRY_AFTER_SECS};
use crate::{BatchMetadata, Compression};

impl OtlpSink {
    /// Compress, frame, and send the encoded payload via reqwest.
    ///
    /// Returns `SendResult::RetryAfter` on 429, `SendResult::Ok` on success,
    /// and surfaces transient 5xx / network errors for worker-pool retry.
    pub(super) async fn send_payload(
        &mut self,
        batch_rows: u64,
    ) -> io::Result<super::super::sink::SendResult> {
        if self.encoder_buf.is_empty() {
            return Ok(super::super::sink::SendResult::Ok);
        }

        let mut zstd_used = false;
        let mut gzip_used = false;
        let mut grpc_used = false;
        let payload: &[u8] = match self.compression {
            Compression::Zstd => {
                if let Some(ref mut compressor) = self.compressor {
                    let bound = zstd::zstd_safe::compress_bound(self.encoder_buf.len());
                    self.compress_buf.clear();
                    self.compress_buf.reserve(bound);
                    let compressed_len = compressor
                        .compress_to_buffer(&self.encoder_buf, &mut self.compress_buf)
                        .map_err(io::Error::other)?;
                    self.compress_buf.truncate(compressed_len);
                    zstd_used = true;
                    &self.compress_buf
                } else {
                    &self.encoder_buf
                }
            }
            Compression::Gzip => {
                let mut compress_buf = std::mem::take(&mut self.compress_buf);
                compress_buf.clear();
                let mut encoder = GzEncoder::new(compress_buf, GzipLevel::fast());
                encoder.write_all(&self.encoder_buf)?;
                self.compress_buf = encoder.finish()?;
                gzip_used = true;
                &self.compress_buf
            }
            Compression::None => &self.encoder_buf,
        };

        let content_type = match self.protocol {
            OtlpProtocol::Grpc => "application/grpc",
            OtlpProtocol::Http => "application/x-protobuf",
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported OTLP protocol '{other}'"),
                ));
            }
        };

        // For gRPC, prepend the 5-byte length-prefixed frame header required by the
        // gRPC wire protocol.
        //
        // The compressed flag reflects whether this specific payload is compressed
        // (i.e. Zstd was configured AND the compressor is present). If the compressor
        // was not initialized for some reason, the payload falls back to uncompressed
        // and the flag must be 0x00.
        let payload_is_compressed = match self.compression {
            Compression::Zstd => self.compressor.is_some(),
            Compression::Gzip => true,
            Compression::None => false,
        };
        let payload: &[u8] = if self.protocol == OtlpProtocol::Grpc {
            if payload.len() > DEFAULT_GRPC_MAX_MESSAGE_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "OTLP gRPC payload too large: {} bytes exceeds max {} bytes",
                        payload.len(),
                        DEFAULT_GRPC_MAX_MESSAGE_BYTES
                    ),
                ));
            }
            write_grpc_frame(&mut self.grpc_buf, payload, payload_is_compressed)?;
            grpc_used = true;
            &self.grpc_buf
        } else {
            payload
        };

        let mut req = self.client.post(&self.endpoint);
        for (k, v) in &self.headers {
            req = req.header(k.as_str(), v.as_str());
        }
        req = req.header("Content-Type", content_type);
        if payload_is_compressed {
            // gRPC compression is signaled via the wire-frame compressed flag byte
            // and the `grpc-encoding` header (per the gRPC-over-HTTP/2 spec).
            // Plain HTTP/protobuf uses `Content-Encoding` instead.
            if self.protocol == OtlpProtocol::Grpc {
                let encoding = match self.compression {
                    Compression::Zstd => "zstd",
                    Compression::Gzip => "gzip",
                    Compression::None => unreachable!("header only set when compressed"),
                };
                req = req.header("grpc-encoding", encoding);
            } else {
                let encoding = match self.compression {
                    Compression::Zstd => "zstd",
                    Compression::Gzip => "gzip",
                    Compression::None => unreachable!("header only set when compressed"),
                };
                req = req.header("Content-Encoding", encoding);
            }
        }

        let compressed_len = payload.len();
        let encoder_len = self.encoder_buf.len();

        let encoder_cap = self.encoder_buf.capacity();
        let compress_cap = self.compress_buf.capacity();
        let grpc_cap = self.grpc_buf.capacity();

        let body = if grpc_used {
            std::mem::take(&mut self.grpc_buf)
        } else if zstd_used || gzip_used {
            std::mem::take(&mut self.compress_buf)
        } else {
            std::mem::take(&mut self.encoder_buf)
        };

        // We restore capacities right away. The taken buffer is replaced with a new empty one with the same capacity!
        self.restore_capacity(encoder_cap, compress_cap, grpc_cap);

        let start = Instant::now();
        match req.body(body).send().await {
            Ok(response) => {
                self.stats.inc_send(start.elapsed().as_nanos() as u64);
                let status = response.status();

                if status.is_success() {
                    if self.protocol == OtlpProtocol::Grpc
                        && let Some(send_result) = classify_grpc_status_headers(response.headers())
                    {
                        return Ok(send_result);
                    }
                    self.stats.inc_lines(batch_rows);
                    self.stats.inc_bytes(encoder_len as u64);
                    let span = tracing::Span::current();
                    span.record("req_bytes", encoder_len as u64);
                    span.record("cmp_bytes", compressed_len as u64);
                    return Ok(super::super::sink::SendResult::Ok);
                }

                // Non-success — read body for error detail, then classify.
                let retry_after = response.headers().get("Retry-After").cloned();
                let detail = response
                    .bytes()
                    .await
                    .map(|b| String::from_utf8_lossy(&b).into_owned())
                    .unwrap_or_default();
                if let Some(send_result) = http_classify::classify_http_status(
                    status.as_u16(),
                    retry_after.as_ref(),
                    &format!("OTLP: {detail}"),
                ) {
                    return Ok(send_result);
                }
                // classify_http_status handles all non-2xx; unreachable in practice.
                Err(io::Error::other(format!(
                    "OTLP request failed with status {status}: {detail}"
                )))
            }
            Err(e) => {
                self.stats.inc_send(start.elapsed().as_nanos() as u64);
                Err(io::Error::other(e))
            }
        }
    }

    pub(crate) fn restore_capacity(
        &mut self,
        encoder_cap: usize,
        compress_cap: usize,
        grpc_cap: usize,
    ) {
        if self.encoder_buf.capacity() < encoder_cap {
            self.encoder_buf = Vec::with_capacity(encoder_cap.max(64 * 1024));
        }
        if self.compress_buf.capacity() < compress_cap {
            self.compress_buf = Vec::with_capacity(compress_cap.max(64 * 1024));
        }
        if self.grpc_buf.capacity() < grpc_cap {
            self.grpc_buf = Vec::with_capacity(grpc_cap.max(64 * 1024));
        }
    }
}

/// Classify gRPC application status from response headers.
///
/// OTLP/gRPC uses HTTP 200 for transport-level success and reports application errors
/// in `grpc-status`/`grpc-message`.
///
/// Behavior:
/// - Header-only or trailers-only responses where `grpc-status` is exposed in the initial
///   header map are classified and returned.
/// - Normal responses that send `grpc-status` only in the trailing headers are currently
///   treated as transport success because reqwest does not expose HTTP/2 trailing headers
///   via the response header map used here.
pub(super) fn classify_grpc_status_headers(
    headers: &reqwest::header::HeaderMap,
) -> Option<super::super::sink::SendResult> {
    let grpc_status = headers.get("grpc-status")?;
    let code = grpc_status
        .to_str()
        .unwrap_or("unknown")
        .trim()
        .parse::<u32>()
        .unwrap_or(2); // default to UNKNOWN
    if code == 0 {
        return None;
    }
    let msg = headers
        .get("grpc-message")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    Some(match code {
        1 | 4 | 10 | 14 => super::super::sink::SendResult::IoError(io::Error::other(format!(
            "gRPC error {code}: {msg}"
        ))),
        8 => super::super::sink::SendResult::RetryAfter(Duration::from_secs(
            DEFAULT_RETRY_AFTER_SECS,
        )),
        _ => super::super::sink::SendResult::Rejected(format!("gRPC error {code}: {msg}")),
    })
}

impl super::super::sink::Sink for OtlpSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = super::super::sink::SendResult> + Send + 'a>> {
        let encoder_cap = self.encoder_buf.capacity();
        let compress_cap = self.compress_buf.capacity();
        let grpc_cap = self.grpc_buf.capacity();

        Box::pin(async move {
            self.encode_batch(batch, metadata);
            let rows = batch.num_rows() as u64;

            let encoder_cap_post = self.encoder_buf.capacity().max(encoder_cap);
            let compress_cap_post = self.compress_buf.capacity().max(compress_cap);
            let grpc_cap_post = self.grpc_buf.capacity().max(grpc_cap);

            let result = match self.send_payload(rows).await {
                Ok(r) => r,
                Err(e) => super::super::sink::SendResult::from_io_error(e),
            };
            self.restore_capacity(encoder_cap_post, compress_cap_post, grpc_cap_post);
            result
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

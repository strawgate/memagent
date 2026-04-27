//! Elasticsearch HTTP transport: bulk send, streaming send, and ES|QL query.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::super::BatchMetadata;
use super::types::{ElasticsearchSink, SendAttempt};

impl ElasticsearchSink {
    pub async fn query_arrow(&self, query: &str) -> io::Result<Vec<RecordBatch>> {
        let query_body = serde_json::json!({ "query": query });
        let query_bytes = serde_json::to_vec(&query_body).map_err(io::Error::other)?;
        let url = format!("{}/_query", self.config.endpoint);
        let mut req = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/vnd.apache.arrow.stream");
        for (k, v) in &self.config.headers {
            req = req.header(k.clone(), v.clone());
        }
        let response = req
            .body(query_bytes)
            .send()
            .await
            .map_err(io::Error::other)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(io::Error::other(format!(
                "ES query failed (HTTP {status}): {body}"
            )));
        }
        let body = response.bytes().await.map_err(io::Error::other)?;
        let cursor = io::Cursor::new(body);
        let reader = StreamReader::try_new(cursor, None).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to parse Arrow IPC stream: {e}"),
            )
        })?;
        reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(io::Error::other)
    }

    pub(super) async fn do_send(&self, body: Vec<u8>, row_ids: Vec<u32>) -> SendAttempt {
        let body_len = body.len();

        let mut req = self
            .client
            .post(&self.config.bulk_url)
            .header("Content-Type", "application/x-ndjson");
        if self.config.compress {
            req = req.header("Content-Encoding", "gzip");
        }
        for (k, v) in &self.config.headers {
            req = req.header(k.clone(), v.clone());
        }

        tracing::Span::current().record("req_bytes", body_len as u64);
        let req = req.body(body);

        let t0 = std::time::Instant::now();
        let response = match req.send().await {
            Ok(response) => response,
            Err(error) => {
                return SendAttempt::IoError {
                    pending_rows: row_ids,
                    rejections: Vec::new(),
                    error: io::Error::other(error),
                };
            }
        };
        let send_ns = t0.elapsed().as_nanos() as u64;
        tracing::Span::current().record("send_ns", send_ns);

        let status = response.status();
        tracing::Span::current().record("status_code", status.as_u16() as u64);

        // 413 is ES-specific: triggers reactive batch splitting.
        if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE {
            let detail = response.text().await.unwrap_or_default();
            let recv_ns = (t0.elapsed().as_nanos() as u64).saturating_sub(send_ns);
            tracing::Span::current().record("recv_ns", recv_ns);
            tracing::Span::current().record("resp_bytes", detail.len() as u64);
            return SendAttempt::IoError {
                pending_rows: row_ids,
                rejections: Vec::new(),
                error: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("ES returned 413 Payload Too Large (body {body_len} bytes): {detail}"),
                ),
            };
        }

        if !status.is_success() {
            let retry_after = response.headers().get("Retry-After").cloned();
            let detail = response
                .text()
                .await
                .unwrap_or_else(|_| "<unreadable>".to_string());
            let recv_ns = (t0.elapsed().as_nanos() as u64).saturating_sub(send_ns);
            tracing::Span::current().record("recv_ns", recv_ns);
            tracing::Span::current().record("resp_bytes", detail.len() as u64);
            if let Some(send_result) = crate::http_classify::classify_http_status(
                status.as_u16(),
                retry_after.as_ref(),
                &format!("ES: {detail}"),
            ) {
                return Self::attempt_from_send_result(row_ids, send_result);
            }
            // classify_http_status handles all non-2xx; unreachable in practice.
            return SendAttempt::IoError {
                pending_rows: row_ids,
                rejections: Vec::new(),
                error: io::Error::other(format!("ES: HTTP {status}: {detail}")),
            };
        }

        let body = match response.bytes().await {
            Ok(body) => body,
            Err(error) => {
                return SendAttempt::IoError {
                    pending_rows: row_ids,
                    rejections: Vec::new(),
                    error: io::Error::other(error),
                };
            }
        };
        let recv_ns = (t0.elapsed().as_nanos() as u64).saturating_sub(send_ns);
        tracing::Span::current().record("recv_ns", recv_ns);
        tracing::Span::current().record("resp_bytes", body.len() as u64);
        if let Some(took) = Self::extract_took(&body) {
            tracing::Span::current().record("took_ms", took);
        }
        match Self::parse_bulk_response_detailed(&body) {
            Ok(result) => Self::bulk_attempt_from_result(row_ids, result),
            Err(error) => Self::attempt_from_send_result(
                row_ids,
                crate::sink::SendResult::from_io_error(error),
            ),
        }
    }

    pub(super) async fn do_send_streaming(
        &self,
        batch: RecordBatch,
        metadata: BatchMetadata,
        row_ids: Vec<u32>,
    ) -> SendAttempt {
        let row_count = batch.num_rows();
        let (tx, rx) = mpsc::channel::<io::Result<Vec<u8>>>(4);
        let emitted = Arc::new(AtomicU64::new(0));
        let producer_emitted = Arc::clone(&emitted);
        let config = Arc::clone(&self.config);
        let producer = tokio::task::spawn_blocking(move || {
            Self::serialize_batch_streaming(batch, metadata, config, tx, producer_emitted)
        });

        let mut req = self
            .client
            .post(&self.config.bulk_url)
            .header("Content-Type", "application/x-ndjson");
        for (k, v) in &self.config.headers {
            req = req.header(k.clone(), v.clone());
        }

        let body = reqwest::Body::wrap_stream(ReceiverStream::new(rx));
        let t0 = std::time::Instant::now();
        let response = match req.body(body).send().await {
            Ok(response) => response,
            Err(error) => {
                return SendAttempt::IoError {
                    pending_rows: row_ids,
                    rejections: Vec::new(),
                    error: io::Error::other(error),
                };
            }
        };
        let send_ns = t0.elapsed().as_nanos() as u64;
        tracing::Span::current().record("send_ns", send_ns);

        let producer_result = producer
            .await
            .map_err(|e| io::Error::other(format!("ES streaming producer task failed: {e}")));

        let payload_len = emitted.load(Ordering::Relaxed) as usize;
        tracing::Span::current().record("req_bytes", payload_len as u64);

        let status = response.status();
        tracing::Span::current().record("status_code", status.as_u16() as u64);

        // 413 is ES-specific: triggers reactive batch splitting.
        if status == reqwest::StatusCode::PAYLOAD_TOO_LARGE {
            let detail = response.text().await.unwrap_or_default();
            let recv_ns = (t0.elapsed().as_nanos() as u64).saturating_sub(send_ns);
            tracing::Span::current().record("recv_ns", recv_ns);
            tracing::Span::current().record("resp_bytes", detail.len() as u64);
            return SendAttempt::IoError {
                pending_rows: row_ids,
                rejections: Vec::new(),
                error: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "ES returned 413 Payload Too Large (streamed body {payload_len} bytes): {detail}"
                    ),
                ),
            };
        }

        if !status.is_success() {
            let retry_after = response.headers().get("Retry-After").cloned();
            let detail = response
                .text()
                .await
                .unwrap_or_else(|_| "<unreadable>".to_string());
            let recv_ns = (t0.elapsed().as_nanos() as u64).saturating_sub(send_ns);
            tracing::Span::current().record("recv_ns", recv_ns);
            tracing::Span::current().record("resp_bytes", detail.len() as u64);
            if let Some(send_result) = crate::http_classify::classify_http_status(
                status.as_u16(),
                retry_after.as_ref(),
                &format!("ES: {detail}"),
            ) {
                return Self::attempt_from_send_result(row_ids, send_result);
            }
            // classify_http_status handles all non-2xx; unreachable in practice.
            return SendAttempt::IoError {
                pending_rows: row_ids,
                rejections: Vec::new(),
                error: io::Error::other(format!("ES: HTTP {status}: {detail}")),
            };
        }

        let body = match response.bytes().await {
            Ok(body) => body,
            Err(error) => {
                return SendAttempt::IoError {
                    pending_rows: row_ids,
                    rejections: Vec::new(),
                    error: io::Error::other(error),
                };
            }
        };
        let recv_ns = (t0.elapsed().as_nanos() as u64).saturating_sub(send_ns);
        tracing::Span::current().record("recv_ns", recv_ns);
        tracing::Span::current().record("resp_bytes", body.len() as u64);
        if let Some(took) = Self::extract_took(&body) {
            tracing::Span::current().record("took_ms", took);
        }

        if let Some(producer_error) = Self::streaming_producer_error(producer_result) {
            if matches!(
                Self::parse_success_item_count(&body),
                Ok(Some(count)) if count == row_ids.len()
            ) {
                self.record_accepted_rows(row_count, payload_len);
                return SendAttempt::Ok;
            }
            return SendAttempt::IoError {
                pending_rows: row_ids,
                rejections: Vec::new(),
                error: producer_error,
            };
        }

        match Self::parse_bulk_response_detailed(&body) {
            Ok(result) if result.retry_items.is_empty() && result.permanent_errors.is_empty() => {
                self.record_accepted_rows(row_count, payload_len);
                SendAttempt::Ok
            }
            Ok(result) => {
                let attempt = Self::bulk_attempt_from_result(row_ids, result);
                match &attempt {
                    SendAttempt::RetryAfter { accepted_rows, .. }
                    | SendAttempt::Rejected { accepted_rows, .. } => {
                        self.record_accepted_partial_rows(*accepted_rows, row_count, payload_len);
                    }
                    SendAttempt::Ok | SendAttempt::IoError { .. } => {}
                }
                attempt
            }
            Err(error) => Self::attempt_from_send_result(
                row_ids,
                crate::sink::SendResult::from_io_error(error),
            ),
        }
    }

    pub(super) fn streaming_producer_error(
        producer_result: io::Result<io::Result<()>>,
    ) -> Option<io::Error> {
        match producer_result {
            Ok(Ok(())) => None,
            Ok(Err(producer_error)) => Some(io::Error::other(format!(
                "ES streaming producer failed: {producer_error}"
            ))),
            Err(join_error) => Some(io::Error::other(format!(
                "ES streaming producer task failed: {join_error}"
            ))),
        }
    }
}

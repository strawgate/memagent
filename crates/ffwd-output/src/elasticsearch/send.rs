//! Elasticsearch bulk send orchestration: batch splitting, retry, and result merging.

use std::future::Future;
use std::io;
use std::time::Duration;

use arrow::array::UInt32Array;
use arrow::compute;
use arrow::record_batch::RecordBatch;

use ffwd_config::ElasticsearchRequestMode;

use super::super::BatchMetadata;
use super::types::{BulkItemResult, ElasticsearchSink, SendAttempt};

impl ElasticsearchSink {
    pub(super) fn project_batch_rows(
        batch: &RecordBatch,
        row_ids: &[u32],
    ) -> io::Result<RecordBatch> {
        if row_ids.len() == batch.num_rows()
            && row_ids
                .iter()
                .enumerate()
                .all(|(idx, row_id)| *row_id as usize == idx)
        {
            return Ok(batch.clone());
        }

        if let Some(row_id) = row_ids
            .iter()
            .copied()
            .find(|row_id| *row_id as usize >= batch.num_rows())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "projected Elasticsearch retry row id {row_id} out of bounds for {} rows",
                    batch.num_rows()
                ),
            ));
        }

        let indices = UInt32Array::from(row_ids.to_vec());
        let columns = batch
            .columns()
            .iter()
            .map(|column| compute::take(column.as_ref(), &indices, None).map_err(io::Error::other))
            .collect::<io::Result<Vec<_>>>()?;
        RecordBatch::try_new(batch.schema(), columns).map_err(io::Error::other)
    }

    pub(super) fn record_accepted_rows(&self, row_count: usize, payload_len: usize) {
        if row_count == 0 {
            return;
        }
        self.stats.inc_lines(row_count as u64);
        self.stats.inc_bytes(payload_len as u64);
    }

    pub(super) fn record_accepted_partial_rows(
        &self,
        accepted_rows: usize,
        total_rows: usize,
        payload_len: usize,
    ) {
        if accepted_rows == 0 || total_rows == 0 {
            return;
        }
        let accepted_bytes = payload_len.saturating_mul(accepted_rows) / total_rows;
        self.record_accepted_rows(accepted_rows, accepted_bytes);
    }

    pub(super) fn finish_success_or_reject(rejections: Vec<String>) -> crate::sink::SendResult {
        if rejections.is_empty() {
            crate::sink::SendResult::Ok
        } else {
            crate::sink::SendResult::Rejected(rejections.join("; "))
        }
    }

    pub(super) fn bulk_attempt_from_result(
        row_ids: Vec<u32>,
        result: BulkItemResult,
    ) -> SendAttempt {
        if result.retry_items.is_empty() && result.permanent_errors.is_empty() {
            return SendAttempt::Ok;
        }

        if result.item_count != row_ids.len() {
            return SendAttempt::Rejected {
                rejections: vec![format!(
                    "ES bulk response item count mismatch: {} response items for {} attempted rows",
                    result.item_count,
                    row_ids.len()
                )],
                accepted_rows: 0,
            };
        }

        let failed_rows = result
            .retry_items
            .len()
            .saturating_add(result.permanent_errors.len());
        let accepted_rows = result.item_count.saturating_sub(failed_rows);

        if result.retry_items.is_empty() {
            return SendAttempt::Rejected {
                rejections: result.permanent_errors,
                accepted_rows,
            };
        }

        let pending_rows = result
            .retry_items
            .into_iter()
            .map(|idx| row_ids[idx])
            .collect();
        SendAttempt::RetryAfter {
            pending_rows,
            rejections: result.permanent_errors,
            accepted_rows,
            delay: Self::ITEM_RETRY_DELAY,
        }
    }

    /// Send a batch, proactively splitting into sub-batches that fit within
    /// `max_bulk_bytes`. Also splits reactively on 413 Payload Too Large.
    pub(super) fn send_batch_inner<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        metadata: &'a BatchMetadata,
        row_ids: Vec<u32>,
        depth: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = SendAttempt> + Send + 'a>> {
        Box::pin(async move {
            const MAX_SPLIT_DEPTH: usize = 6; // up to 64 sub-batches

            let n = row_ids.len();
            if n == 0 {
                return SendAttempt::Ok;
            }

            if self.config.request_mode == ElasticsearchRequestMode::Streaming {
                let attempt = self
                    .do_send_streaming(batch.clone(), metadata.clone(), row_ids.clone())
                    .await;
                if let SendAttempt::IoError { error, .. } = &attempt
                    && error.kind() == io::ErrorKind::InvalidInput
                    && n > 1
                    && depth < MAX_SPLIT_DEPTH
                {
                    return self
                        .send_split_halves(batch, metadata, row_ids, depth)
                        .await;
                }
                return attempt;
            }

            if let Err(error) = self.serialize_batch(batch, metadata) {
                return Self::attempt_from_send_result(
                    row_ids,
                    crate::sink::SendResult::from_io_error(error),
                );
            }
            if self.batch_buf.is_empty() {
                return SendAttempt::Ok;
            }

            let payload_len = self.batch_buf.len();
            // Remember the serialized size so we can restore warm capacity after
            // the buffer is moved into `do_send`.
            let prev_cap = self.batch_buf.capacity();
            let max_bytes = self.config.max_bulk_bytes;

            // Proactive split: if serialized payload exceeds max_bulk_bytes, split
            // the batch in half and send each half separately.
            if payload_len > max_bytes {
                if n > 1 && depth < MAX_SPLIT_DEPTH {
                    self.batch_buf.clear(); // discard oversized payload
                    return self
                        .send_split_halves(batch, metadata, row_ids, depth)
                        .await;
                }
                // Row is too large to fit in a single bulk request even on its own.
                return SendAttempt::Rejected {
                    rejections: vec![format!(
                        "single-row batch ({} bytes) exceeds max_bulk_bytes ({})",
                        payload_len, max_bytes
                    )],
                    accepted_rows: 0,
                };
            }

            // Apply compression if configured, avoiding per-batch fresh encoder allocations.
            let body = if self.config.compress {
                use flate2::Compression;
                use flate2::write::GzEncoder;
                use std::io::Write;

                self.compress_buf.clear();
                let mut enc =
                    GzEncoder::new(std::mem::take(&mut self.compress_buf), Compression::fast());
                if let Err(error) = enc.write_all(&self.batch_buf).map_err(io::Error::other) {
                    return SendAttempt::IoError {
                        pending_rows: row_ids,
                        rejections: Vec::new(),
                        error,
                    };
                }
                match enc.finish().map_err(io::Error::other) {
                    Ok(compressed) => compressed,
                    Err(error) => {
                        return SendAttempt::IoError {
                            pending_rows: row_ids,
                            rejections: Vec::new(),
                            error,
                        };
                    }
                }
            } else {
                std::mem::take(&mut self.batch_buf)
            };

            let compress_cap = body.capacity();

            let attempt = self.do_send(body, row_ids.clone()).await;

            if self.config.compress {
                self.compress_buf.reserve(compress_cap);
            } else {
                self.batch_buf.reserve(prev_cap);
            }

            match attempt {
                SendAttempt::Ok => {
                    self.record_accepted_rows(n, payload_len);
                    SendAttempt::Ok
                }
                attempt @ SendAttempt::RetryAfter { accepted_rows, .. }
                | attempt @ SendAttempt::Rejected { accepted_rows, .. } => {
                    self.record_accepted_partial_rows(accepted_rows, n, payload_len);
                    attempt
                }
                // Reactive split on 413 — server limit lower than our max_bulk_bytes.
                SendAttempt::IoError { error, .. }
                    if error.kind() == io::ErrorKind::InvalidInput
                        && n > 1
                        && depth < MAX_SPLIT_DEPTH =>
                {
                    self.send_split_halves(batch, metadata, row_ids, depth)
                        .await
                }
                attempt @ SendAttempt::IoError { .. } => attempt,
            }
        })
    }

    pub(super) const ITEM_RETRY_DELAY: Duration = Duration::from_millis(250);

    /// Split a batch in half and send each half sequentially.
    ///
    /// **Duplication safety (#1873):** Both halves are always attempted
    /// regardless of individual outcomes. When one half succeeds but the
    /// other fails with a transient error, the failed half is retried
    /// internally (up to `SPLIT_INTERNAL_RETRIES` times) rather than
    /// propagating a retryable result to the worker pool — which would
    /// retry the entire original batch and duplicate the successful half.
    pub(super) async fn send_split_halves(
        &mut self,
        batch: &RecordBatch,
        metadata: &BatchMetadata,
        row_ids: Vec<u32>,
        depth: usize,
    ) -> SendAttempt {
        let n = row_ids.len();
        let mid = n / 2;
        let left = batch.slice(0, mid);
        let right = batch.slice(mid, n - mid);
        let left_rows = row_ids[..mid].to_vec();
        let right_rows = row_ids[mid..].to_vec();

        let left_result = self
            .send_batch_inner(&left, metadata, left_rows, depth + 1)
            .await;

        match left_result {
            SendAttempt::Ok => {
                let right_result = self
                    .send_batch_inner(&right, metadata, right_rows, depth + 1)
                    .await;
                Self::merge_split_attempts(
                    SendAttempt::Ok,
                    Self::label_attempt_rejections("right split rejected", right_result),
                )
            }
            SendAttempt::Rejected {
                rejections,
                accepted_rows,
            } => {
                let right_result = self
                    .send_batch_inner(&right, metadata, right_rows, depth + 1)
                    .await;
                Self::merge_split_attempts(
                    SendAttempt::Rejected {
                        rejections: Self::prefix_rejections("left split rejected", rejections),
                        accepted_rows,
                    },
                    Self::label_attempt_rejections("right split rejected", right_result),
                )
            }
            SendAttempt::RetryAfter {
                pending_rows,
                rejections,
                accepted_rows,
                delay,
            } => {
                let right_result = self
                    .send_batch_inner(&right, metadata, right_rows, depth + 1)
                    .await;
                Self::merge_split_attempts(
                    SendAttempt::RetryAfter {
                        pending_rows,
                        rejections: Self::prefix_rejections("left split rejected", rejections),
                        accepted_rows,
                        delay,
                    },
                    Self::label_attempt_rejections("right split rejected", right_result),
                )
            }
            SendAttempt::IoError {
                pending_rows,
                rejections,
                error,
            } => {
                let right_result = self
                    .send_batch_inner(&right, metadata, right_rows, depth + 1)
                    .await;
                Self::merge_split_attempts(
                    SendAttempt::IoError {
                        pending_rows,
                        rejections: Self::prefix_rejections("left split rejected", rejections),
                        error,
                    },
                    Self::label_attempt_rejections("right split rejected", right_result),
                )
            }
        }
    }

    pub(super) fn prefix_rejections(prefix: &str, rejections: Vec<String>) -> Vec<String> {
        rejections
            .into_iter()
            .map(|rejection| format!("{prefix}: {rejection}"))
            .collect()
    }

    pub(super) fn label_attempt_rejections(prefix: &str, attempt: SendAttempt) -> SendAttempt {
        match attempt {
            SendAttempt::Rejected {
                rejections,
                accepted_rows,
            } => SendAttempt::Rejected {
                rejections: Self::prefix_rejections(prefix, rejections),
                accepted_rows,
            },
            SendAttempt::RetryAfter {
                pending_rows,
                rejections,
                accepted_rows,
                delay,
            } => SendAttempt::RetryAfter {
                pending_rows,
                rejections: Self::prefix_rejections(prefix, rejections),
                accepted_rows,
                delay,
            },
            SendAttempt::IoError {
                pending_rows,
                rejections,
                error,
            } => SendAttempt::IoError {
                pending_rows,
                rejections: Self::prefix_rejections(prefix, rejections),
                error,
            },
            ok @ SendAttempt::Ok => ok,
        }
    }

    pub(super) fn merge_split_attempts(left: SendAttempt, right: SendAttempt) -> SendAttempt {
        match (left, right) {
            (SendAttempt::Ok, SendAttempt::Ok) => SendAttempt::Ok,
            (
                SendAttempt::Rejected {
                    rejections: mut left,
                    accepted_rows,
                },
                SendAttempt::Rejected {
                    rejections: right,
                    accepted_rows: right_accepted,
                },
            ) => {
                left.extend(right);
                SendAttempt::Rejected {
                    rejections: left,
                    accepted_rows: accepted_rows.saturating_add(right_accepted),
                }
            }
            (attempt @ SendAttempt::Rejected { .. }, SendAttempt::Ok)
            | (SendAttempt::Ok, attempt @ SendAttempt::Rejected { .. }) => attempt,
            (
                SendAttempt::RetryAfter {
                    pending_rows,
                    mut rejections,
                    accepted_rows,
                    delay,
                },
                SendAttempt::Rejected {
                    rejections: right_rejections,
                    accepted_rows: right_accepted,
                },
            )
            | (
                SendAttempt::Rejected {
                    rejections: right_rejections,
                    accepted_rows: right_accepted,
                },
                SendAttempt::RetryAfter {
                    pending_rows,
                    mut rejections,
                    accepted_rows,
                    delay,
                },
            ) => {
                rejections.extend(right_rejections);
                SendAttempt::RetryAfter {
                    pending_rows,
                    rejections,
                    accepted_rows: accepted_rows.saturating_add(right_accepted),
                    delay,
                }
            }
            (
                SendAttempt::RetryAfter {
                    mut pending_rows,
                    mut rejections,
                    accepted_rows,
                    delay,
                },
                SendAttempt::RetryAfter {
                    pending_rows: right_rows,
                    rejections: right_rejections,
                    accepted_rows: right_accepted,
                    delay: right_delay,
                },
            ) => {
                pending_rows.extend(right_rows);
                rejections.extend(right_rejections);
                SendAttempt::RetryAfter {
                    pending_rows,
                    rejections,
                    accepted_rows: accepted_rows.saturating_add(right_accepted),
                    delay: delay.max(right_delay),
                }
            }
            (
                SendAttempt::IoError {
                    mut pending_rows,
                    mut rejections,
                    error,
                },
                SendAttempt::IoError {
                    pending_rows: right_rows,
                    rejections: right_rejections,
                    error: right_error,
                },
            ) => {
                pending_rows.extend(right_rows);
                rejections.extend(right_rejections);
                SendAttempt::IoError {
                    pending_rows,
                    rejections,
                    error: Self::combine_io_errors(error, "right split IO error", right_error),
                }
            }
            (
                SendAttempt::IoError {
                    mut pending_rows,
                    mut rejections,
                    error,
                },
                SendAttempt::RetryAfter {
                    pending_rows: right_rows,
                    rejections: right_rejections,
                    ..
                },
            )
            | (
                SendAttempt::RetryAfter {
                    pending_rows: right_rows,
                    rejections: right_rejections,
                    ..
                },
                SendAttempt::IoError {
                    mut pending_rows,
                    mut rejections,
                    error,
                },
            ) => {
                pending_rows.extend(right_rows);
                rejections.extend(right_rejections);
                SendAttempt::IoError {
                    pending_rows,
                    rejections,
                    error,
                }
            }
            (
                SendAttempt::IoError {
                    pending_rows,
                    mut rejections,
                    error,
                },
                SendAttempt::Rejected {
                    rejections: right_rejections,
                    ..
                },
            )
            | (
                SendAttempt::Rejected {
                    rejections: right_rejections,
                    ..
                },
                SendAttempt::IoError {
                    pending_rows,
                    mut rejections,
                    error,
                },
            ) => {
                rejections.extend(right_rejections);
                SendAttempt::IoError {
                    pending_rows,
                    rejections,
                    error,
                }
            }
            (attempt @ SendAttempt::IoError { .. }, SendAttempt::Ok)
            | (SendAttempt::Ok, attempt @ SendAttempt::IoError { .. }) => attempt,
            (
                SendAttempt::RetryAfter {
                    pending_rows,
                    rejections,
                    accepted_rows,
                    delay,
                },
                SendAttempt::Ok,
            )
            | (
                SendAttempt::Ok,
                SendAttempt::RetryAfter {
                    pending_rows,
                    rejections,
                    accepted_rows,
                    delay,
                },
            ) => SendAttempt::RetryAfter {
                pending_rows,
                rejections,
                accepted_rows,
                delay,
            },
        }
    }

    pub(super) fn combine_io_errors(
        left: io::Error,
        right_prefix: &str,
        right: io::Error,
    ) -> io::Error {
        io::Error::new(left.kind(), format!("{left}; {right_prefix}: {right}"))
    }

    /// Convert a [`SendResult`](crate::sink::SendResult) into a [`SendAttempt`],
    /// carrying `row_ids` forward as `pending_rows` for retryable/IO variants.
    pub(super) fn attempt_from_send_result(
        row_ids: Vec<u32>,
        result: crate::sink::SendResult,
    ) -> SendAttempt {
        match result {
            crate::sink::SendResult::Ok => SendAttempt::Ok,
            crate::sink::SendResult::Rejected(reason) => SendAttempt::Rejected {
                rejections: vec![reason],
                accepted_rows: 0,
            },
            crate::sink::SendResult::RetryAfter(delay) => SendAttempt::RetryAfter {
                pending_rows: row_ids,
                rejections: Vec::new(),
                accepted_rows: 0,
                delay,
            },
            crate::sink::SendResult::IoError(error) => SendAttempt::IoError {
                pending_rows: row_ids,
                rejections: Vec::new(),
                error,
            },
        }
    }

    /// Convert permanent ES rejections from `Err` to `Ok(Rejected)` so they
    /// flow through `merge_split_send_results` instead of short-circuiting via
    /// `?`. `parse_bulk_response` returns `InvalidData` for document-level
    /// errors (e.g. mapper_parsing_exception) inside an HTTP 200 response.
    /// These are terminal — retrying is futile — but as `Err` they would skip
    /// the right half of a split send.
    #[cfg(test)]
    pub(super) fn classify_split_result(
        result: io::Result<crate::sink::SendResult>,
    ) -> io::Result<crate::sink::SendResult> {
        match result {
            Ok(result) => Ok(result),
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData
                ) =>
            {
                Ok(crate::sink::SendResult::Rejected(e.to_string()))
            }
            Err(e) => Err(e),
        }
    }

    /// Merge split-half outcomes into the single-result `Sink` contract.
    ///
    /// Precedence is `IoError` > `RetryAfter` > `Rejected` > `Ok`. When both
    /// halves retry, use the longer delay. The shape intentionally mirrors
    /// fanout result reduction so mixed outcomes do not hide retryable work.
    #[cfg(test)]
    pub(super) fn merge_split_send_results(
        left: crate::sink::SendResult,
        right: crate::sink::SendResult,
    ) -> crate::sink::SendResult {
        match (left, right) {
            (crate::sink::SendResult::Ok, crate::sink::SendResult::Ok) => {
                crate::sink::SendResult::Ok
            }
            (crate::sink::SendResult::Rejected(left), crate::sink::SendResult::Rejected(right)) => {
                crate::sink::SendResult::Rejected(format!(
                    "left split rejected: {left}; right split rejected: {right}"
                ))
            }
            (crate::sink::SendResult::Rejected(reason), crate::sink::SendResult::Ok) => {
                crate::sink::SendResult::Rejected(format!("left split rejected: {reason}"))
            }
            (crate::sink::SendResult::Ok, crate::sink::SendResult::Rejected(reason)) => {
                crate::sink::SendResult::Rejected(format!("right split rejected: {reason}"))
            }
            (
                crate::sink::SendResult::RetryAfter(left),
                crate::sink::SendResult::RetryAfter(right),
            ) => crate::sink::SendResult::RetryAfter(left.max(right)),
            (crate::sink::SendResult::IoError(error), _)
            | (_, crate::sink::SendResult::IoError(error)) => {
                crate::sink::SendResult::IoError(error)
            }
            (crate::sink::SendResult::RetryAfter(delay), _)
            | (_, crate::sink::SendResult::RetryAfter(delay)) => {
                crate::sink::SendResult::RetryAfter(delay)
            }
        }
    }
}

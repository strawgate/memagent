//! [`Sink`](crate::sink::Sink) trait implementation for [`ElasticsearchSink`].

use std::future::Future;
use std::io;

use arrow::record_batch::RecordBatch;

use super::super::BatchMetadata;
use super::types::{ElasticsearchSink, SendAttempt};

impl crate::sink::Sink for ElasticsearchSink {
    fn begin_batch(&mut self) {
        self.pending_retry_rows = None;
        self.pending_rejections.clear();
    }

    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        metadata: &'a BatchMetadata,
    ) -> std::pin::Pin<Box<dyn Future<Output = crate::sink::SendResult> + Send + 'a>> {
        Box::pin(async move {
            let row_ids = self
                .pending_retry_rows
                .clone()
                .unwrap_or_else(|| (0..batch.num_rows() as u32).collect());
            let mut rejections = self.pending_rejections.clone();
            if row_ids.is_empty() {
                self.pending_retry_rows = None;
                self.pending_rejections.clear();
                return Self::finish_success_or_reject(rejections);
            }

            let projected_batch = match Self::project_batch_rows(batch, &row_ids) {
                Ok(projected) => projected,
                Err(error) => {
                    return match crate::sink::SendResult::from_io_error(error) {
                        crate::sink::SendResult::Rejected(reason) => {
                            rejections.push(reason);
                            self.pending_retry_rows = None;
                            self.pending_rejections.clear();
                            Self::finish_success_or_reject(rejections)
                        }
                        other => {
                            self.pending_retry_rows = Some(row_ids);
                            self.pending_rejections = rejections;
                            other
                        }
                    };
                }
            };

            match self
                .send_batch_inner(&projected_batch, metadata, row_ids.clone(), 0)
                .await
            {
                SendAttempt::Ok => {
                    self.pending_retry_rows = None;
                    self.pending_rejections.clear();
                    Self::finish_success_or_reject(rejections)
                }
                SendAttempt::Rejected {
                    rejections: new_rejections,
                    ..
                } => {
                    rejections.extend(new_rejections);
                    self.pending_retry_rows = None;
                    self.pending_rejections.clear();
                    Self::finish_success_or_reject(rejections)
                }
                SendAttempt::RetryAfter {
                    pending_rows,
                    rejections: new_rejections,
                    delay,
                    ..
                } => {
                    rejections.extend(new_rejections);
                    self.pending_retry_rows = Some(pending_rows);
                    self.pending_rejections = rejections;
                    crate::sink::SendResult::RetryAfter(delay)
                }
                SendAttempt::IoError {
                    pending_rows,
                    rejections: new_rejections,
                    error,
                } => {
                    rejections.extend(new_rejections);
                    self.pending_retry_rows = Some(pending_rows);
                    self.pending_rejections = rejections;
                    crate::sink::SendResult::IoError(error)
                }
            }
        })
    }

    fn flush(&mut self) -> std::pin::Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn shutdown(&mut self) -> std::pin::Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

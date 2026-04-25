use std::io;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
#[cfg(any(feature = "otlp-research", test))]
use bytes::Bytes;

use crate::InputError;
use crate::blocking_stage::{BlockingWorker, BoundedBlockingStage};

use super::OtlpProtobufDecodeMode;
use super::decode::{
    decode_otlp_json, decode_otlp_protobuf_with_prost, decompress_gzip, decompress_zstd,
};
#[cfg(any(feature = "otlp-research", test))]
use super::projection::{ProjectedOtlpDecoder, ProjectionError};

/// Fixed worker pool that owns all payload-scaling OTLP request CPU work.
pub(super) type OtlpRequestCpuStage = BoundedBlockingStage<OtlpRequestCpuWorker>;

/// Request body and decode metadata handed from the HTTP runtime to CPU workers.
pub(super) struct OtlpRequestCpuJob {
    /// Raw bounded HTTP body bytes, still compressed when `encoding` is not identity.
    pub(super) body: Vec<u8>,
    /// Decoder family selected from the normalized content type.
    pub(super) content: OtlpRequestContent,
    /// Content encoding selected from the normalized content-encoding header.
    pub(super) encoding: OtlpContentEncoding,
    /// Maximum allowed decompressed payload size for compressed requests.
    pub(super) max_body_size: usize,
}

/// OTLP request serialization family selected before CPU-stage submission.
pub(super) enum OtlpRequestContent {
    /// OTLP JSON payload accepted as `application/json`.
    Json,
    /// OTLP protobuf payload used for absent or non-JSON content types.
    Protobuf,
}

/// Compression applied to the HTTP request body before OTLP decode.
#[derive(Clone, Copy)]
pub(super) enum OtlpContentEncoding {
    /// Body is already an OTLP JSON/protobuf payload.
    Identity,
    /// Body must be gzip-decompressed inside the CPU worker.
    Gzip,
    /// Body must be zstd-decompressed inside the CPU worker.
    Zstd,
}

/// Successful CPU-stage output returned to the HTTP handler.
pub(super) struct OtlpRequestCpuOutput {
    /// Materialized Arrow batch ready for receiver queue submission.
    pub(super) batch: RecordBatch,
    /// Decode path used, for projection metrics and fallback accounting.
    pub(super) outcome: OtlpRequestCpuOutcome,
}

/// Decode path used to produce a successful OTLP request batch.
pub(super) enum OtlpRequestCpuOutcome {
    /// OTLP JSON path decoded and materialized the request.
    Json,
    /// Prost reference protobuf path decoded and materialized the request.
    Prost,
    /// Experimental projection path handled the protobuf request directly.
    #[cfg(any(feature = "otlp-research", test))]
    ProjectedSuccess,
    /// Experimental projection found unsupported valid OTLP and fell back to prost.
    #[cfg(any(feature = "otlp-research", test))]
    ProjectedFallback,
}

/// Recoverable CPU-stage failure classified for HTTP response mapping.
#[derive(Debug)]
pub(super) enum OtlpRequestCpuError {
    /// Compression transport failed before decode; invalid data maps to transport errors.
    Decompression(InputError),
    /// JSON/protobuf payload decode or materialization failed.
    Payload(InputError),
    /// Projection rejected malformed protobuf that should not fall back to prost.
    #[cfg(any(feature = "otlp-research", test))]
    ProjectionInvalid(InputError),
}

/// Per-thread OTLP request processor with reusable decoder state.
pub(super) struct OtlpRequestCpuWorker {
    resource_prefix: Arc<str>,
    mode: OtlpProtobufDecodeMode,
    #[cfg(any(feature = "otlp-research", test))]
    projected_decoder: Option<ProjectedOtlpDecoder>,
}

impl OtlpRequestCpuWorker {
    fn new(resource_prefix: Arc<str>, mode: OtlpProtobufDecodeMode) -> Self {
        #[cfg(any(feature = "otlp-research", test))]
        let projected_decoder = if mode == OtlpProtobufDecodeMode::Prost {
            None
        } else {
            Some(ProjectedOtlpDecoder::new(resource_prefix.as_ref()))
        };
        Self {
            resource_prefix,
            mode,
            #[cfg(any(feature = "otlp-research", test))]
            projected_decoder,
        }
    }

    fn decode_json(&self, body: &[u8]) -> Result<OtlpRequestCpuOutput, OtlpRequestCpuError> {
        decode_otlp_json(body, self.resource_prefix.as_ref())
            .map(|batch| OtlpRequestCpuOutput {
                batch,
                outcome: OtlpRequestCpuOutcome::Json,
            })
            .map_err(OtlpRequestCpuError::Payload)
    }

    fn decode_prost(&self, body: &[u8]) -> Result<OtlpRequestCpuOutput, OtlpRequestCpuError> {
        decode_otlp_protobuf_with_prost(body, self.resource_prefix.as_ref())
            .map(|batch| OtlpRequestCpuOutput {
                batch,
                outcome: OtlpRequestCpuOutcome::Prost,
            })
            .map_err(OtlpRequestCpuError::Payload)
    }

    #[cfg(any(feature = "otlp-research", test))]
    fn decode_projected_fallback(
        &mut self,
        body: Vec<u8>,
    ) -> Result<OtlpRequestCpuOutput, OtlpRequestCpuError> {
        let body = Bytes::from(body);
        match super::projection::classify_projected_fallback_support(&body) {
            Ok(()) => {}
            Err(ProjectionError::Unsupported(_)) => {
                return decode_otlp_protobuf_with_prost(&body, self.resource_prefix.as_ref())
                    .map(|batch| OtlpRequestCpuOutput {
                        batch,
                        outcome: OtlpRequestCpuOutcome::ProjectedFallback,
                    })
                    .map_err(OtlpRequestCpuError::Payload);
            }
            Err(err) => {
                return Err(OtlpRequestCpuError::ProjectionInvalid(
                    err.into_input_error(),
                ));
            }
        }
        match self.decode_projected_view(body.clone()) {
            Ok(batch) => Ok(OtlpRequestCpuOutput {
                batch,
                outcome: OtlpRequestCpuOutcome::ProjectedSuccess,
            }),
            Err(ProjectionError::Unsupported(_)) => {
                decode_otlp_protobuf_with_prost(&body, self.resource_prefix.as_ref())
                    .map(|batch| OtlpRequestCpuOutput {
                        batch,
                        outcome: OtlpRequestCpuOutcome::ProjectedFallback,
                    })
                    .map_err(OtlpRequestCpuError::Payload)
            }
            Err(err) => Err(OtlpRequestCpuError::ProjectionInvalid(
                err.into_input_error(),
            )),
        }
    }

    #[cfg(any(feature = "otlp-research", test))]
    fn decode_projected_only(
        &mut self,
        body: Vec<u8>,
    ) -> Result<OtlpRequestCpuOutput, OtlpRequestCpuError> {
        match self.decode_projected_view(Bytes::from(body)) {
            Ok(batch) => Ok(OtlpRequestCpuOutput {
                batch,
                outcome: OtlpRequestCpuOutcome::ProjectedSuccess,
            }),
            Err(ProjectionError::Unsupported(err)) => Err(OtlpRequestCpuError::Payload(
                ProjectionError::Unsupported(err).into_input_error(),
            )),
            Err(err) => Err(OtlpRequestCpuError::ProjectionInvalid(
                err.into_input_error(),
            )),
        }
    }

    #[cfg(any(feature = "otlp-research", test))]
    fn decode_projected_view(&mut self, body: Bytes) -> Result<RecordBatch, ProjectionError> {
        let Some(decoder) = self.projected_decoder.as_mut() else {
            return Err(ProjectionError::Invalid(
                "projected decoder not initialized",
            ));
        };
        decoder.try_decode_view_bytes(body)
    }
}

impl BlockingWorker for OtlpRequestCpuWorker {
    type Job = OtlpRequestCpuJob;
    type Output = OtlpRequestCpuOutput;
    type Error = OtlpRequestCpuError;

    fn process(&mut self, job: Self::Job) -> Result<Self::Output, Self::Error> {
        #[cfg(test)]
        {
            if job.body == b"__panic_worker__" {
                panic!("otlp decode worker panic requested by test");
            }
            if job.body == b"__sleep_worker__" {
                std::thread::sleep(std::time::Duration::from_millis(200));
                return self.decode_prost(&[]);
            }
        }

        let body = decompress_request_body(job.body, job.encoding, job.max_body_size)
            .map_err(OtlpRequestCpuError::Decompression)?;

        match job.content {
            OtlpRequestContent::Json => self.decode_json(&body),
            OtlpRequestContent::Protobuf => match self.mode {
                OtlpProtobufDecodeMode::Prost => self.decode_prost(&body),
                #[cfg(any(feature = "otlp-research", test))]
                OtlpProtobufDecodeMode::ProjectedFallback => self.decode_projected_fallback(body),
                #[cfg(any(feature = "otlp-research", test))]
                OtlpProtobufDecodeMode::ProjectedOnly => self.decode_projected_only(body),
            },
        }
    }
}

fn decompress_request_body(
    body: Vec<u8>,
    encoding: OtlpContentEncoding,
    max_body_size: usize,
) -> Result<Vec<u8>, InputError> {
    match encoding {
        OtlpContentEncoding::Identity => Ok(body),
        OtlpContentEncoding::Gzip => decompress_gzip(&body, max_body_size),
        OtlpContentEncoding::Zstd => decompress_zstd(&body, max_body_size),
    }
}

/// Build the bounded worker-owned CPU stage used by the OTLP HTTP handler.
///
/// Each worker owns its projected decoder state, so protobuf projection reuse
/// does not require locking on the async HTTP runtime. `max_outstanding` bounds
/// active plus queued requests and causes submit-side backpressure when full.
pub(super) fn build_otlp_request_cpu_stage(
    worker_count: usize,
    max_outstanding: usize,
    resource_prefix: Arc<str>,
    mode: OtlpProtobufDecodeMode,
) -> io::Result<OtlpRequestCpuStage> {
    BoundedBlockingStage::new(worker_count, max_outstanding, move |_| {
        OtlpRequestCpuWorker::new(Arc::clone(&resource_prefix), mode)
    })
}

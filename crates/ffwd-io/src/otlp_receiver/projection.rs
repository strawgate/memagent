#![allow(clippy::indexing_slicing)]

//! Experimental OTLP logs wire projection into Arrow.
//!
//! This module decodes the common OTLP logs protobuf shape directly into
//! a [`ColumnarBatchBuilder`], bypassing allocation of the full prost object
//! graph. It intentionally implements a projection, not a complete protobuf
//! object model: unsupported semantic cases return [`ProjectionError::Unsupported`]
//! so the receiver can fall back to the prost decoder.

use std::fmt;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use ffwd_arrow::columnar::builder::ColumnarBatchBuilder;

use crate::InputError;

mod decode;
mod generated;
#[cfg(test)]
mod tests;
mod wire;
mod write;

#[cfg(test)]
use generated::field_numbers as otlp_field;

// Re-export submodule items so the generated module can access them via `super::`.
use wire::{
    StringStorage, WireAny, WireField, WireScratch, for_each_field, require_utf8, subslice_range,
};
use write::{
    write_hex_field, write_hex_to_buf, write_json_escaped_bytes, write_wire_any_complex_json,
    write_wire_str,
};

#[derive(Debug)]
pub(super) enum ProjectionError {
    Invalid(&'static str),
    Unsupported(&'static str),
    Batch(String),
}

impl ProjectionError {
    /// Convert to `InputError`, preserving the `Unsupported` variant so
    /// callers can fall back to the prost reference decoder.
    pub(super) fn into_input_error(self) -> InputError {
        match self {
            ProjectionError::Unsupported(msg) => InputError::Unsupported(msg.to_string()),
            other => InputError::Receiver(other.to_string()),
        }
    }
}

impl fmt::Display for ProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProjectionError::Invalid(msg) => write!(f, "invalid OTLP protobuf: {msg}"),
            ProjectionError::Unsupported(msg) => {
                write!(f, "unsupported OTLP projection case: {msg}")
            }
            ProjectionError::Batch(msg) => write!(f, "OTLP projection batch build error: {msg}"),
        }
    }
}

/// Decode an OTLP ExportLogsServiceRequest payload directly into an Arrow batch.
///
/// `body` contains the protobuf payload bytes and `resource_prefix` selects the
/// column prefix used for projected resource attributes. This detached variant
/// decodes strings into builder-owned storage before returning a `RecordBatch`.
pub(super) fn decode_projected_otlp_logs(
    body: &[u8],
    resource_prefix: &str,
) -> Result<RecordBatch, ProjectionError> {
    decode::decode_projected_otlp_logs_inner(
        body,
        Bytes::new(),
        resource_prefix,
        StringStorage::Decoded,
    )
}

#[cfg(any(feature = "otlp-research", test))]
/// Decode an OTLP ExportLogsServiceRequest payload into an Arrow batch that
/// views the request buffer where possible.
///
/// `body` is cloned as a cheap `Bytes` handle for batch backing storage, while
/// `resource_prefix` selects the projected resource-attribute column prefix.
/// This variant is available for `otlp-research` and tests.
pub(super) fn decode_projected_otlp_logs_view_bytes(
    body: Bytes,
    resource_prefix: &str,
) -> Result<RecordBatch, ProjectionError> {
    let backing = body.clone();
    decode::decode_projected_otlp_logs_inner(
        body.as_ref(),
        backing,
        resource_prefix,
        StringStorage::InputView,
    )
}

/// Classify whether the payload is eligible for direct projection.
///
/// This generated preflight is used only by fallback mode. It allows
/// unsupported-but-valid OTLP shapes to skip builder mutation and go straight
/// to the prost fallback path while preserving malformed-wire rejection.
pub(super) fn classify_projected_fallback_support(body: &[u8]) -> Result<(), ProjectionError> {
    generated::classify_projection_support(body)
}

/// Reusable OTLP projected decoder.
///
/// Holds the `ColumnarBatchBuilder`, field handles, and scratch buffers so
/// successive batches reuse allocated capacity instead of re-allocating from
/// scratch.  This is the production-intended usage pattern: create once,
/// call [`decode_view_bytes`](Self::decode_view_bytes) per request.
#[cfg(any(feature = "otlp-research", test))]
pub struct ProjectedOtlpDecoder {
    builder: ColumnarBatchBuilder,
    handles: generated::OtlpFieldHandles,
    scratch: WireScratch,
    resource_prefix: String,
}

#[cfg(any(feature = "otlp-research", test))]
impl ProjectedOtlpDecoder {
    /// Create a reusable decoder with the given resource attribute prefix.
    pub fn new(resource_prefix: &str) -> Self {
        let (plan, handles) = generated::build_otlp_plan();
        Self {
            builder: ColumnarBatchBuilder::new(plan),
            handles,
            scratch: WireScratch::default(),
            resource_prefix: resource_prefix.to_owned(),
        }
    }

    /// Decode an OTLP payload using the view-bytes path, reusing builder capacity.
    pub fn decode_view_bytes(&mut self, body: Bytes) -> Result<RecordBatch, InputError> {
        self.try_decode_view_bytes(body)
            .map_err(ProjectionError::into_input_error)
    }

    pub(super) fn try_decode_view_bytes(
        &mut self,
        body: Bytes,
    ) -> Result<RecordBatch, ProjectionError> {
        let backing = body.clone();
        self.builder.begin_batch();
        if !backing.is_empty() {
            self.builder
                .set_original_buffer(arrow::buffer::Buffer::from(backing));
        }

        // Invalidate cached dynamic handles from the previous batch -- they
        // reference columns that were drained by begin_batch().
        for entry in &mut self.scratch.attr_field_cache {
            entry.handle = None;
        }

        let string_storage = StringStorage::InputView;
        let decode_result =
            generated::for_each_export_resource_logs(body.as_ref(), |resource_logs| {
                decode::decode_resource_logs_wire(
                    &mut self.builder,
                    &self.handles,
                    &mut self.scratch,
                    &self.resource_prefix,
                    resource_logs,
                    string_storage,
                )
            });

        if let Err(e) = decode_result {
            // Reset builder to idle so the next begin_batch succeeds even
            // if we were mid-row when the error occurred.
            self.builder.discard_batch();
            return Err(e);
        }

        self.builder
            .finish_batch()
            .map_err(|e| ProjectionError::Batch(format!("batch build error: {e}")))
    }
}

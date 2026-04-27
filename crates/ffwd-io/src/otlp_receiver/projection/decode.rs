//! OTLP protobuf decode logic for projection.
//!
//! Walks the OTLP `ExportLogsServiceRequest` protobuf structure and
//! projects fields directly into a `ColumnarBatchBuilder`, bypassing
//! the full prost object graph.

use arrow::buffer::Buffer;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use ffwd_arrow::columnar::builder::ColumnarBatchBuilder;
use ffwd_arrow::columnar::plan::FieldHandle;

use super::ProjectionError;
use super::generated;
use super::wire::{AttrFieldCache, StringStorage, WireAny, WireScratch};

pub(super) fn decode_projected_otlp_logs_inner(
    body: &[u8],
    backing: Bytes,
    resource_prefix: &str,
    string_storage: StringStorage,
) -> Result<RecordBatch, ProjectionError> {
    if body.is_empty() {
        return Ok(RecordBatch::new_empty(
            arrow::datatypes::Schema::empty().into(),
        ));
    }

    let (plan, fields) = generated::build_otlp_plan();
    let mut builder = ColumnarBatchBuilder::new(plan);
    builder.begin_batch();
    if !backing.is_empty() {
        builder.set_original_buffer(Buffer::from(backing));
    }
    let mut scratch = WireScratch::default();

    generated::for_each_export_resource_logs(body, |resource_logs| {
        decode_resource_logs_wire(
            &mut builder,
            &fields,
            &mut scratch,
            resource_prefix,
            resource_logs,
            string_storage,
        )
    })?;

    builder.finish_batch().map_err(|e| {
        ProjectionError::Batch(format!("structured OTLP projection batch build error: {e}"))
    })
}

pub(super) fn decode_resource_logs_wire(
    builder: &mut ColumnarBatchBuilder,
    fields: &generated::OtlpFieldHandles,
    scratch: &mut WireScratch,
    resource_prefix: &str,
    resource_logs: &[u8],
    string_storage: StringStorage,
) -> Result<(), ProjectionError> {
    let mut resource_attrs = Vec::new();

    generated::for_each_resource_logs_resource(resource_logs, |resource| {
        collect_resource_attrs(
            builder,
            scratch,
            &mut resource_attrs,
            resource_prefix,
            resource,
        )
    })?;

    generated::for_each_resource_logs_scope_logs(resource_logs, |scope_logs| {
        decode_scope_logs_wire(
            builder,
            fields,
            scratch,
            &resource_attrs,
            scope_logs,
            string_storage,
        )
    })
}

fn collect_resource_attrs<'a>(
    builder: &mut ColumnarBatchBuilder,
    scratch: &mut WireScratch,
    resource_attrs: &mut Vec<(FieldHandle, WireAny<'a>)>,
    resource_prefix: &str,
    resource: &'a [u8],
) -> Result<(), ProjectionError> {
    generated::for_each_resource_attribute(resource, |attr| {
        if let Some((key, value)) = super::wire::decode_kv_inline(attr)? {
            // `decode_kv_inline` returns the key unvalidated; resource
            // attrs have no per-position cache so we have no memo to lean on.
            // Validate eagerly. The cost is small (handful of attrs per batch)
            // and the alternative — building a `&str` from concatenated bytes
            // via `from_utf8_unchecked` — would require a non-load-bearing
            // unsafe block to save one simdutf8 pass.
            scratch.resource_key.clear();
            scratch
                .resource_key
                .reserve(resource_prefix.len() + key.len());
            scratch
                .resource_key
                .extend_from_slice(resource_prefix.as_bytes());
            scratch.resource_key.extend_from_slice(key);
            let key_str = simdutf8::basic::from_utf8(&scratch.resource_key)
                .map_err(|_e| ProjectionError::Invalid("invalid UTF-8 resource attribute key"))?;
            let handle = builder
                .resolve_dynamic(key_str, generated::wire_any_field_kind(&value))
                .map_err(|e| ProjectionError::Batch(format!("resolve resource attr: {e}")))?;
            resource_attrs.push((handle, value));
        }
        Ok(())
    })
}

fn decode_scope_logs_wire(
    builder: &mut ColumnarBatchBuilder,
    fields: &generated::OtlpFieldHandles,
    scratch: &mut WireScratch,
    resource_attrs: &[(FieldHandle, WireAny<'_>)],
    scope_logs: &[u8],
    string_storage: StringStorage,
) -> Result<(), ProjectionError> {
    let mut scope_fields = generated::ScopeFields::default();

    generated::for_each_scope_logs_scope(scope_logs, |scope| {
        generated::merge_scope_wire(scope, &mut scope_fields)
    })?;

    generated::for_each_scope_logs_log_record(scope_logs, |log_record| {
        decode_log_record_wire(
            builder,
            fields,
            scratch,
            resource_attrs,
            scope_fields,
            log_record,
            string_storage,
        )
    })
}

fn decode_log_record_wire(
    builder: &mut ColumnarBatchBuilder,
    fields: &generated::OtlpFieldHandles,
    scratch: &mut WireScratch,
    resource_attrs: &[(FieldHandle, WireAny<'_>)],
    scope_fields: generated::ScopeFields<'_>,
    log_record: &[u8],
    string_storage: StringStorage,
) -> Result<(), ProjectionError> {
    scratch.attr_ranges.clear();
    let record = generated::decode_log_record_fields(log_record, &mut scratch.attr_ranges)?;

    builder.begin_row();

    let timestamp = if record.time_unix_nano > 0 {
        record.time_unix_nano
    } else {
        record.observed_time_unix_nano
    };
    if let Ok(value) = i64::try_from(timestamp)
        && value > 0
    {
        fields.write_timestamp(builder, value);
    }
    if let Ok(value) = i64::try_from(record.observed_time_unix_nano)
        && value > 0
    {
        fields.write_observed_timestamp(builder, value);
    }
    if let Some(value) = record.severity_text {
        fields.write_severity(builder, value, string_storage)?;
    }
    if record.severity_number > 0 {
        fields.write_severity_number(builder, record.severity_number);
    }
    if let Some(value) = record.body {
        fields.write_body(builder, value, scratch, string_storage)?;
    }
    if let Some(value) = record.trace_id {
        fields.write_trace_id(builder, value)?;
    }
    if let Some(value) = record.span_id {
        fields.write_span_id(builder, value)?;
    }
    if record.flags > 0 {
        fields.write_flags(builder, record.flags);
    }
    if let Some(value) = scope_fields.name
        && !value.is_empty()
    {
        fields.write_scope_name(builder, value, string_storage)?;
    }
    if let Some(value) = scope_fields.version
        && !value.is_empty()
    {
        fields.write_scope_version(builder, value, string_storage)?;
    }

    // Decode and write record attributes inline to avoid a per-row Vec allocation.
    // `attr_ranges` entries are Copy so the immutable index doesn't conflict with
    // the mutable scratch borrows needed by resolve/write.
    for attr_idx in 0..scratch.attr_ranges.len() {
        let (start, len) = scratch.attr_ranges[attr_idx];
        let attr = &log_record[start..start + len];
        if let Some((key, value)) = super::wire::decode_kv_inline(attr)? {
            let handle = resolve_record_attr_field(
                builder,
                &mut scratch.attr_field_cache,
                attr_idx,
                key,
                &value,
            )?;
            generated::write_wire_any(builder, handle, value, scratch, string_storage)?;
        }
    }

    for &(handle, value) in resource_attrs {
        generated::write_wire_any(builder, handle, value, scratch, string_storage)?;
    }

    builder.end_row();
    Ok(())
}

/// Resolve a per-record attribute key to a `FieldHandle`, using the
/// per-position cache to amortize repeated work across rows.
///
/// # Cache invariant
///
/// `cache[position].key` is **always valid UTF-8**. We establish this by
/// validating UTF-8 only on the cache-miss branch, before storing the key.
/// On a cache hit (byte-equal key), the new bytes are valid UTF-8 by
/// transitivity (UTF-8 is a deterministic property of bytes), so we skip
/// re-validation. `decode_kv_inline` deliberately leaves the key bytes
/// unvalidated so this memo can pay off.
///
/// # Cache-hit fast path
///
/// We check `key_len` first (a u32 compare) to reject mismatches before
/// the full `memcmp`, then only proceed to byte equality on length match.
fn resolve_record_attr_field(
    builder: &mut ColumnarBatchBuilder,
    cache: &mut Vec<AttrFieldCache>,
    position: usize,
    key: &[u8],
    value: &WireAny<'_>,
) -> Result<FieldHandle, ProjectionError> {
    let key_len = key.len() as u32;
    if let Some(cached) = cache.get(position)
        && cached.key_len == key_len
        && cached.key.as_slice() == key
        && let Some(handle) = cached.handle
    {
        return Ok(handle);
    }

    // Cache miss: validate now so the cache invariant is maintained.
    let key_str = simdutf8::basic::from_utf8(key)
        .map_err(|_e| ProjectionError::Invalid("invalid UTF-8 attribute key"))?;
    let handle = builder
        .resolve_dynamic(key_str, generated::wire_any_field_kind(value))
        .map_err(|e| ProjectionError::Batch(format!("resolve attr field: {e}")))?;
    if let Some(cached) = cache.get_mut(position) {
        cached.key.clear();
        cached.key.extend_from_slice(key);
        cached.key_len = key_len;
        cached.handle = Some(handle);
    } else {
        cache.push(AttrFieldCache {
            key: key.to_vec(),
            key_len,
            handle: Some(handle),
        });
    }
    Ok(handle)
}

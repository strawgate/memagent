//! Source and CRI metadata attachment for scanned batches.
//!
//! Adds `__source_id`, `file.path` (or style-specific path), and CRI
//! `_timestamp` / `_stream` columns to Arrow RecordBatches before the
//! SQL transform runs.

#[cfg(not(feature = "turmoil"))]
use std::collections::{HashMap, HashSet};
#[cfg(not(feature = "turmoil"))]
use std::sync::Arc;

#[cfg(not(feature = "turmoil"))]
use arrow::array::{
    Array, ArrayRef, LargeStringArray, StringArray, StringViewArray, StringViewBuilder,
    UInt64Builder,
};
#[cfg(not(feature = "turmoil"))]
use arrow::buffer::Buffer;
#[cfg(not(feature = "turmoil"))]
use arrow::datatypes::{DataType, Field, Schema};
#[cfg(not(feature = "turmoil"))]
use arrow::error::ArrowError;
#[cfg(not(feature = "turmoil"))]
use arrow::record_batch::RecordBatch;
#[cfg(not(feature = "turmoil"))]
use arrow::record_batch::RecordBatchOptions;

#[cfg(not(feature = "turmoil"))]
use logfwd_io::input::CriMetadata;
#[cfg(not(feature = "turmoil"))]
use logfwd_types::field_names;
#[cfg(not(feature = "turmoil"))]
use logfwd_types::pipeline::SourceId;
#[cfg(not(feature = "turmoil"))]
use logfwd_types::source_metadata::SourceMetadataPlan;

#[cfg(not(feature = "turmoil"))]
use super::RowOriginSpan;

#[cfg(not(feature = "turmoil"))]
pub(crate) fn source_metadata_for_batch(
    batch: RecordBatch,
    row_origins: &[RowOriginSpan],
    source_paths: &HashMap<SourceId, String>,
    plan: SourceMetadataPlan,
) -> Result<RecordBatch, ArrowError> {
    if !plan.has_any() {
        return Ok(batch);
    }

    let rows_from_origins: usize = row_origins.iter().map(|span| span.rows).sum();
    if rows_from_origins != batch.num_rows() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "source metadata sidecar row count mismatch: spans={rows_from_origins}, batch={}",
            batch.num_rows()
        )));
    }

    let mut columns = Vec::with_capacity(2);
    if plan.has_source_id {
        let mut builder = UInt64Builder::with_capacity(batch.num_rows());
        for span in row_origins {
            for _ in 0..span.rows {
                match span.source_id {
                    Some(sid) => builder.append_value(sid.0),
                    None => builder.append_null(),
                }
            }
        }
        columns.push(MetadataColumn {
            name: field_names::SOURCE_ID,
            data_type: DataType::UInt64,
            array: Arc::new(builder.finish()) as ArrayRef,
        });
    }

    if let Some(source_path_column) = plan.source_path.to_column_name() {
        let array = source_path_metadata_array(batch.num_rows(), row_origins, source_paths)?;
        columns.push(MetadataColumn {
            name: source_path_column,
            data_type: DataType::Utf8View,
            array,
        });
    }

    replace_or_append_columns(batch, columns)
}

#[cfg(not(feature = "turmoil"))]
pub(crate) fn cri_metadata_for_batch(
    batch: RecordBatch,
    cri_metadata: Option<CriMetadata>,
) -> Result<RecordBatch, ArrowError> {
    let Some(cri_metadata) = cri_metadata else {
        return Ok(batch);
    };
    if cri_metadata.rows != batch.num_rows() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "CRI metadata sidecar row count mismatch: rows={}, batch={}",
            cri_metadata.rows,
            batch.num_rows()
        )));
    }

    let has_values = cri_metadata.has_values;
    let schema = batch.schema();
    let needs_timestamp = has_values || schema.index_of(field_names::TIMESTAMP_UNDERSCORE).is_err();
    let needs_stream = has_values || schema.index_of(field_names::CRI_STREAM).is_err();
    if !needs_timestamp && !needs_stream {
        return Ok(batch);
    }

    let (timestamp, stream) = cri_metadata_arrays(batch.num_rows(), cri_metadata)?;
    let columns = [
        needs_timestamp
            .then(|| cri_metadata_column(&batch, field_names::TIMESTAMP_UNDERSCORE, timestamp))
            .transpose()?,
        needs_stream
            .then(|| cri_metadata_column(&batch, field_names::CRI_STREAM, stream))
            .transpose()?,
    ]
    .into_iter()
    .flatten()
    .collect();
    replace_or_append_columns(batch, columns)
}

#[cfg(not(feature = "turmoil"))]
fn cri_metadata_column(
    batch: &RecordBatch,
    name: &'static str,
    sidecar: ArrayRef,
) -> Result<MetadataColumn, ArrowError> {
    let array = overlay_existing_string_values(batch, name, sidecar)?;
    Ok(MetadataColumn {
        name,
        data_type: DataType::Utf8View,
        array,
    })
}

#[cfg(not(feature = "turmoil"))]
fn overlay_existing_string_values(
    batch: &RecordBatch,
    name: &str,
    sidecar: ArrayRef,
) -> Result<ArrayRef, ArrowError> {
    let Ok(existing_index) = batch.schema().index_of(name) else {
        return Ok(sidecar);
    };
    if sidecar.null_count() == 0 {
        return Ok(sidecar);
    }
    let sidecar = sidecar
        .as_any()
        .downcast_ref::<StringViewArray>()
        .ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "CRI metadata column {name} should be Utf8View"
            ))
        })?;
    let existing = batch.column(existing_index);
    let existing = string_view_array(existing)?;
    let mut builder = StringViewBuilder::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if !sidecar.is_null(row) {
            builder.append_value(sidecar.value(row));
        } else if let Some(existing) = &existing {
            if existing.is_null(row) {
                builder.append_null();
            } else {
                builder.append_value(existing.value(row));
            }
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(not(feature = "turmoil"))]
fn string_view_array(array: &ArrayRef) -> Result<Option<StringViewArray>, ArrowError> {
    match array.data_type() {
        DataType::Utf8View => array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .cloned()
            .map(Some)
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(
                    "Utf8View column is not a StringViewArray".to_string(),
                )
            }),
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Utf8 column is not a StringArray".to_string())
                })?;
            let mut builder = StringViewBuilder::with_capacity(array.len());
            for row in 0..array.len() {
                if array.is_null(row) {
                    builder.append_null();
                } else {
                    builder.append_value(array.value(row));
                }
            }
            Ok(Some(builder.finish()))
        }
        DataType::LargeUtf8 => {
            let array = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError(
                        "LargeUtf8 column is not a LargeStringArray".to_string(),
                    )
                })?;
            let mut builder = StringViewBuilder::with_capacity(array.len());
            for row in 0..array.len() {
                if array.is_null(row) {
                    builder.append_null();
                } else {
                    builder.append_value(array.value(row));
                }
            }
            Ok(Some(builder.finish()))
        }
        _ => Ok(None),
    }
}

#[cfg(not(feature = "turmoil"))]
fn cri_metadata_arrays(
    num_rows: usize,
    cri_metadata: CriMetadata,
) -> Result<(ArrayRef, ArrayRef), ArrowError> {
    let mut timestamp = StringViewBuilder::with_capacity(num_rows);
    let mut stream = StringViewBuilder::with_capacity(num_rows);
    let timestamp_buffer = Buffer::from(cri_metadata.timestamp_bytes);
    let timestamp_block =
        (!timestamp_buffer.is_empty()).then(|| timestamp.append_block(timestamp_buffer.clone()));
    for span in cri_metadata.spans {
        let Some(values) = span.values else {
            append_null_metadata_views(&mut timestamp, span.rows);
            append_null_metadata_views(&mut stream, span.rows);
            continue;
        };
        let timestamp_start = values.timestamp_start;
        let timestamp_end = timestamp_start.saturating_add(values.timestamp_len);
        let timestamp_value = timestamp_buffer
            .get(timestamp_start..timestamp_end)
            .and_then(|bytes| std::str::from_utf8(bytes).ok());
        let Some(timestamp_value) = timestamp_value else {
            append_null_metadata_views(&mut timestamp, span.rows);
            append_null_metadata_views(&mut stream, span.rows);
            continue;
        };
        if values.timestamp_len <= 12 {
            append_inline_metadata_values(&mut timestamp, timestamp_value, span.rows);
            append_inline_metadata_values(&mut stream, values.stream.as_str(), span.rows);
            continue;
        }
        let timestamp_offset = u32::try_from(values.timestamp_start).map_err(|_e| {
            ArrowError::InvalidArgumentError(
                "CRI timestamp string offset is too large for Utf8View".to_string(),
            )
        })?;
        let timestamp_len = u32::try_from(values.timestamp_len).map_err(|_e| {
            ArrowError::InvalidArgumentError(
                "CRI timestamp string is too large for Utf8View".to_string(),
            )
        })?;
        let Some(timestamp_block) = timestamp_block else {
            return Err(ArrowError::InvalidArgumentError(
                "CRI metadata span references an empty timestamp buffer".to_string(),
            ));
        };
        append_block_metadata_views(
            &mut timestamp,
            timestamp_block,
            timestamp_offset,
            timestamp_len,
            span.rows,
        )?;

        let stream_value = values.stream.as_str();
        append_inline_metadata_values(&mut stream, stream_value, span.rows);
    }
    Ok((
        finish_string_metadata_array(timestamp, num_rows)?,
        finish_string_metadata_array(stream, num_rows)?,
    ))
}

#[cfg(not(feature = "turmoil"))]
fn source_path_metadata_array(
    num_rows: usize,
    row_origins: &[RowOriginSpan],
    source_paths: &HashMap<SourceId, String>,
) -> Result<ArrayRef, ArrowError> {
    let mut builder = StringViewBuilder::with_capacity(num_rows);
    let mut first_block = None;
    let mut extra_blocks = Vec::new();
    for span in row_origins {
        let Some(source_id) = span.source_id else {
            append_null_metadata_views(&mut builder, span.rows);
            continue;
        };
        let Some((block, len)) = source_path_metadata_block(
            &mut builder,
            source_id,
            source_paths,
            &mut first_block,
            &mut extra_blocks,
        )?
        else {
            append_null_metadata_views(&mut builder, span.rows);
            continue;
        };
        append_block_metadata_views(&mut builder, block, 0, len, span.rows)?;
    }
    finish_string_metadata_array(builder, num_rows)
}

/// Look up or create the `StringView` block index for a source path.
///
/// Caches the first block inline and spills subsequent unique source paths
/// into `extra_blocks` so repeated rows referencing the same source reuse
/// a single buffer allocation.
// Verified: turmoil cfg alignment is correct — this function is only called
// from `source_path_metadata_array` which is also `#[cfg(not(feature = "turmoil"))]`.
#[cfg(not(feature = "turmoil"))]
fn source_path_metadata_block(
    builder: &mut StringViewBuilder,
    source_id: SourceId,
    source_paths: &HashMap<SourceId, String>,
    first_block: &mut Option<(SourceId, u32, u32)>,
    extra_blocks: &mut Vec<(SourceId, u32, u32)>,
) -> Result<Option<(u32, u32)>, ArrowError> {
    if let Some((cached_source_id, block, len)) = first_block.as_ref()
        && *cached_source_id == source_id
    {
        return Ok(Some((*block, *len)));
    }
    if let Some((_, block, len)) = extra_blocks
        .iter()
        .find(|(cached_source_id, _, _)| *cached_source_id == source_id)
    {
        return Ok(Some((*block, *len)));
    }

    let Some(value) = source_paths.get(&source_id).map(String::as_str) else {
        return Ok(None);
    };
    let len = u32::try_from(value.len()).map_err(|_overflow| {
        ArrowError::InvalidArgumentError(
            "source metadata string is too large for Utf8View".to_string(),
        )
    })?;
    let block = builder.append_block(Buffer::from(value.as_bytes().to_vec()));
    if first_block.is_none() {
        *first_block = Some((source_id, block, len));
    } else {
        extra_blocks.push((source_id, block, len));
    }
    Ok(Some((block, len)))
}

#[cfg(not(feature = "turmoil"))]
fn append_block_metadata_views(
    builder: &mut StringViewBuilder,
    block: u32,
    offset: u32,
    len: u32,
    rows: usize,
) -> Result<(), ArrowError> {
    for _ in 0..rows {
        builder.try_append_view(block, offset, len)?;
    }
    Ok(())
}

#[cfg(not(feature = "turmoil"))]
fn append_inline_metadata_values(builder: &mut StringViewBuilder, value: &str, rows: usize) {
    for _ in 0..rows {
        builder.append_value(value);
    }
}

#[cfg(not(feature = "turmoil"))]
fn append_null_metadata_views(builder: &mut StringViewBuilder, rows: usize) {
    for _ in 0..rows {
        builder.append_null();
    }
}

#[cfg(not(feature = "turmoil"))]
fn finish_string_metadata_array(
    mut builder: StringViewBuilder,
    num_rows: usize,
) -> Result<ArrayRef, ArrowError> {
    let array = builder.finish();
    if array.len() != num_rows {
        return Err(ArrowError::InvalidArgumentError(format!(
            "source metadata string column length mismatch: column={}, batch={num_rows}",
            array.len()
        )));
    }
    Ok(Arc::new(array) as ArrayRef)
}

#[cfg(not(feature = "turmoil"))]
struct MetadataColumn {
    name: &'static str,
    data_type: DataType,
    array: ArrayRef,
}

#[cfg(not(feature = "turmoil"))]
fn replace_or_append_columns(
    batch: RecordBatch,
    columns: Vec<MetadataColumn>,
) -> Result<RecordBatch, ArrowError> {
    if columns.is_empty() {
        return Ok(batch);
    }

    let schema = batch.schema();
    let mut fields = Vec::with_capacity(schema.fields().len() + columns.len());
    let mut arrays = Vec::with_capacity(batch.num_columns() + columns.len());
    let mut replaced = HashSet::with_capacity(columns.len());

    for (idx, field) in schema.fields().iter().enumerate() {
        if let Some(column) = columns
            .iter()
            .find(|column| field.name().as_str() == column.name)
        {
            fields.push(Field::new(column.name, column.data_type.clone(), true));
            arrays.push(Arc::clone(&column.array));
            replaced.insert(column.name);
        } else {
            fields.push((**field).clone());
            arrays.push(Arc::clone(batch.column(idx)));
        }
    }

    for column in columns {
        if !replaced.contains(column.name) {
            fields.push(Field::new(column.name, column.data_type, true));
            arrays.push(column.array);
        }
    }

    let new_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    RecordBatch::try_new_with_options(
        new_schema,
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
}

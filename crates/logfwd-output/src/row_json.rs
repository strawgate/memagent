#![allow(clippy::undocumented_unsafe_blocks)]
// xtask-verify: allow(pub_module_needs_tests) reason: integration coverage lives in otlp_sink/arrow sink tests; dedicated unit tests tracked separately
use std::io::{self, Write};

use arrow::array::{
    Array, AsArray, BinaryArray, FixedSizeBinaryArray, LargeBinaryArray, LargeStringArray,
    StringArray, StringViewArray, StructArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use crate::conflict_columns::{ColInfo, ResolvedCol, TypedArrayRef, get_array, is_null};

/// Coalesce a conflict field to a `String` using `str_variants` ordering
/// (Utf8 wins, then Boolean, Int64, Float64).  Returns `None` if all variants
/// are null.
///
/// Used by Loki label extraction to always produce a string value.
pub(crate) fn coalesce_as_str(batch: &RecordBatch, row: usize, col: &ColInfo) -> Option<String> {
    let variant = col.str_variants.iter().find(|v| !is_null(batch, v, row))?;
    let arr = get_array(batch, variant)?;
    let s = match arr.data_type() {
        DataType::Int64 => {
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::Int64Type>()
                    .value_unchecked(row)
            };
            itoa::Buffer::new().format(v).to_string()
        }
        DataType::Float64 => {
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::Float64Type>()
                    .value_unchecked(row)
            };
            if v.is_finite() {
                ryu::Buffer::new().format_finite(v).to_string()
            } else {
                return None;
            }
        }
        DataType::Boolean => {
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            if unsafe { arr.as_boolean().value_unchecked(row) } {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        _ => array_value_to_string(arr, row).ok()?,
    };
    Some(s)
}

const NEEDS_ESCAPE: [bool; 256] = {
    let mut table = [false; 256];
    let mut i = 0;
    while i < 0x20 {
        table[i] = true;
        i += 1;
    }
    table[b'"' as usize] = true;
    table[b'\\' as usize] = true;
    table
};

/// Return the index of the first byte in `bytes` that requires JSON escaping,
/// or `bytes.len()` if there are none.
///
/// Uses a 256-byte lookup table to quickly find `"` or `\` or control
/// characters (< 0x20) in a single pass. This avoids the setup overhead of
/// `memchr2`.
#[inline]
fn first_escape_pos(bytes: &[u8]) -> usize {
    for (i, &b) in bytes.iter().enumerate() {
        if NEEDS_ESCAPE[b as usize] {
            return i;
        }
    }
    bytes.len()
}

/// Write a JSON string value with RFC 8259 escaping.
///
/// **Hot path optimization**: most log field values (timestamps, service names,
/// HTTP paths, hex IDs, etc.) contain zero bytes requiring JSON escaping.  This
/// implementation uses [`first_escape_pos`] to find the first special byte with
/// a SIMD-accelerated scan, then falls through to a bulk `extend_from_slice`
/// for the safe prefix (one optimized `memcpy` instead of N individual byte
/// pushes).  Only when an escape byte is actually found does the slow per-byte
/// path run for that single byte.
///
/// Benchmark impact (wide/10K): ~2–3× throughput improvement over the prior
/// byte-by-byte loop.
pub(crate) fn write_json_string(out: &mut Vec<u8>, v: &str) -> io::Result<()> {
    out.push(b'"');
    let bytes = v.as_bytes();
    let mut start = 0;

    while start < bytes.len() {
        let remaining = &bytes[start..];
        let rel_pos = first_escape_pos(remaining);

        // Bulk-copy the safe prefix — compiles to a single `memcpy`.
        if rel_pos > 0 {
            out.extend_from_slice(&remaining[..rel_pos]);
        }

        if rel_pos == remaining.len() {
            // No more bytes needing escaping — we're done.
            break;
        }

        // Handle the one byte at `start + rel_pos` that needs escaping.
        match remaining[rel_pos] {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            b => Write::write_fmt(out, format_args!("\\u{b:04x}"))?,
        }
        start += rel_pos + 1;
    }

    out.push(b'"');
    Ok(())
}

/// Write bytes as a lowercase hex JSON string prefixed with `0x`.
fn write_json_hex_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    // Reserve for surrounding quotes + "0x" prefix + 2 hex chars per byte.
    out.reserve(4 + bytes.len().saturating_mul(2));
    out.push(b'"');
    out.extend_from_slice(b"0x");
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize]);
        out.push(HEX[(b & 0x0f) as usize]);
    }
    out.push(b'"');
}

/// Write a single Arrow value as JSON, dispatching on the actual Arrow DataType.
///
/// Integer types → unquoted integer, float types → unquoted number (null for
/// non-finite), Null → JSON null, Boolean → true/false/null, everything else →
/// quoted string. This preserves JSON type fidelity on roundtrip without
/// relying on column name suffixes.
pub(crate) fn write_json_value(arr: &dyn Array, row: usize, out: &mut Vec<u8>) -> io::Result<()> {
    if arr.is_null(row) {
        out.extend_from_slice(b"null");
        return Ok(());
    }

    match arr.data_type() {
        DataType::Null => {
            out.extend_from_slice(b"null");
        }
        DataType::Int8 => {
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::Int8Type>()
                    .value_unchecked(row)
            };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::Int16 => {
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::Int16Type>()
                    .value_unchecked(row)
            };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::Int32 => {
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::Int32Type>()
                    .value_unchecked(row)
            };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::Int64 => {
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::Int64Type>()
                    .value_unchecked(row)
            };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::UInt8 => {
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::UInt8Type>()
                    .value_unchecked(row)
            };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::UInt16 => {
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::UInt16Type>()
                    .value_unchecked(row)
            };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::UInt32 => {
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::UInt32Type>()
                    .value_unchecked(row)
            };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::UInt64 => {
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::UInt64Type>()
                    .value_unchecked(row)
            };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::Float32 => {
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::Float32Type>()
                    .value_unchecked(row)
            };
            if v.is_finite() {
                out.extend_from_slice(ryu::Buffer::new().format_finite(v).as_bytes());
            } else {
                out.extend_from_slice(b"null");
            }
        }
        DataType::Float64 => {
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::Float64Type>()
                    .value_unchecked(row)
            };
            if v.is_finite() {
                out.extend_from_slice(ryu::Buffer::new().format_finite(v).as_bytes());
            } else {
                out.extend_from_slice(b"null");
            }
        }
        DataType::Boolean => {
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            let v = unsafe { arr.as_boolean().value_unchecked(row) };
            out.extend_from_slice(if v { b"true" } else { b"false" });
        }
        // String types: use write_json_string (memchr2 SIMD fast-path) instead of
        // the fallback `array_value_to_string` which heap-allocates per call.
        // StreamingBuilder uses Utf8View; these three arms cover all common cases.
        DataType::Utf8 => {
            let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DataType::Utf8 did not downcast to StringArray",
                ));
            };
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            write_json_string(out, unsafe { str_arr.value_unchecked(row) })?;
        }
        DataType::LargeUtf8 => {
            let Some(str_arr) = arr.as_any().downcast_ref::<LargeStringArray>() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DataType::LargeUtf8 did not downcast to LargeStringArray",
                ));
            };
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            write_json_string(out, unsafe { str_arr.value_unchecked(row) })?;
        }
        DataType::Utf8View => {
            let Some(str_arr) = arr.as_any().downcast_ref::<StringViewArray>() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DataType::Utf8View did not downcast to StringViewArray",
                ));
            };
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            write_json_string(out, unsafe { str_arr.value_unchecked(row) })?;
        }
        DataType::Struct(schema_fields) => {
            let Some(struct_arr) = arr.as_any().downcast_ref::<StructArray>() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DataType::Struct did not downcast to StructArray",
                ));
            };
            out.push(b'{');
            let num_fields = struct_arr.num_columns();
            let mut first = true;
            for field_idx in 0..num_fields {
                if !first {
                    out.push(b',');
                }
                first = false;
                let field_name = schema_fields[field_idx].name();
                write_json_string(out, field_name)?;
                out.push(b':');
                let child_arr = struct_arr.column(field_idx);
                if child_arr.is_null(row) {
                    out.extend_from_slice(b"null");
                } else {
                    write_json_value(child_arr.as_ref(), row, out)?;
                }
            }
            out.push(b'}');
        }
        DataType::Binary => {
            let Some(bin_arr) = arr.as_any().downcast_ref::<BinaryArray>() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DataType::Binary did not downcast to BinaryArray",
                ));
            };
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            let bytes = unsafe { bin_arr.value_unchecked(row) };
            write_json_hex_bytes(out, bytes);
        }
        DataType::LargeBinary => {
            let Some(large_bin_arr) = arr.as_any().downcast_ref::<LargeBinaryArray>() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DataType::LargeBinary did not downcast to LargeBinaryArray",
                ));
            };
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            let bytes = unsafe { large_bin_arr.value_unchecked(row) };
            write_json_hex_bytes(out, bytes);
        }
        DataType::FixedSizeBinary(_) => {
            let Some(fixed_bin_arr) = arr.as_any().downcast_ref::<FixedSizeBinaryArray>() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DataType::FixedSizeBinary did not downcast to FixedSizeBinaryArray",
                ));
            };
            // SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
            let bytes = unsafe { fixed_bin_arr.value_unchecked(row) };
            write_json_hex_bytes(out, bytes);
        }
        _ => {
            let fallback = array_value_to_string(arr, row).unwrap_or_default();
            write_json_string(out, &fallback)?;
        }
    }
    Ok(())
}

/// Write a single pre-typed Arrow value as JSON.
///
/// Like [`write_json_value`] but dispatches on a [`TypedArrayRef`] enum
/// discriminant instead of calling virtual `data_type()` + `downcast_ref()`
/// per row.  The caller guarantees the row is not null, eliminating the
/// redundant `is_null()` check at the top of `write_json_value`.
pub(crate) fn write_typed_json_value(
    typed: &TypedArrayRef,
    row: usize,
    out: &mut Vec<u8>,
) -> io::Result<()> {
    match typed {
        TypedArrayRef::Null => {
            out.extend_from_slice(b"null");
        }
        // SAFETY for all typed arrays: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch
        TypedArrayRef::Int8(a) => {
            let v =  unsafe { a.value_unchecked(row) };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        TypedArrayRef::Int16(a) => {
            let v =  unsafe { a.value_unchecked(row) };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        TypedArrayRef::Int32(a) => {
            let v =  unsafe { a.value_unchecked(row) };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        TypedArrayRef::Int64(a) => {
            let v =  unsafe { a.value_unchecked(row) };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        TypedArrayRef::UInt8(a) => {
            let v =  unsafe { a.value_unchecked(row) };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        TypedArrayRef::UInt16(a) => {
            let v =  unsafe { a.value_unchecked(row) };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        TypedArrayRef::UInt32(a) => {
            let v =  unsafe { a.value_unchecked(row) };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        TypedArrayRef::UInt64(a) => {
            let v =  unsafe { a.value_unchecked(row) };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        TypedArrayRef::Float32(a) => {
            let v =  unsafe { a.value_unchecked(row) };
            if v.is_finite() {
                out.extend_from_slice(ryu::Buffer::new().format_finite(v).as_bytes());
            } else {
                out.extend_from_slice(b"null");
            }
        }
        TypedArrayRef::Float64(a) => {
            let v =  unsafe { a.value_unchecked(row) };
            if v.is_finite() {
                out.extend_from_slice(ryu::Buffer::new().format_finite(v).as_bytes());
            } else {
                out.extend_from_slice(b"null");
            }
        }
        TypedArrayRef::Boolean(a) => {
            let v =  unsafe { a.value_unchecked(row) };
            out.extend_from_slice(if v { b"true" } else { b"false" });
        }
        TypedArrayRef::Utf8(a) => {
            write_json_string(out,  unsafe { a.value_unchecked(row) })?;
        }
        TypedArrayRef::LargeUtf8(a) => {
            write_json_string(out,  unsafe { a.value_unchecked(row) })?;
        }
        TypedArrayRef::Utf8View(a) => {
            write_json_string(out,  unsafe { a.value_unchecked(row) })?;
        }
        TypedArrayRef::Struct(a) => {
            out.push(b'{');
            let fields = a.fields();
            let mut first = true;
            for field_idx in 0..a.num_columns() {
                if !first {
                    out.push(b',');
                }
                first = false;
                write_json_string(out, fields[field_idx].name())?;
                out.push(b':');
                let child = a.column(field_idx);
                if child.is_null(row) {
                    out.extend_from_slice(b"null");
                } else {
                    write_json_value(child.as_ref(), row, out)?;
                }
            }
            out.push(b'}');
        }
        TypedArrayRef::Binary(a) => {
            write_json_hex_bytes(out,  unsafe { a.value_unchecked(row) });
        }
        TypedArrayRef::LargeBinary(a) => {
            write_json_hex_bytes(out,  unsafe { a.value_unchecked(row) });
        }
        TypedArrayRef::FixedSizeBinary(a) => {
            write_json_hex_bytes(out,  unsafe { a.value_unchecked(row) });
        }
        TypedArrayRef::Other(arr) => {
            // Fallback: use the dyn Array path (includes is_null check).
            write_json_value(*arr, row, out)?;
        }
    }
    Ok(())
}

/// Write a single row as a JSON object using pre-resolved typed column references.
///
/// This is the fast-path variant of [`write_row_json`] that eliminates per-row:
/// - virtual `is_null()` dispatch → direct `NullBuffer` bitmap check
/// - `data_type()` + `downcast_ref` → pre-resolved `TypedArrayRef` enum
/// - `batch.columns().get()` + `Arc::deref` → cached references
/// - per-field `push(b',')` → comma embedded in key_json prefix
pub fn write_row_json_resolved(
    row: usize,
    cols: &[ResolvedCol],
    out: &mut Vec<u8>,
    newline: bool,
) -> io::Result<()> {
    out.push(b'{');
    // key_json is `b',"fieldname":'`.  For the first non-null field we skip
    // the leading comma (offset 1); for subsequent fields we include it (offset 0).
    // This replaces N separate `push(b',')` calls with one extend_from_slice.
    let mut sep_skip = 1usize;
    for col in cols {
        let Some((key_json, typed)) = col.resolve(row) else {
            continue;
        };

        out.extend_from_slice(&key_json[sep_skip..]);
        write_typed_json_value(typed, row, out)?;
        sep_skip = 0;
    }
    if newline {
        out.extend_from_slice(b"}\n");
    } else {
        out.push(b'}');
    }
    Ok(())
}

/// Write a single row as a JSON object into `out`.
///
/// For fields backed by struct conflict columns or multiple flat typed columns,
/// the first non-null variant (by `json_variants` ordering) is used. If all
/// variants are null the field is omitted entirely — absent keys are the JSON
/// convention for "no value". Type dispatch uses the Arrow DataType, not the
/// column name suffix.
pub fn write_row_json(
    batch: &RecordBatch,
    row: usize,
    cols: &[ColInfo],
    out: &mut Vec<u8>,
    newline: bool,
) -> io::Result<()> {
    out.push(b'{');
    let mut sep_skip = 1usize;
    for col in cols {
        // Find the first non-null variant for this field (json ordering).
        let variant = col.json_variants.iter().find(|v| !is_null(batch, v, row));

        let Some(v) = variant else {
            // All variants null for this row — omit field entirely.
            continue;
        };
        let Some(arr) = get_array(batch, v) else {
            debug_assert!(
                false,
                "non-null variant but array not found for field {}",
                col.field_name
            );
            continue;
        };

        // Key — pre-serialized `,"fieldname":` bytes (comma-prefixed).
        // First non-null field skips the leading comma.
        out.extend_from_slice(&col.key_json[sep_skip..]);
        sep_skip = 0;

        // Value — dispatch on Arrow DataType, not column name suffix
        write_json_value(arr, row, out)?;
    }
    if newline {
        out.extend_from_slice(b"}\n");
    } else {
        out.push(b'}');
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, StringArray, StructArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{DataType, Field, Schema};
    use proptest::prelude::*;

    use crate::{build_col_infos, resolve_col_infos};

    mod arrow_test_utils {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../logfwd-test-utils/src/arrow.rs"
        ));
    }

    fn serialize_row_via_default(batch: &RecordBatch, row: usize) -> String {
        let cols = build_col_infos(batch);
        let mut out = Vec::new();
        write_row_json(batch, row, &cols, &mut out, false).expect("row serialization must succeed");
        String::from_utf8(out).expect("row json must be utf8")
    }

    fn serialize_row_via_resolved(batch: &RecordBatch, row: usize) -> String {
        let cols = build_col_infos(batch);
        let resolved = resolve_col_infos(batch, &cols);
        let mut out = Vec::new();
        write_row_json_resolved(row, &resolved, &mut out, false)
            .expect("resolved row serialization must succeed");
        String::from_utf8(out).expect("row json must be utf8")
    }

    fn scalar_batch(rows: Vec<(String, i64, f64, bool)>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
            Field::new("ratio", DataType::Float64, false),
            Field::new("ok", DataType::Boolean, false),
        ]));

        let text = rows.iter().map(|row| row.0.as_str()).collect::<Vec<_>>();
        let count = rows.iter().map(|row| row.1).collect::<Vec<_>>();
        let ratio = rows.iter().map(|row| row.2).collect::<Vec<_>>();
        let ok = rows.iter().map(|row| row.3).collect::<Vec<_>>();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(text)),
                Arc::new(Int64Array::from(count)),
                Arc::new(Float64Array::from(ratio)),
                Arc::new(arrow::array::BooleanArray::from(ok)),
            ],
        )
        .expect("scalar batch must be valid")
    }

    proptest! {
        #[test]
        fn row_json_produces_valid_json(batch in arrow_test_utils::arb_record_batch()) {
            let cols = build_col_infos(&batch);
            for row in 0..batch.num_rows() {
                let mut out = Vec::new();
                write_row_json(&batch, row, &cols, &mut out, false).expect("serialize");
                let text = String::from_utf8(out).expect("utf8");
                let value: serde_json::Value = serde_json::from_str(&text).expect("valid json");
                prop_assert!(value.is_object(), "row json must be an object: {text}");
            }
        }

        #[test]
        fn row_json_field_count_matches_schema(
            rows in proptest::collection::vec(
                (
                    proptest::collection::vec(any::<char>(), 0..=10).prop_map(|chars| chars.into_iter().collect::<String>()),
                    any::<i64>(),
                    (-1_000_000i64..1_000_000i64).prop_map(|n| n as f64 / 10.0),
                    any::<bool>(),
                ),
                0..20,
            )
        ) {
            let batch = scalar_batch(rows);
            for row in 0..batch.num_rows() {
                let text = serialize_row_via_default(&batch, row);
                let value: serde_json::Value = serde_json::from_str(&text).expect("valid json");
                let object = value.as_object().expect("must be object");
                prop_assert_eq!(object.len(), batch.schema().fields().len());
            }
        }

        #[test]
        fn row_json_null_handling(
            text_values in proptest::collection::vec(prop::option::of(any::<String>()), 0..20),
            int_values in proptest::collection::vec(prop::option::of(any::<i64>()), 0..20),
        ) {
            let row_count = text_values.len().min(int_values.len());
            let text_values = text_values.into_iter().take(row_count).collect::<Vec<_>>();
            let int_values = int_values.into_iter().take(row_count).collect::<Vec<_>>();

            let struct_fields = vec![
                Arc::new(Field::new("inner_text", DataType::Utf8, true)),
                Arc::new(Field::new("inner_int", DataType::Int64, true)),
            ];
            let struct_array = StructArray::new(
                struct_fields.clone().into(),
                vec![
                    Arc::new(StringArray::from(text_values.iter().map(|v| v.as_deref()).collect::<Vec<_>>())),
                    Arc::new(Int64Array::from(int_values.clone())),
                ],
                Some(NullBuffer::from(vec![true; row_count])),
            );
            let schema = Arc::new(Schema::new(vec![Field::new(
                "nested",
                DataType::Struct(struct_fields.into()),
                true,
            )]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(struct_array)]).expect("valid nested batch");

            for row in 0..batch.num_rows() {
                let text = serialize_row_via_default(&batch, row);
                let value: serde_json::Value = serde_json::from_str(&text).expect("valid json");
                let nested = value
                    .get("nested")
                    .and_then(serde_json::Value::as_object)
                    .expect("nested object should be present");

                let inner_text = nested.get("inner_text").expect("inner_text key present");
                let inner_int = nested.get("inner_int").expect("inner_int key present");
                prop_assert_ne!(inner_text, "null", "inner_text must not serialize as string literal 'null'");
                prop_assert_ne!(inner_int, "null", "inner_int must not serialize as string literal 'null'");

                if text_values[row].is_none() {
                    prop_assert!(inner_text.is_null(), "null inner_text must serialize as JSON null");
                }
                if int_values[row].is_none() {
                    prop_assert!(inner_int.is_null(), "null inner_int must serialize as JSON null");
                }
            }
        }

        #[test]
        fn row_json_paths_agree(batch in arrow_test_utils::arb_record_batch_without_null_column()) {
            for row in 0..batch.num_rows() {
                let direct = serialize_row_via_default(&batch, row);
                let resolved = serialize_row_via_resolved(&batch, row);
                prop_assert_eq!(resolved, direct, "resolved and default row serializers diverged");
            }
        }
    }

    #[test]
    fn row_json_special_floats() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ratio",
            DataType::Float64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(1.5),
            ]))],
        )
        .expect("valid float batch");

        let cols = build_col_infos(&batch);
        for row in 0..batch.num_rows() {
            let mut out = Vec::new();
            write_row_json(&batch, row, &cols, &mut out, false).expect("serialize");
            let value: serde_json::Value = serde_json::from_slice(&out).expect("valid json");
            let ratio = value.get("ratio").expect("ratio field present");
            if row < 3 {
                assert!(ratio.is_null(), "NaN and infinities must serialize as null");
            } else {
                assert_eq!(ratio.as_f64(), Some(1.5));
            }
        }
    }
}

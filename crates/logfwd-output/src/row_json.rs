use std::io::{self, Write};

use arrow::array::{
    Array, AsArray, BinaryArray, FixedSizeBinaryArray, LargeBinaryArray, StructArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use crate::conflict_columns::{ColInfo, get_array, is_null};

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
            let v = arr.as_primitive::<arrow::datatypes::Int64Type>().value(row);
            itoa::Buffer::new().format(v).to_string()
        }
        DataType::Float64 => {
            let v = arr
                .as_primitive::<arrow::datatypes::Float64Type>()
                .value(row);
            if v.is_finite() {
                ryu::Buffer::new().format_finite(v).to_string()
            } else {
                return None;
            }
        }
        DataType::Boolean => {
            if arr.as_boolean().value(row) {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        _ => array_value_to_string(arr, row).ok()?,
    };
    Some(s)
}

/// Write a JSON string value with RFC 8259 escaping.
pub(crate) fn write_json_string(out: &mut Vec<u8>, v: &str) -> io::Result<()> {
    out.push(b'"');
    for &b in v.as_bytes() {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            b if b < 0x20 => {
                Write::write_fmt(out, format_args!("\\u{:04x}", b))?;
            }
            _ => out.push(b),
        }
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
            let v = arr.as_primitive::<arrow::datatypes::Int8Type>().value(row);
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::Int16 => {
            let v = arr.as_primitive::<arrow::datatypes::Int16Type>().value(row);
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::Int32 => {
            let v = arr.as_primitive::<arrow::datatypes::Int32Type>().value(row);
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::Int64 => {
            let v = arr.as_primitive::<arrow::datatypes::Int64Type>().value(row);
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::UInt8 => {
            let v = arr.as_primitive::<arrow::datatypes::UInt8Type>().value(row);
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::UInt16 => {
            let v = arr
                .as_primitive::<arrow::datatypes::UInt16Type>()
                .value(row);
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::UInt32 => {
            let v = arr
                .as_primitive::<arrow::datatypes::UInt32Type>()
                .value(row);
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::UInt64 => {
            let v = arr
                .as_primitive::<arrow::datatypes::UInt64Type>()
                .value(row);
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::Float32 => {
            let v = arr
                .as_primitive::<arrow::datatypes::Float32Type>()
                .value(row);
            if v.is_finite() {
                out.extend_from_slice(ryu::Buffer::new().format_finite(v).as_bytes());
            } else {
                out.extend_from_slice(b"null");
            }
        }
        DataType::Float64 => {
            let v = arr
                .as_primitive::<arrow::datatypes::Float64Type>()
                .value(row);
            if v.is_finite() {
                out.extend_from_slice(ryu::Buffer::new().format_finite(v).as_bytes());
            } else {
                out.extend_from_slice(b"null");
            }
        }
        DataType::Boolean => {
            let v = arr.as_boolean().value(row);
            out.extend_from_slice(if v { b"true" } else { b"false" });
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
            let bytes = bin_arr.value(row);
            write_json_hex_bytes(out, bytes);
        }
        DataType::LargeBinary => {
            let Some(large_bin_arr) = arr.as_any().downcast_ref::<LargeBinaryArray>() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DataType::LargeBinary did not downcast to LargeBinaryArray",
                ));
            };
            let bytes = large_bin_arr.value(row);
            write_json_hex_bytes(out, bytes);
        }
        DataType::FixedSizeBinary(_) => {
            let Some(fixed_bin_arr) = arr.as_any().downcast_ref::<FixedSizeBinaryArray>() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DataType::FixedSizeBinary did not downcast to FixedSizeBinaryArray",
                ));
            };
            let bytes = fixed_bin_arr.value(row);
            write_json_hex_bytes(out, bytes);
        }
        _ => {
            let fallback = array_value_to_string(arr, row).unwrap_or_default();
            write_json_string(out, &fallback)?;
        }
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
) -> io::Result<()> {
    out.push(b'{');
    let mut first = true;
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

        if !first {
            out.push(b',');
        }
        first = false;

        // Key — escape to produce valid JSON if field_name contains special chars.
        write_json_string(out, &col.field_name)?;
        out.push(b':');

        // Value — dispatch on Arrow DataType, not column name suffix
        write_json_value(arr, row, out)?;
    }
    out.push(b'}');
    Ok(())
}

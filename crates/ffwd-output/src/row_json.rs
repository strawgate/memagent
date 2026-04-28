// xtask-verify: allow(pub_module_needs_tests) reason: integration coverage lives in otlp_sink/arrow sink tests; dedicated unit tests tracked separately
use std::io;

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
            // SAFETY: arr is a column of batch, so arr.len() == batch.num_rows(); row < batch.num_rows() is the caller's invariant
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::Int64Type>()
                    .value_unchecked(row)
            };
            itoa::Buffer::new().format(v).to_string()
        }
        DataType::Float64 => {
            // SAFETY: arr is a column of batch, so arr.len() == batch.num_rows(); row < batch.num_rows() is the caller's invariant
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
            // SAFETY: arr is a column of batch, so arr.len() == batch.num_rows(); row < batch.num_rows() is the caller's invariant
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

const SWAR_ONES: u64 = 0x0101_0101_0101_0101;
const SWAR_HIGH_BITS: u64 = 0x8080_8080_8080_8080;
const SWAR_TOP3_MASK: u64 = 0xE0E0_E0E0_E0E0_E0E0;

#[inline]
fn swar_zero_byte_mask(chunk: u64) -> u64 {
    (chunk.wrapping_sub(SWAR_ONES)) & !chunk & SWAR_HIGH_BITS
}

/// Return the index of the first byte in `bytes` that requires JSON escaping,
/// or `bytes.len()` if there are none.
///
/// Uses a two-level strategy:
/// 1. Process 8 bytes at a time using SWAR (SIMD Within A Register) bit tricks
///    to detect control chars (< 0x20), `"` (0x22), and `\` (0x5C) in parallel.
/// 2. Falls back to a lookup table for the trailing bytes.
///
/// On typical log data (timestamps, paths, hex IDs) this scans ~8× faster than
/// byte-by-byte iteration.
#[inline]
fn first_escape_pos(bytes: &[u8]) -> usize {
    let len = bytes.len();
    let mut i = 0;

    // SWAR: process 8 bytes per iteration on 64-bit platforms.
    // Detects bytes < 0x20 OR == 0x22 (") OR == 0x5C (\) in parallel.
    while i + 8 <= len {
        let mut lane = [0u8; 8];
        lane.copy_from_slice(&bytes[i..i + 8]);
        let chunk = u64::from_le_bytes(lane);

        // Control characters are exactly the bytes whose top three bits are zero.
        let ctrl = swar_zero_byte_mask(chunk & SWAR_TOP3_MASK);
        let has_quote = swar_zero_byte_mask(chunk ^ SWAR_ONES.wrapping_mul(u64::from(b'"')));
        let has_bslash = swar_zero_byte_mask(chunk ^ SWAR_ONES.wrapping_mul(u64::from(b'\\')));

        let mask = ctrl | has_quote | has_bslash;
        if mask != 0 {
            // Found something — find the first byte position.
            // On little-endian, trailing zeros / 8 gives the byte index.
            let byte_offset = mask.trailing_zeros() as usize / 8;
            return i + byte_offset;
        }
        i += 8;
    }

    // Tail: process remaining bytes with lookup table.
    while i < len {
        if NEEDS_ESCAPE[bytes[i] as usize] {
            return i;
        }
        i += 1;
    }
    len
}

/// Write a JSON string value with RFC 8259 escaping.
///
/// **Hot path optimization**: most log field values (timestamps, service names,
/// HTTP paths, hex IDs, etc.) contain zero bytes requiring JSON escaping.
///
/// Strategy:
/// 1. Use SWAR-accelerated [`first_escape_pos`] to find the first special byte.
/// 2. If no escaping needed (common case), write `"` + value + `"` with a single
///    reserve + 2 memcpy operations.
/// 3. Only when an escape byte is found does the slow per-byte path run.
#[inline]
#[allow(clippy::unnecessary_wraps)] // Signature kept for API consistency with callers using `?`
pub(crate) fn write_json_string(out: &mut Vec<u8>, v: &str) -> io::Result<()> {
    let bytes = v.as_bytes();
    let first_esc = first_escape_pos(bytes);

    if first_esc == bytes.len() {
        // Fast path: no escaping needed (vast majority of log field values).
        // Single reserve + two extend_from_slice calls.
        out.reserve(bytes.len() + 2);
        out.push(b'"');
        out.extend_from_slice(bytes);
        out.push(b'"');
        return Ok(());
    }

    // Slow path: has at least one byte needing escaping.
    // Reserve a reasonable estimate (original length + some overhead for escapes).
    out.reserve(bytes.len() + 8);
    out.push(b'"');

    // Write the safe prefix before the first escape.
    if first_esc > 0 {
        out.extend_from_slice(&bytes[..first_esc]);
    }

    // Process from the first escape byte onward.
    let mut start = first_esc;
    loop {
        // Emit the escape sequence for the current byte.
        match bytes[start] {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            b => {
                // Control char → \u00XX. Avoid format_args! allocation.
                const HEX: &[u8; 16] = b"0123456789abcdef";
                out.extend_from_slice(&[
                    b'\\',
                    b'u',
                    b'0',
                    b'0',
                    HEX[(b >> 4) as usize],
                    HEX[(b & 0x0f) as usize],
                ]);
            }
        }
        start += 1;

        if start >= bytes.len() {
            break;
        }

        // Find the next escape byte in the remainder.
        let remaining = &bytes[start..];
        let next_esc = first_escape_pos(remaining);
        if next_esc > 0 {
            out.extend_from_slice(&remaining[..next_esc]);
        }
        if next_esc == remaining.len() {
            break;
        }
        start += next_esc;
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
///
/// # Safety contract
///
/// Callers must ensure `row < arr.len()`. In debug builds an assertion fires;
/// in release builds out-of-bounds access is undefined behaviour.
pub(crate) fn write_json_value(arr: &dyn Array, row: usize, out: &mut Vec<u8>) -> io::Result<()> {
    debug_assert!(
        row < arr.len(),
        "write_json_value: row {row} out of bounds for arr.len() {}",
        arr.len()
    );
    if arr.is_null(row) {
        out.extend_from_slice(b"null");
        return Ok(());
    }

    match arr.data_type() {
        DataType::Null => {
            out.extend_from_slice(b"null");
        }
        DataType::Int8 => {
            // SAFETY: row < arr.len() is asserted at function entry (debug) / guaranteed by caller
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::Int8Type>()
                    .value_unchecked(row)
            };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::Int16 => {
            // SAFETY: row < arr.len() is asserted at function entry (debug) / guaranteed by caller
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::Int16Type>()
                    .value_unchecked(row)
            };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::Int32 => {
            // SAFETY: row < arr.len() is asserted at function entry (debug) / guaranteed by caller
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::Int32Type>()
                    .value_unchecked(row)
            };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::Int64 => {
            // SAFETY: row < arr.len() is asserted at function entry (debug) / guaranteed by caller
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::Int64Type>()
                    .value_unchecked(row)
            };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::UInt8 => {
            // SAFETY: row < arr.len() is asserted at function entry (debug) / guaranteed by caller
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::UInt8Type>()
                    .value_unchecked(row)
            };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::UInt16 => {
            // SAFETY: row < arr.len() is asserted at function entry (debug) / guaranteed by caller
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::UInt16Type>()
                    .value_unchecked(row)
            };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::UInt32 => {
            // SAFETY: row < arr.len() is asserted at function entry (debug) / guaranteed by caller
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::UInt32Type>()
                    .value_unchecked(row)
            };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::UInt64 => {
            // SAFETY: row < arr.len() is asserted at function entry (debug) / guaranteed by caller
            let v = unsafe {
                arr.as_primitive::<arrow::datatypes::UInt64Type>()
                    .value_unchecked(row)
            };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        DataType::Float32 => {
            // SAFETY: row < arr.len() is asserted at function entry (debug) / guaranteed by caller
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
            // SAFETY: row < arr.len() is asserted at function entry (debug) / guaranteed by caller
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
            // SAFETY: row < arr.len() is asserted at function entry (debug) / guaranteed by caller
            let v = unsafe { arr.as_boolean().value_unchecked(row) };
            out.extend_from_slice(if v { b"true" } else { b"false" });
        }
        // String types: use write_json_string (lookup-table fast-path) instead of
        // the fallback `array_value_to_string` which heap-allocates per call.
        // StreamingBuilder uses Utf8View; these three arms cover all common cases.
        DataType::Utf8 => {
            let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DataType::Utf8 did not downcast to StringArray",
                ));
            };
            // SAFETY: row < arr.len() is asserted at function entry (debug) / guaranteed by caller
            write_json_string(out, unsafe { str_arr.value_unchecked(row) })?;
        }
        DataType::LargeUtf8 => {
            let Some(str_arr) = arr.as_any().downcast_ref::<LargeStringArray>() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DataType::LargeUtf8 did not downcast to LargeStringArray",
                ));
            };
            // SAFETY: row < arr.len() is asserted at function entry (debug) / guaranteed by caller
            write_json_string(out, unsafe { str_arr.value_unchecked(row) })?;
        }
        DataType::Utf8View => {
            let Some(str_arr) = arr.as_any().downcast_ref::<StringViewArray>() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DataType::Utf8View did not downcast to StringViewArray",
                ));
            };
            // SAFETY: row < arr.len() is asserted at function entry (debug) / guaranteed by caller
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
            // SAFETY: row < arr.len() is asserted at function entry (debug) / guaranteed by caller
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
            // SAFETY: row < arr.len() is asserted at function entry (debug) / guaranteed by caller
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
            // SAFETY: row < arr.len() is asserted at function entry (debug) / guaranteed by caller
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
        // SAFETY for all typed arms below: row < a.len() is guaranteed by the caller
        TypedArrayRef::Int8(a) => {
            // SAFETY: row < a.len() is guaranteed by the caller (see block comment above)
            let v = unsafe { a.value_unchecked(row) };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        TypedArrayRef::Int16(a) => {
            // SAFETY: row < a.len() is guaranteed by the caller (see block comment above)
            let v = unsafe { a.value_unchecked(row) };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        TypedArrayRef::Int32(a) => {
            // SAFETY: row < a.len() is guaranteed by the caller (see block comment above)
            let v = unsafe { a.value_unchecked(row) };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        TypedArrayRef::Int64(a) => {
            // SAFETY: row < a.len() is guaranteed by the caller (see block comment above)
            let v = unsafe { a.value_unchecked(row) };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        TypedArrayRef::UInt8(a) => {
            // SAFETY: row < a.len() is guaranteed by the caller (see block comment above)
            let v = unsafe { a.value_unchecked(row) };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        TypedArrayRef::UInt16(a) => {
            // SAFETY: row < a.len() is guaranteed by the caller (see block comment above)
            let v = unsafe { a.value_unchecked(row) };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        TypedArrayRef::UInt32(a) => {
            // SAFETY: row < a.len() is guaranteed by the caller (see block comment above)
            let v = unsafe { a.value_unchecked(row) };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        TypedArrayRef::UInt64(a) => {
            // SAFETY: row < a.len() is guaranteed by the caller (see block comment above)
            let v = unsafe { a.value_unchecked(row) };
            out.extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
        }
        TypedArrayRef::Float32(a) => {
            // SAFETY: row < a.len() is guaranteed by the caller (see block comment above)
            let v = unsafe { a.value_unchecked(row) };
            if v.is_finite() {
                out.extend_from_slice(ryu::Buffer::new().format_finite(v).as_bytes());
            } else {
                out.extend_from_slice(b"null");
            }
        }
        TypedArrayRef::Float64(a) => {
            // SAFETY: row < a.len() is guaranteed by the caller (see block comment above)
            let v = unsafe { a.value_unchecked(row) };
            if v.is_finite() {
                out.extend_from_slice(ryu::Buffer::new().format_finite(v).as_bytes());
            } else {
                out.extend_from_slice(b"null");
            }
        }
        TypedArrayRef::Boolean(a) => {
            // SAFETY: row < a.len() is guaranteed by the caller (see block comment above)
            let v = unsafe { a.value_unchecked(row) };
            out.extend_from_slice(if v { b"true" } else { b"false" });
        }
        TypedArrayRef::Utf8(a) => {
            // SAFETY: row < a.len() is guaranteed by the caller (see block comment above)
            write_json_string(out, unsafe { a.value_unchecked(row) })?;
        }
        TypedArrayRef::LargeUtf8(a) => {
            // SAFETY: row < a.len() is guaranteed by the caller (see block comment above)
            write_json_string(out, unsafe { a.value_unchecked(row) })?;
        }
        TypedArrayRef::Utf8View(a) => {
            // SAFETY: row < a.len() is guaranteed by the caller (see block comment above)
            write_json_string(out, unsafe { a.value_unchecked(row) })?;
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
            // SAFETY: row < a.len() is guaranteed by the caller (see block comment above)
            write_json_hex_bytes(out, unsafe { a.value_unchecked(row) });
        }
        TypedArrayRef::LargeBinary(a) => {
            // SAFETY: row < a.len() is guaranteed by the caller (see block comment above)
            write_json_hex_bytes(out, unsafe { a.value_unchecked(row) });
        }
        TypedArrayRef::FixedSizeBinary(a) => {
            // SAFETY: row < a.len() is guaranteed by the caller (see block comment above)
            write_json_hex_bytes(out, unsafe { a.value_unchecked(row) });
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

/// Write an entire batch as newline-delimited JSON using pre-resolved columns.
///
/// This is the highest-performance batch serialization path. Compared to calling
/// [`write_row_json_resolved`] in a loop manually, this function:
/// - Pre-reserves the output buffer based on batch size estimation
/// - Provides a convenience batch-writing loop over the already-resolved columns
///
/// Returns the number of rows written.
pub fn write_batch_json_resolved(
    cols: &[ResolvedCol],
    num_rows: usize,
    out: &mut Vec<u8>,
) -> io::Result<usize> {
    const MAX_PREALLOC_BYTES: usize = 64 * 1024 * 1024;

    if num_rows == 0 {
        return Ok(0);
    }

    if let Some(row_limit) = cols.iter().map(ResolvedCol::row_limit).min()
        && num_rows > row_limit
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("requested {num_rows} rows but resolved columns only support {row_limit} rows"),
        ));
    }

    // Estimate ~200 bytes per row for narrow, ~600 for wide schemas.
    // Use column count as a heuristic: 5 cols ~ 200B, 20 cols ~ 600B.
    let est_bytes_per_row = 40usize
        .checked_add(cols.len().checked_mul(30).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "column count is too large to estimate JSON row size",
            )
        })?)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "estimated JSON row size overflowed",
            )
        })?;
    let reserve_bytes = num_rows
        .checked_mul(est_bytes_per_row)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "batch is too large to estimate JSON output size",
            )
        })?
        .min(MAX_PREALLOC_BYTES);
    out.try_reserve(reserve_bytes)
        .map_err(|e| io::Error::other(format!("failed to reserve JSON output buffer: {e}")))?;

    for row in 0..num_rows {
        write_row_json_resolved(row, cols, out, true)?;
    }
    Ok(num_rows)
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
    debug_assert!(
        row < batch.num_rows(),
        "write_row_json: row {row} out of bounds for batch.num_rows() {}",
        batch.num_rows()
    );
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
            "/../ffwd-test-utils/src/arrow.rs"
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

    fn first_escape_pos_reference(bytes: &[u8]) -> usize {
        bytes
            .iter()
            .position(|&byte| NEEDS_ESCAPE[byte as usize])
            .unwrap_or(bytes.len())
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

        #[test]
        fn first_escape_pos_matches_reference(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            prop_assert_eq!(first_escape_pos(&bytes), first_escape_pos_reference(&bytes));
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

    #[test]
    fn first_escape_pos_matches_reference_edge_cases() {
        let cases: &[&[u8]] = &[
            b"",
            b"plain-ascii",
            b"contains\"quote",
            b"contains\\backslash",
            b"line\nbreak",
            b"\x1f\x20\x20\x20\x20\x20\x20\x20",
            b"\x20\x20\x20\x20\x20\x20\x20\x1f",
            b"\x00abcdefg",
            b"abcdefg\x00",
            b"\x80\x81\x82\x83\x84\x85\x86\x87",
            b"\x1f\x20\x22\x5caaaa",
            b"aaaaaaa\t",
        ];

        for &bytes in cases {
            assert_eq!(first_escape_pos(bytes), first_escape_pos_reference(bytes));
        }
    }

    #[test]
    fn write_batch_json_resolved_rejects_oversized_num_rows() {
        let batch = scalar_batch(vec![("ok".to_string(), 1, 1.5, true)]);
        let cols = build_col_infos(&batch);
        let resolved = resolve_col_infos(&batch, &cols);

        let err = write_batch_json_resolved(&resolved, batch.num_rows() + 1, &mut Vec::new())
            .expect_err("oversized row counts must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}

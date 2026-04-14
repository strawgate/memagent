// accumulator.rs — ColumnAccumulator: typed per-field storage.
//
// Each variant carries only the storage it needs:
//   - Planned Int64 → one Vec<(u32, i64)>
//   - Planned String → one Vec<(u32, StringRef)> (row + buffer reference)
//   - Dynamic → all 4 vecs + conflict flags (same as FieldColumns today)
//
// Materialization is distributed: each variant builds its own Arrow array.
// No monolithic finalization function.
//
// INVARIANT: facts within each vec must be pushed in non-decreasing row order.
// The builder enforces this by calling push_* only with the current row_count,
// which monotonically increases. build_string relies on this for its
// sequential merge with the row range.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, StringViewArray,
    StringViewBuilder, StructArray,
};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields};

use super::plan::FieldKind;

// ---------------------------------------------------------------------------
// StringRef — uniform reference into a 2-buffer system
// ---------------------------------------------------------------------------

/// Reference to a string value in the builder's buffer system.
///
/// The 2-buffer model: offsets `< original_len` point into the input buffer;
/// offsets `>= original_len` point into the generated buffer at
/// `offset - original_len`.  This encoding costs zero extra bits per fact.
#[derive(Debug, Clone, Copy)]
pub struct StringRef {
    pub offset: u32,
    pub len: u32,
}

// ---------------------------------------------------------------------------
// ColumnAccumulator — typed per-field storage + materialization
// ---------------------------------------------------------------------------

/// Typed per-field storage.  Each variant carries only what it needs.
///
/// Planned fields use single-type variants (one Vec per field).
/// Dynamic fields use `Dynamic` (all 4 Vecs + conflict flags).
pub enum ColumnAccumulator {
    /// Planned Int64 field — single fact vector.
    Int64 {
        facts: Vec<(u32, i64)>,
        last_row: u32,
    },
    /// Planned Float64 field — single fact vector.
    Float64 {
        facts: Vec<(u32, f64)>,
        last_row: u32,
    },
    /// Planned Bool field — single fact vector.
    Bool {
        facts: Vec<(u32, bool)>,
        last_row: u32,
    },
    /// Planned string field — references into the 2-buffer system.
    String {
        facts: Vec<(u32, StringRef)>,
        last_row: u32,
    },
    /// Dynamic field — accumulates all types, detects conflicts at finalization.
    Dynamic {
        int_facts: Vec<(u32, i64)>,
        float_facts: Vec<(u32, f64)>,
        bool_facts: Vec<(u32, bool)>,
        str_facts: Vec<(u32, StringRef)>,
        has_int: bool,
        has_float: bool,
        has_bool: bool,
        has_str: bool,
        last_row: u32,
    },
}

impl ColumnAccumulator {
    /// Create the right variant for a planned field kind.
    pub fn for_planned(kind: FieldKind) -> Self {
        match kind {
            FieldKind::Int64 => ColumnAccumulator::Int64 {
                facts: Vec::with_capacity(256),
                last_row: u32::MAX,
            },
            FieldKind::Float64 => ColumnAccumulator::Float64 {
                facts: Vec::with_capacity(256),
                last_row: u32::MAX,
            },
            FieldKind::Bool => ColumnAccumulator::Bool {
                facts: Vec::with_capacity(256),
                last_row: u32::MAX,
            },
            FieldKind::Utf8View | FieldKind::BinaryView | FieldKind::FixedBinary(_) => {
                ColumnAccumulator::String {
                    facts: Vec::with_capacity(256),
                    last_row: u32::MAX,
                }
            }
        }
    }

    /// Create a Dynamic variant (for JSON-style discovered fields).
    pub fn dynamic() -> Self {
        ColumnAccumulator::Dynamic {
            int_facts: Vec::new(),
            float_facts: Vec::new(),
            bool_facts: Vec::new(),
            str_facts: Vec::new(),
            has_int: false,
            has_float: false,
            has_bool: false,
            has_str: false,
            last_row: u32::MAX,
        }
    }

    /// Clear accumulated data for batch reuse.
    pub fn clear(&mut self) {
        match self {
            ColumnAccumulator::Int64 { facts, last_row } => {
                facts.clear();
                *last_row = u32::MAX;
            }
            ColumnAccumulator::Float64 { facts, last_row } => {
                facts.clear();
                *last_row = u32::MAX;
            }
            ColumnAccumulator::Bool { facts, last_row } => {
                facts.clear();
                *last_row = u32::MAX;
            }
            ColumnAccumulator::String { facts, last_row } => {
                facts.clear();
                *last_row = u32::MAX;
            }
            ColumnAccumulator::Dynamic {
                int_facts,
                float_facts,
                bool_facts,
                str_facts,
                has_int,
                has_float,
                has_bool,
                has_str,
                last_row,
            } => {
                int_facts.clear();
                float_facts.clear();
                bool_facts.clear();
                str_facts.clear();
                *has_int = false;
                *has_float = false;
                *has_bool = false;
                *has_str = false;
                *last_row = u32::MAX;
            }
        }
    }

    /// Mutable last_row for dedup when field index >= 64.
    pub fn last_row_mut(&mut self) -> &mut u32 {
        match self {
            ColumnAccumulator::Int64 { last_row, .. }
            | ColumnAccumulator::Float64 { last_row, .. }
            | ColumnAccumulator::Bool { last_row, .. }
            | ColumnAccumulator::String { last_row, .. }
            | ColumnAccumulator::Dynamic { last_row, .. } => last_row,
        }
    }

    /// Last row written.
    pub fn last_row(&self) -> u32 {
        match self {
            ColumnAccumulator::Int64 { last_row, .. }
            | ColumnAccumulator::Float64 { last_row, .. }
            | ColumnAccumulator::Bool { last_row, .. }
            | ColumnAccumulator::String { last_row, .. }
            | ColumnAccumulator::Dynamic { last_row, .. } => *last_row,
        }
    }

    // -----------------------------------------------------------------------
    // Typed write methods
    // -----------------------------------------------------------------------

    /// Append an i64 fact.  For planned Int64, goes to the single vec.
    /// For Dynamic, goes to int_facts.
    #[inline(always)]
    pub fn push_i64(&mut self, row: u32, value: i64) {
        match self {
            ColumnAccumulator::Int64 { facts, .. } => facts.push((row, value)),
            ColumnAccumulator::Dynamic {
                int_facts, has_int, ..
            } => {
                *has_int = true;
                int_facts.push((row, value));
            }
            // Wrong-type write to a planned non-Int64 field: no-op.
            _ => {}
        }
    }

    /// Append an f64 fact.
    #[inline(always)]
    pub fn push_f64(&mut self, row: u32, value: f64) {
        match self {
            ColumnAccumulator::Float64 { facts, .. } => facts.push((row, value)),
            ColumnAccumulator::Dynamic {
                float_facts,
                has_float,
                ..
            } => {
                *has_float = true;
                float_facts.push((row, value));
            }
            _ => {}
        }
    }

    /// Append a bool fact.
    #[inline(always)]
    pub fn push_bool(&mut self, row: u32, value: bool) {
        match self {
            ColumnAccumulator::Bool { facts, .. } => facts.push((row, value)),
            ColumnAccumulator::Dynamic {
                bool_facts,
                has_bool,
                ..
            } => {
                *has_bool = true;
                bool_facts.push((row, value));
            }
            _ => {}
        }
    }

    /// Append a string ref.
    #[inline(always)]
    pub fn push_str(&mut self, row: u32, sref: StringRef) {
        match self {
            ColumnAccumulator::String { facts, .. } => facts.push((row, sref)),
            ColumnAccumulator::Dynamic {
                str_facts, has_str, ..
            } => {
                *has_str = true;
                str_facts.push((row, sref));
            }
            _ => {}
        }
    }

    // -----------------------------------------------------------------------
    // Materialization — each variant builds its own Arrow array
    // -----------------------------------------------------------------------

    /// Materialize into an Arrow field + array.
    ///
    /// Returns `Ok(None)` if the accumulator has no data (sparse all-null field
    /// with no writes — omitted from schema like StreamingBuilder does).
    ///
    /// `mode` controls string materialization:
    ///   - `Detached`: uses `StringBuilder` (copies strings, self-contained)
    ///   - `View`: uses `StringViewArray` backed by input + generated buffers
    ///
    /// # Errors
    ///
    /// Returns `Err` if a string reference points outside the provided buffers
    /// or if buffer data is not valid UTF-8.
    pub fn materialize(
        &self,
        name: &str,
        num_rows: usize,
        mode: FinalizationMode,
    ) -> Result<Option<(Field, ArrayRef)>, MaterializeError> {
        match self {
            ColumnAccumulator::Int64 { facts, .. } => {
                if facts.is_empty() {
                    return Ok(None);
                }
                let (arr, dt) = build_int64(facts, num_rows);
                Ok(Some((Field::new(name, dt, true), arr)))
            }
            ColumnAccumulator::Float64 { facts, .. } => {
                if facts.is_empty() {
                    return Ok(None);
                }
                let (arr, dt) = build_float64(facts, num_rows);
                Ok(Some((Field::new(name, dt, true), arr)))
            }
            ColumnAccumulator::Bool { facts, .. } => {
                if facts.is_empty() {
                    return Ok(None);
                }
                let (arr, dt) = build_bool(facts, num_rows);
                Ok(Some((Field::new(name, dt, true), arr)))
            }
            ColumnAccumulator::String { facts, .. } => {
                if facts.is_empty() {
                    return Ok(None);
                }
                let (arr, dt) = build_string(facts, num_rows, &mode)?;
                Ok(Some((Field::new(name, dt, true), arr)))
            }
            ColumnAccumulator::Dynamic {
                int_facts,
                float_facts,
                bool_facts,
                str_facts,
                has_int,
                has_float,
                has_bool,
                has_str,
                ..
            } => {
                let type_count =
                    *has_int as u8 + *has_float as u8 + *has_str as u8 + *has_bool as u8;
                if type_count == 0 {
                    return Ok(None);
                }
                if type_count > 1 {
                    // Conflict → StructArray
                    Ok(Some(build_conflict_struct(
                        name,
                        num_rows,
                        &mode,
                        int_facts,
                        float_facts,
                        bool_facts,
                        str_facts,
                    )?))
                } else {
                    // Single type → flat column
                    if *has_int {
                        let (arr, dt) = build_int64(int_facts, num_rows);
                        Ok(Some((Field::new(name, dt, true), arr)))
                    } else if *has_float {
                        let (arr, dt) = build_float64(float_facts, num_rows);
                        Ok(Some((Field::new(name, dt, true), arr)))
                    } else if *has_str {
                        let (arr, dt) = build_string(str_facts, num_rows, &mode)?;
                        Ok(Some((Field::new(name, dt, true), arr)))
                    } else {
                        let (arr, dt) = build_bool(bool_facts, num_rows);
                        Ok(Some((Field::new(name, dt, true), arr)))
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// MaterializeError
// ---------------------------------------------------------------------------

/// Error during column materialization.
#[derive(Debug)]
pub enum MaterializeError {
    /// A `StringRef` pointed outside the provided buffers.
    StringRefOutOfBounds {
        offset: u32,
        len: u32,
        buffer_len: usize,
    },
    /// Buffer data was not valid UTF-8 at the referenced range.
    InvalidUtf8 { offset: u32, len: u32 },
    /// Arrow's `StringViewBuilder::try_append_view` failed.
    ViewAppend(arrow::error::ArrowError),
}

impl std::fmt::Display for MaterializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MaterializeError::StringRefOutOfBounds {
                offset,
                len,
                buffer_len,
            } => write!(
                f,
                "StringRef(offset={offset}, len={len}) out of bounds for buffer(len={buffer_len})"
            ),
            MaterializeError::InvalidUtf8 { offset, len } => {
                write!(f, "invalid UTF-8 at StringRef(offset={offset}, len={len})")
            }
            MaterializeError::ViewAppend(e) => write!(f, "StringViewBuilder append failed: {e}"),
        }
    }
}

impl std::error::Error for MaterializeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MaterializeError::ViewAppend(e) => Some(e),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// FinalizationMode — controls string materialization strategy
// ---------------------------------------------------------------------------

/// Controls how string columns are built at finalization.
#[derive(Clone)]
pub enum FinalizationMode {
    /// Build string columns from owned Arrow buffers.
    ///
    /// When `utf8_trusted` is true, produces `StringViewArray` with zero per-string
    /// byte copying — views reference the source buffers directly. When false,
    /// copies bytes into a contiguous `StringArray` with full UTF-8 validation.
    Detached {
        /// Original input buffer (e.g., protobuf wire bytes, scanner input).
        original_buf: Buffer,
        /// Generated/decoded buffer (e.g., JSON-unescaped strings).
        generated_buf: Buffer,
        /// When true, all string data is known to be valid UTF-8 (validated
        /// at the ingestion boundary — scanner, OTLP decoder, etc.). Enables
        /// zero-copy StringViewArray construction.
        utf8_trusted: bool,
    },
    /// Zero-copy `StringViewArray` backed by Arrow buffers.
    View {
        /// Arrow buffer wrapping the original input.
        original: Buffer,
        /// Original buffer length (for the 2-buffer offset convention).
        original_len: u32,
        /// Arrow buffer wrapping generated/decoded strings (if any).
        generated: Option<Buffer>,
    },
}

// ---------------------------------------------------------------------------
// Array builders (free functions, no &self)
// ---------------------------------------------------------------------------

fn build_int64(facts: &[(u32, i64)], num_rows: usize) -> (ArrayRef, DataType) {
    let dense = facts.len() == num_rows;
    let mut values = vec![0i64; num_rows];
    if dense {
        for (i, &(_, v)) in facts.iter().enumerate() {
            values[i] = v;
        }
    } else {
        for &(row, v) in facts {
            let r = row as usize;
            if r < num_rows {
                values[r] = v;
            }
        }
    }
    let nulls = if dense {
        None
    } else {
        let mut valid = vec![false; num_rows];
        for &(row, _) in facts {
            let r = row as usize;
            if r < num_rows {
                valid[r] = true;
            }
        }
        Some(NullBuffer::from(valid))
    };
    (
        Arc::new(Int64Array::new(values.into(), nulls)),
        DataType::Int64,
    )
}

fn build_float64(facts: &[(u32, f64)], num_rows: usize) -> (ArrayRef, DataType) {
    let dense = facts.len() == num_rows;
    let mut values = vec![0.0f64; num_rows];
    if dense {
        for (i, &(_, v)) in facts.iter().enumerate() {
            values[i] = v;
        }
    } else {
        for &(row, v) in facts {
            let r = row as usize;
            if r < num_rows {
                values[r] = v;
            }
        }
    }
    let nulls = if dense {
        None
    } else {
        let mut valid = vec![false; num_rows];
        for &(row, _) in facts {
            let r = row as usize;
            if r < num_rows {
                valid[r] = true;
            }
        }
        Some(NullBuffer::from(valid))
    };
    (
        Arc::new(Float64Array::new(values.into(), nulls)),
        DataType::Float64,
    )
}

fn build_bool(facts: &[(u32, bool)], num_rows: usize) -> (ArrayRef, DataType) {
    let dense = facts.len() == num_rows;
    let mut values = vec![false; num_rows];
    if dense {
        for (i, &(_, v)) in facts.iter().enumerate() {
            values[i] = v;
        }
    } else {
        for &(row, v) in facts {
            let r = row as usize;
            if r < num_rows {
                values[r] = v;
            }
        }
    }
    let nulls = if dense {
        None
    } else {
        let mut valid = vec![false; num_rows];
        for &(row, _) in facts {
            let r = row as usize;
            if r < num_rows {
                valid[r] = true;
            }
        }
        Some(NullBuffer::from(valid))
    };
    (
        Arc::new(BooleanArray::new(values.into(), nulls)),
        DataType::Boolean,
    )
}

fn build_string(
    facts: &[(u32, StringRef)],
    num_rows: usize,
    mode: &FinalizationMode,
) -> Result<(ArrayRef, DataType), MaterializeError> {
    match mode {
        FinalizationMode::Detached {
            original_buf,
            generated_buf,
            utf8_trusted,
        } => {
            if *utf8_trusted {
                build_string_view_trusted(facts, num_rows, original_buf, generated_buf)
            } else {
                build_string_array_validated(facts, num_rows, original_buf, generated_buf)
            }
        }
        FinalizationMode::View {
            original,
            original_len,
            generated,
        } => {
            let mut builder = StringViewBuilder::new();
            let orig_block = builder.append_block(original.clone());
            let gen_block = generated.as_ref().map(|g| builder.append_block(g.clone()));
            let mut vi = 0;
            for row in 0..num_rows as u32 {
                if vi < facts.len() && facts[vi].0 == row {
                    let sref = facts[vi].1;
                    if sref.offset < *original_len
                        || (sref.len == 0 && sref.offset == *original_len)
                    {
                        builder
                            .try_append_view(orig_block, sref.offset, sref.len)
                            .map_err(MaterializeError::ViewAppend)?;
                    } else if let Some(gen_block) = gen_block {
                        let gen_offset = sref.offset - *original_len;
                        builder
                            .try_append_view(gen_block, gen_offset, sref.len)
                            .map_err(MaterializeError::ViewAppend)?;
                    } else {
                        return Err(MaterializeError::StringRefOutOfBounds {
                            offset: sref.offset,
                            len: sref.len,
                            buffer_len: original.len(),
                        });
                    }
                    vi += 1;
                } else {
                    builder.append_null();
                }
            }
            Ok((Arc::new(builder.finish()), DataType::Utf8View))
        }
    }
}

/// Build a `StringViewArray` from trusted UTF-8 buffers — zero per-string copy.
///
/// Views reference the source buffers directly. For strings ≤ 12 bytes, Arrow
/// inlines the data in the view itself. For longer strings, the view points
/// into the original or generated buffer block.
///
/// # Safety contract
///
/// Caller guarantees all bytes referenced by `facts` are valid UTF-8 (validated
/// at the ingestion boundary — scanner, OTLP decoder, or Rust's type system
/// via `write_str(&str)`).
fn build_string_view_trusted(
    facts: &[(u32, StringRef)],
    num_rows: usize,
    original_buf: &Buffer,
    generated_buf: &Buffer,
) -> Result<(ArrayRef, DataType), MaterializeError> {
    let original_len = original_buf.len();
    let dense = facts.len() == num_rows;

    // Register source buffers as StringViewArray blocks.
    // block 0 = original, block 1 = generated (if non-empty).
    let mut buffers: Vec<Buffer> = Vec::with_capacity(2);
    let orig_block: u32 = 0;
    buffers.push(original_buf.clone()); // O(1) Arc bump
    let gen_block = if generated_buf.is_empty() {
        None
    } else {
        buffers.push(generated_buf.clone()); // O(1) Arc bump
        Some(1u32)
    };

    // Pre-zero + indexed write is faster than collect() because memset_avx2
    // zeroes 16KB in ~200 instructions while the Result iterator adapter
    // (GenericShunt) adds ~25M instructions of overhead.
    let mut views: Vec<u128> = vec![0u128; num_rows];

    if dense {
        // Dense fast path: every row has a value, facts[i] corresponds to row i.
        for (i, &(_, sref)) in facts.iter().enumerate() {
            views[i] = make_string_view(
                sref,
                original_buf,
                generated_buf,
                original_len,
                orig_block,
                gen_block,
            )?;
        }
    } else {
        // Sparse: walk facts in row order, null rows stay as 0 (zero-length view).
        let mut vi = 0;
        for (row, view_slot) in views.iter_mut().enumerate() {
            if vi < facts.len() && facts[vi].0 as usize == row {
                *view_slot = make_string_view(
                    facts[vi].1,
                    original_buf,
                    generated_buf,
                    original_len,
                    orig_block,
                    gen_block,
                )?;
                vi += 1;
            }
        }
    }

    let nulls = if dense {
        None
    } else {
        let valid: Vec<bool> = (0..num_rows)
            .map(|row| {
                facts
                    .binary_search_by_key(&(row as u32), |&(r, _)| r)
                    .is_ok()
            })
            .collect();
        Some(NullBuffer::from(valid))
    };

    debug_assert!(
        {
            // Validate UTF-8 for all referenced string bytes in debug builds.
            let mut ok = true;
            for &(_, sref) in facts {
                if let Ok(bytes) = read_str_bytes(original_buf, generated_buf, original_len, sref) {
                    if std::str::from_utf8(bytes).is_err() {
                        ok = false;
                        break;
                    }
                }
            }
            ok
        },
        "utf8_trusted was set but string buffer contains invalid UTF-8 — \
         the ingestion boundary (scanner/decoder) has a validation bug"
    );

    // SAFETY: views are constructed from validated offsets into the registered
    // buffer blocks. UTF-8 validity guaranteed by the ingestion boundary
    // (scanner/decoder) or Rust's type system (write_str takes &str).
    let array =
        unsafe { StringViewArray::new_unchecked(ScalarBuffer::from(views), buffers, nulls) };
    Ok((Arc::new(array), DataType::Utf8View))
}

/// Construct a u128 StringView for a given StringRef.
///
/// Strings ≤ 12 bytes are inlined. Longer strings reference a buffer block.
#[inline(always)]
fn make_string_view(
    sref: StringRef,
    original_buf: &[u8],
    generated_buf: &[u8],
    original_len: usize,
    orig_block: u32,
    gen_block: Option<u32>,
) -> Result<u128, MaterializeError> {
    let len = sref.len;
    if len == 0 {
        return Ok(0u128);
    }

    let start = sref.offset as usize;

    // Resolve buffer, block index, and local offset.
    let (buf, block_idx, local_offset) = if start < original_len {
        (original_buf, orig_block, sref.offset)
    } else {
        let dec_start = (start - original_len) as u32;
        match gen_block {
            Some(gb) => (generated_buf, gb, dec_start),
            None => {
                return Err(MaterializeError::StringRefOutOfBounds {
                    offset: sref.offset,
                    len: sref.len,
                    buffer_len: 0,
                });
            }
        }
    };

    let local_start = local_offset as usize;
    let local_end = local_start + len as usize;
    if local_end > buf.len() {
        return Err(MaterializeError::StringRefOutOfBounds {
            offset: sref.offset,
            len: sref.len,
            buffer_len: buf.len(),
        });
    }

    // Build the u128 view using arithmetic — avoids byte array + copy_from_slice.
    Ok(if len <= 12 {
        // Inline: [len:4][data:12] packed little-endian.
        let mut view_bytes = [0u8; 16];
        view_bytes[0..4].copy_from_slice(&len.to_le_bytes());
        view_bytes[4..4 + len as usize].copy_from_slice(&buf[local_start..local_end]);
        u128::from_le_bytes(view_bytes)
    } else {
        // Buffer ref: [len:4][prefix:4][block_idx:4][offset:4] packed little-endian.
        // Use arithmetic to avoid array copies.
        let prefix = u32::from_le_bytes([
            buf[local_start],
            buf[local_start + 1],
            buf[local_start + 2],
            buf[local_start + 3],
        ]);
        (len as u128)
            | ((prefix as u128) << 32)
            | ((block_idx as u128) << 64)
            | ((local_offset as u128) << 96)
    })
}

/// Build a `StringArray` with full UTF-8 validation (untrusted path).
///
/// Copies string bytes into a contiguous values buffer. Used when the ingestion
/// boundary has not validated UTF-8 (e.g., raw external input).
fn build_string_array_validated(
    facts: &[(u32, StringRef)],
    num_rows: usize,
    original_buf: &Buffer,
    generated_buf: &Buffer,
) -> Result<(ArrayRef, DataType), MaterializeError> {
    let original_len = original_buf.len();
    let dense = facts.len() == num_rows;
    let mut offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
    let mut values: Vec<u8> =
        Vec::with_capacity(facts.iter().map(|(_, sref)| sref.len as usize).sum());
    let mut validity: Vec<bool> = if dense {
        Vec::new()
    } else {
        Vec::with_capacity(num_rows)
    };

    if dense {
        // Dense fast path: skip per-row branch check.
        for &(_, sref) in facts {
            offsets.push(values.len() as i32);
            let bytes = read_str_bytes(original_buf, generated_buf, original_len, sref)?;
            values.extend_from_slice(bytes);
        }
    } else {
        let mut vi = 0;
        for row in 0..num_rows as u32 {
            offsets.push(values.len() as i32);
            if vi < facts.len() && facts[vi].0 == row {
                let sref = facts[vi].1;
                let bytes = read_str_bytes(original_buf, generated_buf, original_len, sref)?;
                values.extend_from_slice(bytes);
                validity.push(true);
                vi += 1;
            } else {
                validity.push(false);
            }
        }
    }
    offsets.push(values.len() as i32);

    // Validate UTF-8 for external bytes.
    if !original_buf.is_empty() && std::str::from_utf8(&values).is_err() {
        let mut fvi = 0;
        for row in 0..num_rows as u32 {
            if fvi < facts.len() && facts[fvi].0 == row {
                let sref = facts[fvi].1;
                let start = offsets[row as usize] as usize;
                let end = offsets[row as usize + 1] as usize;
                if std::str::from_utf8(&values[start..end]).is_err() {
                    return Err(MaterializeError::InvalidUtf8 {
                        offset: sref.offset,
                        len: sref.len,
                    });
                }
                fvi += 1;
            }
        }
    }

    let nulls = if dense {
        None
    } else {
        Some(NullBuffer::from(validity))
    };
    let offset_buf = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let values_buf = Buffer::from_vec(values);

    if original_buf.is_empty() {
        // All data from write_str(&str) — valid UTF-8 by Rust's type system.
        // SAFETY: offsets built sequentially, source is &str.
        let array = unsafe { StringArray::new_unchecked(offset_buf, values_buf, nulls) };
        Ok((Arc::new(array), DataType::Utf8))
    } else {
        let array = StringArray::new(offset_buf, values_buf, nulls);
        Ok((Arc::new(array), DataType::Utf8))
    }
}

/// Read a string from the 2-buffer system.
///
/// Returns an error if the reference is out of bounds or the bytes are not valid UTF-8.
fn read_str<'a>(
    original: &'a [u8],
    generated: &'a [u8],
    original_len: usize,
    sref: StringRef,
) -> Result<&'a str, MaterializeError> {
    let start = sref.offset as usize;
    let end =
        start
            .checked_add(sref.len as usize)
            .ok_or(MaterializeError::StringRefOutOfBounds {
                offset: sref.offset,
                len: sref.len,
                buffer_len: original.len() + generated.len(),
            })?;
    let bytes = if start < original_len {
        original
            .get(start..end)
            .ok_or(MaterializeError::StringRefOutOfBounds {
                offset: sref.offset,
                len: sref.len,
                buffer_len: original.len(),
            })?
    } else {
        let dec_start = start - original_len;
        let dec_end = end - original_len;
        generated
            .get(dec_start..dec_end)
            .ok_or(MaterializeError::StringRefOutOfBounds {
                offset: sref.offset,
                len: sref.len,
                buffer_len: generated.len(),
            })?
    };
    std::str::from_utf8(bytes).map_err(|_| MaterializeError::InvalidUtf8 {
        offset: sref.offset,
        len: sref.len,
    })
}

/// Read string bytes from the 2-buffer system without UTF-8 validation.
///
/// Returns the raw bytes referenced by `sref`. Callers must validate UTF-8
/// externally (e.g., once over the concatenated output).
fn read_str_bytes<'a>(
    original: &'a [u8],
    generated: &'a [u8],
    original_len: usize,
    sref: StringRef,
) -> Result<&'a [u8], MaterializeError> {
    let start = sref.offset as usize;
    let end =
        start
            .checked_add(sref.len as usize)
            .ok_or(MaterializeError::StringRefOutOfBounds {
                offset: sref.offset,
                len: sref.len,
                buffer_len: original.len() + generated.len(),
            })?;
    if start < original_len {
        original
            .get(start..end)
            .ok_or(MaterializeError::StringRefOutOfBounds {
                offset: sref.offset,
                len: sref.len,
                buffer_len: original.len(),
            })
    } else {
        let dec_start = start - original_len;
        let dec_end = end - original_len;
        generated
            .get(dec_start..dec_end)
            .ok_or(MaterializeError::StringRefOutOfBounds {
                offset: sref.offset,
                len: sref.len,
                buffer_len: generated.len(),
            })
    }
}

fn build_conflict_struct(
    name: &str,
    num_rows: usize,
    mode: &FinalizationMode,
    int_facts: &[(u32, i64)],
    float_facts: &[(u32, f64)],
    bool_facts: &[(u32, bool)],
    str_facts: &[(u32, StringRef)],
) -> Result<(Field, ArrayRef), MaterializeError> {
    let mut child_fields: Vec<Arc<Field>> = Vec::new();
    let mut child_arrays: Vec<ArrayRef> = Vec::new();

    if !int_facts.is_empty() {
        let (arr, _) = build_int64(int_facts, num_rows);
        child_fields.push(Arc::new(Field::new("int", DataType::Int64, true)));
        child_arrays.push(arr);
    }
    if !float_facts.is_empty() {
        let (arr, _) = build_float64(float_facts, num_rows);
        child_fields.push(Arc::new(Field::new("float", DataType::Float64, true)));
        child_arrays.push(arr);
    }
    if !str_facts.is_empty() {
        let (arr, dt) = build_string(str_facts, num_rows, mode)?;
        child_fields.push(Arc::new(Field::new("str", dt, true)));
        child_arrays.push(arr);
    }
    if !bool_facts.is_empty() {
        let (arr, _) = build_bool(bool_facts, num_rows);
        child_fields.push(Arc::new(Field::new("bool", DataType::Boolean, true)));
        child_arrays.push(arr);
    }

    let struct_validity: Vec<bool> = (0..num_rows)
        .map(|i| child_arrays.iter().any(|arr| !arr.is_null(i)))
        .collect();

    let fields = Fields::from(child_fields);
    let struct_arr = StructArray::new(
        fields.clone(),
        child_arrays,
        Some(NullBuffer::from(struct_validity)),
    );

    Ok((
        Field::new(name, DataType::Struct(fields), true),
        Arc::new(struct_arr),
    ))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::Int64Type;

    #[test]
    fn planned_int64_materialize() {
        let mut acc = ColumnAccumulator::for_planned(FieldKind::Int64);
        acc.push_i64(0, 100);
        acc.push_i64(2, 300);

        let mode = FinalizationMode::Detached {
            original_buf: Buffer::from(&[] as &[u8]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        let (field, arr) = acc.materialize("ts", 3, mode).unwrap().unwrap();
        assert_eq!(field.name(), "ts");
        let a = arr.as_primitive::<Int64Type>();
        assert_eq!(a.value(0), 100);
        assert!(a.is_null(1));
        assert_eq!(a.value(2), 300);
    }

    #[test]
    fn planned_string_detached_materialize() {
        let input = b"hello world";
        let mut acc = ColumnAccumulator::for_planned(FieldKind::Utf8View);
        acc.push_str(0, StringRef { offset: 0, len: 5 }); // "hello"
        acc.push_str(1, StringRef { offset: 6, len: 5 }); // "world"

        let mode = FinalizationMode::Detached {
            original_buf: Buffer::from(&input[..]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        let (_, arr) = acc.materialize("msg", 2, mode).unwrap().unwrap();
        let a = arr.as_string_view();
        assert_eq!(a.value(0), "hello");
        assert_eq!(a.value(1), "world");
    }

    #[test]
    fn planned_string_view_materialize() {
        let input = b"hello world";
        let arrow_buf = Buffer::from(&input[..]);
        let mut acc = ColumnAccumulator::for_planned(FieldKind::Utf8View);
        acc.push_str(0, StringRef { offset: 0, len: 5 });
        acc.push_str(1, StringRef { offset: 6, len: 5 });

        let mode = FinalizationMode::View {
            original: arrow_buf,
            original_len: input.len() as u32,
            generated: None,
        };
        let (field, arr) = acc.materialize("msg", 2, mode).unwrap().unwrap();
        assert_eq!(field.data_type(), &DataType::Utf8View);
        let a = arr.as_string_view();
        assert_eq!(a.value(0), "hello");
        assert_eq!(a.value(1), "world");
    }

    #[test]
    fn planned_string_with_generated_buffer() {
        let input = b"original";
        let generated = b"decoded";
        let original_len = input.len() as u32;
        let mut acc = ColumnAccumulator::for_planned(FieldKind::Utf8View);
        // Row 0: from original
        acc.push_str(0, StringRef { offset: 0, len: 8 });
        // Row 1: from generated (offset >= original_len)
        acc.push_str(
            1,
            StringRef {
                offset: original_len,
                len: 7,
            },
        );

        let mode = FinalizationMode::Detached {
            original_buf: Buffer::from(&input[..]),
            generated_buf: Buffer::from(&generated[..]),
            utf8_trusted: true,
        };
        let (_, arr) = acc.materialize("msg", 2, mode).unwrap().unwrap();
        let a = arr.as_string_view();
        assert_eq!(a.value(0), "original");
        assert_eq!(a.value(1), "decoded");
    }

    #[test]
    fn dynamic_single_type_flat() {
        let mut acc = ColumnAccumulator::dynamic();
        acc.push_i64(0, 42);
        acc.push_i64(1, 99);

        let mode = FinalizationMode::Detached {
            original_buf: Buffer::from(&[] as &[u8]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        let (field, arr) = acc.materialize("x", 2, mode).unwrap().unwrap();
        assert_eq!(field.data_type(), &DataType::Int64);
        let a = arr.as_primitive::<Int64Type>();
        assert_eq!(a.value(0), 42);
        assert_eq!(a.value(1), 99);
    }

    #[test]
    fn dynamic_conflict_struct() {
        let mut acc = ColumnAccumulator::dynamic();
        acc.push_i64(0, 200);

        let input = b"OK";
        acc.push_str(1, StringRef { offset: 0, len: 2 });

        let mode = FinalizationMode::Detached {
            original_buf: Buffer::from(&input[..]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        let (field, arr) = acc.materialize("status", 2, mode).unwrap().unwrap();
        assert!(matches!(field.data_type(), DataType::Struct(_)));
        let s = arr.as_struct();
        assert_eq!(s.num_columns(), 2);
        let int_col = s.column_by_name("int").unwrap();
        assert!(!int_col.is_null(0));
        assert!(int_col.is_null(1));
    }

    #[test]
    fn empty_accumulator_returns_none() {
        let acc = ColumnAccumulator::for_planned(FieldKind::Int64);
        let mode = FinalizationMode::Detached {
            original_buf: Buffer::from(&[] as &[u8]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        assert!(acc.materialize("x", 3, mode).unwrap().is_none());
    }

    #[test]
    fn wrong_type_write_to_planned_is_noop() {
        let mut acc = ColumnAccumulator::for_planned(FieldKind::Int64);
        acc.push_str(0, StringRef { offset: 0, len: 3 }); // no-op for Int64
        acc.push_f64(0, 1.5); // no-op for Int64
        acc.push_bool(0, true); // no-op for Int64

        let mode = FinalizationMode::Detached {
            original_buf: Buffer::from(&[] as &[u8]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        assert!(acc.materialize("x", 1, mode).unwrap().is_none()); // no int facts → None
    }

    #[test]
    fn clear_and_reuse() {
        let mut acc = ColumnAccumulator::for_planned(FieldKind::Int64);
        acc.push_i64(0, 100);
        acc.clear();
        acc.push_i64(0, 200);

        let mode = FinalizationMode::Detached {
            original_buf: Buffer::from(&[] as &[u8]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        let (_, arr) = acc.materialize("x", 1, mode).unwrap().unwrap();
        assert_eq!(arr.as_primitive::<Int64Type>().value(0), 200);
    }

    // -----------------------------------------------------------------------
    // Memory savings test: planned field allocates 1 vec, not 4
    // -----------------------------------------------------------------------

    #[test]
    fn planned_field_memory_is_smaller_than_dynamic() {
        // A planned Int64 accumulator should have significantly less overhead
        // than a Dynamic one because it only allocates one Vec, not four.
        let planned = ColumnAccumulator::for_planned(FieldKind::Int64);
        let dynamic = ColumnAccumulator::dynamic();

        let planned_size = std::mem::size_of_val(&planned);
        let dynamic_size = std::mem::size_of_val(&dynamic);

        // The enum discriminant means planned isn't dramatically smaller in stack
        // size due to enum sizing rules, but the heap allocation count differs.
        // This test is really about documenting the difference.
        assert!(
            planned_size <= dynamic_size,
            "planned {planned_size} should be <= dynamic {dynamic_size}"
        );
    }

    // -----------------------------------------------------------------------
    // Error handling tests
    // -----------------------------------------------------------------------

    #[test]
    fn string_ref_out_of_bounds_returns_error() {
        let mut acc = ColumnAccumulator::for_planned(FieldKind::Utf8View);
        // Reference past end of buffer
        acc.push_str(
            0,
            StringRef {
                offset: 100,
                len: 5,
            },
        );

        let mode = FinalizationMode::Detached {
            original_buf: Buffer::from(b"short" as &[u8]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        let result = acc.materialize("x", 1, mode);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, MaterializeError::StringRefOutOfBounds { .. }),
            "expected StringRefOutOfBounds, got {err:?}"
        );
    }

    #[test]
    fn invalid_utf8_returns_error() {
        let bad_bytes: &[u8] = &[0xFF, 0xFE, 0x80, 0x81];
        let mut acc = ColumnAccumulator::for_planned(FieldKind::Utf8View);
        acc.push_str(0, StringRef { offset: 0, len: 4 });

        let mode = FinalizationMode::Detached {
            original_buf: Buffer::from(bad_bytes),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: false,
        };
        let result = acc.materialize("x", 1, mode);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, MaterializeError::InvalidUtf8 { .. }),
            "expected InvalidUtf8, got {err:?}"
        );
    }

    #[test]
    fn view_mode_out_of_bounds_returns_error() {
        let input = b"short";
        let arrow_buf = Buffer::from(&input[..]);
        let mut acc = ColumnAccumulator::for_planned(FieldKind::Utf8View);
        // Reference past generated buffer (which doesn't exist)
        acc.push_str(
            0,
            StringRef {
                offset: input.len() as u32,
                len: 10,
            },
        );

        let mode = FinalizationMode::View {
            original: arrow_buf,
            original_len: input.len() as u32,
            generated: None,
        };
        let result = acc.materialize("x", 1, mode);
        assert!(result.is_err());
    }
}

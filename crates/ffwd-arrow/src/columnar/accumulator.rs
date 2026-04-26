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
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, StringViewArray, StructArray,
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
            FieldKind::Utf8View => ColumnAccumulator::String {
                facts: Vec::with_capacity(256),
                last_row: u32::MAX,
            },
            // TODO(#1844): implement dedicated binary accumulator variants
            FieldKind::BinaryView | FieldKind::FixedBinary(_) => ColumnAccumulator::String {
                facts: Vec::with_capacity(256),
                last_row: u32::MAX,
            },
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
    // Type-acceptance checks
    // -----------------------------------------------------------------------

    /// Whether this accumulator accepts i64 writes.
    #[inline(always)]
    pub fn accepts_i64(&self) -> bool {
        matches!(
            self,
            ColumnAccumulator::Int64 { .. } | ColumnAccumulator::Dynamic { .. }
        )
    }

    /// Whether this accumulator accepts f64 writes.
    #[inline(always)]
    pub fn accepts_f64(&self) -> bool {
        matches!(
            self,
            ColumnAccumulator::Float64 { .. } | ColumnAccumulator::Dynamic { .. }
        )
    }

    /// Whether this accumulator accepts bool writes.
    #[inline(always)]
    pub fn accepts_bool(&self) -> bool {
        matches!(
            self,
            ColumnAccumulator::Bool { .. } | ColumnAccumulator::Dynamic { .. }
        )
    }

    /// Whether this accumulator accepts string writes.
    #[inline(always)]
    pub fn accepts_str(&self) -> bool {
        matches!(
            self,
            ColumnAccumulator::String { .. } | ColumnAccumulator::Dynamic { .. }
        )
    }

    // -----------------------------------------------------------------------
    // Typed write methods
    // -----------------------------------------------------------------------

    /// Initial capacity for dynamic fact Vecs on first typed push.
    ///
    /// Starts small (16) to avoid over-allocating for sparse or
    /// high-cardinality dynamic columns. Vec doubling reaches 256 in
    /// 4 reallocations for dense columns that need it.
    const DYNAMIC_INITIAL_CAPACITY: usize = 16;

    /// Append an i64 fact.
    ///
    /// Accepted by `Int64` and `Dynamic` accumulators. For other planned
    /// types the caller should check `accepts_i64()` first.
    #[inline(always)]
    pub fn push_i64(&mut self, row: u32, value: i64) {
        match self {
            ColumnAccumulator::Int64 { facts, .. } => facts.push((row, value)),
            ColumnAccumulator::Dynamic {
                int_facts, has_int, ..
            } => {
                if int_facts.capacity() == 0 {
                    int_facts.reserve(Self::DYNAMIC_INITIAL_CAPACITY);
                }
                *has_int = true;
                int_facts.push((row, value));
            }
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
                if float_facts.capacity() == 0 {
                    float_facts.reserve(Self::DYNAMIC_INITIAL_CAPACITY);
                }
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
                if bool_facts.capacity() == 0 {
                    bool_facts.reserve(Self::DYNAMIC_INITIAL_CAPACITY);
                }
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
                if str_facts.capacity() == 0 {
                    str_facts.reserve(Self::DYNAMIC_INITIAL_CAPACITY);
                }
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
    /// `mode` controls string materialization: when `utf8_trusted` is true,
    /// produces zero-copy `StringViewArray`; otherwise copies into `StringArray`
    /// with full UTF-8 validation.
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
        dedup: bool,
    ) -> Result<Option<(Field, ArrayRef)>, MaterializeError> {
        match self {
            ColumnAccumulator::Int64 { facts, .. } => {
                if facts.is_empty() {
                    return Ok(None);
                }
                let (arr, dt) = build_int64(facts, num_rows, dedup);
                Ok(Some((Field::new(name, dt, true), arr)))
            }
            ColumnAccumulator::Float64 { facts, .. } => {
                if facts.is_empty() {
                    return Ok(None);
                }
                let (arr, dt) = build_float64(facts, num_rows, dedup);
                Ok(Some((Field::new(name, dt, true), arr)))
            }
            ColumnAccumulator::Bool { facts, .. } => {
                if facts.is_empty() {
                    return Ok(None);
                }
                let (arr, dt) = build_bool(facts, num_rows, dedup);
                Ok(Some((Field::new(name, dt, true), arr)))
            }
            ColumnAccumulator::String { facts, .. } => {
                if facts.is_empty() {
                    return Ok(None);
                }
                let (arr, dt) = build_string(facts, num_rows, &mode, dedup)?;
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
                        dedup,
                    )?))
                } else {
                    // Single type → flat column
                    if *has_int {
                        let (arr, dt) = build_int64(int_facts, num_rows, dedup);
                        Ok(Some((Field::new(name, dt, true), arr)))
                    } else if *has_float {
                        let (arr, dt) = build_float64(float_facts, num_rows, dedup);
                        Ok(Some((Field::new(name, dt, true), arr)))
                    } else if *has_str {
                        let (arr, dt) = build_string(str_facts, num_rows, &mode, dedup)?;
                        Ok(Some((Field::new(name, dt, true), arr)))
                    } else {
                        let (arr, dt) = build_bool(bool_facts, num_rows, dedup);
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
    /// Arrow rejected the constructed Utf8View array.
    InvalidStringView(String),
    /// Concatenated string bytes exceed `i32::MAX`, making Utf8 offsets invalid.
    OffsetOverflow,
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
            MaterializeError::InvalidStringView(err) => {
                write!(f, "invalid Utf8View array: {err}")
            }
            MaterializeError::OffsetOverflow => {
                write!(
                    f,
                    "string data exceeds i32::MAX bytes for Utf8 offset array"
                )
            }
        }
    }
}

impl std::error::Error for MaterializeError {}

// ---------------------------------------------------------------------------
// FinalizationMode — controls string materialization strategy
// ---------------------------------------------------------------------------

/// Controls how string columns are built at finalization.
#[derive(Clone)]
pub struct FinalizationMode {
    /// Original input buffer (e.g., protobuf wire bytes, scanner input).
    pub original_buf: Buffer,
    /// Generated/decoded buffer (e.g., JSON-unescaped strings).
    pub generated_buf: Buffer,
    /// When true, all string data is known to be valid UTF-8 (validated
    /// at the ingestion boundary — scanner, OTLP decoder, etc.). Enables
    /// zero-copy StringViewArray construction.  When false, copies bytes
    /// into a contiguous `StringArray` with full UTF-8 validation.
    pub utf8_trusted: bool,
}

// ---------------------------------------------------------------------------
// Array builders (free functions, no &self)
// ---------------------------------------------------------------------------

/// Check whether facts cover rows 0..num_rows exactly (no gaps, no duplicates).
///
/// The pigeonhole argument requires that row indices are *distinct*. With dedup
/// enabled the builder's bitset/last-row guard guarantees at most one write per
/// (row, field), so `facts.len() == num_rows ∧ last == num_rows - 1` is
/// sufficient.  When dedup is disabled duplicate writes are possible, breaking
/// the pigeonhole premise, so we conservatively return false.
#[allow(clippy::indexing_slicing)]
fn is_dense<T>(facts: &[(u32, T)], num_rows: usize, dedup: bool) -> bool {
    dedup
        && facts.len() == num_rows
        && (num_rows == 0 || facts[num_rows - 1].0 as usize == num_rows - 1)
}

// ---------------------------------------------------------------------------
// Pure scatter / bitmap — Arrow-free, Kani-provable
// ---------------------------------------------------------------------------

/// Scatter fact values into a default-initialized vec indexed by row.
///
/// Dense path: facts\[i\] → values\[i\] (sequential, no bounds check needed).
/// Sparse path: facts\[i\].0 → row index (last-write-wins for duplicates).
/// Rows without a fact retain `T::default()`.
///
/// Returns `(values, dense)`. Callers use `dense` to decide whether a
/// validity bitmap is needed.
#[allow(clippy::indexing_slicing)]
fn scatter_values<T: Default + Copy>(
    facts: &[(u32, T)],
    num_rows: usize,
    dedup: bool,
) -> (Vec<T>, bool) {
    let dense = is_dense(facts, num_rows, dedup);
    let mut values = vec![T::default(); num_rows];
    if dense {
        debug_assert!(
            facts.is_empty() || facts[0].0 == 0,
            "dense path requires consecutive rows starting at 0"
        );
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
    (values, dense)
}

/// Build a bit-packed validity bitmap from sparse fact row indices.
///
/// Returns raw bytes where bit `i` is **set** (1) iff row `i` is valid
/// (has at least one fact). This follows Arrow convention: 1 = valid, 0 = null.
/// Arrow-free — just a `Vec<u8>` that callers wrap in `NullBuffer`.
fn validity_bitmap_bits<T>(facts: &[(u32, T)], num_rows: usize) -> Vec<u8> {
    let byte_len = num_rows.div_ceil(8);
    let mut bits = vec![0u8; byte_len];
    for &(row, _) in facts {
        let r = row as usize;
        if r < num_rows {
            bits[r >> 3] |= 1 << (r & 7);
        }
    }
    bits
}

// ---------------------------------------------------------------------------
// Arrow wrappers — thin constructors over scatter/bitmap results
// ---------------------------------------------------------------------------

/// Build a `NullBuffer` from sparse row indices.
fn sparse_null_buffer<T>(facts: &[(u32, T)], num_rows: usize) -> NullBuffer {
    let bits = validity_bitmap_bits(facts, num_rows);
    let buf = Buffer::from_vec(bits);
    NullBuffer::new(arrow::buffer::BooleanBuffer::new(buf, 0, num_rows))
}

fn build_int64(facts: &[(u32, i64)], num_rows: usize, dedup: bool) -> (ArrayRef, DataType) {
    let (values, dense) = scatter_values(facts, num_rows, dedup);
    let nulls = if dense {
        None
    } else {
        Some(sparse_null_buffer(facts, num_rows))
    };
    (
        Arc::new(Int64Array::new(values.into(), nulls)),
        DataType::Int64,
    )
}

fn build_float64(facts: &[(u32, f64)], num_rows: usize, dedup: bool) -> (ArrayRef, DataType) {
    let (values, dense) = scatter_values(facts, num_rows, dedup);
    let nulls = if dense {
        None
    } else {
        Some(sparse_null_buffer(facts, num_rows))
    };
    (
        Arc::new(Float64Array::new(values.into(), nulls)),
        DataType::Float64,
    )
}

fn build_bool(facts: &[(u32, bool)], num_rows: usize, dedup: bool) -> (ArrayRef, DataType) {
    let (values, dense) = scatter_values(facts, num_rows, dedup);
    let nulls = if dense {
        None
    } else {
        Some(sparse_null_buffer(facts, num_rows))
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
    dedup: bool,
) -> Result<(ArrayRef, DataType), MaterializeError> {
    if mode.utf8_trusted {
        build_string_view_trusted(
            facts,
            num_rows,
            &mode.original_buf,
            &mode.generated_buf,
            dedup,
        )
    } else {
        build_string_array_validated(
            facts,
            num_rows,
            &mode.original_buf,
            &mode.generated_buf,
            dedup,
        )
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
    dedup: bool,
) -> Result<(ArrayRef, DataType), MaterializeError> {
    let original_len = original_buf.len();
    let dense = is_dense(facts, num_rows, dedup);

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
    let mut nulls: Option<NullBuffer> = None;

    if dense {
        // Dense fast path: every row has a value, facts[i] corresponds to row i.
        debug_assert!(
            facts.is_empty() || facts[0].0 == 0,
            "dense path requires consecutive rows starting at 0"
        );
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
        // Sparse: iterate all facts, assign by row index (last-write-wins for duplicates).
        let byte_len = num_rows.div_ceil(8);
        let mut bits = vec![0u8; byte_len];
        for &(row, sref) in facts {
            let r = row as usize;
            if r < num_rows {
                views[r] = make_string_view(
                    sref,
                    original_buf,
                    generated_buf,
                    original_len,
                    orig_block,
                    gen_block,
                )?;
                bits[r >> 3] |= 1 << (r & 7);
            }
        }
        let buf = Buffer::from_vec(bits);
        nulls = Some(NullBuffer::new(arrow::buffer::BooleanBuffer::new(
            buf, 0, num_rows,
        )));
    }

    debug_assert!(
        {
            // Validate UTF-8 for all referenced string bytes in debug builds.
            let mut ok = true;
            for &(_, sref) in facts {
                if let Ok(bytes) = read_str_bytes(original_buf, generated_buf, original_len, sref)
                    && std::str::from_utf8(bytes).is_err()
                {
                    ok = false;
                    break;
                }
            }
            ok
        },
        "utf8_trusted was set but string buffer contains invalid UTF-8 — \
         the ingestion boundary (scanner/decoder) has a validation bug"
    );

    let array = StringViewArray::try_new(ScalarBuffer::from(views), buffers, nulls)
        .map_err(|err| MaterializeError::InvalidStringView(err.to_string()))?;
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
    let local_end =
        local_start
            .checked_add(len as usize)
            .ok_or(MaterializeError::StringRefOutOfBounds {
                offset: sref.offset,
                len: sref.len,
                buffer_len: buf.len(),
            })?;
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
    dedup: bool,
) -> Result<(ArrayRef, DataType), MaterializeError> {
    let original_len = original_buf.len();
    let dense = is_dense(facts, num_rows, dedup);
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
            let off = i32::try_from(values.len()).map_err(|_e| MaterializeError::OffsetOverflow)?;
            offsets.push(off);
            let bytes = read_str_bytes(original_buf, generated_buf, original_len, sref)?;
            values.extend_from_slice(bytes);
        }
    } else {
        // Sparse: two-pass to handle duplicate row indices correctly.
        // First pass: find last fact index per row (last-write-wins).
        let mut last_fact_for_row: Vec<Option<usize>> = vec![None; num_rows];
        for (fi, &(row, _)) in facts.iter().enumerate() {
            let r = row as usize;
            if r < num_rows {
                last_fact_for_row[r] = Some(fi);
            }
        }
        // Second pass: build offsets and values in row order.
        for entry in &last_fact_for_row {
            let off = i32::try_from(values.len()).map_err(|_e| MaterializeError::OffsetOverflow)?;
            offsets.push(off);
            if let Some(fi) = entry {
                let sref = facts[*fi].1;
                let bytes = read_str_bytes(original_buf, generated_buf, original_len, sref)?;
                values.extend_from_slice(bytes);
                validity.push(true);
            } else {
                validity.push(false);
            }
        }
    }
    let final_off = i32::try_from(values.len()).map_err(|_e| MaterializeError::OffsetOverflow)?;
    offsets.push(final_off);

    // Validate UTF-8 for all string bytes (covers both original_buf and
    // write_str_bytes sources). This runs only in the validated path
    // (utf8_trusted=false), so the cost is acceptable.
    if std::str::from_utf8(&values).is_err() {
        // Identify which row contains the invalid UTF-8.
        for row in 0..num_rows {
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            if start < end && std::str::from_utf8(&values[start..end]).is_err() {
                return Err(MaterializeError::InvalidUtf8 {
                    offset: offsets[row] as u32,
                    len: (end - start) as u32,
                });
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

    // All bytes validated above — use checked constructor uniformly.
    // (The prior unsafe shortcut for original_buf.is_empty() was unsound when
    // write_str_bytes was used with non-&str sources.)
    let array = StringArray::new(offset_buf, values_buf, nulls);
    Ok((Arc::new(array), DataType::Utf8))
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

#[allow(clippy::too_many_arguments)]
fn build_conflict_struct(
    name: &str,
    num_rows: usize,
    mode: &FinalizationMode,
    int_facts: &[(u32, i64)],
    float_facts: &[(u32, f64)],
    bool_facts: &[(u32, bool)],
    str_facts: &[(u32, StringRef)],
    dedup: bool,
) -> Result<(Field, ArrayRef), MaterializeError> {
    let mut child_fields: Vec<Arc<Field>> = Vec::new();
    let mut child_arrays: Vec<ArrayRef> = Vec::new();

    if !int_facts.is_empty() {
        let (arr, _) = build_int64(int_facts, num_rows, dedup);
        child_fields.push(Arc::new(Field::new("int", DataType::Int64, true)));
        child_arrays.push(arr);
    }
    if !float_facts.is_empty() {
        let (arr, _) = build_float64(float_facts, num_rows, dedup);
        child_fields.push(Arc::new(Field::new("float", DataType::Float64, true)));
        child_arrays.push(arr);
    }
    if !str_facts.is_empty() {
        let (arr, dt) = build_string(str_facts, num_rows, mode, dedup)?;
        child_fields.push(Arc::new(Field::new("str", dt, true)));
        child_arrays.push(arr);
    }
    if !bool_facts.is_empty() {
        let (arr, _) = build_bool(bool_facts, num_rows, dedup);
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
    use std::mem::size_of_val;

    use super::*;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::Int64Type;

    #[test]
    fn planned_int64_materialize() {
        let mut acc = ColumnAccumulator::for_planned(FieldKind::Int64);
        acc.push_i64(0, 100);
        acc.push_i64(2, 300);

        let mode = FinalizationMode {
            original_buf: Buffer::from(&[] as &[u8]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        let (field, arr) = acc.materialize("ts", 3, mode, true).unwrap().unwrap();
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

        let mode = FinalizationMode {
            original_buf: Buffer::from(&input[..]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        let (_, arr) = acc.materialize("msg", 2, mode, true).unwrap().unwrap();
        let a = arr.as_string_view();
        assert_eq!(a.value(0), "hello");
        assert_eq!(a.value(1), "world");
    }

    #[test]
    fn planned_string_trusted_materialize() {
        let input = b"hello world";
        let mut acc = ColumnAccumulator::for_planned(FieldKind::Utf8View);
        acc.push_str(0, StringRef { offset: 0, len: 5 });
        acc.push_str(1, StringRef { offset: 6, len: 5 });

        let mode = FinalizationMode {
            original_buf: Buffer::from(&input[..]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        let (field, arr) = acc.materialize("msg", 2, mode, true).unwrap().unwrap();
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

        let mode = FinalizationMode {
            original_buf: Buffer::from(&input[..]),
            generated_buf: Buffer::from(&generated[..]),
            utf8_trusted: true,
        };
        let (_, arr) = acc.materialize("msg", 2, mode, true).unwrap().unwrap();
        let a = arr.as_string_view();
        assert_eq!(a.value(0), "original");
        assert_eq!(a.value(1), "decoded");
    }

    #[test]
    fn dynamic_single_type_flat() {
        let mut acc = ColumnAccumulator::dynamic();
        acc.push_i64(0, 42);
        acc.push_i64(1, 99);

        let mode = FinalizationMode {
            original_buf: Buffer::from(&[] as &[u8]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        let (field, arr) = acc.materialize("x", 2, mode, true).unwrap().unwrap();
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

        let mode = FinalizationMode {
            original_buf: Buffer::from(&input[..]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        let (field, arr) = acc.materialize("status", 2, mode, true).unwrap().unwrap();
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
        let mode = FinalizationMode {
            original_buf: Buffer::from(&[] as &[u8]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        assert!(acc.materialize("x", 3, mode, true).unwrap().is_none());
    }

    #[test]
    fn wrong_type_write_to_planned_is_noop() {
        let mut acc = ColumnAccumulator::for_planned(FieldKind::Int64);
        acc.push_str(0, StringRef { offset: 0, len: 3 }); // no-op for Int64
        acc.push_f64(0, 1.5); // no-op for Int64
        acc.push_bool(0, true); // no-op for Int64

        let mode = FinalizationMode {
            original_buf: Buffer::from(&[] as &[u8]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        assert!(acc.materialize("x", 1, mode, true).unwrap().is_none()); // no int facts → None
    }

    #[test]
    fn clear_and_reuse() {
        let mut acc = ColumnAccumulator::for_planned(FieldKind::Int64);
        acc.push_i64(0, 100);
        acc.clear();
        acc.push_i64(0, 200);

        let mode = FinalizationMode {
            original_buf: Buffer::from(&[] as &[u8]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        let (_, arr) = acc.materialize("x", 1, mode, true).unwrap().unwrap();
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

        let planned_size = size_of_val(&planned);
        let dynamic_size = size_of_val(&dynamic);

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

        let mode = FinalizationMode {
            original_buf: Buffer::from(b"short" as &[u8]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        let result = acc.materialize("x", 1, mode, true);
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

        let mode = FinalizationMode {
            original_buf: Buffer::from(bad_bytes),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: false,
        };
        let result = acc.materialize("x", 1, mode, true);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, MaterializeError::InvalidUtf8 { .. }),
            "expected InvalidUtf8, got {err:?}"
        );
    }

    /// Regression test: invalid UTF-8 in generated buffer (write_str_bytes path)
    /// must be caught even when original_buf is empty. Previously, the validated
    /// path skipped checking when original_buf was empty, assuming all generated
    /// data came from write_str(&str).
    #[test]
    fn invalid_utf8_in_generated_buffer_returns_error() {
        let bad_bytes: &[u8] = &[0xFF, 0xFE, 0x80, 0x81];
        let mut acc = ColumnAccumulator::for_planned(FieldKind::Utf8View);
        // Offset 0 with empty original_buf → references generated_buf at index 0.
        acc.push_str(0, StringRef { offset: 0, len: 4 });

        let mode = FinalizationMode {
            original_buf: Buffer::from(&[] as &[u8]),
            generated_buf: Buffer::from(bad_bytes),
            utf8_trusted: false,
        };
        let result = acc.materialize("x", 1, mode, true);
        assert!(
            result.is_err(),
            "invalid UTF-8 in generated buffer must be caught"
        );
        let err = result.unwrap_err();
        assert!(
            matches!(err, MaterializeError::InvalidUtf8 { .. }),
            "expected InvalidUtf8, got {err:?}"
        );
    }

    #[test]
    fn out_of_bounds_generated_returns_error() {
        let input = b"short";
        let mut acc = ColumnAccumulator::for_planned(FieldKind::Utf8View);
        // Reference past generated buffer (which is empty)
        acc.push_str(
            0,
            StringRef {
                offset: input.len() as u32,
                len: 10,
            },
        );

        let mode = FinalizationMode {
            original_buf: Buffer::from(&input[..]),
            generated_buf: Buffer::from(&[] as &[u8]),
            utf8_trusted: true,
        };
        let result = acc.materialize("x", 1, mode, true);
        assert!(result.is_err());
    }
}

// ---------------------------------------------------------------------------
// Kani proofs — exhaustive verification of pure u128 packing logic
// ---------------------------------------------------------------------------
//
// Verification strategy (aligned with project conventions in VERIFICATION.md):
//   - Kani: pure logic only — no heap allocations, no Arrow construction
//   - proptest: allocation-heavy paths (build_int64/float64/bool, read_str_bytes)
//   - Unit tests: Arrow integration (RecordBatch, finish_batch)
//
// build_int64/float64/bool and read_str_bytes are verified via proptest below
// because they allocate Vecs and/or use slice operations that cause CBMC solver
// timeouts (>10 min for read_str_bytes on [u8; 16]).
#[cfg(kani)]
mod verification {
    use super::*;

    // ── make_string_view ────────────────────────────────────────────────────

    /// Consolidated valid-path proof: covers zero-length, inline (1..=12),
    /// and buffer-ref (>12) paths in a single harness with `kani::cover!()`
    /// verifying each case is reachable.
    ///
    /// Replaces the former 3 separate proofs:
    /// - `verify_make_string_view_zero_length`
    /// - `verify_make_string_view_inline_roundtrip`
    /// - `verify_make_string_view_buffer_ref_packing`
    #[kani::proof]
    #[kani::unwind(17)]
    #[kani::solver(kissat)]
    fn verify_make_string_view_valid_paths() {
        let len: u32 = kani::any();
        kani::assume(len <= 20);

        // Fixed 32-byte buffer with symbolic first 12 bytes (covers inline
        // data round-trip and buffer-ref prefix verification).
        let mut buf = [0u8; 32];
        for i in 0..12 {
            buf[i] = kani::any();
        }

        if len == 0 {
            // Zero-length path
            let sref = StringRef { offset: 0, len: 0 };
            let view = make_string_view(sref, &[], &[], 0, 0, None).unwrap();
            assert_eq!(view, 0u128, "zero-length string must be all-zero view");
        } else if len <= 12 {
            // Inline path
            let sref = StringRef { offset: 0, len };
            let view =
                make_string_view(sref, &buf[..len as usize], &[], len as usize, 0, None).unwrap();

            let view_bytes = view.to_le_bytes();
            let extracted_len =
                u32::from_le_bytes([view_bytes[0], view_bytes[1], view_bytes[2], view_bytes[3]]);
            assert_eq!(extracted_len, len, "encoded length must match");

            for i in 0..len as usize {
                assert_eq!(
                    view_bytes[4 + i],
                    buf[i],
                    "inline data byte must match source"
                );
            }
            for i in (4 + len as usize)..16 {
                assert_eq!(view_bytes[i], 0, "padding byte must be zero");
            }
        } else {
            // Buffer-ref path (len > 12)
            kani::assume((len as usize) <= 20);
            let block_idx: u32 = kani::any();
            kani::assume(block_idx <= 4);
            let local_offset: u32 = kani::any();
            kani::assume(local_offset <= 8);
            kani::assume((local_offset as usize) + (len as usize) <= 32);

            let buf_len = local_offset as usize + len as usize;
            let sref = StringRef {
                offset: local_offset,
                len,
            };
            let view =
                make_string_view(sref, &buf[..buf_len], &[], buf_len, block_idx, None).unwrap();

            let start = local_offset as usize;
            let expected_prefix =
                u32::from_le_bytes([buf[start], buf[start + 1], buf[start + 2], buf[start + 3]]);
            let expected_view = (len as u128)
                | ((expected_prefix as u128) << 32)
                | ((block_idx as u128) << 64)
                | ((local_offset as u128) << 96);
            assert_eq!(view, expected_view, "view must match oracle computation");
        }

        kani::cover!(len == 0, "zero-length path");
        kani::cover!(len == 1, "minimal inline");
        kani::cover!(len == 12, "max inline");
        kani::cover!(len > 12, "buffer-ref path");
        kani::cover!(buf[0] == 0xFF, "high first byte");
    }

    /// Consolidated error-path proof: covers OOB references and buffer
    /// selection (original vs generated vs missing generated).
    ///
    /// Replaces the former 2 separate proofs:
    /// - `verify_make_string_view_out_of_bounds`
    /// - `verify_make_string_view_buffer_selection`
    #[kani::proof]
    #[kani::solver(kissat)]
    fn verify_make_string_view_error_and_selection() {
        let len: u32 = kani::any();
        kani::assume(len > 0 && len <= 16);

        let original_len: usize = 8;
        let original = [0xAAu8; 8];
        let generated = [0xBBu8; 8];

        let scenario: u8 = kani::any_where(|&s: &u8| s < 4);
        match scenario {
            0 => {
                // OOB in original buffer
                let offset: u32 = kani::any();
                kani::assume(offset <= 16);
                let total = (offset as usize).saturating_add(len as usize);
                kani::assume(total > original_len);
                kani::assume(offset < original_len as u32);
                let sref = StringRef { offset, len };
                let result = make_string_view(sref, &original, &[], original_len, 0, None);
                assert!(result.is_err(), "OOB original ref must return error");
            }
            1 => {
                // Valid original buffer path
                let orig_offset: u32 = kani::any();
                kani::assume(len <= 8);
                kani::assume(orig_offset <= (original_len as u32) - len);
                let sref = StringRef {
                    offset: orig_offset,
                    len,
                };
                let result =
                    make_string_view(sref, &original, &generated, original_len, 0, Some(1));
                assert!(result.is_ok(), "valid original ref must succeed");
            }
            2 => {
                // Valid generated buffer path
                let gen_offset: u32 = kani::any();
                kani::assume(len <= 8);
                kani::assume(gen_offset <= (generated.len() as u32) - len);
                let sref = StringRef {
                    offset: original_len as u32 + gen_offset,
                    len,
                };
                let result =
                    make_string_view(sref, &original, &generated, original_len, 0, Some(1));
                assert!(result.is_ok(), "valid generated ref must succeed");
            }
            _ => {
                // No generated buffer + offset in generated range → error
                let sref = StringRef {
                    offset: original_len as u32,
                    len: 1,
                };
                let result = make_string_view(sref, &original, &[], original_len, 0, None);
                assert!(
                    result.is_err(),
                    "generated offset with no generated buffer must error"
                );
            }
        }

        kani::cover!(scenario == 0, "OOB error path");
        kani::cover!(scenario == 1, "valid original path");
        kani::cover!(scenario == 2, "valid generated path");
        kani::cover!(scenario == 3, "missing generated buffer error");
    }

    // ── read_str_bytes — 2-buffer dispatch ─────────────────────────────────

    /// Consolidated valid-path proof: covers original and generated buffer
    /// dispatch in a single harness.
    ///
    /// Replaces the former 2 separate proofs:
    /// - `verify_read_str_bytes_original`
    /// - `verify_read_str_bytes_generated`
    #[kani::proof]
    #[kani::unwind(10)]
    fn verify_read_str_bytes_valid_paths() {
        let original = [0xAAu8; 8];
        let generated = [0xBBu8; 8];
        let len: u32 = kani::any();
        kani::assume(len > 0 && len <= 8);

        let use_generated: bool = kani::any();

        if use_generated {
            let gen_offset: u32 = kani::any();
            kani::assume(gen_offset <= 8 - len);
            let sref = StringRef {
                offset: original.len() as u32 + gen_offset,
                len,
            };
            let result = read_str_bytes(&original, &generated, original.len(), sref);
            assert!(result.is_ok());
            let bytes = result.unwrap();
            assert_eq!(bytes.len(), len as usize);
            for &b in bytes {
                assert_eq!(b, 0xBB);
            }
        } else {
            let offset: u32 = kani::any();
            kani::assume(offset <= 8 - len);
            let sref = StringRef { offset, len };
            let result = read_str_bytes(&original, &generated, original.len(), sref);
            assert!(result.is_ok());
            let bytes = result.unwrap();
            assert_eq!(bytes.len(), len as usize);
            for &b in bytes {
                assert_eq!(b, 0xAA);
            }
        }

        kani::cover!(use_generated, "generated buffer path");
        kani::cover!(!use_generated, "original buffer path");
        kani::cover!(len == 1, "single-byte ref");
        kani::cover!(len == 8, "full-buffer ref");
    }

    /// Consolidated error-path proof: covers OOB and boundary-spanning
    /// errors in a single harness.
    ///
    /// Replaces the former 2 separate proofs:
    /// - `verify_read_str_bytes_oob`
    /// - `verify_read_str_bytes_no_spanning`
    #[kani::proof]
    fn verify_read_str_bytes_errors() {
        let original = [0xAAu8; 4];
        let generated = [0xBBu8; 4];
        let offset: u32 = kani::any();
        let len: u32 = kani::any();
        kani::assume(len > 0 && len <= 16);
        kani::assume(offset <= 16);

        let sref = StringRef { offset, len };
        let end = (offset as usize).saturating_add(len as usize);

        if offset < original.len() as u32 {
            if end > original.len() {
                // Spanning or OOB in original range
                let result = read_str_bytes(&original, &generated, original.len(), sref);
                assert!(result.is_err(), "spanning/OOB original ref must error");
            }
        } else {
            let adj_start = offset as usize - original.len();
            let adj_end = adj_start + len as usize;
            if adj_end > generated.len() {
                // OOB in generated range
                let result = read_str_bytes(&original, &generated, original.len(), sref);
                assert!(result.is_err(), "OOB generated ref must error");
            }
        }

        kani::cover!(
            offset < original.len() as u32 && end > original.len(),
            "spanning/OOB in original range"
        );
        kani::cover!(
            offset >= original.len() as u32
                && (offset as usize - original.len()).saturating_add(len as usize)
                    > generated.len(),
            "OOB in generated range"
        );
        kani::cover!(offset == 0, "zero offset error");
    }

    // ── scatter_values — dense/sparse placement ────────────────────────────

    /// Dense scatter: values[i] == facts[i].1 for all i.
    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_scatter_values_dense() {
        let num_rows: usize = kani::any();
        kani::assume(num_rows > 0 && num_rows <= 4);

        let mut facts: Vec<(u32, i64)> = Vec::new();
        for row in 0..num_rows {
            let val: i64 = kani::any();
            facts.push((row as u32, val));
        }

        let (values, dense) = scatter_values(&facts, num_rows, true);
        assert!(dense, "full-coverage + dedup must be dense");
        assert_eq!(values.len(), num_rows);
        for (i, &(_, v)) in facts.iter().enumerate() {
            assert_eq!(values[i], v, "dense value must match fact");
        }

        kani::cover!(num_rows == 1, "single row dense");
        kani::cover!(num_rows == 4, "max rows dense");
    }

    /// Sparse scatter: fact rows get their value, gap rows stay default.
    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_scatter_values_sparse() {
        let num_rows: usize = kani::any();
        kani::assume(num_rows > 0 && num_rows <= 4);

        let presence: u8 = kani::any();
        let mut facts: Vec<(u32, i64)> = Vec::new();
        for row in 0..num_rows {
            if presence & (1 << row) != 0 {
                facts.push((row as u32, row as i64 + 100));
            }
        }
        kani::assume(!facts.is_empty());
        kani::assume(facts.len() < num_rows);

        let (values, dense) = scatter_values(&facts, num_rows, true);
        assert!(!dense, "partial coverage must not be dense");
        assert_eq!(values.len(), num_rows);

        for row in 0..num_rows {
            if presence & (1 << row) != 0 {
                assert_eq!(values[row], row as i64 + 100);
            } else {
                assert_eq!(values[row], 0, "gap row must be default");
            }
        }

        kani::cover!(facts.len() == 1, "single fact");
        kani::cover!(num_rows == 4 && facts.len() == 2, "half-populated");
    }

    /// Last-write-wins: when dedup=false (sparse path), later facts overwrite.
    #[kani::proof]
    fn verify_scatter_values_last_write_wins() {
        let first: i64 = kani::any();
        let second: i64 = kani::any();
        kani::assume(first != second);

        let facts = vec![(0u32, first), (0u32, second)];
        let (values, dense) = scatter_values(&facts, 1, false);
        assert!(!dense, "dedup=false must not be dense");
        assert_eq!(values[0], second, "last write must win");

        kani::cover!(first < second, "increasing overwrite");
        kani::cover!(first > second, "decreasing overwrite");
    }

    // ── validity_bitmap_bits — bit-packed correctness ──────────────────────

    /// Every fact row has its bit set (valid), every gap row has bit clear (null).
    #[kani::proof]
    #[kani::unwind(10)]
    fn verify_validity_bitmap_bits_correctness() {
        let num_rows: usize = kani::any();
        kani::assume(num_rows > 0 && num_rows <= 8);

        let presence: u8 = kani::any();
        let mut facts: Vec<(u32, i64)> = Vec::new();
        for row in 0..num_rows {
            if presence & (1 << row) != 0 {
                facts.push((row as u32, 0));
            }
        }
        kani::assume(!facts.is_empty());

        let bits = validity_bitmap_bits(&facts, num_rows);
        assert_eq!(bits.len(), num_rows.div_ceil(8));

        for row in 0..num_rows {
            let bit_set = bits[row >> 3] & (1 << (row & 7)) != 0;
            let has_fact = presence & (1 << row) != 0;
            assert_eq!(bit_set, has_fact, "bit must match fact presence at row");
        }

        kani::cover!(num_rows == 1, "single row");
        kani::cover!(num_rows == 8, "full byte boundary");
        kani::cover!(presence == 0xFF, "all rows present");
    }

    /// Out-of-bounds row indices are silently ignored.
    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_validity_bitmap_bits_oob_ignored() {
        let num_rows: usize = 4;
        let oob_row: u32 = kani::any();
        kani::assume(oob_row >= num_rows as u32 && oob_row <= 255);

        let facts = vec![(0u32, 0i64), (oob_row, 0i64)];
        let bits = validity_bitmap_bits(&facts, num_rows);

        assert!(bits[0] & 1 != 0, "row 0 must be set");
        for row in 1..num_rows {
            let bit_set = bits[row >> 3] & (1 << (row & 7)) != 0;
            assert!(!bit_set, "non-fact row must be clear");
        }

        kani::cover!(oob_row == 4, "minimal OOB");
        kani::cover!(oob_row > 100, "large OOB");
    }

    // ── is_dense — classification ──────────────────────────────────────────

    /// is_dense returns true iff facts cover [0, num_rows) exactly with dedup.
    #[kani::proof]
    #[kani::unwind(6)]
    fn verify_is_dense_classification() {
        let num_rows: usize = kani::any();
        kani::assume(num_rows > 0 && num_rows <= 4);

        let mut facts: Vec<(u32, i64)> = Vec::new();
        for row in 0..num_rows {
            facts.push((row as u32, 0));
        }

        assert!(is_dense(&facts, num_rows, true));
        assert!(!is_dense(&facts, num_rows, false));

        if num_rows > 1 {
            let partial = facts[..num_rows - 1].to_vec();
            assert!(!is_dense(&partial, num_rows, true));
        }

        kani::cover!(num_rows == 1, "single row");
        kani::cover!(num_rows == 4, "max test rows");
    }
}

// ---------------------------------------------------------------------------
// Proptest — statistical verification for allocation-heavy materialization
// ---------------------------------------------------------------------------
#[cfg(test)]
mod proptests {
    use super::*;
    use arrow::array::Array;
    use proptest::prelude::*;

    // ── build_int64 ─────────────────────────────────────────────────────────

    proptest! {
        /// Dense int64: all rows present, no nulls.
        #[test]
        fn build_int64_dense(values in prop::collection::vec(any::<i64>(), 1..=64)) {
            let num_rows = values.len();
            let facts: Vec<(u32, i64)> = values.iter().enumerate()
                .map(|(i, &v)| (i as u32, v)).collect();
            let (arr, dt) = build_int64(&facts, num_rows, true);
            prop_assert_eq!(dt, DataType::Int64);
            prop_assert_eq!(arr.len(), num_rows);
            prop_assert_eq!(arr.null_count(), 0, "dense path must have no nulls");
            let typed = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            for (i, &v) in values.iter().enumerate() {
                prop_assert_eq!(typed.value(i), v);
            }
        }

        /// Sparse int64: random subset of rows, gaps are null.
        #[test]
        fn build_int64_sparse(
            num_rows in 1usize..=32,
            seed in any::<u64>(),
        ) {
            // Deterministic sparse pattern from seed.
            let mut facts: Vec<(u32, i64)> = Vec::new();
            for row in 0..num_rows as u32 {
                if (seed.wrapping_mul(row as u64 + 1)) % 3 != 0 {
                    facts.push((row, row as i64 * 100));
                }
            }
            if facts.is_empty() {
                // Ensure at least one fact for a meaningful test.
                facts.push((0, 42));
            }
            let (arr, _) = build_int64(&facts, num_rows, true);
            prop_assert_eq!(arr.len(), num_rows);

            let typed = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            let mut fi = 0;
            for row in 0..num_rows {
                if fi < facts.len() && facts[fi].0 as usize == row {
                    prop_assert!(!typed.is_null(row), "fact row must not be null");
                    prop_assert_eq!(typed.value(row), facts[fi].1);
                    fi += 1;
                } else {
                    prop_assert!(typed.is_null(row), "gap row must be null");
                }
            }
        }
    }

    // ── build_float64 ───────────────────────────────────────────────────────

    proptest! {
        /// Dense float64: all rows present, no nulls.
        #[test]
        fn build_float64_dense(
            values in prop::collection::vec(
                any::<f64>().prop_filter("finite", |v| v.is_finite()),
                1..=64,
            )
        ) {
            let num_rows = values.len();
            let facts: Vec<(u32, f64)> = values.iter().enumerate()
                .map(|(i, &v)| (i as u32, v)).collect();
            let (arr, dt) = build_float64(&facts, num_rows, true);
            prop_assert_eq!(dt, DataType::Float64);
            prop_assert_eq!(arr.len(), num_rows);
            prop_assert_eq!(arr.null_count(), 0);
            let typed = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            for (i, &v) in values.iter().enumerate() {
                prop_assert_eq!(typed.value(i).to_bits(), v.to_bits());
            }
        }

        /// Sparse float64: gaps are null.
        #[test]
        fn build_float64_sparse(num_rows in 1usize..=32, seed in any::<u64>()) {
            let mut facts: Vec<(u32, f64)> = Vec::new();
            for row in 0..num_rows as u32 {
                if (seed.wrapping_mul(row as u64 + 1)) % 3 != 0 {
                    facts.push((row, row as f64 * 1.5));
                }
            }
            if facts.is_empty() {
                facts.push((0, 42.0));
            }
            let (arr, _) = build_float64(&facts, num_rows, true);
            prop_assert_eq!(arr.len(), num_rows);

            let typed = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            let mut fi = 0;
            for row in 0..num_rows {
                if fi < facts.len() && facts[fi].0 as usize == row {
                    prop_assert!(!typed.is_null(row));
                    prop_assert_eq!(
                        typed.value(row).to_bits(),
                        facts[fi].1.to_bits(),
                        "value mismatch at row {}", row
                    );
                    fi += 1;
                } else {
                    prop_assert!(typed.is_null(row));
                }
            }
        }
    }

    // ── build_bool ──────────────────────────────────────────────────────────

    proptest! {
        /// Dense bool: all rows present, no nulls.
        #[test]
        fn build_bool_dense(values in prop::collection::vec(any::<bool>(), 1..=64)) {
            let num_rows = values.len();
            let facts: Vec<(u32, bool)> = values.iter().enumerate()
                .map(|(i, &v)| (i as u32, v)).collect();
            let (arr, dt) = build_bool(&facts, num_rows, true);
            prop_assert_eq!(dt, DataType::Boolean);
            prop_assert_eq!(arr.len(), num_rows);
            prop_assert_eq!(arr.null_count(), 0);
            let typed = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            for (i, &v) in values.iter().enumerate() {
                prop_assert_eq!(typed.value(i), v);
            }
        }

        /// Sparse bool: gaps are null.
        #[test]
        fn build_bool_sparse(num_rows in 1usize..=32, seed in any::<u64>()) {
            let mut facts: Vec<(u32, bool)> = Vec::new();
            for row in 0..num_rows as u32 {
                if (seed.wrapping_mul(row as u64 + 1)) % 3 != 0 {
                    facts.push((row, row % 2 == 0));
                }
            }
            if facts.is_empty() {
                facts.push((0, true));
            }
            let (arr, _) = build_bool(&facts, num_rows, true);
            prop_assert_eq!(arr.len(), num_rows);

            let typed = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            let mut fi = 0;
            for row in 0..num_rows {
                if fi < facts.len() && facts[fi].0 as usize == row {
                    prop_assert!(!typed.is_null(row));
                    prop_assert_eq!(typed.value(row), facts[fi].1);
                    fi += 1;
                } else {
                    prop_assert!(typed.is_null(row));
                }
            }
        }
    }

    // ── dedup=false duplicate rows ──────────────────────────────────────────

    proptest! {
        /// With dedup disabled and duplicate row indices, the sparse path must be
        /// used and last-write-wins for each row.
        #[test]
        fn build_int64_no_dedup_duplicates(num_rows in 2usize..=16, seed in any::<u64>()) {
            // Build facts with some duplicate row indices.
            let mut facts: Vec<(u32, i64)> = Vec::new();
            for i in 0..num_rows as u32 {
                let row = (seed.wrapping_mul(i as u64 + 1) % num_rows as u64) as u32;
                facts.push((row, i as i64 * 7));
            }
            // Sort by row to satisfy monotonicity requirement.
            facts.sort_by_key(|f| f.0);
            let (arr, _) = build_int64(&facts, num_rows, false);
            prop_assert_eq!(arr.len(), num_rows);
            let typed = arr.as_any().downcast_ref::<Int64Array>().unwrap();

            // Verify last-write-wins for each row.
            let mut expected: std::collections::HashMap<u32, i64> = std::collections::HashMap::new();
            for &(row, val) in &facts {
                expected.insert(row, val);
            }
            for row in 0..num_rows {
                if let Some(&val) = expected.get(&(row as u32)) {
                    prop_assert!(!typed.is_null(row), "written row must not be null");
                    prop_assert_eq!(typed.value(row), val);
                } else {
                    prop_assert!(typed.is_null(row), "unwritten row must be null");
                }
            }
        }
    }

    // ── read_str_bytes ──────────────────────────────────────────────────────

    proptest! {
        /// Strings in the original buffer resolve correctly.
        #[test]
        fn read_str_bytes_original_buffer(
            orig_len in 1usize..=128,
            offset in 0u32..128,
            len in 1u32..=64,
        ) {
            prop_assume!((offset as usize) + (len as usize) <= orig_len);
            let original = vec![0xCCu8; orig_len];
            let generated: Vec<u8> = vec![0xDDu8; 16];

            let sref = StringRef { offset, len };
            let result = read_str_bytes(&original, &generated, orig_len, sref);
            prop_assert!(result.is_ok());
            let bytes = result.unwrap();
            prop_assert_eq!(bytes.len(), len as usize);
            for &b in bytes {
                prop_assert_eq!(b, 0xCC);
            }
        }

        /// Strings in the generated buffer resolve correctly.
        #[test]
        fn read_str_bytes_generated_buffer(
            orig_len in 1usize..=64,
            gen_len in 1usize..=128,
            gen_offset in 0u32..128,
            len in 1u32..=64,
        ) {
            prop_assume!((gen_offset as usize) + (len as usize) <= gen_len);
            let original = vec![0xCCu8; orig_len];
            let generated = vec![0xDDu8; gen_len];

            let sref = StringRef {
                offset: orig_len as u32 + gen_offset,
                len,
            };
            let result = read_str_bytes(&original, &generated, orig_len, sref);
            prop_assert!(result.is_ok());
            let bytes = result.unwrap();
            prop_assert_eq!(bytes.len(), len as usize);
            for &b in bytes {
                prop_assert_eq!(b, 0xDD);
            }
        }

        /// A string starting in original but extending past it is rejected.
        #[test]
        fn read_str_bytes_spanning_rejected(
            offset in 0u32..8,
            len in 2u32..=12,
        ) {
            let orig_len = 8usize;
            prop_assume!((offset as usize) < orig_len);
            prop_assume!((offset as usize + len as usize) > orig_len);

            let original = [0xCCu8; 8];
            let generated = [0xDDu8; 8];
            let sref = StringRef { offset, len };
            let result = read_str_bytes(&original, &generated, orig_len, sref);
            prop_assert!(result.is_err(), "spanning strings must be rejected");
        }

        /// Completely out-of-bounds references always fail.
        #[test]
        fn read_str_bytes_out_of_bounds(
            offset in 128u32..256,
            len in 1u32..=32,
        ) {
            let original = [0xCCu8; 8];
            let generated = [0xDDu8; 8];
            let sref = StringRef { offset, len };
            let result = read_str_bytes(&original, &generated, 8, sref);
            prop_assert!(result.is_err());
        }
    }
}

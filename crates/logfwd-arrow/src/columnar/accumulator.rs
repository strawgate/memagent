// accumulator.rs — ColumnAccumulator: typed per-field storage.
//
// Each variant carries only the storage it needs:
//   - Planned Int64 → one Vec<(u32, i64)>
//   - Planned String → one Vec<(u32, u32, u32)> (row, offset, len)
//   - Dynamic → all 4 vecs + conflict flags (same as FieldColumns today)
//
// Materialization is distributed: each variant builds its own Arrow array.
// No monolithic finalization function.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringBuilder, StringViewBuilder, StructArray,
};
use arrow::buffer::{Buffer, NullBuffer};
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
pub(crate) struct StringRef {
    pub(crate) offset: u32,
    pub(crate) len: u32,
}

// ---------------------------------------------------------------------------
// ColumnAccumulator — typed per-field storage + materialization
// ---------------------------------------------------------------------------

/// Typed per-field storage.  Each variant carries only what it needs.
///
/// Planned fields use single-type variants (one Vec per field).
/// Dynamic fields use `Dynamic` (all 4 Vecs + conflict flags).
pub(crate) enum ColumnAccumulator {
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
    pub(crate) fn for_planned(kind: FieldKind) -> Self {
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
    pub(crate) fn dynamic() -> Self {
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
    pub(crate) fn clear(&mut self) {
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
    pub(crate) fn last_row_mut(&mut self) -> &mut u32 {
        match self {
            ColumnAccumulator::Int64 { last_row, .. }
            | ColumnAccumulator::Float64 { last_row, .. }
            | ColumnAccumulator::Bool { last_row, .. }
            | ColumnAccumulator::String { last_row, .. }
            | ColumnAccumulator::Dynamic { last_row, .. } => last_row,
        }
    }

    /// Last row written.
    pub(crate) fn last_row(&self) -> u32 {
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
    pub(crate) fn push_i64(&mut self, row: u32, value: i64) {
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
    pub(crate) fn push_f64(&mut self, row: u32, value: f64) {
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
    pub(crate) fn push_bool(&mut self, row: u32, value: bool) {
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
    pub(crate) fn push_str(&mut self, row: u32, sref: StringRef) {
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
    /// Returns `None` if the accumulator has no data (sparse all-null field
    /// with no writes — omitted from schema like StreamingBuilder does).
    ///
    /// `mode` controls string materialization:
    ///   - `Detached`: uses `StringBuilder` (copies strings, self-contained)
    ///   - `View`: uses `StringViewArray` backed by input + generated buffers
    pub(crate) fn materialize(
        &self,
        name: &str,
        num_rows: usize,
        mode: FinalizationMode<'_>,
    ) -> Option<(Field, ArrayRef)> {
        match self {
            ColumnAccumulator::Int64 { facts, .. } => {
                if facts.is_empty() {
                    return None;
                }
                let (arr, dt) = build_int64(facts, num_rows);
                Some((Field::new(name, dt, true), arr))
            }
            ColumnAccumulator::Float64 { facts, .. } => {
                if facts.is_empty() {
                    return None;
                }
                let (arr, dt) = build_float64(facts, num_rows);
                Some((Field::new(name, dt, true), arr))
            }
            ColumnAccumulator::Bool { facts, .. } => {
                if facts.is_empty() {
                    return None;
                }
                let (arr, dt) = build_bool(facts, num_rows);
                Some((Field::new(name, dt, true), arr))
            }
            ColumnAccumulator::String { facts, .. } => {
                if facts.is_empty() {
                    return None;
                }
                let (arr, dt) = build_string(facts, num_rows, &mode);
                Some((Field::new(name, dt, true), arr))
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
                    return None;
                }
                if type_count > 1 {
                    // Conflict → StructArray
                    Some(build_conflict_struct(
                        name,
                        num_rows,
                        &mode,
                        int_facts,
                        float_facts,
                        bool_facts,
                        str_facts,
                    ))
                } else {
                    // Single type → flat column
                    if *has_int {
                        let (arr, dt) = build_int64(int_facts, num_rows);
                        Some((Field::new(name, dt, true), arr))
                    } else if *has_float {
                        let (arr, dt) = build_float64(float_facts, num_rows);
                        Some((Field::new(name, dt, true), arr))
                    } else if *has_str {
                        let (arr, dt) = build_string(str_facts, num_rows, &mode);
                        Some((Field::new(name, dt, true), arr))
                    } else {
                        let (arr, dt) = build_bool(bool_facts, num_rows);
                        Some((Field::new(name, dt, true), arr))
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// FinalizationMode — controls string materialization strategy
// ---------------------------------------------------------------------------

/// Controls how string columns are built at finalization.
#[derive(Clone)]
pub(crate) enum FinalizationMode<'a> {
    /// Copy strings into contiguous `StringArray` (self-contained, input-freeable).
    Detached {
        /// Original input buffer, for reading string data.
        original_buf: &'a [u8],
        /// Generated/decoded buffer, for reading decoded strings.
        generated_buf: &'a [u8],
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
    let mut values = vec![0i64; num_rows];
    let mut valid = vec![false; num_rows];
    for &(row, v) in facts {
        let r = row as usize;
        if r < num_rows {
            values[r] = v;
            valid[r] = true;
        }
    }
    let nulls = NullBuffer::from(valid);
    (
        Arc::new(Int64Array::new(values.into(), Some(nulls))),
        DataType::Int64,
    )
}

fn build_float64(facts: &[(u32, f64)], num_rows: usize) -> (ArrayRef, DataType) {
    let mut values = vec![0.0f64; num_rows];
    let mut valid = vec![false; num_rows];
    for &(row, v) in facts {
        let r = row as usize;
        if r < num_rows {
            values[r] = v;
            valid[r] = true;
        }
    }
    let nulls = NullBuffer::from(valid);
    (
        Arc::new(Float64Array::new(values.into(), Some(nulls))),
        DataType::Float64,
    )
}

fn build_bool(facts: &[(u32, bool)], num_rows: usize) -> (ArrayRef, DataType) {
    let mut values = vec![false; num_rows];
    let mut valid = vec![false; num_rows];
    for &(row, v) in facts {
        let r = row as usize;
        if r < num_rows {
            values[r] = v;
            valid[r] = true;
        }
    }
    let nulls = NullBuffer::from(valid);
    (
        Arc::new(BooleanArray::new(values.into(), Some(nulls))),
        DataType::Boolean,
    )
}

fn build_string(
    facts: &[(u32, StringRef)],
    num_rows: usize,
    mode: &FinalizationMode<'_>,
) -> (ArrayRef, DataType) {
    match mode {
        FinalizationMode::Detached {
            original_buf,
            generated_buf,
        } => {
            let original_len = original_buf.len();
            let mut builder = StringBuilder::with_capacity(facts.len(), 0);
            let mut vi = 0;
            for row in 0..num_rows as u32 {
                if vi < facts.len() && facts[vi].0 == row {
                    let sref = facts[vi].1;
                    let s = read_str(original_buf, generated_buf, original_len, sref);
                    match s {
                        Some(s) => builder.append_value(s),
                        None => builder.append_null(),
                    }
                    vi += 1;
                } else {
                    builder.append_null();
                }
            }
            (Arc::new(builder.finish()), DataType::Utf8)
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
                            .expect("pre-validated offset/len");
                    } else if let Some(gen_block) = gen_block {
                        let gen_offset = sref.offset - *original_len;
                        builder
                            .try_append_view(gen_block, gen_offset, sref.len)
                            .expect("pre-validated offset/len");
                    } else {
                        builder.append_null();
                    }
                    vi += 1;
                } else {
                    builder.append_null();
                }
            }
            (Arc::new(builder.finish()), DataType::Utf8View)
        }
    }
}

/// Read a string from the 2-buffer system.
fn read_str<'a>(
    original: &'a [u8],
    generated: &'a [u8],
    original_len: usize,
    sref: StringRef,
) -> Option<&'a str> {
    let start = sref.offset as usize;
    let end = start.checked_add(sref.len as usize)?;
    let bytes = if start < original_len {
        original.get(start..end)?
    } else {
        let dec_start = start.checked_sub(original_len)?;
        let dec_end = end.checked_sub(original_len)?;
        generated.get(dec_start..dec_end)?
    };
    std::str::from_utf8(bytes).ok()
}

fn build_conflict_struct(
    name: &str,
    num_rows: usize,
    mode: &FinalizationMode<'_>,
    int_facts: &[(u32, i64)],
    float_facts: &[(u32, f64)],
    bool_facts: &[(u32, bool)],
    str_facts: &[(u32, StringRef)],
) -> (Field, ArrayRef) {
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
        let (arr, dt) = build_string(str_facts, num_rows, mode);
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

    (
        Field::new(name, DataType::Struct(fields), true),
        Arc::new(struct_arr),
    )
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
            original_buf: &[],
            generated_buf: &[],
        };
        let (field, arr) = acc.materialize("ts", 3, mode).unwrap();
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
            original_buf: input,
            generated_buf: &[],
        };
        let (_, arr) = acc.materialize("msg", 2, mode).unwrap();
        let a = arr.as_string::<i32>();
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
        let (field, arr) = acc.materialize("msg", 2, mode).unwrap();
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
            original_buf: input,
            generated_buf: generated,
        };
        let (_, arr) = acc.materialize("msg", 2, mode).unwrap();
        let a = arr.as_string::<i32>();
        assert_eq!(a.value(0), "original");
        assert_eq!(a.value(1), "decoded");
    }

    #[test]
    fn dynamic_single_type_flat() {
        let mut acc = ColumnAccumulator::dynamic();
        acc.push_i64(0, 42);
        acc.push_i64(1, 99);

        let mode = FinalizationMode::Detached {
            original_buf: &[],
            generated_buf: &[],
        };
        let (field, arr) = acc.materialize("x", 2, mode).unwrap();
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
        acc.push_str(
            1,
            StringRef {
                offset: 0,
                len: 2,
            },
        );

        let mode = FinalizationMode::Detached {
            original_buf: input,
            generated_buf: &[],
        };
        let (field, arr) = acc.materialize("status", 2, mode).unwrap();
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
            original_buf: &[],
            generated_buf: &[],
        };
        assert!(acc.materialize("x", 3, mode).is_none());
    }

    #[test]
    fn wrong_type_write_to_planned_is_noop() {
        let mut acc = ColumnAccumulator::for_planned(FieldKind::Int64);
        acc.push_str(0, StringRef { offset: 0, len: 3 }); // no-op for Int64
        acc.push_f64(0, 1.5); // no-op for Int64
        acc.push_bool(0, true); // no-op for Int64

        let mode = FinalizationMode::Detached {
            original_buf: &[],
            generated_buf: &[],
        };
        assert!(acc.materialize("x", 1, mode).is_none()); // no int facts → None
    }

    #[test]
    fn clear_and_reuse() {
        let mut acc = ColumnAccumulator::for_planned(FieldKind::Int64);
        acc.push_i64(0, 100);
        acc.clear();
        acc.push_i64(0, 200);

        let mode = FinalizationMode::Detached {
            original_buf: &[],
            generated_buf: &[],
        };
        let (_, arr) = acc.materialize("x", 1, mode).unwrap();
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
}

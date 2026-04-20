use arrow::array::{
    Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, LargeBinaryArray, LargeStringArray,
    PrimitiveArray, StringArray, StringViewArray, StructArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use arrow::record_batch::RecordBatch;
use logfwd_types::field_names;
use std::collections::HashSet;

use crate::row_json::write_json_string;

/// Where to find one typed variant of a conflict field.
pub enum ColVariant {
    /// A top-level flat Arrow column.
    Flat { col_idx: usize, dt: DataType },
    /// One child field inside a StructArray conflict column.
    StructField {
        struct_col_idx: usize,
        field_idx: usize,
        dt: DataType,
    },
}

/// Describes one output JSON field, potentially backed by a struct conflict
/// column or multiple flat typed columns.
pub struct ColInfo {
    /// Logical field name (e.g. "status", "body").
    pub field_name: String,
    /// Pre-serialized `"fieldname":` bytes for zero-copy key emission.
    ///
    /// Built once at `build_col_infos` time so `write_row_json` can emit
    /// the key with a single `extend_from_slice` instead of re-running the
    /// SIMD escape scan on every row.
    pub key_json: Box<[u8]>,
    /// Variants ordered for JSON output: Int64 > Float64 > Boolean > Utf8.
    pub json_variants: Vec<ColVariant>,
    /// Variants ordered for the virtual coalesced Utf8 column: Utf8 first,
    /// then Boolean, Int64, Float64.  Used by Loki label extraction.
    pub str_variants: Vec<ColVariant>,
}

/// Returns `true` if a Struct column's child fields are all type-name fields
/// ("int", "float", "str", "bool") — i.e. it is a conflict struct column.
///
/// Note: the current builders only emit "int", "float", and "str" children.
/// "bool" is included for forward compatibility — if a future builder adds a
/// bool child, this detection and the priority functions will handle it
/// automatically.
pub(crate) fn is_conflict_struct(fields: &arrow::datatypes::Fields) -> bool {
    if fields.is_empty() {
        return false;
    }
    let unique_count = fields
        .iter()
        .map(|f| f.name().as_str())
        .collect::<HashSet<_>>()
        .len();
    unique_count == fields.len()
        && fields.iter().all(|f| {
            let child_name = f.name().as_str();
            field_names::CONFLICT_CHILDREN.contains(&child_name)
                && conflict_child_type_matches(child_name, f.data_type())
        })
}

fn conflict_child_type_matches(child_name: &str, dt: &DataType) -> bool {
    match child_name {
        "int" => matches!(dt, DataType::Int64),
        "float" => matches!(dt, DataType::Float64),
        "str" => matches!(
            dt,
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
        ),
        "bool" => matches!(dt, DataType::Boolean),
        _ => false,
    }
}

/// JSON output priority: higher wins per row.  Int64 > Float64 > Boolean > Utf8.
pub(crate) fn json_priority(dt: &DataType) -> u8 {
    match dt {
        DataType::Int64 => 4,
        DataType::Float64 => 3,
        DataType::Boolean => 2,
        _ => 1,
    }
}

/// String-coalesce priority: Utf8 wins (for Loki labels etc.), then Bool, Int, Float.
pub(crate) fn str_priority(dt: &DataType) -> u8 {
    match dt {
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => 4,
        DataType::Boolean => 3,
        DataType::Int64 => 2,
        DataType::Float64 => 1,
        _ => 0,
    }
}

/// Extract the `DataType` from any `ColVariant`.
pub(crate) fn variant_dt(v: &ColVariant) -> &DataType {
    match v {
        ColVariant::Flat { dt, .. } => dt,
        ColVariant::StructField { dt, .. } => dt,
    }
}

/// Returns `true` if the given `ColVariant` is null at `row` in `batch`.
pub(crate) fn is_null(batch: &RecordBatch, variant: &ColVariant, row: usize) -> bool {
    match variant {
        ColVariant::Flat { col_idx, .. } => batch
            .columns()
            .get(*col_idx)
            .is_none_or(|col| col.is_null(row)),
        ColVariant::StructField {
            struct_col_idx,
            field_idx,
            ..
        } => {
            let Some(sa) = batch
                .columns()
                .get(*struct_col_idx)
                .and_then(|col| col.as_any().downcast_ref::<StructArray>())
            else {
                return true;
            };
            sa.is_null(row)
                || sa
                    .columns()
                    .get(*field_idx)
                    .is_none_or(|child| child.is_null(row))
        }
    }
}

/// Return a reference to the underlying Arrow array for a `ColVariant`.
pub(crate) fn get_array<'b>(batch: &'b RecordBatch, variant: &ColVariant) -> Option<&'b dyn Array> {
    match variant {
        ColVariant::Flat { col_idx, .. } => batch.columns().get(*col_idx).map(AsRef::as_ref),
        ColVariant::StructField {
            struct_col_idx,
            field_idx,
            ..
        } => {
            let sa = batch
                .columns()
                .get(*struct_col_idx)
                .and_then(|col| col.as_any().downcast_ref::<StructArray>())?;
            sa.columns().get(*field_idx).map(AsRef::as_ref)
        }
    }
}

// ---------------------------------------------------------------------------
// Pre-resolved typed column references (hot-loop optimization)
// ---------------------------------------------------------------------------

/// Pre-downcast concrete array reference, avoiding per-row `data_type()` +
/// `downcast_ref` + virtual `is_null()` overhead.  Built once per batch in
/// [`resolve_col_infos`].
#[allow(clippy::enum_variant_names)]
pub enum TypedArrayRef<'a> {
    Null,
    Int8(&'a PrimitiveArray<Int8Type>),
    Int16(&'a PrimitiveArray<Int16Type>),
    Int32(&'a PrimitiveArray<Int32Type>),
    Int64(&'a PrimitiveArray<Int64Type>),
    UInt8(&'a PrimitiveArray<UInt8Type>),
    UInt16(&'a PrimitiveArray<UInt16Type>),
    UInt32(&'a PrimitiveArray<UInt32Type>),
    UInt64(&'a PrimitiveArray<UInt64Type>),
    Float32(&'a PrimitiveArray<Float32Type>),
    Float64(&'a PrimitiveArray<Float64Type>),
    Boolean(&'a BooleanArray),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
    Struct(&'a StructArray),
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
    FixedSizeBinary(&'a FixedSizeBinaryArray),
    /// Fallback for unsupported types — uses the dyn Array path.
    Other(&'a dyn Array),
}

/// Downcast a `&dyn Array` to the concrete typed reference once per batch.
fn resolve_typed(arr: &dyn Array) -> TypedArrayRef<'_> {
    match arr.data_type() {
        DataType::Null => TypedArrayRef::Null,
        DataType::Int8 => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::Int8),
        DataType::Int16 => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::Int16),
        DataType::Int32 => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::Int32),
        DataType::Int64 => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::Int64),
        DataType::UInt8 => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::UInt8),
        DataType::UInt16 => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::UInt16),
        DataType::UInt32 => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::UInt32),
        DataType::UInt64 => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::UInt64),
        DataType::Float32 => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::Float32),
        DataType::Float64 => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::Float64),
        DataType::Boolean => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::Boolean),
        DataType::Utf8 => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::Utf8),
        DataType::LargeUtf8 => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::LargeUtf8),
        DataType::Utf8View => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::Utf8View),
        DataType::Struct(_) => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::Struct),
        DataType::Binary => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::Binary),
        DataType::LargeBinary => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::LargeBinary),
        DataType::FixedSizeBinary(_) => arr
            .as_any()
            .downcast_ref()
            .map_or(TypedArrayRef::Other(arr), TypedArrayRef::FixedSizeBinary),
        _ => TypedArrayRef::Other(arr),
    }
}

/// A column variant with pre-resolved typed array and cached null bitmap.
pub struct ResolvedVariant<'a> {
    pub(crate) typed: TypedArrayRef<'a>,
    pub(crate) nulls: Option<&'a NullBuffer>,
    /// Parent struct's null bitmap (for struct child variants only).
    pub(crate) parent_nulls: Option<&'a NullBuffer>,
}

impl ResolvedVariant<'_> {
    #[inline]
    pub(crate) fn is_null(&self, row: usize) -> bool {
        matches!(self.typed, TypedArrayRef::Null)
            || self.parent_nulls.is_some_and(|n| n.is_null(row))
            || self.nulls.is_some_and(|n| n.is_null(row))
    }
}

/// Pre-resolved column info for the hot loop.
///
/// Most columns are flat with exactly one variant (`Single`), which avoids
/// Vec allocation and iterator overhead. Multi-variant conflict columns use
/// `Multi`.
pub enum ResolvedCol<'a> {
    /// Flat column with a single type — the 90%+ common case.
    Single {
        key_json: &'a [u8],
        typed: TypedArrayRef<'a>,
        nulls: Option<&'a NullBuffer>,
    },
    /// Struct conflict column or multiple flat typed variants.
    Multi {
        key_json: &'a [u8],
        variants: Vec<ResolvedVariant<'a>>,
    },
}

impl ResolvedCol<'_> {
    /// Find the first non-null typed array at `row`, or `None` if all null.
    #[inline]
    pub fn resolve(&self, row: usize) -> Option<(&[u8], &TypedArrayRef<'_>)> {
        match self {
            ResolvedCol::Single {
                key_json,
                typed,
                nulls,
            } => {
                if matches!(typed, TypedArrayRef::Null) || nulls.is_some_and(|n| n.is_null(row)) {
                    None
                } else {
                    Some((key_json, typed))
                }
            }
            ResolvedCol::Multi { key_json, variants } => {
                let v = variants.iter().find(|v| !v.is_null(row))?;
                Some((key_json, &v.typed))
            }
        }
    }
}

/// Pre-resolve all `ColInfo` variants to typed array references.
///
/// Called once per batch.  The returned `ResolvedCol` slices eliminate per-row:
/// - virtual `is_null()` dispatch → direct `NullBuffer` bitmap check
/// - `data_type()` + `downcast_ref` → pre-resolved enum discriminant
/// - `batch.columns().get()` + `Arc::deref` → cached references
pub fn resolve_col_infos<'a>(batch: &'a RecordBatch, cols: &'a [ColInfo]) -> Vec<ResolvedCol<'a>> {
    cols.iter()
        .filter_map(|col| {
            let key_json: &'a [u8] = &col.key_json;

            // Fast path: single flat variant (no Vec, no iterator in hot loop)
            if col.json_variants.len() == 1
                && let ColVariant::Flat { col_idx, .. } = &col.json_variants[0]
            {
                let array = batch.columns().get(*col_idx)?;
                let arr_ref = array.as_ref();
                return Some(ResolvedCol::Single {
                    key_json,
                    typed: resolve_typed(arr_ref),
                    nulls: arr_ref.nulls(),
                });
            }

            let variants: Vec<ResolvedVariant<'a>> = col
                .json_variants
                .iter()
                .filter_map(|v| match v {
                    ColVariant::Flat { col_idx, .. } => {
                        let array = batch.columns().get(*col_idx)?;
                        let arr_ref = array.as_ref();
                        Some(ResolvedVariant {
                            typed: resolve_typed(arr_ref),
                            nulls: arr_ref.nulls(),
                            parent_nulls: None,
                        })
                    }
                    ColVariant::StructField {
                        struct_col_idx,
                        field_idx,
                        ..
                    } => {
                        let parent_col = batch.columns().get(*struct_col_idx)?;
                        let sa = parent_col.as_any().downcast_ref::<StructArray>()?;
                        let child = sa.columns().get(*field_idx)?;
                        let child_ref = child.as_ref();
                        Some(ResolvedVariant {
                            typed: resolve_typed(child_ref),
                            nulls: child_ref.nulls(),
                            parent_nulls: parent_col.nulls(),
                        })
                    }
                })
                .collect();

            if variants.is_empty() {
                None
            } else {
                Some(ResolvedCol::Multi { key_json, variants })
            }
        })
        .collect()
}

fn sort_variants(info: &mut ColInfo) {
    info.json_variants
        .sort_by_key(|v| std::cmp::Reverse(json_priority(variant_dt(v))));
    info.str_variants
        .sort_by_key(|v| std::cmp::Reverse(str_priority(variant_dt(v))));
}

fn upsert_col_info(
    infos: &mut Vec<ColInfo>,
    field_name: &str,
    mut json_variants: Vec<ColVariant>,
    mut str_variants: Vec<ColVariant>,
) {
    if let Some(existing) = infos.iter_mut().find(|c| c.field_name == field_name) {
        existing.json_variants.append(&mut json_variants);
        existing.str_variants.append(&mut str_variants);
        sort_variants(existing);
    } else {
        let key_json = build_key_json(field_name);
        let mut info = ColInfo {
            field_name: field_name.to_string(),
            key_json,
            json_variants,
            str_variants,
        };
        sort_variants(&mut info);
        infos.push(info);
    }
}

/// Pre-serialize `,"fieldname":` into a heap-allocated byte slice.
///
/// Called once per field at `build_col_infos` time.  The result is stored in
/// `ColInfo::key_json` so `write_row_json` can emit the key with a single
/// `extend_from_slice`.  The leading comma allows the hot loop to write
/// `&key_json[1..]` for the first field and `&key_json[..]` for subsequent
/// fields, eliminating a separate `push(b',')` per field per row.
fn build_key_json(field_name: &str) -> Box<[u8]> {
    let mut buf = Vec::with_capacity(field_name.len() + 4); // , + " + name + " + :
    buf.push(b',');
    write_json_string(&mut buf, field_name)
        .expect("field name JSON serialization cannot fail for valid UTF-8 field names");
    buf.push(b':');
    buf.into_boxed_slice()
}

/// Build a grouped, ordered list of output fields from a RecordBatch schema.
///
/// Handles struct conflict columns (`status: Struct { int, str }`) and plain
/// flat columns.  Flat column names are used verbatim — no suffix stripping.
/// The returned `ColInfo` items contain two independently ordered variant
/// lists for the two coalesce strategies (JSON and string).
pub fn build_col_infos(batch: &RecordBatch) -> Vec<ColInfo> {
    let schema = batch.schema();
    let mut infos: Vec<ColInfo> = Vec::new();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        if field_names::is_internal_column(field.name()) {
            continue;
        }
        match field.data_type() {
            DataType::Struct(child_fields) if is_conflict_struct(child_fields) => {
                // Struct conflict column: one ColInfo, variants = child fields.
                let json_variants: Vec<ColVariant> = child_fields
                    .iter()
                    .enumerate()
                    .map(|(field_idx, f)| ColVariant::StructField {
                        struct_col_idx: col_idx,
                        field_idx,
                        dt: f.data_type().clone(),
                    })
                    .collect();
                let str_variants: Vec<ColVariant> = child_fields
                    .iter()
                    .enumerate()
                    .map(|(field_idx, f)| ColVariant::StructField {
                        struct_col_idx: col_idx,
                        field_idx,
                        dt: f.data_type().clone(),
                    })
                    .collect();

                upsert_col_info(
                    &mut infos,
                    field.name().as_str(),
                    json_variants,
                    str_variants,
                );
            }
            dt => {
                // Plain flat column — use the column name verbatim.
                // The scanner no longer produces `_int`/`_str`/`_float` suffixed
                // flat columns; single-type fields use the bare JSON key name and
                // multi-type conflicts use StructArray.  User-defined SQL aliases
                // (e.g. `SELECT duration_ms_int AS dur_int`) must be preserved
                // exactly — stripping the suffix would mangle the alias (#705).
                upsert_col_info(
                    &mut infos,
                    field.name().as_str(),
                    vec![ColVariant::Flat {
                        col_idx,
                        dt: dt.clone(),
                    }],
                    vec![ColVariant::Flat {
                        col_idx,
                        dt: dt.clone(),
                    }],
                );
            }
        }
    }

    infos
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray, StructArray};
    use arrow::datatypes::{Field, Fields, Schema};
    use std::sync::Arc;

    #[test]
    fn struct_variant_out_of_bounds_is_treated_as_null_and_missing() {
        let struct_fields = Fields::from(vec![Field::new("int", DataType::Int64, true)]);
        let struct_array = StructArray::new(
            struct_fields.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(1)]))],
            None,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Struct(struct_fields),
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(struct_array)]).expect("valid batch");

        let bad_child = ColVariant::StructField {
            struct_col_idx: 0,
            field_idx: 99,
            dt: DataType::Int64,
        };

        assert!(is_null(&batch, &bad_child, 0));
        assert!(get_array(&batch, &bad_child).is_none());
    }

    #[test]
    fn flat_variant_out_of_bounds_is_treated_as_null_and_missing() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![Some(10)]))])
            .expect("valid batch");

        let bad_flat = ColVariant::Flat {
            col_idx: 42,
            dt: DataType::Int64,
        };

        assert!(is_null(&batch, &bad_flat, 0));
        assert!(get_array(&batch, &bad_flat).is_none());
    }

    #[test]
    fn build_col_infos_skips_internal_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, true),
            Field::new("__source_id", DataType::Int64, true),
            Field::new("__typename", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["hello"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![7])) as ArrayRef,
                Arc::new(StringArray::from(vec!["LogEvent"])) as ArrayRef,
            ],
        )
        .expect("valid batch");

        let infos = build_col_infos(&batch);

        assert_eq!(infos.len(), 2);
        assert_eq!(infos[0].field_name, "msg");
        assert_eq!(infos[1].field_name, "__typename");
    }
}

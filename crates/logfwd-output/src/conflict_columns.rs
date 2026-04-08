use arrow::array::{Array, StructArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use logfwd_types::field_names;
use std::collections::HashSet;

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
    /// Logical field name (e.g. "status", "_raw").
    pub field_name: String,
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
        ColVariant::Flat { col_idx, .. } => batch.column(*col_idx).is_null(row),
        ColVariant::StructField {
            struct_col_idx,
            field_idx,
            ..
        } => {
            let Some(sa) = batch
                .column(*struct_col_idx)
                .as_any()
                .downcast_ref::<StructArray>()
            else {
                return true;
            };
            sa.is_null(row) || sa.column(*field_idx).is_null(row)
        }
    }
}

/// Return a reference to the underlying Arrow array for a `ColVariant`.
pub(crate) fn get_array<'b>(batch: &'b RecordBatch, variant: &ColVariant) -> Option<&'b dyn Array> {
    match variant {
        ColVariant::Flat { col_idx, .. } => Some(batch.column(*col_idx).as_ref()),
        ColVariant::StructField {
            struct_col_idx,
            field_idx,
            ..
        } => {
            let sa = batch
                .column(*struct_col_idx)
                .as_any()
                .downcast_ref::<StructArray>()?;
            Some(sa.column(*field_idx).as_ref())
        }
    }
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
        match field.data_type() {
            DataType::Struct(child_fields) if is_conflict_struct(child_fields) => {
                // Struct conflict column: one ColInfo, variants = child fields.
                let mut json_variants: Vec<ColVariant> = child_fields
                    .iter()
                    .enumerate()
                    .map(|(field_idx, f)| ColVariant::StructField {
                        struct_col_idx: col_idx,
                        field_idx,
                        dt: f.data_type().clone(),
                    })
                    .collect();
                let mut str_variants: Vec<ColVariant> = child_fields
                    .iter()
                    .enumerate()
                    .map(|(field_idx, f)| ColVariant::StructField {
                        struct_col_idx: col_idx,
                        field_idx,
                        dt: f.data_type().clone(),
                    })
                    .collect();

                json_variants.sort_by_key(|v| std::cmp::Reverse(json_priority(variant_dt(v))));
                str_variants.sort_by_key(|v| std::cmp::Reverse(str_priority(variant_dt(v))));
                let field_name = field.name().as_str();
                if let Some(existing) = infos.iter_mut().find(|c| c.field_name == field_name) {
                    existing.json_variants.extend(json_variants);
                    existing.str_variants.extend(str_variants);
                    existing
                        .json_variants
                        .sort_by_key(|v| std::cmp::Reverse(json_priority(variant_dt(v))));
                    existing
                        .str_variants
                        .sort_by_key(|v| std::cmp::Reverse(str_priority(variant_dt(v))));
                } else {
                    infos.push(ColInfo {
                        field_name: field_name.to_string(),
                        json_variants,
                        str_variants,
                    });
                }
            }
            dt => {
                // Plain flat column — use the column name verbatim.
                // The scanner no longer produces `_int`/`_str`/`_float` suffixed
                // flat columns; single-type fields use the bare JSON key name and
                // multi-type conflicts use StructArray.  User-defined SQL aliases
                // (e.g. `SELECT duration_ms_int AS dur_int`) must be preserved
                // exactly — stripping the suffix would mangle the alias (#705).
                let field_name = field.name().as_str();
                if let Some(existing) = infos.iter_mut().find(|c| c.field_name == field_name) {
                    existing.json_variants.push(ColVariant::Flat {
                        col_idx,
                        dt: dt.clone(),
                    });
                    existing.str_variants.push(ColVariant::Flat {
                        col_idx,
                        dt: dt.clone(),
                    });
                    // Re-sort both lists.
                    existing
                        .json_variants
                        .sort_by_key(|v| std::cmp::Reverse(json_priority(variant_dt(v))));
                    existing
                        .str_variants
                        .sort_by_key(|v| std::cmp::Reverse(str_priority(variant_dt(v))));
                } else {
                    infos.push(ColInfo {
                        field_name: field_name.to_string(),
                        json_variants: vec![ColVariant::Flat {
                            col_idx,
                            dt: dt.clone(),
                        }],
                        str_variants: vec![ColVariant::Flat {
                            col_idx,
                            dt: dt.clone(),
                        }],
                    });
                }
            }
        }
    }

    infos
}

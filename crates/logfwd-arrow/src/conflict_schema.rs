//! Schema normalization for type-conflict batches.
//!
//! The Arrow builders now emit a `Struct` column for each field that contains
//! values of multiple types across rows.  For example a `status` field that is
//! an integer in some rows and a string in others is represented as:
//!
//! ```text
//! status: Struct { int: Int64, str: Utf8View }
//! ```
//!
//! A struct column is a *conflict struct* iff **all** its child field names are
//! drawn from the set `{"int", "float", "str", "bool"}`.
//!
//! [`normalize_conflict_columns`] replaces every such struct column in-place
//! with a flat `Utf8` column of the same name:
//!
//! ```text
//! status: Struct { int: Int64, str: Utf8View }  →  status: Utf8
//! ```
//!
//! The flat column is computed as:
//! ```text
//! COALESCE(CAST(int AS Utf8), CAST(float AS Utf8), CASE WHEN bool IS TRUE THEN 'true' WHEN bool IS FALSE THEN 'false' ELSE NULL END, str)
//! ```
//! so it is non-null whenever any typed child is non-null.
//!
//! After normalization, `SELECT status FROM logs` resolves in both clean
//! batches (`status: Int64` already flat) and conflict batches (struct replaced
//! with flat `Utf8` column here). Users call `int(status)` / `float(status)`
//! for numeric operations on conflict-origin columns.

use std::sync::Arc;

use arrow::array::{Array, StringBuilder, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConflictValueSource {
    Int,
    Float,
    Str,
    Bool,
}

#[allow(clippy::fn_params_excessive_bools)] // Pure presence flags, not a config API
fn pick_conflict_value_source(
    int_present: bool,
    float_present: bool,
    bool_present: bool,
    str_present: bool,
) -> Option<ConflictValueSource> {
    if int_present {
        Some(ConflictValueSource::Int)
    } else if float_present {
        Some(ConflictValueSource::Float)
    } else if bool_present {
        Some(ConflictValueSource::Bool)
    } else if str_present {
        Some(ConflictValueSource::Str)
    } else {
        None
    }
}

fn conflict_child_kind(name: &str) -> Option<ConflictValueSource> {
    let bytes = name.as_bytes();
    match bytes.len() {
        3 if bytes[0] == b'i' && bytes[1] == b'n' && bytes[2] == b't' => {
            Some(ConflictValueSource::Int)
        }
        3 if bytes[0] == b's' && bytes[1] == b't' && bytes[2] == b'r' => {
            Some(ConflictValueSource::Str)
        }
        4 if bytes[0] == b'b' && bytes[1] == b'o' && bytes[2] == b'o' && bytes[3] == b'l' => {
            Some(ConflictValueSource::Bool)
        }
        5 if bytes[0] == b'f'
            && bytes[1] == b'l'
            && bytes[2] == b'o'
            && bytes[3] == b'a'
            && bytes[4] == b't' =>
        {
            Some(ConflictValueSource::Float)
        }
        _ => None,
    }
}

fn is_conflict_child_name(name: &str) -> bool {
    conflict_child_kind(name).is_some()
}

/// Returns `true` iff every child field name is one of the conflict-type names.
///
/// An empty struct is never a conflict struct.
fn is_conflict_struct(fields: &Fields) -> bool {
    !fields.is_empty()
        && fields
            .iter()
            .all(|field| is_conflict_child_name(field.name().as_str()))
}

/// Returns `true` if `schema` contains at least one conflict-struct column (i.e. a
/// `Struct` whose child field names are all in `{"int", "float", "str", "bool"}`).
///
/// Use this as a cheap pre-check before calling [`normalize_conflict_columns`] to avoid
/// an unnecessary `RecordBatch::clone()` when the batch has no conflict columns.
#[logfwd_lint_attrs::no_panic]
#[logfwd_lint_attrs::pure]
pub fn has_conflict_struct_columns(schema: &Schema) -> bool {
    schema
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), DataType::Struct(cf) if is_conflict_struct(cf)))
}

/// Replace every conflict struct column with a flat `Utf8` column of the same name.
///
/// A struct column is a conflict struct iff all its child field names are in
/// `{"int", "float", "str", "bool"}` (see `is_conflict_struct`).
///
/// Batches with no conflict struct columns are returned unchanged (zero
/// allocation). Any schema metadata is preserved.
pub fn normalize_conflict_columns(batch: RecordBatch) -> RecordBatch {
    let schema = batch.schema();

    // Fast path: no struct columns at all.
    if !has_conflict_struct_columns(&schema) {
        return batch;
    }

    let mut new_fields: Vec<Field> = Vec::with_capacity(schema.fields().len());
    let mut new_arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());

    for (col_idx, field) in schema.fields().iter().enumerate() {
        if let DataType::Struct(child_fields) = field.data_type()
            && is_conflict_struct(child_fields)
        {
            // Downcast to StructArray and extract named children.
            let struct_arr = batch
                .column(col_idx)
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("column declared as Struct must downcast to StructArray");

            let find_child = |name: &str| -> Option<&dyn Array> {
                child_fields
                    .iter()
                    .enumerate()
                    .find(|(_, f)| f.name() == name)
                    .map(|(i, _)| struct_arr.column(i).as_ref())
            };

            let merged = merge_to_utf8(
                find_child("int"),
                find_child("float"),
                find_child("bool"),
                find_child("str"),
                batch.num_rows(),
            );

            // Replace in-place: same name, flat Utf8 type.
            new_fields.push(Field::new(field.name(), DataType::Utf8, true));
            new_arrays.push(merged);
            continue;
        }

        // Non-conflict column: keep as-is.
        new_fields.push((**field).clone());
        new_arrays.push(Arc::clone(batch.column(col_idx)));
    }

    let new_schema = Arc::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ));

    RecordBatch::try_new(new_schema, new_arrays)
        .expect("normalize_conflict_columns: schema/array length mismatch — this is a bug")
}

/// Merge int, float, bool, and str variants into a single `Utf8` column via COALESCE.
///
/// Priority order: int (cast to str) > float (cast to str) > bool ("true"/"false") > str.
pub fn merge_to_utf8(
    int_col: Option<&dyn Array>,
    float_col: Option<&dyn Array>,
    bool_col: Option<&dyn Array>,
    str_col: Option<&dyn Array>,
    num_rows: usize,
) -> Arc<dyn Array> {
    use arrow::array::{BooleanArray, StringArray};
    use arrow::compute;

    // Arrow always supports Int64 → Utf8 and Float64 → Utf8 (formats as decimal).
    // Utf8View → Utf8 is also infallible. These casts cannot fail at runtime.
    let int_s = int_col.map(|c| {
        compute::cast(c, &DataType::Utf8).expect("Int64 → Utf8 cast is always supported by Arrow")
    });
    let float_s = float_col.map(|c| {
        compute::cast(c, &DataType::Utf8).expect("Float64 → Utf8 cast is always supported by Arrow")
    });
    // StreamingBuilder emits str columns as Utf8View; finish_batch_detached emits Utf8.
    // Both cast cleanly to Utf8 here. This loses the zero-copy StringView property,
    // but normalize_conflict_columns is only called in the SQL transform path,
    // so the trade-off is intentional and acceptable.
    let str_s = str_col.map(|c| {
        compute::cast(c, &DataType::Utf8)
            .expect("Utf8/Utf8View → Utf8 cast is always supported by Arrow")
    });

    let int_arr = int_s
        .as_ref()
        .and_then(|a| a.as_any().downcast_ref::<StringArray>());
    let float_arr = float_s
        .as_ref()
        .and_then(|a| a.as_any().downcast_ref::<StringArray>());
    let bool_arr = bool_col.and_then(|a| a.as_any().downcast_ref::<BooleanArray>());
    let str_arr = str_s
        .as_ref()
        .and_then(|a| a.as_any().downcast_ref::<StringArray>());

    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 8);
    for i in 0..num_rows {
        match pick_conflict_value_source(
            int_arr.is_some_and(|a| !a.is_null(i)),
            float_arr.is_some_and(|a| !a.is_null(i)),
            bool_arr.is_some_and(|a| !a.is_null(i)),
            str_arr.is_some_and(|a| !a.is_null(i)),
        ) {
            Some(ConflictValueSource::Int) => {
                builder.append_value(int_arr.expect("int presence was checked above").value(i));
            }
            Some(ConflictValueSource::Float) => builder.append_value(
                float_arr
                    .expect("float presence was checked above")
                    .value(i),
            ),
            Some(ConflictValueSource::Bool) => {
                let b = bool_arr.expect("bool presence was checked above").value(i);
                builder.append_value(if b { "true" } else { "false" });
            }
            Some(ConflictValueSource::Str) => {
                builder.append_value(str_arr.expect("str presence was checked above").value(i));
            }
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

#[cfg(kani)]
mod verification {
    use super::*;

    fn conflict_kind_for_tag(tag: u8) -> Option<ConflictValueSource> {
        match tag {
            0 => Some(ConflictValueSource::Int),
            1 => Some(ConflictValueSource::Float),
            2 => Some(ConflictValueSource::Str),
            3 => Some(ConflictValueSource::Bool),
            _ => None,
        }
    }

    #[kani::proof]
    fn verify_is_conflict_child_name_policy() {
        let tag: u8 = kani::any_where(|&tag| tag <= 4);
        assert_eq!(
            is_conflict_child_name(match tag {
                0 => "int",
                1 => "float",
                2 => "str",
                3 => "bool",
                _ => "other",
            }),
            conflict_kind_for_tag(tag).is_some()
        );
        kani::cover!(tag <= 3, "allowed conflict child names are accepted");
        kani::cover!(tag == 4, "unknown child names are rejected");
    }

    #[kani::proof]
    fn verify_pick_conflict_value_source_precedence() {
        let int_present: bool = kani::any();
        let float_present: bool = kani::any();
        let bool_present: bool = kani::any();
        let str_present: bool = kani::any();

        let expected = if int_present {
            Some(ConflictValueSource::Int)
        } else if float_present {
            Some(ConflictValueSource::Float)
        } else if bool_present {
            Some(ConflictValueSource::Bool)
        } else if str_present {
            Some(ConflictValueSource::Str)
        } else {
            None
        };

        assert_eq!(
            pick_conflict_value_source(int_present, float_present, bool_present, str_present),
            expected
        );
        kani::cover!(int_present, "int wins when present");
        kani::cover!(
            !int_present && float_present,
            "float wins when int is absent"
        );
        kani::cover!(
            !int_present && !float_present && bool_present,
            "bool wins next"
        );
        kani::cover!(
            !int_present && !float_present && !bool_present && str_present,
            "str wins when it is the only populated child"
        );
        kani::cover!(
            !int_present && !float_present && !bool_present && !str_present,
            "all-null rows stay null"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, StringViewBuilder};
    use arrow::datatypes::{Field, Schema};

    /// Build a plain batch with no struct columns.
    fn make_batch(fields: Vec<Field>, arrays: Vec<Arc<dyn Array>>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    /// Build a RecordBatch with a single struct conflict column.
    ///
    /// The struct has child fields `"int"` (Int64) and `"str"` (Utf8View).
    fn make_conflict_struct_batch(
        field_name: &str,
        int_vals: Vec<Option<i64>>,
        str_vals: Vec<Option<&str>>,
    ) -> RecordBatch {
        assert_eq!(
            int_vals.len(),
            str_vals.len(),
            "int_vals and str_vals must have the same length"
        );

        let int_arr: Arc<dyn Array> = Arc::new(Int64Array::from(int_vals));

        let mut sv = StringViewBuilder::new();
        for v in &str_vals {
            match v {
                Some(s) => sv.append_value(s),
                None => sv.append_null(),
            }
        }
        let str_arr: Arc<dyn Array> = Arc::new(sv.finish());

        let struct_fields = Fields::from(vec![
            Field::new("int", DataType::Int64, true),
            Field::new("str", DataType::Utf8View, true),
        ]);
        let struct_arr = StructArray::new(
            struct_fields.clone(),
            vec![Arc::clone(&int_arr), Arc::clone(&str_arr)],
            None, // no nulls at the struct level
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            field_name,
            DataType::Struct(struct_fields),
            true,
        )]));

        RecordBatch::try_new(schema, vec![Arc::new(struct_arr) as Arc<dyn Array>])
            .expect("make_conflict_struct_batch: failed to build RecordBatch")
    }

    /// Build a RecordBatch with a single struct column from arbitrary children.
    fn make_struct_batch(field_name: &str, children: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
        let struct_fields: Fields = children
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let arrays: Vec<Arc<dyn Array>> = children.into_iter().map(|(_, arr)| arr).collect();
        let struct_arr = StructArray::new(struct_fields.clone(), arrays, None);
        let schema = Arc::new(Schema::new(vec![Field::new(
            field_name,
            DataType::Struct(struct_fields),
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(struct_arr) as Arc<dyn Array>]).unwrap()
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /// A flat Utf8 column (no struct) must pass through unchanged.
    #[test]
    fn no_conflict_passthrough() {
        let batch = make_batch(
            vec![
                Field::new("status", DataType::Int64, true),
                Field::new("level", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![200i64, 404])),
                Arc::new(StringArray::from(vec!["INFO", "WARN"])),
            ],
        );
        let normalized = normalize_conflict_columns(batch.clone());
        assert_eq!(normalized.schema(), batch.schema());
        assert_eq!(normalized.num_columns(), 2);
    }

    /// A batch with no struct columns at all must be returned unchanged (fast path).
    #[test]
    fn no_op_for_plain_batch() {
        let batch = make_batch(
            vec![Field::new("msg", DataType::Utf8, true)],
            vec![Arc::new(StringArray::from(vec!["hello", "world"]))],
        );
        let result = normalize_conflict_columns(batch.clone());
        // Pointer-level identity not guaranteed, but schema and data must match.
        assert_eq!(result.schema(), batch.schema());
        assert_eq!(result.num_columns(), 1);
        assert_eq!(result.num_rows(), 2);
    }

    /// Struct{int=200,str=null} + Struct{int=null,str="OK"} -> flat ["200","OK"].
    #[test]
    fn conflict_struct_normalizes_to_utf8() {
        let batch =
            make_conflict_struct_batch("status", vec![Some(200), None], vec![None, Some("OK")]);
        let normalized = normalize_conflict_columns(batch);

        // The struct column must be replaced in-place -- still 1 column.
        assert_eq!(normalized.num_columns(), 1);

        let status = normalized
            .column_by_name("status")
            .expect("status column must exist");
        assert_eq!(
            *status.data_type(),
            DataType::Utf8,
            "conflict struct must be replaced with flat Utf8"
        );
        let arr = status.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "200"); // from int child
        assert_eq!(arr.value(1), "OK"); // from str child
    }

    /// When both int and str are non-null in the same row, int takes priority.
    #[test]
    fn int_priority_over_str() {
        let batch = make_conflict_struct_batch(
            "x",
            vec![Some(42), None],
            vec![Some("should-lose"), Some("wins")],
        );
        let normalized = normalize_conflict_columns(batch);
        let x = normalized.column_by_name("x").unwrap();
        let arr = x.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            arr.value(0),
            "42",
            "int must win over str when both non-null"
        );
        assert_eq!(arr.value(1), "wins", "str fills in when int is null");
    }

    /// Tests that boolean columns correctly coalesce into the flattened string.
    #[test]
    fn bool_conflict_normalizes_to_utf8() {
        use arrow::array::BooleanArray;

        let batch = make_struct_batch(
            "status",
            vec![
                (
                    "int",
                    Arc::new(Int64Array::from(vec![None, None])) as Arc<dyn Array>,
                ),
                (
                    "bool",
                    Arc::new(BooleanArray::from(vec![Some(true), Some(false)])) as Arc<dyn Array>,
                ),
            ],
        );

        let normalized = normalize_conflict_columns(batch);
        let status = normalized.column_by_name("status").unwrap();
        let arr = status.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(arr.value(0), "true");
        assert_eq!(arr.value(1), "false");
    }

    /// A struct row where all children are null must produce a null in output.
    #[test]
    fn all_null_row_produces_null() {
        let batch = make_conflict_struct_batch("v", vec![None, None], vec![None, None]);
        let normalized = normalize_conflict_columns(batch);
        let v = normalized.column_by_name("v").unwrap();
        let arr = v.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(arr.is_null(0), "all-null struct row must produce null");
        assert!(arr.is_null(1), "all-null struct row must produce null");
    }

    /// A struct whose children are NOT type-names must NOT be normalized.
    #[test]
    fn non_conflict_struct_not_touched() {
        // Struct with child fields "x" and "y" -- not type names.
        let x_arr: Arc<dyn Array> = Arc::new(Int64Array::from(vec![1i64, 2]));
        let y_arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b"]));

        let struct_fields = Fields::from(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Utf8, true),
        ]);
        let struct_arr = StructArray::new(
            struct_fields.clone(),
            vec![Arc::clone(&x_arr), Arc::clone(&y_arr)],
            None,
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            "point",
            DataType::Struct(struct_fields),
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(struct_arr) as Arc<dyn Array>]).unwrap();

        let result = normalize_conflict_columns(batch.clone());

        // Schema must be unchanged.
        assert_eq!(result.schema(), batch.schema());
        // The column must still be a Struct, not Utf8.
        assert!(
            matches!(result.schema().field(0).data_type(), DataType::Struct(_)),
            "non-conflict struct must not be normalized"
        );
    }

    /// Boolean values in a conflict struct must be preserved as "true"/"false".
    ///
    /// Regression test: `merge_to_utf8` previously ignored the `bool` child,
    /// causing boolean values to be silently dropped (normalized to null).
    #[test]
    fn bool_child_preserved_as_string() {
        use arrow::array::BooleanArray;

        let batch = make_struct_batch(
            "active",
            vec![
                (
                    "bool",
                    Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]))
                        as Arc<dyn Array>,
                ),
                (
                    "str",
                    Arc::new(StringArray::from(vec![
                        None::<&str>,
                        None,
                        Some("fallback"),
                    ])) as Arc<dyn Array>,
                ),
            ],
        );

        let normalized = normalize_conflict_columns(batch);
        let active = normalized.column_by_name("active").unwrap();
        assert_eq!(*active.data_type(), DataType::Utf8);

        let arr = active.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "true", "true bool must normalize to \"true\"");
        assert_eq!(
            arr.value(1),
            "false",
            "false bool must normalize to \"false\""
        );
        assert_eq!(arr.value(2), "fallback", "str fallback when bool is null");
    }

    /// bool takes lower priority than int (int wins when both non-null in same row).
    #[test]
    fn int_priority_over_bool() {
        use arrow::array::BooleanArray;

        let batch = make_struct_batch(
            "x",
            vec![
                (
                    "int",
                    Arc::new(Int64Array::from(vec![Some(42i64), None])) as Arc<dyn Array>,
                ),
                (
                    "bool",
                    Arc::new(BooleanArray::from(vec![Some(true), Some(false)])) as Arc<dyn Array>,
                ),
            ],
        );

        let normalized = normalize_conflict_columns(batch);
        let x = normalized.column_by_name("x").unwrap();
        let arr = x.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            arr.value(0),
            "42",
            "int must win over bool when both non-null"
        );
        assert_eq!(arr.value(1), "false", "bool fills in when int is null");
    }

    /// Any metadata on the schema must be preserved after normalization.
    #[test]
    fn schema_metadata_preserved() {
        use std::collections::HashMap;

        let batch = make_struct_batch(
            "status",
            vec![
                (
                    "int",
                    Arc::new(Int64Array::from(vec![Some(200i64), None])) as Arc<dyn Array>,
                ),
                (
                    "str",
                    Arc::new(StringArray::from(vec![None, Some("OK")])) as Arc<dyn Array>,
                ),
            ],
        );

        let mut meta = HashMap::new();
        meta.insert("custom.key".to_string(), "custom.value".to_string());
        let schema_with_meta = Arc::new(batch.schema().as_ref().clone().with_metadata(meta));
        let batch = RecordBatch::try_new(schema_with_meta, batch.columns().to_vec()).unwrap();

        let normalized = normalize_conflict_columns(batch);
        assert_eq!(
            normalized.schema().metadata().get("custom.key"),
            Some(&"custom.value".to_string()),
            "schema metadata must be preserved after normalization"
        );
    }
}

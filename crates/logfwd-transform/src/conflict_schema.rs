//! Schema normalization for type-conflict batches.
//!
//! When the scanner detects that a field appears with multiple types across
//! rows in a batch (e.g. `status` is an integer in some rows and a string in
//! others), it emits *suffixed* columns: `status__int: Int64` and
//! `status__str: Utf8View`. SQL that references the bare name `status` would
//! then fail to resolve against the batch schema.
//!
//! [`normalize_conflict_columns`] detects such conflict groups and adds a
//! computed bare column for each one:
//!
//! ```text
//! status__int: Int64, status__str: Utf8View  →  + status: Utf8
//! ```
//!
//! The bare column is computed as:
//! ```text
//! COALESCE(CAST(status__int AS Utf8), CAST(status__float AS Utf8), status__str)
//! ```
//! so it is non-null whenever any typed variant is non-null.
//!
//! After normalization, `SELECT status FROM logs` resolves in both clean
//! batches (`status: Int64` already bare) and conflict batches (bare `status`
//! column added here). Users call `int(status)` / `float(status)` for numeric
//! operations on conflict-origin columns.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

/// Suffixes the scanner appends when a type conflict is detected.
const CONFLICT_SUFFIXES: &[&str] = &["__int", "__float", "__str"];

/// The schema metadata key written by the builders when conflict columns are emitted.
const CONFLICT_GROUPS_METADATA_KEY: &str = "logfwd.conflict_groups";

/// Parse `"status:int,str;duration:float,int"` into `["status", "duration"]`.
fn parse_conflict_group_bases(meta: &str) -> Vec<&str> {
    meta.split(';')
        .filter_map(|group| group.split_once(':').map(|(base, _)| base))
        .filter(|base| !base.is_empty())
        .collect()
}

/// Add a bare `Utf8` column for every conflict group that lacks one.
///
/// Uses the `logfwd.conflict_groups` schema metadata key as the authoritative
/// discriminator. If the key is absent, the batch has no conflict columns and
/// is returned unchanged. This avoids false positives from user fields whose
/// names happen to end in `__int`, `__str`, etc.
///
/// Batches with no conflict groups are returned unchanged (zero allocation).
/// Any schema metadata (including `logfwd.conflict_groups`) is preserved.
pub fn normalize_conflict_columns(batch: RecordBatch) -> RecordBatch {
    let schema = batch.schema();

    // Use the metadata as the authoritative source of conflict groups.
    // If absent, there are no conflict columns — return unchanged.
    let meta_val = match schema.metadata().get(CONFLICT_GROUPS_METADATA_KEY) {
        Some(v) => v.clone(),
        None => return batch,
    };

    let base_names = parse_conflict_group_bases(&meta_val);
    if base_names.is_empty() {
        return batch;
    }

    // Collect the set of bare column names already present.
    let existing_bare: std::collections::HashSet<&str> =
        schema.fields().iter().map(|f| f.name().as_str()).collect();

    // Build groups from the metadata base names, looking up variant columns by name.
    let mut groups: BTreeMap<&str, Vec<(&str, usize)>> = BTreeMap::new();
    for base in &base_names {
        // Skip if a bare column already exists.
        if existing_bare.contains(*base) {
            continue;
        }
        for suf in CONFLICT_SUFFIXES {
            let col_name = format!("{base}{suf}");
            if let Ok(idx) = schema.index_of(&col_name) {
                groups.entry(*base).or_default().push((suf, idx));
            }
        }
    }

    // Only keep groups that actually have variant columns.
    groups.retain(|_, members| !members.is_empty());

    if groups.is_empty() {
        return batch;
    }

    let mut extra_fields: Vec<Field> = Vec::with_capacity(groups.len());
    let mut extra_arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(groups.len());

    for (base, members) in &groups {
        let int_col = members
            .iter()
            .find(|(s, _)| *s == "__int")
            .map(|(_, idx)| batch.column(*idx).as_ref());
        let float_col = members
            .iter()
            .find(|(s, _)| *s == "__float")
            .map(|(_, idx)| batch.column(*idx).as_ref());
        let str_col = members
            .iter()
            .find(|(s, _)| *s == "__str")
            .map(|(_, idx)| batch.column(*idx).as_ref());

        let merged = merge_to_utf8(int_col, float_col, str_col, batch.num_rows());
        extra_fields.push(Field::new(*base, DataType::Utf8, true));
        extra_arrays.push(merged);
    }

    // Append the computed bare columns, preserving existing schema metadata.
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| (**f).clone()).collect();
    fields.extend(extra_fields);
    let new_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));

    let mut all_arrays: Vec<Arc<dyn Array>> = batch.columns().to_vec();
    all_arrays.extend(extra_arrays);

    RecordBatch::try_new(new_schema, all_arrays)
        .expect("normalize_conflict_columns: schema/array length mismatch — this is a bug")
}

/// Merge int, float, and str variants into a single `Utf8` column via COALESCE.
///
/// Priority order: int (cast to str) > float (cast to str) > str.
pub(crate) fn merge_to_utf8(
    int_col: Option<&dyn Array>,
    float_col: Option<&dyn Array>,
    str_col: Option<&dyn Array>,
    num_rows: usize,
) -> Arc<dyn Array> {
    use arrow::array::StringArray;
    use arrow::compute;

    let int_s =
        int_col.map(|c| compute::cast(c, &DataType::Utf8).expect("cast int column to Utf8"));
    let float_s =
        float_col.map(|c| compute::cast(c, &DataType::Utf8).expect("cast float column to Utf8"));
    let str_s =
        str_col.map(|c| compute::cast(c, &DataType::Utf8).expect("cast str column to Utf8"));

    let int_arr = int_s
        .as_ref()
        .and_then(|a| a.as_any().downcast_ref::<StringArray>());
    let float_arr = float_s
        .as_ref()
        .and_then(|a| a.as_any().downcast_ref::<StringArray>());
    let str_arr = str_s
        .as_ref()
        .and_then(|a| a.as_any().downcast_ref::<StringArray>());

    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 8);
    for i in 0..num_rows {
        let val = int_arr
            .and_then(|a| (!a.is_null(i)).then(|| a.value(i)))
            .or_else(|| float_arr.and_then(|a| (!a.is_null(i)).then(|| a.value(i))))
            .or_else(|| str_arr.and_then(|a| (!a.is_null(i)).then(|| a.value(i))));
        match val {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};

    use std::collections::HashMap;

    fn make_batch(fields: Vec<Field>, arrays: Vec<Arc<dyn Array>>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    /// Build a batch with `logfwd.conflict_groups` metadata.
    fn make_conflict_batch(
        conflict_groups_meta: &str,
        fields: Vec<Field>,
        arrays: Vec<Arc<dyn Array>>,
    ) -> RecordBatch {
        let mut meta = HashMap::new();
        meta.insert(
            "logfwd.conflict_groups".to_string(),
            conflict_groups_meta.to_string(),
        );
        let schema = Arc::new(Schema::new_with_metadata(fields, meta));
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    #[test]
    fn no_conflict_passthrough() {
        // Clean batch: bare column names, no metadata → return unchanged.
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

    #[test]
    fn single_suffixed_col_not_treated_as_conflict() {
        // A lone `foo__str` column with no metadata must not synthesize a bare `foo`.
        let batch = make_batch(
            vec![Field::new("error__str", DataType::Utf8, true)],
            vec![Arc::new(StringArray::from(vec!["oops"]))],
        );
        let normalized = normalize_conflict_columns(batch.clone());
        assert_eq!(normalized.num_columns(), 1);
        assert!(normalized.column_by_name("error").is_none());
    }

    #[test]
    fn conflict_adds_bare_utf8_column() {
        // Conflict: status__int + status__str → add bare status: Utf8
        let batch = make_conflict_batch(
            "status:int,str",
            vec![
                Field::new("status__int", DataType::Int64, true),
                Field::new("status__str", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![Some(200), None])),
                Arc::new(StringArray::from(vec![None, Some("OK")])),
            ],
        );
        let normalized = normalize_conflict_columns(batch);
        assert_eq!(normalized.num_columns(), 3);

        let status = normalized
            .column_by_name("status")
            .expect("bare status column");
        assert_eq!(*status.data_type(), DataType::Utf8);
        let arr = status.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "200"); // from int
        assert_eq!(arr.value(1), "OK"); // from str
    }

    #[test]
    fn conflict_int_takes_priority_over_str() {
        // When both int and str are non-null for the same row, int wins.
        let batch = make_conflict_batch(
            "x:int,str",
            vec![
                Field::new("x__int", DataType::Int64, true),
                Field::new("x__str", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![Some(42), None])),
                Arc::new(StringArray::from(vec![Some("should-lose"), Some("wins")])),
            ],
        );
        let normalized = normalize_conflict_columns(batch);
        let x = normalized.column_by_name("x").unwrap();
        let arr = x.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "42"); // int wins
        assert_eq!(arr.value(1), "wins"); // str fills in
    }

    #[test]
    fn bare_column_already_present_not_overwritten() {
        // If a bare `status` column already exists (clean batch), do not add another.
        // The metadata lists status as a conflict group but bare name is present.
        let batch = make_conflict_batch(
            "status:int,str",
            vec![
                Field::new("status", DataType::Int64, true),
                Field::new("status__int", DataType::Int64, true),
                Field::new("status__str", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![200i64])),
                Arc::new(Int64Array::from(vec![200i64])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
            ],
        );
        let normalized = normalize_conflict_columns(batch);
        // Should still be 3 columns — bare `status` already present.
        assert_eq!(normalized.num_columns(), 3);
    }

    #[test]
    fn all_null_conflict_group() {
        let batch = make_conflict_batch(
            "v:int,str",
            vec![
                Field::new("v__int", DataType::Int64, true),
                Field::new("v__str", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![Option::<i64>::None, None])),
                Arc::new(StringArray::from(vec![Option::<&str>::None, None])),
            ],
        );
        let normalized = normalize_conflict_columns(batch);
        let v = normalized.column_by_name("v").unwrap();
        let arr = v.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(arr.is_null(0));
        assert!(arr.is_null(1));
    }

    /// Regression: a batch with real user fields `foo__int` and `foo__str` but
    /// NO `logfwd.conflict_groups` metadata must be returned unchanged — no
    /// synthetic `foo` column should be added.
    #[test]
    fn no_metadata_no_false_positive_conflict() {
        // User genuinely has fields named foo__int and foo__str — no conflict.
        let batch = make_batch(
            vec![
                Field::new("foo__int", DataType::Int64, true),
                Field::new("foo__str", DataType::Utf8, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![Some(1i64), Some(2)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        );
        let normalized = normalize_conflict_columns(batch.clone());
        // Must be unchanged — no synthetic `foo` column.
        assert_eq!(normalized.num_columns(), 2);
        assert!(
            normalized.column_by_name("foo").is_none(),
            "normalize must not synthesize a bare 'foo' without conflict metadata"
        );
    }

    #[test]
    fn schema_metadata_preserved_after_normalization() {
        use std::collections::HashMap;
        let mut meta = HashMap::new();
        meta.insert(
            "logfwd.conflict_groups".to_string(),
            "status:int,str".to_string(),
        );
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("status__int", DataType::Int64, true),
                Field::new("status__str", DataType::Utf8, true),
            ],
            meta,
        ));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(200i64), None])) as Arc<dyn Array>,
                Arc::new(StringArray::from(vec![None, Some("OK")])) as Arc<dyn Array>,
            ],
        )
        .unwrap();
        let normalized = normalize_conflict_columns(batch);
        assert_eq!(
            normalized.schema().metadata().get("logfwd.conflict_groups"),
            Some(&"status:int,str".to_string()),
        );
    }
}

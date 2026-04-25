//! Blocklist enrichment processor.
//!
//! Adds two columns to every batch:
//!
//! - `<prefix>_match: Boolean` — `true` if the source column value is listed
//!   in the blocklist.
//! - `<prefix>_category: Utf8` — the category for the matched entry, or `NULL`
//!   for non-matches.
//!
//! The blocklist is loaded from a CSV file at construction time.  The CSV must
//! have:
//! - A `key` column (the value to match against the source column).
//! - An optional `category` column (label for matched entries).
//!
//! This processor is stateless — it adds columns based on a pre-loaded table
//! without buffering any batches.
//!
//! ## Config
//!
//! ```yaml
//! processors:
//!   - type: blocklist
//!     source_column: client_ip
//!     path: /etc/ffwd/blocklist.csv
//!     prefix: bl
//! ```
//!
//! ## CSV format
//!
//! ```csv
//! key,category
//! 1.2.3.4,malware-c2
//! 5.6.7.8,port-scanner
//! bad-user-agent,bot
//! ```
//!
//! The key column is required; `category` is optional.  All comparisons are
//! case-sensitive exact matches.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, BooleanBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use ffwd_output::BatchMetadata;
use smallvec::{SmallVec, smallvec};

use crate::processor::{Processor, ProcessorError};

// ---------------------------------------------------------------------------
// BlocklistProcessor
// ---------------------------------------------------------------------------

/// Stateless processor that tags each row with blocklist match and category
/// columns.
///
/// Adds `{prefix}_match: Boolean` and `{prefix}_category: Utf8` to every
/// output batch.  Non-matched and NULL source values get `false` and `NULL`
/// respectively.
#[derive(Debug)]
pub struct BlocklistProcessor {
    /// Column to look up in the blocklist.
    source_column: String,
    /// Output column name prefix.
    prefix: String,
    /// `key → category` map loaded from the CSV.
    entries: HashMap<String, Option<String>>,
}

impl BlocklistProcessor {
    /// Build from an in-memory CSV.
    ///
    /// # Errors
    ///
    /// Returns an error if the CSV is missing the required `key` header or
    /// contains a parse error.
    pub fn from_reader<R: io::Read>(
        source_column: impl Into<String>,
        prefix: impl Into<String>,
        reader: R,
    ) -> Result<Self, String> {
        let entries = load_blocklist(reader)?;
        Ok(BlocklistProcessor {
            source_column: source_column.into(),
            prefix: prefix.into(),
            entries,
        })
    }

    /// Load from a file path.
    pub fn open(
        source_column: impl Into<String>,
        prefix: impl Into<String>,
        path: &Path,
    ) -> Result<Self, String> {
        let file = std::fs::File::open(path)
            .map_err(|e| format!("blocklist: failed to open '{}': {e}", path.display()))?;
        Self::from_reader(source_column, prefix, io::BufReader::new(file))
    }

    /// Number of entries in the blocklist.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if no entries were loaded.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Processor for BlocklistProcessor {
    fn name(&self) -> &'static str {
        "blocklist"
    }

    fn is_stateful(&self) -> bool {
        false
    }

    fn process(
        &mut self,
        batch: RecordBatch,
        _meta: &BatchMetadata,
    ) -> Result<SmallVec<[RecordBatch; 1]>, ProcessorError> {
        let num_rows = batch.num_rows();

        if num_rows == 0 {
            // Still append columns so downstream sees a consistent schema.
            let enriched = append_columns(batch, 0, &self.prefix, None)?;
            return Ok(smallvec![enriched]);
        }

        // Locate the source column.
        let Some(col) = batch.column_by_name(&self.source_column) else {
            return Err(ProcessorError::Permanent(format!(
                "blocklist: source column '{}' not found in batch",
                self.source_column
            )));
        };

        let mut match_builder = BooleanBuilder::with_capacity(num_rows);
        let mut cat_builder = StringBuilder::with_capacity(num_rows, num_rows * 8);

        // Look up each row directly from the Arrow array — zero per-row allocations.
        match col.data_type() {
            DataType::Utf8 => {
                use arrow::array::AsArray;
                let arr = col.as_string::<i32>();
                for i in 0..num_rows {
                    let key = if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    };
                    append_lookup(&self.entries, key, &mut match_builder, &mut cat_builder);
                }
            }
            DataType::Utf8View => {
                use arrow::array::AsArray;
                let arr = col.as_string_view();
                for i in 0..num_rows {
                    let key = if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    };
                    append_lookup(&self.entries, key, &mut match_builder, &mut cat_builder);
                }
            }
            DataType::LargeUtf8 => {
                use arrow::array::AsArray;
                let arr = col.as_string::<i64>();
                for i in 0..num_rows {
                    let key = if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    };
                    append_lookup(&self.entries, key, &mut match_builder, &mut cat_builder);
                }
            }
            _ => {
                // Unsupported column type — return an error rather than silently
                // treating all rows as non-matches, which would hide schema drift.
                return Err(ProcessorError::Permanent(format!(
                    "blocklist: source_column '{}' has unsupported type {:?} (expected Utf8/Utf8View/LargeUtf8)",
                    self.source_column,
                    col.data_type()
                )));
            }
        }

        let match_col = Arc::new(match_builder.finish());
        let cat_col = Arc::new(cat_builder.finish());

        append_pre_built_columns(
            batch,
            &self.prefix,
            match_col as Arc<dyn Array>,
            cat_col as Arc<dyn Array>,
        )
        .map(|b| smallvec![b])
    }

    fn flush(&mut self) -> SmallVec<[RecordBatch; 1]> {
        SmallVec::new() // stateless — nothing to flush
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Perform a single-row blocklist lookup and append the result.
fn append_lookup(
    entries: &HashMap<String, Option<String>>,
    key: Option<&str>,
    match_builder: &mut BooleanBuilder,
    cat_builder: &mut StringBuilder,
) {
    match key.and_then(|k| entries.get(k)) {
        Some(cat) => {
            match_builder.append_value(true);
            match cat {
                Some(c) => cat_builder.append_value(c.as_str()),
                None => cat_builder.append_null(),
            }
        }
        None => {
            match_builder.append_value(false);
            cat_builder.append_null();
        }
    }
}

/// Append `{prefix}_match = all-false` and `{prefix}_category = all-NULL` columns.
fn append_columns(
    batch: RecordBatch,
    num_rows: usize,
    prefix: &str,
    _source: Option<&Arc<dyn Array>>,
) -> Result<RecordBatch, ProcessorError> {
    let mut match_builder = BooleanBuilder::with_capacity(num_rows);
    let mut cat_builder = StringBuilder::with_capacity(num_rows, 0);
    for _ in 0..num_rows {
        match_builder.append_value(false);
        cat_builder.append_null();
    }
    append_pre_built_columns(
        batch,
        prefix,
        Arc::new(match_builder.finish()) as Arc<dyn Array>,
        Arc::new(cat_builder.finish()) as Arc<dyn Array>,
    )
}

fn append_pre_built_columns(
    batch: RecordBatch,
    prefix: &str,
    match_col: Arc<dyn Array>,
    cat_col: Arc<dyn Array>,
) -> Result<RecordBatch, ProcessorError> {
    let match_name = format!("{prefix}_match");
    let cat_name = format!("{prefix}_category");

    let schema = batch.schema();
    if schema.column_with_name(&match_name).is_some() {
        return Err(ProcessorError::Permanent(format!(
            "blocklist: output column '{match_name}' already exists in batch"
        )));
    }
    if schema.column_with_name(&cat_name).is_some() {
        return Err(ProcessorError::Permanent(format!(
            "blocklist: output column '{cat_name}' already exists in batch"
        )));
    }

    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(&match_name, DataType::Boolean, false)));
    fields.push(Arc::new(Field::new(&cat_name, DataType::Utf8, true)));

    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));

    let mut columns: Vec<Arc<dyn Array>> = batch.columns().to_vec();
    columns.push(match_col);
    columns.push(cat_col);

    RecordBatch::try_new(schema, columns)
        .map_err(|e| ProcessorError::Permanent(format!("blocklist: failed to build batch: {e}")))
}

/// Read the blocklist CSV into a `HashMap<key, Option<category>>`.
fn load_blocklist<R: io::Read>(reader: R) -> Result<HashMap<String, Option<String>>, String> {
    let mut csv = csv::ReaderBuilder::new().flexible(true).from_reader(reader);

    let headers: Vec<String> = csv
        .headers()
        .map_err(|e| format!("blocklist CSV header error: {e}"))?
        .iter()
        .map(|h| h.trim().to_lowercase())
        .collect();

    let key_idx = headers
        .iter()
        .position(|h| h == "key")
        .ok_or_else(|| "blocklist CSV missing required 'key' column".to_string())?;

    let cat_idx = headers.iter().position(|h| h == "category");

    let mut entries: HashMap<String, Option<String>> = HashMap::new();

    for (row_num, record) in csv.records().enumerate() {
        let record =
            record.map_err(|e| format!("blocklist CSV parse error at row {}: {e}", row_num + 1))?;

        let key = record.get(key_idx).unwrap_or("").trim().to_owned();
        if key.is_empty() {
            continue;
        }

        let category = cat_idx
            .and_then(|i| record.get(i))
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned);

        if let Some(existing) = entries.get(&key) {
            if existing != &category {
                return Err(format!(
                    "blocklist CSV duplicate key '{}' at row {} with conflicting category",
                    key,
                    row_num + 1
                ));
            }
            // Same key, same category — skip duplicate.
            continue;
        }
        entries.insert(key, category);
    }

    Ok(entries)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BooleanArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use ffwd_output::BatchMetadata;
    use std::sync::Arc;

    fn meta() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 0,
        }
    }

    fn make_batch(ips: &[Option<&str>]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("ip", DataType::Utf8, true)]));
        let arr: StringArray = ips.iter().map(|s| *s).collect();
        RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
    }

    const CSV: &[u8] = b"key,category\n1.2.3.4,malware-c2\n5.6.7.8,port-scanner\nbad-bot,\n";

    #[test]
    fn load_from_reader() {
        let proc = BlocklistProcessor::from_reader("ip", "bl", CSV).unwrap();
        assert_eq!(proc.len(), 3);
    }

    #[test]
    fn match_adds_columns() {
        let mut proc = BlocklistProcessor::from_reader("ip", "bl", CSV).unwrap();
        let batch = make_batch(&[Some("1.2.3.4"), Some("8.8.8.8"), None]);
        let output = proc.process(batch, &meta()).unwrap();
        assert_eq!(output.len(), 1);
        let out = &output[0];

        let matches = out
            .column_by_name("bl_match")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(matches.value(0)); // matched
        assert!(!matches.value(1)); // miss
        assert!(!matches.value(2)); // NULL source → miss

        let cats = out
            .column_by_name("bl_category")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(cats.value(0), "malware-c2");
        assert!(cats.is_null(1));
        assert!(cats.is_null(2));
    }

    #[test]
    fn no_category_column_is_null() {
        let csv = b"key\n1.2.3.4\n5.6.7.8\n";
        let mut proc = BlocklistProcessor::from_reader("ip", "bl", &csv[..]).unwrap();
        let batch = make_batch(&[Some("1.2.3.4")]);
        let out = &proc.process(batch, &meta()).unwrap()[0];

        let matches = out
            .column_by_name("bl_match")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(matches.value(0));

        let cats = out
            .column_by_name("bl_category")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(cats.is_null(0));
    }

    #[test]
    fn source_column_absent_returns_error() {
        let mut proc = BlocklistProcessor::from_reader("missing_col", "bl", CSV).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));
        let arr: StringArray = vec![Some("1.2.3.4")].into_iter().collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let result = proc.process(batch, &meta());
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("source column"), "got: {msg}");
    }

    #[test]
    fn empty_batch_passed_through() {
        let mut proc = BlocklistProcessor::from_reader("ip", "bl", CSV).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("ip", DataType::Utf8, true)]));
        let values: Vec<Option<&str>> = vec![];
        let arr: StringArray = values.into_iter().collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let out = proc.process(batch, &meta()).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 0);
    }

    #[test]
    fn missing_key_column_returns_error() {
        let csv = b"value,category\n1.2.3.4,bad\n";
        let result = BlocklistProcessor::from_reader("ip", "bl", &csv[..]);
        assert!(result.is_err());
    }

    #[test]
    fn matched_entry_with_empty_category_is_null() {
        let mut proc = BlocklistProcessor::from_reader("ip", "bl", CSV).unwrap();
        let batch = make_batch(&[Some("bad-bot")]);
        let out = &proc.process(batch, &meta()).unwrap()[0];

        let matches = out
            .column_by_name("bl_match")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(matches.value(0));

        let cats = out
            .column_by_name("bl_category")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(cats.is_null(0), "empty category should be NULL");
    }
}

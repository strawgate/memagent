use std::io;
use std::sync::Arc;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;

use logfwd_types::diagnostics::ComponentStats;

use super::{Compression, build_col_infos, str_value, write_row_json};

// ---------------------------------------------------------------------------
// JsonLinesSink
// ---------------------------------------------------------------------------

/// Writes newline-delimited JSON and POSTs over HTTP.
#[allow(dead_code)] // HTTP send fields retained for future async Sink migration.
pub struct JsonLinesSink {
    name: String,
    url: String,
    headers: Vec<(String, String)>,
    pub(crate) batch_buf: Vec<u8>,
    compress_buf: Vec<u8>,
    compression: Compression,
    http_agent: ureq::Agent,
    stats: Arc<ComponentStats>,
}

impl JsonLinesSink {
    pub fn new(
        name: String,
        url: String,
        headers: Vec<(String, String)>,
        compression: Compression,
        stats: Arc<ComponentStats>,
    ) -> Self {
        let http_agent = ureq::config::Config::builder()
            .timeout_global(Some(std::time::Duration::from_secs(30)))
            .build()
            .new_agent();
        JsonLinesSink {
            name,
            url,
            headers,
            batch_buf: Vec::with_capacity(64 * 1024),
            compress_buf: Vec::new(),
            compression,
            http_agent,
            stats,
        }
    }

    /// Check whether the batch is "raw passthrough eligible": has a `_raw` column
    /// and every other column is null for every row (no transforms modified fields).
    fn is_raw_passthrough(batch: &RecordBatch) -> bool {
        let schema = batch.schema();
        let has_raw = schema.fields().iter().any(|f| f.name() == "_raw");
        if !has_raw {
            return false;
        }
        // Simple heuristic: if the only non-null column is _raw, passthrough.
        // Use enumerated iteration to avoid `index_of` which could panic on
        // schema inconsistency (issue #317).
        for (idx, field) in schema.fields().iter().enumerate() {
            if field.name() == "_raw" {
                continue;
            }
            if batch.column(idx).null_count() < batch.num_rows() {
                return false;
            }
        }
        true
    }

    /// Serialize the batch into `self.batch_buf` as newline-delimited JSON.
    ///
    /// Returns `Err` if the schema claims `_raw` passthrough but the column is
    /// unexpectedly absent — this indicates a logic error and should not be
    /// silently swallowed (issue #317).
    pub fn serialize_batch(&mut self, batch: &RecordBatch) -> io::Result<()> {
        self.batch_buf.clear();
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        if Self::is_raw_passthrough(batch) {
            // Fast path: memcpy _raw values directly.
            // Use `position` instead of `index_of().expect(...)` to avoid panicking
            // if the schema is unexpectedly inconsistent (issue #317).
            let raw_idx = batch
                .schema()
                .fields()
                .iter()
                .position(|f| f.name() == "_raw")
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "_raw column absent despite passthrough eligibility check",
                    )
                })?;
            let col = batch.column(raw_idx);
            for row in 0..num_rows {
                if !col.is_null(row) {
                    self.batch_buf
                        .extend_from_slice(str_value(col, row).as_bytes());
                    self.batch_buf.push(b'\n');
                }
            }
        } else {
            let cols = build_col_infos(batch);
            for row in 0..num_rows {
                write_row_json(batch, row, &cols, &mut self.batch_buf)?;
                self.batch_buf.push(b'\n');
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use logfwd_types::diagnostics::ComponentStats;

    fn make_sink() -> JsonLinesSink {
        JsonLinesSink::new(
            "test".to_string(),
            "http://localhost:1".to_string(), // unreachable — tests only call serialize_batch
            vec![],
            Compression::None,
            Arc::new(ComponentStats::default()),
        )
    }

    fn batch_with_raw_only(raw_values: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("_raw", DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(raw_values))]).unwrap()
    }

    fn batch_raw_plus_nulls(raw_values: Vec<&str>) -> RecordBatch {
        let n = raw_values.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("_raw", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true), // will be all-null
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(raw_values)),
                Arc::new(StringArray::from(vec![None::<&str>; n])), // all null
            ],
        )
        .unwrap()
    }

    fn batch_raw_with_non_null_col() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_raw", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["raw data"])),
                Arc::new(StringArray::from(vec!["ERROR"])), // not null — blocks passthrough
            ],
        )
        .unwrap()
    }

    fn batch_no_raw() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["hello"]))]).unwrap()
    }

    // --- is_raw_passthrough ---

    #[test]
    fn passthrough_raw_only_column() {
        let batch = batch_with_raw_only(vec!["line 1", "line 2"]);
        assert!(JsonLinesSink::is_raw_passthrough(&batch));
    }

    #[test]
    fn passthrough_raw_plus_all_null_cols() {
        let batch = batch_raw_plus_nulls(vec!["line 1", "line 2"]);
        assert!(JsonLinesSink::is_raw_passthrough(&batch));
    }

    #[test]
    fn no_passthrough_when_no_raw_col() {
        let batch = batch_no_raw();
        assert!(!JsonLinesSink::is_raw_passthrough(&batch));
    }

    #[test]
    fn no_passthrough_when_non_null_col_present() {
        let batch = batch_raw_with_non_null_col();
        assert!(!JsonLinesSink::is_raw_passthrough(&batch));
    }

    #[test]
    fn no_passthrough_empty_schema() {
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(schema);
        assert!(!JsonLinesSink::is_raw_passthrough(&batch));
    }

    // --- serialize_batch: raw passthrough path ---

    #[test]
    fn serialize_passthrough_copies_raw_verbatim() {
        let mut sink = make_sink();
        let batch = batch_with_raw_only(vec![r#"{"a":1}"#, r#"{"b":2}"#]);
        sink.serialize_batch(&batch).unwrap();
        let out = std::str::from_utf8(&sink.batch_buf).unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines, vec![r#"{"a":1}"#, r#"{"b":2}"#]);
    }

    #[test]
    fn serialize_passthrough_raw_plus_null_cols() {
        let mut sink = make_sink();
        let batch = batch_raw_plus_nulls(vec!["raw-line-1", "raw-line-2"]);
        sink.serialize_batch(&batch).unwrap();
        let out = std::str::from_utf8(&sink.batch_buf).unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines, vec!["raw-line-1", "raw-line-2"]);
    }

    // --- serialize_batch: JSON serialization path ---

    #[test]
    fn serialize_json_path_produces_valid_ndjson() {
        let mut sink = make_sink();
        let batch = batch_raw_with_non_null_col();
        sink.serialize_batch(&batch).unwrap();
        let out = std::str::from_utf8(&sink.batch_buf).unwrap();
        // Each line must be a valid JSON object ending with \n
        for line in out.lines() {
            assert!(line.starts_with('{'), "expected JSON object, got: {line}");
            assert!(line.ends_with('}'));
        }
        assert!(
            out.contains("ERROR"),
            "field value must appear in JSON output"
        );
    }

    // --- serialize_batch: edge cases ---

    #[test]
    fn serialize_empty_batch_produces_no_output() {
        let mut sink = make_sink();
        let schema = Arc::new(Schema::new(vec![Field::new("_raw", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(Vec::<&str>::new()))],
        )
        .unwrap();
        sink.serialize_batch(&batch).unwrap();
        assert!(sink.batch_buf.is_empty());
    }

    #[test]
    fn serialize_clear_buf_between_calls() {
        // buf from previous call must not leak into next call
        let mut sink = make_sink();
        sink.serialize_batch(&batch_with_raw_only(vec!["first"]))
            .unwrap();
        assert!(!sink.batch_buf.is_empty());
        let empty_schema = Arc::new(Schema::new(vec![Field::new("_raw", DataType::Utf8, true)]));
        let empty_batch = RecordBatch::try_new(
            empty_schema,
            vec![Arc::new(StringArray::from(Vec::<&str>::new()))],
        )
        .unwrap();
        sink.serialize_batch(&empty_batch).unwrap();
        assert!(
            sink.batch_buf.is_empty(),
            "buf should be cleared between calls"
        );
    }
}

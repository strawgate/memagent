use std::borrow::Cow;
use std::future::Future;
use std::io::{self, Write};
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use tokio::io::AsyncWriteExt;

use logfwd_types::diagnostics::ComponentStats;
use logfwd_types::field_names;

use super::sink::{SendResult, Sink, SinkFactory};
use arrow::util::display::array_value_to_string;

use super::{
    BatchMetadata, ColVariant, build_col_infos, get_array, is_null, str_value, write_json_value,
    write_row_json,
};

// ---------------------------------------------------------------------------
// StdoutSink
// ---------------------------------------------------------------------------

/// Output format for StdoutSink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StdoutFormat {
    Json,
    Text,
    /// Human-readable colored output for debugging/testing.
    Console,
}

/// Writes log records to stdout, one per line.
///
/// Implements the async [`Sink`] trait using `tokio::io::stdout()`.
pub struct StdoutSink {
    name: String,
    format: StdoutFormat,
    buf: Vec<u8>,
    /// Reusable buffer for collecting the full batch output before writing to
    /// async stdout.  Separate from `buf` which is used as row-level scratch
    /// space inside `write_batch_to`.
    output_buf: Vec<u8>,
    color: bool,
    stats: Arc<ComponentStats>,
}

impl StdoutSink {
    pub fn new(name: String, format: StdoutFormat, stats: Arc<ComponentStats>) -> Self {
        let color = format == StdoutFormat::Console
            && std::env::var_os("NO_COLOR").is_none()
            // SAFETY: isatty is a simple query on a well-known fd; no invariants to uphold.
            && unsafe { libc::isatty(libc::STDOUT_FILENO) != 0 };
        StdoutSink {
            name,
            format,
            buf: Vec::with_capacity(8192),
            output_buf: Vec::with_capacity(8192),
            color,
            stats,
        }
    }

    /// Write into an arbitrary `Write` destination (useful for testing).
    pub fn write_batch_to<W: Write>(
        &mut self,
        batch: &RecordBatch,
        _metadata: &BatchMetadata,
        dest: &mut W,
    ) -> io::Result<()> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        match self.format {
            StdoutFormat::Text => {
                // Try to find _raw column.
                let raw_idx = batch
                    .schema()
                    .fields()
                    .iter()
                    .position(|f| f.name() == "_raw");
                if let Some(idx) = raw_idx {
                    let col = batch.column(idx);
                    for row in 0..num_rows {
                        if !col.is_null(row) {
                            dest.write_all(str_value(col, row).as_bytes())?;
                            dest.write_all(b"\n")?;
                        }
                    }
                } else {
                    // No _raw column — fall back to JSON and warn once.
                    static WARNED: std::sync::atomic::AtomicBool =
                        std::sync::atomic::AtomicBool::new(false);
                    if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                        tracing::warn!(
                            sink = %self.name,
                            "stdout/file 'text' format requires a '_raw' column in the output \
                             batch; falling back to JSON. Add _raw to your SQL SELECT or use \
                             format: json."
                        );
                    }
                    let cols = build_col_infos(batch);
                    for row in 0..num_rows {
                        self.buf.clear();
                        write_row_json(batch, row, &cols, &mut self.buf)?;
                        self.buf.push(b'\n');
                        dest.write_all(&self.buf)?;
                    }
                }
            }
            StdoutFormat::Json => {
                let cols = build_col_infos(batch);
                for row in 0..num_rows {
                    self.buf.clear();
                    write_row_json(batch, row, &cols, &mut self.buf)?;
                    self.buf.push(b'\n');
                    dest.write_all(&self.buf)?;
                }
            }
            StdoutFormat::Console => {
                self.write_console(batch, dest)?;
            }
        }
        Ok(())
    }

    /// Serialize a batch into `self.output_buf`.
    ///
    /// This avoids writing row-by-row to the async writer; instead the whole
    /// batch is rendered into the reusable output buffer and then flushed in a
    /// single `write_all` to `tokio::io::Stdout`.  `self.buf` is used as
    /// row-level scratch space by `write_batch_to`, so the output buffer is
    /// temporarily taken out to avoid aliasing borrows.
    fn serialize_batch(&mut self, batch: &RecordBatch, metadata: &BatchMetadata) -> io::Result<()> {
        let mut output = std::mem::take(&mut self.output_buf);
        output.clear();
        let result = self.write_batch_to(batch, metadata, &mut output);
        self.output_buf = output;
        result
    }

    fn write_console<W: Write>(&mut self, batch: &RecordBatch, dest: &mut W) -> io::Result<()> {
        let schema = batch.schema();
        let fields = schema.fields();

        // Resolve canonical names first, then variants, then conflict suffixes.
        let ts_idx = find_preferred_column(
            fields,
            field_names::TIMESTAMP,
            field_names::TIMESTAMP_VARIANTS,
            &[],
        );
        let level_idx = find_preferred_column(
            fields,
            field_names::SEVERITY,
            field_names::SEVERITY_VARIANTS,
            &[],
        );
        let msg_idx = find_preferred_column(
            fields,
            field_names::BODY,
            field_names::BODY_VARIANTS,
            &[field_names::RAW],
        );

        let cols = build_col_infos(batch);

        for row in 0..batch.num_rows() {
            self.buf.clear();

            // Timestamp (dim).
            if let Some(idx) = ts_idx {
                let col = batch.column(idx);
                if !col.is_null(row) {
                    let ts = safe_col_to_string(col, row);
                    // Show just the time portion if it's a full ISO timestamp.
                    let short = ts.find('T').map_or(ts.as_ref(), |i| &ts[i + 1..]);
                    if self.color {
                        self.buf.extend_from_slice(b"\x1b[2m");
                    }
                    self.buf.extend_from_slice(short.as_bytes());
                    if self.color {
                        self.buf.extend_from_slice(b"\x1b[0m");
                    }
                    self.buf.extend_from_slice(b"  ");
                }
            }

            // Level (colored).
            if let Some(idx) = level_idx {
                let col = batch.column(idx);
                if !col.is_null(row) {
                    let level = safe_col_to_string(col, row);
                    if self.color {
                        let color = match level.as_ref() {
                            "ERROR" => "\x1b[1;31m", // bold red
                            "WARN" => "\x1b[33m",    // yellow
                            "INFO" => "\x1b[32m",    // green
                            "DEBUG" => "\x1b[2m",    // dim
                            _ => "",
                        };
                        self.buf.extend_from_slice(color.as_bytes());
                    }
                    // Pad to 5 chars for alignment.
                    write!(self.buf, "{:<5}", level)?;
                    if self.color {
                        self.buf.extend_from_slice(b"\x1b[0m");
                    }
                    self.buf.extend_from_slice(b"  ");
                }
            }

            // Message.
            if let Some(idx) = msg_idx {
                let col = batch.column(idx);
                if !col.is_null(row) {
                    let msg = safe_col_to_string(col, row);
                    self.buf.extend_from_slice(msg.as_bytes());
                }
            }

            // Remaining fields as key=value pairs (dim).
            let mut has_extra = false;
            for col in &cols {
                // Skip the well-known columns and _raw. Check ALL variant
                // indices — find_col may have matched a different variant
                // (e.g. message_str vs message_int).
                let col_matches_well_known = col.json_variants.iter().any(|v| match v {
                    ColVariant::Flat { col_idx, .. } => {
                        Some(*col_idx) == ts_idx
                            || Some(*col_idx) == level_idx
                            || Some(*col_idx) == msg_idx
                    }
                    ColVariant::StructField { .. } => false,
                });
                if col.field_name == "_raw" || col_matches_well_known {
                    continue;
                }

                // Find first non-null variant for this field in this row.
                let Some(winner) = col.json_variants.iter().find(|v| !is_null(batch, v, row))
                else {
                    continue;
                };
                let Some(arr) = get_array(batch, winner) else {
                    continue;
                };

                if has_extra {
                    self.buf.push(b' ');
                } else {
                    self.buf.extend_from_slice(b"  ");
                    has_extra = true;
                }

                if self.color {
                    self.buf.extend_from_slice(b"\x1b[2m");
                }
                self.buf.extend_from_slice(col.field_name.as_bytes());
                self.buf.push(b'=');

                // Dispatch on Arrow DataType, not column name suffix.
                match arr.data_type() {
                    DataType::Int64 => {
                        let v = arr.as_primitive::<arrow::datatypes::Int64Type>().value(row);
                        self.buf
                            .extend_from_slice(itoa::Buffer::new().format(v).as_bytes());
                    }
                    DataType::Float64 => {
                        let v = arr
                            .as_primitive::<arrow::datatypes::Float64Type>()
                            .value(row);
                        if v.is_finite() {
                            self.buf
                                .extend_from_slice(ryu::Buffer::new().format_finite(v).as_bytes());
                        } else {
                            self.buf.extend_from_slice(v.to_string().as_bytes());
                        }
                    }
                    DataType::Struct(_) => {
                        // Encode structs (e.g. grok() or geo_lookup() results) as
                        // inline JSON so the values are visible in console output.
                        // Without this arm, str_value() returns "" for any Struct,
                        // silently discarding parsed data (#1620).
                        write_json_value(arr, row, &mut self.buf)?;
                    }
                    _ => {
                        let s = safe_array_value_to_string(arr, row);
                        self.buf.extend_from_slice(s.as_bytes());
                    }
                }

                if self.color {
                    self.buf.extend_from_slice(b"\x1b[0m");
                }
            }

            self.buf.push(b'\n');
            dest.write_all(&self.buf)?;
        }
        Ok(())
    }
}

/// Safely convert any Arrow column value to a display string.
///
/// Unlike `str_value` (which panics on non-string types), this handles all
/// Arrow data types via `array_value_to_string` for non-Utf8 columns.
fn safe_col_to_string(col: &dyn Array, row: usize) -> Cow<'_, str> {
    match col.data_type() {
        DataType::Utf8 => Cow::Borrowed(col.as_string::<i32>().value(row)),
        DataType::Utf8View => Cow::Borrowed(col.as_string_view().value(row)),
        DataType::LargeUtf8 => Cow::Borrowed(col.as_string::<i64>().value(row)),
        _ => Cow::Owned(safe_array_value_to_string(col, row)),
    }
}

fn find_preferred_column(
    fields: &arrow::datatypes::Fields,
    canonical: &str,
    variants: &[&str],
    extra_fallback: &[&str],
) -> Option<usize> {
    if let Some(idx) = find_exact_column(fields, canonical) {
        return Some(idx);
    }
    for &name in variants {
        if let Some(idx) = find_exact_column(fields, name) {
            return Some(idx);
        }
    }
    for &name in extra_fallback {
        if let Some(idx) = find_exact_column(fields, name) {
            return Some(idx);
        }
    }

    for base in std::iter::once(canonical)
        .chain(variants.iter().copied())
        .chain(extra_fallback.iter().copied())
    {
        let conflict_str = format!("{base}__str");
        if let Some(idx) = find_exact_column(fields, conflict_str.as_str()) {
            return Some(idx);
        }
    }

    None
}

fn find_exact_column(fields: &arrow::datatypes::Fields, name: &str) -> Option<usize> {
    fields.iter().position(|f| f.name() == name)
}

/// Convert an Arrow value to string without panicking or silently erasing errors.
fn safe_array_value_to_string(col: &dyn Array, row: usize) -> String {
    match array_value_to_string(col, row) {
        Ok(s) => s,
        Err(e) => {
            tracing::debug!(error = %e, dtype = ?col.data_type(), row, "stdout: value formatting failed");
            "<format_error>".to_string()
        }
    }
}

// ---------------------------------------------------------------------------
// Async Sink implementation
// ---------------------------------------------------------------------------

impl Sink for StdoutSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        Box::pin(async move {
            // Serialize the entire batch into the reusable output buffer
            // synchronously (CPU work, no I/O — fine on an async task).
            if let Err(e) = self.serialize_batch(batch, metadata) {
                return SendResult::IoError(e);
            }

            let bytes_written = self.output_buf.len() as u64;

            // Write the pre-rendered buffer to async stdout in one shot.
            let mut stdout = tokio::io::stdout();
            if let Err(e) = stdout.write_all(&self.output_buf).await {
                return SendResult::IoError(e);
            }
            if let Err(e) = stdout.flush().await {
                return SendResult::IoError(e);
            }

            self.stats.inc_lines(batch.num_rows() as u64);
            self.stats.inc_bytes(bytes_written);
            SendResult::Ok
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { tokio::io::stdout().flush().await })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        // Stdout has no connection or session to tear down; flushing any
        // buffered data is the only meaningful cleanup.
        Box::pin(async { tokio::io::stdout().flush().await })
    }
}

// ---------------------------------------------------------------------------
// StdoutSinkFactory
// ---------------------------------------------------------------------------

/// Factory that creates stdout sink instances for the output worker pool.
///
/// Because stdout is a single shared resource, this factory is single-use:
/// only one worker should write to stdout at a time.
pub struct StdoutSinkFactory {
    name: String,
    format: StdoutFormat,
    stats: Arc<ComponentStats>,
}

impl StdoutSinkFactory {
    /// Create a new factory for stdout sinks.
    pub fn new(name: String, format: StdoutFormat, stats: Arc<ComponentStats>) -> Self {
        StdoutSinkFactory {
            name,
            format,
            stats,
        }
    }
}

impl SinkFactory for StdoutSinkFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
        Ok(Box::new(StdoutSink::new(
            self.name.clone(),
            self.format,
            Arc::clone(&self.stats),
        )))
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn is_single_use(&self) -> bool {
        true
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

    fn make_metadata() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::default(),
            observed_time_ns: 0,
        }
    }

    /// A batch with a `_raw` column should be written as plain text lines.
    #[test]
    fn text_format_with_raw_column_writes_raw_lines() {
        let schema = Arc::new(Schema::new(vec![Field::new("_raw", DataType::Utf8, true)]));
        let raw = StringArray::from(vec![Some("line one"), Some("line two")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(raw)]).unwrap();

        let mut sink = StdoutSink::new(
            "test-raw".to_string(),
            StdoutFormat::Text,
            Arc::new(ComponentStats::new()),
        );
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &make_metadata(), &mut out)
            .unwrap();

        let output = String::from_utf8(out).unwrap();
        assert_eq!(output, "line one\nline two\n");
    }

    /// A batch *without* a `_raw` column should fall back to JSON output so
    /// existing behaviour is preserved.
    #[test]
    fn text_format_without_raw_column_falls_back_to_json() {
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
        let msg = StringArray::from(vec![Some("hello"), Some("world")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(msg)]).unwrap();

        let mut sink = StdoutSink::new(
            "test-fallback".to_string(),
            StdoutFormat::Text,
            Arc::new(ComponentStats::new()),
        );
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &make_metadata(), &mut out)
            .unwrap();

        let output = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 2, "expected two JSON lines, got: {output:?}");
        // Each line must be valid JSON containing the msg field.
        let row0: serde_json::Value = serde_json::from_str(lines[0]).expect("row 0 is valid JSON");
        let row1: serde_json::Value = serde_json::from_str(lines[1]).expect("row 1 is valid JSON");
        assert_eq!(row0["msg"], "hello");
        assert_eq!(row1["msg"], "world");
    }

    /// The JSON fallback output for text format must match what the explicit
    /// JSON format would produce for the same batch.
    #[test]
    fn text_format_fallback_output_matches_json_format() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));
        let col = StringArray::from(vec![Some("value")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();
        let meta = make_metadata();

        let mut text_sink = StdoutSink::new(
            "text".to_string(),
            StdoutFormat::Text,
            Arc::new(ComponentStats::new()),
        );
        let mut json_sink = StdoutSink::new(
            "json".to_string(),
            StdoutFormat::Json,
            Arc::new(ComponentStats::new()),
        );

        let mut text_out: Vec<u8> = Vec::new();
        let mut json_out: Vec<u8> = Vec::new();
        text_sink
            .write_batch_to(&batch, &meta, &mut text_out)
            .unwrap();
        json_sink
            .write_batch_to(&batch, &meta, &mut json_out)
            .unwrap();

        assert_eq!(
            text_out, json_out,
            "text fallback should be identical to json format"
        );
    }

    /// Regression: console format with a Boolean column must not panic.
    /// Before the fix, the extra-fields rendering hit `unreachable!()` for
    /// non-string, non-numeric types.
    #[test]
    fn console_format_boolean_column_does_not_panic() {
        use arrow::array::BooleanArray;

        let schema = Arc::new(Schema::new(vec![
            Field::new("message", DataType::Utf8, true),
            Field::new("is_error", DataType::Boolean, true),
        ]));
        let msg = StringArray::from(vec![Some("hello")]);
        let flag = BooleanArray::from(vec![Some(true)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(msg), Arc::new(flag)]).unwrap();

        let mut sink = StdoutSink::new(
            "test-bool".to_string(),
            StdoutFormat::Console,
            Arc::new(ComponentStats::new()),
        );
        let mut out: Vec<u8> = Vec::new();
        // Must not panic.
        sink.write_batch_to(&batch, &make_metadata(), &mut out)
            .unwrap();
        let output = String::from_utf8(out).unwrap();
        assert!(
            output.contains("is_error=true"),
            "boolean value should appear in output: {output:?}"
        );
    }

    /// Regression: console format must recognise `_timestamp` as a timestamp
    /// column. Before the fix, `_timestamp` was missing from the `find_col`
    /// name list, so the timestamp was omitted from console output.
    #[test]
    fn console_format_underscore_timestamp_shown() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_timestamp", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let ts = StringArray::from(vec![Some("2024-01-15T10:30:00Z")]);
        let msg = StringArray::from(vec![Some("hello")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(msg)]).unwrap();

        let mut sink = StdoutSink::new(
            "test-ts".to_string(),
            StdoutFormat::Console,
            Arc::new(ComponentStats::new()),
        );
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &make_metadata(), &mut out)
            .unwrap();
        let output = String::from_utf8(out).unwrap();
        assert!(
            output.contains("10:30:00Z"),
            "_timestamp value should appear in console output: {output:?}"
        );
    }

    /// Regression: console format with `_raw` as the only content column must
    /// show the content. Before the fix, `_raw` was not in the `find_col`
    /// message variant list, producing empty console lines.
    #[test]
    fn console_format_raw_as_message_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("_raw", DataType::Utf8, true)]));
        let raw = StringArray::from(vec![Some("raw log line here")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(raw)]).unwrap();

        let mut sink = StdoutSink::new(
            "test-raw-console".to_string(),
            StdoutFormat::Console,
            Arc::new(ComponentStats::new()),
        );
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &make_metadata(), &mut out)
            .unwrap();
        let output = String::from_utf8(out).unwrap();
        assert!(
            output.contains("raw log line here"),
            "_raw content should appear in console output: {output:?}"
        );
    }

    #[test]
    fn console_format_utf8view_message_column() {
        use arrow::array::StringViewArray;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "message",
            DataType::Utf8View,
            true,
        )]));
        let msg = StringViewArray::from(vec![Some("view log line")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(msg)]).unwrap();

        let mut sink = StdoutSink::new(
            "test-utf8view-console".to_string(),
            StdoutFormat::Console,
            Arc::new(ComponentStats::new()),
        );
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &make_metadata(), &mut out)
            .unwrap();
        let output = String::from_utf8(out).unwrap();
        assert!(
            output.contains("view log line"),
            "Utf8View message should appear in console output: {output:?}"
        );
    }

    #[test]
    fn console_format_largeutf8_message_column() {
        use arrow::array::LargeStringArray;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "message",
            DataType::LargeUtf8,
            true,
        )]));
        let msg = LargeStringArray::from(vec![Some("large log line")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(msg)]).unwrap();

        let mut sink = StdoutSink::new(
            "test-largeutf8-console".to_string(),
            StdoutFormat::Console,
            Arc::new(ComponentStats::new()),
        );
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &make_metadata(), &mut out)
            .unwrap();
        let output = String::from_utf8(out).unwrap();
        assert!(
            output.contains("large log line"),
            "LargeUtf8 message should appear in console output: {output:?}"
        );
    }

    #[test]
    fn console_format_prefers_canonical_message_over_raw() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(field_names::RAW, DataType::Utf8, true),
            Field::new(field_names::BODY, DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("raw line")])),
                Arc::new(StringArray::from(vec![Some("parsed message")])),
            ],
        )
        .expect("valid batch");

        let mut sink = StdoutSink::new(
            "test-canonical-message".to_string(),
            StdoutFormat::Console,
            Arc::new(ComponentStats::new()),
        );
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &make_metadata(), &mut out)
            .expect("console write");
        let output = String::from_utf8(out).expect("utf8");
        assert!(
            output.contains("parsed message"),
            "canonical message must win when message and _raw are both present: {output:?}"
        );
        assert!(
            !output.contains("raw line"),
            "_raw must not shadow canonical message: {output:?}"
        );
    }

    #[test]
    fn console_format_prefers_canonical_timestamp_over_variant() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(field_names::TIMESTAMP_UNDERSCORE, DataType::Utf8, true),
            Field::new(field_names::TIMESTAMP, DataType::Utf8, true),
            Field::new(field_names::BODY, DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("2024-01-01T00:00:00Z")])),
                Arc::new(StringArray::from(vec![Some("2025-01-01T01:02:03Z")])),
                Arc::new(StringArray::from(vec![Some("hello")])),
            ],
        )
        .expect("valid batch");

        let mut sink = StdoutSink::new(
            "test-canonical-timestamp".to_string(),
            StdoutFormat::Console,
            Arc::new(ComponentStats::new()),
        );
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &make_metadata(), &mut out)
            .expect("console write");
        let output = String::from_utf8(out).expect("utf8");
        assert!(
            output.starts_with("01:02:03Z  hello"),
            "canonical timestamp must be selected before _timestamp: {output:?}"
        );
        assert!(
            !output.starts_with("00:00:00Z"),
            "_timestamp variant must not shadow canonical timestamp in leading slot: {output:?}"
        );
    }
}

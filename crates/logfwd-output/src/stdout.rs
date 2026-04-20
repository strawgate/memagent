use std::borrow::Cow;
use std::future::Future;
use std::io::{self, IsTerminal, Write};
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use tokio::io::AsyncWriteExt;

use memchr::memchr_iter;

use logfwd_types::diagnostics::ComponentStats;
use logfwd_types::field_names;

use super::sink::{SendResult, Sink, SinkFactory};
use arrow::util::display::array_value_to_string;

use super::{
    BatchMetadata, ColVariant, build_col_infos, get_array, is_null, resolve_col_infos,
    write_json_value, write_row_json_resolved,
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
    message_field: String,
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
        Self::with_message_field(name, format, field_names::BODY.to_string(), stats)
    }

    /// Create a sink with a custom text/console message field fallback.
    ///
    /// The sink always prefers canonical `body` when present, then falls back
    /// to this configured field, then legacy aliases (`message`, `msg`).
    pub fn with_message_field(
        name: String,
        format: StdoutFormat,
        message_field: String,
        stats: Arc<ComponentStats>,
    ) -> Self {
        let color = format == StdoutFormat::Console
            && std::env::var_os("NO_COLOR").is_none()
            && io::stdout().is_terminal();
        StdoutSink {
            name,
            format,
            message_field,
            buf: Vec::with_capacity(8192),
            output_buf: Vec::with_capacity(8192),
            color,
            stats,
        }
    }

    /// Write a batch into a `Vec<u8>` destination.
    ///
    /// All production callers (FileSink, StdoutSink::serialize_batch) pass
    /// `&mut Vec<u8>`, allowing the JSON path to write directly without an
    /// intermediate per-row scratch buffer.  Console format still uses
    /// `self.buf` to build each row before flushing it.
    ///
    /// Returns the number of newline-terminated lines written, so callers
    /// (e.g. `FileSink`) can track line counts without a separate `memchr`
    /// scan over the output buffer.
    pub fn write_batch_to(
        &mut self,
        batch: &RecordBatch,
        _metadata: &BatchMetadata,
        dest: &mut Vec<u8>,
    ) -> io::Result<usize> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(0);
        }

        let lines = match self.format {
            StdoutFormat::Text => {
                let msg_indices =
                    resolve_message_indices(batch.schema().fields(), &self.message_field);
                if msg_indices.is_empty() {
                    // Configured message field is absent — fall back to JSON and warn once.
                    static WARNED: std::sync::atomic::AtomicBool =
                        std::sync::atomic::AtomicBool::new(false);
                    if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                        tracing::warn!(
                            sink = %self.name,
                            "stdout/file 'text' format requires message field '{}' in the output \
                             batch; falling back to JSON. Add that field to your SQL SELECT or use \
                             format: json."
                            ,
                            self.message_field
                        );
                    }
                    let cols = build_col_infos(batch);
                    let resolved = resolve_col_infos(batch, &cols);
                    for row in 0..num_rows {
                        write_row_json_resolved(row, &resolved, dest)?;
                        dest.push(b'\n');
                    }
                    num_rows
                } else {
                    let mut count = 0usize;
                    for row in 0..num_rows {
                        if let Some(col) = msg_indices
                            .iter()
                            .map(|&idx| batch.column(idx))
                            .find(|col| !col.is_null(row))
                        {
                            dest.extend_from_slice(
                                safe_array_value_to_string(col.as_ref(), row).as_bytes(),
                            );
                            dest.push(b'\n');
                            count += 1;
                        }
                    }
                    count
                }
            }
            StdoutFormat::Json => {
                let cols = build_col_infos(batch);
                let resolved = resolve_col_infos(batch, &cols);
                for row in 0..num_rows {
                    write_row_json_resolved(row, &resolved, dest)?;
                    dest.push(b'\n');
                }
                num_rows
            }
            StdoutFormat::Console => {
                let start_len = dest.len();
                self.write_console(batch, dest)?;
                // Console format: count lines in output (variable per row).
                // Slice from start_len to count only newly-appended newlines.
                memchr_iter(b'\n', &dest[start_len..]).count()
            }
        };
        Ok(lines)
    }

    /// Serialize a batch into `self.output_buf`.
    ///
    /// This avoids writing row-by-row to the async writer; instead the whole
    /// batch is rendered into the reusable output buffer and then flushed in a
    /// single `write_all` to `tokio::io::Stdout`.  Console format uses
    /// `self.buf` as per-row scratch space, so the output buffer is temporarily
    /// taken out to avoid aliasing borrows.  JSON and Text formats write
    /// directly into the destination buffer and do not use `self.buf`.
    fn serialize_batch(
        &mut self,
        batch: &RecordBatch,
        metadata: &BatchMetadata,
    ) -> io::Result<usize> {
        let mut output = std::mem::take(&mut self.output_buf);
        output.clear();
        let result = self.write_batch_to(batch, metadata, &mut output);
        self.output_buf = output;
        result
    }

    fn write_console(&mut self, batch: &RecordBatch, dest: &mut Vec<u8>) -> io::Result<()> {
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
        let msg_idx = resolve_message_idx(fields, self.message_field.as_str());

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
                // Skip the well-known columns.
                // Check ALL variant
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
                if col_matches_well_known {
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
            Write::write_all(dest, &self.buf)?;
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
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            let Some(arr) = col
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            else {
                return Cow::Owned(safe_array_value_to_string(col, row));
            };
            let ns = arr.value(row);
            let secs = ns.div_euclid(1_000_000_000);
            let nanos = ns.rem_euclid(1_000_000_000) as u32;
            Cow::Owned(logfwd_arrow::star_schema::chrono_timestamp(secs, nanos))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
            let Some(arr) = col
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            else {
                return Cow::Owned(safe_array_value_to_string(col, row));
            };
            let us = arr.value(row);
            let secs = us.div_euclid(1_000_000);
            let nanos = (us.rem_euclid(1_000_000) * 1_000) as u32;
            Cow::Owned(logfwd_arrow::star_schema::chrono_timestamp(secs, nanos))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
            let Some(arr) = col
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
            else {
                return Cow::Owned(safe_array_value_to_string(col, row));
            };
            let ms = arr.value(row);
            let secs = ms.div_euclid(1_000);
            let nanos = (ms.rem_euclid(1_000) * 1_000_000) as u32;
            Cow::Owned(logfwd_arrow::star_schema::chrono_timestamp(secs, nanos))
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => {
            let Some(arr) = col
                .as_any()
                .downcast_ref::<arrow::array::TimestampSecondArray>()
            else {
                return Cow::Owned(safe_array_value_to_string(col, row));
            };
            let s = arr.value(row);
            Cow::Owned(logfwd_arrow::star_schema::chrono_timestamp(s, 0))
        }
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

/// Resolve the best message column for text or console output.
///
/// Priority: canonical `body` → configured `message_field` → legacy aliases.
fn resolve_message_idx(fields: &arrow::datatypes::Fields, message_field: &str) -> Option<usize> {
    resolve_message_indices(fields, message_field)
        .into_iter()
        .next()
}

fn push_unique(indices: &mut Vec<usize>, idx: Option<usize>) {
    if let Some(idx) = idx
        && !indices.contains(&idx)
    {
        indices.push(idx);
    }
}

fn resolve_message_indices(fields: &arrow::datatypes::Fields, message_field: &str) -> Vec<usize> {
    let mut indices = Vec::with_capacity(4);

    // Canonical body always wins when present.
    push_unique(
        &mut indices,
        find_preferred_column(fields, field_names::BODY, &[], &[]),
    );

    // Then honor configured message field, including conflict-suffixed variant.
    push_unique(&mut indices, find_exact_column(fields, message_field));
    let conflict_name = format!("{message_field}__str");
    push_unique(
        &mut indices,
        find_exact_column(fields, conflict_name.as_str()),
    );

    // Legacy fallbacks for external schemas.
    for &name in field_names::BODY_VARIANTS {
        push_unique(&mut indices, find_exact_column(fields, name));
        let conflict_name = format!("{name}__str");
        push_unique(
            &mut indices,
            find_exact_column(fields, conflict_name.as_str()),
        );
    }

    indices
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

fn map_stdout_io_error(error: io::Error) -> SendResult {
    if error.kind() == io::ErrorKind::BrokenPipe {
        return SendResult::Rejected("stdout pipe closed (BrokenPipe)".to_string());
    }
    SendResult::IoError(error)
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
            let lines_written = match self.serialize_batch(batch, metadata) {
                Ok(n) => n as u64,
                Err(e) => return SendResult::IoError(e),
            };

            let bytes_written = self.output_buf.len() as u64;

            // Write the pre-rendered buffer to async stdout in one shot.
            let mut stdout = tokio::io::stdout();
            if let Err(e) = stdout.write_all(&self.output_buf).await {
                return map_stdout_io_error(e);
            }
            if let Err(e) = stdout.flush().await {
                return map_stdout_io_error(e);
            }

            self.stats.inc_lines(lines_written);
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
    message_field: String,
    stats: Arc<ComponentStats>,
}

impl StdoutSinkFactory {
    /// Create a new factory for stdout sinks.
    pub fn new(name: String, format: StdoutFormat, stats: Arc<ComponentStats>) -> Self {
        Self::with_message_field(name, format, field_names::BODY.to_string(), stats)
    }

    /// Create a factory with a custom text/console message field fallback.
    pub fn with_message_field(
        name: String,
        format: StdoutFormat,
        message_field: String,
        stats: Arc<ComponentStats>,
    ) -> Self {
        StdoutSinkFactory {
            name,
            format,
            message_field,
            stats,
        }
    }
}

impl SinkFactory for StdoutSinkFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
        let sink = if self.message_field == field_names::BODY {
            StdoutSink::new(self.name.clone(), self.format, Arc::clone(&self.stats))
        } else {
            StdoutSink::with_message_field(
                self.name.clone(),
                self.format,
                self.message_field.clone(),
                Arc::clone(&self.stats),
            )
        };
        Ok(Box::new(sink))
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

    /// A batch with a `body` column should be written as plain text lines.
    #[test]
    fn text_format_with_raw_column_writes_raw_lines() {
        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
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

    /// A batch without canonical `body` should still render text when a
    /// supported alias is present.
    #[test]
    fn text_format_message_alias_writes_text_lines() {
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
        assert_eq!(output, "hello\nworld\n");
    }

    #[test]
    fn text_format_honors_message_alias_fallback() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "message",
            DataType::Utf8,
            true,
        )]));
        let msg = StringArray::from(vec![Some("hello"), Some("world")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(msg)]).unwrap();

        let mut sink = StdoutSink::new(
            "test-message-alias".to_string(),
            StdoutFormat::Text,
            Arc::new(ComponentStats::new()),
        );
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &make_metadata(), &mut out)
            .unwrap();

        let output = String::from_utf8(out).unwrap();
        assert_eq!(output, "hello\nworld\n");
    }

    #[test]
    fn text_format_falls_back_to_alias_when_body_is_null() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("body-1"),
                    None,
                    Some("body-3"),
                ])),
                Arc::new(StringArray::from(vec![None, Some("msg-2"), Some("msg-3")])),
            ],
        )
        .unwrap();

        let mut sink = StdoutSink::new(
            "test-mixed-schema".to_string(),
            StdoutFormat::Text,
            Arc::new(ComponentStats::new()),
        );
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &make_metadata(), &mut out)
            .unwrap();

        let output = String::from_utf8(out).unwrap();
        assert_eq!(output, "body-1\nmsg-2\nbody-3\n");
    }

    #[test]
    fn text_format_non_string_body_does_not_panic() {
        use arrow::array::Int64Array;

        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Int64, true)]));
        let body = Int64Array::from(vec![Some(42), Some(7)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(body)]).unwrap();

        let mut sink = StdoutSink::new(
            "test-non-string-body".to_string(),
            StdoutFormat::Text,
            Arc::new(ComponentStats::new()),
        );
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &make_metadata(), &mut out)
            .unwrap();

        let output = String::from_utf8(out).unwrap();
        assert_eq!(output, "42\n7\n");
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

    #[test]
    fn console_format_honors_custom_message_field_without_duplication() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("level", DataType::Utf8, true),
            Field::new("custom_msg", DataType::Utf8, true),
            Field::new("req_id", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("INFO")])),
                Arc::new(StringArray::from(vec![Some("hello")])),
                Arc::new(StringArray::from(vec![Some("r-1")])),
            ],
        )
        .unwrap();

        let mut sink = StdoutSink::with_message_field(
            "console".to_string(),
            StdoutFormat::Console,
            "custom_msg".to_string(),
            Arc::new(ComponentStats::new()),
        );
        let mut out = Vec::new();
        sink.write_batch_to(&batch, &make_metadata(), &mut out)
            .unwrap();
        let rendered = String::from_utf8(out).unwrap();

        assert!(
            rendered.contains("hello"),
            "console output should include configured message value: {rendered:?}"
        );
        assert!(
            !rendered.contains("custom_msg=hello"),
            "configured message field should not be duplicated in extras: {rendered:?}"
        );
    }

    #[test]
    fn console_format_prefers_canonical_body_over_alias() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("message", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("canonical-body")])),
                Arc::new(StringArray::from(vec![Some("alias-message")])),
            ],
        )
        .unwrap();

        let mut sink = StdoutSink::with_message_field(
            "console".to_string(),
            StdoutFormat::Console,
            "message".to_string(),
            Arc::new(ComponentStats::new()),
        );
        let mut out = Vec::new();
        sink.write_batch_to(&batch, &make_metadata(), &mut out)
            .unwrap();
        let rendered = String::from_utf8(out).unwrap();

        assert!(
            rendered.contains("canonical-body"),
            "console output should render canonical body first: {rendered:?}"
        );
    }

    #[tokio::test]
    async fn send_batch_counts_emitted_lines_for_text_mode() {
        use std::sync::atomic::Ordering;

        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
        let body = StringArray::from(vec![Some("printed"), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(body)]).unwrap();

        let stats = Arc::new(ComponentStats::new());
        let mut sink = StdoutSink::new(
            "test-lines".to_string(),
            StdoutFormat::Text,
            Arc::clone(&stats),
        );
        let meta = make_metadata();

        let mut expected = Vec::new();
        sink.write_batch_to(&batch, &meta, &mut expected).unwrap();
        let expected_lines = memchr_iter(b'\n', &expected).count() as u64;

        sink.send_batch(&batch, &meta).await.unwrap();

        assert_eq!(
            stats.lines_total.load(Ordering::Relaxed),
            expected_lines,
            "lines_total should reflect emitted lines, not raw row count"
        );
    }

    #[test]
    fn broken_pipe_maps_to_terminal_rejected() {
        let result = map_stdout_io_error(io::Error::new(io::ErrorKind::BrokenPipe, "epipe"));
        assert!(
            matches!(result, SendResult::Rejected(_)),
            "BrokenPipe must map to terminal non-retryable outcome"
        );
    }
}

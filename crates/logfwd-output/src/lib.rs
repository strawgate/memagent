//! Output sink trait and implementations for serializing Arrow RecordBatches
//! to various formats: stdout JSON/text, JSON lines over HTTP, OTLP protobuf.

mod fanout;
mod json_lines;
mod otlp_sink;
mod stdout;

// Placeholder sinks — not yet wired into build_output_sink.
#[allow(dead_code)]
mod elasticsearch;
#[allow(dead_code)]
mod loki;
#[allow(dead_code)]
mod parquet;

pub use fanout::FanOut;
use json_lines::*;
use otlp_sink::*;
use stdout::*;

use std::io::{self, Write};

use arrow::array::{Array, AsArray};
use arrow::record_batch::RecordBatch;

use logfwd_config::{Format, OutputConfig, OutputType};

// ---------------------------------------------------------------------------
// Trait + metadata
// ---------------------------------------------------------------------------

/// Metadata about the batch for output serialization.
pub struct BatchMetadata {
    /// Resource attributes (k8s pod name, namespace, etc.)
    pub resource_attrs: Vec<(String, String)>,
    /// Observed timestamp in nanoseconds.
    pub observed_time_ns: u64,
}

/// Every output implements this trait.
pub trait OutputSink: Send {
    /// Serialize and send a batch.
    fn send_batch(&mut self, batch: &RecordBatch, metadata: &BatchMetadata) -> io::Result<()>;
    /// Flush any buffered data.
    fn flush(&mut self) -> io::Result<()>;
    /// Output name (from config).
    fn name(&self) -> &str;
}

// ---------------------------------------------------------------------------
// Column naming helpers
// ---------------------------------------------------------------------------

/// Parse a typed column name into (field_name, type_suffix).
///
/// "duration_ms_int" -> ("duration_ms", "int")
/// "level_str"       -> ("level", "str")
/// "_raw"            -> ("_raw", "")
pub fn parse_column_name(col_name: &str) -> (&str, &str) {
    if let Some(pos) = col_name.rfind('_') {
        let suffix = &col_name[pos + 1..];
        if suffix == "str" || suffix == "int" || suffix == "float" {
            return (&col_name[..pos], suffix);
        }
    }
    (col_name, "")
}

// ---------------------------------------------------------------------------
// JSON serialization helpers (shared by StdoutSink and JsonLinesSink)
// ---------------------------------------------------------------------------

/// Describes one output column: its index in the RecordBatch, the field name
/// (with type suffix stripped), the type suffix, and the Arrow data type.
pub(crate) struct ColInfo {
    idx: usize,
    field_name: String,
    type_suffix: String,
}

/// Build a de-duplicated ordered list of columns for JSON output.
/// When the same field_name appears multiple times (e.g. status_int and
/// status_str), prefer int > float > str.
pub(crate) fn build_col_infos(batch: &RecordBatch) -> Vec<ColInfo> {
    let schema = batch.schema();
    let mut infos: Vec<ColInfo> = Vec::new();
    // Collect all columns.
    for (idx, field) in schema.fields().iter().enumerate() {
        let name = field.name().as_str();
        let (field_name, type_suffix) = parse_column_name(name);
        infos.push(ColInfo {
            idx,
            field_name: field_name.to_string(),
            type_suffix: type_suffix.to_string(),
        });
    }
    // De-duplicate: for each field_name keep the best-typed column.
    // Priority: int > float > str > untyped
    fn type_priority(suffix: &str) -> u8 {
        match suffix {
            "int" => 3,
            "float" => 2,
            "str" => 1,
            _ => 0,
        }
    }
    // Use a stable sort + dedup to keep the highest-priority for each name.
    infos.sort_by(|a, b| {
        a.field_name
            .cmp(&b.field_name)
            .then_with(|| type_priority(&b.type_suffix).cmp(&type_priority(&a.type_suffix)))
    });
    infos.dedup_by(|a, b| a.field_name == b.field_name);
    // Re-sort by original column index to maintain stable output order.
    infos.sort_by_key(|c| c.idx);
    infos
}

/// Write a single row as a JSON object into `out`.
pub(crate) fn write_row_json(batch: &RecordBatch, row: usize, cols: &[ColInfo], out: &mut Vec<u8>) {
    out.push(b'{');
    let mut first = true;
    for col in cols {
        let arr = batch.column(col.idx);
        if arr.is_null(row) {
            continue;
        }
        if !first {
            out.push(b',');
        }
        first = false;
        // Key
        out.push(b'"');
        out.extend_from_slice(col.field_name.as_bytes());
        out.push(b'"');
        out.push(b':');
        // Value — type-aware
        match col.type_suffix.as_str() {
            "int" => {
                let arr = arr.as_primitive::<arrow::datatypes::Int64Type>();
                let v = arr.value(row);
                // Write integer directly; no quotes in JSON.
                let _ = Write::write_fmt(out, format_args!("{}", v));
            }
            "float" => {
                let arr = arr.as_primitive::<arrow::datatypes::Float64Type>();
                let v = arr.value(row);
                let _ = Write::write_fmt(out, format_args!("{}", v));
            }
            _ => {
                // str or untyped — treat as string
                let arr = arr.as_string::<i32>();
                let v = arr.value(row);
                out.push(b'"');
                // Minimal JSON escape
                for &b in v.as_bytes() {
                    match b {
                        b'"' => out.extend_from_slice(b"\\\""),
                        b'\\' => out.extend_from_slice(b"\\\\"),
                        b'\n' => out.extend_from_slice(b"\\n"),
                        b'\r' => out.extend_from_slice(b"\\r"),
                        b'\t' => out.extend_from_slice(b"\\t"),
                        _ => out.push(b),
                    }
                }
                out.push(b'"');
            }
        }
    }
    out.push(b'}');
}

/// Compression algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Zstd,
    Gzip,
    None,
}

// ---------------------------------------------------------------------------
// Output construction (factory)
// ---------------------------------------------------------------------------

/// Build an output sink from configuration.
pub fn build_output_sink(name: &str, cfg: &OutputConfig) -> Result<Box<dyn OutputSink>, String> {
    match cfg.output_type {
        OutputType::Stdout => {
            let fmt = match cfg.format.as_ref() {
                Some(Format::Json) => StdoutFormat::Json,
                Some(Format::Console) => StdoutFormat::Console,
                _ => StdoutFormat::Text,
            };
            Ok(Box::new(StdoutSink::new(name.to_string(), fmt)))
        }
        OutputType::Otlp => {
            let endpoint = cfg
                .endpoint
                .as_ref()
                .ok_or_else(|| format!("output '{name}': OTLP requires 'endpoint'"))?;
            let protocol = match cfg.protocol.as_deref() {
                Some("grpc") => OtlpProtocol::Grpc,
                _ => OtlpProtocol::Http,
            };
            let compression = match cfg.compression.as_deref() {
                Some("zstd") => Compression::Zstd,
                Some("gzip") => Compression::Gzip,
                _ => Compression::None,
            };
            Ok(Box::new(OtlpSink::new(
                name.to_string(),
                endpoint.clone(),
                protocol,
                compression,
            )))
        }
        OutputType::Http => {
            let endpoint = cfg
                .endpoint
                .as_ref()
                .ok_or_else(|| format!("output '{name}': HTTP requires 'endpoint'"))?;
            Ok(Box::new(JsonLinesSink::new(
                name.to_string(),
                endpoint.clone(),
                vec![],
            )))
        }
        _ => Err(format!(
            "output '{name}': type {:?} not yet supported",
            cfg.output_type
        )),
    }
}

// ---------------------------------------------------------------------------
// Test helper
// ---------------------------------------------------------------------------

/// Test helper: captures all sent batches for assertion.
#[cfg(test)]
pub struct CaptureSink {
    name: String,
    pub batches: Vec<RecordBatch>,
}

#[cfg(test)]
impl CaptureSink {
    pub fn new(name: &str) -> Self {
        CaptureSink {
            name: name.to_string(),
            batches: Vec::new(),
        }
    }
}

#[cfg(test)]
impl OutputSink for CaptureSink {
    fn send_batch(&mut self, batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        self.batches.push(batch.clone());
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("level_str", DataType::Utf8, true),
            Field::new("status_int", DataType::Int64, true),
        ]));
        let level = StringArray::from(vec![Some("ERROR"), Some("INFO")]);
        let status = Int64Array::from(vec![Some(500), Some(200)]);
        RecordBatch::try_new(schema, vec![Arc::new(level), Arc::new(status)]).unwrap()
    }

    fn make_metadata() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: vec![],
            observed_time_ns: 1_700_000_000_000_000_000,
        }
    }

    #[test]
    fn test_parse_column_name() {
        assert_eq!(parse_column_name("status_int"), ("status", "int"));
        assert_eq!(parse_column_name("level_str"), ("level", "str"));
        assert_eq!(
            parse_column_name("duration_ms_float"),
            ("duration_ms", "float")
        );
        assert_eq!(parse_column_name("_raw"), ("_raw", ""));
        assert_eq!(parse_column_name("plain"), ("plain", ""));
    }

    #[test]
    fn test_stdout_json() {
        let batch = make_test_batch();
        let meta = make_metadata();
        let mut sink = StdoutSink::new("test".to_string(), StdoutFormat::Json);
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &meta, &mut out).unwrap();

        let output = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = output.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);
        // First row: level=ERROR, status=500
        assert!(
            lines[0].contains("\"level\":\"ERROR\""),
            "got: {}",
            lines[0]
        );
        assert!(lines[0].contains("\"status\":500"), "got: {}", lines[0]);
        // Second row: level=INFO, status=200
        assert!(lines[1].contains("\"level\":\"INFO\""), "got: {}", lines[1]);
        assert!(lines[1].contains("\"status\":200"), "got: {}", lines[1]);
    }

    #[test]
    fn test_fanout() {
        // FanOut to two sinks that write to Vec<u8>.
        // We use StdoutSink with write_batch_to to capture output.
        let batch = make_test_batch();
        let meta = make_metadata();

        let mut sink1 = StdoutSink::new("s1".to_string(), StdoutFormat::Json);
        let mut sink2 = StdoutSink::new("s2".to_string(), StdoutFormat::Json);

        let mut out1: Vec<u8> = Vec::new();
        let mut out2: Vec<u8> = Vec::new();

        sink1.write_batch_to(&batch, &meta, &mut out1).unwrap();
        sink2.write_batch_to(&batch, &meta, &mut out2).unwrap();

        // Both should have identical output.
        assert_eq!(out1, out2);
        assert!(!out1.is_empty());

        // Also test FanOut trait dispatch works.
        let fanout_s1 = StdoutSink::new("f1".to_string(), StdoutFormat::Json);
        let fanout_s2 = StdoutSink::new("f2".to_string(), StdoutFormat::Json);
        let mut fanout = FanOut::new(vec![Box::new(fanout_s1), Box::new(fanout_s2)]);
        // send_batch writes to real stdout, but should not error.
        let result = fanout.send_batch(&batch, &meta);
        assert!(result.is_ok());
    }

    #[test]
    fn test_otlp_encoding() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_str", DataType::Utf8, true),
            Field::new("level_str", DataType::Utf8, true),
            Field::new("message_str", DataType::Utf8, true),
            Field::new("status_int", DataType::Int64, true),
        ]));
        let ts = StringArray::from(vec![Some("2024-01-15T10:30:00Z")]);
        let level = StringArray::from(vec![Some("ERROR")]);
        let msg = StringArray::from(vec![Some("something broke")]);
        let status = Int64Array::from(vec![Some(500)]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ts),
                Arc::new(level),
                Arc::new(msg),
                Arc::new(status),
            ],
        )
        .unwrap();

        let meta = BatchMetadata {
            resource_attrs: vec![("k8s.pod.name".to_string(), "myapp-abc".to_string())],
            observed_time_ns: 1_700_000_000_000_000_000,
        };

        let mut sink = OtlpSink::new(
            "test-otlp".to_string(),
            "http://localhost:4318".to_string(),
            OtlpProtocol::Http,
            Compression::None,
        );
        sink.encode_batch(&batch, &meta);

        // Should produce non-empty protobuf bytes.
        assert!(!sink.encoder_buf.is_empty());
        // First byte should be tag for field 1 (ResourceLogs), wire type 2 = 0x0A
        assert_eq!(sink.encoder_buf[0], 0x0A);
    }

    #[test]
    fn test_raw_passthrough() {
        // Build a batch with only _raw column (simulating no transforms).
        let schema = Arc::new(Schema::new(vec![Field::new("_raw", DataType::Utf8, true)]));
        let raw = StringArray::from(vec![
            Some(r#"{"ts":"2024-01-15","msg":"hello"}"#),
            Some(r#"{"ts":"2024-01-15","msg":"world"}"#),
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(raw)]).unwrap();

        let mut sink = JsonLinesSink::new(
            "test-jsonl".to_string(),
            "http://localhost:9200".to_string(),
            vec![],
        );
        sink.serialize_batch(&batch);

        let output = String::from_utf8(sink.batch_buf.clone()).unwrap();
        let lines: Vec<&str> = output.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);
        // Should be the original JSON, not re-serialized.
        assert_eq!(lines[0], r#"{"ts":"2024-01-15","msg":"hello"}"#);
        assert_eq!(lines[1], r#"{"ts":"2024-01-15","msg":"world"}"#);
    }

    #[test]
    fn test_stdout_text_format() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_raw", DataType::Utf8, true),
            Field::new("level_str", DataType::Utf8, true),
        ]));
        let raw = StringArray::from(vec![Some("original log line")]);
        let level = StringArray::from(vec![Some("INFO")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(raw), Arc::new(level)]).unwrap();
        let meta = make_metadata();

        let mut sink = StdoutSink::new("test".to_string(), StdoutFormat::Text);
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &meta, &mut out).unwrap();
        let output = String::from_utf8(out).unwrap();
        assert_eq!(output.trim(), "original log line");
    }

    #[test]
    fn test_type_preference_dedup() {
        // When both status_int and status_str exist, int should win.
        let schema = Arc::new(Schema::new(vec![
            Field::new("status_str", DataType::Utf8, true),
            Field::new("status_int", DataType::Int64, true),
        ]));
        let status_s = StringArray::from(vec![Some("500")]);
        let status_i = Int64Array::from(vec![Some(500)]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(status_s), Arc::new(status_i)]).unwrap();
        let meta = make_metadata();

        let mut sink = StdoutSink::new("test".to_string(), StdoutFormat::Json);
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &meta, &mut out).unwrap();
        let output = String::from_utf8(out).unwrap();
        // Should have integer 500, not string "500"
        assert!(output.contains("\"status\":500"), "got: {}", output);
        // Should NOT have the string version
        assert!(!output.contains("\"status\":\"500\""), "got: {}", output);
    }

    #[test]
    fn test_float_column_json() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "duration_ms_float",
            DataType::Float64,
            true,
        )]));
        let dur = Float64Array::from(vec![Some(3.14)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dur)]).unwrap();
        let meta = make_metadata();

        let mut sink = StdoutSink::new("test".to_string(), StdoutFormat::Json);
        let mut out: Vec<u8> = Vec::new();
        sink.write_batch_to(&batch, &meta, &mut out).unwrap();
        let output = String::from_utf8(out).unwrap();
        assert!(output.contains("\"duration_ms\":3.14"), "got: {}", output);
    }

    #[test]
    fn test_build_output_sink_stdout() {
        let cfg = OutputConfig {
            name: Some("test".to_string()),
            output_type: OutputType::Stdout,
            endpoint: None,
            protocol: None,
            compression: None,
            format: Some(Format::Json),
            path: None,
        };
        let sink = build_output_sink("test", &cfg).unwrap();
        assert_eq!(sink.name(), "test");
    }

    #[test]
    fn test_build_output_sink_otlp() {
        let cfg = OutputConfig {
            name: Some("otel".to_string()),
            output_type: OutputType::Otlp,
            endpoint: Some("http://localhost:4318".to_string()),
            protocol: Some("http".to_string()),
            compression: Some("zstd".to_string()),
            format: None,
            path: None,
        };
        let sink = build_output_sink("otel", &cfg).unwrap();
        assert_eq!(sink.name(), "otel");
    }

    #[test]
    fn test_build_output_sink_http() {
        let cfg = OutputConfig {
            name: Some("es".to_string()),
            output_type: OutputType::Http,
            endpoint: Some("http://localhost:9200".to_string()),
            protocol: None,
            compression: None,
            format: None,
            path: None,
        };
        let sink = build_output_sink("es", &cfg).unwrap();
        assert_eq!(sink.name(), "es");
    }

    #[test]
    fn test_build_output_sink_missing_endpoint() {
        let cfg = OutputConfig {
            name: Some("bad".to_string()),
            output_type: OutputType::Otlp,
            endpoint: None,
            protocol: None,
            compression: None,
            format: None,
            path: None,
        };
        let result = build_output_sink("bad", &cfg);
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.contains("endpoint"), "got: {err}");
    }
}

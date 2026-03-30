use std::io::{self, Write};

use arrow::array::{Array, AsArray};
use arrow::record_batch::RecordBatch;

use super::{BatchMetadata, OutputSink, build_col_infos, str_value, write_row_json};

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
pub struct StdoutSink {
    name: String,
    format: StdoutFormat,
    buf: Vec<u8>,
    color: bool,
}

impl StdoutSink {
    pub fn new(name: String, format: StdoutFormat) -> Self {
        let color = format == StdoutFormat::Console
            && std::env::var_os("NO_COLOR").is_none()
            && unsafe { libc::isatty(libc::STDOUT_FILENO) != 0 };
        StdoutSink {
            name,
            format,
            buf: Vec::with_capacity(8192),
            color,
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
                    // Fall back to JSON
                    let cols = build_col_infos(batch);
                    for row in 0..num_rows {
                        self.buf.clear();
                        write_row_json(batch, row, &cols, &mut self.buf);
                        self.buf.push(b'\n');
                        dest.write_all(&self.buf)?;
                    }
                }
            }
            StdoutFormat::Json => {
                let cols = build_col_infos(batch);
                for row in 0..num_rows {
                    self.buf.clear();
                    write_row_json(batch, row, &cols, &mut self.buf);
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

    fn write_console<W: Write>(&mut self, batch: &RecordBatch, dest: &mut W) -> io::Result<()> {
        let schema = batch.schema();
        let fields = schema.fields();

        // Find well-known columns by name (with or without type suffix).
        let ts_idx = find_col(fields, &["timestamp_str", "timestamp"]);
        let level_idx = find_col(fields, &["level_str", "level"]);
        let msg_idx = find_col(fields, &["message_str", "message", "msg_str", "msg"]);

        let cols = build_col_infos(batch);

        for row in 0..batch.num_rows() {
            self.buf.clear();

            // Timestamp (dim).
            if let Some(idx) = ts_idx {
                let col = batch.column(idx);
                if !col.is_null(row) {
                    let ts = str_value(col, row);
                    // Show just the time portion if it's a full ISO timestamp.
                    let short = ts.find('T').map(|i| &ts[i + 1..]).unwrap_or(ts);
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
                    let level = str_value(col, row);
                    if self.color {
                        let color = match level {
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
                    self.buf.extend_from_slice(str_value(col, row).as_bytes());
                }
            }

            // Remaining fields as key=value pairs (dim).
            let mut has_extra = false;
            for col in &cols {
                // Skip the well-known columns and _raw.
                if Some(col.idx) == ts_idx
                    || Some(col.idx) == level_idx
                    || Some(col.idx) == msg_idx
                    || col.field_name == "_raw"
                {
                    continue;
                }

                let arr = batch.column(col.idx);
                if arr.is_null(row) {
                    continue;
                }

                if !has_extra {
                    self.buf.extend_from_slice(b"  ");
                    has_extra = true;
                } else {
                    self.buf.push(b' ');
                }

                if self.color {
                    self.buf.extend_from_slice(b"\x1b[2m");
                }
                self.buf.extend_from_slice(col.field_name.as_bytes());
                self.buf.push(b'=');

                match col.type_suffix.as_str() {
                    "int" => {
                        let arr = arr.as_primitive::<arrow::datatypes::Int64Type>();
                        write!(self.buf, "{}", arr.value(row))?;
                    }
                    "float" => {
                        let arr = arr.as_primitive::<arrow::datatypes::Float64Type>();
                        write!(self.buf, "{}", arr.value(row))?;
                    }
                    _ => {
                        self.buf.extend_from_slice(str_value(arr, row).as_bytes());
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

/// Find a column index by trying multiple name variants.
fn find_col(fields: &arrow::datatypes::Fields, names: &[&str]) -> Option<usize> {
    for name in names {
        if let Some(idx) = fields.iter().position(|f| f.name() == *name) {
            return Some(idx);
        }
    }
    None
}

impl OutputSink for StdoutSink {
    fn send_batch(&mut self, batch: &RecordBatch, metadata: &BatchMetadata) -> io::Result<()> {
        let mut stdout = io::stdout().lock();
        self.write_batch_to(batch, metadata, &mut stdout)
    }

    fn flush(&mut self) -> io::Result<()> {
        io::stdout().flush()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

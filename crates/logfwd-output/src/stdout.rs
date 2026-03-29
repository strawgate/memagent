use std::io::{self, Write};

use arrow::array::{Array, AsArray};
use arrow::record_batch::RecordBatch;

use super::{BatchMetadata, OutputSink, build_col_infos, write_row_json};

// ---------------------------------------------------------------------------
// StdoutSink
// ---------------------------------------------------------------------------

/// Output format for StdoutSink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StdoutFormat {
    Json,
    Text,
}

/// Writes log records to stdout, one per line.
pub struct StdoutSink {
    name: String,
    format: StdoutFormat,
    buf: Vec<u8>,
}

impl StdoutSink {
    pub fn new(name: String, format: StdoutFormat) -> Self {
        StdoutSink {
            name,
            format,
            buf: Vec::with_capacity(8192),
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
                    let arr = batch.column(idx).as_string::<i32>();
                    for row in 0..num_rows {
                        if !arr.is_null(row) {
                            dest.write_all(arr.value(row).as_bytes())?;
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
        }
        Ok(())
    }
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

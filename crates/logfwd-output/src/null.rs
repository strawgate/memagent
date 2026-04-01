//! Null output sink — discards all batches. Used for benchmarking and as a
//! blackhole collector target.

use std::io;

use arrow::record_batch::RecordBatch;

use crate::{BatchMetadata, OutputSink};

/// Discards all data. Useful as the output side of a blackhole receiver
/// or for benchmarking pipeline overhead without network I/O.
pub struct NullSink {
    name: String,
}

impl NullSink {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl OutputSink for NullSink {
    fn send_batch(&mut self, _batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_sink_accepts_and_discards() {
        let mut sink = NullSink::new("blackhole");
        assert_eq!(sink.name(), "blackhole");

        let schema = std::sync::Arc::new(arrow::datatypes::Schema::empty());
        let batch = RecordBatch::new_empty(schema);
        let meta = BatchMetadata {
            resource_attrs: vec![],
            observed_time_ns: 0,
        };
        assert!(sink.send_batch(&batch, &meta).is_ok());
        assert!(sink.flush().is_ok());
    }
}

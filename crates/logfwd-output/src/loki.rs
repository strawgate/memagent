use std::io;

use arrow::record_batch::RecordBatch;

use super::{BatchMetadata, OutputSink};

// ---------------------------------------------------------------------------
// LokiSink (placeholder)
// ---------------------------------------------------------------------------

/// Sends log records to Grafana Loki.
pub struct LokiSink {
    name: String,
    #[allow(dead_code)]
    endpoint: String,
}

impl LokiSink {
    pub fn new(name: String, endpoint: String) -> Self {
        LokiSink { name, endpoint }
    }
}

impl OutputSink for LokiSink {
    fn send_batch(&mut self, _batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        // TODO: implement Loki push API
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

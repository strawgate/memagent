use std::io;

use arrow::record_batch::RecordBatch;

use super::{BatchMetadata, OutputSink};

// ---------------------------------------------------------------------------
// ElasticsearchSink (placeholder)
// ---------------------------------------------------------------------------

/// Sends log records to Elasticsearch.
pub struct ElasticsearchSink {
    name: String,
    endpoint: String,
}

impl ElasticsearchSink {
    pub fn new(name: String, endpoint: String) -> Self {
        ElasticsearchSink { name, endpoint }
    }
}

impl OutputSink for ElasticsearchSink {
    fn send_batch(&mut self, _batch: &RecordBatch, _metadata: &BatchMetadata) -> io::Result<()> {
        // TODO: implement Elasticsearch bulk API
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

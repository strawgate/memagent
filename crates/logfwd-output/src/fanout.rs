use std::io;

use arrow::record_batch::RecordBatch;

use super::{BatchMetadata, OutputSink};

// ---------------------------------------------------------------------------
// FanOut
// ---------------------------------------------------------------------------

/// Multiplexes output to multiple sinks.
pub struct FanOut {
    sinks: Vec<Box<dyn OutputSink>>,
}

impl FanOut {
    pub fn new(sinks: Vec<Box<dyn OutputSink>>) -> Self {
        FanOut { sinks }
    }
}

impl OutputSink for FanOut {
    fn send_batch(&mut self, batch: &RecordBatch, meta: &BatchMetadata) -> io::Result<()> {
        for sink in &mut self.sinks {
            sink.send_batch(batch, meta)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        for sink in &mut self.sinks {
            sink.flush()?;
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "fanout"
    }
}

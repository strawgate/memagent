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
        // Try ALL sinks before returning an error. Don't short-circuit.
        let mut first_err: Option<io::Error> = None;
        for sink in &mut self.sinks {
            if let Err(e) = sink.send_batch(batch, meta) {
                eprintln!("fanout: sink '{}' failed: {e}", sink.name());
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut first_err: Option<io::Error> = None;
        for sink in &mut self.sinks {
            if let Err(e) = sink.flush()
                && first_err.is_none()
            {
                first_err = Some(e);
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn name(&self) -> &str {
        "fanout"
    }
}

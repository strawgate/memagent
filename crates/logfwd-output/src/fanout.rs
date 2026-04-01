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

#[derive(Debug)]
pub struct FanOutError {
    failed_sinks: Vec<String>,
    first_error: io::Error,
}

impl FanOutError {
    fn new(failed_sinks: Vec<String>, first_error: io::Error) -> Self {
        Self {
            failed_sinks,
            first_error,
        }
    }

    pub fn failed_sinks(&self) -> &[String] {
        &self.failed_sinks
    }
}

impl std::fmt::Display for FanOutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "fanout failed for sink(s): {}",
            self.failed_sinks.join(", ")
        )
    }
}

impl std::error::Error for FanOutError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.first_error)
    }
}

impl FanOut {
    pub fn new(sinks: Vec<Box<dyn OutputSink>>) -> Self {
        FanOut { sinks }
    }
}

impl OutputSink for FanOut {
    fn send_batch(&mut self, batch: &RecordBatch, meta: &BatchMetadata) -> io::Result<()> {
        // Try ALL sinks before returning an error. Don't short-circuit.
        let mut failed_sinks = Vec::new();
        let mut first_err: Option<io::Error> = None;
        for sink in &mut self.sinks {
            if let Err(e) = sink.send_batch(batch, meta) {
                eprintln!("fanout: sink '{}' failed: {e}", sink.name());
                failed_sinks.push(sink.name().to_string());
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        match first_err {
            Some(e) => Err(io::Error::other(FanOutError::new(failed_sinks, e))),
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

use std::io;
use std::time::Duration;

use arrow::record_batch::RecordBatch;

use super::{
    BatchMetadata, OutputSink,
    sink::{FlushFut, SendFut, SendResult, Sink},
};

// ---------------------------------------------------------------------------
// FanOut
// ---------------------------------------------------------------------------

/// Synchronous fan-out multiplexer (legacy [`OutputSink`] path).
///
/// Multiplexes output to multiple sinks sequentially.  All sinks are called
/// regardless of failures (no short-circuit), and the first error is
/// returned after all sinks have been tried.
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

// ---------------------------------------------------------------------------
// FanOutSink — async multiplexer implementing the Sink trait
// ---------------------------------------------------------------------------

/// Async fan-out multiplexer that implements the [`Sink`] trait.
///
/// Sends each batch to all contained sinks sequentially.  Per-sink
/// [`SendResult`] values are handled independently: a failing or back-pressured
/// sink does not prevent the others from receiving the batch.
///
/// All sink futures are collected synchronously (encoding is done inline in
/// each sink's `send_batch`) before the first `.await`, so the returned future
/// contains only owned data.
///
/// Build a `FanOutSink` from a list of [`Box<dyn Sink>`] values:
///
/// ```rust,ignore
/// use logfwd_output::{FanOutSink, StdoutSink, StdoutFormat};
///
/// let fanout = FanOutSink::new("all".to_string(), vec![
///     Box::new(StdoutSink::new("out".to_string(), StdoutFormat::Json)),
/// ]);
/// ```
pub struct FanOutSink {
    sinks: Vec<Box<dyn Sink>>,
    name: String,
}

impl FanOutSink {
    /// Create a new `FanOutSink` with the given name and sinks.
    pub fn new(name: String, sinks: Vec<Box<dyn Sink>>) -> Self {
        FanOutSink { sinks, name }
    }
}

impl Sink for FanOutSink {
    fn send_batch(&mut self, batch: &RecordBatch, metadata: &BatchMetadata) -> SendFut {
        // Collect per-sink futures synchronously (encoding happens inside each
        // sink's send_batch), so the returned future owns only the Vec of
        // boxed futures and doesn't borrow self, batch, or metadata.
        let futures: Vec<(String, SendFut)> = self
            .sinks
            .iter_mut()
            .map(|s| (s.name().to_string(), s.send_batch(batch, metadata)))
            .collect();

        Box::pin(async move {
            let mut retry_after: Option<Duration> = None;

            for (name, fut) in futures {
                match fut.await {
                    Err(e) => {
                        eprintln!("fanout: sink '{name}' error: {e}");
                    }
                    Ok(SendResult::Ok) => {}
                    Ok(SendResult::RetryAfter(d)) => {
                        retry_after = Some(match retry_after {
                            Some(prev) => prev.max(d),
                            None => d,
                        });
                    }
                    Ok(SendResult::Rejected(msg)) => {
                        eprintln!("fanout: sink '{name}' rejected batch: {msg}");
                    }
                }
            }

            Ok(match retry_after {
                Some(d) => SendResult::RetryAfter(d),
                None => SendResult::Ok,
            })
        })
    }

    fn flush(&mut self) -> FlushFut {
        let futures: Vec<(String, FlushFut)> = self
            .sinks
            .iter_mut()
            .map(|s| (s.name().to_string(), s.flush()))
            .collect();

        Box::pin(async move {
            let mut first_err: Option<io::Error> = None;
            for (name, fut) in futures {
                if let Err(e) = fut.await {
                    eprintln!("fanout: sink '{name}' flush error: {e}");
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
            match first_err {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn shutdown(&mut self) -> FlushFut {
        let futures: Vec<(String, FlushFut)> = self
            .sinks
            .iter_mut()
            .map(|s| (s.name().to_string(), s.shutdown()))
            .collect();

        Box::pin(async move {
            let mut first_err: Option<io::Error> = None;
            for (name, fut) in futures {
                if let Err(e) = fut.await {
                    eprintln!("fanout: sink '{name}' shutdown error: {e}");
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
            match first_err {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })
    }
}

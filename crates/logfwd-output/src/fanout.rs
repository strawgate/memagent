use std::io;

use arrow::record_batch::RecordBatch;

#[allow(deprecated)]
use super::{BatchMetadata, OutputSink};

// ---------------------------------------------------------------------------
// FanOut
// ---------------------------------------------------------------------------

/// Multiplexes output to multiple sinks.
#[allow(deprecated)]
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

#[allow(deprecated)]
impl FanOut {
    pub fn new(sinks: Vec<Box<dyn OutputSink>>) -> Self {
        FanOut { sinks }
    }
}

#[allow(deprecated)]
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
        let mut failed_sinks = Vec::new();
        let mut first_err: Option<io::Error> = None;
        for sink in &mut self.sinks {
            if let Err(e) = sink.flush() {
                eprintln!("fanout: sink '{}' failed to flush: {e}", sink.name());
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

    fn name(&self) -> &'static str {
        "fanout"
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn empty_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(Vec::<&str>::new()))],
        )
        .unwrap()
    }

    fn meta() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::default(),
            observed_time_ns: 0,
        }
    }

    struct MockSink {
        name: String,
        fail_send: bool,
        fail_flush: bool,
        send_calls: Arc<AtomicU32>,
        flush_calls: Arc<AtomicU32>,
    }

    impl MockSink {
        fn new(name: &str, fail_send: bool) -> (Self, Arc<AtomicU32>, Arc<AtomicU32>) {
            let send_calls = Arc::new(AtomicU32::new(0));
            let flush_calls = Arc::new(AtomicU32::new(0));
            (
                MockSink {
                    name: name.to_string(),
                    fail_send,
                    fail_flush: false,
                    send_calls: Arc::clone(&send_calls),
                    flush_calls: Arc::clone(&flush_calls),
                },
                send_calls,
                flush_calls,
            )
        }
        fn with_fail_flush(mut self) -> Self {
            self.fail_flush = true;
            self
        }
    }

    #[allow(deprecated)]
    impl OutputSink for MockSink {
        fn send_batch(&mut self, _batch: &RecordBatch, _meta: &BatchMetadata) -> io::Result<()> {
            self.send_calls.fetch_add(1, Ordering::Relaxed);
            if self.fail_send {
                Err(io::Error::other("mock send failure"))
            } else {
                Ok(())
            }
        }
        fn flush(&mut self) -> io::Result<()> {
            self.flush_calls.fetch_add(1, Ordering::Relaxed);
            if self.fail_flush {
                Err(io::Error::other("mock flush failure"))
            } else {
                Ok(())
            }
        }
        fn name(&self) -> &str {
            &self.name
        }
    }

    #[test]
    fn all_sinks_succeed_returns_ok() {
        let (a, a_calls, _) = MockSink::new("a", false);
        let (b, b_calls, _) = MockSink::new("b", false);
        let mut fanout = FanOut::new(vec![Box::new(a), Box::new(b)]);
        fanout.send_batch(&empty_batch(), &meta()).unwrap();
        assert_eq!(a_calls.load(Ordering::Relaxed), 1);
        assert_eq!(b_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn first_sink_fails_all_still_called() {
        // FanOut must NOT short-circuit — all sinks must be called even if the first fails.
        let (a, a_calls, _) = MockSink::new("a", true); // fails
        let (b, b_calls, _) = MockSink::new("b", false);
        let mut fanout = FanOut::new(vec![Box::new(a), Box::new(b)]);
        let err = fanout.send_batch(&empty_batch(), &meta()).unwrap_err();
        assert_eq!(
            a_calls.load(Ordering::Relaxed),
            1,
            "failing sink must be called"
        );
        assert_eq!(
            b_calls.load(Ordering::Relaxed),
            1,
            "subsequent sink must still be called"
        );
        // err.to_string() uses FanOutError::Display which lists the failed sink names.
        let msg = err.to_string();
        assert!(
            msg.contains('a'),
            "error must name the failing sink, got: {msg}"
        );
    }

    #[test]
    fn middle_sink_fails_returns_err_with_failed_name() {
        let (a, _, _) = MockSink::new("alpha", false);
        let (b, _, _) = MockSink::new("beta", true); // fails
        let (c, c_calls, _) = MockSink::new("gamma", false);
        let mut fanout = FanOut::new(vec![Box::new(a), Box::new(b), Box::new(c)]);
        let err = fanout.send_batch(&empty_batch(), &meta()).unwrap_err();
        assert_eq!(
            c_calls.load(Ordering::Relaxed),
            1,
            "third sink must be called after middle failure"
        );
        // err.to_string() uses FanOutError::Display which lists the failed sink names.
        let msg = err.to_string();
        assert!(
            msg.contains("beta"),
            "error must name the failed sink, got: {msg}"
        );
    }

    #[test]
    fn all_sinks_fail_first_error_returned() {
        let (a, _, _) = MockSink::new("x", true);
        let (b, _, _) = MockSink::new("y", true);
        let mut fanout = FanOut::new(vec![Box::new(a), Box::new(b)]);
        let err = fanout.send_batch(&empty_batch(), &meta()).unwrap_err();
        // err.to_string() uses FanOutError::Display which lists all failed sink names.
        let msg = err.to_string();
        assert!(
            msg.contains('x') && msg.contains('y'),
            "both sink names expected in: {msg}"
        );
    }

    #[test]
    fn flush_all_succeed_returns_ok() {
        let (a, _, a_flush) = MockSink::new("a", false);
        let (b, _, b_flush) = MockSink::new("b", false);
        let mut fanout = FanOut::new(vec![Box::new(a), Box::new(b)]);
        fanout.flush().unwrap();
        assert_eq!(a_flush.load(Ordering::Relaxed), 1);
        assert_eq!(b_flush.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn flush_one_fails_returns_err() {
        let (a, _, _) = MockSink::new("a", false);
        let (b_base, _, _) = MockSink::new("b", false);
        let b = b_base.with_fail_flush();
        let mut fanout = FanOut::new(vec![Box::new(a), Box::new(b)]);
        assert!(fanout.flush().is_err());
    }

    #[test]
    fn flush_all_fail_all_names_captured() {
        // Both sinks fail to flush — both names must appear in the error.
        let (a_base, _, _) = MockSink::new("alice", false);
        let (b_base, _, _) = MockSink::new("bob", false);
        let a = a_base.with_fail_flush();
        let b = b_base.with_fail_flush();
        let mut fanout = FanOut::new(vec![Box::new(a), Box::new(b)]);
        let err = fanout.flush().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("alice") && msg.contains("bob"),
            "both sink names expected in flush error, got: {msg}"
        );
    }

    #[test]
    fn flush_first_fails_second_still_called() {
        // FanOut flush must NOT short-circuit — both sinks must be called even if the first fails.
        let (a_base, _, a_flush) = MockSink::new("a", false);
        let (b_base, _, b_flush) = MockSink::new("b", false);
        let a = a_base.with_fail_flush();
        let b = b_base.with_fail_flush();
        let mut fanout = FanOut::new(vec![Box::new(a), Box::new(b)]);
        fanout.flush().unwrap_err();
        assert_eq!(
            a_flush.load(Ordering::Relaxed),
            1,
            "first sink must be flushed"
        );
        assert_eq!(
            b_flush.load(Ordering::Relaxed),
            1,
            "second sink must be flushed even after first fails"
        );
    }

    #[test]
    fn empty_sinks_list_returns_ok() {
        let mut fanout = FanOut::new(vec![]);
        fanout.send_batch(&empty_batch(), &meta()).unwrap();
        fanout.flush().unwrap();
    }
}

//! Pure state machine for batch accumulation.
//!
//! Extracted from `pipeline.rs` to enable deterministic testing.
//! Collects incoming data + checkpoint metadata, decides when to flush
//! based on size threshold or timeout.

use std::collections::HashMap;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use logfwd_core::pipeline::SourceId;
use logfwd_io::tail::ByteOffset;

/// Action returned by the accumulator after ingesting data or checking timeout.
pub enum AccumulatorAction {
    /// Buffer reached threshold or timeout -- flush this data.
    Flush {
        data: Bytes,
        checkpoints: HashMap<SourceId, ByteOffset>,
        queued_at: Option<Instant>,
        reason: &'static str,
    },
    /// Not enough data yet.
    Continue,
}

pub struct BatchAccumulator {
    buf: BytesMut,
    checkpoints: HashMap<SourceId, ByteOffset>,
    queued_at: Option<Instant>,
    batch_target_bytes: usize,
}

impl BatchAccumulator {
    pub fn new(batch_target_bytes: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(batch_target_bytes),
            checkpoints: HashMap::new(),
            queued_at: None,
            batch_target_bytes,
        }
    }

    /// Ingest data from an input. Returns Flush if buffer reached target size.
    pub fn ingest(
        &mut self,
        bytes: Bytes,
        input_checkpoints: Vec<(SourceId, ByteOffset)>,
        queued_at: Instant,
    ) -> AccumulatorAction {
        if self.queued_at.is_none() {
            self.queued_at = Some(queued_at);
        }
        self.buf.extend_from_slice(&bytes);
        for (sid, offset) in input_checkpoints {
            self.checkpoints.insert(sid, offset);
        }
        if self.buf.len() >= self.batch_target_bytes {
            self.take_flush("size")
        } else {
            AccumulatorAction::Continue
        }
    }

    /// Check if a timeout-based flush should happen.
    /// Returns Flush if there is any buffered data.
    pub fn check_timeout(&mut self) -> AccumulatorAction {
        if self.buf.is_empty() {
            AccumulatorAction::Continue
        } else {
            self.take_flush("timeout")
        }
    }

    /// Force-drain remaining data (shutdown path).
    pub fn drain(&mut self) -> Option<(Bytes, HashMap<SourceId, ByteOffset>, Option<Instant>)> {
        if self.buf.is_empty() {
            return None;
        }
        let data = self.buf.split().freeze();
        let checkpoints = std::mem::take(&mut self.checkpoints);
        let queued_at = self.queued_at.take();
        Some((data, checkpoints, queued_at))
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    fn take_flush(&mut self, reason: &'static str) -> AccumulatorAction {
        let data = self.buf.split().freeze();
        let checkpoints = std::mem::take(&mut self.checkpoints);
        let queued_at = self.queued_at.take();
        AccumulatorAction::Flush {
            data,
            checkpoints,
            queued_at,
            reason,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sid(n: u64) -> SourceId {
        SourceId(n)
    }

    #[test]
    fn ingest_below_threshold_returns_continue() {
        let mut acc = BatchAccumulator::new(100);
        let result = acc.ingest(
            Bytes::from_static(b"hello\n"),
            vec![(sid(1), ByteOffset(6))],
            Instant::now(),
        );
        assert!(matches!(result, AccumulatorAction::Continue));
        assert_eq!(acc.len(), 6);
    }

    #[test]
    fn ingest_at_threshold_returns_flush() {
        let mut acc = BatchAccumulator::new(10);
        let result = acc.ingest(
            Bytes::from(vec![b'x'; 15]),
            vec![(sid(1), ByteOffset(15))],
            Instant::now(),
        );
        assert!(matches!(
            result,
            AccumulatorAction::Flush { reason: "size", .. }
        ));
        assert!(acc.is_empty());
    }

    #[test]
    fn check_timeout_empty_returns_continue() {
        let mut acc = BatchAccumulator::new(100);
        assert!(matches!(acc.check_timeout(), AccumulatorAction::Continue));
    }

    #[test]
    fn check_timeout_nonempty_returns_flush() {
        let mut acc = BatchAccumulator::new(100);
        acc.ingest(Bytes::from_static(b"data\n"), vec![], Instant::now());
        let result = acc.check_timeout();
        assert!(matches!(
            result,
            AccumulatorAction::Flush {
                reason: "timeout",
                ..
            }
        ));
        assert!(acc.is_empty());
    }

    #[test]
    fn drain_returns_remaining_data() {
        let mut acc = BatchAccumulator::new(100);
        acc.ingest(
            Bytes::from_static(b"drain me\n"),
            vec![(sid(1), ByteOffset(9))],
            Instant::now(),
        );
        let result = acc.drain();
        assert!(result.is_some());
        let (data, checkpoints, _) = result.unwrap();
        assert_eq!(&data[..], b"drain me\n");
        assert!(checkpoints.contains_key(&sid(1)));
        assert!(acc.is_empty());
    }

    #[test]
    fn drain_empty_returns_none() {
        let mut acc = BatchAccumulator::new(100);
        assert!(acc.drain().is_none());
    }

    #[test]
    fn checkpoint_map_keeps_latest_per_source() {
        let mut acc = BatchAccumulator::new(1000);
        acc.ingest(
            Bytes::from_static(b"a\n"),
            vec![(sid(1), ByteOffset(2))],
            Instant::now(),
        );
        acc.ingest(
            Bytes::from_static(b"b\n"),
            vec![(sid(1), ByteOffset(4))],
            Instant::now(),
        );
        let (_, checkpoints, _) = acc.drain().unwrap();
        assert_eq!(checkpoints[&sid(1)], ByteOffset(4));
    }
}

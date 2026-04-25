use bytes::Bytes;

use crate::filter_hints::FilterHints;
use crate::format::FormatDecoder;
use crate::input::{CriMetadata, InputCadence, SourceEvent, InputSource};
#[cfg(test)]
use crate::poll_cadence::PollCadenceSignal;
use crate::tail::ByteOffset;
use ffwd_core::checkpoint_tracker::CheckpointTracker;
use ffwd_types::diagnostics::{ComponentHealth, ComponentStats};
use ffwd_types::pipeline::SourceId;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

/// Maximum remainder buffer size before discarding (prevents OOM on
/// input without newlines). Applied per source.
const MAX_REMAINDER_BYTES: usize = 2 * 1024 * 1024;

const INITIAL_CRI_METADATA_SPANS: usize = 16;
const INITIAL_CRI_TIMESTAMP_BYTES: usize = 1024;

/// Per-source state for framing and checkpoint tracking.
///
/// Each logical source (identified by `Option<SourceId>`) gets its own
/// remainder buffer, format processor, and checkpoint tracker. This
/// prevents cross-contamination between sources for both newline framing
/// and stateful format processing (e.g., CRI P/F aggregation).
struct SourceState {
    /// Partial-line bytes after the last newline.
    remainder: Vec<u8>,
    /// Per-source format processor (CRI aggregator state is source-scoped).
    format: FormatDecoder,
    /// Kani-proven checkpoint offset tracker. Tracks the relationship
    /// between file read position and the last complete newline boundary.
    tracker: CheckpointTracker,
    /// True when remainder bytes are known to start mid-line due to overflow
    /// truncation. The first completed line formed from this remainder must be
    /// discarded to avoid emitting a garbled fragment (#1030).
    overflow_tainted: bool,
}

impl SourceState {
    fn is_reclaimable(&self) -> bool {
        self.remainder.is_empty() && !self.overflow_tainted && !self.format.has_pending_state()
    }
}

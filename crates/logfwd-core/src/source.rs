// source.rs — Core source types for the log pipeline.
//
// Defines SourceId, SourceOutput, SourceEvent, BatchMetadata,
// SourceCheckpoint, and the Source trait used by all log-input
// implementations.

use std::io;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// SourceId
// ---------------------------------------------------------------------------

/// Stable identifier for a log source.
///
/// `id` is the numeric key used in checkpoints and metrics; `name` is the
/// human-readable label from configuration.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourceId {
    /// Numeric identifier, unique within a running pipeline.
    pub id: u64,
    /// Human-readable name from configuration (e.g., `"nginx-access"`).
    pub name: Arc<str>,
}

// ---------------------------------------------------------------------------
// SourceEvent
// ---------------------------------------------------------------------------

/// Lifecycle events emitted by a source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceEvent {
    /// The underlying file was rotated (new inode detected).
    Rotated,
    /// The underlying file was truncated.
    Truncated,
    /// The source has reached end-of-file and has no more data.
    Eof,
}

// ---------------------------------------------------------------------------
// BatchMetadata
// ---------------------------------------------------------------------------

/// Metadata attached to a [`SourceOutput::Structured`] batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchMetadata {
    /// Numeric identifier of the source that produced this batch.
    pub source_id: u64,
    /// Wall-clock timestamp (nanoseconds since Unix epoch) of the first
    /// record in the batch.
    pub timestamp_ns: u64,
}

// ---------------------------------------------------------------------------
// SourceOutput
// ---------------------------------------------------------------------------

/// Output produced by a [`Source`] on a successful [`Source::poll`].
pub enum SourceOutput {
    /// Raw bytes, typically NDJSON, not yet parsed into Arrow.
    Raw { json_lines: Vec<u8>, line_count: usize },
    /// Pre-parsed Arrow [`RecordBatch`] with accompanying metadata.
    Structured { batch: RecordBatch, metadata: BatchMetadata },
    /// A source lifecycle event (rotation, truncation, EOF).
    Event(SourceEvent),
}

// ---------------------------------------------------------------------------
// SourceCheckpoint
// ---------------------------------------------------------------------------

/// Durable read-position cursor for exactly-once delivery.
///
/// Serialized and persisted to disk so the pipeline can resume after a
/// restart without re-reading already-processed data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCheckpoint {
    /// Numeric identifier of the source this checkpoint belongs to.
    pub source_id: u64,
    /// Opaque byte sequence encoding the source's current position
    /// (e.g., a file byte offset encoded as little-endian u64).
    pub position: Vec<u8>,
    /// Wall-clock time (nanoseconds since Unix epoch) when this checkpoint
    /// was recorded.
    pub timestamp_ns: u64,
}

// ---------------------------------------------------------------------------
// Source trait
// ---------------------------------------------------------------------------

/// A log source that can be polled for output and acknowledged.
///
/// Implementations must be [`Send`] so the pipeline can move sources across
/// threads. All methods are blocking; there is no async runtime in this
/// codebase.
pub trait Source: Send {
    /// Poll for the next output.
    ///
    /// Returns `Ok(None)` when no data is currently available (non-blocking).
    /// Returns `Ok(Some(_))` when new data or an event is ready.
    fn poll(&mut self) -> io::Result<Option<SourceOutput>>;

    /// Acknowledge that the pipeline has durably processed up to
    /// `checkpoint`. The source may release any buffered state up to this
    /// position.
    fn ack(&mut self, checkpoint: &SourceCheckpoint);

    /// Return the stable identifier for this source.
    fn id(&self) -> &SourceId;

    /// Capture the current read position as a [`SourceCheckpoint`].
    fn checkpoint(&self) -> SourceCheckpoint;

    /// Perform a clean shutdown, flushing any pending state.
    fn shutdown(&mut self);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // --- SourceId hashing ---------------------------------------------------

    #[test]
    fn source_id_equal_ids_are_equal() {
        let a = SourceId { id: 42, name: Arc::from("pipeline") };
        let b = SourceId { id: 42, name: Arc::from("pipeline") };
        assert_eq!(a, b);
    }

    #[test]
    fn source_id_different_numeric_ids_not_equal() {
        let a = SourceId { id: 1, name: Arc::from("x") };
        let b = SourceId { id: 2, name: Arc::from("x") };
        assert_ne!(a, b);
    }

    #[test]
    fn source_id_different_names_not_equal() {
        let a = SourceId { id: 1, name: Arc::from("foo") };
        let b = SourceId { id: 1, name: Arc::from("bar") };
        assert_ne!(a, b);
    }

    #[test]
    fn source_id_equal_ids_hash_equal() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let hash_of = |s: &SourceId| -> u64 {
            let mut h = DefaultHasher::new();
            s.hash(&mut h);
            h.finish()
        };

        let a = SourceId { id: 99, name: Arc::from("auth") };
        let b = SourceId { id: 99, name: Arc::from("auth") };
        assert_eq!(hash_of(&a), hash_of(&b));
    }

    #[test]
    fn source_id_usable_as_hashmap_key() {
        let mut map: HashMap<SourceId, &str> = HashMap::new();
        let id = SourceId { id: 7, name: Arc::from("auth") };
        map.insert(id.clone(), "auth-logs");
        assert_eq!(map[&id], "auth-logs");
    }

    // --- SourceCheckpoint serialization -------------------------------------

    #[test]
    fn source_checkpoint_roundtrip_json() {
        let cp = SourceCheckpoint {
            source_id: 99,
            position: b"offset=12345".to_vec(),
            timestamp_ns: 1_700_000_000_000_000_000,
        };
        let json = serde_json::to_string(&cp).unwrap();
        let restored: SourceCheckpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(cp, restored);
    }

    #[test]
    fn source_checkpoint_serialized_field_names() {
        let cp = SourceCheckpoint {
            source_id: 1,
            position: vec![0, 1, 2],
            timestamp_ns: 0,
        };
        let json = serde_json::to_string(&cp).unwrap();
        assert!(json.contains("\"source_id\":1"), "missing source_id: {json}");
        assert!(json.contains("\"timestamp_ns\":0"), "missing timestamp_ns: {json}");
        assert!(json.contains("\"position\":"), "missing position: {json}");
    }

    #[test]
    fn source_checkpoint_empty_position_roundtrip() {
        let cp = SourceCheckpoint { source_id: 0, position: vec![], timestamp_ns: 0 };
        let json = serde_json::to_string(&cp).unwrap();
        let restored: SourceCheckpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(cp, restored);
    }
}

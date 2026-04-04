use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use logfwd_core::pipeline::SourceId;

use crate::filter_hints::FilterHints;
use crate::tail::{ByteOffset, FileTailer, TailConfig, TailEvent};

/// Events produced by an input source.
pub enum InputEvent {
    /// New data read from the source.
    ///
    /// `source_id` identifies which logical source produced the data (e.g.,
    /// which tailed file). `None` for push sources that don't track identity.
    Data {
        bytes: Vec<u8>,
        source_id: Option<SourceId>,
    },
    /// The underlying file was rotated (new inode).
    ///
    /// `source_id` identifies the source that was rotated. `None` for push
    /// sources or when the source identity is unknown.
    Rotated { source_id: Option<SourceId> },
    /// The underlying file was truncated.
    ///
    /// `source_id` identifies the source that was truncated. `None` for push
    /// sources or when the source identity is unknown.
    Truncated { source_id: Option<SourceId> },
    /// The source has been fully consumed and has no new data.
    ///
    /// Emitted once per "caught-up" transition (i.e., after the first poll
    /// cycle where reads find no new bytes).  Downstream components (e.g.
    /// `FramedInput`) use this to flush any partial-line remainder that was
    /// not terminated by a newline — a common situation for static files and
    /// for the last line written before a log rotation.
    ///
    /// `source_id` identifies the source that reached EOF. `None` for push
    /// sources or when the source identity is unknown.
    EndOfFile { source_id: Option<SourceId> },
}

/// Trait for input sources that produce raw bytes.
pub trait InputSource: Send {
    /// Poll for new events. Returns empty vec if no new data.
    fn poll(&mut self) -> io::Result<Vec<InputEvent>>;
    /// Name of this input (from config).
    fn name(&self) -> &str;

    /// Apply filter hints for predicate pushdown. Inputs that support
    /// pushdown use these to skip data early (e.g., XDP severity filtering).
    /// Default implementation ignores hints — correct but slower.
    fn apply_hints(&mut self, _hints: &FilterHints) {}

    /// Return checkpoint data for all active sources.
    ///
    /// For file inputs, returns `(SourceId, ByteOffset)` per tailed file.
    /// Default: empty (push sources, generators).
    fn checkpoint_data(&self) -> Vec<(SourceId, ByteOffset)> {
        vec![]
    }

    /// Restore a file offset by SourceId (fingerprint). Default: no-op.
    ///
    /// Used for checkpoint restore — the checkpoint stores fingerprint + offset.
    /// The input source finds the matching file by fingerprint, not path.
    fn set_offset_by_source(&mut self, _source_id: SourceId, _offset: u64) {}
}

/// An input source backed by a `FileTailer`.
pub struct FileInput {
    name: String,
    tailer: FileTailer,
}

impl FileInput {
    /// Create a new `FileInput` wrapping a `FileTailer`.
    pub fn new(name: String, paths: &[PathBuf], config: TailConfig) -> io::Result<Self> {
        let tailer = FileTailer::new(paths, config)?;
        Ok(FileInput { name, tailer })
    }

    /// Create a new `FileInput` from glob patterns.
    ///
    /// Patterns are expanded immediately and re-evaluated periodically to
    /// discover files created after startup (e.g., new Kubernetes pods).
    pub fn new_with_globs(name: String, patterns: &[&str], config: TailConfig) -> io::Result<Self> {
        let tailer = FileTailer::new_with_globs(patterns, config)?;
        Ok(FileInput { name, tailer })
    }
}

impl InputSource for FileInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        // Snapshot path-to-source_id mapping BEFORE polling. This is
        // critical because `tailer.poll()` may mutate internal state
        // (e.g., re-open a rotated file with a new fingerprint), which
        // would cause `source_id_for_path` to return the NEW identity
        // for bytes that were read from the OLD file.
        let source_id_snapshot: HashMap<PathBuf, Option<SourceId>> = self
            .tailer
            .file_paths()
            .into_iter()
            .map(|(sid, path)| (path, Some(sid)))
            .collect();

        let tail_events = self.tailer.poll()?;
        let mut events = Vec::with_capacity(tail_events.len());
        for te in tail_events {
            match te {
                TailEvent::Data { path, bytes } => {
                    // Use the snapshot for data events — the bytes were
                    // read from the file as it existed before poll().
                    // Fall back to live lookup for files opened during
                    // this poll (new files discovered by glob rescan).
                    let source_id = source_id_snapshot
                        .get(&path)
                        .copied()
                        .flatten()
                        .or_else(|| self.tailer.source_id_for_path(&path));
                    events.push(InputEvent::Data { bytes, source_id });
                }
                TailEvent::Rotated { path } => {
                    // After rotation, the old source_id is in the snapshot;
                    // the live tailer now has the new file's identity.
                    let source_id = source_id_snapshot
                        .get(&path)
                        .copied()
                        .flatten()
                        .or_else(|| self.tailer.source_id_for_path(&path));
                    events.push(InputEvent::Rotated { source_id });
                }
                TailEvent::Truncated { path } => {
                    let source_id = source_id_snapshot
                        .get(&path)
                        .copied()
                        .flatten()
                        .or_else(|| self.tailer.source_id_for_path(&path));
                    events.push(InputEvent::Truncated { source_id });
                }
                TailEvent::EndOfFile { path } => {
                    // EndOfFile comes from the live state (no-data poll),
                    // so live lookup is fine here — the file wasn't mutated.
                    let source_id = self.tailer.source_id_for_path(&path);
                    events.push(InputEvent::EndOfFile { source_id });
                }
            }
        }
        Ok(events)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn checkpoint_data(&self) -> Vec<(SourceId, ByteOffset)> {
        self.tailer.file_offsets()
    }

    fn set_offset_by_source(&mut self, source_id: SourceId, offset: u64) {
        if let Err(e) = self.tailer.set_offset_by_source(source_id, offset) {
            tracing::warn!(source_id = source_id.0, error = %e, "failed to restore offset");
        }
    }
}

use std::io;
use std::path::{Path, PathBuf};

use logfwd_core::pipeline::SourceId;

use crate::filter_hints::FilterHints;
use crate::tail::{ByteOffset, FileTailer, TailConfig, TailEvent};

/// Events produced by an input source.
#[non_exhaustive]
pub enum InputEvent {
    /// New data read from the source.
    Data { bytes: Vec<u8> },
    /// The underlying file was rotated (new inode).
    Rotated,
    /// The underlying file was truncated.
    Truncated,
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

    /// Return identity-to-path mappings for checkpoint persistence.
    ///
    /// Called on file open/rotation, not per-batch.
    /// Default: empty (push sources, generators).
    fn source_paths(&self) -> Vec<(SourceId, PathBuf)> {
        vec![]
    }

    /// Restore a file offset from checkpoint. Default: no-op.
    fn set_offset(&mut self, _path: &Path, _offset: u64) {}
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
        let tail_events = self.tailer.poll()?;
        let mut events = Vec::with_capacity(tail_events.len());
        for te in tail_events {
            match te {
                TailEvent::Data { bytes, .. } => {
                    events.push(InputEvent::Data { bytes });
                }
                TailEvent::Rotated { .. } => {
                    events.push(InputEvent::Rotated);
                }
                TailEvent::Truncated { .. } => {
                    events.push(InputEvent::Truncated);
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

    fn source_paths(&self) -> Vec<(SourceId, PathBuf)> {
        self.tailer.file_paths()
    }

    fn set_offset(&mut self, path: &Path, offset: u64) {
        if let Err(e) = self.tailer.set_offset(path, offset) {
            eprintln!("warn: failed to restore offset for {}: {e}", path.display());
        }
    }
}

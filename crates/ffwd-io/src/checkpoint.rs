//! Checkpoint persistence: tracks read progress for each source across restarts.
//!
//! # Design
//! - `SourceCheckpoint` stores the source id, optional file path, and byte offset.
//! - `CheckpointStore` is a trait so alternative back-ends can be added later.
//! - `FileCheckpointStore` serialises checkpoints to `{data_dir}/checkpoints.json`
//!   using an atomic write (write-to-tmp → fsync → rename) so a crash mid-write
//!   can never corrupt the last good checkpoint.
//! - `flush` is synchronous; the underlying I/O is small (one JSON file,
//!   typically a few KB) so it completes quickly.

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// SourceCheckpoint
// ---------------------------------------------------------------------------

/// Tracks how far a single source has been durably processed.
///
/// For file sources the `path` and `offset` fields are meaningful.
/// For network sources only `source_id` and `offset` are used.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceCheckpoint {
    /// Stable identifier for the source (e.g. xxh64 of the source name / path).
    pub source_id: u64,
    /// Absolute path to the file being tailed, if this is a file source.
    pub path: Option<PathBuf>,
    /// Byte offset up to which data has been durably processed.
    pub offset: u64,
}

// ---------------------------------------------------------------------------
// CheckpointStore trait
// ---------------------------------------------------------------------------

/// Persists reading progress for all sources.
pub trait CheckpointStore: Send {
    /// Update (or insert) an in-memory checkpoint for a source.
    ///
    /// This is cheap and side-effect-free; call it after every successful
    /// batch. Persistence happens on the next `flush`.
    fn update(&mut self, checkpoint: SourceCheckpoint);

    /// Flush all in-memory checkpoints to durable storage.
    ///
    /// Uses an atomic write (tmp file → fsync → rename) so a crash
    /// mid-write leaves the previous checkpoint intact.
    fn flush(&mut self) -> io::Result<()>;

    /// Return the last known checkpoint for `source_id`, if any.
    fn load(&self, source_id: u64) -> Option<SourceCheckpoint>;

    /// Return all stored checkpoints.
    fn load_all(&self) -> Vec<SourceCheckpoint>;
}

// ---------------------------------------------------------------------------
// FileCheckpointStore
// ---------------------------------------------------------------------------

/// A `CheckpointStore` backed by a single JSON file on disk.
///
/// ```text
/// {data_dir}/checkpoints.json      ← live checkpoint file
/// {data_dir}/checkpoints.json.tmp  ← written then renamed atomically
/// ```
pub struct FileCheckpointStore {
    data_dir: PathBuf,
    checkpoints: BTreeMap<u64, SourceCheckpoint>,
}

impl FileCheckpointStore {
    /// Open (or create) a checkpoint store rooted at `data_dir`.
    ///
    /// If `{data_dir}/checkpoints.json` already exists its contents are
    /// loaded into memory so previous progress survives a restart.
    pub fn open(data_dir: impl Into<PathBuf>) -> io::Result<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir)?;

        let checkpoints_path = data_dir.join("checkpoints.json");
        let checkpoints = if checkpoints_path.exists() {
            let bytes = std::fs::read(&checkpoints_path)?;
            match serde_json::from_slice::<Vec<SourceCheckpoint>>(&bytes) {
                Ok(list) => list.into_iter().map(|c| (c.source_id, c)).collect(),
                Err(e) => {
                    tracing::warn!(
                        path = %checkpoints_path.display(),
                        error = %e,
                        "checkpoint file is corrupt — starting fresh"
                    );
                    BTreeMap::new()
                }
            }
        } else {
            BTreeMap::new()
        };

        Ok(Self {
            data_dir,
            checkpoints,
        })
    }

    /// Return the path to the live checkpoint file.
    pub fn checkpoints_path(&self) -> PathBuf {
        self.data_dir.join("checkpoints.json")
    }
}

impl CheckpointStore for FileCheckpointStore {
    fn update(&mut self, checkpoint: SourceCheckpoint) {
        self.checkpoints.insert(checkpoint.source_id, checkpoint);
    }

    /// Atomically persist all checkpoints to disk.
    ///
    /// Steps:
    /// 1. Serialise in-memory checkpoints to JSON.
    /// 2. Write to `checkpoints.json.tmp`.
    /// 3. `fsync` the tmp file.
    /// 4. Rename `checkpoints.json.tmp` → `checkpoints.json`.
    /// 5. `fsync` the parent directory so the rename is durable on
    ///    crash (required on Linux/ext4). (#386)
    fn flush(&mut self) -> io::Result<()> {
        let list: Vec<&SourceCheckpoint> = self.checkpoints.values().collect();
        let json = serde_json::to_vec_pretty(&list)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        crate::atomic_write::atomic_write_file(&self.checkpoints_path(), &json)
    }

    fn load(&self, source_id: u64) -> Option<SourceCheckpoint> {
        self.checkpoints.get(&source_id).cloned()
    }

    fn load_all(&self) -> Vec<SourceCheckpoint> {
        self.checkpoints.values().cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Return a sensible default data directory.
///
/// Checks `$FFWD_DATA_DIR` first, then `$LOGFWD_DATA_DIR` for backward
/// compatibility. For root processes the default is `/var/lib/ffwd`, falling
/// back to `/var/lib/logfwd` when the new path does not yet exist but the
/// legacy path does. Non-root processes follow the same pattern with
/// `$HOME/.ffwd` / `$HOME/.logfwd`.
pub fn default_data_dir() -> PathBuf {
    // Check for an explicit override via environment variable first.
    if let Ok(dir) = std::env::var("FFWD_DATA_DIR") {
        return PathBuf::from(dir);
    }
    if let Ok(dir) = std::env::var("LOGFWD_DATA_DIR") {
        return PathBuf::from(dir);
    }

    #[cfg(unix)]
    {
        if libc_geteuid() == 0 {
            let new_path = PathBuf::from("/var/lib/ffwd");
            if new_path.exists() {
                return new_path;
            }
            let legacy_path = PathBuf::from("/var/lib/logfwd");
            if legacy_path.exists() {
                return legacy_path;
            }
            return new_path;
        }
    }

    if let Ok(home) = std::env::var("HOME") {
        let home = PathBuf::from(home);
        let new_path = home.join(".ffwd");
        if new_path.exists() {
            return new_path;
        }
        let legacy_path = home.join(".logfwd");
        if legacy_path.exists() {
            return legacy_path;
        }
        new_path
    } else {
        PathBuf::from(".ffwd")
    }
}

#[cfg(unix)]
fn libc_geteuid() -> u32 {
    // SAFETY: `geteuid` has no preconditions and returns the current process
    // effective uid on all Unix targets.
    unsafe { libc::geteuid() }
}

// ---------------------------------------------------------------------------
// InMemoryCheckpointStore — for testing
// ---------------------------------------------------------------------------

/// A `CheckpointStore` backed by an in-memory BTreeMap.
///
/// Useful for deterministic testing: no file I/O, inspectable state.
pub struct InMemoryCheckpointStore {
    checkpoints: BTreeMap<u64, SourceCheckpoint>,
    flush_count: u64,
}

impl InMemoryCheckpointStore {
    /// Create an empty in-memory store.
    pub fn new() -> Self {
        Self {
            checkpoints: BTreeMap::new(),
            flush_count: 0,
        }
    }

    /// Number of times `flush()` was called.
    pub fn flush_count(&self) -> u64 {
        self.flush_count
    }
}

impl Default for InMemoryCheckpointStore {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckpointStore for InMemoryCheckpointStore {
    fn update(&mut self, checkpoint: SourceCheckpoint) {
        self.checkpoints.insert(checkpoint.source_id, checkpoint);
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_count += 1;
        Ok(())
    }

    fn load(&self, source_id: u64) -> Option<SourceCheckpoint> {
        self.checkpoints.get(&source_id).cloned()
    }

    fn load_all(&self) -> Vec<SourceCheckpoint> {
        self.checkpoints.values().cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_checkpoint(source_id: u64, path: &str, offset: u64) -> SourceCheckpoint {
        SourceCheckpoint {
            source_id,
            path: Some(PathBuf::from(path)),
            offset,
        }
    }

    /// Open a fresh store, add checkpoints, flush, then re-open and verify.
    #[test]
    fn test_persist_and_reload() {
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().to_path_buf();

        // Write checkpoints.
        let mut store = FileCheckpointStore::open(&data_dir).unwrap();
        store.update(make_checkpoint(1, "/var/log/app.log", 1024));
        store.update(make_checkpoint(2, "/var/log/nginx.log", 4096));
        store.flush().unwrap();

        // Reload from disk.
        let store2 = FileCheckpointStore::open(&data_dir).unwrap();
        let cp1 = store2.load(1).expect("checkpoint 1 missing after reload");
        let cp2 = store2.load(2).expect("checkpoint 2 missing after reload");

        assert_eq!(cp1.offset, 1024);
        assert_eq!(cp1.path, Some(PathBuf::from("/var/log/app.log")));
        assert_eq!(cp2.offset, 4096);
        assert_eq!(cp2.path, Some(PathBuf::from("/var/log/nginx.log")));
    }

    /// `load_all` returns every stored checkpoint.
    #[test]
    fn test_load_all() {
        let dir = TempDir::new().unwrap();
        let mut store = FileCheckpointStore::open(dir.path()).unwrap();
        store.update(make_checkpoint(10, "/a.log", 100));
        store.update(make_checkpoint(20, "/b.log", 200));
        store.flush().unwrap();

        let store2 = FileCheckpointStore::open(dir.path()).unwrap();
        let mut all = store2.load_all();
        all.sort_by_key(|c| c.source_id);
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].source_id, 10);
        assert_eq!(all[1].source_id, 20);
    }

    /// Updating a checkpoint for the same source replaces the previous entry.
    #[test]
    fn test_update_replaces_existing() {
        let dir = TempDir::new().unwrap();
        let mut store = FileCheckpointStore::open(dir.path()).unwrap();
        store.update(make_checkpoint(1, "/app.log", 100));
        store.update(make_checkpoint(1, "/app.log", 500));
        store.flush().unwrap();

        let store2 = FileCheckpointStore::open(dir.path()).unwrap();
        let cp = store2.load(1).unwrap();
        assert_eq!(cp.offset, 500, "offset should be the latest value");
    }

    /// A checkpoint that has been flushed but not yet overwritten survives
    /// a simulated crash (we just re-open the store without flushing new data).
    #[test]
    fn test_crash_recovery() {
        let dir = TempDir::new().unwrap();

        // "Process 1": write and flush.
        {
            let mut store = FileCheckpointStore::open(dir.path()).unwrap();
            store.update(make_checkpoint(42, "/crash.log", 8192));
            store.flush().unwrap();
        }

        // Simulate crash: "Process 2" opens the store without previous state.
        let store = FileCheckpointStore::open(dir.path()).unwrap();
        let cp = store
            .load(42)
            .expect("checkpoint lost after simulated crash");
        assert_eq!(cp.offset, 8192);
    }

    /// Flush is atomic: if the final checkpoint file exists it is always valid JSON.
    #[test]
    fn test_atomic_write_produces_valid_json() {
        let dir = TempDir::new().unwrap();
        let mut store = FileCheckpointStore::open(dir.path()).unwrap();
        store.update(make_checkpoint(99, "/atomic.log", 0));
        store.flush().unwrap();

        let bytes = std::fs::read(store.checkpoints_path()).unwrap();
        let parsed: Vec<SourceCheckpoint> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.len(), 1);
    }

    /// A fresh store (no file on disk) returns `None` for any `load`.
    #[test]
    fn test_empty_store() {
        let dir = TempDir::new().unwrap();
        let store = FileCheckpointStore::open(dir.path()).unwrap();
        assert!(store.load(1).is_none());
        assert!(store.load_all().is_empty());
    }

    /// #386: flush performs directory fsync after rename.
    ///
    /// We can't directly test fsync semantics (that's a kernel guarantee), but
    /// we verify flush succeeds and the file exists after a full cycle. The
    /// code path now opens the parent directory and calls sync_all() on it.
    #[test]
    fn test_flush_directory_fsync() {
        let dir = TempDir::new().unwrap();
        let mut store = FileCheckpointStore::open(dir.path()).unwrap();
        store.update(make_checkpoint(1, "/var/log/app.log", 2048));
        // This exercises the full flush path including directory fsync.
        store.flush().unwrap();

        // Verify the checkpoint file exists and is valid.
        let bytes = std::fs::read(store.checkpoints_path()).unwrap();
        let parsed: Vec<SourceCheckpoint> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].offset, 2048);
    }

    /// A corrupt checkpoint file should not prevent the store from opening.
    /// Instead it logs a warning and starts with empty state.
    #[test]
    fn test_corrupt_checkpoint_recovers() {
        let dir = TempDir::new().unwrap();
        let cp_path = dir.path().join("checkpoints.json");
        std::fs::write(&cp_path, b"NOT VALID JSON {{{").unwrap();

        let store = FileCheckpointStore::open(dir.path())
            .expect("open should succeed despite corrupt file");
        assert!(store.load_all().is_empty(), "should start with empty state");
    }

    /// `default_data_dir` returns a non-empty path.
    #[test]
    fn test_default_data_dir() {
        let p = default_data_dir();
        assert!(!p.as_os_str().is_empty());
    }
}

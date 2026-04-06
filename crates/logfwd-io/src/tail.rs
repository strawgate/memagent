//! File tailer: watches log files for new data and reads it as it arrives.
//!
//! Design decisions (from research):
//! - Watch the DIRECTORY, not individual files (editors do atomic save via rename)
//! - Use notify crate (kqueue on macOS, inotify on Linux) as a latency hint
//! - Poll as safety net (inotify misses events on NFS, overlayfs, queue overflow)
//! - Track files by (device, inode, fingerprint) to handle rotation and inode reuse
//! - Checkpoint offsets for crash recovery (atomic write via rename)
//!
//! The tailer yields raw byte chunks to the pipeline. It does NOT parse lines —
//! that's the pipeline's job.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use logfwd_types::pipeline::SourceId;

/// Byte offset within a file. Newtype prevents mixing with SourceId.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteOffset(pub u64);

/// Identity of a file based on device + inode + content fingerprint.
/// Survives renames. Detects inode reuse via fingerprint mismatch.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FileIdentity {
    pub device: u64,
    pub inode: u64,
    pub fingerprint: u64,
}

impl FileIdentity {
    /// Derive a stable `SourceId` from the compound key (device, inode, fingerprint).
    ///
    /// Hashing all three fields prevents collisions between files that share
    /// the same first N bytes (same fingerprint) but live on different inodes.
    /// Two files on the same inode+device are the same file, so the fingerprint
    /// differentiates after inode reuse (e.g., log rotation).
    pub fn source_id(&self) -> SourceId {
        // Empty file sentinel: fingerprint 0 means no data to checkpoint.
        if self.fingerprint == 0 {
            return SourceId(0);
        }
        let mut h = xxhash_rust::xxh64::Xxh64::new(0);
        h.update(&self.device.to_le_bytes());
        h.update(&self.inode.to_le_bytes());
        h.update(&self.fingerprint.to_le_bytes());
        SourceId(h.digest())
    }
}

/// State tracked per tailed file.
struct TailedFile {
    identity: FileIdentity,
    file: File,
    offset: u64,
    /// Last time we successfully read new data.
    last_read: Instant,
    /// Whether we have already emitted an `EndOfFile` event for the current
    /// "no new data" streak.  Reset to `false` whenever new data is read so
    /// that a fresh `EndOfFile` can be emitted the next time reads stall.
    eof_emitted: bool,
}

/// Saved state for a file evicted from the open-file LRU cache.
struct EvictedFile {
    identity: FileIdentity,
    offset: u64,
    path: PathBuf,
    source_id: SourceId,
}

/// Internal result from read_new_data — distinguishes truncation from no-data.
enum ReadResult {
    /// New data available.
    Data(Vec<u8>),
    /// File was truncated, then new data read from beginning.
    /// Downstream should clear remainder BEFORE processing the new data.
    TruncatedThenData(Vec<u8>),
    /// File was truncated but no new data yet.
    Truncated,
    /// No new data.
    NoData,
}

/// Events emitted by the tailer.
#[non_exhaustive]
pub enum TailEvent {
    /// New data available. The Vec is raw bytes read from the file.
    /// NOT necessarily aligned on line boundaries — the pipeline handles that.
    Data {
        path: PathBuf,
        bytes: Vec<u8>,
        /// Identity of the source that produced this data, computed at read
        /// time (before any rotation mutates the tailer's internal state).
        source_id: Option<SourceId>,
    },
    /// A file was rotated (old file at path replaced by new file).
    Rotated {
        path: PathBuf,
        /// Identity of the *old* (pre-rotation) file.
        source_id: Option<SourceId>,
    },
    /// A file was truncated (copytruncate rotation).
    Truncated {
        path: PathBuf,
        /// Identity of the file at the time truncation was detected.
        source_id: Option<SourceId>,
    },
    /// The file has been fully read and contains no new data.
    ///
    /// Emitted once after every transition from "has data" to "no data"
    /// (i.e., the first poll cycle after all available bytes have been
    /// consumed). This lets downstream components flush any partial-line
    /// remainder that was not terminated by a newline.
    ///
    /// The event is suppressed on subsequent polls until new data arrives,
    /// at which point the flag resets so a fresh `EndOfFile` can be emitted
    /// the next time the file is caught up.
    EndOfFile {
        path: PathBuf,
        /// Identity of the source that reached EOF.
        source_id: Option<SourceId>,
    },
}

/// Configuration for the file tailer.
#[derive(Clone, Debug)]
pub struct TailConfig {
    /// How often to poll for changes as a safety net (milliseconds).
    pub poll_interval_ms: u64,
    /// Read buffer size per file.
    pub read_buf_size: usize,
    /// Fingerprint size: how many bytes to hash at the start of the file.
    pub fingerprint_bytes: usize,
    /// Whether to start reading from the end of existing files (true)
    /// or from the beginning (false).
    pub start_from_end: bool,
    /// How often to re-evaluate glob patterns to discover new files (milliseconds).
    /// Set to 0 to disable periodic glob rescanning.
    pub glob_rescan_interval_ms: u64,
    /// Maximum number of file descriptors to keep open simultaneously.
    /// When the limit is exceeded, the least-recently-read files are closed
    /// until the count is within the limit. Evicted files are re-opened
    /// automatically on the next poll if they have new data.
    pub max_open_files: usize,
    /// Maximum bytes read from one file during a single poll cycle.
    /// Enforces fairness across files under heavy write load.
    pub per_file_read_budget_bytes: usize,
}

impl Default for TailConfig {
    fn default() -> Self {
        TailConfig {
            poll_interval_ms: 250,
            read_buf_size: 256 * 1024,
            fingerprint_bytes: 1024,
            start_from_end: true,
            glob_rescan_interval_ms: 5000,
            max_open_files: 1024,
            per_file_read_budget_bytes: 256 * 1024,
        }
    }
}

/// Compute the fingerprint of a file: xxhash64 of the first N bytes.
fn compute_fingerprint(file: &mut File, max_bytes: usize) -> io::Result<u64> {
    let pos = file.stream_position()?;
    file.seek(SeekFrom::Start(0))?;

    let mut buf = vec![0u8; max_bytes];
    let n = file.read(&mut buf)?;

    file.seek(SeekFrom::Start(pos))?;

    if n == 0 {
        // Empty file gets a sentinel fingerprint.
        return Ok(0);
    }
    Ok(xxhash_rust::xxh64::xxh64(&buf[..n], 0))
}

/// Build a FileIdentity for a path.
fn identify_file(path: &Path, fingerprint_bytes: usize) -> io::Result<FileIdentity> {
    let mut file = File::open(path)?;
    let meta = file.metadata()?;
    let fingerprint = compute_fingerprint(&mut file, fingerprint_bytes)?;
    Ok(FileIdentity {
        device: meta.dev(),
        inode: meta.ino(),
        fingerprint,
    })
}

/// Extract the root directory from a glob pattern — the longest prefix path
/// before the first wildcard character (`*`, `?`, `[`, `{`).
///
/// Examples:
/// - `/var/log/*.log` → `/var/log`
/// - `/var/log/**/*.log` → `/var/log`
/// - `*.log` → `.`
fn glob_root(pattern: &str) -> PathBuf {
    let wildcard_pos = pattern.find(['*', '?', '[', '{']).unwrap_or(pattern.len());
    let prefix = &pattern[..wildcard_pos];
    if prefix.is_empty() {
        return PathBuf::from(".");
    }
    // If the prefix ends with `/`, the wildcard starts a new path segment,
    // so everything before that `/` is a concrete directory.
    // E.g. "/var/log/*.log" → prefix "/var/log/" → root "/var/log"
    // If it doesn't end with `/`, the wildcard is mid-filename.
    // E.g. "/var/log/app*.log" → prefix "/var/log/app" → root "/var/log"
    if prefix.ends_with('/') {
        let trimmed = prefix.trim_end_matches('/');
        if trimmed.is_empty() {
            PathBuf::from("/")
        } else {
            PathBuf::from(trimmed)
        }
    } else {
        let parent = Path::new(prefix).parent().unwrap_or(Path::new(""));
        if parent.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            parent.to_path_buf()
        }
    }
}

/// Compute the maximum directory depth a glob pattern can match.
/// Counts path components below the root directory.
/// Returns `None` if the pattern contains `**` (unbounded depth).
fn glob_max_depth(pattern: &str) -> Option<usize> {
    if pattern.contains("**") {
        return None;
    }
    let root = glob_root(pattern);
    // Exclude the CurDir (`.`) component so relative roots don't
    // inflate the depth count — Path::new(".").components().count() == 1
    // but logically the root has 0 "meaningful" segments.
    let root_depth = root
        .components()
        .filter(|c| *c != std::path::Component::CurDir)
        .count();
    let total_depth = Path::new(pattern)
        .components()
        .filter(|c| *c != std::path::Component::CurDir)
        .count();
    // Ensure at least depth 1 — a file is always at depth ≥1 from its directory.
    Some(total_depth.saturating_sub(root_depth).max(1))
}

/// Expand a list of glob patterns into the set of matching `PathBuf` values.
///
/// Uses `globset::GlobSet` for fast multi-pattern matching and `walkdir` for
/// directory traversal with symlink loop detection.
///
/// Patterns that match no files are silently skipped. Errors from directory
/// traversal (e.g., permission denied) are also skipped.
fn expand_glob_patterns(patterns: &[&str]) -> Vec<PathBuf> {
    if patterns.is_empty() {
        return Vec::new();
    }

    // Build a GlobSet for multi-pattern matching.
    let mut builder = globset::GlobSetBuilder::new();
    for pattern in patterns {
        match globset::GlobBuilder::new(pattern)
            .literal_separator(true) // `*` does not cross `/`
            .build()
        {
            Ok(g) => {
                builder.add(g);
            }
            Err(e) => {
                tracing::warn!(pattern, error = %e, "tail.invalid_glob_pattern");
            }
        }
    }
    let glob_set = match builder.build() {
        Ok(gs) => gs,
        Err(e) => {
            tracing::warn!(error = %e, "tail.globset_build_failed");
            return Vec::new();
        }
    };

    // Collect root directories and per-root max depth for bounded traversal.
    let mut roots: Vec<(PathBuf, Option<usize>)> = Vec::new();
    for pattern in patterns {
        let root = glob_root(pattern);
        let depth = glob_max_depth(pattern);
        // Merge: if the same root already exists, take the larger depth.
        if let Some(existing) = roots.iter_mut().find(|(r, _)| r == &root) {
            existing.1 = match (existing.1, depth) {
                (None, _) | (_, None) => None, // unbounded wins
                (Some(a), Some(b)) => Some(a.max(b)),
            };
        } else {
            roots.push((root, depth));
        }
    }

    let mut paths = Vec::new();
    for (root, max_depth) in &roots {
        let mut walker = walkdir::WalkDir::new(root).follow_links(true);
        if let Some(d) = max_depth {
            walker = walker.max_depth(*d);
        }
        for entry in walker.into_iter().filter_map(Result::ok) {
            if !entry.file_type().is_file() {
                continue;
            }
            // Normalize cwd-relative paths for matching. WalkDir from root "."
            // may return either "app.log" or "./app.log" depending on platform
            // and entry depth. Try all forms so both bare patterns like "*.log"
            // and prefixed patterns like "./*.log" match correctly (#1375).
            let entry_path = entry.path();
            let stripped = entry_path.strip_prefix(".").unwrap_or(entry_path);
            let prefixed = Path::new(".").join(stripped);
            if glob_set.is_match(entry_path)
                || glob_set.is_match(stripped)
                || glob_set.is_match(&prefixed)
            {
                paths.push(entry.into_path());
            }
        }
    }
    paths
}

// ---------------------------------------------------------------------------
// FileDiscovery — filesystem watching and file discovery logic
// ---------------------------------------------------------------------------

/// Owns the filesystem watcher and file/glob discovery state.
///
/// Separated from `FileReader` so that discovery logic (watch registration,
/// glob expansion, rotation detection) is decoupled from byte-level I/O.
struct FileDiscovery {
    /// Notify watcher for filesystem events.
    watcher: notify::RecommendedWatcher,
    /// Directories currently registered with the notify watcher.
    watched_dirs: HashSet<PathBuf>,
    /// Glob patterns to re-evaluate periodically for new file discovery.
    glob_patterns: Vec<String>,
    /// Paths we've been asked to watch (literal paths, including those discovered via globs).
    watch_paths: Vec<PathBuf>,
    /// Channel receiving filesystem events from the watcher.
    fs_events: crossbeam_channel::Receiver<notify::Result<notify::Event>>,
    /// Last time we re-evaluated glob patterns.
    last_glob_rescan: Instant,
}

impl FileDiscovery {
    /// Register a directory with the notify watcher if it has not been registered yet.
    fn watch_dir(&mut self, dir: &Path) -> io::Result<()> {
        if self.watched_dirs.insert(dir.to_path_buf()) {
            use notify::Watcher;
            self.watcher
                .watch(dir, notify::RecursiveMode::NonRecursive)
                .map_err(io::Error::other)?;
        }
        Ok(())
    }

    /// Re-evaluate all stored glob patterns and start tailing any newly-discovered files.
    ///
    /// Already-watched paths are skipped to avoid duplicate entries.
    fn rescan_globs(&mut self, reader: &mut FileReader) -> bool {
        let mut had_error = false;
        if self.glob_patterns.is_empty() {
            return had_error;
        }

        let pattern_refs: Vec<&str> = self.glob_patterns.iter().map(String::as_str).collect();
        let candidates = expand_glob_patterns(&pattern_refs);

        // Canonicalize for dedup, falling back to raw path if resolve fails (#799).
        let existing: HashSet<PathBuf> = self
            .watch_paths
            .iter()
            .map(|p| fs::canonicalize(p).unwrap_or_else(|_| p.clone()))
            .collect();
        let new_paths: Vec<PathBuf> = candidates
            .into_iter()
            .filter(|p| {
                fs::canonicalize(p).map_or_else(
                    |_| !existing.contains(p), // fallback to raw path check
                    |c| !existing.contains(&c),
                )
            })
            .collect();

        for path in new_paths {
            // Watch the parent directory for future events.
            if let Some(parent) = path.parent()
                && let Err(e) = self.watch_dir(parent)
            {
                tracing::warn!(path = %parent.display(), error = %e, "tail.watch_dir_failed");
                had_error = true;
            }

            // New files from glob discovery respect the start_from_end config.
            if let Err(e) = reader.open_file_at(&path, reader.config.start_from_end) {
                tracing::warn!(path = %path.display(), error = %e, "tail.open_failed");
                had_error = true;
            }

            self.watch_paths.push(path);
        }
        had_error
    }

    /// Drain filesystem event notifications. Returns whether something changed.
    fn drain_events(&self) -> (bool, bool) {
        let mut something_changed = false;
        let mut had_error = false;
        while let Ok(res) = self.fs_events.try_recv() {
            match res {
                Ok(_event) => {
                    something_changed = true;
                }
                Err(e) => {
                    tracing::error!(error = %e, "tail.fs_event_error");
                    had_error = true;
                }
            }
        }
        (something_changed, had_error)
    }

    /// Check for new or rotated files among the watched paths.
    ///
    /// For rotated files, drains remaining data from the old fd before
    /// switching to the new file (always read from the beginning).
    /// For newly-appeared files, respects `config.start_from_end`.
    fn detect_changes(&self, reader: &mut FileReader, events: &mut Vec<TailEvent>) -> bool {
        let mut had_error = false;
        let watch_paths = self.watch_paths.clone();
        for path in &watch_paths {
            if !path.exists() {
                continue;
            }

            let current_identity = match identify_file(path, reader.config.fingerprint_bytes) {
                Ok(id) => id,
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "tail.identify_failed");
                    had_error = true;
                    continue;
                }
            };

            // Check for rotation or new file — borrow released before any mutation.
            //
            // We only consider it a rotation if the device/inode changed.
            // We ignore fingerprint changes for already-open files because
            // the fingerprint can change if the file was very small (<fingerprint_bytes)
            // and then grew. The open FD we hold is still valid for the same
            // logical file.
            //
            // Actual copytruncate rotation is handled separately via the size
            // check in `read_new_data`.
            let is_rotated = reader.files.get(path).is_some_and(|tailed| {
                tailed.identity.device != current_identity.device
                    || tailed.identity.inode != current_identity.inode
            });
            let is_new = !reader.files.contains_key(path);

            if is_rotated {
                // Capture the old file's identity before any mutation so that
                // drained bytes and the Rotated event carry the correct source_id.
                let pre_rotate_source_id = reader.source_id_for_path(path);

                // Drain any bytes written to the old fd after the last read but
                // before the rename.  The kernel keeps the old inode alive while
                // our File handle is open, so these bytes are still readable.
                had_error |= reader.drain_file(path, pre_rotate_source_id, events);

                // Now that the old fd is fully drained, emit the rotation event
                // and switch to the new file.
                events.push(TailEvent::Rotated {
                    path: path.clone(),
                    source_id: pre_rotate_source_id,
                });
                let _ = reader.files.remove(path);
                if let Err(e) = reader.open_file_at(path, false) {
                    tracing::warn!(path = %path.display(), error = %e, "tail.open_after_rotation_failed");
                    had_error = true;
                }
            } else if is_new {
                if let Err(e) = reader.open_file_at(path, reader.config.start_from_end) {
                    tracing::warn!(path = %path.display(), error = %e, "tail.open_new_file_failed");
                    had_error = true;
                }
            }
        }
        had_error
    }

    /// Remove entries for files that have been unlinked (nlink == 0).
    ///
    /// Using nlink instead of !path.exists() avoids data loss: on Unix a
    /// file can be unlinked while the FD is still open, so the path
    /// disappears but unread data remains readable through the FD.
    fn cleanup_deleted(&mut self, reader: &mut FileReader, events: &mut Vec<TailEvent>) -> bool {
        let mut had_error = false;
        let deleted: Vec<PathBuf> = reader
            .files
            .iter()
            .filter(|(_, tailed)| {
                tailed
                    .file
                    .metadata()
                    .map(|m| m.nlink() == 0)
                    .unwrap_or(true)
            })
            .map(|(path, _)| path.clone())
            .collect();
        for path in &deleted {
            // Capture source_id before removing the file entry.
            let source_id = reader.source_id_for_path(path);
            // Drain any remaining data before closing the FD.
            had_error |= reader.drain_file(path, source_id, events);
            reader.files.remove(path);
            reader.evicted_offsets.remove(path); // Bug G: prevent unbounded leak
        }
        // Remove deleted paths from watch_paths when using glob patterns so
        // the list does not grow unboundedly with file churn. (#810)
        // Glob-discovered paths will be re-added by the next rescan_globs() call
        // when the file reappears.  Literal-path tailers (glob_patterns empty)
        // must keep deleted paths so they detect the file if it is re-created.
        if !self.glob_patterns.is_empty() && !deleted.is_empty() {
            let deleted_set: HashSet<&PathBuf> = deleted.iter().collect();
            self.watch_paths.retain(|p| !deleted_set.contains(p));
        }
        had_error
    }
}

// ---------------------------------------------------------------------------
// FileReader — open file descriptors and byte reading
// ---------------------------------------------------------------------------

/// Owns the open file descriptors, read buffer, and byte-level I/O.
///
/// Separated from `FileDiscovery` so that reading logic (open, seek, read,
/// truncation detection, LRU eviction) is decoupled from watch/glob state.
struct FileReader {
    /// Files we're actively tailing, keyed by canonical path.
    files: HashMap<PathBuf, TailedFile>,
    /// Read buffer, reused across reads to avoid allocation.
    read_buf: Vec<u8>,
    /// Saved state for files evicted from the open-file LRU cache.
    /// When a file is re-opened after eviction, we seek to the saved offset
    /// to avoid duplicating or losing data. The identity is retained so that
    /// `file_offsets()` can include evicted files in checkpoint data (#697)
    /// and `open_file_at` can verify the fingerprint still matches (#817).
    evicted_offsets: HashMap<PathBuf, EvictedFile>,
    /// Configuration for read buffer size, fingerprint bytes, etc.
    config: TailConfig,
}

impl FileReader {
    /// Maximum bytes to read from a single file per poll cycle.
    /// Prevents OOM when a file grows significantly between polls (#800).
    const MAX_READ_PER_POLL: usize = 4 * 1024 * 1024; // 4 MiB

    /// Open and start tailing a file.
    ///
    /// If `start_from_end` is true, seeks to EOF. If the file was previously
    /// evicted from the LRU cache, restores the saved offset instead.
    ///
    /// Takes `start_from_end` as a parameter (Bug E) instead of reading
    /// from `self.config` to avoid the fragile save/restore pattern.
    fn open_file_at(&mut self, path: &Path, start_from_end: bool) -> io::Result<()> {
        let identity = identify_file(path, self.config.fingerprint_bytes)?;
        let mut file = File::open(path)?;
        let file_size = file.metadata()?.len();

        let offset = if let Some(evicted) = self.evicted_offsets.remove(path) {
            // Verify the file identity still matches before restoring the
            // saved offset. If the file was deleted and a new file appeared
            // at the same path, the fingerprint will differ and we must not
            // seek to the stale offset — that would skip data. (#817)
            if evicted.identity == identity {
                let safe_offset = if evicted.offset > file_size {
                    tracing::warn!(
                        path = %path.display(),
                        saved_offset = evicted.offset,
                        file_size,
                        "evicted offset exceeds file size — resetting to 0"
                    );
                    0
                } else {
                    evicted.offset
                };
                file.seek(SeekFrom::Start(safe_offset))?
            } else {
                tracing::warn!(
                    path = %path.display(),
                    evicted_identity = ?evicted.identity,
                    current_identity = ?identity,
                    "evicted offset identity mismatch — ignoring saved offset"
                );
                if start_from_end {
                    file.seek(SeekFrom::End(0))?
                } else {
                    0
                }
            }
        } else if start_from_end {
            file.seek(SeekFrom::End(0))?
        } else {
            0
        };

        self.files.insert(
            path.to_path_buf(),
            TailedFile {
                identity,
                file,
                offset,
                last_read: Instant::now(),
                eof_emitted: false,
            },
        );

        Ok(())
    }

    /// Read new data from a file, capped at [`Self::MAX_READ_PER_POLL`].
    /// Returns [`ReadResult::Truncated`] or [`ReadResult::TruncatedThenData`]
    /// if the file was truncated since the last read.
    fn read_new_data(&mut self, path: &Path) -> io::Result<ReadResult> {
        let tailed = match self.files.get_mut(path) {
            Some(t) => t,
            None => return Ok(ReadResult::NoData),
        };

        // Check current file size.
        let meta = tailed.file.metadata()?;
        let current_size = meta.len();

        let was_truncated = current_size < tailed.offset;
        if was_truncated {
            // File was truncated (copytruncate rotation) (#796).
            // Reset offset, fingerprint, and eof flag.
            tailed.offset = 0;
            tailed.eof_emitted = false; // Bug A: allow fresh EndOfFile after truncation
            tailed.file.seek(SeekFrom::Start(0))?;
            tailed.identity.fingerprint =
                compute_fingerprint(&mut tailed.file, self.config.fingerprint_bytes)?;
            tailed.file.seek(SeekFrom::Start(0))?;
        }

        if current_size <= tailed.offset {
            return Ok(if was_truncated {
                ReadResult::Truncated
            } else {
                ReadResult::NoData
            });
        }

        // Read available bytes with per-file fairness budget (#801) and
        // global safety cap (#800).
        let per_file_budget = self
            .config
            .per_file_read_budget_bytes
            .clamp(1, Self::MAX_READ_PER_POLL);
        let mut result = Vec::with_capacity(self.config.read_buf_size);
        loop {
            let remaining = per_file_budget.saturating_sub(result.len());
            if remaining == 0 {
                break; // continue on next poll
            }
            let read_len = remaining.min(self.read_buf.len());
            let n = tailed.file.read(&mut self.read_buf[..read_len])?;
            if n == 0 {
                break;
            }
            result.extend_from_slice(&self.read_buf[..n]);
            tailed.offset += n as u64;
        }

        if result.is_empty() {
            return Ok(if was_truncated {
                ReadResult::Truncated
            } else {
                ReadResult::NoData
            });
        }

        tailed.last_read = Instant::now();

        // Re-fingerprint files that started empty (fingerprint 0). Without
        // this, files that were empty at open time never acquire a real
        // fingerprint and stay invisible to source-aware checkpointing.
        if tailed.identity.fingerprint == 0 {
            let current_pos = tailed.file.stream_position()?;
            tailed.file.seek(SeekFrom::Start(0))?;
            let new_fp = compute_fingerprint(&mut tailed.file, self.config.fingerprint_bytes)?;
            tailed.file.seek(SeekFrom::Start(current_pos))?;
            if new_fp != 0 {
                tailed.identity.fingerprint = new_fp;
            }
        }

        Ok(if was_truncated {
            ReadResult::TruncatedThenData(result)
        } else {
            ReadResult::Data(result)
        })
    }

    /// Drain remaining data from a single file, emitting Data/Truncated events.
    ///
    /// Factored from the repeated drain pattern used during rotation and
    /// deletion cleanup. Preserves source_id on all emitted events.
    fn drain_file(
        &mut self,
        path: &Path,
        source_id: Option<SourceId>,
        events: &mut Vec<TailEvent>,
    ) -> bool {
        match self.read_new_data(path) {
            Ok(ReadResult::Data(data)) => {
                events.push(TailEvent::Data {
                    path: path.to_path_buf(),
                    bytes: data,
                    source_id,
                });
                false
            }
            Ok(ReadResult::TruncatedThenData(data)) => {
                events.push(TailEvent::Truncated {
                    path: path.to_path_buf(),
                    source_id,
                });
                events.push(TailEvent::Data {
                    path: path.to_path_buf(),
                    bytes: data,
                    source_id,
                });
                false
            }
            Ok(ReadResult::Truncated) => {
                events.push(TailEvent::Truncated {
                    path: path.to_path_buf(),
                    source_id,
                });
                false
            }
            Ok(ReadResult::NoData) => false,
            Err(e) => {
                tracing::error!(path = %path.display(), error = %e, "tail.drain_file_error");
                true
            }
        }
    }

    /// Read new data from all tailed files, emitting Data/Truncated/EndOfFile events.
    fn read_all(&mut self, events: &mut Vec<TailEvent>) -> bool {
        let mut had_error = false;
        let paths: Vec<PathBuf> = self.files.keys().cloned().collect();
        for path in paths {
            // Capture source_id BEFORE read_new_data: truncation detection
            // inside read_new_data updates the fingerprint, so the post-read
            // identity is the NEW file. Downstream uses source_id to clear
            // per-source state keyed by the OLD identity.
            let pre_read_source_id = self.source_id_for_path(&path);
            match self.read_new_data(&path) {
                Ok(ReadResult::Data(data)) => {
                    // New data arrived — reset the EOF-emitted flag so a fresh
                    // EndOfFile event can be emitted the next time reads stall.
                    if let Some(tailed) = self.files.get_mut(&path) {
                        tailed.eof_emitted = false;
                    }
                    // Compute source_id AFTER read_new_data: empty files acquire
                    // their real fingerprint during the read, so the post-read
                    // identity is the correct one to associate with this data.
                    let source_id = self.source_id_for_path(&path);
                    events.push(TailEvent::Data {
                        path: path.clone(),
                        bytes: data,
                        source_id,
                    });
                }
                Ok(ReadResult::TruncatedThenData(data)) => {
                    // Copytruncate detected + new data from beginning (#796).
                    // Emit Truncated FIRST so downstream clears remainder,
                    // then emit the new data. Use pre-read source_id for
                    // Truncated (what downstream knows), post-read for Data.
                    events.push(TailEvent::Truncated {
                        path: path.clone(),
                        source_id: pre_read_source_id,
                    });
                    if let Some(tailed) = self.files.get_mut(&path) {
                        tailed.eof_emitted = false;
                    }
                    let source_id = self.source_id_for_path(&path);
                    events.push(TailEvent::Data {
                        path: path.clone(),
                        bytes: data,
                        source_id,
                    });
                }
                Ok(ReadResult::Truncated) => {
                    // Truncated but no new data yet. Use pre-read source_id
                    // so downstream can find and clear the old identity.
                    events.push(TailEvent::Truncated {
                        path: path.clone(),
                        source_id: pre_read_source_id,
                    });
                }
                Ok(ReadResult::NoData) => {
                    // No new data. Emit EndOfFile once so downstream can flush
                    // any partial-line remainder that was not newline-terminated.
                    if let Some(tailed) = self.files.get_mut(&path) {
                        if !tailed.eof_emitted {
                            tailed.eof_emitted = true;
                            let source_id = self.source_id_for_path(&path);
                            events.push(TailEvent::EndOfFile {
                                path: path.clone(),
                                source_id,
                            });
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(path = %path.display(), error = %e, "tail.read_error");
                    had_error = true;
                }
            }
        }
        had_error
    }

    /// Evict least-recently-read files when over the open-file limit.
    ///
    /// Saves each evicted file's offset so it can resume from the correct
    /// position when re-opened on a future glob rescan.
    fn evict_lru(&mut self, max_open: usize) {
        if self.files.len() > max_open {
            let mut by_age: Vec<(PathBuf, Instant)> = self
                .files
                .iter()
                .map(|(path, tailed)| (path.clone(), tailed.last_read))
                .collect();
            by_age.sort_by_key(|(_, last_read)| *last_read);
            let to_remove = self.files.len() - max_open;
            for (path, _) in by_age.into_iter().take(to_remove) {
                if let Some(tailed) = self.files.remove(&path) {
                    let source_id = tailed.identity.source_id();
                    self.evicted_offsets.insert(
                        path.clone(),
                        EvictedFile {
                            identity: tailed.identity,
                            offset: tailed.offset,
                            path,
                            source_id,
                        },
                    );
                }
            }
        }
    }

    /// Get the current offset for a file (for checkpointing).
    fn get_offset(&self, path: &Path) -> Option<u64> {
        self.files.get(path).map(|f| f.offset)
    }

    /// Set the offset for a file (for restoring from checkpoint).
    ///
    /// Validates the offset against the current file size (#656). If the saved
    /// offset exceeds the file size (file was truncated between runs), resets
    /// to 0 instead of reading garbage.
    fn set_offset(&mut self, path: &Path, offset: u64) -> io::Result<()> {
        if let Some(tailed) = self.files.get_mut(path) {
            let file_size = tailed.file.metadata()?.len();
            let safe_offset = if offset > file_size {
                tracing::warn!(
                    path = %path.display(),
                    saved_offset = offset,
                    file_size,
                    "checkpoint offset exceeds file size — resetting to 0"
                );
                0
            } else {
                offset
            };
            tailed.offset = safe_offset;
            tailed.file.seek(SeekFrom::Start(safe_offset))?;
        }
        Ok(())
    }

    /// Restore a file offset by SourceId (compound identity), not path.
    ///
    /// Scans all tailed files for a matching compound identity
    /// (device + inode + fingerprint). Used for checkpoint restore — the
    /// checkpoint stores source_id + offset, not path.
    fn set_offset_by_source(&mut self, source_id: SourceId, offset: u64) -> io::Result<()> {
        for tailed in self.files.values_mut() {
            if tailed.identity.source_id() == source_id {
                let file_size = tailed.file.metadata()?.len();
                let safe_offset = if offset > file_size {
                    tracing::warn!(
                        source_id = source_id.0,
                        saved_offset = offset,
                        file_size,
                        "checkpoint source offset exceeds file size — resetting to 0"
                    );
                    0
                } else {
                    offset
                };
                tailed.offset = safe_offset;
                tailed.file.seek(SeekFrom::Start(safe_offset))?;
                return Ok(());
            }
        }
        Ok(()) // source not found — file may not exist yet
    }

    /// Number of files currently being tailed.
    fn num_files(&self) -> usize {
        self.files.len()
    }

    /// Look up the `SourceId` for a given path.
    ///
    /// Returns `None` if the path is not currently tailed or is an empty file
    /// (fingerprint 0).
    fn source_id_for_path(&self, path: &Path) -> Option<SourceId> {
        self.files.get(path).and_then(|tailed| {
            let sid = tailed.identity.source_id();
            if sid == SourceId(0) { None } else { Some(sid) }
        })
    }

    /// Hot path: source identity + offset for all tailed files.
    ///
    /// Includes both actively-tailed files and files evicted from the LRU
    /// cache (#697). This ensures evicted file offsets are persisted to the
    /// checkpoint file, surviving crashes while files are in the evicted state.
    ///
    /// Skips empty files (fingerprint 0) — they have no data to checkpoint.
    /// Called on every channel send (~100ms). No PathBuf allocation.
    fn file_offsets(&self) -> Vec<(SourceId, ByteOffset)> {
        let active = self
            .files
            .iter()
            .filter(|(_, tailed)| tailed.identity.fingerprint != 0)
            .map(|(_, tailed)| (tailed.identity.source_id(), ByteOffset(tailed.offset)));

        let evicted = self
            .evicted_offsets
            .values()
            .filter(|e| e.identity.fingerprint != 0)
            .map(|e| (e.source_id, ByteOffset(e.offset)));

        active.chain(evicted).collect()
    }

    /// Cold path: source identity + canonical path for all tailed files.
    ///
    /// Includes evicted files so checkpoint paths stay consistent with offsets.
    /// Called on file open/close/rotate, not per-batch. PathBuf cloning is
    /// acceptable here since this runs infrequently.
    fn file_paths(&self) -> Vec<(SourceId, PathBuf)> {
        let active = self
            .files
            .iter()
            .filter(|(_, tailed)| tailed.identity.fingerprint != 0)
            .map(|(path, tailed)| (tailed.identity.source_id(), path.clone()));

        let evicted = self
            .evicted_offsets
            .values()
            .filter(|e| e.identity.fingerprint != 0)
            .map(|e| (e.source_id, e.path.clone()));

        active.chain(evicted).collect()
    }
}

// ---------------------------------------------------------------------------
// FileTailer — public composition of FileDiscovery + FileReader
// ---------------------------------------------------------------------------

/// The file tailer. Watches one or more file paths and yields data as it appears.
pub struct FileTailer {
    discovery: FileDiscovery,
    reader: FileReader,
    config: TailConfig,
    /// Last time we did a full poll scan.
    last_poll: Instant,
    /// Consecutive polls that observed one or more I/O / watcher errors.
    consecutive_error_polls: u32,
    /// Next time we're allowed to run a full poll after an error burst.
    error_backoff_until: Option<Instant>,
}

impl FileTailer {
    /// Create a new tailer watching the given file paths.
    pub fn new(paths: &[PathBuf], config: TailConfig) -> io::Result<Self> {
        let (tx, rx) = crossbeam_channel::unbounded();

        let mut watcher = notify::recommended_watcher(move |res| {
            let _ = tx.send(res);
        })
        .map_err(io::Error::other)?;

        // Watch the parent directories (not the files themselves).
        // This catches file creation, rename, and deletion events
        // that inotify/kqueue on the file itself would miss.
        let mut watched_dirs = HashSet::new();
        for path in paths {
            if let Some(parent) = path.parent()
                && watched_dirs.insert(parent.to_path_buf())
            {
                use notify::Watcher;
                watcher
                    .watch(parent, notify::RecursiveMode::NonRecursive)
                    .map_err(io::Error::other)?;
            }
        }

        let mut tailer = FileTailer {
            discovery: FileDiscovery {
                watcher,
                watched_dirs,
                glob_patterns: Vec::new(),
                watch_paths: paths.to_vec(),
                fs_events: rx,
                last_glob_rescan: Instant::now(),
            },
            reader: FileReader {
                files: HashMap::new(),
                read_buf: vec![0u8; config.read_buf_size],
                evicted_offsets: HashMap::new(),
                config: config.clone(),
            },
            config,
            last_poll: Instant::now(),
            consecutive_error_polls: 0,
            error_backoff_until: None,
        };

        // Open existing files. Warn about missing paths (#730).
        for path in paths {
            if path.exists() {
                if let Err(e) = tailer
                    .reader
                    .open_file_at(path, tailer.config.start_from_end)
                {
                    tracing::warn!(path = %path.display(), error = %e, "tail.open_failed");
                }
            } else {
                tracing::warn!(path = %path.display(), "tail.file_not_found — pipeline will wait until file appears");
            }
        }

        Ok(tailer)
    }

    /// Create a new tailer from glob patterns.
    ///
    /// Each pattern is expanded immediately to find existing files and then
    /// re-evaluated every [`TailConfig::glob_rescan_interval_ms`] milliseconds
    /// to pick up files that appear after construction (e.g., new Kubernetes pods).
    ///
    /// Patterns that match no files at construction time are silently ignored —
    /// they will be retried on the next rescan.
    pub fn new_with_globs(patterns: &[&str], config: TailConfig) -> io::Result<Self> {
        // Expand patterns to get the initial set of concrete paths.
        let initial_paths: Vec<PathBuf> = expand_glob_patterns(patterns);

        // Warn when glob patterns match no files (#730).
        if initial_paths.is_empty() {
            for pattern in patterns {
                tracing::warn!(
                    pattern,
                    "tail.glob_no_matches — pipeline will wait until matching files appear"
                );
            }
        }

        let mut tailer = Self::new(&initial_paths, config)?;
        tailer.discovery.glob_patterns = patterns.iter().map(ToString::to_string).collect();
        Ok(tailer)
    }

    /// Maximum bytes to read from a single file per poll cycle.
    /// Prevents OOM when a file grows significantly between polls (#800).
    /// Exposed for test assertions; production code uses `FileReader::MAX_READ_PER_POLL`.
    #[cfg(test)]
    const MAX_READ_PER_POLL: usize = FileReader::MAX_READ_PER_POLL;

    /// Poll for new data. Returns a batch of events.
    /// Call this in your main loop. It will:
    /// 1. Drain any filesystem notifications (low latency path)
    /// 2. If enough time has passed, do a full poll scan (safety net)
    /// 3. If enough time has passed, re-evaluate glob patterns (new file discovery)
    /// 4. Read new data from any files that have grown
    pub fn poll(&mut self) -> io::Result<Vec<TailEvent>> {
        let mut events = Vec::new();

        // Always drain the fs_events channel regardless of backoff so the
        // unbounded notify channel does not accumulate unboundedly while
        // we intentionally skip the expensive work below.
        let (something_changed, mut had_error) = self.discovery.drain_events();

        if let Some(until) = self.error_backoff_until
            && Instant::now() < until
        {
            // Still account for any errors seen during drain above.
            if had_error {
                self.update_error_backoff(true);
            }
            return Ok(events);
        }

        // Periodic full poll as safety net.
        let poll_interval = Duration::from_millis(self.config.poll_interval_ms);
        let glob_rescan_due = self.config.glob_rescan_interval_ms > 0
            && self.discovery.last_glob_rescan.elapsed()
                >= Duration::from_millis(self.config.glob_rescan_interval_ms);
        let should_poll =
            something_changed || self.last_poll.elapsed() >= poll_interval || glob_rescan_due;

        if !should_poll {
            if had_error {
                self.update_error_backoff(true);
            }
            return Ok(events);
        }
        self.last_poll = Instant::now();

        // Re-evaluate glob patterns to discover new files.
        if glob_rescan_due {
            had_error |= self.discovery.rescan_globs(&mut self.reader);
            self.discovery.last_glob_rescan = Instant::now();
        }

        // Check for new/rotated files.
        had_error |= self.discovery.detect_changes(&mut self.reader, &mut events);

        // Read new data from all tailed files.
        had_error |= self.reader.read_all(&mut events);

        // Remove entries for files that have been unlinked (nlink == 0).
        had_error |= self
            .discovery
            .cleanup_deleted(&mut self.reader, &mut events);

        // Evict least-recently-read files when over the open-file limit.
        self.reader.evict_lru(self.config.max_open_files);
        self.update_error_backoff(had_error);

        Ok(events)
    }

    fn update_error_backoff(&mut self, had_error: bool) {
        const INITIAL_BACKOFF_MS: u64 = 100;
        const MAX_BACKOFF_MS: u64 = 5000;

        if had_error {
            self.consecutive_error_polls = self.consecutive_error_polls.saturating_add(1);
            let exponent = self.consecutive_error_polls.saturating_sub(1).min(6);
            let multiplier = 1u64 << exponent;
            let backoff_ms = INITIAL_BACKOFF_MS
                .saturating_mul(multiplier)
                .min(MAX_BACKOFF_MS);
            self.error_backoff_until = Some(Instant::now() + Duration::from_millis(backoff_ms));
            tracing::warn!(
                consecutive_error_polls = self.consecutive_error_polls,
                backoff_ms,
                "tail.poll_backoff_after_error"
            );
        } else {
            self.consecutive_error_polls = 0;
            self.error_backoff_until = None;
        }
    }

    /// Get the current offset for a file (for checkpointing).
    pub fn get_offset(&self, path: &Path) -> Option<u64> {
        self.reader.get_offset(path)
    }

    /// Set the offset for a file (for restoring from checkpoint).
    ///
    /// Validates the offset against the current file size (#656). If the saved
    /// offset exceeds the file size (file was truncated between runs), resets
    /// to 0 instead of reading garbage.
    pub fn set_offset(&mut self, path: &Path, offset: u64) -> io::Result<()> {
        self.reader.set_offset(path, offset)
    }

    /// Restore a file offset by SourceId (compound identity), not path.
    ///
    /// Scans all tailed files for a matching compound identity
    /// (device + inode + fingerprint). Used for checkpoint restore — the
    /// checkpoint stores source_id + offset, not path.
    pub fn set_offset_by_source(&mut self, source_id: SourceId, offset: u64) -> io::Result<()> {
        self.reader.set_offset_by_source(source_id, offset)
    }

    /// Number of files currently being tailed.
    pub fn num_files(&self) -> usize {
        self.reader.num_files()
    }

    /// Look up the `SourceId` for a given path.
    ///
    /// Returns `None` if the path is not currently tailed or is an empty file
    /// (fingerprint 0).
    pub fn source_id_for_path(&self, path: &Path) -> Option<SourceId> {
        self.reader.source_id_for_path(path)
    }

    /// Hot path: source identity + offset for all tailed files.
    ///
    /// Includes both actively-tailed files and files evicted from the LRU
    /// cache (#697). This ensures evicted file offsets are persisted to the
    /// checkpoint file, surviving crashes while files are in the evicted state.
    ///
    /// Skips empty files (fingerprint 0) — they have no data to checkpoint.
    /// Called on every channel send (~100ms). No PathBuf allocation.
    pub fn file_offsets(&self) -> Vec<(SourceId, ByteOffset)> {
        self.reader.file_offsets()
    }

    /// Cold path: source identity + canonical path for all tailed files.
    ///
    /// Includes evicted files so checkpoint paths stay consistent with offsets.
    /// Called on file open/close/rotate, not per-batch. PathBuf cloning is
    /// acceptable here since this runs infrequently.
    pub fn file_paths(&self) -> Vec<(SourceId, PathBuf)> {
        self.reader.file_paths()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Mutex;

    static CWD_LOCK: Mutex<()> = Mutex::new(());

    // ---- glob_root / glob_max_depth unit tests ----

    #[test]
    fn glob_root_absolute_star() {
        assert_eq!(glob_root("/var/log/*.log"), PathBuf::from("/var/log"));
    }

    #[test]
    fn glob_root_absolute_double_star() {
        assert_eq!(glob_root("/var/log/**/*.log"), PathBuf::from("/var/log"));
    }

    #[test]
    fn glob_root_relative_star() {
        assert_eq!(glob_root("*.log"), PathBuf::from("."));
    }

    #[test]
    fn glob_root_mid_filename_wildcard() {
        // Wildcard in the middle of a filename component.
        assert_eq!(glob_root("/var/log/app*.log"), PathBuf::from("/var/log"));
    }

    #[test]
    fn glob_root_no_wildcard() {
        // A literal path has no wildcard, so parent dir is the walk root.
        assert_eq!(glob_root("/var/log/app.log"), PathBuf::from("/var/log"));
    }

    #[test]
    fn glob_root_relative_no_wildcard() {
        // Bare filename with no path separator or wildcard — parent is current dir.
        assert_eq!(glob_root("test.log"), PathBuf::from("."));
        assert_eq!(glob_root("app.log"), PathBuf::from("."));
    }

    #[test]
    fn glob_root_relative_mid_filename_wildcard() {
        // `app*.log` — wildcard mid-filename, root is current dir (parent of "app" is "").
        assert_eq!(glob_root("app*.log"), PathBuf::from("."));
    }

    #[test]
    fn glob_max_depth_single_level() {
        // /var/log/*.log → root=/var/log, pattern has 3 components, root has 2 → depth 1
        assert_eq!(glob_max_depth("/var/log/*.log"), Some(1));
    }

    #[test]
    fn glob_max_depth_double_star_unbounded() {
        assert_eq!(glob_max_depth("/var/log/**/*.log"), None);
    }

    #[test]
    fn glob_max_depth_relative_is_at_least_1() {
        // *.log → root=., 1 component each, but max(0,1) = 1
        assert_eq!(glob_max_depth("*.log"), Some(1));
    }

    #[test]
    fn glob_max_depth_relative_subdir() {
        // */*.log → root=. (0 effective components), pattern has 2 → depth 2.
        // WalkDir must search at depth 2 to find files in subdirectories.
        assert_eq!(glob_max_depth("*/*.log"), Some(2));
    }

    #[test]
    fn glob_max_depth_relative_dot_prefix() {
        // `./` is syntactic sugar for the current directory and should not
        // change traversal depth compared with the equivalent relative pattern.
        assert_eq!(glob_max_depth("./*.log"), glob_max_depth("*.log"));
        assert_eq!(glob_max_depth("./foo/*.log"), glob_max_depth("foo/*.log"));
    }

    // ---- end glob helper tests ----

    #[test]
    fn test_tail_new_data() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");

        // Create file with initial content.
        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "line 1").unwrap();
            writeln!(f, "line 2").unwrap();
        }

        let config = TailConfig {
            start_from_end: false, // read existing content
            poll_interval_ms: 10,
            ..Default::default()
        };

        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();

        // First poll should read existing content.
        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();
        let data_events: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .collect();

        assert!(!data_events.is_empty(), "should read existing data");
        let all_data: Vec<u8> = data_events.into_iter().flatten().collect();
        assert!(all_data.starts_with(b"line 1\n"));

        // Append more data.
        {
            let mut f = fs::OpenOptions::new().append(true).open(&log_path).unwrap();
            writeln!(f, "line 3").unwrap();
            writeln!(f, "line 4").unwrap();
        }

        // Poll again — should get the new data.
        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();
        let new_data: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();

        let new_str = String::from_utf8_lossy(&new_data);
        assert!(
            new_str.contains("line 3"),
            "should see appended data, got: {new_str}"
        );
    }

    #[test]
    fn test_tail_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("trunc.log");

        // Write initial data.
        {
            let mut f = File::create(&log_path).unwrap();
            for i in 0..100 {
                writeln!(f, "line {i}").unwrap();
            }
        }

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();

        // Read initial data.
        std::thread::sleep(Duration::from_millis(50));
        tailer.poll().unwrap();

        let offset_before = tailer.get_offset(&log_path).unwrap();
        assert!(offset_before > 0);

        // Truncate and write new data (simulating copytruncate).
        {
            let f = File::create(&log_path).unwrap(); // truncates
            let mut f = io::BufWriter::new(f);
            writeln!(f, "after truncate 1").unwrap();
            writeln!(f, "after truncate 2").unwrap();
        }

        // Poll — should detect truncation and read from beginning.
        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();
        let new_data: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();

        let new_str = String::from_utf8_lossy(&new_data);
        assert!(
            new_str.contains("after truncate"),
            "should read data after truncation, got: {new_str}"
        );
    }

    #[test]
    fn test_tail_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("rotate.log");
        let rotated_path = dir.path().join("rotate.log.1");

        // Write initial data.
        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "before rotation").unwrap();
        }

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();

        // Read initial data.
        std::thread::sleep(Duration::from_millis(50));
        tailer.poll().unwrap();

        // Rotate: rename old file, create new one.
        fs::rename(&log_path, &rotated_path).unwrap();
        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "after rotation 1").unwrap();
            writeln!(f, "after rotation 2").unwrap();
        }

        // Poll — should detect rotation and read new file from beginning.
        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();

        let has_rotation = events
            .iter()
            .any(|e| matches!(e, TailEvent::Rotated { .. }));
        assert!(has_rotation, "should detect rotation");

        let new_data: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();

        let new_str = String::from_utf8_lossy(&new_data);
        assert!(
            new_str.contains("after rotation"),
            "should read new file content, got: {new_str}"
        );
    }

    /// #816: Ensure rotated drain path emits Truncated if file was copytruncated before rename
    #[test]
    fn test_tail_rotation_drains_truncated_file() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("drain_trunc.log");
        let rotated_path = dir.path().join("drain_trunc.log.1");

        // Write initial data.
        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "initial").unwrap();
        }

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();

        // First poll — drain initial data.
        std::thread::sleep(Duration::from_millis(50));
        tailer.poll().unwrap();

        // Overwrite the file to be smaller than the current offset, simulating copytruncate.
        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "new").unwrap();
        }

        // Rotate: rename old file, create new one.
        fs::rename(&log_path, &rotated_path).unwrap();
        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "post-rotation").unwrap();
        }

        // Poll must detect rotation and emit Truncated THEN Data for the drained bytes.
        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();

        let trunc_pos = events
            .iter()
            .position(|e| matches!(e, TailEvent::Truncated { .. }))
            .expect("should have a Truncated event from drain");

        let data_pos = events
            .iter()
            .position(|e| matches!(e, TailEvent::Data { .. }))
            .expect("should have a Data event from drain");

        assert!(trunc_pos < data_pos, "Truncated must precede Data");
    }

    /// Regression test: bytes appended to the old file after the last poll but
    /// before the rename must not be lost.
    #[test]
    fn test_tail_rotation_drains_old_data() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("drain.log");
        let rotated_path = dir.path().join("drain.log.1");

        // Write initial data.
        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "initial line").unwrap();
        }

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();

        // First poll — drain initial data.
        std::thread::sleep(Duration::from_millis(50));
        tailer.poll().unwrap();

        // Append lines to the OLD file WITHOUT polling first.
        // These are the bytes that would be lost without the drain-on-rotation fix.
        {
            let mut f = fs::OpenOptions::new().append(true).open(&log_path).unwrap();
            writeln!(f, "pre-rotation line 1").unwrap();
            writeln!(f, "pre-rotation line 2").unwrap();
        }

        // Rotate: rename old file, create new one.
        fs::rename(&log_path, &rotated_path).unwrap();
        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "post-rotation line").unwrap();
        }

        // This poll must detect rotation AND deliver the pre-rotation bytes.
        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();

        let has_rotation = events
            .iter()
            .any(|e| matches!(e, TailEvent::Rotated { .. }));
        assert!(has_rotation, "should detect rotation");

        let all_data: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();

        let s = String::from_utf8_lossy(&all_data);
        assert!(
            s.contains("pre-rotation line 1"),
            "pre-rotation bytes must not be lost, got: {s}"
        );
        assert!(
            s.contains("pre-rotation line 2"),
            "pre-rotation bytes must not be lost, got: {s}"
        );

        // Data event with pre-rotation bytes must come BEFORE the Rotated event.
        let first_data_pos = events
            .iter()
            .position(|e| matches!(e, TailEvent::Data { .. }))
            .expect("should have a Data event");
        let rotated_pos = events
            .iter()
            .position(|e| matches!(e, TailEvent::Rotated { .. }))
            .expect("should have a Rotated event");
        assert!(
            first_data_pos < rotated_pos,
            "Data event must precede Rotated event"
        );
    }

    #[test]
    fn test_tail_start_from_end() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("tail_end.log");

        // Write existing data.
        {
            let mut f = File::create(&log_path).unwrap();
            for i in 0..100 {
                writeln!(f, "old line {i}").unwrap();
            }
        }

        let config = TailConfig {
            start_from_end: true, // skip existing data
            poll_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();

        // First poll should get no data (started from end).
        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();
        let data: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        assert!(data.is_empty(), "should skip existing data");

        // Append new data.
        {
            let mut f = fs::OpenOptions::new().append(true).open(&log_path).unwrap();
            writeln!(f, "new line 1").unwrap();
        }

        // Should see only the new data.
        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();
        let data: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        let s = String::from_utf8_lossy(&data);
        assert!(s.contains("new line 1"), "should see new data, got: {s}");
        assert!(!s.contains("old line"), "should NOT see old data");
    }

    #[test]
    fn test_file_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity.log");

        {
            let mut f = File::create(&path).unwrap();
            writeln!(f, "hello world").unwrap();
        }

        let id1 = identify_file(&path, 1024).unwrap();
        let id2 = identify_file(&path, 1024).unwrap();
        assert_eq!(id1, id2, "same file should have same identity");

        // Overwrite with different content.
        {
            let mut f = File::create(&path).unwrap();
            writeln!(f, "different content").unwrap();
        }

        let id3 = identify_file(&path, 1024).unwrap();
        assert_ne!(
            id1.fingerprint, id3.fingerprint,
            "different content should have different fingerprint"
        );
    }

    /// Verify that `new_with_globs` picks up files that exist at construction time.
    #[test]
    fn test_glob_initial_discovery() {
        let dir = tempfile::tempdir().unwrap();

        // Create two log files upfront.
        let log_a = dir.path().join("a.log");
        let log_b = dir.path().join("b.log");
        {
            let mut f = File::create(&log_a).unwrap();
            writeln!(f, "file a").unwrap();
        }
        {
            let mut f = File::create(&log_b).unwrap();
            writeln!(f, "file b").unwrap();
        }

        let pattern = format!("{}/*.log", dir.path().display());
        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            glob_rescan_interval_ms: 60_000, // long interval — not relevant for this test
            ..Default::default()
        };
        let mut tailer = FileTailer::new_with_globs(&[&pattern], config).unwrap();

        // Both files should have been discovered immediately.
        assert_eq!(tailer.num_files(), 2, "should tail both initial log files");

        // Poll should return data from both files.
        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();
        let all_data: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();

        let s = String::from_utf8_lossy(&all_data);
        assert!(s.contains("file a"), "should read file a");
        assert!(s.contains("file b"), "should read file b");
    }

    /// Verify that a new file appearing after construction is discovered on the next rescan.
    #[test]
    fn test_glob_rescan_discovers_new_file() {
        let dir = tempfile::tempdir().unwrap();

        let pattern = format!("{}/*.log", dir.path().display());
        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            // Very short rescan interval so the test doesn't have to wait long.
            glob_rescan_interval_ms: 50,
            ..Default::default()
        };
        let mut tailer = FileTailer::new_with_globs(&[&pattern], config).unwrap();

        // No files exist yet — tailer starts with nothing.
        assert_eq!(tailer.num_files(), 0, "no files should be tailed initially");

        // Create a new log file (simulating a new Kubernetes pod).
        let new_log = dir.path().join("pod-xyz.log");
        {
            let mut f = File::create(&new_log).unwrap();
            writeln!(f, "pod xyz line 1").unwrap();
        }

        // Wait for the glob rescan interval to expire, then poll.
        std::thread::sleep(Duration::from_millis(150));
        let events = tailer.poll().unwrap();

        assert_eq!(
            tailer.num_files(),
            1,
            "newly-created file should now be tailed"
        );

        let all_data: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();

        let s = String::from_utf8_lossy(&all_data);
        assert!(
            s.contains("pod xyz line 1"),
            "should read data from newly-discovered file, got: {s}"
        );
    }

    /// Verify that `rescan_globs` does not add the same file twice.
    #[test]
    fn test_glob_rescan_no_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("dedup.log");
        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "dedup content").unwrap();
        }

        let pattern = format!("{}/*.log", dir.path().display());
        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            glob_rescan_interval_ms: 50,
            ..Default::default()
        };
        let mut tailer = FileTailer::new_with_globs(&[&pattern], config).unwrap();

        // File discovered at construction.
        assert_eq!(tailer.num_files(), 1);
        let initial_watch_count = tailer.discovery.watch_paths.len();

        // Wait for rescan and poll again — file should not be added twice.
        std::thread::sleep(Duration::from_millis(150));
        tailer.poll().unwrap();

        assert_eq!(
            tailer.discovery.watch_paths.len(),
            initial_watch_count,
            "watch_paths should not grow after rescan of already-known file"
        );
        assert_eq!(tailer.num_files(), 1, "should still tail exactly one file");
    }

    /// #1375: cwd-relative patterns with `./` must match discovered files.
    #[test]
    fn test_expand_glob_patterns_matches_dot_slash_cwd_relative() {
        let _cwd_guard = CWD_LOCK.lock().unwrap();

        // RAII guard restores cwd even if an assert panics.
        // RAII guard restores cwd even if an assert panics.
        // Declared AFTER dir so it drops BEFORE dir (reverse order),
        // ensuring cwd is restored before the temp directory is deleted.
        struct CwdGuard(PathBuf);
        impl Drop for CwdGuard {
            fn drop(&mut self) {
                let _ = std::env::set_current_dir(&self.0);
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let _restore = CwdGuard(std::env::current_dir().unwrap());
        std::env::set_current_dir(dir.path()).unwrap();

        let nested = PathBuf::from("logs");
        fs::create_dir_all(&nested).unwrap();
        let target = nested.join("app.log");
        let other = nested.join("app.txt");
        {
            let mut f = File::create(&target).unwrap();
            writeln!(f, "hello").unwrap();
        }
        File::create(&other).unwrap();

        // Test prefixed pattern: ./logs/*.log
        let matches = expand_glob_patterns(&["./logs/*.log"]);
        let normalized: Vec<PathBuf> = matches
            .iter()
            .map(|p| {
                p.strip_prefix(".")
                    .map_or_else(|_| p.clone(), Path::to_path_buf)
            })
            .collect();
        assert!(
            normalized.iter().any(|p| p == &target),
            "pattern ./logs/*.log should match logs/app.log, got: {matches:?}"
        );
        assert!(
            !normalized.iter().any(|p| p == &other),
            "pattern ./logs/*.log should not match non-log file, got: {matches:?}"
        );

        // Test bare pattern: *.log in cwd — the primary #1375 case.
        let cwd_file = PathBuf::from("bare.log");
        File::create(&cwd_file).unwrap();
        let bare_matches = expand_glob_patterns(&["*.log"]);
        let bare_norm: Vec<PathBuf> = bare_matches
            .iter()
            .map(|p| {
                p.strip_prefix(".")
                    .map_or_else(|_| p.clone(), Path::to_path_buf)
            })
            .collect();
        assert!(
            bare_norm.iter().any(|p| p == &cwd_file),
            "bare pattern *.log should match cwd file bare.log, got: {bare_matches:?}"
        );
    }

    /// Verify that when open files exceed `max_open_files`, the least-recently-read
    /// files are evicted until the count is within the limit.
    #[test]
    fn test_eviction_lru() {
        let dir = tempfile::tempdir().unwrap();

        // Create 10 log files.
        let mut log_paths = Vec::new();
        for i in 0..10 {
            let p = dir.path().join(format!("{i}.log"));
            {
                let mut f = File::create(&p).unwrap();
                writeln!(f, "file {i}").unwrap();
            }
            log_paths.push(p);
        }

        let pattern = format!("{}/*.log", dir.path().display());
        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            glob_rescan_interval_ms: 60_000,
            max_open_files: 5,
            ..Default::default()
        };
        let mut tailer = FileTailer::new_with_globs(&[&pattern], config).unwrap();

        // All 10 files discovered at construction — eviction happens during poll.
        assert_eq!(tailer.num_files(), 10, "all files opened before first poll");

        // Poll — should evict 5 least-recently-read files.
        std::thread::sleep(Duration::from_millis(50));
        tailer.poll().unwrap();

        assert_eq!(
            tailer.num_files(),
            5,
            "should have evicted down to max_open_files=5"
        );
    }

    /// Verify that writing to an evicted file causes it to be re-opened on the next
    /// glob rescan + poll cycle.
    #[test]
    fn test_evicted_file_reopen() {
        let dir = tempfile::tempdir().unwrap();

        // Create 3 files; limit to 2 so one will always be evicted.
        let mut log_paths = Vec::new();
        for i in 0..3 {
            let p = dir.path().join(format!("{i}.log"));
            {
                let mut f = File::create(&p).unwrap();
                writeln!(f, "initial {i}").unwrap();
            }
            log_paths.push(p);
        }

        let pattern = format!("{}/*.log", dir.path().display());
        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            glob_rescan_interval_ms: 50, // short rescan so the test is fast
            max_open_files: 2,
            ..Default::default()
        };
        let mut tailer = FileTailer::new_with_globs(&[&pattern], config).unwrap();

        // After first poll, eviction brings count to 2.
        std::thread::sleep(Duration::from_millis(50));
        tailer.poll().unwrap();
        assert_eq!(tailer.num_files(), 2, "evicted to max_open_files=2");

        // Append data to all files. The evicted file will be re-discovered.
        for p in &log_paths {
            let mut f = fs::OpenOptions::new().append(true).open(p).unwrap();
            writeln!(f, "new data").unwrap();
        }

        // Wait for glob rescan interval, then poll — evicted file re-opened.
        std::thread::sleep(Duration::from_millis(150));
        let events = tailer.poll().unwrap();

        // At least one Data event should arrive (evicted file re-opened + read).
        let all_data: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        let s = String::from_utf8_lossy(&all_data);
        assert!(
            s.contains("new data"),
            "should receive data after evicted file is re-opened, got: {s}"
        );
    }

    /// Verify that a deleted file is removed from the `files` map on the next poll.
    #[test]
    fn test_tail_growing_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("growing.log");

        // Create file with small content.
        {
            let mut f = File::create(&log_path).unwrap();
            f.write_all(&[b'a'; 100]).unwrap();
        }

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            fingerprint_bytes: 500,
            ..Default::default()
        };

        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();

        // Initial poll reads the 100 'a's.
        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();
        assert!(events.iter().any(|e| matches!(e, TailEvent::Data { .. })));

        // Grow file past fingerprint_bytes.
        {
            let mut f = fs::OpenOptions::new().append(true).open(&log_path).unwrap();
            f.write_all(&[b'b'; 1000]).unwrap();
        }

        // Poll again.
        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();

        // If the bug exists, this will contain a Rotated event because the
        // fingerprint grew from 100 bytes to 500 bytes.
        let rotated = events
            .iter()
            .any(|e| matches!(e, TailEvent::Rotated { .. }));
        assert!(
            !rotated,
            "should not trigger rotation just because fingerprint grew"
        );

        // Should have received only the new 'b's.
        let data: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        assert_eq!(data.len(), 1000);
        assert!(data.iter().all(|&b| b == b'b'));
    }

    #[test]
    fn test_deleted_file_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("delete_me.log");

        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "soon gone").unwrap();
        }

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();

        // Confirm file is being tailed.
        std::thread::sleep(Duration::from_millis(50));
        tailer.poll().unwrap();
        assert_eq!(tailer.num_files(), 1, "file should be open before deletion");

        // Delete the file.
        fs::remove_file(&log_path).unwrap();

        // Next poll must clean up the stale entry.
        std::thread::sleep(Duration::from_millis(50));
        tailer.poll().unwrap();
        assert_eq!(
            tailer.num_files(),
            0,
            "deleted file should be removed from the files map"
        );
        // For literal-path tailers, watch_paths must KEEP the path so the
        // file can be detected if it is re-created. (#810)
        assert_eq!(
            tailer.discovery.watch_paths.len(),
            1,
            "literal-path tailers must keep watch_paths entry after deletion"
        );
    }

    /// #816: Ensure deleted drain path emits Truncated if file was copytruncated before deletion
    #[test]
    fn test_deleted_file_cleanup_drains_truncated_file() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("delete_trunc.log");

        // Write initial data.
        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "initial").unwrap();
        }

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();

        // First poll — drain initial data.
        std::thread::sleep(Duration::from_millis(50));
        tailer.poll().unwrap();

        // Overwrite the file to be smaller than the current offset, simulating copytruncate.
        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "new").unwrap();
        }

        fs::remove_file(&log_path).unwrap();

        // Poll must emit Truncated THEN Data for the drained bytes.
        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();

        let trunc_pos = events
            .iter()
            .position(|e| matches!(e, TailEvent::Truncated { .. }))
            .expect("should have a Truncated event from drain");

        let data_pos = events
            .iter()
            .position(|e| matches!(e, TailEvent::Data { .. }))
            .expect("should have a Data event from drain");

        assert!(trunc_pos < data_pos, "Truncated must precede Data");
    }

    /// Regression test: glob-discovered file deletions must shrink watch_paths
    /// so the list does not grow unboundedly with file churn. (#810)
    #[test]
    fn test_glob_deleted_file_removed_from_watch_paths() {
        let dir = tempfile::tempdir().unwrap();
        let pattern = format!("{}/*.log", dir.path().display());

        // Create several files matching the glob.
        let paths: Vec<_> = (0..5)
            .map(|i| {
                let p = dir.path().join(format!("churn-{i}.log"));
                let mut f = File::create(&p).unwrap();
                writeln!(f, "data {i}").unwrap();
                p
            })
            .collect();

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            glob_rescan_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new_with_globs(&[&pattern], config).unwrap();

        // Read initial data so the tailer advances past the initial content.
        std::thread::sleep(Duration::from_millis(50));
        tailer.poll().unwrap();

        let paths_before = tailer.discovery.watch_paths.len();
        assert_eq!(paths_before, 5, "should have 5 watch_paths before deletion");

        // Delete all files.
        for p in &paths {
            fs::remove_file(p).unwrap();
        }

        // Poll to trigger deletion cleanup.
        std::thread::sleep(Duration::from_millis(50));
        tailer.poll().unwrap();

        assert_eq!(
            tailer.discovery.watch_paths.len(),
            0,
            "watch_paths must shrink to 0 after all glob files are deleted (#810)"
        );
        assert_eq!(tailer.num_files(), 0, "files map must also be empty");
    }

    #[test]
    fn test_file_offsets_returns_fingerprint_and_offset() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, r#"{{"msg":"hello"}}"#).unwrap();
            writeln!(f, r#"{{"msg":"world"}}"#).unwrap();
        }

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();

        std::thread::sleep(Duration::from_millis(50));
        let _ = tailer.poll().unwrap();

        let offsets = tailer.file_offsets();
        assert_eq!(offsets.len(), 1, "should have one file");
        let (sid, byte_off) = &offsets[0];
        assert_ne!(
            sid.0, 0,
            "fingerprint should be non-zero for file with content"
        );
        assert!(byte_off.0 > 0, "offset should be > 0 after reading data");
    }

    #[test]
    fn test_file_offsets_skips_empty_files() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("empty.log");
        File::create(&log_path).unwrap(); // empty file

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let _ = tailer.poll().unwrap();

        let offsets = tailer.file_offsets();
        assert!(
            offsets.is_empty(),
            "empty files (fp=0) should be filtered out"
        );
    }

    #[test]
    fn test_file_offsets_multiple_files() {
        let dir = tempfile::tempdir().unwrap();
        let path_a = dir.path().join("a.log");
        let path_b = dir.path().join("b.log");
        {
            let mut f = File::create(&path_a).unwrap();
            writeln!(f, "aaaa").unwrap();
        }
        {
            let mut f = File::create(&path_b).unwrap();
            writeln!(f, "bbbb").unwrap();
        }

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(&[path_a, path_b], config).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let _ = tailer.poll().unwrap();

        let offsets = tailer.file_offsets();
        assert_eq!(offsets.len(), 2, "should have two files");
        let sids: Vec<_> = offsets.iter().map(|(s, _)| s.0).collect();
        assert_ne!(
            sids[0], sids[1],
            "distinct files should have distinct fingerprints"
        );
    }

    #[test]
    fn test_file_paths_matches_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("test.log");
        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "data").unwrap();
        }

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let _ = tailer.poll().unwrap();

        let offsets = tailer.file_offsets();
        let paths = tailer.file_paths();
        assert_eq!(offsets.len(), paths.len());
        // SourceIds should match between the two calls
        let offset_sids: Vec<_> = offsets.iter().map(|(s, _)| s.0).collect();
        let path_sids: Vec<_> = paths.iter().map(|(s, _)| s.0).collect();
        assert_eq!(offset_sids, path_sids);
    }

    // -----------------------------------------------------------------------
    // Bug fix regression tests
    // -----------------------------------------------------------------------

    /// #796: Copytruncate must emit TailEvent::Truncated before new data.
    #[test]
    fn test_truncation_emits_truncated_event() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("trunc_event.log");

        // Write initial data.
        {
            let mut f = File::create(&log_path).unwrap();
            for i in 0..50 {
                writeln!(f, "original line {i}").unwrap();
            }
        }

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();

        // Read all initial data.
        std::thread::sleep(Duration::from_millis(50));
        tailer.poll().unwrap();

        // Truncate and write new data (copytruncate).
        {
            let mut f = File::create(&log_path).unwrap(); // truncates
            writeln!(f, "after truncate").unwrap();
        }

        // Poll should emit Truncated THEN Data.
        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();

        let has_truncated = events
            .iter()
            .any(|e| matches!(e, TailEvent::Truncated { .. }));
        assert!(
            has_truncated,
            "must emit TailEvent::Truncated on copytruncate"
        );

        let has_data = events.iter().any(|e| matches!(e, TailEvent::Data { .. }));
        assert!(has_data, "must emit data after truncation");

        // Truncated must come BEFORE Data in the event list.
        let trunc_idx = events
            .iter()
            .position(|e| matches!(e, TailEvent::Truncated { .. }))
            .unwrap();
        let data_idx = events
            .iter()
            .position(|e| matches!(e, TailEvent::Data { .. }))
            .unwrap();
        assert!(
            trunc_idx < data_idx,
            "Truncated event must precede Data event"
        );
    }

    /// #800: read_new_data must not exceed MAX_READ_PER_POLL.
    #[test]
    fn test_read_cap_prevents_oom() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("large.log");

        // Write more than MAX_READ_PER_POLL (4 MiB) — use 5 MiB.
        let target_size = 5 * 1024 * 1024;
        {
            let mut f = File::create(&log_path).unwrap();
            let line = "x".repeat(1023) + "\n"; // 1 KiB per line
            let lines_needed = target_size / 1024;
            for _ in 0..lines_needed {
                f.write_all(line.as_bytes()).unwrap();
            }
        }

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();

        // First poll should read at most MAX_READ_PER_POLL bytes.
        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();

        let total_bytes: usize = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.len()),
                _ => None,
            })
            .sum();

        assert!(
            total_bytes <= FileTailer::MAX_READ_PER_POLL,
            "read should be capped at MAX_READ_PER_POLL, got {} bytes",
            total_bytes
        );
        assert!(total_bytes > 0, "should read some data");

        // Second poll should read the remaining data.
        std::thread::sleep(Duration::from_millis(50));
        let events2 = tailer.poll().unwrap();
        let total_bytes2: usize = events2
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.len()),
                _ => None,
            })
            .sum();
        assert!(total_bytes2 > 0, "second poll should read remaining data");
    }

    /// #656: set_offset must reset to 0 if offset > file size.
    #[test]
    fn test_set_offset_validates_against_file_size() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("stale.log");

        // Write a small file (100 bytes).
        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "small file content").unwrap();
        }

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();

        // Try to set offset beyond file size (stale checkpoint).
        tailer.set_offset(&log_path, 999_999).unwrap();

        // Offset should be reset to 0, not 999_999.
        let offset = tailer.get_offset(&log_path).unwrap();
        assert_eq!(offset, 0, "stale offset should reset to 0");
    }

    /// #1037: set_offset_by_source must reset to 0 when checkpoint offset > file size.
    #[test]
    fn test_set_offset_by_source_validates_against_file_size() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("source_stale.log");
        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "small file content").unwrap();
        }

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        tailer.poll().unwrap();

        let source_id = tailer
            .file_offsets()
            .into_iter()
            .map(|(sid, _)| sid)
            .next()
            .expect("non-empty file should have source id");

        // Try to set an offset beyond file size (stale checkpoint).
        tailer.set_offset_by_source(source_id, 999_999).unwrap();
        let offset = tailer.get_offset(&log_path).unwrap();
        assert_eq!(offset, 0, "stale source offset should reset to 0");
    }

    /// #697: Evicted file offsets must appear in file_offsets() so they are
    /// included in checkpoint data and survive crashes.
    #[test]
    fn test_evicted_offsets_in_checkpoint_data() {
        let dir = tempfile::tempdir().unwrap();

        // Create 3 files with content; limit to 2 so one is evicted.
        let mut log_paths = Vec::new();
        for i in 0..3 {
            let p = dir.path().join(format!("{i}.log"));
            {
                let mut f = File::create(&p).unwrap();
                writeln!(f, "content for file {i}").unwrap();
            }
            log_paths.push(p);
        }

        let pattern = format!("{}/*.log", dir.path().display());
        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            glob_rescan_interval_ms: 60_000,
            max_open_files: 2,
            ..Default::default()
        };
        let mut tailer = FileTailer::new_with_globs(&[&pattern], config).unwrap();

        // Read initial data, then trigger eviction.
        std::thread::sleep(Duration::from_millis(50));
        tailer.poll().unwrap();
        assert_eq!(tailer.num_files(), 2, "evicted to max_open_files=2");

        // file_offsets() must include evicted files too.
        let offsets = tailer.file_offsets();
        assert_eq!(
            offsets.len(),
            3,
            "file_offsets() must include 2 active + 1 evicted file"
        );

        // All offsets should be non-zero (we read data from all 3 files).
        for (sid, off) in &offsets {
            assert!(
                sid.0 != 0,
                "SourceId should be non-zero for files with data"
            );
            assert!(off.0 > 0, "offset should be non-zero after reading data");
        }
    }

    /// #817: open_file_at must verify fingerprint before restoring evicted offset.
    /// If a file is evicted, deleted, and a new file appears at the same path,
    /// the saved offset must be ignored.
    #[test]
    fn test_evicted_offset_fingerprint_mismatch() {
        let dir = tempfile::tempdir().unwrap();

        // Create 3 files; limit to 2 so one is evicted.
        let mut log_paths = Vec::new();
        for i in 0..3 {
            let p = dir.path().join(format!("{i}.log"));
            {
                let mut f = File::create(&p).unwrap();
                // Write enough data so each file has a unique fingerprint.
                writeln!(f, "unique content for file number {i} with padding to ensure distinct fingerprints").unwrap();
            }
            log_paths.push(p);
        }

        let pattern = format!("{}/*.log", dir.path().display());
        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            glob_rescan_interval_ms: 50,
            max_open_files: 2,
            ..Default::default()
        };
        let mut tailer = FileTailer::new_with_globs(&[&pattern], config).unwrap();

        // Read and trigger eviction.
        std::thread::sleep(Duration::from_millis(50));
        tailer.poll().unwrap();
        assert_eq!(tailer.num_files(), 2);

        // Find which file was evicted by checking which path is not in files.
        let evicted_path = log_paths
            .iter()
            .find(|p| tailer.get_offset(p).is_none())
            .expect("one file should be evicted")
            .clone();

        // Delete the evicted file and create a new one at the same path
        // with completely different content.
        fs::remove_file(&evicted_path).unwrap();
        {
            let mut f = File::create(&evicted_path).unwrap();
            writeln!(f, "THIS IS A COMPLETELY DIFFERENT FILE WITH NEW CONTENT").unwrap();
        }

        // Wait for glob rescan to pick it up and re-open.
        std::thread::sleep(Duration::from_millis(150));
        let events = tailer.poll().unwrap();

        // The new file should be read from the beginning, not from the stale offset.
        let data: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { path, bytes, .. } if path == &evicted_path => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        let s = String::from_utf8_lossy(&data);
        assert!(
            s.contains("COMPLETELY DIFFERENT FILE"),
            "new file should be read from beginning, not stale offset. Got: {s}"
        );
    }

    /// Glob-discovered files must respect start_from_end config.
    #[test]
    fn glob_rescan_respects_start_from_end() {
        // Verify that files discovered during glob rescan (new pods, new log files)
        // respect the start_from_end config rather than hardcoding false.
        let dir = tempfile::tempdir().unwrap();
        let pattern = format!("{}/*.log", dir.path().display());

        // Write an initial file with existing content before the tailer starts.
        let initial_path = dir.path().join("initial.log");
        {
            let mut f = File::create(&initial_path).unwrap();
            for i in 0..10 {
                writeln!(f, "old line {i}").unwrap();
            }
        }

        let config = TailConfig {
            start_from_end: true,
            poll_interval_ms: 10,
            glob_rescan_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new_with_globs(&[&pattern], config).unwrap();

        // First poll: initial file is opened with start_from_end=true, so no old data.
        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();
        let old_data: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        assert!(
            old_data.is_empty(),
            "initial poll with start_from_end=true should produce no data from existing content"
        );

        // Now create a NEW file (simulates a new pod log appearing during rescan).
        let new_path = dir.path().join("new-pod.log");
        {
            let mut f = File::create(&new_path).unwrap();
            for i in 0..10 {
                writeln!(f, "new pod old line {i}").unwrap();
            }
        }

        // Poll enough times for the glob rescan to discover and open the new file.
        std::thread::sleep(Duration::from_millis(100));
        let events = tailer.poll().unwrap();
        let discovered_data: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        assert!(
            discovered_data.is_empty(),
            "glob-rescan-discovered file with start_from_end=true must not return old content, got: {}",
            String::from_utf8_lossy(&discovered_data)
        );

        // Appending new content to the discovered file MUST be visible.
        {
            let mut f = fs::OpenOptions::new().append(true).open(&new_path).unwrap();
            writeln!(f, "new pod new line").unwrap();
        }

        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();
        let new_content: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        let s = String::from_utf8_lossy(&new_content);
        assert!(
            s.contains("new pod new line"),
            "newly appended content must be visible after start_from_end discovery, got: {s}"
        );
    }

    /// #1043: open_file_at must not restore an evicted offset that exceeds EOF.
    #[test]
    fn test_evicted_offset_clamped_when_file_shrinks() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.log");
        let b = dir.path().join("b.log");

        // Both files share the same 4-byte fingerprint prefix so that
        // whichever file gets evicted will still match identity after truncation,
        // exercising the stale-offset clamping path (not the identity-mismatch path).
        {
            let mut fa = File::create(&a).unwrap();
            // 200 bytes so restored offset can exceed the later truncated size.
            write!(fa, "ABCD").unwrap();
            write!(fa, "{}", "x".repeat(196)).unwrap();
            let mut fb = File::create(&b).unwrap();
            write!(fb, "ABCD").unwrap();
            writeln!(fb, " initial b content").unwrap();
        }

        let pattern = format!("{}/*.log", dir.path().display());
        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            glob_rescan_interval_ms: 50,
            max_open_files: 1,
            fingerprint_bytes: 4,
            ..Default::default()
        };
        let mut tailer = FileTailer::new_with_globs(&[&pattern], config).unwrap();

        // Initial poll reads both and then evicts one due to max_open_files=1.
        std::thread::sleep(Duration::from_millis(50));
        tailer.poll().unwrap();
        assert_eq!(tailer.num_files(), 1, "one file should be evicted");

        let evicted = if tailer.get_offset(&a).is_none() {
            a.clone()
        } else if tailer.get_offset(&b).is_none() {
            b.clone()
        } else {
            panic!("expected either a.log or b.log to be evicted");
        };

        // Shrink the evicted file but preserve the first 4 bytes ("ABCD")
        // so identity still matches and stale-offset path is exercised.
        {
            let mut f = File::create(&evicted).unwrap(); // truncate
            writeln!(f, "ABCD_shrunk").unwrap(); // size << previous offset
        }

        // Wait for rescan so evicted file is re-opened.
        std::thread::sleep(Duration::from_millis(150));
        let events = tailer.poll().unwrap();

        // If stale offset is incorrectly restored past EOF, we'd read nothing.
        // With clamping, we should read from beginning and see "ABCD_shrunk".
        let data: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { path, bytes, .. } if path == &evicted => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        let s = String::from_utf8_lossy(&data);
        assert!(
            s.contains("ABCD_shrunk"),
            "re-opened shrunken file should be read from beginning, got: {s}"
        );
    }

    /// #730: Non-existent file paths should not prevent construction.
    #[test]
    fn test_nonexistent_path_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does_not_exist.log");

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        };

        // Should succeed — missing files are warned but not fatal.
        let tailer = FileTailer::new(std::slice::from_ref(&missing), config);
        assert!(tailer.is_ok(), "missing path should not fail construction");
        assert_eq!(tailer.unwrap().num_files(), 0);
    }

    /// #543: poll errors should trigger exponential backoff instead of spinning.
    #[test]
    fn test_error_backoff_grows_exponentially() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("backoff.log");
        File::create(&log_path).unwrap();

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 0,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();

        // Inject watcher errors directly through the discovery receiver.
        let (tx, rx) = crossbeam_channel::unbounded();
        tailer.discovery.fs_events = rx;

        tx.send(Err(notify::Error::generic("boom-1"))).unwrap();
        let first_poll_at = Instant::now();
        let _ = tailer.poll().unwrap();
        let first_until = tailer
            .error_backoff_until
            .expect("first error should schedule backoff");
        let first_delay = first_until.duration_since(first_poll_at);
        assert_eq!(tailer.consecutive_error_polls, 1);

        // Wait until the first backoff has expired before triggering the second error,
        // so the poll is not suppressed by the active backoff window.
        while Instant::now() < first_until {
            std::thread::sleep(Duration::from_millis(5));
        }
        tx.send(Err(notify::Error::generic("boom-2"))).unwrap();
        let second_poll_at = Instant::now();
        let _ = tailer.poll().unwrap();
        let second_until = tailer
            .error_backoff_until
            .expect("second error should schedule backoff");
        let second_delay = second_until.duration_since(second_poll_at);
        assert_eq!(tailer.consecutive_error_polls, 2);

        assert!(
            second_until > first_until,
            "second backoff should be longer"
        );
        assert!(
            second_delay > first_delay,
            "exponential backoff should grow"
        );
    }

    /// #544: file I/O and watcher errors must not use eprintln!.
    #[test]
    fn test_tail_uses_tracing_not_eprintln() {
        let source = include_str!("tail.rs");
        let forbidden = ["eprint", "ln!("].concat();
        assert!(
            !source.contains(&forbidden),
            "tailer should log via tracing, not eprintln!"
        );
    }

    /// #801: a hot file must not consume the entire poll; each file gets a byte budget.
    #[test]
    fn test_per_file_budget_prevents_starvation() {
        let dir = tempfile::tempdir().unwrap();
        let hot_path = dir.path().join("hot.log");
        let cold_path = dir.path().join("cold.log");

        {
            let mut hot = File::create(&hot_path).unwrap();
            // 1 MiB hot file.
            hot.write_all(&vec![b'x'; 1024 * 1024]).unwrap();
        }
        {
            let mut cold = File::create(&cold_path).unwrap();
            cold.write_all(b"cold-line\n").unwrap();
        }

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 0,
            per_file_read_budget_bytes: 64 * 1024,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(&[hot_path.clone(), cold_path.clone()], config).unwrap();

        let events = tailer.poll().unwrap();
        let hot_bytes: usize = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { path, bytes, .. } if path == &hot_path => Some(bytes.len()),
                _ => None,
            })
            .sum();
        let cold_bytes: usize = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { path, bytes, .. } if path == &cold_path => Some(bytes.len()),
                _ => None,
            })
            .sum();

        // With the capped read slice the hot file must not exceed the budget at all.
        assert!(
            hot_bytes <= 64 * 1024,
            "hot file should be budget-limited, got {hot_bytes}"
        );
        assert!(cold_bytes > 0, "cold file should still be read this cycle");
    }

    /// #811: copytruncate must reset offset when file shrinks below current offset.
    #[test]
    fn test_copytruncate_resets_offset_on_size_drop() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("copytruncate.log");

        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "old-line-1").unwrap();
            writeln!(f, "old-line-2").unwrap();
            writeln!(f, "old-line-3").unwrap();
        }

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();
        std::thread::sleep(Duration::from_millis(30));
        let _ = tailer.poll().unwrap();
        let prior_offset = tailer.get_offset(&log_path).unwrap();
        assert!(prior_offset > 0);

        // Truncate to a much smaller file so len < prior offset.
        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "new-small-line").unwrap();
        }

        std::thread::sleep(Duration::from_millis(30));
        let events = tailer.poll().unwrap();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, TailEvent::Truncated { .. })),
            "must emit Truncated when size drops below offset"
        );
        let data: Vec<u8> = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .flatten()
            .collect();
        let body = String::from_utf8_lossy(&data);
        assert!(
            body.contains("new-small-line"),
            "must read from reset offset"
        );
    }
}

// ---------------------------------------------------------------------------
// Kani proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;

    /// Pure model of the `eof_emitted` state transition in `FileTailer::poll()`.
    ///
    /// Mirrors the logic from the `ReadResult::Data`, `ReadResult::TruncatedThenData`,
    /// and `ReadResult::NoData` arms exactly, making the invariant explicitly testable.
    ///
    /// Returns `(new_eof_emitted, should_emit_eof_event)`.
    fn eof_transition(eof_emitted: bool, had_data: bool) -> (bool, bool) {
        if had_data {
            (false, false)
        } else if !eof_emitted {
            (true, true)
        } else {
            (true, false)
        }
    }

    /// EndOfFile is emitted at most once per no-data streak: the event fires only
    /// when `eof_emitted` transitions from `false` to `true`, never while it is
    /// already `true`.
    #[kani::proof]
    fn verify_eof_emitted_at_most_once_per_no_data_streak() {
        let eof_emitted: bool = kani::any();
        let (_, fires) = eof_transition(eof_emitted, false); // NoData
        if fires {
            assert!(!eof_emitted, "EndOfFile may only fire when flag was false");
        }
        kani::cover!(fires, "EndOfFile event fired");
        kani::cover!(
            !fires && eof_emitted,
            "EndOfFile suppressed — already emitted"
        );
    }

    /// Data always resets the eof_emitted flag to false and never fires EndOfFile.
    ///
    /// This ensures a fresh EndOfFile can be emitted the next time reads stall,
    /// correctly signalling the downstream framer to flush any partial-line remainder.
    #[kani::proof]
    fn verify_data_resets_eof_flag() {
        let eof_emitted: bool = kani::any();
        let (new_flag, fires) = eof_transition(eof_emitted, true); // Data
        assert!(!new_flag, "Data must reset eof_emitted to false");
        assert!(!fires, "Data must not emit EndOfFile");
        kani::cover!(eof_emitted, "eof_emitted was true before data arrived");
        kani::cover!(!eof_emitted, "eof_emitted was already false");
    }

    /// Two consecutive NoData polls emit EndOfFile exactly once (on the first).
    #[kani::proof]
    fn verify_two_no_data_polls_emit_exactly_once() {
        let (state1, fires1) = eof_transition(false, false); // first NoData
        let (state2, fires2) = eof_transition(state1, false); // second NoData
        assert!(fires1, "first NoData poll must emit EndOfFile");
        assert!(!fires2, "second NoData poll must not emit again");
        assert!(state1 && state2, "flag stays true after both polls");
        kani::cover!(true, "two-poll no-data sequence verified");
    }

    /// After data resets the flag, the next NoData streak fires EndOfFile again.
    ///
    /// Sequence: NoData → Data → NoData.  Both stalls must emit exactly one event.
    #[kani::proof]
    fn verify_eof_fires_again_after_data_resets_flag() {
        let (after_nodata, fires1) = eof_transition(false, false); // first stall
        let (after_data, _) = eof_transition(after_nodata, true); // data arrives
        let (_, fires2) = eof_transition(after_data, false); // second stall
        assert!(fires1, "first stall must emit EndOfFile");
        assert!(fires2, "second stall must emit EndOfFile after data reset");
        kani::cover!(true, "data-reset cycle verified");
    }
}

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

/// Identity of a file based on device + inode + content fingerprint.
/// Survives renames. Detects inode reuse via fingerprint mismatch.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FileIdentity {
    pub device: u64,
    pub inode: u64,
    pub fingerprint: u64,
}

/// State tracked per tailed file.
struct TailedFile {
    #[expect(dead_code, reason = "retained for debug logging")]
    path: PathBuf,
    identity: FileIdentity,
    file: File,
    offset: u64,
    /// Last time we successfully read new data.
    last_read: Instant,
}

/// Events emitted by the tailer.
pub enum TailEvent {
    /// New data available. The Vec is raw bytes read from the file.
    /// NOT necessarily aligned on line boundaries — the pipeline handles that.
    Data { path: PathBuf, bytes: Vec<u8> },
    /// A file was rotated (old file at path replaced by new file).
    Rotated { path: PathBuf },
    /// A file was truncated (copytruncate rotation).
    Truncated { path: PathBuf },
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
    let meta = fs::metadata(path)?;
    let mut file = File::open(path)?;
    let fingerprint = compute_fingerprint(&mut file, fingerprint_bytes)?;
    Ok(FileIdentity {
        device: meta.dev(),
        inode: meta.ino(),
        fingerprint,
    })
}

/// Expand a list of glob patterns into the set of matching `PathBuf` values.
///
/// Patterns that match no files are silently skipped. Errors from the glob
/// iterator (e.g., permission denied on individual entries) are also skipped.
fn expand_glob_patterns(patterns: &[&str]) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    for pattern in patterns {
        match glob::glob(pattern) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    paths.push(entry);
                }
            }
            Err(e) => {
                eprintln!("warn: invalid glob pattern {pattern:?}: {e}");
            }
        }
    }
    paths
}

/// The file tailer. Watches one or more file paths and yields data as it appears.
pub struct FileTailer {
    config: TailConfig,
    /// Files we're actively tailing, keyed by canonical path.
    files: HashMap<PathBuf, TailedFile>,
    /// Paths we've been asked to watch (literal paths, including those discovered via globs).
    watch_paths: Vec<PathBuf>,
    /// Glob patterns to re-evaluate periodically for new file discovery.
    glob_patterns: Vec<String>,
    /// Read buffer, reused across reads to avoid allocation.
    read_buf: Vec<u8>,
    /// Notify watcher for filesystem events.
    watcher: notify::RecommendedWatcher,
    /// Directories currently registered with the notify watcher.
    watched_dirs: HashSet<PathBuf>,
    /// Saved offsets for files evicted from the open-file LRU cache.
    /// When a file is re-opened after eviction, we seek to the saved offset
    /// to avoid duplicating or losing data.
    evicted_offsets: HashMap<PathBuf, u64>,
    /// Channel receiving filesystem events from the watcher.
    fs_events: crossbeam_channel::Receiver<notify::Result<notify::Event>>,
    /// Last time we did a full poll scan.
    last_poll: Instant,
    /// Last time we re-evaluated glob patterns.
    last_glob_rescan: Instant,
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
            read_buf: vec![0u8; config.read_buf_size],
            config,
            files: HashMap::new(),
            watch_paths: paths.to_vec(),
            glob_patterns: Vec::new(),
            watcher,
            watched_dirs,
            fs_events: rx,
            last_poll: Instant::now(),
            last_glob_rescan: Instant::now(),
            evicted_offsets: HashMap::new(),
        };

        // Open existing files.
        for path in paths {
            if path.exists()
                && let Err(e) = tailer.open_file(path)
            {
                eprintln!("warn: could not open {}: {e}", path.display());
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

        let mut tailer = Self::new(&initial_paths, config)?;
        tailer.glob_patterns = patterns.iter().map(|s| s.to_string()).collect();
        Ok(tailer)
    }

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
    fn rescan_globs(&mut self) {
        if self.glob_patterns.is_empty() {
            return;
        }

        let pattern_refs: Vec<&str> = self.glob_patterns.iter().map(String::as_str).collect();
        let candidates = expand_glob_patterns(&pattern_refs);

        let existing: HashSet<&PathBuf> = self.watch_paths.iter().collect();
        let new_paths: Vec<PathBuf> = candidates
            .into_iter()
            .filter(|p| !existing.contains(p))
            .collect();

        for path in new_paths {
            // Watch the parent directory for future events.
            if let Some(parent) = path.parent()
                && let Err(e) = self.watch_dir(parent)
            {
                eprintln!("warn: could not watch {}: {e}", parent.display());
            }

            // Open the file (new files from glob discovery always read from the beginning).
            let saved = self.config.start_from_end;
            self.config.start_from_end = false;
            if let Err(e) = self.open_file(&path) {
                eprintln!("warn: could not open {}: {e}", path.display());
            }
            self.config.start_from_end = saved;

            self.watch_paths.push(path);
        }
    }

    /// Open and start tailing a file. If start_from_end is true, seeks to EOF.
    /// If the file was previously evicted from the LRU cache, restores the
    /// saved offset so reads resume where they left off.
    fn open_file(&mut self, path: &Path) -> io::Result<()> {
        let identity = identify_file(path, self.config.fingerprint_bytes)?;
        let mut file = File::open(path)?;

        let offset = if let Some(saved) = self.evicted_offsets.remove(path) {
            file.seek(SeekFrom::Start(saved))?
        } else if self.config.start_from_end {
            file.seek(SeekFrom::End(0))?
        } else {
            0
        };

        self.files.insert(
            path.to_path_buf(),
            TailedFile {
                path: path.to_path_buf(),
                identity,
                file,
                offset,
                last_read: Instant::now(),
            },
        );

        Ok(())
    }

    /// Poll for new data. Returns a batch of events.
    /// Call this in your main loop. It will:
    /// 1. Drain any filesystem notifications (low latency path)
    /// 2. If enough time has passed, do a full poll scan (safety net)
    /// 3. If enough time has passed, re-evaluate glob patterns (new file discovery)
    /// 4. Read new data from any files that have grown
    pub fn poll(&mut self) -> io::Result<Vec<TailEvent>> {
        let mut events = Vec::new();

        // Drain filesystem notifications. These tell us something changed
        // but we still need to read() to get the data.
        let mut something_changed = false;
        while let Ok(res) = self.fs_events.try_recv() {
            if let Ok(_event) = res {
                something_changed = true;
            }
        }

        // Periodic full poll as safety net.
        let poll_interval = Duration::from_millis(self.config.poll_interval_ms);
        let glob_rescan_due = self.config.glob_rescan_interval_ms > 0
            && self.last_glob_rescan.elapsed()
                >= Duration::from_millis(self.config.glob_rescan_interval_ms);
        let should_poll =
            something_changed || self.last_poll.elapsed() >= poll_interval || glob_rescan_due;

        if !should_poll {
            return Ok(events);
        }
        self.last_poll = Instant::now();

        // Re-evaluate glob patterns to discover new files.
        if glob_rescan_due {
            self.rescan_globs();
            self.last_glob_rescan = Instant::now();
        }

        // Check for new/rotated files.
        let watch_paths = self.watch_paths.clone();
        for path in &watch_paths {
            if !path.exists() {
                continue;
            }

            let current_identity = match identify_file(path, self.config.fingerprint_bytes) {
                Ok(id) => id,
                Err(_) => continue,
            };

            // Check for rotation or new file — borrow released before any mutation.
            let is_rotated = self
                .files
                .get(path)
                .map(|tailed| tailed.identity != current_identity)
                .unwrap_or(false);
            let is_new = !self.files.contains_key(path);

            if is_rotated {
                // Drain any bytes written to the old fd after the last read but
                // before the rename.  The kernel keeps the old inode alive while
                // our File handle is open, so these bytes are still readable.
                match self.read_new_data(path) {
                    Ok(Some(data)) => {
                        events.push(TailEvent::Data {
                            path: path.clone(),
                            bytes: data,
                        });
                    }
                    Ok(None) => {}
                    Err(e) => {
                        eprintln!("warn: error draining rotated file {}: {e}", path.display());
                    }
                }

                // Now that the old fd is fully drained, emit the rotation event
                // and switch to the new file.
                events.push(TailEvent::Rotated { path: path.clone() });
                let _ = self.files.remove(path);
                let saved_start_from_end = self.config.start_from_end;
                self.config.start_from_end = false; // read new file from beginning
                let _ = self.open_file(path);
                self.config.start_from_end = saved_start_from_end;
            } else if is_new {
                // New file appeared.
                let saved = self.config.start_from_end;
                self.config.start_from_end = false; // new files read from beginning
                let _ = self.open_file(path);
                self.config.start_from_end = saved;
            }
        }

        // Read new data from all tailed files.
        let paths: Vec<PathBuf> = self.files.keys().cloned().collect();
        for path in paths {
            match self.read_new_data(&path) {
                Ok(Some(data)) => {
                    events.push(TailEvent::Data {
                        path: path.clone(),
                        bytes: data,
                    });
                }
                Ok(None) => {} // no new data
                Err(e) => {
                    eprintln!("warn: error reading {}: {e}", path.display());
                }
            }
        }

        // Remove entries for files that have been unlinked (nlink == 0).
        // Using nlink instead of !path.exists() avoids data loss: on Unix a
        // file can be unlinked while the FD is still open, so the path
        // disappears but unread data remains readable through the FD.
        let deleted: Vec<PathBuf> = self
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
            // Drain any remaining data before closing the FD.
            match self.read_new_data(path) {
                Ok(Some(data)) => {
                    events.push(TailEvent::Data {
                        path: path.clone(),
                        bytes: data,
                    });
                }
                Ok(None) => {}
                Err(e) => {
                    eprintln!("warn: error draining deleted file {}: {e}", path.display());
                }
            }
            self.files.remove(path);
        }

        // Evict least-recently-read files when over the open-file limit.
        // Save each evicted file's offset so it can resume from the correct
        // position when re-opened on a future glob rescan.
        if self.files.len() > self.config.max_open_files {
            let mut by_age: Vec<(PathBuf, Instant)> = self
                .files
                .iter()
                .map(|(path, tailed)| (path.clone(), tailed.last_read))
                .collect();
            by_age.sort_by_key(|(_, last_read)| *last_read);
            let to_remove = self.files.len() - self.config.max_open_files;
            for (path, _) in by_age.into_iter().take(to_remove) {
                if let Some(evicted) = self.files.remove(&path) {
                    self.evicted_offsets.insert(path, evicted.offset);
                }
            }
        }

        Ok(events)
    }

    /// Read ALL available new data from a file. Drains until read() returns 0.
    /// Returns None if no new data.
    fn read_new_data(&mut self, path: &Path) -> io::Result<Option<Vec<u8>>> {
        let tailed = match self.files.get_mut(path) {
            Some(t) => t,
            None => return Ok(None),
        };

        // Check current file size.
        let meta = tailed.file.metadata()?;
        let current_size = meta.len();

        if current_size < tailed.offset {
            // File was truncated (copytruncate rotation).
            tailed.offset = 0;
            tailed.file.seek(SeekFrom::Start(0))?;
            tailed.identity.fingerprint =
                compute_fingerprint(&mut tailed.file, self.config.fingerprint_bytes)?;
            tailed.file.seek(SeekFrom::Start(0))?;
        }

        if current_size <= tailed.offset {
            return Ok(None);
        }

        // Read ALL available bytes in a loop until we drain everything.
        let mut result = Vec::new();
        loop {
            let n = tailed.file.read(&mut self.read_buf)?;
            if n == 0 {
                break;
            }
            result.extend_from_slice(&self.read_buf[..n]);
            tailed.offset += n as u64;
        }

        if result.is_empty() {
            return Ok(None);
        }

        tailed.last_read = Instant::now();
        Ok(Some(result))
    }

    /// Get the current offset for a file (for checkpointing).
    pub fn get_offset(&self, path: &Path) -> Option<u64> {
        self.files.get(path).map(|f| f.offset)
    }

    /// Set the offset for a file (for restoring from checkpoint).
    pub fn set_offset(&mut self, path: &Path, offset: u64) -> io::Result<()> {
        if let Some(tailed) = self.files.get_mut(path) {
            tailed.offset = offset;
            tailed.file.seek(SeekFrom::Start(offset))?;
        }
        Ok(())
    }

    /// Number of files currently being tailed.
    pub fn num_files(&self) -> usize {
        self.files.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

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
            let mut f = std::io::BufWriter::new(f);
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
        let initial_watch_count = tailer.watch_paths.len();

        // Wait for rescan and poll again — file should not be added twice.
        std::thread::sleep(Duration::from_millis(150));
        tailer.poll().unwrap();

        assert_eq!(
            tailer.watch_paths.len(),
            initial_watch_count,
            "watch_paths should not grow after rescan of already-known file"
        );
        assert_eq!(tailer.num_files(), 1, "should still tail exactly one file");
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
    }
}

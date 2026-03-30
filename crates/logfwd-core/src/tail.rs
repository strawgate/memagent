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

use std::collections::HashMap;
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
}

impl Default for TailConfig {
    fn default() -> Self {
        TailConfig {
            poll_interval_ms: 250,
            read_buf_size: 256 * 1024,
            fingerprint_bytes: 1024,
            start_from_end: true,
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

/// The file tailer. Watches one or more file paths and yields data as it appears.
pub struct FileTailer {
    config: TailConfig,
    /// Files we're actively tailing, keyed by canonical path.
    files: HashMap<PathBuf, TailedFile>,
    /// Paths we've been asked to watch.
    watch_paths: Vec<PathBuf>,
    /// Read buffer, reused across reads to avoid allocation.
    read_buf: Vec<u8>,
    /// Notify watcher for filesystem events.
    _watcher: notify::RecommendedWatcher,
    /// Channel receiving filesystem events from the watcher.
    fs_events: crossbeam_channel::Receiver<notify::Result<notify::Event>>,
    /// Last time we did a full poll scan.
    last_poll: Instant,
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
        let mut watched_dirs = std::collections::HashSet::new();
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
            _watcher: watcher,
            fs_events: rx,
            last_poll: Instant::now(),
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

    /// Open and start tailing a file. If start_from_end is true, seeks to EOF.
    fn open_file(&mut self, path: &Path) -> io::Result<()> {
        let identity = identify_file(path, self.config.fingerprint_bytes)?;
        let mut file = File::open(path)?;

        let offset = if self.config.start_from_end {
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
    /// 3. Read new data from any files that have grown
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
        let should_poll = something_changed || self.last_poll.elapsed() >= poll_interval;

        if !should_poll {
            return Ok(events);
        }
        self.last_poll = Instant::now();

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
                        eprintln!(
                            "warn: error draining rotated file {}: {e}",
                            path.display()
                        );
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

        let mut tailer = FileTailer::new(&[log_path.clone()], config).unwrap();

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
        let mut tailer = FileTailer::new(&[log_path.clone()], config).unwrap();

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
        let mut tailer = FileTailer::new(&[log_path.clone()], config).unwrap();

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
        let mut tailer = FileTailer::new(&[log_path.clone()], config).unwrap();

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
        let mut tailer = FileTailer::new(&[log_path.clone()], config).unwrap();

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
}

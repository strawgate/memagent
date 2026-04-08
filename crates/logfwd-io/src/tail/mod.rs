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

mod discovery;
mod glob;
mod identity;
mod reader;
mod tailer;

pub use identity::ByteOffset;
pub use tailer::{FileTailer, TailConfig, TailEvent};

#[cfg(test)]
use glob::{expand_glob_patterns, glob_max_depth, glob_root};
#[cfg(test)]
use identity::identify_file;

#[cfg(test)]
mod tests {
    use super::*;
    use logfwd_types::diagnostics::ComponentHealth;
    use std::fs::{self, File};
    use std::io::{self, Write};
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

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

    /// #1375/#1463: glob patterns must match files regardless of `./` prefix
    /// normalization, including bare `*.log`.
    ///
    /// This stays cwd-independent by asserting path-form matching directly
    /// (`entry_path`, stripped, and `./`-prefixed forms) instead of mutating
    /// process cwd in tests.
    #[test]
    fn test_expand_glob_patterns_path_normalization() {
        let mut builder = globset::GlobSetBuilder::new();
        builder.add(globset::Glob::new("*.log").unwrap());
        let bare_set = builder.build().unwrap();
        let bare_entry = PathBuf::from("./app.log");
        let bare_stripped = bare_entry.strip_prefix(".").unwrap();
        let bare_prefixed = PathBuf::from("./").join(bare_stripped);
        assert!(
            bare_set.is_match(&bare_entry)
                || bare_set.is_match(bare_stripped)
                || bare_set.is_match(&bare_prefixed),
            "bare '*.log' must match at least one normalized form"
        );

        let dir = tempfile::tempdir().unwrap();
        let logs = dir.path().join("logs");
        fs::create_dir_all(&logs).unwrap();

        let target = logs.join("app.log");
        let other = logs.join("app.txt");
        {
            let mut f = File::create(&target).unwrap();
            writeln!(f, "hello").unwrap();
        }
        File::create(&other).unwrap();

        // Absolute pattern with subdirectory.
        let pattern = format!("{}/*.log", logs.display());
        let matches = expand_glob_patterns(&[&pattern]);
        assert!(
            matches.iter().any(|p| p == &target),
            "absolute pattern should match app.log, got: {matches:?}"
        );
        assert!(
            !matches.iter().any(|p| p == &other),
            "should not match .txt file, got: {matches:?}"
        );

        // Flat file in root for wildcard testing.
        let flat = dir.path().join("flat.log");
        File::create(&flat).unwrap();
        let flat_pattern = format!("{}/*.log", dir.path().display());
        let flat_matches = expand_glob_patterns(&[&flat_pattern]);
        assert!(
            flat_matches.iter().any(|p| p == &flat),
            "wildcard pattern should match flat.log, got: {flat_matches:?}"
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

    /// Regression / integration test for #1693: when a file is truncated and then
    /// deleted, the next `poll()` must emit a `Truncated` event with the old
    /// source_id followed by a `Data` event with a *new* source_id (the
    /// post-truncation fingerprint).
    ///
    /// This exercises the truncation-detection path through `poll()` →
    /// `read_all` (which detects the truncation) rather than calling
    /// `drain_file` directly.  A focused unit test for `drain_file` itself
    /// lives in `reader.rs`.
    ///
    /// Using the old source_id would associate post-truncation bytes with the
    /// previous file identity, causing duplicate ingestion after restart.
    #[test]
    fn test_poll_truncated_then_data_uses_new_source_id() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("drain_trunc_src.log");

        // Write initial content large enough to establish a non-zero fingerprint.
        {
            let mut f = File::create(&log_path).unwrap();
            // Write > fingerprint_bytes (default 1024) so the fingerprint is stable.
            writeln!(f, "{}", "A".repeat(300)).unwrap();
        }

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();

        // First poll — consume the initial data so offset > 0.
        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();
        assert!(
            events.iter().any(|e| matches!(e, TailEvent::Data { .. })),
            "expected initial data from first poll"
        );

        // Capture the source_id established after the first read.
        let old_source_id = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { source_id, .. } => *source_id,
                _ => None,
            })
            .last()
            .expect("should have a source_id from initial data");

        // Truncate-and-rewrite the file (copytruncate simulation): new content is
        // SHORTER than the old content so `current_size < tailed.offset` is true.
        // Also differs in content so the fingerprint changes.
        {
            let mut f = File::create(&log_path).unwrap();
            writeln!(f, "new short line").unwrap();
        }

        // Delete the file so drain_file is triggered via the deleted-file path.
        fs::remove_file(&log_path).unwrap();

        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();

        let truncated_event = events
            .iter()
            .find(|e| matches!(e, TailEvent::Truncated { .. }))
            .expect("should have a Truncated event");
        let data_event = events
            .iter()
            .find(|e| matches!(e, TailEvent::Data { .. }))
            .expect("should have a Data event");

        // The Truncated event must carry the OLD source_id (what downstream expects).
        if let TailEvent::Truncated { source_id, .. } = truncated_event {
            assert_eq!(
                *source_id,
                Some(old_source_id),
                "Truncated event must use the pre-truncation source_id"
            );
        }

        // The Data event must carry the NEW source_id (post-truncation fingerprint).
        if let TailEvent::Data {
            source_id: data_source_id,
            ..
        } = data_event
        {
            assert!(
                data_source_id.is_some(),
                "Data event after truncation must have a source_id (not None)"
            );
            assert_ne!(
                *data_source_id,
                Some(old_source_id),
                "Data event after truncation must use a NEW source_id, not the stale pre-truncation id"
            );
        }
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

    /// #800: when the configured budget exceeds the hard cap, reads must clamp
    /// exactly at MAX_READ_PER_POLL rather than "within one extra buffer".
    #[test]
    fn test_read_cap_clamps_exactly_at_max_read_per_poll() {
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
            per_file_read_budget_bytes: FileTailer::MAX_READ_PER_POLL * 2,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();

        // First poll should read exactly MAX_READ_PER_POLL bytes because the
        // configured budget exceeds the hard cap and the file is larger still.
        std::thread::sleep(Duration::from_millis(50));
        let events = tailer.poll().unwrap();

        let total_bytes: usize = events
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.len()),
                _ => None,
            })
            .sum();

        assert_eq!(
            total_bytes,
            FileTailer::MAX_READ_PER_POLL,
            "first poll should clamp exactly at MAX_READ_PER_POLL"
        );

        // Second poll should read the remaining 1 MiB.
        std::thread::sleep(Duration::from_millis(50));
        let events2 = tailer.poll().unwrap();
        let total_bytes2: usize = events2
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.len()),
                _ => None,
            })
            .sum();
        assert_eq!(
            total_bytes2,
            target_size - FileTailer::MAX_READ_PER_POLL,
            "second poll should read the exact remainder"
        );
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

    /// #1600 follow-up: restoring checkpoint offsets by source_id must also
    /// update evicted entries (not just currently-open files).
    #[test]
    fn test_set_offset_by_source_updates_evicted_entries() {
        let dir = tempfile::tempdir().unwrap();
        let mut log_paths = Vec::new();
        for i in 0..3 {
            let p = dir.path().join(format!("evicted-{i}.log"));
            {
                let mut f = File::create(&p).unwrap();
                writeln!(f, "line-0-{i}").unwrap();
                writeln!(f, "line-1-{i}").unwrap();
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

        // Read initial data and trigger one eviction.
        std::thread::sleep(Duration::from_millis(50));
        tailer.poll().unwrap();
        assert_eq!(tailer.num_files(), 2, "expected one file to be evicted");

        let (evicted_sid, evicted_path) = tailer
            .file_paths()
            .into_iter()
            .find(|(_, path)| tailer.get_offset(path).is_none())
            .expect("expected one evicted file path");
        assert!(
            log_paths.iter().any(|p| p == &evicted_path),
            "evicted path should come from test set"
        );

        tailer
            .set_offset_by_source(evicted_sid, 5)
            .expect("set offset by source should succeed for evicted entry");

        let updated = tailer
            .file_offsets()
            .into_iter()
            .find(|(sid, _)| *sid == evicted_sid)
            .map(|(_, off)| off.0)
            .expect("evicted source id should still be present in checkpoint data");
        assert_eq!(updated, 5, "evicted checkpoint offset was not updated");
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

    #[test]
    fn test_error_backoff_marks_tailer_degraded_until_clean_poll() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("health.log");
        File::create(&log_path).unwrap();

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 0,
            ..Default::default()
        };
        let mut tailer = FileTailer::new(std::slice::from_ref(&log_path), config).unwrap();
        assert_eq!(tailer.health(), ComponentHealth::Healthy);

        let (tx, rx) = crossbeam_channel::unbounded();
        tailer.discovery.fs_events = rx;
        tx.send(Err(notify::Error::generic("boom-health"))).unwrap();

        let _ = tailer.poll().unwrap();
        assert_eq!(tailer.health(), ComponentHealth::Degraded);

        let until = tailer
            .error_backoff_until
            .expect("error poll should schedule backoff");
        while Instant::now() < until {
            std::thread::sleep(Duration::from_millis(5));
        }

        let _ = tailer.poll().unwrap();
        assert_eq!(tailer.health(), ComponentHealth::Healthy);
    }

    /// #544: file I/O and watcher errors must not use eprintln!.
    #[test]
    fn test_tail_uses_tracing_not_eprintln() {
        let source = [
            include_str!("mod.rs"),
            include_str!("identity.rs"),
            include_str!("glob.rs"),
            include_str!("discovery.rs"),
            include_str!("reader.rs"),
            include_str!("tailer.rs"),
        ]
        .concat();
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

        // The hot file is larger than the fairness budget, so one poll should
        // read exactly one budget slice, not one extra buffer.
        assert_eq!(
            hot_bytes,
            64 * 1024,
            "hot file should clamp exactly to the configured budget"
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

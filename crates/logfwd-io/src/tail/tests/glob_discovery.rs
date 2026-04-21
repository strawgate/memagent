//! Tests for glob-based file discovery, rescan, deduplication, eviction,
//! and path normalization.

use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use super::super::*;
use super::super::glob::expand_glob_patterns;
use super::{create_test_stats, poll_until};

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
        glob_rescan_interval_ms: 60_000, // long interval -- not relevant for this test
        ..Default::default()
    };
    let mut tailer = FileTailer::new_with_globs(&[&pattern], config, create_test_stats()).unwrap();

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
    let mut tailer = FileTailer::new_with_globs(&[&pattern], config, create_test_stats()).unwrap();

    // No files exist yet -- tailer starts with nothing.
    assert_eq!(tailer.num_files(), 0, "no files should be tailed initially");

    // Create a new log file (simulating a new Kubernetes pod).
    let new_log = dir.path().join("pod-xyz.log");
    {
        let mut f = File::create(&new_log).unwrap();
        writeln!(f, "pod xyz line 1").unwrap();
    }

    // Poll until the rescan discovers and reads the new file.
    let events = poll_until(
        &mut tailer,
        Duration::from_secs(2),
        |events, tailer| {
            if tailer.num_files() != 1 {
                return false;
            }
            let all_data: Vec<u8> = events
                .iter()
                .filter_map(|e| match e {
                    TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                    _ => None,
                })
                .flatten()
                .collect();
            String::from_utf8_lossy(&all_data).contains("pod xyz line 1")
        },
        "timed out waiting for glob rescan to discover new file",
    );

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
    let mut tailer = FileTailer::new_with_globs(&[&pattern], config, create_test_stats()).unwrap();

    // File discovered at construction.
    assert_eq!(tailer.num_files(), 1);
    let initial_watch_count = tailer.discovery.watch_paths.len();

    // Wait for rescan and poll again -- file should not be added twice.
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
    let mut tailer = FileTailer::new_with_globs(&[&pattern], config, create_test_stats()).unwrap();

    // All 10 files discovered at construction -- eviction happens during poll.
    assert_eq!(tailer.num_files(), 10, "all files opened before first poll");

    // Poll -- should evict 5 least-recently-read files.
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
    let mut tailer = FileTailer::new_with_globs(&[&pattern], config, create_test_stats()).unwrap();

    // After first poll, eviction brings count to 2.
    let _ = poll_until(
        &mut tailer,
        Duration::from_secs(1),
        |_, tailer| tailer.num_files() == 2,
        "timed out waiting for initial max_open_files eviction",
    );
    assert_eq!(tailer.num_files(), 2, "evicted to max_open_files=2");

    // Append data to all files. The evicted file will be re-discovered.
    for p in &log_paths {
        let mut f = fs::OpenOptions::new().append(true).open(p).unwrap();
        writeln!(f, "new data").unwrap();
    }

    // Poll until glob rescan re-opens an evicted file and emits new data.
    let events = poll_until(
        &mut tailer,
        Duration::from_secs(2),
        |events, _| {
            let all_data: Vec<u8> = events
                .iter()
                .filter_map(|e| match e {
                    TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                    _ => None,
                })
                .flatten()
                .collect();
            String::from_utf8_lossy(&all_data).contains("new data")
        },
        "timed out waiting for evicted file rescan data",
    );

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
    let mut tailer = FileTailer::new_with_globs(&[&pattern], config, create_test_stats()).unwrap();

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

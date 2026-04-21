//! Tests for file offset checkpointing, source ID management, and
//! evicted-file offset handling.

use std::fs::{self, File};
use std::io::Write;
use std::time::Duration;

use super::super::*;
use super::{create_test_stats, poll_until};

#[cfg(unix)]
use super::filesystem_tracks_nlink_after_unlink;

#[cfg(unix)]
#[test]
fn test_deleted_file_cleanup() {
    if !filesystem_tracks_nlink_after_unlink() {
        return;
    }
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
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

    // Confirm file is being tailed.
    std::thread::sleep(Duration::from_millis(50));
    tailer.poll().unwrap();
    assert_eq!(tailer.num_files(), 1, "file should be open before deletion");

    // Delete the file.
    fs::remove_file(&log_path).unwrap();

    // Cleanup happens once the tailer observes the deletion; notify / metadata
    // reads are not instantaneous, so poll a few times before asserting.
    let cleaned_up = (0..20).any(|_| {
        std::thread::sleep(Duration::from_millis(25));
        tailer.poll().unwrap();
        tailer.num_files() == 0
    });
    assert!(
        cleaned_up,
        "deleted file should be removed from the files map (num_files={})",
        tailer.num_files()
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
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

    // First poll -- drain initial data.
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
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

    // First poll -- consume the initial data so offset > 0.
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
        .next_back()
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
#[cfg(unix)]
#[test]
fn test_glob_deleted_file_removed_from_watch_paths() {
    if !filesystem_tracks_nlink_after_unlink() {
        return;
    }
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
    let mut tailer = FileTailer::new_with_globs(&[&pattern], config, create_test_stats()).unwrap();

    // Read initial data so the tailer advances past the initial content.
    let _ = poll_until(
        &mut tailer,
        Duration::from_secs(1),
        |events, _| events.iter().any(|e| matches!(e, TailEvent::Data { .. })),
        "timed out waiting for initial glob tail data",
    );

    let paths_before = tailer.discovery.watch_paths.len();
    assert_eq!(paths_before, 5, "should have 5 watch_paths before deletion");

    // Delete all files.
    for p in &paths {
        fs::remove_file(p).unwrap();
    }

    // Poll until deletion cleanup is reflected in both watch_paths and files.
    let _ = poll_until(
        &mut tailer,
        Duration::from_secs(1),
        |_, tailer| tailer.discovery.watch_paths.is_empty() && tailer.num_files() == 0,
        "timed out waiting for deleted files cleanup",
    );

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
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

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
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();
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
    let mut tailer = FileTailer::new(&[path_a, path_b], config, create_test_stats()).unwrap();
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
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();
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
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

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
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();
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
    let mut tailer = FileTailer::new_with_globs(&[&pattern], config, create_test_stats()).unwrap();

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
    let mut tailer = FileTailer::new_with_globs(&[&pattern], config, create_test_stats()).unwrap();

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
            writeln!(
                f,
                "unique content for file number {i} with padding to ensure distinct fingerprints"
            )
            .unwrap();
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
    let mut tailer = FileTailer::new_with_globs(&[&pattern], config, create_test_stats()).unwrap();

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

#[test]
fn test_source_id_for_missing_path_is_none() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("source-id.log");
    File::create(&log_path).unwrap();

    let tailer = FileTailer::new(
        std::slice::from_ref(&log_path),
        TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            ..Default::default()
        },
        create_test_stats(),
    )
    .unwrap();

    assert_eq!(
        tailer.source_id_for_path(&dir.path().join("missing.log")),
        None
    );
}

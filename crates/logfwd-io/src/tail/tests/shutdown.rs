//! Tests for `poll_shutdown` behavior: terminal EOF emission, budget
//! exhaustion, deleted/evicted file cleanup during shutdown.

use std::fs;
use std::time::Duration;

use super::super::*;
use super::{create_test_stats, poll_until};

#[test]
fn poll_shutdown_emits_eof_for_caught_up_file() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("shutdown-eof.log");
    fs::write(&log_path, b"final-without-newline").unwrap();

    let config = TailConfig {
        start_from_end: false,
        poll_interval_ms: 60_000,
        ..Default::default()
    };
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

    let events = tailer.poll_shutdown().unwrap();
    let data: Vec<u8> = events
        .iter()
        .filter_map(|event| match event {
            TailEvent::Data { bytes, .. } => Some(bytes.as_slice()),
            _ => None,
        })
        .flatten()
        .copied()
        .collect();

    assert_eq!(data, b"final-without-newline");
    assert!(
        events.iter().any(|event| {
            matches!(
                event,
                TailEvent::EndOfFile {
                    source_id: Some(_),
                    ..
                }
            )
        }),
        "caught-up shutdown drain should emit source-scoped terminal EOF"
    );
}

#[test]
fn poll_shutdown_skips_global_eof_for_unknown_file_source_id() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("empty.log");
    fs::write(&log_path, b"").unwrap();

    let config = TailConfig {
        start_from_end: false,
        poll_interval_ms: 0,
        ..Default::default()
    };
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

    let events = tailer.poll_shutdown().unwrap();
    assert!(
        !events.iter().any(|event| matches!(
            event,
            TailEvent::EndOfFile {
                source_id: None,
                ..
            }
        )),
        "file shutdown must not emit global EOF for unknown source id"
    );
}

#[test]
fn poll_shutdown_skips_eof_after_idle_eof_was_emitted() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("shutdown-after-idle-eof.log");
    fs::write(&log_path, b"final-without-newline").unwrap();

    let config = TailConfig {
        start_from_end: false,
        poll_interval_ms: 10,
        ..Default::default()
    };
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

    let _ = poll_until(
        &mut tailer,
        Duration::from_secs(2),
        |events, _| {
            events.iter().any(
                |event| matches!(event, TailEvent::EndOfFile { path, .. } if path == &log_path),
            )
        },
        "timed out waiting for idle EOF",
    );

    let events = tailer.poll_shutdown().unwrap();
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, TailEvent::EndOfFile { path, .. } if path == &log_path)),
        "shutdown must not emit a duplicate EOF after idle EOF already fired"
    );
}

#[test]
fn poll_shutdown_skips_eof_when_read_budget_is_exhausted() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("shutdown-budget.log");
    fs::write(&log_path, b"abcdefghi").unwrap();

    let config = TailConfig {
        start_from_end: false,
        poll_interval_ms: 60_000,
        read_buf_size: 4,
        per_file_read_budget_bytes: 4,
        ..Default::default()
    };
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

    let events = tailer.poll_shutdown().unwrap();
    let data_len: usize = events
        .iter()
        .filter_map(|event| match event {
            TailEvent::Data { bytes, .. } => Some(bytes.len()),
            _ => None,
        })
        .sum();

    assert_eq!(data_len, 4);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, TailEvent::EndOfFile { .. })),
        "shutdown must not emit EOF while unread file bytes remain"
    );
}

#[cfg(unix)]
#[test]
fn poll_shutdown_emits_eof_when_deleted_cleanup_catches_up() {
    if !super::filesystem_tracks_nlink_after_unlink() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("shutdown-deleted.log");
    fs::write(&log_path, b"abcdefgh").unwrap();

    let config = TailConfig {
        start_from_end: false,
        poll_interval_ms: 60_000,
        read_buf_size: 4,
        per_file_read_budget_bytes: 4,
        ..Default::default()
    };
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

    fs::remove_file(&log_path).unwrap();

    let events = tailer.poll_shutdown().unwrap();
    let data_len: usize = events
        .iter()
        .filter_map(|event| match event {
            TailEvent::Data { bytes, .. } => Some(bytes.len()),
            _ => None,
        })
        .sum();

    assert_eq!(data_len, 8);
    assert!(
        events.iter().any(|event| {
            matches!(
                event,
                TailEvent::EndOfFile { path, .. } if path == &log_path
            )
        }),
        "deleted file that catches up during shutdown cleanup should emit EOF"
    );
}

#[cfg(unix)]
#[test]
fn poll_shutdown_keeps_deleted_file_until_cleanup_catches_up() {
    if !super::filesystem_tracks_nlink_after_unlink() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("shutdown-deleted-backlog.log");
    fs::write(&log_path, b"abcdefghijkl").unwrap();

    let config = TailConfig {
        start_from_end: false,
        poll_interval_ms: 60_000,
        read_buf_size: 4,
        per_file_read_budget_bytes: 4,
        ..Default::default()
    };
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

    fs::remove_file(&log_path).unwrap();

    let first = tailer.poll_shutdown().unwrap();
    let first_data_len: usize = first
        .iter()
        .filter_map(|event| match event {
            TailEvent::Data { bytes, .. } => Some(bytes.len()),
            _ => None,
        })
        .sum();
    assert_eq!(first_data_len, 8);
    assert!(
        !first
            .iter()
            .any(|event| matches!(event, TailEvent::EndOfFile { .. })),
        "deleted file should stay tracked while unread bytes remain"
    );

    let second = tailer.poll_shutdown().unwrap();
    let second_data_len: usize = second
        .iter()
        .filter_map(|event| match event {
            TailEvent::Data { bytes, .. } => Some(bytes.len()),
            _ => None,
        })
        .sum();
    assert_eq!(second_data_len, 4);
    assert!(
        second.iter().any(|event| {
            matches!(
                event,
                TailEvent::EndOfFile { path, .. } if path == &log_path
            )
        }),
        "deleted file should emit EOF once shutdown repoll catches up"
    );
}

#[cfg(unix)]
#[test]
fn poll_shutdown_deleted_cleanup_uses_post_drain_source_id_for_eof() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("shutdown-deleted-source.log");
    fs::write(&log_path, b"").unwrap();

    let config = TailConfig {
        start_from_end: false,
        poll_interval_ms: 60_000,
        read_buf_size: 8,
        per_file_read_budget_bytes: 8,
        ..Default::default()
    };
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

    fs::write(&log_path, b"tail").unwrap();
    fs::remove_file(&log_path).unwrap();

    let events = tailer.poll_shutdown().unwrap();
    let data_source_id = events
        .iter()
        .find_map(|event| match event {
            TailEvent::Data { source_id, .. } => *source_id,
            _ => None,
        })
        .expect("deleted file shutdown drain should emit data with a source id");
    let eof_source_id = events
        .iter()
        .find_map(|event| match event {
            TailEvent::EndOfFile { source_id, .. } => *source_id,
            _ => None,
        })
        .expect("deleted file shutdown cleanup should emit EOF with a source id");

    assert_eq!(
        eof_source_id, data_source_id,
        "deleted-file shutdown EOF must target the post-drain data source"
    );
}

#[test]
fn poll_shutdown_skips_eof_for_evicted_truncated_file() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.log");
    let b = dir.path().join("b.log");
    fs::write(&a, b"evicted-before-truncate").unwrap();
    fs::write(&b, b"active-without-newline").unwrap();

    let pattern = format!("{}/*.log", dir.path().display());
    let config = TailConfig {
        start_from_end: false,
        poll_interval_ms: 10,
        glob_rescan_interval_ms: 60_000,
        max_open_files: 1,
        fingerprint_bytes: 4,
        ..Default::default()
    };
    let mut tailer = FileTailer::new_with_globs(&[&pattern], config, create_test_stats()).unwrap();

    let _ = poll_until(
        &mut tailer,
        Duration::from_secs(1),
        |_, tailer| tailer.num_files() == 1,
        "timed out waiting for initial single-open-file eviction",
    );

    let evicted = if tailer.get_offset(&a).is_none() {
        a
    } else if tailer.get_offset(&b).is_none() {
        b
    } else {
        panic!("expected either a.log or b.log to be evicted");
    };

    fs::write(&evicted, b"x").unwrap();
    tailer.discovery.watch_paths.clear();

    let events = tailer.poll_shutdown().unwrap();
    assert!(
        !events.iter().any(|event| {
            matches!(
                event,
                TailEvent::EndOfFile { path, .. } if path == &evicted
            )
        }),
        "evicted truncated file must not flush stale remainder with shutdown EOF"
    );
}

#[test]
fn poll_shutdown_emits_eof_for_evicted_caught_up_file() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.log");
    let b = dir.path().join("b.log");
    fs::write(&a, b"evicted-without-newline").unwrap();
    fs::write(&b, b"active-without-newline").unwrap();

    let pattern = format!("{}/*.log", dir.path().display());
    let config = TailConfig {
        start_from_end: false,
        poll_interval_ms: 10,
        glob_rescan_interval_ms: 60_000,
        max_open_files: 1,
        fingerprint_bytes: 4,
        ..Default::default()
    };
    let mut tailer = FileTailer::new_with_globs(&[&pattern], config, create_test_stats()).unwrap();

    let _ = poll_until(
        &mut tailer,
        Duration::from_secs(1),
        |_, tailer| tailer.num_files() == 1,
        "timed out waiting for initial single-open-file eviction",
    );

    let evicted = if tailer.get_offset(&a).is_none() {
        a
    } else if tailer.get_offset(&b).is_none() {
        b
    } else {
        panic!("expected either a.log or b.log to be evicted");
    };

    tailer.discovery.watch_paths.clear();

    let events = tailer.poll_shutdown().unwrap();
    assert!(
        events.iter().any(|event| {
            matches!(
                event,
                TailEvent::EndOfFile { path, .. } if path == &evicted
            )
        }),
        "evicted caught-up file should still emit shutdown EOF"
    );
}

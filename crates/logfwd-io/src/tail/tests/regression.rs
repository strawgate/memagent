//! Bug fix regression tests, error backoff, adaptive fast polling, read
//! budget enforcement, and miscellaneous tail behavior edge cases.

use std::fs::{self, File};
use std::io::Write;
use std::time::{Duration, Instant};

use logfwd_types::diagnostics::ComponentHealth;

use super::super::*;
use super::{create_test_stats, poll_until};

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
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

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

    // Write more than MAX_READ_PER_POLL (4 MiB) -- use 5 MiB.
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
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

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

/// #1043: reopening an evicted shrunken file must emit truncation before data.
#[test]
fn test_evicted_offset_emits_truncation_when_file_shrinks() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.log");
    let b = dir.path().join("b.log");

    // Both files share the same 4-byte fingerprint prefix so that
    // whichever file gets evicted will still match identity after truncation,
    // exercising the stale-offset truncation path (not the identity-mismatch path).
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
    let mut tailer = FileTailer::new_with_globs(&[&pattern], config, create_test_stats()).unwrap();

    // Initial poll reads both and then evicts one due to max_open_files=1.
    let _ = poll_until(
        &mut tailer,
        Duration::from_secs(1),
        |_, tailer| tailer.num_files() == 1,
        "timed out waiting for initial single-open-file eviction",
    );
    assert_eq!(tailer.num_files(), 1, "one file should be evicted");

    let evicted = if tailer.get_offset(&a).is_none() {
        a
    } else if tailer.get_offset(&b).is_none() {
        b
    } else {
        panic!("expected either a.log or b.log to be evicted");
    };

    // Shrink the evicted file but preserve the first 4 bytes ("ABCD")
    // so identity still matches and stale-offset path is exercised.
    {
        let mut f = File::create(&evicted).unwrap(); // truncate
        writeln!(f, "ABCD_shrunk").unwrap(); // size << previous offset
    }

    // Poll until rescan re-opens the evicted file and emits truncated content.
    let events = poll_until(
        &mut tailer,
        Duration::from_secs(2),
        |events, _| {
            let has_truncation = events.iter().any(|e| {
                matches!(
                    e,
                    TailEvent::Truncated { path, .. } if path == &evicted
                )
            });
            let data: Vec<u8> = events
                .iter()
                .filter_map(|e| match e {
                    TailEvent::Data { path, bytes, .. } if path == &evicted => Some(bytes.clone()),
                    _ => None,
                })
                .flatten()
                .collect();
            has_truncation && String::from_utf8_lossy(&data).contains("ABCD_shrunk")
        },
        "timed out waiting for stale-offset rescan data",
    );

    let trunc_pos = events
        .iter()
        .position(|e| matches!(e, TailEvent::Truncated { path, .. } if path == &evicted))
        .expect("should emit truncation for shrunken evicted file");
    let data_pos = events
        .iter()
        .position(|e| matches!(e, TailEvent::Data { path, .. } if path == &evicted))
        .expect("should emit data after shrunken evicted file truncation");
    assert!(trunc_pos < data_pos, "Truncated must precede Data");

    // If stale offset is incorrectly reset before reopen, truncation is skipped.
    // Preserving it lets read_new_data observe EOF-before-offset, seek to 0, and read.
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

    // Should succeed -- missing files are warned but not fatal.
    let tailer = FileTailer::new(std::slice::from_ref(&missing), config, create_test_stats());
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
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

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
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();
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

#[test]
fn test_directory_path_does_not_fail_construction() {
    let dir = tempfile::tempdir().unwrap();
    let directory_as_path = dir.path().join("logs");
    fs::create_dir_all(&directory_as_path).unwrap();

    let config = TailConfig {
        start_from_end: false,
        poll_interval_ms: 10,
        ..Default::default()
    };
    let tailer = FileTailer::new(
        std::slice::from_ref(&directory_as_path),
        config,
        create_test_stats(),
    );
    assert!(
        tailer.is_ok(),
        "directory entries should warn and continue rather than failing construction"
    );
    assert_eq!(
        tailer.unwrap().num_files(),
        0,
        "directory path must not be tracked as a tailed file"
    );
}

#[test]
fn test_poll_during_active_backoff_still_records_watcher_errors() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("backoff-active.log");
    File::create(&log_path).unwrap();

    let mut tailer = FileTailer::new(
        std::slice::from_ref(&log_path),
        TailConfig {
            start_from_end: false,
            poll_interval_ms: 60_000,
            ..Default::default()
        },
        create_test_stats(),
    )
    .unwrap();

    let (tx, rx) = crossbeam_channel::unbounded();
    tailer.discovery.fs_events = rx;
    tailer.error_backoff_until = Some(Instant::now() + Duration::from_millis(200));
    tx.send(Err(notify::Error::generic("still-backing-off")))
        .unwrap();

    let events = tailer.poll().unwrap();
    assert!(
        events.is_empty(),
        "active backoff should suppress polling work"
    );
    assert_eq!(
        tailer.consecutive_error_polls, 1,
        "watcher errors should still advance backoff state while suppressed"
    );
}

#[test]
fn test_watcher_error_updates_backoff_even_without_poll_tick() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("no-tick-backoff.log");
    File::create(&log_path).unwrap();

    let mut tailer = FileTailer::new(
        std::slice::from_ref(&log_path),
        TailConfig {
            start_from_end: false,
            poll_interval_ms: 60_000,
            ..Default::default()
        },
        create_test_stats(),
    )
    .unwrap();
    let (tx, rx) = crossbeam_channel::unbounded();
    tailer.discovery.fs_events = rx;
    tx.send(Err(notify::Error::generic("watcher-only-error")))
        .unwrap();

    let events = tailer.poll().unwrap();
    assert!(events.is_empty());
    assert_eq!(tailer.consecutive_error_polls, 1);
    assert!(
        tailer.error_backoff_until.is_some(),
        "watcher-only error should schedule backoff even when should_poll=false"
    );
}

/// #544: file I/O and watcher errors must not use eprintln!.
#[test]
fn test_tail_uses_tracing_not_eprintln() {
    let source = [
        include_str!("../mod.rs"),
        include_str!("../identity.rs"),
        include_str!("../glob.rs"),
        include_str!("../discovery.rs"),
        include_str!("../reader.rs"),
        include_str!("../tailer.rs"),
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
    let mut tailer = FileTailer::new(
        &[hot_path.clone(), cold_path.clone()],
        config,
        create_test_stats(),
    )
    .unwrap();

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
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();
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

#[test]
fn test_adaptive_fast_poll_drains_bulk_without_waiting_for_poll_interval() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("bulk.log");
    fs::write(&log_path, vec![b'x'; 320 * 1024]).unwrap();

    let config = TailConfig {
        start_from_end: false,
        poll_interval_ms: 10_000,
        per_file_read_budget_bytes: 64 * 1024,
        adaptive_fast_polls_max: 8,
        ..Default::default()
    };
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();
    tailer.force_poll_due();

    let first = tailer.poll().unwrap();
    let first_bytes: usize = first
        .iter()
        .filter_map(|e| match e {
            TailEvent::Data { bytes, .. } => Some(bytes.len()),
            _ => None,
        })
        .sum();
    assert_eq!(first_bytes, 64 * 1024);
    assert!(
        tailer.adaptive_fast_polls_remaining() > 0,
        "budget saturation should arm adaptive fast polls"
    );

    // Immediate repoll burst (no sleep) should still drain more bytes even
    // though poll_interval_ms is very large.
    let fast_start = Instant::now();
    let mut second_bytes = 0usize;
    while fast_start.elapsed() < Duration::from_millis(250) && second_bytes == 0 {
        let second = tailer.poll().unwrap();
        second_bytes = second
            .iter()
            .filter_map(|e| match e {
                TailEvent::Data { bytes, .. } => Some(bytes.len()),
                _ => None,
            })
            .sum();
    }
    assert_eq!(
        second_bytes,
        64 * 1024,
        "adaptive fast path should bypass poll interval during backlog"
    );
}

#[test]
fn test_adaptive_fast_poll_stays_idle_for_small_live_tail_updates() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("live-tail.log");
    fs::write(&log_path, b"warmup\n").unwrap();

    let config = TailConfig {
        start_from_end: false,
        poll_interval_ms: 5_000,
        per_file_read_budget_bytes: 64 * 1024,
        adaptive_fast_polls_max: 8,
        ..Default::default()
    };
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();
    tailer.force_poll_due();

    let first = tailer.poll().unwrap();
    let first_bytes: usize = first
        .iter()
        .filter_map(|e| match e {
            TailEvent::Data { bytes, .. } => Some(bytes.len()),
            _ => None,
        })
        .sum();
    assert!(first_bytes > 0);
    assert_eq!(
        tailer.adaptive_fast_polls_remaining(),
        0,
        "small reads must not arm adaptive fast polling"
    );

    // Without a filesystem change and without a saturated read, repeated polls
    // before poll_interval should remain idle.
    let second = tailer.poll().unwrap();
    assert!(second.is_empty(), "idle live-tail should not spin");
}

/// Directional benchmark for issue #1258.
///
/// Run with:
/// `cargo test -p logfwd-io tail::tests::regression::bench_adaptive_fast_polling_directional -- --ignored --nocapture`
#[test]
#[ignore = "benchmark (directional)"]
fn bench_adaptive_fast_polling_directional() {
    fn run_once(adaptive_fast_polls_max: u8) -> (u128, usize) {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("bench.log");
        let total_bytes = 32 * 1024 * 1024;
        fs::write(&log_path, vec![b'z'; total_bytes]).unwrap();

        let config = TailConfig {
            start_from_end: false,
            poll_interval_ms: 10,
            per_file_read_budget_bytes: 64 * 1024,
            adaptive_fast_polls_max,
            ..Default::default()
        };
        let mut tailer =
            FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();
        tailer.force_poll_due();
        let (_tx, rx) = crossbeam_channel::unbounded();
        // Replace the watcher channel so no external FS events perturb this directional bench.
        tailer.discovery.fs_events = rx;

        let start = Instant::now();
        let mut total_read = 0usize;
        while start.elapsed() < Duration::from_millis(250) {
            let events = tailer.poll().unwrap();
            for event in events {
                if let TailEvent::Data { bytes, .. } = event {
                    total_read += bytes.len();
                }
            }
        }

        (start.elapsed().as_millis(), total_read)
    }

    let (baseline_ms, baseline_bytes) = run_once(0);
    let (adaptive_ms, adaptive_bytes) = run_once(8);

    println!(
        "baseline_ms={baseline_ms} baseline_bytes={baseline_bytes} adaptive_ms={adaptive_ms} adaptive_bytes={adaptive_bytes}"
    );
}

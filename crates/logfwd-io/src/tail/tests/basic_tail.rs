//! Basic file tailing tests: reading new data, truncation, rotation,
//! start-from-end, file identity, and growing fingerprint.

use std::fs::{self, File};
use std::io::{self, Write};
use std::time::Duration;

use super::super::*;
use super::super::identity::identify_file;
use super::{create_test_stats, poll_until};

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

    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

    // First poll should read existing content.
    let events = poll_until(
        &mut tailer,
        Duration::from_secs(1),
        |events, _| events.iter().any(|e| matches!(e, TailEvent::Data { .. })),
        "timed out waiting for initial tail data",
    );
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

    // Poll again -- should get the new data.
    let events = poll_until(
        &mut tailer,
        Duration::from_secs(1),
        |events, _| {
            let new_data: Vec<u8> = events
                .iter()
                .filter_map(|e| match e {
                    TailEvent::Data { bytes, .. } => Some(bytes.clone()),
                    _ => None,
                })
                .flatten()
                .collect();
            String::from_utf8_lossy(&new_data).contains("line 3")
        },
        "timed out waiting for appended tail data",
    );
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
fn test_zero_read_buf_size_is_normalized_by_public_constructor() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("zero-read-buf.log");
    fs::write(&log_path, b"abcdef").unwrap();

    let config = TailConfig {
        start_from_end: false,
        poll_interval_ms: 10,
        read_buf_size: 0,
        ..Default::default()
    };

    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();
    let events = poll_until(
        &mut tailer,
        Duration::from_secs(1),
        |events, _| {
            events
                .iter()
                .any(|event| matches!(event, TailEvent::Data { .. }))
        },
        "timed out waiting for zero read buffer normalization data",
    );

    let data: Vec<u8> = events
        .iter()
        .filter_map(|event| match event {
            TailEvent::Data { bytes, .. } => Some(bytes.clone()),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(data, b"abcdef");
}

#[test]
fn test_eof_requires_fresh_idle_window_after_data() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("eof-idle-reset.log");
    File::create(&log_path).unwrap();

    let config = TailConfig {
        start_from_end: false,
        poll_interval_ms: 50,
        ..Default::default()
    };
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

    // Reach an initial EOF cycle.
    let _ = poll_until(
        &mut tailer,
        Duration::from_secs(2),
        |events, _| {
            events
                .iter()
                .any(|e| matches!(e, TailEvent::EndOfFile { .. }))
        },
        "timed out waiting for initial EOF",
    );

    // New data must reset EOF idle gating.
    {
        let mut f = fs::OpenOptions::new().append(true).open(&log_path).unwrap();
        write!(f, "partial-without-newline").unwrap();
    }
    let _ = poll_until(
        &mut tailer,
        Duration::from_secs(2),
        |events, _| events.iter().any(|e| matches!(e, TailEvent::Data { .. })),
        "timed out waiting for post-EOF data",
    );

    // Two immediate no-data polls after fresh data should still not emit EOF.
    tailer.force_poll_due();
    let no_data_1 = tailer.poll().unwrap();
    assert!(
        !no_data_1
            .iter()
            .any(|e| matches!(e, TailEvent::EndOfFile { .. })),
        "first no-data poll after fresh data must not emit EOF"
    );

    tailer.force_poll_due();
    let no_data_2 = tailer.poll().unwrap();
    assert!(
        !no_data_2
            .iter()
            .any(|e| matches!(e, TailEvent::EndOfFile { .. })),
        "second no-data poll before idle-duration gate must not emit EOF"
    );

    // EOF should re-emit once a fresh idle window fully elapses.
    let _ = poll_until(
        &mut tailer,
        Duration::from_secs(2),
        |events, _| {
            events
                .iter()
                .any(|e| matches!(e, TailEvent::EndOfFile { .. }))
        },
        "timed out waiting for re-armed EOF",
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
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();
    tailer.force_poll_due();

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

    // Poll -- should detect truncation and read from beginning.
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
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();
    tailer.force_poll_due();

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

    // Poll -- should detect rotation and read new file from beginning.
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
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

    // First poll -- drain initial data.
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
    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

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

    let mut tailer =
        FileTailer::new(std::slice::from_ref(&log_path), config, create_test_stats()).unwrap();

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

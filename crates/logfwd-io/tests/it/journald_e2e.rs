//! End-to-end integration tests for the journald input source.
//!
//! These tests exercise both the subprocess and native backends against the
//! real systemd journal on the host. They write test entries via `logger(1)`,
//! then verify the input reads them back with correct field mapping.
//!
//! All tests are `#[ignore]` because they require:
//! - Linux with systemd/journald running
//! - `logger` and `journalctl` in PATH
//! - libsystemd.so.0 for native backend tests
//!
//! CI runs them in the "Test (network integration)" job via `--run-ignored all`.

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use logfwd_io::input::{InputEvent, InputSource};
use logfwd_io::journal_ffi;
use logfwd_io::journald_input::{
    JournaldBackend, JournaldBackendPref, JournaldConfig, JournaldInput,
};
use logfwd_types::diagnostics::ComponentStats;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns `true` if systemd-journald is running and `logger` is available.
fn journald_available() -> bool {
    Command::new("journalctl")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
        && Command::new("logger")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
}

/// Write `n` test entries to the journal with a unique tag and message prefix.
/// Returns the tag used so callers can filter on it.
fn write_journal_entries(tag: &str, n: usize) -> String {
    for i in 0..n {
        let status = Command::new("logger")
            .arg("-t")
            .arg(tag)
            .arg(format!("test-entry-{i}"))
            .status()
            .expect("failed to run logger");
        assert!(status.success(), "logger exited with {status}");
    }
    tag.to_string()
}

/// Poll `input` until at least `expected` bytes of data arrive or `timeout`
/// elapses. Returns all collected raw bytes.
fn poll_until_bytes(input: &mut dyn InputSource, expected: usize, timeout: Duration) -> Vec<u8> {
    let deadline = std::time::Instant::now() + timeout;
    let mut backoff = Duration::from_millis(10);
    let max_backoff = Duration::from_millis(200);
    let mut all = Vec::new();

    while std::time::Instant::now() < deadline {
        for event in input.poll().unwrap() {
            if let InputEvent::Data { bytes, .. } = event {
                all.extend_from_slice(&bytes);
            }
        }
        if all.len() >= expected {
            // One more drain pass.
            std::thread::sleep(Duration::from_millis(50));
            for event in input.poll().unwrap() {
                if let InputEvent::Data { bytes, .. } = event {
                    all.extend_from_slice(&bytes);
                }
            }
            return all;
        }
        std::thread::sleep(backoff);
        backoff = (backoff * 2).min(max_backoff);
    }
    all
}

/// Poll `input` until we've collected at least `min_lines` newline-delimited
/// JSON entries, or `timeout` elapses. Returns the parsed lines.
fn poll_until_lines(
    input: &mut dyn InputSource,
    min_lines: usize,
    timeout: Duration,
) -> Vec<String> {
    let raw = poll_until_bytes(input, min_lines * 20, timeout);
    let text = String::from_utf8_lossy(&raw);
    text.lines()
        .filter(|l| !l.is_empty())
        .map(ToString::to_string)
        .collect()
}

/// Poll `input` until at least one line containing `needle` is found, or
/// `timeout` elapses. Returns all collected lines.
fn poll_until_match(input: &mut dyn InputSource, needle: &str, timeout: Duration) -> Vec<String> {
    let deadline = std::time::Instant::now() + timeout;
    let mut all_bytes = Vec::new();

    while std::time::Instant::now() < deadline {
        for event in input.poll().unwrap() {
            if let InputEvent::Data { bytes, .. } = event {
                all_bytes.extend_from_slice(&bytes);
            }
        }
        let text = String::from_utf8_lossy(&all_bytes);
        if text.contains(needle) {
            // Drain once more to collect any trailing entries.
            std::thread::sleep(Duration::from_millis(100));
            for event in input.poll().unwrap() {
                if let InputEvent::Data { bytes, .. } = event {
                    all_bytes.extend_from_slice(&bytes);
                }
            }
            let text = String::from_utf8_lossy(&all_bytes);
            return text
                .lines()
                .filter(|l| !l.is_empty())
                .map(ToString::to_string)
                .collect();
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let text = String::from_utf8_lossy(&all_bytes);
    text.lines()
        .filter(|l| !l.is_empty())
        .map(ToString::to_string)
        .collect()
}

/// Create a unique tag for this test to avoid cross-test contamination.
fn unique_tag(test_name: &str) -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("logfwd-test-{test_name}-{ts}")
}

fn make_stats() -> Arc<ComponentStats> {
    Arc::new(ComponentStats::new())
}

// ---------------------------------------------------------------------------
// Subprocess backend tests
// ---------------------------------------------------------------------------

/// Happy path: write entries, read them back via subprocess backend.
#[test]
#[ignore]
fn subprocess_reads_journal_entries() {
    if !journald_available() {
        eprintln!("SKIP: journald not available");
        return;
    }

    let config = JournaldConfig {
        backend: JournaldBackendPref::Subprocess,
        since_now: true,
        current_boot_only: true,
        ..Default::default()
    };

    let mut input = JournaldInput::new("test-sub", config, make_stats())
        .expect("failed to create journald input");

    assert_eq!(input.backend(), JournaldBackend::Subprocess);

    // Write entries AFTER creating the input in since_now mode.
    std::thread::sleep(Duration::from_millis(500));
    let tag = unique_tag("subprocess-basic");
    write_journal_entries(&tag, 5);

    let lines = poll_until_match(&mut input, &tag, Duration::from_secs(10));

    let our_entries: Vec<&String> = lines.iter().filter(|l| l.contains(&tag)).collect();
    assert!(
        !our_entries.is_empty(),
        "expected entries with tag={tag} via subprocess backend"
    );

    // Verify output is valid JSON.
    for line in &our_entries {
        let parsed: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("invalid JSON: {e}\nline: {line}"));
        assert!(parsed.is_object(), "expected JSON object, got {parsed}");
    }
}

/// Verify entries contain expected fields from the journal export format.
#[test]
#[ignore]
fn subprocess_entries_contain_standard_fields() {
    if !journald_available() {
        eprintln!("SKIP: journald not available");
        return;
    }

    let config = JournaldConfig {
        backend: JournaldBackendPref::Subprocess,
        since_now: true,
        current_boot_only: true,
        ..Default::default()
    };

    let mut input = JournaldInput::new("test-fields", config, make_stats())
        .expect("failed to create journald input");

    // Write entries AFTER creating the input (since_now only sees new entries).
    std::thread::sleep(Duration::from_millis(500));
    let tag = unique_tag("subprocess-fields");
    write_journal_entries(&tag, 3);

    let lines = poll_until_match(&mut input, &tag, Duration::from_secs(10));
    assert!(!lines.is_empty(), "expected journal entries");

    // Find our tagged entries (field names are lowercased after normalization).
    let our_entries: Vec<serde_json::Value> = lines
        .iter()
        .filter_map(|l| serde_json::from_str(l).ok())
        .filter(|v: &serde_json::Value| {
            v.get("syslog_identifier")
                .and_then(|s| s.as_str())
                .is_some_and(|s| s == tag)
        })
        .collect();

    assert!(
        !our_entries.is_empty(),
        "expected to find entries with tag={tag} in {} total lines",
        lines.len()
    );

    // Every entry should have these standard journal fields (lowercased).
    for entry in &our_entries {
        assert!(
            entry.get("message").is_some(),
            "entry missing message field: {entry}"
        );
        assert!(
            entry.get("priority").is_some(),
            "entry missing priority field: {entry}"
        );
        assert!(
            entry.get("_pid").is_some(),
            "entry missing _pid field: {entry}"
        );
        assert!(
            entry.get("_hostname").is_some(),
            "entry missing _hostname field: {entry}"
        );
        // Synthesized fields from normalization.
        assert!(
            entry.get("level").is_some(),
            "entry missing synthesized level field: {entry}"
        );
        assert!(
            entry.get("timestamp").is_some(),
            "entry missing synthesized timestamp field: {entry}"
        );
    }

    // Verify our message content.
    let messages: Vec<&str> = our_entries
        .iter()
        .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
        .collect();
    assert!(
        messages.iter().any(|m| m.contains("test-entry-0")),
        "expected test-entry-0 in messages: {messages:?}"
    );
}

/// Since-now mode skips historical entries.
#[test]
#[ignore]
fn subprocess_since_now_skips_history() {
    if !journald_available() {
        eprintln!("SKIP: journald not available");
        return;
    }

    let old_tag = unique_tag("subprocess-old");
    write_journal_entries(&old_tag, 3);
    std::thread::sleep(Duration::from_millis(200));

    let config = JournaldConfig {
        backend: JournaldBackendPref::Subprocess,
        since_now: true,
        current_boot_only: true,
        ..Default::default()
    };

    let mut input = JournaldInput::new("test-since-now", config, make_stats())
        .expect("failed to create journald input");

    // Wait a bit, then write new entries.
    std::thread::sleep(Duration::from_millis(500));
    let new_tag = unique_tag("subprocess-new");
    write_journal_entries(&new_tag, 2);

    let lines = poll_until_match(&mut input, &new_tag, Duration::from_secs(10));

    // We should see the new entries but not the old ones.
    let has_new = lines.iter().any(|l| l.contains(&new_tag));
    let has_old = lines.iter().any(|l| l.contains(&old_tag));

    assert!(has_new, "expected new entries (tag={new_tag}) in output");
    assert!(
        !has_old,
        "since_now should skip old entries (tag={old_tag})"
    );
}

/// Exclude units filter works — entries from excluded systemd units are dropped.
///
/// Note: `logger(1)` entries typically have no `_SYSTEMD_UNIT` field, so we
/// verify the filter doesn't suppress our test entries (positive case) and
/// that the underlying filter mechanism is wired up. The exclude_units logic
/// operates on `_SYSTEMD_UNIT`, which is set by systemd for service-managed
/// processes.
#[test]
#[ignore]
fn subprocess_exclude_units_filters() {
    if !journald_available() {
        eprintln!("SKIP: journald not available");
        return;
    }

    // Exclude a common system unit. Our logger entries don't carry
    // _SYSTEMD_UNIT, so they should always pass through regardless.
    let config = JournaldConfig {
        backend: JournaldBackendPref::Subprocess,
        since_now: true,
        current_boot_only: true,
        exclude_units: vec!["ssh.service".to_string()],
        ..Default::default()
    };

    let mut input = JournaldInput::new("test-exclude", config, make_stats())
        .expect("failed to create journald input");

    // Write entries that should NOT be excluded (logger → no _SYSTEMD_UNIT).
    std::thread::sleep(Duration::from_millis(500));
    let tag = unique_tag("subprocess-excl");
    write_journal_entries(&tag, 3);

    let lines = poll_until_match(&mut input, &tag, Duration::from_secs(10));

    // Our logger entries must pass through the exclude filter.
    let our_entries: Vec<&String> = lines.iter().filter(|l| l.contains(&tag)).collect();
    assert!(
        !our_entries.is_empty(),
        "logger entries should not be excluded (tag={tag})"
    );

    // Verify no ssh.service entries snuck through (if any were generated).
    let excluded_entries: Vec<&String> = lines
        .iter()
        .filter(|l| {
            serde_json::from_str::<serde_json::Value>(l)
                .ok()
                .and_then(|v| {
                    v.get("_systemd_unit")
                        .and_then(|u| u.as_str())
                        .map(String::from)
                })
                .is_some_and(|u| u == "ssh.service")
        })
        .collect();

    assert!(
        excluded_entries.is_empty(),
        "exclude_units should filter ssh.service entries, found {} lines",
        excluded_entries.len()
    );
}

/// Input reports healthy after startup.
#[test]
#[ignore]
fn subprocess_health_becomes_healthy() {
    if !journald_available() {
        eprintln!("SKIP: journald not available");
        return;
    }

    let config = JournaldConfig {
        backend: JournaldBackendPref::Subprocess,
        since_now: true,
        current_boot_only: true,
        ..Default::default()
    };

    let mut input = JournaldInput::new("test-health", config, make_stats())
        .expect("failed to create journald input");

    // Write an entry to trigger the reader thread to emit data.
    let tag = unique_tag("subprocess-health");
    write_journal_entries(&tag, 1);

    // Poll until data arrives — health should transition from Starting to OK.
    let _ = poll_until_lines(&mut input, 1, Duration::from_secs(10));

    let health = input.health();
    assert!(
        matches!(health, logfwd_types::diagnostics::ComponentHealth::Healthy),
        "expected Healthy after receiving data, got {health:?}"
    );
}

/// Drop stops the reader thread cleanly.
#[test]
#[ignore]
fn subprocess_drop_stops_reader() {
    if !journald_available() {
        eprintln!("SKIP: journald not available");
        return;
    }

    let config = JournaldConfig {
        backend: JournaldBackendPref::Subprocess,
        since_now: true,
        current_boot_only: true,
        ..Default::default()
    };

    let input = JournaldInput::new("test-drop", config, make_stats())
        .expect("failed to create journald input");

    // Capture the backend confirmation.
    assert_eq!(input.backend(), JournaldBackend::Subprocess);

    // Drop signals the reader thread to stop (via the running flag / child kill).
    drop(input);

    // If we reach here without hanging, the reader thread exited cleanly.
}

// ---------------------------------------------------------------------------
// Native backend tests
// ---------------------------------------------------------------------------

/// Happy path: read entries via native sd_journal API.
#[test]
#[ignore]
fn native_reads_journal_entries() {
    if !journald_available() {
        eprintln!("SKIP: journald not available");
        return;
    }
    if !journal_ffi::is_native_available() {
        eprintln!("SKIP: libsystemd.so.0 not available");
        return;
    }

    let config = JournaldConfig {
        backend: JournaldBackendPref::Native,
        since_now: true,
        current_boot_only: true,
        ..Default::default()
    };

    let mut input = JournaldInput::new("test-native", config, make_stats())
        .expect("failed to create journald input");

    assert_eq!(input.backend(), JournaldBackend::Native);

    // Write entries AFTER creating the input in since_now mode.
    std::thread::sleep(Duration::from_millis(500));
    let tag = unique_tag("native-basic");
    write_journal_entries(&tag, 5);

    let lines = poll_until_match(&mut input, &tag, Duration::from_secs(10));

    let our_entries: Vec<&String> = lines.iter().filter(|l| l.contains(&tag)).collect();
    assert!(
        !our_entries.is_empty(),
        "expected entries with tag={tag} via native backend"
    );

    // Verify output is valid JSON.
    for line in &our_entries {
        let parsed: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("invalid JSON: {e}\nline: {line}"));
        assert!(parsed.is_object(), "expected JSON object");
    }
}

/// Native backend entries have the same field contract as subprocess.
#[test]
#[ignore]
fn native_entries_contain_standard_fields() {
    if !journald_available() {
        eprintln!("SKIP: journald not available");
        return;
    }
    if !journal_ffi::is_native_available() {
        eprintln!("SKIP: libsystemd.so.0 not available");
        return;
    }

    let config = JournaldConfig {
        backend: JournaldBackendPref::Native,
        since_now: true,
        current_boot_only: true,
        ..Default::default()
    };

    let mut input = JournaldInput::new("test-native-fields", config, make_stats())
        .expect("failed to create journald input");

    // Write entries AFTER creating the input (since_now only sees new entries).
    std::thread::sleep(Duration::from_millis(500));
    let tag = unique_tag("native-fields");
    write_journal_entries(&tag, 3);

    let lines = poll_until_match(&mut input, &tag, Duration::from_secs(10));
    assert!(!lines.is_empty(), "expected journal entries");

    let our_entries: Vec<serde_json::Value> = lines
        .iter()
        .filter_map(|l| serde_json::from_str(l).ok())
        .filter(|v: &serde_json::Value| {
            v.get("syslog_identifier")
                .and_then(|s| s.as_str())
                .is_some_and(|s| s == tag)
        })
        .collect();

    assert!(!our_entries.is_empty(), "expected entries with tag={tag}");

    for entry in &our_entries {
        assert!(entry.get("message").is_some(), "missing message");
        assert!(entry.get("priority").is_some(), "missing priority");
        assert!(entry.get("_pid").is_some(), "missing _pid");
        assert!(entry.get("_hostname").is_some(), "missing _hostname");
        // Synthesized fields from normalization.
        assert!(entry.get("level").is_some(), "missing synthesized level");
        assert!(
            entry.get("timestamp").is_some(),
            "missing synthesized timestamp"
        );
    }
}

/// Native backend since_now mode.
#[test]
#[ignore]
fn native_since_now_skips_history() {
    if !journald_available() {
        eprintln!("SKIP: journald not available");
        return;
    }
    if !journal_ffi::is_native_available() {
        eprintln!("SKIP: libsystemd.so.0 not available");
        return;
    }

    let old_tag = unique_tag("native-old");
    write_journal_entries(&old_tag, 3);
    std::thread::sleep(Duration::from_millis(200));

    let config = JournaldConfig {
        backend: JournaldBackendPref::Native,
        since_now: true,
        current_boot_only: true,
        ..Default::default()
    };

    let mut input = JournaldInput::new("test-native-since", config, make_stats())
        .expect("failed to create journald input");

    std::thread::sleep(Duration::from_millis(500));
    let new_tag = unique_tag("native-new");
    write_journal_entries(&new_tag, 2);

    let lines = poll_until_match(&mut input, &new_tag, Duration::from_secs(10));

    let has_new = lines.iter().any(|l| l.contains(&new_tag));
    let has_old = lines.iter().any(|l| l.contains(&old_tag));

    assert!(has_new, "expected new entries (tag={new_tag}) in output");
    assert!(
        !has_old,
        "since_now should skip old entries (tag={old_tag})"
    );
}

// ---------------------------------------------------------------------------
// Auto-fallback backend test
// ---------------------------------------------------------------------------

/// Auto backend selects native when libsystemd is available.
#[test]
#[ignore]
fn auto_backend_selects_native_when_available() {
    if !journald_available() {
        eprintln!("SKIP: journald not available");
        return;
    }

    let config = JournaldConfig {
        backend: JournaldBackendPref::Auto,
        since_now: true,
        current_boot_only: true,
        ..Default::default()
    };

    let input = JournaldInput::new("test-auto", config, make_stats())
        .expect("failed to create journald input");

    let expected = if journal_ffi::is_native_available() {
        JournaldBackend::Native
    } else {
        JournaldBackend::Subprocess
    };

    assert_eq!(
        input.backend(),
        expected,
        "auto backend should select {expected:?} based on libsystemd availability"
    );
}

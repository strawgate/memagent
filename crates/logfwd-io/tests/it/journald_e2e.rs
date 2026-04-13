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
use logfwd_io::journald_input::{JournaldBackend, JournaldBackendPref, JournaldConfig, JournaldInput};
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
            .is_ok()
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
        .map(|l| l.to_string())
        .collect()
}

/// Poll `input` until at least one line containing `needle` is found, or
/// `timeout` elapses. Returns all collected lines.
fn poll_until_match(
    input: &mut dyn InputSource,
    needle: &str,
    timeout: Duration,
) -> Vec<String> {
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
                .map(|l| l.to_string())
                .collect();
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let text = String::from_utf8_lossy(&all_bytes);
    text.lines()
        .filter(|l| !l.is_empty())
        .map(|l| l.to_string())
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

    let tag = unique_tag("subprocess-basic");
    write_journal_entries(&tag, 5);

    // Small sleep to let journal flush.
    std::thread::sleep(Duration::from_millis(200));

    let config = JournaldConfig {
        backend: JournaldBackendPref::Subprocess,
        current_boot_only: true,
        ..Default::default()
    };

    let mut input = JournaldInput::new("test-sub", config, make_stats())
        .expect("failed to create journald input");

    assert_eq!(input.backend(), JournaldBackend::Subprocess);

    let lines = poll_until_lines(&mut input, 1, Duration::from_secs(10));

    assert!(
        !lines.is_empty(),
        "expected at least one journal entry via subprocess backend"
    );

    // Verify output is valid JSON.
    for line in &lines {
        let parsed: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSON: {e}\nline: {line}"));
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

    // Find our tagged entries.
    let our_entries: Vec<serde_json::Value> = lines
        .iter()
        .filter_map(|l| serde_json::from_str(l).ok())
        .filter(|v: &serde_json::Value| {
            v.get("SYSLOG_IDENTIFIER")
                .and_then(|s| s.as_str())
                .map(|s| s == tag)
                .unwrap_or(false)
        })
        .collect();

    assert!(
        !our_entries.is_empty(),
        "expected to find entries with tag={tag} in {} total lines",
        lines.len()
    );

    // Every entry should have these standard journal fields.
    for entry in &our_entries {
        assert!(
            entry.get("MESSAGE").is_some(),
            "entry missing MESSAGE field: {entry}"
        );
        assert!(
            entry.get("PRIORITY").is_some(),
            "entry missing PRIORITY field: {entry}"
        );
        assert!(
            entry.get("_PID").is_some(),
            "entry missing _PID field: {entry}"
        );
        assert!(
            entry.get("_HOSTNAME").is_some(),
            "entry missing _HOSTNAME field: {entry}"
        );
    }

    // Verify our message content.
    let messages: Vec<&str> = our_entries
        .iter()
        .filter_map(|e| e.get("MESSAGE").and_then(|m| m.as_str()))
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
    assert!(!has_old, "since_now should skip old entries (tag={old_tag})");
}

/// Exclude units filter works.
#[test]
#[ignore]
fn subprocess_exclude_units_filters() {
    if !journald_available() {
        eprintln!("SKIP: journald not available");
        return;
    }

    let config = JournaldConfig {
        backend: JournaldBackendPref::Subprocess,
        since_now: true,
        current_boot_only: true,
        // Exclude sshd — a commonly noisy unit on the test host.
        exclude_units: vec!["sshd".to_string()],
        ..Default::default()
    };

    let mut input = JournaldInput::new("test-exclude", config, make_stats())
        .expect("failed to create journald input");

    // Write an entry that should appear (logger goes to user/syslog, not sshd).
    let tag = unique_tag("subprocess-excl");
    write_journal_entries(&tag, 2);

    let lines = poll_until_lines(&mut input, 1, Duration::from_secs(10));

    // Verify no sshd entries snuck through.
    let sshd_entries: Vec<&String> = lines
        .iter()
        .filter(|l| {
            serde_json::from_str::<serde_json::Value>(l)
                .ok()
                .and_then(|v| v.get("_SYSTEMD_UNIT").and_then(|u| u.as_str()).map(String::from))
                .map(|u| u.contains("sshd"))
                .unwrap_or(false)
        })
        .collect();

    assert!(
        sshd_entries.is_empty(),
        "exclude_units should filter sshd entries, found {} sshd lines",
        sshd_entries.len()
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
        matches!(
            health,
            logfwd_types::diagnostics::ComponentHealth::Healthy
        ),
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

    // Drop should kill the child process and join the reader thread.
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

    let tag = unique_tag("native-basic");
    write_journal_entries(&tag, 5);
    std::thread::sleep(Duration::from_millis(200));

    let config = JournaldConfig {
        backend: JournaldBackendPref::Native,
        current_boot_only: true,
        ..Default::default()
    };

    let mut input = JournaldInput::new("test-native", config, make_stats())
        .expect("failed to create journald input");

    assert_eq!(input.backend(), JournaldBackend::Native);

    let lines = poll_until_lines(&mut input, 1, Duration::from_secs(10));

    assert!(
        !lines.is_empty(),
        "expected at least one journal entry via native backend"
    );

    // Verify output is valid JSON.
    for line in &lines {
        let parsed: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("invalid JSON: {e}\nline: {line}"));
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
            v.get("SYSLOG_IDENTIFIER")
                .and_then(|s| s.as_str())
                .map(|s| s == tag)
                .unwrap_or(false)
        })
        .collect();

    assert!(
        !our_entries.is_empty(),
        "expected entries with tag={tag}"
    );

    for entry in &our_entries {
        assert!(entry.get("MESSAGE").is_some(), "missing MESSAGE");
        assert!(entry.get("PRIORITY").is_some(), "missing PRIORITY");
        assert!(entry.get("_PID").is_some(), "missing _PID");
        assert!(entry.get("_HOSTNAME").is_some(), "missing _HOSTNAME");
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
    assert!(!has_old, "since_now should skip old entries (tag={old_tag})");
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

/// Debug: exhaustive test of recovery strategies
#[test]
#[ignore]
fn debug_native_recovery_strategies() {
    if !journal_ffi::is_native_available() {
        eprintln!("SKIP");
        return;
    }
    
    let flags = journal_ffi::SD_JOURNAL_LOCAL_ONLY | journal_ffi::SD_JOURNAL_SYSTEM;
    let mut j = journal_ffi::Journal::open(flags).expect("open");
    
    // Apply boot filter like production code does
    let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .unwrap().trim().replace('-', "");
    j.add_match(format!("_BOOT_ID={boot_id}").as_bytes()).unwrap();
    
    j.seek_tail().unwrap();
    let prev = j.previous().unwrap();
    eprintln!("previous: {prev}");
    
    let cursor = if prev { Some(j.cursor().unwrap()) } else { None };
    eprintln!("cursor: {cursor:?}");
    
    // Drain any entries
    loop { if !j.next().unwrap() { break; } }
    
    // Write entries
    let tag = unique_tag("recovery");
    write_journal_entries(&tag, 3);
    eprintln!("wrote 3 entries with tag: {tag}");
    
    // Try multiple wait + next cycles
    for attempt in 0..10 {
        let w = j.wait(500_000).unwrap_or(-1); // 500ms
        eprintln!("attempt {attempt}: wait={w}");
        
        let mut found = 0;
        loop {
            match j.next() {
                Ok(true) => {
                    found += 1;
                    // Check if it's our entry
                    j.restart_data();
                    let mut is_ours = false;
                    while let Ok(Some(field)) = j.enumerate_data() {
                        let s = String::from_utf8_lossy(field);
                        if s.contains(&tag) {
                            is_ours = true;
                            eprintln!("  found our entry: {}", &s[..s.len().min(80)]);
                        }
                    }
                    if is_ours {
                        eprintln!("SUCCESS on attempt {attempt}!");
                        return;
                    }
                }
                Ok(false) => break,
                Err(e) => { eprintln!("  next error: {e}"); break; }
            }
        }
        eprintln!("  found {found} entries, none ours");
        
        // Try seek_cursor recovery on attempt 5
        if attempt == 5 {
            if let Some(ref c) = cursor {
                eprintln!("  trying seek_cursor recovery");
                j.seek_cursor(c).unwrap();
            }
        }
    }
    eprintln!("FAILED: never found our entries");
    panic!("native recovery failed");
}

/// Debug: minimal native since_now test without any filters
#[test]
#[ignore]
fn debug_native_no_filter() {
    if !journal_ffi::is_native_available() {
        eprintln!("SKIP");
        return;
    }
    
    let flags = journal_ffi::SD_JOURNAL_LOCAL_ONLY | journal_ffi::SD_JOURNAL_SYSTEM;
    let mut j = journal_ffi::Journal::open(flags).expect("open");
    
    // NO match filters - just open + seek_tail + previous
    j.seek_tail().unwrap();
    let _ = j.previous().unwrap();
    
    // Drain
    loop { if !j.next().unwrap() { break; } }
    
    let tag = unique_tag("nofilter");
    write_journal_entries(&tag, 2);
    eprintln!("wrote: {tag}");
    
    for i in 0..10 {
        let w = j.wait(500_000).unwrap_or(-1);
        let mut found = false;
        loop {
            match j.next() {
                Ok(true) => {
                    j.restart_data();
                    while let Ok(Some(f)) = j.enumerate_data() {
                        let s = String::from_utf8_lossy(f);
                        if s.contains(&tag) {
                            eprintln!("FOUND on iter {i}: {}", &s[..s.len().min(100)]);
                            found = true;
                        }
                    }
                }
                Ok(false) => break,
                Err(_) => break,
            }
        }
        if found { return; }
        eprintln!("iter {i}: wait={w}, not found yet");
    }
    panic!("never found entries");
}

/// Debug: seek_head + drain all + then follow new entries
#[test]
#[ignore]
fn debug_native_drain_then_follow() {
    if !journal_ffi::is_native_available() {
        eprintln!("SKIP");
        return;
    }
    
    let flags = journal_ffi::SD_JOURNAL_LOCAL_ONLY | journal_ffi::SD_JOURNAL_SYSTEM;
    let mut j = journal_ffi::Journal::open(flags).expect("open");
    
    // Seek to head and drain ALL existing entries
    j.seek_head().unwrap();
    let mut count = 0u64;
    loop {
        match j.next() {
            Ok(true) => count += 1,
            _ => break,
        }
    }
    eprintln!("drained {count} existing entries");
    
    // Now write new entries
    let tag = unique_tag("drain-follow");
    write_journal_entries(&tag, 2);
    eprintln!("wrote: {tag}");
    
    // Follow
    for i in 0..10 {
        let w = j.wait(500_000).unwrap_or(-1);
        let mut found = false;
        loop {
            match j.next() {
                Ok(true) => {
                    j.restart_data();
                    while let Ok(Some(f)) = j.enumerate_data() {
                        let s = String::from_utf8_lossy(f);
                        if s.contains(&tag) {
                            eprintln!("FOUND on iter {i}: {}", &s[..s.len().min(100)]);
                            found = true;
                        }
                    }
                }
                Ok(false) => break,
                Err(_) => break,
            }
        }
        if found { return; }
        eprintln!("iter {i}: wait={w}, not found");
    }
    panic!("never found");
}

/// Debug: fresh journal handle opened AFTER entries are written
#[test]
#[ignore]
fn debug_native_fresh_handle() {
    if !journal_ffi::is_native_available() {
        eprintln!("SKIP");
        return;
    }
    
    // Write entries first
    let tag = unique_tag("fresh");
    write_journal_entries(&tag, 2);
    std::thread::sleep(Duration::from_millis(500));
    
    // Open a fresh handle
    let flags = journal_ffi::SD_JOURNAL_LOCAL_ONLY | journal_ffi::SD_JOURNAL_SYSTEM;
    let mut j = journal_ffi::Journal::open(flags).expect("open");
    
    j.seek_tail().unwrap();
    
    // Go back 10 entries from tail
    for _ in 0..10 {
        if !j.previous().unwrap() { break; }
    }
    
    // Now try to find our entries
    let mut found = false;
    for _ in 0..100 {
        match j.next() {
            Ok(true) => {
                j.restart_data();
                while let Ok(Some(f)) = j.enumerate_data() {
                    let s = String::from_utf8_lossy(f);
                    if s.contains(&tag) {
                        eprintln!("FOUND: {}", &s[..s.len().min(100)]);
                        found = true;
                    }
                }
            }
            _ => break,
        }
    }
    assert!(found, "entries should be visible in a fresh handle");
    
    // Now write MORE entries and see if wait+next works
    let tag2 = unique_tag("fresh2");
    write_journal_entries(&tag2, 2);
    eprintln!("wrote tag2: {tag2}");
    
    for i in 0..10 {
        let w = j.wait(500_000).unwrap_or(-1);
        let mut found2 = false;
        loop {
            match j.next() {
                Ok(true) => {
                    j.restart_data();
                    while let Ok(Some(f)) = j.enumerate_data() {
                        let s = String::from_utf8_lossy(f);
                        if s.contains(&tag2) {
                            eprintln!("FOUND tag2 on iter {i}: {}", &s[..s.len().min(100)]);
                            found2 = true;
                        }
                    }
                }
                _ => break,
            }
        }
        if found2 { return; }
        eprintln!("iter {i}: wait={w}, tag2 not found");
    }
    panic!("tag2 never found in follow mode");
}

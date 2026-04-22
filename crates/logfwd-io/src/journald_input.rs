//! Journald (systemd journal) input source.
//!
//! Reads the systemd journal using either:
//! - **Native backend**: the `sd_journal` C API loaded at runtime via `dlopen`
//!   (~10× lower per-entry latency, no subprocess overhead).
//! - **Subprocess backend**: `journalctl --follow --output=export`, parsing the
//!   binary-safe export format and converting to JSON.
//!
//! Both backends produce newline-delimited JSON bytes so the downstream pipeline
//! processes them identically. The `backend` config controls selection:
//! `auto` (default) tries native first then falls back to subprocess.

use std::io::{self, BufRead, BufReader, Read};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;

use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, Ordering};
use std::time::Duration;

use crossbeam_channel::{Receiver, TrySendError, bounded};
use logfwd_types::diagnostics::{ComponentHealth, ComponentStats};

use crate::input::{InputEvent, InputSource};
use crate::journal_ffi::{self, SD_JOURNAL_APPEND, SD_JOURNAL_INVALIDATE};

/// Channel capacity between the reader thread and `poll()`.
const CHANNEL_CAPACITY: usize = 4096;

/// Maximum lines drained per `poll()` call to prevent one busy input from
/// starving other pipeline inputs.
const MAX_LINES_PER_POLL: usize = 512;

/// Maximum total bytes emitted per `poll()` call.
const MAX_BYTES_PER_POLL: usize = 2 * 1024 * 1024;

/// Backoff before restarting a crashed `journalctl` process.
const RESTART_BACKOFF: Duration = Duration::from_secs(1);

/// Wait timeout for `sd_journal_wait` in microseconds (250ms).
const NATIVE_WAIT_USEC: u64 = 250_000;

/// Maximum allowed binary field length in the export format (16 MB).
/// Fields larger than this are skipped to prevent malicious/corrupt journals
/// from causing huge allocations.
const MAX_BINARY_FIELD_SIZE: usize = 16 * 1024 * 1024;

/// Health encoding used in the atomic health byte.
const HEALTH_OK: u8 = 0;
const HEALTH_STARTING: u8 = 1;
const HEALTH_DEGRADED: u8 = 2;

/// Which journal-reading backend to use (IO-layer enum, decoupled from config).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JournaldBackendPref {
    /// Use native API if available, otherwise subprocess (default).
    #[default]
    Auto,
    /// Require the native `sd_journal` C API via `dlopen`.
    Native,
    /// Always use a `journalctl` subprocess.
    Subprocess,
}

/// Runtime configuration for the journald input, derived from config types.
#[derive(Debug, Clone)]
pub struct JournaldConfig {
    /// Systemd units to include (empty = all).
    pub include_units: Vec<String>,
    /// Systemd units to exclude.
    pub exclude_units: Vec<String>,
    /// Only read entries from the current boot.
    pub current_boot_only: bool,
    /// Start reading from "now" (skip history).
    pub since_now: bool,
    /// Path to `journalctl` binary (subprocess fallback only).
    pub journalctl_path: String,
    /// Custom journal directory.
    pub journal_directory: Option<String>,
    /// Journal namespace.
    pub journal_namespace: Option<String>,
    /// Which backend to use (auto/native/subprocess).
    pub backend: JournaldBackendPref,
}

impl Default for JournaldConfig {
    fn default() -> Self {
        Self {
            include_units: Vec::new(),
            exclude_units: Vec::new(),
            current_boot_only: true,
            since_now: false,
            journalctl_path: "journalctl".to_string(),
            journal_directory: None,
            journal_namespace: None,
            backend: JournaldBackendPref::Auto,
        }
    }
}

/// Which backend the reader thread is using.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournaldBackend {
    /// Native `sd_journal` API via `dlopen`.
    Native,
    /// `journalctl` subprocess fallback.
    Subprocess,
}

const BACKEND_NATIVE: u8 = 0;
const BACKEND_SUBPROCESS: u8 = 1;

/// Journald input that tails the systemd journal.
///
/// Prefers the native `sd_journal` API. Falls back to a `journalctl` subprocess
/// if `libsystemd.so.0` is not available.
pub struct JournaldInput {
    name: String,
    rx: Receiver<Vec<u8>>,
    is_running: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
    #[allow(dead_code)] // Retained for future per-input stats; accounted_bytes handles counting.
    stats: Arc<ComponentStats>,
    /// Atomic so the reader thread can update it on auto-fallback.
    backend: Arc<AtomicU8>,
    /// PID of the subprocess child (0 = no child / native backend).
    /// Used by `Drop` to kill the child so `read_until` unblocks.
    child_pid: Arc<AtomicU32>,
}

impl JournaldInput {
    /// Create a new journald input. Spawns a background thread that reads the
    /// journal and sends JSON entries over a bounded channel.
    pub fn new(
        name: impl Into<String>,
        config: JournaldConfig,
        stats: Arc<ComponentStats>,
    ) -> io::Result<Self> {
        let name = name.into();
        let (tx, rx) = bounded(CHANNEL_CAPACITY);
        let is_running = Arc::new(AtomicBool::new(true));
        let health = Arc::new(AtomicU8::new(HEALTH_STARTING));
        let child_pid = Arc::new(AtomicU32::new(0));

        let thread_running = Arc::clone(&is_running);
        let thread_health = Arc::clone(&health);
        let thread_child_pid = Arc::clone(&child_pid);
        let thread_name = name.clone();

        // Resolve backend based on config + runtime availability.
        let native_available = journal_ffi::is_native_available();
        let use_native = match config.backend {
            JournaldBackendPref::Auto => native_available,
            JournaldBackendPref::Native => {
                if !native_available {
                    return Err(io::Error::other(
                        "backend: native requested but libsystemd.so.0 is not available",
                    ));
                }
                true
            }
            JournaldBackendPref::Subprocess => false,
        };

        let backend = Arc::new(AtomicU8::new(if use_native {
            BACKEND_NATIVE
        } else {
            BACKEND_SUBPROCESS
        }));
        let thread_backend = Arc::clone(&backend);

        if use_native {
            tracing::info!("journald input '{thread_name}': using native sd_journal API");
        } else {
            let reason = if matches!(config.backend, JournaldBackendPref::Subprocess) {
                "subprocess backend selected by config"
            } else {
                "libsystemd.so.0 not available, falling back to journalctl subprocess"
            };
            tracing::info!("journald input '{thread_name}': {reason}");
        }

        std::thread::Builder::new()
            .name(format!("journald-{thread_name}"))
            .spawn(move || {
                if use_native {
                    // When backend is auto, fall back to subprocess if native
                    // startup fails (e.g. permission denied, missing namespace).
                    native_reader_loop(&config, &tx, &thread_running, &thread_health);

                    // If health is degraded and backend was auto, try subprocess.
                    if matches!(config.backend, JournaldBackendPref::Auto)
                        && thread_health.load(Ordering::Acquire) == HEALTH_DEGRADED
                        && thread_running.load(Ordering::Acquire)
                    {
                        tracing::warn!("native journal backend failed, falling back to subprocess");
                        thread_backend.store(BACKEND_SUBPROCESS, Ordering::Release);
                        thread_health.store(HEALTH_STARTING, Ordering::Release);
                        subprocess_reader_loop(
                            config,
                            tx,
                            thread_running,
                            thread_health,
                            thread_child_pid,
                        );
                    }
                } else {
                    subprocess_reader_loop(
                        config,
                        tx,
                        thread_running,
                        thread_health,
                        thread_child_pid,
                    );
                }
            })
            .map_err(io::Error::other)?;

        Ok(Self {
            name,
            rx,
            is_running,
            health,
            stats,
            backend,
            child_pid,
        })
    }

    /// Which backend this input is using.
    pub fn backend(&self) -> JournaldBackend {
        match self.backend.load(Ordering::Acquire) {
            BACKEND_NATIVE => JournaldBackend::Native,
            _ => JournaldBackend::Subprocess,
        }
    }
}

impl Drop for JournaldInput {
    fn drop(&mut self) {
        self.is_running.store(false, Ordering::Release);
        // Atomically take the child PID (if any) so only one thread calls kill.
        let pid = self.child_pid.swap(0, Ordering::AcqRel);
        if pid != 0 {
            // SAFETY: sending SIGKILL to a child PID we exclusively own via the
            // swap above. The reader thread also uses swap(0) before child.wait(),
            // so only one side ever sends the signal.
            unsafe {
                #[cfg(unix)]
                libc::kill(pid as i32, libc::SIGKILL);
            }
        }
    }
}

impl InputSource for JournaldInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let mut events = Vec::new();
        let mut total_bytes: usize = 0;
        let mut lines_read: usize = 0;

        loop {
            match self.rx.try_recv() {
                Ok(line) => {
                    let len = line.len();
                    total_bytes += len;
                    lines_read += 1;
                    // Note: do NOT call stats.inc_bytes() here — the downstream
                    // FramedInput already charges `accounted_bytes` to stats.
                    events.push(InputEvent::Data {
                        bytes: Bytes::from(line),
                        source_id: None,
                        accounted_bytes: len as u64,
                        cri_metadata: None,
                    });
                    if lines_read >= MAX_LINES_PER_POLL || total_bytes >= MAX_BYTES_PER_POLL {
                        break;
                    }
                }
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => break,
            }
        }

        Ok(events)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        match self.health.load(Ordering::Acquire) {
            HEALTH_OK => ComponentHealth::Healthy,
            HEALTH_STARTING => ComponentHealth::Starting,
            _ => ComponentHealth::Degraded,
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Native sd_journal backend
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Read the journal using the native `sd_journal` C API.
///
/// Opens the journal, applies match filters, seeks to the desired position,
/// then enters a loop: drain all available entries → wait for new entries.
/// Each entry is serialized to a JSON object and sent over the channel.
fn native_reader_loop(
    config: &JournaldConfig,
    tx: &crossbeam_channel::Sender<Vec<u8>>,
    running: &Arc<AtomicBool>,
    health: &Arc<AtomicU8>,
) {
    health.store(HEALTH_STARTING, Ordering::Release);

    let mut journal = match open_native_journal(config) {
        Ok(j) => j,
        Err(e) => {
            tracing::error!(error = %e, "failed to open native journal");
            health.store(HEALTH_DEGRADED, Ordering::Release);
            return;
        }
    };

    // Apply include_units as match filters (server-side filtering).
    if let Err(e) = apply_unit_matches(&mut journal, &config.include_units) {
        tracing::error!(error = %e, "failed to apply journal match filters");
        health.store(HEALTH_DEGRADED, Ordering::Release);
        return;
    }

    // Seek to starting position.
    if let Err(e) = seek_start(&mut journal, config) {
        tracing::error!(error = %e, "failed to seek journal");
        health.store(HEALTH_DEGRADED, Ordering::Release);
        return;
    }

    health.store(HEALTH_OK, Ordering::Release);
    tracing::info!("native journald reader started");

    // Pre-normalize exclude units for fast comparison.
    let exclude_units: Vec<String> = config.exclude_units.iter().map(|u| fixup_unit(u)).collect();

    // Reusable buffer for JSON serialization.
    let mut json_buf = Vec::with_capacity(4096);

    // Track the last-read cursor so we can recover after INVALIDATE.
    // Starts as None — only set after actually emitting an entry.
    let mut last_cursor: Option<String> = None;

    loop {
        if !running.load(Ordering::Acquire) {
            return;
        }

        // Drain all available entries.
        loop {
            if !running.load(Ordering::Acquire) {
                return;
            }

            match journal.next() {
                Ok(true) => {
                    // Build JSON from entry fields.
                    match entry_to_json(&mut journal, &exclude_units, &mut json_buf) {
                        Ok(Some(())) => {
                            // Save cursor of this entry before sending. We do
                            // this per-entry (not at drain-loop exit) because
                            // cursor() is only reliable while positioned on a
                            // valid entry (next()→true).
                            if let Ok(c) = journal.cursor() {
                                last_cursor = Some(c);
                            }

                            json_buf.push(b'\n');
                            // Move the buffer to avoid cloning; allocate a
                            // fresh one for the next entry.
                            let payload =
                                std::mem::replace(&mut json_buf, Vec::with_capacity(4096));

                            match tx.try_send(payload) {
                                Ok(()) => {}
                                Err(TrySendError::Full(payload)) => {
                                    if tx.send(payload).is_err() {
                                        return; // channel closed
                                    }
                                }
                                Err(TrySendError::Disconnected(_)) => return,
                            }
                        }
                        Ok(None) => {
                            // Entry excluded; skip.
                            json_buf.clear();
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "failed to read journal entry");
                            json_buf.clear();
                        }
                    }
                }
                Ok(false) => break, // No more entries; wait.
                Err(e) => {
                    tracing::warn!(error = %e, "sd_journal_next error");
                    break;
                }
            }
        }

        // Wait for new entries (with periodic timeout to check running flag).
        let wait_result = journal.wait(NATIVE_WAIT_USEC);
        update_native_wait_health(&wait_result, health);
        match wait_result {
            Ok(SD_JOURNAL_INVALIDATE) => {
                // Journal files were rotated/added/removed. The internal file
                // handle state may be stale, causing next() to return false
                // even when new entries exist. Re-seek to restore position.
                if let Some(ref cursor) = last_cursor {
                    match journal.seek_cursor(cursor) {
                        Ok(()) => {
                            // seek_cursor sets up position but does NOT land
                            // on an entry — must call next() first, which
                            // returns the cursor entry (if it still exists)
                            // or the next closest entry.
                            match journal.next() {
                                Ok(true) => {
                                    if journal.test_cursor(cursor).unwrap_or(false) {
                                        // On the cursor entry (already
                                        // emitted). The drain loop's next()
                                        // will advance to new entries.
                                    } else {
                                        // Cursor was rotated out; we landed
                                        // on the first unread entry. Back up
                                        // so the drain loop's next() returns
                                        // it instead of skipping it.
                                        let _ = journal.previous();
                                    }
                                }
                                Ok(false) => {
                                    // No entries at or after cursor.
                                }
                                Err(e) => {
                                    tracing::warn!(error = %e, "sd_journal_next error during invalidate recovery");
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "failed to re-seek after journal invalidate");
                        }
                    }
                } else {
                    // No entries read yet — restore initial seek position.
                    if let Err(e) = seek_start(&mut journal, config) {
                        tracing::warn!(error = %e, "failed to re-seek after journal invalidate");
                    }
                }
            }
            Ok(_) => {} // NOP or APPEND — next loop iteration will drain.
            Err(e) => {
                tracing::warn!(error = %e, "sd_journal_wait error");
                // Brief sleep to avoid spinning on persistent errors.
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

fn update_native_wait_health(wait_result: &io::Result<i32>, health: &Arc<AtomicU8>) {
    match wait_result {
        Ok(SD_JOURNAL_APPEND) | Ok(SD_JOURNAL_INVALIDATE) => {
            health.store(HEALTH_OK, Ordering::Release);
        }
        Ok(_) => {}
        Err(_) => {
            health.store(HEALTH_DEGRADED, Ordering::Release);
        }
    }
}

/// Open a journal handle with the appropriate flags/directory/namespace.
fn open_native_journal(config: &JournaldConfig) -> io::Result<journal_ffi::Journal> {
    // SD_JOURNAL_LOCAL_ONLY reads both system and user journals from the
    // local machine, matching the default behavior of `journalctl --follow`.
    let flags = journal_ffi::SD_JOURNAL_LOCAL_ONLY;

    let mut journal = if let Some(ref dir) = config.journal_directory {
        // sd_journal_open_directory only accepts 0 or SD_JOURNAL_OS_ROOT.
        journal_ffi::Journal::open_directory(dir, 0)?
    } else if let Some(ref ns) = config.journal_namespace {
        journal_ffi::Journal::open_namespace(ns, flags)?
    } else {
        journal_ffi::Journal::open(flags)?
    };

    // Apply _BOOT_ID filter for current_boot_only.
    if config.current_boot_only {
        let boot_id = read_current_boot_id()?;
        let match_str = format!("_BOOT_ID={boot_id}");
        journal.add_match(match_str.as_bytes())?;
    }

    Ok(journal)
}

/// Read the current boot ID from `/proc/sys/kernel/random/boot_id`.
fn read_current_boot_id() -> io::Result<String> {
    let raw = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    // The kernel returns a UUID with hyphens; systemd stores it without hyphens.
    Ok(raw.trim().replace('-', ""))
}

/// Add `_SYSTEMD_UNIT=<unit>` match expressions for each include unit.
/// Multiple matches on the same field are OR'd by sd_journal.
fn apply_unit_matches(
    journal: &mut journal_ffi::Journal,
    include_units: &[String],
) -> io::Result<()> {
    for unit in include_units {
        let match_str = format!("_SYSTEMD_UNIT={}", fixup_unit(unit));
        journal.add_match(match_str.as_bytes())?;
    }
    Ok(())
}

/// Seek to the starting position based on config.
fn seek_start(journal: &mut journal_ffi::Journal, config: &JournaldConfig) -> io::Result<()> {
    if config.since_now {
        // Seek to tail, then the read loop will pick up only new entries.
        journal.seek_tail()?;
        // sd_journal_seek_tail positions *after* the last entry.
        // sd_journal_previous would land on the last entry, but we want to
        // start from *after* it, so next() will get the first new entry.
        journal.previous()?;
    } else {
        journal.seek_head()?;
    }
    Ok(())
}

/// Serialize the current journal entry to JSON, applying exclude filters.
///
/// Writes the JSON bytes into `buf`. Returns `Ok(Some(()))` if the entry was
/// serialized, `Ok(None)` if it was excluded.
fn entry_to_json(
    journal: &mut journal_ffi::Journal,
    exclude_units: &[String],
    buf: &mut Vec<u8>,
) -> io::Result<Option<()>> {
    // First pass: check exclude filter before full serialization.
    if !exclude_units.is_empty()
        && let Ok(Some(field_data)) = journal.get_data("_SYSTEMD_UNIT")
    {
        // field_data is "FIELD=value" bytes.
        if let Some(eq_pos) = memchr::memchr(b'=', field_data) {
            let value = &field_data[eq_pos + 1..];
            let unit_str = String::from_utf8_lossy(value);
            let normalized = fixup_unit(&unit_str);
            if exclude_units.contains(&normalized) {
                return Ok(None);
            }
        }
    }

    // Collect all fields, then use the shared serializer.
    let mut fields: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(32);
    journal.restart_data();
    while let Some(field_bytes) = journal.enumerate_data()? {
        // Skip oversized fields (same bound as subprocess parser).
        if field_bytes.len() > MAX_BINARY_FIELD_SIZE {
            continue;
        }
        let Some(eq_pos) = memchr::memchr(b'=', field_bytes) else {
            continue;
        };
        fields.push((
            field_bytes[..eq_pos].to_vec(),
            field_bytes[eq_pos + 1..].to_vec(),
        ));
    }

    normalize_fields(&mut fields);
    write_fields_as_json(buf, &fields);
    Ok(Some(()))
}

// ── Field normalization ───────────────────────────────────────────────
// Raw journald fields use UPPERCASE names (e.g. `MESSAGE`, `_PID`,
// `PRIORITY`). We normalize them so that:
//
//  1. All field names are lowercased (SQL-friendly, no quoting needed).
//  2. `message` (née MESSAGE) is a recognized OTLP body variant.
//  3. `_source_realtime_timestamp` is converted to `timestamp` (RFC 3339)
//     so the OTLP encoder picks it up as the log record timestamp.
//  4. `priority` (syslog int 0-7) is mapped to a `level` field with
//     standard severity text (FATAL/ERROR/WARN/INFO/DEBUG) so the OTLP
//     encoder can derive severity_number + severity_text.
//  5. Double-underscore cursor/internal fields (`__CURSOR`, etc.) are
//     dropped — they are internal to journalctl export format.

/// Normalize journald fields in-place: lowercase names, synthesize
/// `timestamp` from `_SOURCE_REALTIME_TIMESTAMP`, synthesize `level`
/// from `PRIORITY`, and drop `__`-prefixed internal fields.
fn normalize_fields(fields: &mut Vec<(Vec<u8>, Vec<u8>)>) {
    let mut priority_value: Option<u8> = None;
    let mut source_ts_usec: Option<i64> = None;
    let mut has_timestamp = false;
    let mut has_level = false;

    // First pass: lowercase names, capture PRIORITY and _SOURCE_REALTIME_TIMESTAMP,
    // drop __-prefixed fields, detect pre-existing timestamp/level.
    fields.retain_mut(|(name, value)| {
        // Drop double-underscore internal fields (__CURSOR, __REALTIME_TIMESTAMP, etc).
        if name.starts_with(b"__") {
            return false;
        }

        // Capture raw PRIORITY value before lowercasing the name.
        if name == b"PRIORITY" && value.len() == 1 && value[0].is_ascii_digit() {
            priority_value = Some(value[0] - b'0');
        }

        // Parse _SOURCE_REALTIME_TIMESTAMP to µs before lowercasing (avoids clone).
        if name == b"_SOURCE_REALTIME_TIMESTAMP"
            && let Ok(s) = std::str::from_utf8(value)
        {
            source_ts_usec = s.parse::<i64>().ok();
        }

        // Lowercase the field name in-place.
        name.make_ascii_lowercase();

        // Detect pre-existing timestamp/level (after lowercasing).
        if name == b"timestamp" {
            has_timestamp = true;
        }
        if name == b"level" {
            has_level = true;
        }

        true
    });

    // Synthesize `timestamp` from `_source_realtime_timestamp` (µs epoch → RFC 3339).
    if !has_timestamp
        && let Some(us) = source_ts_usec
        && let Some(rfc3339) = usec_to_rfc3339(us)
    {
        fields.push((b"timestamp".to_vec(), rfc3339.into_bytes()));
    }

    // Synthesize `level` from PRIORITY (syslog 0-7 → OTLP severity text).
    if !has_level
        && let Some(prio) = priority_value
        && let Some(level_text) = syslog_priority_to_level(prio)
    {
        fields.push((b"level".to_vec(), level_text.as_bytes().to_vec()));
    }
}

/// Map syslog priority (0-7) to the OTLP severity text that
/// `logfwd_core::otlp::parse_severity` recognizes.
///
/// Syslog priorities:
///   0 = emerg, 1 = alert, 2 = crit  →  FATAL
///   3 = err                          →  ERROR
///   4 = warning                      →  WARN
///   5 = notice                       →  INFO  (no OTLP "notice" level)
///   6 = info                         →  INFO
///   7 = debug                        →  DEBUG
fn syslog_priority_to_level(priority: u8) -> Option<&'static str> {
    match priority {
        0..=2 => Some("FATAL"),
        3 => Some("ERROR"),
        4 => Some("WARN"),
        5 | 6 => Some("INFO"),
        7 => Some("DEBUG"),
        _ => None,
    }
}

/// Serialize a list of `(name, value)` field pairs as a JSON object into `buf`.
///
/// This is the **single** JSON serializer used by both the native and subprocess
/// backends, guaranteeing identical output for the same field set.
fn write_fields_as_json(buf: &mut Vec<u8>, fields: &[(Vec<u8>, Vec<u8>)]) {
    buf.push(b'{');

    let mut first = true;
    for (name, value) in fields {
        if !first {
            buf.push(b',');
        }
        first = false;

        buf.push(b'"');
        // Field names are ASCII identifiers — no escaping needed in practice,
        // but we escape defensively.
        json_escape_into(buf, name);
        buf.extend_from_slice(b"\":\"");
        json_escape_into(buf, value);
        buf.push(b'"');
    }

    buf.push(b'}');
}

/// Escape bytes for inclusion in a JSON string value.
///
/// Valid UTF-8 sequences are written directly (with standard JSON escaping for
/// control characters, `"`, and `\`). Invalid bytes are replaced with the
/// Unicode replacement character (`\uFFFD`).
fn json_escape_into(buf: &mut Vec<u8>, input: &[u8]) {
    let mut remaining = input;
    while !remaining.is_empty() {
        match std::str::from_utf8(remaining) {
            Ok(valid) => {
                // All remaining bytes are valid UTF-8.
                escape_str_into(buf, valid);
                break;
            }
            Err(e) => {
                // Write the valid prefix.
                let (valid_bytes, after) = remaining.split_at(e.valid_up_to());
                if !valid_bytes.is_empty()
                    && let Ok(valid_str) = std::str::from_utf8(valid_bytes)
                {
                    escape_str_into(buf, valid_str);
                }
                // Replace the invalid byte(s) with U+FFFD.
                buf.extend_from_slice(b"\\uFFFD");
                // Advance past the error.
                let skip = e.error_len().unwrap_or(1);
                remaining = &after[skip..];
            }
        }
    }
}

/// Escape a valid UTF-8 string for JSON.
fn escape_str_into(buf: &mut Vec<u8>, s: &str) {
    for &b in s.as_bytes() {
        match b {
            b'"' => buf.extend_from_slice(b"\\\""),
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            // Control characters U+0000..U+001F
            0x00..=0x1f => {
                buf.extend_from_slice(b"\\u00");
                buf.push(HEX[(b >> 4) as usize]);
                buf.push(HEX[(b & 0xf) as usize]);
            }
            _ => buf.push(b),
        }
    }
}

const HEX: &[u8; 16] = b"0123456789abcdef";

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Subprocess fallback backend
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Build the `journalctl` command with appropriate flags.
///
/// Uses `--output=export` for machine-optimized binary-safe output
/// (faster than `--output=json`). Each entry is a block of `FIELD=value\n`
/// lines separated by blank lines.
fn build_command(config: &JournaldConfig) -> Command {
    let mut cmd = Command::new(&config.journalctl_path);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    cmd.arg("--follow");
    cmd.arg("--all");
    cmd.arg("--output=export");

    if let Some(dir) = &config.journal_directory {
        cmd.arg(format!("--directory={dir}"));
    }
    if let Some(ns) = &config.journal_namespace {
        cmd.arg(format!("--namespace={ns}"));
    }

    if config.current_boot_only {
        cmd.arg("--boot");
    }

    if config.since_now {
        cmd.arg("--since=now");
    } else {
        cmd.arg("--since=2000-01-01");
    }

    for unit in &config.include_units {
        cmd.arg(format!("_SYSTEMD_UNIT={}", fixup_unit(unit)));
    }

    cmd
}

/// Read the journal via `journalctl` subprocess (fallback when libsystemd
/// is not available).
/// Reader loop for the `journalctl --output=export` subprocess backend.
///
/// The export format is:
/// - Each field is `NAME=value\n` (text fields)
/// - Binary fields: `NAME\n` followed by 8-byte LE length, then raw bytes, then `\n`
/// - Entries separated by a blank line (`\n`)
///
/// We parse each entry into a set of key-value pairs and serialize to JSON
/// (same format as the native backend), so the downstream pipeline sees
/// identical data regardless of backend.
fn subprocess_reader_loop(
    config: JournaldConfig,
    tx: crossbeam_channel::Sender<Vec<u8>>,
    running: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
    child_pid: Arc<AtomicU32>,
) {
    while running.load(Ordering::Acquire) {
        health.store(HEALTH_STARTING, Ordering::Release);

        let mut child = match spawn_journalctl(&config) {
            Ok(child) => child,
            Err(e) => {
                tracing::error!(error = %e, "failed to spawn journalctl");
                health.store(HEALTH_DEGRADED, Ordering::Release);
                if !backoff_or_stop(&running) {
                    return;
                }
                continue;
            }
        };

        // Publish PID so `Drop` can kill the child.
        child_pid.store(child.id(), Ordering::Release);

        health.store(HEALTH_OK, Ordering::Release);
        tracing::info!("journalctl started (pid={})", child.id());

        let Some(stdout) = child.stdout.take() else {
            tracing::error!("journalctl stdout not captured");
            child_pid.store(0, Ordering::Release);
            let _ = child.kill();
            let _ = child.wait();
            health.store(HEALTH_DEGRADED, Ordering::Release);
            if !backoff_or_stop(&running) {
                return;
            }
            continue;
        };

        // Drain stderr so it doesn't block.
        let stderr = child.stderr.take();
        let stderr_thread = stderr.map(|stderr| {
            std::thread::Builder::new()
                .name("journald-stderr".to_string())
                .spawn(move || drain_stderr(stderr))
                .ok()
        });

        let reader = BufReader::with_capacity(256 * 1024, stdout);
        let exited_cleanly = read_export_entries(reader, &tx, &running, &config.exclude_units);

        // Atomically take the PID before kill+wait so Drop cannot race.
        child_pid.swap(0, Ordering::AcqRel);
        let _ = child.kill();
        let _ = child.wait();

        if let Some(Some(handle)) = stderr_thread {
            let _ = handle.join();
        }

        if exited_cleanly || !running.load(Ordering::Acquire) {
            return;
        }

        tracing::warn!("journalctl exited unexpectedly, restarting after backoff");
        health.store(HEALTH_DEGRADED, Ordering::Release);
        if !backoff_or_stop(&running) {
            return;
        }
    }
}

/// Read export-format entries from a `BufReader`, parse to JSON, and send
/// over the channel. Returns `true` if we exited because the channel closed
/// or the running flag was cleared.
fn read_export_entries<R: Read>(
    mut reader: BufReader<R>,
    tx: &crossbeam_channel::Sender<Vec<u8>>,
    running: &Arc<AtomicBool>,
    exclude_units: &[String],
) -> bool {
    // Pre-normalize exclude units once to avoid per-entry allocations.
    let exclude_units: Vec<String> = exclude_units.iter().map(|u| fixup_unit(u)).collect();

    // Accumulate fields for the current entry.
    let mut fields: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(32);
    let mut line_buf = Vec::with_capacity(1024);

    loop {
        if !running.load(Ordering::Acquire) {
            return true;
        }

        line_buf.clear();
        let bytes_read = match reader.read_until(b'\n', &mut line_buf) {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(error = %e, "journalctl read error");
                return false;
            }
        };

        if bytes_read == 0 {
            // EOF — process exited.
            return false;
        }

        // Guard against oversized text fields (e.g. corrupt/malicious journal).
        if line_buf.len() > MAX_BINARY_FIELD_SIZE {
            tracing::warn!(
                len = line_buf.len(),
                "export text field exceeds maximum size, skipping"
            );
            line_buf.clear();
            continue;
        }

        // Strip trailing newline.
        let line = if line_buf.last() == Some(&b'\n') {
            &line_buf[..line_buf.len() - 1]
        } else {
            &line_buf[..]
        };

        if line.is_empty() {
            // Blank line = end of entry.
            if !fields.is_empty() {
                // Check exclude filter before serializing.
                if should_emit_export_entry(&fields, &exclude_units) {
                    let json = export_fields_to_json(&mut fields);
                    match tx.try_send(json) {
                        Ok(()) => {}
                        Err(TrySendError::Full(json)) => {
                            if tx.send(json).is_err() {
                                return true;
                            }
                        }
                        Err(TrySendError::Disconnected(_)) => return true,
                    }
                }
                fields.clear();
            }
            continue;
        }

        // Look for `=` separator. In export format, text fields are `NAME=value`.
        if let Some(eq_pos) = memchr::memchr(b'=', line) {
            let name = line[..eq_pos].to_vec();
            let value = line[eq_pos + 1..].to_vec();
            fields.push((name, value));
        } else {
            // Field name without `=` means binary field follows.
            // Next 8 bytes are LE length, then that many bytes of data.
            let field_name = line.to_vec();
            let mut len_buf = [0u8; 8];
            if reader.read_exact(&mut len_buf).is_err() {
                return false;
            }
            let data_len_u64 = u64::from_le_bytes(len_buf);

            // Cap binary field length to prevent malicious/corrupt journals
            // from causing huge allocations. Compare as u64 to avoid truncation
            // on 32-bit platforms.
            if data_len_u64 > MAX_BINARY_FIELD_SIZE as u64 {
                tracing::warn!(
                    field = %String::from_utf8_lossy(&field_name),
                    data_len = data_len_u64,
                    "binary journal field exceeds maximum size, skipping field"
                );
                // Skip past the oversized field data.
                // Read in chunks to avoid huge allocations.
                let mut remaining = data_len_u64;
                let mut discard = vec![0u8; 64 * 1024];
                while remaining > 0 {
                    let chunk = remaining.min(discard.len() as u64) as usize;
                    if reader.read_exact(&mut discard[..chunk]).is_err() {
                        return false;
                    }
                    remaining -= chunk as u64;
                }
                // Consume the trailing newline after the binary payload.
                let mut trailing_newline = [0u8; 1];
                if reader.read_exact(&mut trailing_newline).is_err() {
                    return false;
                }
                if trailing_newline[0] != b'\n' {
                    tracing::warn!("binary journal field missing trailing newline delimiter");
                    return false;
                }
                // Skip this field but continue parsing the entry.
                continue;
            }

            let data_len = data_len_u64 as usize;

            // Read the binary data + trailing newline.
            let mut skip_buf = vec![0u8; data_len + 1];
            if reader.read_exact(&mut skip_buf).is_err() {
                return false;
            }
            // Store as hex-encoded placeholder so the field is visible.
            let placeholder = if data_len > 64 {
                format!("[binary {data_len} bytes]").into_bytes()
            } else {
                skip_buf[..data_len].to_vec()
            };
            fields.push((field_name, placeholder));
        }
    }
}

/// Check whether an export-format entry should be emitted, based on exclude
/// filters. Looks at the `_SYSTEMD_UNIT` field.
///
/// `exclude_units` must already be normalized via [`fixup_unit`] to avoid
/// per-entry allocations on the hot path.
fn should_emit_export_entry(fields: &[(Vec<u8>, Vec<u8>)], exclude_units: &[String]) -> bool {
    if exclude_units.is_empty() {
        return true;
    }
    for (name, value) in fields {
        if name == b"_SYSTEMD_UNIT"
            && let Ok(unit) = std::str::from_utf8(value)
        {
            let normalized = fixup_unit(unit);
            if exclude_units.contains(&normalized) {
                return false;
            }
        }
    }
    true
}

/// Convert a set of export-format fields to a JSON object (newline-terminated).
///
/// Uses the same `normalize_fields` + `write_fields_as_json` pipeline as the
/// native backend, guaranteeing identical output for the same field set.
fn export_fields_to_json(fields: &mut Vec<(Vec<u8>, Vec<u8>)>) -> Vec<u8> {
    normalize_fields(fields);
    let mut buf = Vec::with_capacity(512);
    write_fields_as_json(&mut buf, fields);
    buf.push(b'\n');
    buf
}

// ── Shared helpers ────────────────────────────────────────────────────

/// Ensure a unit name has a `.` — append `.service` if missing (like Vector).
fn fixup_unit(unit: &str) -> String {
    if unit.contains('.') {
        unit.to_string()
    } else {
        format!("{unit}.service")
    }
}

/// Spawn `journalctl` with the configured arguments.
fn spawn_journalctl(config: &JournaldConfig) -> io::Result<Child> {
    build_command(config).spawn()
}

/// Convert a `_SOURCE_REALTIME_TIMESTAMP` (microseconds since Unix epoch)
/// to an RFC 3339 string like `2024-04-12T00:00:00.000000Z`.
///
/// Returns `None` for negative or unparsable values. Uses a simple
/// civil-time calculation (no chrono dependency needed at runtime).
fn usec_to_rfc3339(us: i64) -> Option<String> {
    if us < 0 {
        return None;
    }
    let secs = us / 1_000_000;
    let frac_us = (us % 1_000_000) as u32;

    // Convert Unix timestamp to calendar date via days-since-epoch.
    // Algorithm from Howard Hinnant's `chrono`-compatible date library.
    let days = secs / 86400;
    let time_of_day = (secs % 86400) as u32;

    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    let hour = time_of_day / 3600;
    let minute = (time_of_day % 3600) / 60;
    let second = time_of_day % 60;

    Some(format!(
        "{y:04}-{m:02}-{d:02}T{hour:02}:{minute:02}:{second:02}.{frac_us:06}Z"
    ))
}

/// Sleep for [`RESTART_BACKOFF`], checking the running flag periodically.
fn backoff_or_stop(running: &Arc<AtomicBool>) -> bool {
    let steps = 10;
    let step_duration = RESTART_BACKOFF / steps;
    for _ in 0..steps {
        if !running.load(Ordering::Acquire) {
            return false;
        }
        std::thread::sleep(step_duration);
    }
    running.load(Ordering::Acquire)
}

/// Drain stderr to tracing so journalctl warnings are visible.
fn drain_stderr(stderr: std::process::ChildStderr) {
    let reader = BufReader::new(stderr);
    for line in reader.split(b'\n').flatten() {
        let text = String::from_utf8_lossy(&line);
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            tracing::warn!(target: "journalctl", "{trimmed}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write};

    // ── fixup_unit ────────────────────────────────────────────────────

    #[test]
    fn fixup_unit_appends_service() {
        assert_eq!(fixup_unit("sshd"), "sshd.service");
        assert_eq!(fixup_unit("docker.service"), "docker.service");
        assert_eq!(fixup_unit("sysinit.target"), "sysinit.target");
    }

    // ── export format parsing ─────────────────────────────────────────

    #[test]
    fn export_fields_to_json_basic() {
        let mut fields = vec![
            (b"MESSAGE".to_vec(), b"hello world".to_vec()),
            (b"PRIORITY".to_vec(), b"6".to_vec()),
            (b"_SYSTEMD_UNIT".to_vec(), b"sshd.service".to_vec()),
        ];
        let json = export_fields_to_json(&mut fields);
        let text = String::from_utf8(json).unwrap();
        assert!(text.starts_with('{'));
        assert!(text.ends_with("}\n"));
        let v: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        // Normalized: field names are lowercased.
        assert_eq!(v["message"], "hello world");
        assert_eq!(v["priority"], "6");
        assert_eq!(v["_systemd_unit"], "sshd.service");
        // Synthesized: PRIORITY 6 → level INFO.
        assert_eq!(v["level"], "INFO");
    }

    #[test]
    fn export_fields_to_json_escapes_special_chars() {
        let mut fields = vec![(b"MESSAGE".to_vec(), b"line1\nline2\ttab\"quote".to_vec())];
        let json = export_fields_to_json(&mut fields);
        let text = String::from_utf8(json).unwrap();
        let v: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(v["message"], "line1\nline2\ttab\"quote");
    }

    #[test]
    fn export_fields_to_json_empty() {
        let mut fields: Vec<(Vec<u8>, Vec<u8>)> = vec![];
        let json = export_fields_to_json(&mut fields);
        assert_eq!(&json, b"{}\n");
    }

    #[test]
    fn should_emit_export_entry_no_excludes() {
        let fields = vec![(b"MESSAGE".to_vec(), b"hello".to_vec())];
        assert!(should_emit_export_entry(&fields, &[]));
    }

    #[test]
    fn should_emit_export_entry_excludes_matching_unit() {
        let fields = vec![
            (b"MESSAGE".to_vec(), b"hello".to_vec()),
            (b"_SYSTEMD_UNIT".to_vec(), b"sshd.service".to_vec()),
        ];
        assert!(!should_emit_export_entry(&fields, &[fixup_unit("sshd")]));
    }

    #[test]
    fn should_emit_export_entry_allows_non_matching_unit() {
        let fields = vec![
            (b"MESSAGE".to_vec(), b"hello".to_vec()),
            (b"_SYSTEMD_UNIT".to_vec(), b"sshd.service".to_vec()),
        ];
        assert!(should_emit_export_entry(&fields, &[fixup_unit("docker")]));
    }

    #[test]
    fn should_emit_export_entry_excludes_matching_unit_when_multiple() {
        let fields = vec![
            (b"_SYSTEMD_UNIT".to_vec(), b"session-1.scope".to_vec()),
            (b"_SYSTEMD_UNIT".to_vec(), b"sshd.service".to_vec()),
        ];
        assert!(!should_emit_export_entry(&fields, &[fixup_unit("sshd")]));
    }

    #[test]
    fn read_export_entries_applies_client_side_exclude_filter() {
        let mut export = Vec::new();
        export.extend_from_slice(b"MESSAGE=excluded\n");
        export.extend_from_slice(b"_SYSTEMD_UNIT=sshd.service\n\n");
        export.extend_from_slice(b"MESSAGE=allowed\n");
        export.extend_from_slice(b"_SYSTEMD_UNIT=docker.service\n\n");

        let mut tmp = tempfile::NamedTempFile::new().expect("temp export file should create");
        tmp.write_all(&export).expect("export payload should write");
        tmp.as_file_mut()
            .seek(SeekFrom::Start(0))
            .expect("export payload should rewind");
        let reader = BufReader::new(tmp.reopen().expect("temp export file should reopen"));
        let (tx, rx) = bounded(4);
        let running = Arc::new(AtomicBool::new(true));

        let exited_cleanly =
            read_export_entries(reader, &tx, &running, &["sshd.service".to_string()]);
        assert!(
            !exited_cleanly,
            "cursor EOF should be treated as subprocess exit"
        );

        let entries: Vec<Vec<u8>> = rx.try_iter().collect();
        assert_eq!(entries.len(), 1, "excluded entry should be dropped");
        let entry = String::from_utf8(entries[0].clone()).expect("json should be utf-8");
        let parsed: serde_json::Value =
            serde_json::from_str(entry.trim()).expect("entry should be valid json");
        assert_eq!(parsed["message"], "allowed");
        assert_eq!(parsed["_systemd_unit"], "docker.service");
    }

    #[test]
    fn read_export_entries_oversized_binary_length_does_not_overflow() {
        let mut export = Vec::new();
        export.extend_from_slice(b"BINARY_FIELD\n");
        export.extend_from_slice(&u64::MAX.to_le_bytes());

        let mut tmp = tempfile::NamedTempFile::new().expect("temp export file should create");
        tmp.write_all(&export).expect("export payload should write");
        tmp.as_file_mut()
            .seek(SeekFrom::Start(0))
            .expect("export payload should rewind");
        let reader = BufReader::new(tmp.reopen().expect("temp export file should reopen"));
        let (tx, rx) = bounded(1);
        let running = Arc::new(AtomicBool::new(true));

        let exited_cleanly = read_export_entries(reader, &tx, &running, &[]);
        assert!(!exited_cleanly, "short stream should fail without panic");
        assert!(
            rx.try_recv().is_err(),
            "oversized field should not emit data"
        );
    }

    #[test]
    fn wait_error_health_stays_degraded_until_wait_recovers() {
        let health = Arc::new(AtomicU8::new(HEALTH_OK));

        let first_error = Err(io::Error::other("first wait failure"));
        update_native_wait_health(&first_error, &health);
        assert_eq!(health.load(Ordering::Acquire), HEALTH_DEGRADED);

        let second_error = Err(io::Error::other("second wait failure"));
        update_native_wait_health(&second_error, &health);
        assert_eq!(health.load(Ordering::Acquire), HEALTH_DEGRADED);

        let nop_timeout = Ok(journal_ffi::SD_JOURNAL_NOP);
        update_native_wait_health(&nop_timeout, &health);
        assert_eq!(health.load(Ordering::Acquire), HEALTH_DEGRADED);

        let recovered = Ok(SD_JOURNAL_APPEND);
        update_native_wait_health(&recovered, &health);
        assert_eq!(health.load(Ordering::Acquire), HEALTH_OK);

        // Verify INVALIDATE also recovers health.
        health.store(HEALTH_DEGRADED, Ordering::Release);
        let invalidate_recovery = Ok(SD_JOURNAL_INVALIDATE);
        update_native_wait_health(&invalidate_recovery, &health);
        assert_eq!(health.load(Ordering::Acquire), HEALTH_OK);
    }

    // ── build_command (subprocess) ────────────────────────────────────

    #[test]
    fn build_command_basic() {
        let config = JournaldConfig::default();
        let cmd = build_command(&config);
        let args: Vec<_> = cmd
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        assert!(args.contains(&"--follow".to_string()));
        assert!(args.contains(&"--output=export".to_string()));
        assert!(args.contains(&"--boot".to_string()));
        assert!(args.contains(&"--since=2000-01-01".to_string()));
    }

    #[test]
    fn build_command_since_now() {
        let config = JournaldConfig {
            since_now: true,
            ..Default::default()
        };
        let cmd = build_command(&config);
        let args: Vec<_> = cmd
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        assert!(args.contains(&"--since=now".to_string()));
        assert!(!args.contains(&"--since=2000-01-01".to_string()));
    }

    #[test]
    fn build_command_with_units() {
        let config = JournaldConfig {
            include_units: vec!["sshd".to_string(), "docker.service".to_string()],
            ..Default::default()
        };
        let cmd = build_command(&config);
        let args: Vec<_> = cmd
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        assert!(args.contains(&"_SYSTEMD_UNIT=sshd.service".to_string()));
        assert!(args.contains(&"_SYSTEMD_UNIT=docker.service".to_string()));
    }

    #[test]
    fn build_command_all_boots() {
        let config = JournaldConfig {
            current_boot_only: false,
            ..Default::default()
        };
        let cmd = build_command(&config);
        let args: Vec<_> = cmd
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        assert!(!args.contains(&"--boot".to_string()));
    }

    #[test]
    fn build_command_with_directory_and_namespace() {
        let config = JournaldConfig {
            journal_directory: Some("/var/log/journal".to_string()),
            journal_namespace: Some("myapp".to_string()),
            ..Default::default()
        };
        let cmd = build_command(&config);
        let args: Vec<_> = cmd
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        assert!(args.contains(&"--directory=/var/log/journal".to_string()));
        assert!(args.contains(&"--namespace=myapp".to_string()));
    }

    // ── json_escape_into ──────────────────────────────────────────────

    #[test]
    fn json_escape_basic_string() {
        let mut buf = Vec::new();
        json_escape_into(&mut buf, b"hello world");
        assert_eq!(&buf, b"hello world");
    }

    #[test]
    fn json_escape_special_chars() {
        let mut buf = Vec::new();
        json_escape_into(&mut buf, b"line1\nline2\ttab\"quote\\backslash");
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            r#"line1\nline2\ttab\"quote\\backslash"#
        );
    }

    #[test]
    fn json_escape_control_chars() {
        let mut buf = Vec::new();
        json_escape_into(&mut buf, &[0x00, 0x1f]);
        assert_eq!(String::from_utf8(buf).unwrap(), r"\u0000\u001f");
    }

    // ── backend selection ─────────────────────────────────────────────

    #[test]
    fn backend_detection_does_not_panic() {
        let available = journal_ffi::is_native_available();
        if available {
            eprintln!("native sd_journal API is available");
        } else {
            eprintln!("native sd_journal API not available, subprocess fallback");
        }
    }

    // ── equivalence: shared write_fields_as_json ───────────────────────
    // Both native and subprocess backends call `write_fields_as_json` for
    // serialization. These tests verify the shared serializer produces
    // correct JSON for various field sets.

    #[test]
    fn write_fields_as_json_roundtrips() {
        let mut fields = vec![
            (b"__CURSOR".to_vec(), b"s=abc;i=1".to_vec()),
            (
                b"__REALTIME_TIMESTAMP".to_vec(),
                b"1712880000000000".to_vec(),
            ),
            (b"MESSAGE".to_vec(), b"hello world".to_vec()),
            (b"PRIORITY".to_vec(), b"6".to_vec()),
            (b"_SYSTEMD_UNIT".to_vec(), b"test.service".to_vec()),
        ];

        let json = export_fields_to_json(&mut fields);
        let v: serde_json::Value = serde_json::from_slice(&json).unwrap();
        // Lowercased field names.
        assert_eq!(v["message"], "hello world");
        assert_eq!(v["priority"], "6");
        assert_eq!(v["_systemd_unit"], "test.service");
        // __CURSOR dropped (double-underscore internal field).
        assert!(v.get("__cursor").is_none());
        assert!(v.get("__CURSOR").is_none());
        // Synthesized level from PRIORITY 6 (info).
        assert_eq!(v["level"], "INFO");
    }

    #[test]
    fn write_fields_as_json_with_special_characters() {
        let mut fields = vec![
            (
                b"MESSAGE".to_vec(),
                b"has \"quotes\" and\nnewlines".to_vec(),
            ),
            (
                b"_CMDLINE".to_vec(),
                b"/usr/bin/test --flag=val\\ue".to_vec(),
            ),
        ];

        let json = export_fields_to_json(&mut fields);
        let v: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(v["message"], "has \"quotes\" and\nnewlines");
        assert_eq!(v["_cmdline"], "/usr/bin/test --flag=val\\ue");
    }

    // ── normalization tests ──────────────────────────────────────────────

    #[test]
    fn normalize_lowercases_field_names() {
        let mut fields = vec![
            (b"MESSAGE".to_vec(), b"hello".to_vec()),
            (b"_PID".to_vec(), b"42".to_vec()),
            (b"SYSLOG_IDENTIFIER".to_vec(), b"test".to_vec()),
        ];
        normalize_fields(&mut fields);
        let names: Vec<&[u8]> = fields.iter().map(|(n, _)| n.as_slice()).collect();
        assert!(names.contains(&b"message".as_slice()));
        assert!(names.contains(&b"_pid".as_slice()));
        assert!(names.contains(&b"syslog_identifier".as_slice()));
    }

    #[test]
    fn normalize_drops_double_underscore_fields() {
        let mut fields = vec![
            (b"MESSAGE".to_vec(), b"hello".to_vec()),
            (b"__CURSOR".to_vec(), b"s=abc".to_vec()),
            (b"__REALTIME_TIMESTAMP".to_vec(), b"123".to_vec()),
            (b"__MONOTONIC_TIMESTAMP".to_vec(), b"456".to_vec()),
        ];
        normalize_fields(&mut fields);
        let names: Vec<&[u8]> = fields.iter().map(|(n, _)| n.as_slice()).collect();
        assert!(names.contains(&b"message".as_slice()));
        assert!(!names.iter().any(|n| n.starts_with(b"__")));
    }

    #[test]
    fn normalize_synthesizes_timestamp_from_source_realtime() {
        let mut fields = vec![
            (b"MESSAGE".to_vec(), b"hello".to_vec()),
            // 2024-04-12T00:00:00.000000Z in µs
            (
                b"_SOURCE_REALTIME_TIMESTAMP".to_vec(),
                b"1712880000000000".to_vec(),
            ),
        ];
        normalize_fields(&mut fields);
        let ts = fields
            .iter()
            .find(|(n, _)| n == b"timestamp")
            .map(|(_, v)| String::from_utf8(v.clone()).unwrap());
        assert!(ts.is_some(), "timestamp field should be synthesized");
        let ts = ts.unwrap();
        assert_eq!(ts, "2024-04-12T00:00:00.000000Z", "got: {ts}");
    }

    #[test]
    fn normalize_synthesizes_level_from_priority() {
        for (prio, expected) in [
            (b'0', "FATAL"),
            (b'1', "FATAL"),
            (b'2', "FATAL"),
            (b'3', "ERROR"),
            (b'4', "WARN"),
            (b'5', "INFO"),
            (b'6', "INFO"),
            (b'7', "DEBUG"),
        ] {
            let mut fields = vec![(b"PRIORITY".to_vec(), vec![prio])];
            normalize_fields(&mut fields);
            let level = fields
                .iter()
                .find(|(n, _)| n == b"level")
                .map(|(_, v)| String::from_utf8(v.clone()).unwrap());
            assert_eq!(
                level.as_deref(),
                Some(expected),
                "PRIORITY={} should map to {expected}",
                prio as char
            );
        }
    }

    #[test]
    fn normalize_no_level_without_priority() {
        let mut fields = vec![(b"MESSAGE".to_vec(), b"hello".to_vec())];
        normalize_fields(&mut fields);
        assert!(
            !fields.iter().any(|(n, _)| n == b"level"),
            "no level without PRIORITY"
        );
    }

    #[test]
    fn syslog_priority_to_level_all_values() {
        assert_eq!(syslog_priority_to_level(0), Some("FATAL"));
        assert_eq!(syslog_priority_to_level(3), Some("ERROR"));
        assert_eq!(syslog_priority_to_level(4), Some("WARN"));
        assert_eq!(syslog_priority_to_level(5), Some("INFO"));
        assert_eq!(syslog_priority_to_level(6), Some("INFO"));
        assert_eq!(syslog_priority_to_level(7), Some("DEBUG"));
        assert_eq!(syslog_priority_to_level(8), None);
    }

    #[test]
    fn normalize_preserves_existing_timestamp() {
        let mut fields = vec![
            (b"TIMESTAMP".to_vec(), b"user-supplied".to_vec()),
            (
                b"_SOURCE_REALTIME_TIMESTAMP".to_vec(),
                b"1712880000000000".to_vec(),
            ),
        ];
        normalize_fields(&mut fields);
        let timestamps: Vec<_> = fields.iter().filter(|(n, _)| n == b"timestamp").collect();
        assert_eq!(timestamps.len(), 1, "should not duplicate timestamp");
        assert_eq!(timestamps[0].1, b"user-supplied");
    }

    #[test]
    fn normalize_preserves_existing_level() {
        let mut fields = vec![
            (b"LEVEL".to_vec(), b"CUSTOM".to_vec()),
            (b"PRIORITY".to_vec(), b"3".to_vec()),
        ];
        normalize_fields(&mut fields);
        let levels: Vec<_> = fields.iter().filter(|(n, _)| n == b"level").collect();
        assert_eq!(levels.len(), 1, "should not duplicate level");
        assert_eq!(levels[0].1, b"CUSTOM");
    }

    // ── backend pref defaults ──────────────────────────────────────────

    #[test]
    fn backend_pref_defaults_to_auto() {
        assert_eq!(JournaldBackendPref::default(), JournaldBackendPref::Auto);
    }

    // ── json_escape_into handles invalid UTF-8 ─────────────────────────

    #[test]
    fn json_escape_invalid_utf8_replaced() {
        let mut buf = Vec::new();
        // 0xFF is not valid UTF-8.
        json_escape_into(&mut buf, &[b'a', 0xFF, b'b']);
        let s = String::from_utf8(buf).expect("output should be valid UTF-8");
        assert_eq!(s, "a\\uFFFDb");
    }

    #[test]
    fn json_escape_mixed_valid_invalid() {
        let mut buf = Vec::new();
        // Valid UTF-8 "hello", then 0xFE (invalid), then "world".
        let input = b"hello\xFEworld";
        json_escape_into(&mut buf, input);
        let s = String::from_utf8(buf).expect("output should be valid UTF-8");
        assert_eq!(s, "hello\\uFFFDworld");
    }
}

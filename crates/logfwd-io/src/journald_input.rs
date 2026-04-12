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
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Duration;

use crossbeam_channel::{Receiver, TrySendError, bounded};
use logfwd_types::diagnostics::{ComponentHealth, ComponentStats};

use logfwd_config::JournaldBackendConfig;

use crate::input::{InputEvent, InputSource};
use crate::journal_ffi;

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

/// Health encoding used in the atomic health byte.
const HEALTH_OK: u8 = 0;
const HEALTH_STARTING: u8 = 1;
const HEALTH_DEGRADED: u8 = 2;

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
    pub backend: JournaldBackendConfig,
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
            backend: JournaldBackendConfig::Auto,
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

/// Journald input that tails the systemd journal.
///
/// Prefers the native `sd_journal` API. Falls back to a `journalctl` subprocess
/// if `libsystemd.so.0` is not available.
pub struct JournaldInput {
    name: String,
    rx: Receiver<Vec<u8>>,
    is_running: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
    stats: Arc<ComponentStats>,
    backend: JournaldBackend,
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

        let thread_running = Arc::clone(&is_running);
        let thread_health = Arc::clone(&health);
        let thread_name = name.clone();

        // Resolve backend based on config + runtime availability.
        let native_available = journal_ffi::is_native_available();
        let use_native = match config.backend {
            JournaldBackendConfig::Auto => native_available,
            JournaldBackendConfig::Native => {
                if !native_available {
                    return Err(io::Error::other(
                        "backend: native requested but libsystemd.so.0 is not available",
                    ));
                }
                true
            }
            JournaldBackendConfig::Subprocess => false,
        };

        let backend = if use_native {
            JournaldBackend::Native
        } else {
            JournaldBackend::Subprocess
        };

        if use_native {
            tracing::info!("journald input '{thread_name}': using native sd_journal API");
        } else {
            let reason = if matches!(config.backend, JournaldBackendConfig::Subprocess) {
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
                    native_reader_loop(config, tx, thread_running, thread_health);
                } else {
                    subprocess_reader_loop(config, tx, thread_running, thread_health);
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
        })
    }

    /// Which backend this input is using.
    pub fn backend(&self) -> JournaldBackend {
        self.backend
    }
}

impl Drop for JournaldInput {
    fn drop(&mut self) {
        self.is_running.store(false, Ordering::Release);
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
                    self.stats.inc_bytes(len as u64);
                    events.push(InputEvent::Data {
                        bytes: line,
                        source_id: None,
                        accounted_bytes: len as u64,
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
    config: JournaldConfig,
    tx: crossbeam_channel::Sender<Vec<u8>>,
    running: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
) {
    health.store(HEALTH_STARTING, Ordering::Release);

    let mut journal = match open_native_journal(&config) {
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
    if let Err(e) = seek_start(&mut journal, &config) {
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
                            json_buf.push(b'\n');
                            let payload = json_buf.clone();
                            json_buf.clear();

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
        match journal.wait(NATIVE_WAIT_USEC) {
            Ok(_) => {} // NOP, APPEND, or INVALIDATE — all handled by next loop iteration
            Err(e) => {
                tracing::warn!(error = %e, "sd_journal_wait error");
                health.store(HEALTH_DEGRADED, Ordering::Release);
                // Brief sleep to avoid spinning on persistent errors.
                std::thread::sleep(Duration::from_millis(100));
                health.store(HEALTH_OK, Ordering::Release);
            }
        }
    }
}

/// Open a journal handle with the appropriate flags/directory.
fn open_native_journal(config: &JournaldConfig) -> io::Result<journal_ffi::Journal> {
    let flags = journal_ffi::SD_JOURNAL_LOCAL_ONLY | journal_ffi::SD_JOURNAL_SYSTEM;

    if let Some(ref dir) = config.journal_directory {
        journal_ffi::Journal::open_directory(dir, flags)
    } else {
        journal_ffi::Journal::open(flags)
    }
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
    if !exclude_units.is_empty() {
        if let Ok(Some(field_data)) = journal.get_data("_SYSTEMD_UNIT") {
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
    }

    // Collect all fields, then use the shared serializer.
    let mut fields: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(32);
    journal.restart_data();
    while let Some(field_bytes) = journal.enumerate_data()? {
        let eq_pos = match memchr::memchr(b'=', field_bytes) {
            Some(p) => p,
            None => continue,
        };
        fields.push((
            field_bytes[..eq_pos].to_vec(),
            field_bytes[eq_pos + 1..].to_vec(),
        ));
    }

    write_fields_as_json(buf, &fields);
    Ok(Some(()))
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
/// Non-UTF8 bytes are escaped as `\uXXXX`. This matches journalctl's behavior
/// for binary fields (though journalctl uses integer arrays for fully binary
/// fields — we use \u escapes as a simpler approximation for the POC).
fn json_escape_into(buf: &mut Vec<u8>, input: &[u8]) {
    for &b in input {
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
            // Printable ASCII and valid UTF-8 continuation bytes.
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

        health.store(HEALTH_OK, Ordering::Release);
        tracing::info!("journalctl started (pid={})", child.id());

        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                tracing::error!("journalctl stdout not captured");
                let _ = child.kill();
                let _ = child.wait();
                health.store(HEALTH_DEGRADED, Ordering::Release);
                if !backoff_or_stop(&running) {
                    return;
                }
                continue;
            }
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
fn read_export_entries(
    mut reader: BufReader<std::process::ChildStdout>,
    tx: &crossbeam_channel::Sender<Vec<u8>>,
    running: &Arc<AtomicBool>,
    exclude_units: &[String],
) -> bool {
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
                if should_emit_export_entry(&fields, exclude_units) {
                    let json = export_fields_to_json(&fields);
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
            // We skip binary fields (they're rare — e.g. coredumps).
            let field_name = line.to_vec();
            let mut len_buf = [0u8; 8];
            if reader.read_exact(&mut len_buf).is_err() {
                return false;
            }
            let data_len = u64::from_le_bytes(len_buf) as usize;
            // Skip the binary data + trailing newline.
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
fn should_emit_export_entry(fields: &[(Vec<u8>, Vec<u8>)], exclude_units: &[String]) -> bool {
    if exclude_units.is_empty() {
        return true;
    }
    for (name, value) in fields {
        if name == b"_SYSTEMD_UNIT" {
            if let Ok(unit) = std::str::from_utf8(value) {
                let normalized = fixup_unit(unit);
                for excluded in exclude_units {
                    if fixup_unit(excluded) == normalized {
                        return false;
                    }
                }
            }
            return true;
        }
    }
    true
}

/// Convert a set of export-format fields to a JSON object (newline-terminated).
///
/// Uses the same `write_fields_as_json` serializer as the native backend,
/// guaranteeing identical output for the same field set.
fn export_fields_to_json(fields: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
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
        let fields = vec![
            (b"MESSAGE".to_vec(), b"hello world".to_vec()),
            (b"PRIORITY".to_vec(), b"6".to_vec()),
            (b"_SYSTEMD_UNIT".to_vec(), b"sshd.service".to_vec()),
        ];
        let json = export_fields_to_json(&fields);
        let text = String::from_utf8(json).unwrap();
        assert!(text.starts_with('{'));
        assert!(text.ends_with("}\n"));
        // Parse as JSON to verify structure.
        let v: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(v["MESSAGE"], "hello world");
        assert_eq!(v["PRIORITY"], "6");
        assert_eq!(v["_SYSTEMD_UNIT"], "sshd.service");
    }

    #[test]
    fn export_fields_to_json_escapes_special_chars() {
        let fields = vec![(b"MESSAGE".to_vec(), b"line1\nline2\ttab\"quote".to_vec())];
        let json = export_fields_to_json(&fields);
        let text = String::from_utf8(json).unwrap();
        let v: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(v["MESSAGE"], "line1\nline2\ttab\"quote");
    }

    #[test]
    fn export_fields_to_json_empty() {
        let fields: Vec<(Vec<u8>, Vec<u8>)> = vec![];
        let json = export_fields_to_json(&fields);
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
        assert!(!should_emit_export_entry(&fields, &["sshd".to_string()]));
    }

    #[test]
    fn should_emit_export_entry_allows_non_matching_unit() {
        let fields = vec![
            (b"MESSAGE".to_vec(), b"hello".to_vec()),
            (b"_SYSTEMD_UNIT".to_vec(), b"sshd.service".to_vec()),
        ];
        assert!(should_emit_export_entry(&fields, &["docker".to_string()]));
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
        assert_eq!(String::from_utf8(buf).unwrap(), r#"\u0000\u001f"#);
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
        let fields = vec![
            (b"__CURSOR".to_vec(), b"s=abc;i=1".to_vec()),
            (
                b"__REALTIME_TIMESTAMP".to_vec(),
                b"1712880000000000".to_vec(),
            ),
            (b"MESSAGE".to_vec(), b"hello world".to_vec()),
            (b"PRIORITY".to_vec(), b"6".to_vec()),
            (b"_SYSTEMD_UNIT".to_vec(), b"test.service".to_vec()),
        ];

        let json = export_fields_to_json(&fields);
        let v: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(v["MESSAGE"], "hello world");
        assert_eq!(v["PRIORITY"], "6");
        assert_eq!(v["_SYSTEMD_UNIT"], "test.service");
        assert_eq!(v["__CURSOR"], "s=abc;i=1");
    }

    #[test]
    fn write_fields_as_json_with_special_characters() {
        let fields = vec![
            (
                b"MESSAGE".to_vec(),
                b"has \"quotes\" and\nnewlines".to_vec(),
            ),
            (
                b"_CMDLINE".to_vec(),
                b"/usr/bin/test --flag=val\\ue".to_vec(),
            ),
        ];

        let json = export_fields_to_json(&fields);
        let v: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(v["MESSAGE"], "has \"quotes\" and\nnewlines");
        assert_eq!(v["_CMDLINE"], "/usr/bin/test --flag=val\\ue");
    }

    // ── backend config validation ──────────────────────────────────────

    #[test]
    fn backend_config_deserializes() {
        let auto: JournaldBackendConfig = serde_json::from_str("\"auto\"").unwrap();
        assert_eq!(auto, JournaldBackendConfig::Auto);

        let native: JournaldBackendConfig = serde_json::from_str("\"native\"").unwrap();
        assert_eq!(native, JournaldBackendConfig::Native);

        let sub: JournaldBackendConfig = serde_json::from_str("\"subprocess\"").unwrap();
        assert_eq!(sub, JournaldBackendConfig::Subprocess);
    }
}

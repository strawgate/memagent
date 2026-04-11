//! Journald (systemd journal) input source.
//!
//! Spawns a `journalctl --follow --output=json` subprocess in a background
//! thread. Journal entries arrive as newline-delimited JSON objects. A bounded
//! crossbeam channel bridges the blocking reader to the non-blocking `poll()`
//! interface required by [`InputSource`].
//!
//! If the subprocess exits unexpectedly, the background thread waits
//! [`RESTART_BACKOFF`] before relaunching. This matches the resilience model
//! used by Vector and Filebeat for the same `journalctl` subprocess pattern.

use std::io::{self, BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Duration;

use crossbeam_channel::{Receiver, TrySendError, bounded};
use logfwd_types::diagnostics::{ComponentHealth, ComponentStats};

use crate::input::{InputEvent, InputSource};

/// Channel capacity between the reader thread and `poll()`.
const CHANNEL_CAPACITY: usize = 4096;

/// Maximum lines drained per `poll()` call to prevent one busy input from
/// starving other pipeline inputs.
const MAX_LINES_PER_POLL: usize = 512;

/// Maximum total bytes emitted per `poll()` call.
const MAX_BYTES_PER_POLL: usize = 2 * 1024 * 1024;

/// Backoff before restarting a crashed `journalctl` process.
const RESTART_BACKOFF: Duration = Duration::from_secs(1);

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
    /// Path to `journalctl` binary.
    pub journalctl_path: String,
    /// Custom journal directory.
    pub journal_directory: Option<String>,
    /// Journal namespace.
    pub journal_namespace: Option<String>,
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
        }
    }
}

/// Journald input that tails the systemd journal via a `journalctl` subprocess.
pub struct JournaldInput {
    name: String,
    rx: Receiver<Vec<u8>>,
    is_running: Arc<AtomicBool>,
    health: Arc<AtomicU8>,
    stats: Arc<ComponentStats>,
}

impl JournaldInput {
    /// Create a new journald input. Spawns a background thread that manages
    /// the `journalctl` subprocess lifecycle.
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

        std::thread::Builder::new()
            .name(format!("journald-{thread_name}"))
            .spawn(move || {
                reader_loop(config, tx, thread_running, thread_health);
            })
            .map_err(io::Error::other)?;

        Ok(Self {
            name,
            rx,
            is_running,
            health,
            stats,
        })
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
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    // Reader thread exited — report unhealthy but don't error.
                    // The is_running flag will be false if we're shutting down.
                    break;
                }
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

// ── Background reader ─────────────────────────────────────────────────

/// Build the `journalctl` command with appropriate flags.
fn build_command(config: &JournaldConfig) -> Command {
    let mut cmd = Command::new(&config.journalctl_path);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    cmd.arg("--follow");
    cmd.arg("--all");
    cmd.arg("--output=json");
    // --show-cursor appends a cursor line after each entry for future
    // checkpoint support.
    cmd.arg("--show-cursor");

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
        // Without a cursor, read full history from earliest available.
        cmd.arg("--since=2000-01-01");
    }

    // Apply unit filters as journalctl match expressions.
    // journalctl treats multiple matches on the same field as OR.
    for unit in &config.include_units {
        cmd.arg(format!("_SYSTEMD_UNIT={}", fixup_unit(unit)));
    }

    cmd
}

/// Ensure a unit name has a `.` — append `.service` if missing (like Vector).
fn fixup_unit(unit: &str) -> String {
    if unit.contains('.') {
        unit.to_string()
    } else {
        format!("{unit}.service")
    }
}

/// Parse a `journalctl --output=json` line, optionally applying exclude filters.
///
/// Returns `None` if the line should be skipped (cursor metadata, empty, or
/// excluded by unit filter).
fn should_emit_line(line: &[u8], exclude_units: &[String]) -> bool {
    // Skip empty lines
    if line.is_empty() {
        return false;
    }

    // journalctl --show-cursor emits lines like:
    //   -- cursor: s=...;i=...;b=...;m=...;t=...;x=...
    if line.starts_with(b"-- cursor:") {
        return false;
    }

    // Skip lines that don't look like JSON objects
    if line.first() != Some(&b'{') {
        return false;
    }

    // Apply exclude_units filter by scanning for _SYSTEMD_UNIT in the JSON.
    // This is a fast-path heuristic: we look for the field in the raw bytes
    // to avoid parsing JSON for every excluded entry.
    if !exclude_units.is_empty() {
        // Extract _SYSTEMD_UNIT value from the JSON line
        if let Some(unit) = extract_systemd_unit(line) {
            let normalized = fixup_unit(&unit);
            for excluded in exclude_units {
                if fixup_unit(excluded) == normalized {
                    return false;
                }
            }
        }
    }

    true
}

/// Fast extraction of `_SYSTEMD_UNIT` from a JSON line without full parsing.
fn extract_systemd_unit(line: &[u8]) -> Option<String> {
    // Look for "_SYSTEMD_UNIT":"<value>"
    let needle = b"\"_SYSTEMD_UNIT\":\"";
    let line_str = line;
    if let Some(pos) = memchr::memmem::find(line_str, needle) {
        let start = pos + needle.len();
        if let Some(end_offset) = memchr::memchr(b'"', &line_str[start..]) {
            let value = &line_str[start..start + end_offset];
            return String::from_utf8(value.to_vec()).ok();
        }
    }
    None
}

/// Main reader loop running in a background thread.
///
/// Spawns `journalctl`, reads lines, sends them over the channel. Restarts
/// on unexpected exit with backoff.
fn reader_loop(
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

        // Read stdout line by line.
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

        // Spawn a thread to drain stderr so it doesn't block.
        let stderr = child.stderr.take();
        let stderr_thread = stderr.map(|stderr| {
            std::thread::Builder::new()
                .name("journald-stderr".to_string())
                .spawn(move || drain_stderr(stderr))
                .ok()
        });

        let reader = BufReader::with_capacity(256 * 1024, stdout);
        let mut exited_cleanly = false;

        for line_result in reader.split(b'\n') {
            if !running.load(Ordering::Acquire) {
                exited_cleanly = true;
                break;
            }

            match line_result {
                Ok(line) => {
                    if !should_emit_line(&line, &config.exclude_units) {
                        continue;
                    }
                    // Add a trailing newline so the downstream framer/scanner
                    // can delimit records the same way as file or TCP inputs.
                    let mut payload = line;
                    payload.push(b'\n');

                    match tx.try_send(payload) {
                        Ok(()) => {}
                        Err(TrySendError::Full(payload)) => {
                            // Blocking send — apply backpressure to journalctl.
                            if tx.send(payload).is_err() {
                                // Receiver dropped — shutting down.
                                exited_cleanly = true;
                                break;
                            }
                        }
                        Err(TrySendError::Disconnected(_)) => {
                            exited_cleanly = true;
                            break;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "journalctl read error");
                    break;
                }
            }
        }

        // Clean up the child process.
        let _ = child.kill();
        let _ = child.wait();

        // Wait for stderr thread to finish.
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

/// Spawn `journalctl` with the configured arguments.
fn spawn_journalctl(config: &JournaldConfig) -> io::Result<Child> {
    build_command(config).spawn()
}

/// Sleep for [`RESTART_BACKOFF`], checking the running flag periodically.
/// Returns `true` if we should continue, `false` if shutting down.
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

    #[test]
    fn fixup_unit_appends_service() {
        assert_eq!(fixup_unit("sshd"), "sshd.service");
        assert_eq!(fixup_unit("docker.service"), "docker.service");
        assert_eq!(fixup_unit("sysinit.target"), "sysinit.target");
    }

    #[test]
    fn should_emit_line_skips_empty() {
        assert!(!should_emit_line(b"", &[]));
    }

    #[test]
    fn should_emit_line_skips_cursor() {
        assert!(!should_emit_line(
            b"-- cursor: s=abc;i=123;b=xyz;m=456;t=789;x=000",
            &[]
        ));
    }

    #[test]
    fn should_emit_line_skips_non_json() {
        assert!(!should_emit_line(b"some random text", &[]));
    }

    #[test]
    fn should_emit_line_accepts_json() {
        assert!(should_emit_line(
            br#"{"MESSAGE":"hello","_SYSTEMD_UNIT":"test.service"}"#,
            &[]
        ));
    }

    #[test]
    fn should_emit_line_excludes_unit() {
        let line = br#"{"MESSAGE":"hello","_SYSTEMD_UNIT":"sshd.service"}"#;
        assert!(!should_emit_line(line, &["sshd".to_string()]));
        assert!(should_emit_line(line, &["docker".to_string()]));
    }

    #[test]
    fn extract_systemd_unit_works() {
        let line = br#"{"MESSAGE":"hello","_SYSTEMD_UNIT":"sshd.service","PRIORITY":"6"}"#;
        assert_eq!(extract_systemd_unit(line), Some("sshd.service".to_string()));
    }

    #[test]
    fn extract_systemd_unit_missing() {
        let line = br#"{"MESSAGE":"hello","PRIORITY":"6"}"#;
        assert_eq!(extract_systemd_unit(line), None);
    }

    #[test]
    fn build_command_basic() {
        let config = JournaldConfig::default();
        let cmd = build_command(&config);
        let args: Vec<_> = cmd
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        assert!(args.contains(&"--follow".to_string()));
        assert!(args.contains(&"--output=json".to_string()));
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
}

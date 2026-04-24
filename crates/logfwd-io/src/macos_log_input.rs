use std::io;
#[cfg(target_os = "macos")]
use std::io::{BufRead, BufReader};
#[cfg(target_os = "macos")]
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

#[cfg(target_os = "macos")]
use bytes::Bytes;
use crossbeam_channel::{Receiver, bounded};
use logfwd_types::diagnostics::{ComponentHealth, ComponentStats};

use crate::input::{InputEvent, InputSource};

#[cfg(target_os = "macos")]
const CHANNEL_CAPACITY: usize = 4096;

/// Maximum lines drained per `poll()` call to prevent one busy input from
/// monopolizing the runtime.
const MAX_LINES_PER_POLL: usize = 1000;

/// Maximum total bytes drained per `poll()` call (approx 4MB).
const MAX_BYTES_PER_POLL: usize = 4 * 1024 * 1024;

/// Subprocess-based input reading `log stream --style ndjson`.
pub struct MacosLogInput {
    name: String,
    stats: Arc<ComponentStats>,
    is_finished: Arc<AtomicBool>,
    rx: Receiver<InputEvent>,
    #[allow(dead_code)]
    thread: Option<thread::JoinHandle<()>>,
    #[cfg(target_os = "macos")]
    child_pid: Arc<std::sync::atomic::AtomicU32>,
}

impl MacosLogInput {
    #[cfg(target_os = "macos")]
    pub fn new(
        name: &str,
        level: Option<&str>,
        subsystem: Option<&str>,
        process: Option<&str>,
        stats: Arc<ComponentStats>,
    ) -> Self {
        let (tx, rx) = bounded(CHANNEL_CAPACITY);
        let is_finished = Arc::new(AtomicBool::new(false));
        let finished_flag = Arc::clone(&is_finished);
        let child_pid = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let child_pid_clone = Arc::clone(&child_pid);
        let thread_name = name.to_string();

        let mut cmd = Command::new("log");
        cmd.arg("stream").arg("--style").arg("ndjson");

        if let Some(l) = level {
            cmd.arg("--level").arg(l);
        }

        // Combine predicates
        let mut predicates = Vec::new();
        if let Some(sub) = subsystem {
            predicates.push(format!("subsystem == '{}'", sub.replace('\'', "\\'")));
        }
        if let Some(proc) = process {
            predicates.push(format!("process == '{}'", proc.replace('\'', "\\'")));
        }

        if !predicates.is_empty() {
            cmd.arg("--predicate").arg(predicates.join(" AND "));
        }

        let thread_result = thread::Builder::new()
            .name(format!("macos-log-{name}"))
            .spawn(move || {
                let mut child = match cmd.stdout(Stdio::piped()).spawn() {
                    Ok(child) => child,
                    Err(e) => {
                        tracing::error!("Failed to spawn log stream for input {thread_name}: {e}");
                        finished_flag.store(true, Ordering::Release);
                        return;
                    }
                };
                child_pid_clone.store(child.id(), Ordering::Release);

                let stdout = child.stdout.take().expect("Stdout should be piped");
                let mut reader = BufReader::new(stdout);
                let mut line = String::new();

                loop {
                    line.clear();
                    match reader.read_line(&mut line) {
                        Ok(0) => break, // EOF
                        Ok(_) => {
                            // Trim trailing newline
                            if line.ends_with('\n') {
                                line.pop();
                            }
                            if line.ends_with('\r') {
                                line.pop();
                            }

                            if line.is_empty()
                                || line == "["
                                || line == "]"
                                || line.starts_with(',')
                            {
                                let trimmed = line.trim_start_matches(',');
                                if trimmed.is_empty() || trimmed == "[" || trimmed == "]" {
                                    continue;
                                }
                                line = trimmed.to_string();
                            }

                            let len = line.len();
                            let event = InputEvent::Data {
                                bytes: Bytes::from(line.clone()),
                                source_id: None,
                                accounted_bytes: len as u64,
                                cri_metadata: None,
                            };

                            if tx.send(event).is_err() {
                                break; // Receiver disconnected
                            }
                        }
                        Err(e) => {
                            tracing::error!("Error reading log stream for {thread_name}: {e}");
                            break;
                        }
                    }
                }
                finished_flag.store(true, Ordering::Release);
            });

        let thread = match thread_result {
            Ok(t) => Some(t),
            Err(e) => {
                tracing::error!("Failed to spawn macos_log thread: {e}");
                is_finished.store(true, Ordering::Release);
                None
            }
        };

        Self {
            name: name.to_string(),
            stats,
            is_finished,
            rx,
            thread,
            child_pid,
        }
    }

    #[cfg(not(target_os = "macos"))]
    pub fn new(
        name: &str,
        _level: Option<&str>,
        _subsystem: Option<&str>,
        _process: Option<&str>,
        stats: Arc<ComponentStats>,
    ) -> Self {
        let (_, rx) = bounded(1);
        tracing::error!("macos_log input '{name}' is only supported on macOS");
        Self {
            name: name.to_string(),
            stats,
            is_finished: Arc::new(AtomicBool::new(true)),
            rx,
            thread: None,
            #[cfg(target_os = "macos")]
            child_pid: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        }
    }
}

impl InputSource for MacosLogInput {
    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        if cfg!(not(target_os = "macos")) {
            return ComponentHealth::Degraded;
        }
        if self.is_finished.load(Ordering::Acquire) {
            ComponentHealth::Degraded
        } else {
            ComponentHealth::Healthy
        }
    }

    fn is_finished(&self) -> bool {
        self.is_finished.load(Ordering::Acquire)
    }

    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let mut events = Vec::new();
        let mut lines_read = 0;
        let mut total_bytes = 0;

        loop {
            match self.rx.try_recv() {
                Ok(event) => {
                    if let InputEvent::Data {
                        accounted_bytes, ..
                    } = &event
                    {
                        total_bytes += *accounted_bytes as usize;
                        lines_read += 1;
                    }
                    events.push(event);

                    if lines_read >= MAX_LINES_PER_POLL || total_bytes >= MAX_BYTES_PER_POLL {
                        break;
                    }
                }
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    self.is_finished.store(true, Ordering::Release);
                    break;
                }
            }
        }

        self.stats
            .lines_total
            .fetch_add(events.len() as u64, Ordering::Relaxed);
        self.stats
            .bytes_total
            .fetch_add(total_bytes as u64, Ordering::Relaxed);

        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use std::sync::atomic::AtomicU64;

    #[test]
    fn test_macos_log_input_degraded_on_linux() {
        let stats = Arc::new(ComponentStats::default());

        let mut input = MacosLogInput::new("test", None, None, None, stats);

        if cfg!(target_os = "macos") {
            assert!(matches!(input.health(), ComponentHealth::Healthy));
        } else {
            assert!(matches!(input.health(), ComponentHealth::Degraded));
            // On non-macos, it should immediately be finished
            assert!(input.is_finished());
            assert!(input.poll().unwrap().is_empty());
        }
    }
}

#[cfg(target_os = "macos")]
impl Drop for MacosLogInput {
    fn drop(&mut self) {
        let pid = self.child_pid.load(Ordering::Acquire);
        if pid > 0 {
            // Safety: calling libc::kill on a child process we own.
            // The PID was set by us from Command::spawn() and only cleared here.
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGTERM);
            }
        }
    }
}

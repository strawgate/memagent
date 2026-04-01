//! Lazy stderr capture for the diagnostics dashboard.
//!
//! Does nothing until `/api/logs` is first requested. Then:
//! 1. `dup(2)` saves the original stderr fd
//! 2. `pipe()` creates a pipe
//! 3. `dup2(write_fd, 2)` redirects stderr to the pipe
//! 4. A reader thread tees pipe data to the original fd + ring buffer
//!
//! If nobody polls for 60s, the capture is torn down and stderr restored.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const MAX_LINES: usize = 1000;
const IDLE_TIMEOUT_SECS: u64 = 60;

/// Shared state between the reader thread, the HTTP handler, and teardown.
struct CaptureState {
    lines: Mutex<VecDeque<String>>,
    last_poll: AtomicU64, // epoch millis of last /api/logs request
    active: AtomicBool,
    /// Original stderr fd (before dup2). -1 if not capturing.
    #[cfg(unix)]
    orig_fd: std::sync::atomic::AtomicI32,
}

impl CaptureState {
    fn new() -> Self {
        Self {
            lines: Mutex::new(VecDeque::with_capacity(MAX_LINES)),
            last_poll: AtomicU64::new(0),
            active: AtomicBool::new(false),
            #[cfg(unix)]
            orig_fd: std::sync::atomic::AtomicI32::new(-1),
        }
    }

    fn push_line(&self, line: String) {
        if let Ok(mut buf) = self.lines.lock() {
            if buf.len() >= MAX_LINES {
                buf.pop_front();
            }
            buf.push_back(line);
        }
    }

    fn get_lines(&self) -> Vec<String> {
        self.last_poll.store(epoch_ms(), Ordering::Relaxed);
        match self.lines.lock() {
            Ok(buf) => buf.iter().cloned().collect(),
            Err(_) => vec![],
        }
    }

    fn idle_secs(&self) -> u64 {
        let last = self.last_poll.load(Ordering::Relaxed);
        if last == 0 {
            return 0;
        }
        (epoch_ms().saturating_sub(last)) / 1000
    }
}

fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Handle to the stderr capture system. Clone-cheap (Arc inside).
#[derive(Clone)]
pub struct StderrCapture {
    state: Arc<CaptureState>,
}

impl Default for StderrCapture {
    fn default() -> Self {
        Self::new()
    }
}

impl StderrCapture {
    pub fn new() -> Self {
        Self {
            state: Arc::new(CaptureState::new()),
        }
    }

    /// Called by the /api/logs handler. Starts capture on first call.
    /// Returns the last N lines of stderr.
    pub fn get_logs(&self) -> Vec<String> {
        if !self.state.active.load(Ordering::Relaxed) {
            self.start();
        }
        self.state.get_lines()
    }

    /// Is capture currently active?
    pub fn is_active(&self) -> bool {
        self.state.active.load(Ordering::Relaxed)
    }

    #[cfg(unix)]
    fn start(&self) {
        // Only one thread should set this up.
        if self
            .state
            .active
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return; // another thread already started capture
        }

        self.state.last_poll.store(epoch_ms(), Ordering::Relaxed);

        unsafe {
            // Save original stderr.
            let orig = libc::dup(2);
            if orig < 0 {
                self.state.active.store(false, Ordering::Relaxed);
                return;
            }
            self.state.orig_fd.store(orig, Ordering::Relaxed);

            // Create pipe.
            let mut fds = [0i32; 2];
            if libc::pipe(fds.as_mut_ptr()) != 0 {
                libc::close(orig);
                self.state.active.store(false, Ordering::Relaxed);
                return;
            }
            let (read_fd, write_fd) = (fds[0], fds[1]);

            // Redirect stderr to write end of pipe.
            if libc::dup2(write_fd, 2) < 0 {
                libc::close(read_fd);
                libc::close(write_fd);
                libc::close(orig);
                self.state.active.store(false, Ordering::Relaxed);
                return;
            }
            libc::close(write_fd); // fd 2 now holds the write end

            // Spawn reader thread.
            let state = Arc::clone(&self.state);
            std::thread::Builder::new()
                .name("stderr-capture".into())
                .spawn(move || {
                    reader_loop(read_fd, orig, &state);
                })
                .ok();
        }
    }

    #[cfg(not(unix))]
    fn start(&self) {
        // No-op on non-unix platforms.
    }
}

/// Reader thread: reads from pipe, tees to original stderr, pushes to buffer.
/// Exits and restores stderr when idle for IDLE_TIMEOUT_SECS.
#[cfg(unix)]
fn reader_loop(read_fd: i32, orig_fd: i32, state: &CaptureState) {
    let mut buf = vec![0u8; 4096];
    let mut partial = String::new();

    // Set read_fd to non-blocking so we can check idle timeout.
    unsafe {
        let flags = libc::fcntl(read_fd, libc::F_GETFL);
        libc::fcntl(read_fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
    }

    loop {
        // Check idle timeout.
        if state.idle_secs() > IDLE_TIMEOUT_SECS {
            // Restore original stderr and exit.
            unsafe {
                libc::dup2(orig_fd, 2);
                libc::close(orig_fd);
                libc::close(read_fd);
            }
            state.active.store(false, Ordering::Relaxed);
            return;
        }

        let n = unsafe { libc::read(read_fd, buf.as_mut_ptr().cast::<libc::c_void>(), buf.len()) };

        match n.cmp(&0) {
            std::cmp::Ordering::Greater => {
                let bytes = &buf[..n as usize];

                // Tee to original stderr so terminal still works.
                unsafe {
                    libc::write(orig_fd, bytes.as_ptr().cast::<libc::c_void>(), n as usize);
                }

                // Split into lines and push to buffer.
                let chunk = String::from_utf8_lossy(bytes);
                partial.push_str(&chunk);
                while let Some(pos) = partial.find('\n') {
                    let line = partial[..pos].to_string();
                    partial.drain(..=pos);
                    let clean = strip_ansi(&line);
                    if !clean.is_empty() {
                        state.push_line(clean);
                    }
                }
            }
            std::cmp::Ordering::Equal => {
                // EOF — pipe write end closed (process exiting).
                break;
            }
            std::cmp::Ordering::Less => {
                // EAGAIN (non-blocking, no data) — sleep briefly.
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    }

    // Cleanup.
    unsafe {
        libc::close(read_fd);
        libc::close(orig_fd);
    }
    state.active.store(false, Ordering::Relaxed);
}

/// Strip ANSI escape sequences (colors, bold, etc).
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            // Skip until we hit a letter (the terminator of the escape sequence).
            for c2 in chars.by_ref() {
                if c2.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_ansi() {
        assert_eq!(strip_ansi("\x1b[1mBOLD\x1b[0m"), "BOLD");
        assert_eq!(strip_ansi("\x1b[31;1mRED\x1b[0m"), "RED");
        assert_eq!(strip_ansi("no escapes"), "no escapes");
        assert_eq!(strip_ansi(""), "");
    }

    #[test]
    fn test_capture_state_ring_buffer() {
        let state = CaptureState::new();
        for i in 0..1500 {
            state.push_line(format!("line {i}"));
        }
        let lines = state.get_lines();
        assert_eq!(lines.len(), MAX_LINES);
        assert_eq!(lines[0], "line 500");
        assert_eq!(lines[MAX_LINES - 1], "line 1499");
    }
}

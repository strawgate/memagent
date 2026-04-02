//! Stderr capture for the diagnostics dashboard.
//!
//! Started when the diagnostics server starts. Then:
//! 1. `dup(2)` saves the original stderr fd
//! 2. `pipe()` creates a pipe
//! 3. `dup2(write_fd, 2)` redirects stderr to the pipe
//! 4. A reader thread tees pipe data to the original fd + 1 MiB ring buffer
//!
//! The buffer evicts the oldest lines when full, keeping the most recent ~1 MiB
//! of log output available for `/api/logs`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const MAX_BYTES: usize = 1024 * 1024; // 1 MiB

struct LogBuf {
    lines: VecDeque<String>,
    total_bytes: usize,
}

impl LogBuf {
    fn new() -> Self {
        Self {
            lines: VecDeque::new(),
            total_bytes: 0,
        }
    }

    fn push(&mut self, line: String) {
        let line_bytes = line.len() + 1; // +1 accounts for the stripped newline
        // Drop lines that would alone exceed the cap — they can never fit.
        if line_bytes > MAX_BYTES {
            return;
        }
        // Evict oldest lines until there is room for the new one.
        while self.total_bytes + line_bytes > MAX_BYTES {
            if let Some(removed) = self.lines.pop_front() {
                self.total_bytes = self.total_bytes.saturating_sub(removed.len() + 1);
            } else {
                break;
            }
        }
        self.total_bytes += line_bytes;
        self.lines.push_back(line);
    }

    fn get_lines(&self) -> Vec<String> {
        self.lines.iter().cloned().collect()
    }
}

/// Shared state between the reader thread and the HTTP handler.
struct CaptureState {
    buf: Mutex<LogBuf>,
    active: AtomicBool,
}

impl CaptureState {
    fn new() -> Self {
        Self {
            buf: Mutex::new(LogBuf::new()),
            active: AtomicBool::new(false),
        }
    }

    fn push_line(&self, line: String) {
        if let Ok(mut buf) = self.buf.lock() {
            buf.push(line);
        }
    }

    fn get_lines(&self) -> Vec<String> {
        match self.buf.lock() {
            Ok(buf) => buf.get_lines(),
            Err(_) => vec![],
        }
    }
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

    /// Start capturing stderr. Called once by the diagnostics server at startup.
    /// Safe to call multiple times — only the first call takes effect.
    pub fn start(&self) -> std::io::Result<()> {
        #[cfg(unix)]
        return self.start_unix();
        #[cfg(not(unix))]
        Ok(())
    }

    /// Returns all buffered log lines (up to ~1 MiB worth).
    pub fn get_logs(&self) -> Vec<String> {
        self.state.get_lines()
    }

    /// Is capture currently active?
    pub fn is_active(&self) -> bool {
        self.state.active.load(Ordering::Relaxed)
    }

    #[cfg(unix)]
    fn start_unix(&self) -> std::io::Result<()> {
        // Only one thread should set this up.
        if self
            .state
            .active
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return Ok(()); // already started
        }

        unsafe {
            // Save original stderr.
            let orig = libc::dup(2);
            if orig < 0 {
                self.state.active.store(false, Ordering::Relaxed);
                return Err(std::io::Error::last_os_error());
            }

            // Create pipe.
            let mut fds = [0i32; 2];
            if libc::pipe(fds.as_mut_ptr()) != 0 {
                libc::close(orig);
                self.state.active.store(false, Ordering::Relaxed);
                return Err(std::io::Error::last_os_error());
            }
            let (read_fd, write_fd) = (fds[0], fds[1]);

            // Redirect stderr to write end of pipe.
            if libc::dup2(write_fd, 2) < 0 {
                libc::close(read_fd);
                libc::close(write_fd);
                libc::close(orig);
                self.state.active.store(false, Ordering::Relaxed);
                return Err(std::io::Error::last_os_error());
            }
            libc::close(write_fd); // fd 2 now holds the write end

            // Spawn reader thread. If spawn fails, restore fd 2 so stderr still works.
            let state = Arc::clone(&self.state);
            let spawn_result = std::thread::Builder::new()
                .name("stderr-capture".into())
                .spawn(move || {
                    reader_loop(read_fd, orig, &state);
                });
            if let Err(e) = spawn_result {
                libc::dup2(orig, 2);
                libc::close(read_fd);
                libc::close(orig);
                self.state.active.store(false, Ordering::Relaxed);
                return Err(e);
            }
        }
        Ok(())
    }
}

/// Reader thread: blocking reads from pipe, tees to original stderr, pushes to buffer.
/// Runs until EOF (process exit).
#[cfg(unix)]
fn reader_loop(read_fd: i32, orig_fd: i32, state: &CaptureState) {
    let mut buf = vec![0u8; 4096];
    // Accumulate raw bytes so multi-byte UTF-8 sequences split across reads
    // are decoded correctly per complete line, not per chunk.
    let mut partial: Vec<u8> = Vec::new();

    loop {
        let n =
            unsafe { libc::read(read_fd, buf.as_mut_ptr().cast::<libc::c_void>(), buf.len()) };

        if n < 0 {
            // EINTR: signal interrupted the read — retry.
            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            if errno == libc::EINTR {
                continue;
            }
            // Any other error: pipe broken or process exiting.
            break;
        }
        if n == 0 {
            // EOF — write end of pipe was closed (process exiting).
            break;
        }

        let bytes = &buf[..n as usize];

        // Tee to original stderr so terminal still works.
        unsafe {
            libc::write(orig_fd, bytes.as_ptr().cast::<libc::c_void>(), n as usize);
        }

        // Accumulate and split on newline bytes; decode each complete line once.
        partial.extend_from_slice(bytes);
        while let Some(pos) = partial.iter().position(|&b| b == b'\n') {
            let line_bytes = &partial[..pos];
            let line = String::from_utf8_lossy(line_bytes);
            let clean = strip_ansi(&line);
            if !clean.is_empty() {
                state.push_line(clean);
            }
            partial.drain(..=pos);
        }
    }

    // Restore stderr to the original fd before closing it, so any eprintln!()
    // calls after this thread exits still go to the terminal rather than the
    // now-broken pipe.
    unsafe {
        libc::dup2(orig_fd, 2);
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

#[cfg(kani)]
mod verification {
    use super::*;

    #[kani::proof]
    #[kani::unwind(9)]
    fn strip_ansi_preserves_non_escape_chars() {
        // Prove: for inputs ≤8 bytes with no \x1b, output == input
        let len: usize = kani::any();
        kani::assume(len <= 8);
        let mut bytes = [0u8; 8];
        for i in 0..len {
            bytes[i] = kani::any();
            kani::assume(bytes[i] != 0x1b); // no escape chars
        }
        let input = std::str::from_utf8(&bytes[..len]);
        if let Ok(s) = input {
            let result = strip_ansi(s);
            assert_eq!(result, s);
        }
    }

    #[kani::proof]
    #[kani::unwind(13)]
    fn strip_ansi_removes_escapes() {
        // Prove: output never contains \x1b
        let len: usize = kani::any();
        kani::assume(len <= 12);
        let mut bytes = [0u8; 12];
        for i in 0..len {
            bytes[i] = kani::any();
        }
        let input = std::str::from_utf8(&bytes[..len]);
        if let Ok(s) = input {
            let result = strip_ansi(s);
            assert!(!result.contains('\x1b'));
        }
    }
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
    fn test_ring_buffer_byte_cap() {
        let mut buf = LogBuf::new();
        // Each line is "line XXXX" = 9 chars, +1 for newline = 10 bytes each.
        // Push enough to exceed 1 MiB.
        let line_bytes = 10usize;
        let n = MAX_BYTES / line_bytes + 500;
        for i in 0..n {
            buf.push(format!("line {:04}", i));
        }
        // Byte total must stay within the cap.
        assert!(buf.total_bytes <= MAX_BYTES, "total_bytes {} > MAX_BYTES", buf.total_bytes);
        assert!(!buf.lines.is_empty());
        // Most-recent line should be the last one pushed.
        assert_eq!(buf.lines.back().unwrap(), &format!("line {:04}", n - 1));
    }

    #[test]
    fn test_ring_buffer_single_oversized_line() {
        // A single line larger than MAX_BYTES is dropped to enforce the cap.
        let mut buf = LogBuf::new();
        let big = "x".repeat(MAX_BYTES + 1);
        buf.push(big);
        assert_eq!(buf.lines.len(), 0);
        assert_eq!(buf.total_bytes, 0);
    }

    #[test]
    fn test_capture_state_push_get() {
        let state = CaptureState::new();
        state.push_line("hello".into());
        state.push_line("world".into());
        let lines = state.get_lines();
        assert_eq!(lines, vec!["hello", "world"]);
    }
}

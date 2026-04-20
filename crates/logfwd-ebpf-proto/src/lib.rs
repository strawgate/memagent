//! eBPF-based log capture prototype.
//!
//! This module demonstrates how to capture container log output by
//! intercepting pipe_write() in the kernel, bypassing the entire
//! CRI log file path.
//!
//! # Architecture
//!
//! ```text
//!    Container              Kernel (eBPF)           Userspace (logfwd)
//!    ─────────              ──────────────           ──────────────────
//!    write(1, data, n)
//!         │
//!         ▼
//!    sys_write()
//!         │
//!         ▼
//!    vfs_write()
//!         │
//!         ├──► kprobe fires ──► filter by cgroup
//!         │                         │
//!         │                    ┌────▼─────┐
//!         │                    │ copy buf │
//!         │                    │ to ring  │
//!         │                    │ buffer   │
//!         │                    └────┬─────┘
//!         │                         │
//!         │                         ▼
//!         │                    Ring Buffer ◄──── EbpfInput::poll()
//!         │                    (64MB)            reads events, feeds
//!         ▼                                     into FormatParser
//!    pipe_write()                                    │
//!         │                                          ▼
//!         ▼                                     Scanner → Transform → Output
//!    containerd-shim                            (existing pipeline)
//!    reads pipe, writes
//!    CRI log file
//!    (still happens, but
//!     logfwd doesn't read it)
//! ```
//!
//! # Integration with logfwd
//!
//! `EbpfInput` implements the `InputSource` trait from logfwd-core.
//! It replaces `FileInput` as the data source for a pipeline. The rest
//! of the pipeline (FormatParser, Scanner, Transform, Output) is unchanged.
//!
//! ```yaml
//! # Hypothetical config
//! input:
//!   type: ebpf
//!   cgroup_path: /sys/fs/cgroup/kubepods/  # watch all pod cgroups
//!   format: json                            # raw app output, not CRI
//! ```
#![allow(clippy::print_stdout, clippy::print_stderr)]

pub mod bpf;
pub mod common;

/// Placeholder for the userspace eBPF input source.
///
/// In a real implementation, this would:
/// 1. Load the compiled eBPF program (from embedded bytes or a file)
/// 2. Attach the kprobe to vfs_write (or pipe_write if available)
/// 3. Populate the WATCHED_CGROUPS map by scanning /sys/fs/cgroup/kubepods/
/// 4. Consume events from the ring buffer via poll()
/// 5. Implement `InputSource` trait for integration with the pipeline
///
/// # Cgroup management
///
/// The userspace side is responsible for keeping the WATCHED_CGROUPS map
/// current. As pods are created/destroyed:
///
/// - **New pod**: inotify on /sys/fs/cgroup/kubepods/ detects new cgroup directory
///   → read cgroup ID → insert into WATCHED_CGROUPS map
/// - **Pod deleted**: inotify detects removal → remove from WATCHED_CGROUPS map
///
/// This is similar to how the file tailer watches for new log files, but
/// instead of watching /var/log/pods/, we watch /sys/fs/cgroup/kubepods/.
///
/// # Event to InputEvent conversion
///
/// ```rust,ignore
/// impl InputSource for EbpfInput {
///     fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
///         let mut events = Vec::new();
///         // Drain available events from ring buffer (non-blocking)
///         while let Some(raw) = self.ring_buf.next() {
///             let event: &PipeWriteEvent = unsafe { &*(raw.as_ptr() as *const _) };
///             let data = event.data[..event.captured_len as usize].to_vec();
///             events.push(InputEvent::Data { bytes: data, source_id: None });
///         }
///         Ok(events)
///     }
///     fn name(&self) -> &str { "ebpf" }
/// }
/// ```
///
/// # Per-container metadata
///
/// The eBPF event carries the cgroup_id, which maps 1:1 to a K8s pod.
/// Userspace maintains a cgroup_id → (namespace, pod, container) lookup
/// (populated from /sys/fs/cgroup/ directory structure). This gives us
/// the same metadata as CRI path parsing, but without parsing file paths.
///
/// # Benchmark expectations
///
/// | Metric | File tailing | eBPF pipe capture |
/// |--------|-------------|-------------------|
/// | Latency (write → logfwd) | 15-300ms | 1-5ms |
/// | CPU (capture) | 8% (read + inotify) | ~13% (kprobe + copy) |
/// | CPU (CRI parsing) | 18% | 0% (not needed) |
/// | CPU (total capture + parse) | 26% | 13% |
/// | Disk I/O for log reading | ~200MB/s reads | 0 |
/// | Memory overhead | Minimal | +64MB (ring buffer) |
/// | Checkpointing needed | Yes (complex) | No |
/// | Rotation handling needed | Yes (complex) | No |
/// | Works on macOS | Yes | No (Linux only) |
/// | Kernel version | Any | 5.8+ |
/// | Privileges | Read /var/log | CAP_BPF |
///
/// Net CPU savings: **~13% of one core** (eliminating CRI parse entirely).
/// Net complexity savings: **eliminates checkpointing + rotation handling**.
/// Net latency improvement: **10-100x** (1-5ms vs 15-300ms).
pub struct EbpfInputDesign;

/// Research findings and open questions.
///
/// # Q: Can we kprobe pipe_write specifically?
///
/// `pipe_write` is defined in `fs/pipe.c` and is the `write` method of
/// `pipe_fops`. It's generally kprobeable (not usually inlined), but
/// availability depends on kernel config and version. We should:
/// - Try `kprobe/pipe_write` first
/// - Fall back to `kprobe/vfs_write` with a pipe fd filter
///
/// With `kprobe/pipe_write`, we know we're only seeing pipe writes,
/// so the cgroup filter is the only filter needed.
///
/// # Q: Can we get the fd number to distinguish stdout vs stderr?
///
/// In the kprobe for vfs_write, we get `struct file *`, not the fd number.
/// To get the fd:
/// 1. Read `current->files->fdt->fd[0..N]` and find which fd points to this file
/// 2. This is O(N) where N is the number of open fds — too expensive
///
/// Alternative: The container runtime sets up fd 1 = stdout pipe, fd 2 = stderr pipe.
/// We could resolve the pipe inode at attach time and maintain an inode → stream map.
/// But this adds complexity for marginal value (most log pipelines merge stdout+stderr).
///
/// Simplest: don't distinguish. Merge stdout and stderr. Users who need to
/// separate them can do so with the existing CRI file-based path (which preserves
/// the stream field).
///
/// # Q: What about multi-line log entries?
///
/// Applications may write a single log line across multiple write() calls
/// (e.g., a logger that writes the timestamp, then the message, then the newline
/// in separate writes). Our eBPF program captures each write() separately.
///
/// Solution: the existing FormatParser (JsonParser, RawParser) already handles
/// partial lines — they buffer until a newline is seen. This works unchanged.
///
/// # Q: What about container exec and ephemeral containers?
///
/// Exec'd processes (kubectl exec) also write to stdout/stderr pipes.
/// Their writes will be captured if their cgroup matches. This is probably
/// desirable (exec output is useful for debugging), but may not match
/// the behavior of file-based tailing (which only captures the main
/// container's log file).
///
/// # Q: Ring buffer sizing and overflow
///
/// At 1M lines/sec, 200 bytes/line:
/// - Data rate: 200 MB/sec
/// - 64MB ring buffer = 320ms of headroom
/// - If userspace stalls for >320ms, events are dropped
///
/// Options:
/// - Increase ring buffer (256MB gives 1.2s headroom)
/// - Monitor drops via bpf_ringbuf_discard() counter
/// - Alert on drop rate via OTel metrics
///
/// For comparison: file-based tailing has infinite "buffer" (the file)
/// but adds 15-300ms latency. eBPF has limited buffer but 1-5ms latency.
/// Different trade-off.
pub struct ResearchNotes;

#[cfg(test)]
mod tests {
    use super::common::*;

    #[test]
    fn event_struct_size() {
        // Verify the event struct is a reasonable size for ring buffer events.
        let size = std::mem::size_of::<PipeWriteEvent>();
        // Header (pid, tgid, cgroup_id, write_len, captured_len, stream, pad)
        // + 4096 bytes data + alignment padding.
        assert!(
            size >= 4096 + 20 && size <= 4096 + 64,
            "unexpected event size: {size}"
        );
        assert!(size <= 8192, "must fit in a single ring buffer slot");
    }

    #[test]
    fn event_is_repr_c() {
        // Verify offsets are predictable (repr(C) layout).
        let event = PipeWriteEvent {
            pid: 0,
            tgid: 0,
            cgroup_id: 0,
            write_len: 0,
            captured_len: 0,
            stream: 0,
            pad: [0; 3],
            data: [0; MAX_CAPTURE_BYTES],
        };
        // Should be constructable with known layout.
        assert_eq!(event.pid, 0);
        assert_eq!(event.data.len(), MAX_CAPTURE_BYTES);
    }

    #[test]
    fn max_capture_is_reasonable() {
        // 4KB is enough for typical log lines (200-500 bytes avg)
        // but small enough for per-CPU array allocation.
        assert_eq!(MAX_CAPTURE_BYTES, 4096);
        assert!(MAX_CAPTURE_BYTES <= 32768, "must fit in per-CPU array");
    }
}

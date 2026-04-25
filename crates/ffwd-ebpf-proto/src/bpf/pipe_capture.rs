//! eBPF kernel program: intercept pipe_write() for container stdout/stderr.
//!
//! This captures container log output BEFORE containerd-shim reads it and
//! writes it to the CRI log file. The data we see here is the raw application
//! output — no CRI timestamp/stream/flags formatting.
//!
//! # Kernel write path for container stdout
//!
//! ```text
//! Container process: write(1 /*stdout*/, "log line\n", 9)
//!     → sys_write()
//!         → vfs_write()
//!             → pipe_write()       ← WE ATTACH HERE
//!                 → data lands in pipe buffer
//!                     → containerd-shim: read() from pipe
//!                         → shim formats as CRI: "2024-01-15T... stdout F log line"
//!                             → shim: vfs_write() to /var/log/pods/.../0.log
//! ```
//!
//! By hooking pipe_write(), we get the raw "log line\n" before any CRI
//! formatting is applied. This means:
//! - No CRI timestamp parsing needed
//! - No partial line reassembly (CRI "P" flag) needed
//! - We see exactly what the application wrote
//!
//! # Filtering strategy
//!
//! We only want writes from container processes, not every pipe on the system.
//! Filter chain:
//!
//! 1. **File descriptor check**: only fd 1 (stdout) or fd 2 (stderr)
//!    - Eliminates all non-stdio pipe writes (inter-process pipes, etc.)
//!    - Cheap: single register comparison
//!
//! 2. **Cgroup check**: only processes in K8s pod cgroups
//!    - bpf_get_current_cgroup_id() returns the v2 cgroup ID
//!    - We maintain a BPF hashmap of watched cgroup IDs
//!    - Userspace populates this map by scanning /sys/fs/cgroup/kubepods/
//!    - Eliminates host processes, system services, containerd itself
//!
//! 3. **Pipe check** (optional): verify the fd is actually a pipe
//!    - Read struct file from the task's fd table
//!    - Check f_op points to pipe operations
//!    - More expensive; skip if cgroup filter is sufficient
//!
//! # Attach point: vfs_write vs pipe_write
//!
//! pipe_write is an internal function that may not be kprobeable on all
//! kernels (it's sometimes inlined). Safer to attach to vfs_write and
//! filter for pipe file descriptors. The trade-off:
//!
//! - vfs_write: always kprobeable, but fires for ALL writes (need more filtering)
//! - pipe_write: more targeted, but may not be available
//!
//! Strategy: try pipe_write first, fall back to vfs_write with pipe fd filter.
//!
//! # Data capture
//!
//! vfs_write(struct file *file, const char __user *buf, size_t count, loff_t *pos)
//!
//! We use bpf_probe_read_user() to copy `buf` into a per-CPU array (to avoid
//! the 512-byte stack limit), then submit to a ring buffer.
//!
//! # eBPF program (pseudo-Aya)
//!
//! ```rust,ignore
//! use aya_ebpf::{macros::kprobe, programs::ProbeContext, maps::{RingBuf, HashMap, PerCpuArray}};
//! use aya_ebpf::helpers::{bpf_get_current_pid_tgid, bpf_get_current_cgroup_id, bpf_probe_read_user};
//!
//! #[map]
//! static EVENTS: RingBuf = RingBuf::with_byte_size(64 * 1024 * 1024, 0); // 64MB
//!
//! #[map]
//! static WATCHED_CGROUPS: HashMap<u64, u8> = HashMap::with_max_entries(4096, 0);
//!
//! #[map]
//! static HEAP: PerCpuArray<PipeWriteEvent> = PerCpuArray::with_max_entries(1, 0);
//!
//! #[kprobe]
//! fn capture_pipe_write(ctx: ProbeContext) -> u32 {
//!     // arg0: struct file *file
//!     // arg1: const char __user *buf
//!     // arg2: size_t count
//!
//!     // --- Filter 1: cgroup ---
//!     let cgroup_id = unsafe { bpf_get_current_cgroup_id() };
//!     if WATCHED_CGROUPS.get(&cgroup_id).is_none() {
//!         return 0; // not a watched container
//!     }
//!
//!     // --- Filter 2: check if this is stdout/stderr ---
//!     // We could check the fd number, but in the kprobe for vfs_write we
//!     // get the struct file*, not the fd number. To get the fd:
//!     // - Read current task's files_struct
//!     // - Walk the fd table to find which fd maps to this struct file*
//!     // This is expensive. Alternative: check if the file is a pipe by
//!     // reading f_inode->i_pipe (non-null for pipes).
//!     //
//!     // Simpler approach: just capture all writes from watched cgroups.
//!     // The volume is low (only container process writes) and userspace
//!     // can further filter by fd if needed.
//!
//!     let buf: *const u8 = ctx.arg(1).unwrap_or(core::ptr::null());
//!     let count: usize = ctx.arg::<usize>(2).unwrap_or(0);
//!     if buf.is_null() || count == 0 {
//!         return 0;
//!     }
//!
//!     // --- Capture data ---
//!     let event = unsafe { HEAP.get_ptr_mut(0) };
//!     if event.is_none() { return 0; }
//!     let event = unsafe { &mut *event.unwrap() };
//!
//!     let pid_tgid = bpf_get_current_pid_tgid();
//!     event.pid = pid_tgid as u32;
//!     event.tgid = (pid_tgid >> 32) as u32;
//!     event.cgroup_id = cgroup_id;
//!     event.write_len = count as u32;
//!     let capture = count.min(MAX_CAPTURE_BYTES);
//!     event.captured_len = capture as u32;
//!     event.stream = 0; // would need fd resolution for stdout vs stderr
//!
//!     unsafe {
//!         bpf_probe_read_user(
//!             event.data.as_mut_ptr() as *mut _,
//!             capture as u32,
//!             buf as *const _,
//!         );
//!     }
//!
//!     // Submit to ring buffer
//!     EVENTS.output(event, 0);
//!     0
//! }
//! ```
//!
//! # Performance analysis
//!
//! Cost per container write():
//! - bpf_get_current_cgroup_id(): ~20ns
//! - HashMap lookup: ~30ns
//! - bpf_probe_read_user() for 200 bytes: ~50ns
//! - Ring buffer submit: ~30ns
//! - Total: ~130ns per captured write
//!
//! At 1M lines/sec (200 bytes avg):
//! - eBPF overhead: 130ms/sec = 13% of one CPU core
//! - Ring buffer throughput: 200MB/sec into 64MB buffer = 320ms headroom
//! - Userspace read: trivial (memory-mapped ring buffer, no syscall)
//!
//! Compared to file tailing:
//! - File tailing: 8% CPU (read syscalls + inotify) + 18% (CRI parsing)
//! - eBPF: ~13% CPU but NO CRI parsing needed = net savings of ~13%
//!
//! # What we skip vs file tailing
//!
//! | Step                          | File tailing | eBPF pipe_write |
//! |-------------------------------|-------------|-----------------|
//! | containerd-shim reads pipe    | happens     | happens (we intercept before) |
//! | CRI format: add timestamp     | happens     | happens (but we don't see it) |
//! | CRI format: add stream/flags  | happens     | happens (but we don't see it) |
//! | Write to /var/log/pods/       | happens     | happens (but we don't read it) |
//! | fsync                         | happens     | happens (but we don't wait)    |
//! | inotify detection             | needed      | NOT needed                    |
//! | File read()                   | needed      | NOT needed                    |
//! | CRI line parsing              | needed      | NOT needed                    |
//! | Partial line reassembly       | needed      | NOT needed                    |
//! | File offset checkpointing     | needed      | NOT needed                    |
//! | File rotation handling        | needed      | NOT needed                    |
//!
//! # Limitations
//!
//! 1. Data is still written to disk by containerd-shim (we don't prevent it)
//! 2. If ring buffer overflows, events are silently dropped
//! 3. Requires Linux 5.8+ (ring buffer) and CAP_BPF
//! 4. Cgroup v2 required for bpf_get_current_cgroup_id()
//! 5. Can't distinguish stdout vs stderr without fd resolution (expensive)
//! 6. Max 4KB capture per write (larger writes are truncated)
//! 7. Short-lived container processes may write before we add their cgroup to the watch map

pub mod design_notes {
    //! This module exists only for documentation. The actual eBPF program
    //! would be compiled separately using `cargo +nightly build --target bpfel-unknown-none`.
}

//! eBPF kernel program: intercept vfs_write to capture container pipe writes.
//!
//! Attach point: kprobe on vfs_write (pipe_write not kprobeable on all kernels).
//! Filters by PID exclusion list to avoid capturing our own output.
//! Uses ring buffer for zero-copy kernel→userspace data transfer.
//!
//! Build: cargo +nightly build --target bpfel-unknown-none -Z build-std=core --release

#![no_std]
#![no_main]
#![allow(dangerous_implicit_autorefs)]

use aya_ebpf::{
    helpers::{bpf_get_current_cgroup_id, bpf_get_current_pid_tgid, bpf_probe_read_user_buf},
    macros::{kprobe, map},
    maps::{Array, HashMap, RingBuf},
    programs::ProbeContext,
};
use pipe_capture_common::{PipeEvent, MAX_DATA};

/// Ring buffer for kernel→userspace event transfer. 64MB provides ~320ms
/// of headroom at 200MB/s data rate.
#[map]
static EVENTS: RingBuf = RingBuf::with_byte_size(64 * 1024 * 1024, 0);

/// PIDs to exclude (our own process, to prevent recursive capture).
#[map]
static EXCLUDE_PIDS: HashMap<u32, u8> = HashMap::with_max_entries(64, 0);

/// Counters: [0]=writes seen, [1]=submitted, [2]=read errors, [3]=ring full (drops).
#[map]
static COUNTERS: Array<u64> = Array::with_max_entries(4, 0);

#[kprobe]
pub fn pipe_write_probe(ctx: ProbeContext) -> u32 {
    match try_pipe_write(&ctx) {
        Ok(()) => 0,
        Err(_) => 0,
    }
}

#[inline(always)]
fn inc_counter(idx: u32) {
    // SAFETY: `idx` is one of this program's fixed counter slots; if the BPF map
    // lookup succeeds, aya returns a verifier-checked pointer to that slot.
    if let Some(val) = unsafe { COUNTERS.get_ptr_mut(idx) } {
        // SAFETY: the pointer came from the successful BPF map lookup above and
        // remains valid for this program invocation.
        unsafe { *val += 1 };
    }
}

fn try_pipe_write(ctx: &ProbeContext) -> Result<(), i64> {
    // vfs_write(struct file *file, const char __user *buf, size_t count, loff_t *pos)
    let buf: *const u8 = ctx.arg(1).ok_or(1i64)?;
    let count: usize = ctx.arg(2).ok_or(1i64)?;
    if count == 0 {
        return Ok(());
    }

    let pid_tgid = bpf_get_current_pid_tgid();
    let tgid = (pid_tgid >> 32) as u32;

    // Filter: exclude our own process to prevent recursive capture.
    // SAFETY: `tgid` is a plain scalar key and the read-only BPF map lookup does
    // not retain references beyond this expression.
    if unsafe { EXCLUDE_PIDS.get(&tgid).is_some() } {
        return Ok(());
    }

    inc_counter(0); // total writes seen

    // SAFETY: bpf_get_current_cgroup_id is valid in kprobe program context.
    let cgroup_id = unsafe { bpf_get_current_cgroup_id() };
    let capture_len = if count > MAX_DATA { MAX_DATA } else { count };

    // Reserve ring buffer space. If full, skip (data dropped).
    match EVENTS.reserve::<PipeEvent>(0) {
        Some(mut entry) => {
            let event = entry.as_mut_ptr();
            // SAFETY: `event` points to RingBuf-reserved PipeEvent storage;
            // every field read by userspace is initialized before submit.
            unsafe {
                (*event).pid = pid_tgid as u32;
                (*event).tgid = tgid;
                (*event).cgroup_id = cgroup_id;
                (*event).len = count as u32;
                (*event).captured = capture_len as u32;

                let dst = &mut (*event).data;
                match bpf_probe_read_user_buf(buf, &mut dst[..capture_len]) {
                    Ok(()) => {
                        inc_counter(1); // submitted
                        entry.submit(0);
                    }
                    Err(_) => {
                        inc_counter(2); // read failed
                        entry.discard(0);
                    }
                }
            }
        }
        None => {
            inc_counter(3); // ring buffer full — dropped
        }
    }

    Ok(())
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}

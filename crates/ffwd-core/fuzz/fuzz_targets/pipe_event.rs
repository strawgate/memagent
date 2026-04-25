//! Fuzz the PipeEvent struct deserialization.
//!
//! Simulates corrupted data arriving from the eBPF ring buffer.
//! The userspace consumer casts raw bytes to PipeEvent — what if:
//! - The data is shorter than sizeof(PipeEvent)?
//! - captured_len > MAX_DATA?
//! - captured_len > len?
//! - The data field contains arbitrary bytes?

#![no_main]
use libfuzzer_sys::fuzz_target;

// Re-define PipeEvent here to avoid depending on the ebpf-proto crate
// (which may have different compilation requirements).
const MAX_DATA: usize = 4096;

#[repr(C)]
#[derive(Clone, Copy)]
struct PipeEvent {
    pid: u32,
    tgid: u32,
    cgroup_id: u64,
    len: u32,
    captured: u32,
    data: [u8; MAX_DATA],
}

fuzz_target!(|data: &[u8]| {
    let event_size = std::mem::size_of::<PipeEvent>();

    if data.len() < event_size {
        return;
    }

    // Safe deserialization: copy into aligned buffer instead of raw pointer cast
    // (fuzz input may not be aligned to PipeEvent's alignment requirement).
    let mut buf = vec![0u8; event_size];
    buf.copy_from_slice(&data[..event_size]);
    let event = unsafe { &*(buf.as_ptr() as *const PipeEvent) };

    // Validate that accessing fields doesn't panic.
    let _pid = event.pid;
    let _tgid = event.tgid;
    let _cgroup = event.cgroup_id;
    let _len = event.len;
    let _captured = event.captured;

    // Simulate safe data extraction (what our consumer should do).
    let captured = event.captured as usize;
    if captured <= MAX_DATA {
        let safe_data = &event.data[..captured];
        // Try to interpret as UTF-8 (common operation in log processing).
        let _text = std::str::from_utf8(safe_data);
        // Try to find newlines.
        let _newlines = safe_data.iter().filter(|&&b| b == b'\n').count();
    }

    // Test the defensive bounds check that our consumer SHOULD do:
    let safe_captured = (event.captured as usize).min(MAX_DATA);
    let safe_len = (event.len as usize).min(safe_captured);
    let _ = &event.data[..safe_len];
});

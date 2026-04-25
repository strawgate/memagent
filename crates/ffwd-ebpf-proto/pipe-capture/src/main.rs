//! Userspace loader for the eBPF pipe capture program.
//!
//! Loads the compiled eBPF program, attaches to vfs_write, and consumes
//! events from the ring buffer. Outputs captured data to a file.
//!
//! Usage:
//!   sudo ./pipe-capture <ebpf-binary-path> [output-file]
//!
//! Build eBPF program first:
//!   cd pipe-capture-ebpf
//!   cargo +nightly build --target bpfel-unknown-none -Z build-std=core --release
//!
//! Then build userspace:
//!   cargo build --release
//!
//! Run:
//!   sudo ./target/release/pipe-capture target/bpfel-unknown-none/release/pipe-capture

use aya::maps::{HashMap, RingBuf};
use aya::programs::KProbe;
use aya::Ebpf;
use pipe_capture_common::PipeEvent;
use std::fs::File;
use std::io::Write;
use std::time::{Duration, Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ebpf_path = std::env::args().nth(1).unwrap_or_else(|| {
        "target/bpfel-unknown-none/release/pipe-capture".to_string()
    });
    let output_path = std::env::args()
        .nth(2)
        .unwrap_or("/tmp/ebpf-capture.log".to_string());

    eprintln!("Loading eBPF from {ebpf_path}...");
    let ebpf_bytes = std::fs::read(&ebpf_path)?;
    let mut ebpf = Ebpf::load(&ebpf_bytes)?;

    // Exclude our own PID to prevent recursive capture.
    let my_pid = std::process::id();
    let mut exclude: HashMap<_, u32, u8> =
        HashMap::try_from(ebpf.map_mut("EXCLUDE_PIDS").unwrap())?;
    exclude.insert(my_pid, 1, 0)?;

    // Attach kprobe to vfs_write.
    let prog: &mut KProbe = ebpf
        .program_mut("pipe_write_probe")
        .expect("program not found")
        .try_into()?;
    prog.load()?;
    prog.attach("vfs_write", 0)?;
    eprintln!("Attached to vfs_write (excluded pid={my_pid})");
    eprintln!("Output: {output_path}");
    eprintln!("Run a container to generate output. Ctrl-C to stop.");

    // Consume events from ring buffer.
    let mut ring = RingBuf::try_from(ebpf.map_mut("EVENTS").unwrap())?;
    let mut out = File::create(&output_path)?;

    let start = Instant::now();
    let mut events = 0u64;
    let mut bytes = 0u64;
    let mut last = Instant::now();

    loop {
        while let Some(item) = ring.next() {
            // SAFETY: the EVENTS ring buffer is written only with PipeEvent-sized
            // records by the paired eBPF program. We use `read_unaligned`
            // because ring buffer items may not satisfy PipeEvent's alignment.
            let ev = unsafe { std::ptr::read_unaligned(item.as_ptr() as *const PipeEvent) };
            events += 1;
            bytes += ev.captured as u64;
            let _ = out.write_all(&ev.data[..ev.captured as usize]);
        }
        if last.elapsed() > Duration::from_secs(1) {
            let e = start.elapsed().as_secs_f64();
            eprintln!(
                "[{e:.1}s] {events} events {:.1}MB {:.0}/s",
                bytes as f64 / 1048576.0,
                events as f64 / e,
            );
            last = Instant::now();
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

#![cfg_attr(not(target_os = "linux"), allow(clippy::print_stderr))]

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    linux::run()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("sensor-ebpf is only supported on Linux");
    std::process::exit(1);
}

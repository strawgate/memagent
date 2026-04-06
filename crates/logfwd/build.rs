/// Capture build-time metadata for rich `--version` output.
use std::process::Command;

fn git_output(args: &[&str]) -> Option<Vec<u8>> {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| o.stdout)
}

fn main() {
    // Short git hash
    let git_hash = git_output(&["rev-parse", "--short", "HEAD"]).map_or_else(
        || "unknown".to_string(),
        |o| String::from_utf8_lossy(&o).trim().to_string(),
    );
    println!("cargo:rustc-env=LOGFWD_GIT_HASH={git_hash}");

    // Dirty flag
    let dirty = git_output(&["status", "--porcelain"]).is_some_and(|o| !o.is_empty());
    if dirty {
        println!("cargo:rustc-env=LOGFWD_GIT_DIRTY=-dirty");
    } else {
        println!("cargo:rustc-env=LOGFWD_GIT_DIRTY=");
    }

    // Build date
    let date = Command::new("date")
        .args(["-u", "+%Y-%m-%d"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map_or_else(
            || "unknown".to_string(),
            |o| String::from_utf8_lossy(&o.stdout).trim().to_string(),
        );
    println!("cargo:rustc-env=LOGFWD_BUILD_DATE={date}");

    // Target triple (set by cargo)
    println!(
        "cargo:rustc-env=LOGFWD_TARGET={}",
        std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string())
    );

    // Build profile
    println!(
        "cargo:rustc-env=LOGFWD_PROFILE={}",
        std::env::var("PROFILE").unwrap_or_else(|_| "unknown".to_string())
    );

    // Only re-run if git HEAD changes (avoids rebuilds on every file edit).
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/refs/heads/");
}

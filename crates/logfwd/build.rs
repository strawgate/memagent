/// Capture build-time metadata for rich `--version` output.
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn git_output(args: &[&str]) -> Option<Vec<u8>> {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| o.stdout)
}

/// Compute UTC date as YYYY-MM-DD from the current system time.
/// Falls back to SOURCE_DATE_EPOCH if set (reproducible builds).
fn build_date() -> String {
    let epoch_secs = std::env::var("SOURCE_DATE_EPOCH")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)
        });

    // Days since Unix epoch → calendar date (proleptic Gregorian)
    let mut days = (epoch_secs / 86400) as u32;
    let mut year = 1970u32;
    loop {
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let days_in_year = if leap { 366 } else { 365 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        year += 1;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days: [u32; 12] = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 1u32;
    for &md in &month_days {
        if days < md {
            break;
        }
        days -= md;
        month += 1;
    }
    format!("{year:04}-{month:02}-{:02}", days + 1)
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

    // Build date (cross-platform; no external `date` command)
    println!("cargo:rustc-env=LOGFWD_BUILD_DATE={}", build_date());

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

    // Re-run when HEAD, refs, or the index changes.
    // .git/index tracks staged/unstaged changes so LOGFWD_GIT_DIRTY stays fresh.
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/refs/heads/");
    println!("cargo:rerun-if-changed=../../.git/index");
    // SOURCE_DATE_EPOCH override for reproducible builds
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
}

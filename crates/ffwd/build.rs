/// Capture build-time metadata for rich `--version` output.
use std::path::PathBuf;
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

fn git_string(args: &[&str]) -> String {
    git_output(args).map_or_else(
        || "unknown".to_string(),
        |o| String::from_utf8_lossy(&o).trim().to_string(),
    )
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
                .map_or(0, |d| d.as_secs())
        });

    // Days since Unix epoch -> calendar date (proleptic Gregorian)
    let mut days = (epoch_secs / 86400) as u32;
    let mut year = 1970u32;
    loop {
        let leap =
            year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
        let days_in_year = if leap { 366 } else { 365 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        year += 1;
    }
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
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
    println!(
        "cargo:rustc-env=FFWD_GIT_HASH={}",
        git_string(&["rev-parse", "--short", "HEAD"])
    );

    let dirty = git_output(&["status", "--porcelain"]).is_some_and(|o| !o.is_empty());
    println!(
        "cargo:rustc-env=FFWD_GIT_DIRTY={}",
        if dirty { "-dirty" } else { "" }
    );

    println!("cargo:rustc-env=FFWD_BUILD_DATE={}", build_date());

    println!(
        "cargo:rustc-env=FFWD_TARGET={}",
        std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string())
    );

    println!(
        "cargo:rustc-env=FFWD_PROFILE={}",
        std::env::var("PROFILE").unwrap_or_else(|_| "unknown".to_string())
    );

    // Re-run when git state changes. Compute .git path from CARGO_MANIFEST_DIR
    // so this works regardless of workspace nesting depth.
    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR")
        && let Some(git_dir) = find_git_dir(&PathBuf::from(manifest_dir))
    {
        println!("cargo:rerun-if-changed={}", git_dir.join("HEAD").display());
        println!(
            "cargo:rerun-if-changed={}",
            git_dir.join("refs/heads").display()
        );
        println!("cargo:rerun-if-changed={}", git_dir.join("index").display());
    }
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
}

/// Walk up from `start` to find the `.git` directory.
fn find_git_dir(start: &std::path::Path) -> Option<PathBuf> {
    let mut dir = start.to_path_buf();
    loop {
        let candidate = dir.join(".git");
        if candidate.is_dir() {
            return Some(candidate);
        }
        if !dir.pop() {
            return None;
        }
    }
}

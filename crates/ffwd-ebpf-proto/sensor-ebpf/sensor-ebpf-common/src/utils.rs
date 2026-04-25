use std::io;

/// Tracefs paths to probe for the sched_process_exit format file.
/// Some systems mount tracefs at `/sys/kernel/tracing`, others at
/// `/sys/kernel/debug/tracing`.
pub const SCHED_PROCESS_EXIT_FORMAT_PATHS: &[&str] = &[
    "/sys/kernel/tracing/events/sched/sched_process_exit/format",
    "/sys/kernel/debug/tracing/events/sched/sched_process_exit/format",
];

pub struct RuntimeConfig {
    pub exit_code_offset: Option<u32>,
    pub group_dead_offset: Option<u32>,
}

/// Discover `task_struct.exit_code` offset from `/sys/kernel/btf/vmlinux`.
///
/// Uses `pahole` (from the `dwarves` package) to introspect kernel BTF.
/// Returns the byte offset on success.
pub fn find_exit_code_offset() -> io::Result<u32> {
    // Try pahole — the standard tool for BTF struct layout.
    let output = std::process::Command::new("pahole")
        .args(["-C", "task_struct", "/sys/kernel/btf/vmlinux"])
        .output()
        .map_err(|e| {
            io::Error::other(format!(
                "pahole not available (install dwarves package for exit_code support): {e}"
            ))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(io::Error::other(format!(
            "pahole failed (exit {}): {}",
            output.status,
            stderr.trim()
        )));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        let trimmed = line.trim();
        // Look for: `int exit_code; /* 1234 4 */`
        if trimmed.contains("exit_code")
            && trimmed.contains("/*")
            && let Some(comment) = trimmed.split("/*").nth(1)
            && let Some(offset_str) = comment.split_whitespace().next()
            && let Ok(offset) = offset_str.parse::<u32>()
            && offset > 0
            && offset < 16384
        {
            return Ok(offset);
        }
    }

    Err(io::Error::other(
        "exit_code field not found in pahole task_struct output",
    ))
}

/// Discover `sched_process_exit.group_dead` offset from tracepoint format.
///
/// Probes multiple tracefs mount points and validates that the field size
/// is within the expected range for a boolean field (<= 4 bytes).
pub fn find_sched_process_exit_group_dead_offset() -> io::Result<Option<u32>> {
    let format_text = read_tracepoint_format()?;
    let Some(field) = parse_tracepoint_field(&format_text, "group_dead") else {
        return Ok(None);
    };
    // The kernel defines group_dead as bool (1 byte). Accept sizes up to 4
    // (some kernels may widen to int), but warn and disable on anything larger.
    if field.size == 0 || field.size > 4 {
        return Err(io::Error::other(format!(
            "sched_process_exit.group_dead has unexpected size {}; disabling",
            field.size
        )));
    }
    Ok(Some(field.offset))
}

/// Read the tracepoint format file, probing multiple tracefs mount points.
pub fn read_tracepoint_format() -> io::Result<String> {
    let mut last_err = None;
    for path in SCHED_PROCESS_EXIT_FORMAT_PATHS {
        match std::fs::read_to_string(path) {
            Ok(text) => return Ok(text),
            Err(e) => {
                last_err = Some(e);
            }
        }
    }
    Err(io::Error::other(format!(
        "failed to read sched_process_exit tracepoint format from any tracefs path (last error: {})",
        last_err.unwrap()
    )))
}

pub struct TracepointField {
    pub offset: u32,
    pub size: u32,
}

pub fn parse_tracepoint_field(format_text: &str, field_name: &str) -> Option<TracepointField> {
    for line in format_text.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with("field:") || !trimmed.contains("offset:") {
            continue;
        }
        let Some(field_decl) = trimmed
            .strip_prefix("field:")
            .and_then(|field| field.split(';').next())
            .map(str::trim)
        else {
            continue;
        };
        let Some(field_name_with_suffix) = field_decl.split_whitespace().last() else {
            continue;
        };
        // Remove array brackets if present (e.g., `comm[16]` -> `comm`).
        let actual_name = field_name_with_suffix.split('[').next().unwrap_or_default();
        if actual_name != field_name {
            continue;
        }

        let offset = trimmed
            .split("offset:")
            .nth(1)
            .and_then(|o| o.split(';').next())
            .map(str::trim)
            .and_then(|s| s.parse::<u32>().ok())?;

        let size = trimmed
            .split("size:")
            .nth(1)
            .and_then(|s| s.split(';').next())
            .map(str::trim)
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(0);

        return Some(TracepointField { offset, size });
    }
    None
}

use std::sync::OnceLock;

/// Returns (rss_bytes, cpu_user_ms, cpu_sys_ms) for the current process.
pub(crate) fn process_metrics() -> Option<(u64, u64, u64)> {
    get_process_metrics_linux().or_else(get_process_metrics_unix)
}

/// Reads /proc/self/stat to get RSS, utime and stime (Linux).
fn get_process_metrics_linux() -> Option<(u64, u64, u64)> {
    use std::fs;
    use std::io::Read;
    let mut f = fs::File::open("/proc/self/stat").ok()?;
    let mut buf = Vec::with_capacity(4096);
    f.by_ref().take(4096).read_to_end(&mut buf).ok()?;
    let stat = String::from_utf8_lossy(&buf);
    parse_proc_stat(&stat)
}

/// Fallback using getrusage (macOS, BSDs).
#[cfg(unix)]
fn get_process_metrics_unix() -> Option<(u64, u64, u64)> {
    // SAFETY: `zeroed()` is valid for `rusage` (all-zero is a valid bit pattern),
    // and `getrusage` is called with a valid `RUSAGE_SELF` flag and a valid
    // mutable pointer. No other invariants are required.
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &raw mut usage) != 0 {
            return None;
        }
        let user_ms =
            (usage.ru_utime.tv_sec as u64) * 1000 + (usage.ru_utime.tv_usec as u64) / 1000;
        let sys_ms = (usage.ru_stime.tv_sec as u64) * 1000 + (usage.ru_stime.tv_usec as u64) / 1000;
        // ru_maxrss is bytes on macOS, KB on Linux
        #[cfg(target_os = "macos")]
        let rss_bytes = usage.ru_maxrss as u64;
        #[cfg(not(target_os = "macos"))]
        let rss_bytes = (usage.ru_maxrss as u64) * 1024;
        Some((rss_bytes, user_ms, sys_ms))
    }
}

#[cfg(not(unix))]
fn get_process_metrics_unix() -> Option<(u64, u64, u64)> {
    None
}

/// Parses `/proc/self/stat` content and returns `(rss_bytes, user_ms, sys_ms)`.
pub(super) fn parse_proc_stat(stat: &str) -> Option<(u64, u64, u64)> {
    // Field 14 is utime, field 15 is stime, field 24 is rss (in pages).
    // They are space-separated, but the second field (comm) can contain spaces
    // and is enclosed in parentheses.
    let last_paren = stat.rfind(')')?;
    let after_comm = &stat[last_paren + 1..];
    let mut parts = after_comm.split_whitespace();

    // The fields in /proc/self/stat are:
    // 1: pid
    // 2: (comm)
    // -- after_comm starts here --
    // 3: state (parts[0])
    // 4: ppid (parts[1])
    // ...
    // 14: utime (parts[11])
    // 15: stime (parts[12])
    // ...
    // 24: rss (parts[21])
    let utime_ticks: u64 = parts.nth(11)?.parse().ok()?;
    let stime_ticks: u64 = parts.next()?.parse().ok()?;

    // Skip to field 24 (index 21 after_comm).
    // parts is now at index 13 (after stime).
    // To get to index 21, we need to skip 8 elements.
    let rss_pages: u64 = parts.nth(8)?.parse().ok()?;

    let ticks_per_sec = get_ticks_per_sec()?;
    let page_size = get_page_size()?;

    let user_ms = (utime_ticks * 1000) / ticks_per_sec;
    let sys_ms = (stime_ticks * 1000) / ticks_per_sec;
    let rss_bytes = rss_pages * page_size;

    Some((rss_bytes, user_ms, sys_ms))
}

pub(super) fn get_ticks_per_sec() -> Option<u64> {
    static CLK_TCK: OnceLock<Option<u64>> = OnceLock::new();
    *CLK_TCK.get_or_init(|| getconf_u64("CLK_TCK"))
}

pub(super) fn get_page_size() -> Option<u64> {
    static PAGE_SIZE: OnceLock<Option<u64>> = OnceLock::new();
    *PAGE_SIZE.get_or_init(|| getconf_u64("PAGESIZE"))
}

fn getconf_u64(name: &str) -> Option<u64> {
    #[cfg(unix)]
    {
        let key = match name {
            "CLK_TCK" => libc::_SC_CLK_TCK,
            "PAGESIZE" => libc::_SC_PAGESIZE,
            _ => return None,
        };
        // SAFETY: `sysconf` is thread-safe and we pass a valid key constant.
        let value = unsafe { libc::sysconf(key) };
        (value > 0).then_some(value as u64)
    }
    #[cfg(not(unix))]
    {
        let _ = name;
        None
    }
}

use std::time::{SystemTime, UNIX_EPOCH};

/// Minimal JSON-string escaping (backslash, double-quote, control chars).
pub(super) fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x00'..='\x1f' => {
                use std::fmt::Write;
                write!(out, "\\u{:04x}", c as u32).expect("write to String is infallible");
            }
            _ => out.push(c),
        }
    }
    out
}

pub(super) fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

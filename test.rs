fn priority_to_int(p: &str) -> Option<u8> {
    match p.to_lowercase().as_str() {
        "emerg" | "emergency" | "0" => Some(0),
        "alert" | "1" => Some(1),
        "crit" | "critical" | "2" => Some(2),
        "err" | "error" | "3" => Some(3),
        "warning" | "warn" | "4" => Some(4),
        "notice" | "5" => Some(5),
        "info" | "6" => Some(6),
        "debug" | "7" => Some(7),
        _ => None,
    }
}
fn main() {}

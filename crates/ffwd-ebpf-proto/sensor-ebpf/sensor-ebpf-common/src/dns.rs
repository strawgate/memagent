//! DNS wire-format parsing helpers (userspace only, requires `std` feature).

use std::string::String;

/// Parse DNS wire-format label-encoded bytes into a dotted domain name.
///
/// Returns `(Option<name>, Option<qtype>)`:
/// - `name` is `None` when the wire bytes cannot be decoded at all (empty input,
///   leading compression pointer, non-UTF-8 label).
/// - `qtype` is `None` when the type/class bytes are not present after the name
///   terminator (truncated capture).
///
/// A fully decoded name with a missing qtype is possible when the kernel capture
/// was truncated right after the zero-terminator.
pub fn dns_wire_to_dotted(wire: &[u8]) -> (Option<String>, Option<u16>) {
    if wire.is_empty() {
        return (None, None);
    }

    let mut name = String::with_capacity(wire.len());
    let mut pos = 0;
    while pos < wire.len() {
        let label_len = wire[pos] as usize;
        if label_len == 0 {
            pos += 1; // skip the 0-terminator
            break;
        }
        // Compression pointer or invalid label length — stop parsing.
        if label_len > 63 || pos + 1 + label_len > wire.len() {
            return (if name.is_empty() { None } else { Some(name) }, None);
        }
        if !name.is_empty() {
            name.push('.');
        }
        let label = &wire[pos + 1..pos + 1 + label_len];
        match std::str::from_utf8(label) {
            Ok(s) => name.push_str(s),
            Err(_) => return (if name.is_empty() { None } else { Some(name) }, None),
        }
        pos += 1 + label_len;
    }

    if name.is_empty() {
        // Root domain (".") — valid but unusual; treat as decodable.
        return (Some(String::from(".")), extract_qtype(wire, pos));
    }

    let qtype = extract_qtype(wire, pos);
    (Some(name), qtype)
}

/// Extract the 2-byte big-endian QTYPE at the given position, if available.
fn extract_qtype(wire: &[u8], pos: usize) -> Option<u16> {
    let bytes = wire.get(pos..)?.get(..2)?;
    Some(u16::from_be_bytes([bytes[0], bytes[1]]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_domain() {
        // www.google.com, qtype=1 (A), qclass=1 (IN)
        let wire = b"\x03www\x06google\x03com\x00\x00\x01\x00\x01";
        let (name, qtype) = dns_wire_to_dotted(wire);
        assert_eq!(name.as_deref(), Some("www.google.com"));
        assert_eq!(qtype, Some(1));
    }

    #[test]
    fn parses_aaaa_query() {
        let wire = b"\x06github\x03com\x00\x00\x1c\x00\x01";
        let (name, qtype) = dns_wire_to_dotted(wire);
        assert_eq!(name.as_deref(), Some("github.com"));
        assert_eq!(qtype, Some(28)); // AAAA
    }

    #[test]
    fn truncated_before_qtype() {
        let wire = b"\x07example\x03com\x00";
        let (name, qtype) = dns_wire_to_dotted(wire);
        assert_eq!(name.as_deref(), Some("example.com"));
        assert_eq!(qtype, None);
    }

    #[test]
    fn empty_input() {
        let (name, qtype) = dns_wire_to_dotted(b"");
        assert_eq!(name, None);
        assert_eq!(qtype, None);
    }

    #[test]
    fn truncated_label() {
        // Label says 10 bytes but only 3 follow
        let wire = b"\x0aabc";
        let (name, qtype) = dns_wire_to_dotted(wire);
        assert_eq!(name, None);
        assert_eq!(qtype, None);
    }

    #[test]
    fn compression_pointer_at_start() {
        // 0xc0 0x0c = compression pointer
        let wire = b"\xc0\x0c";
        let (name, qtype) = dns_wire_to_dotted(wire);
        assert_eq!(name, None);
        assert_eq!(qtype, None);
    }
}

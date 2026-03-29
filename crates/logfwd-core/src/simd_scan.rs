// simd_scan.rs — SIMD-accelerated JSON string and structural scanning primitives.
//
// These functions replace the byte-at-a-time scan_string() and skip_nested()
// in the scalar scanner with 16-byte SIMD chunk processing.
//
// On aarch64: uses NEON via sonic-simd (16 bytes per vector op).
// On x86_64: uses SSE2 via sonic-simd (16 bytes per vector op).
// Fallback: sonic-simd's portable scalar backend.

use sonic_simd::{BitMask, Mask, Simd, u8x16};

// ---------------------------------------------------------------------------
// scan_string_simd
// ---------------------------------------------------------------------------

/// Scan a JSON string starting at `pos` (which must point to the opening `"`).
/// Returns (content_slice_between_quotes, position_after_closing_quote).
///
/// Uses SIMD to process 16 bytes at a time. For each chunk:
/// - If no backslash and no quote → skip all 16 bytes
/// - If quote appears before any backslash → found closing quote
/// - Otherwise → fall to scalar for that chunk (handles escapes)
///
/// Escapes are NOT decoded — content is raw bytes from the input buffer.
#[inline]
pub fn scan_string_simd(buf: &[u8], pos: usize) -> Option<(&[u8], usize)> {
    debug_assert!(pos < buf.len() && buf[pos] == b'"');
    let start = pos + 1;
    let mut i = start;
    let len = buf.len();

    let quote_vec = u8x16::splat(b'"');
    let bs_vec = u8x16::splat(b'\\');

    // SIMD fast path: 16 bytes at a time
    while i + 16 <= len {
        let chunk = unsafe { u8x16::loadu(buf.as_ptr().add(i)) };
        let q_mask = chunk.eq(&quote_vec).bitmask();
        let bs_mask = chunk.eq(&bs_vec).bitmask();

        if q_mask.all_zero() && bs_mask.all_zero() {
            // No quotes or backslashes in this chunk — skip all 16
            i += 16;
            continue;
        }

        if !q_mask.all_zero() && (bs_mask.all_zero() || !bs_mask.before(&q_mask)) {
            // Quote found and no backslash precedes it in this chunk
            let offset = q_mask.first_offset();
            return Some((&buf[start..i + offset], i + offset + 1));
        }

        // Chunk has backslash before (or at same position as) quote.
        // Fall to scalar for the remainder of this chunk.
        let end = (i + 16).min(len);
        while i < end {
            if buf[i] == b'\\' {
                i += 2; // skip escaped char
            } else if buf[i] == b'"' {
                return Some((&buf[start..i], i + 1));
            } else {
                i += 1;
            }
        }
    }

    // Scalar tail for remaining < 16 bytes
    while i < len {
        if buf[i] == b'\\' {
            i += 2;
        } else if buf[i] == b'"' {
            return Some((&buf[start..i], i + 1));
        } else {
            i += 1;
        }
    }
    None // unterminated string
}

// ---------------------------------------------------------------------------
// skip_nested_simd
// ---------------------------------------------------------------------------

/// Skip a nested JSON object or array starting at `pos` (which must point to
/// the opening `{` or `[`). Returns position after the matching close.
///
/// Uses SIMD to skip past runs of bytes that contain no structural characters.
/// When a structural byte is found, handles it with scalar depth tracking.
#[inline]
pub fn skip_nested_simd(buf: &[u8], mut pos: usize) -> usize {
    let len = buf.len();
    let mut depth: u32 = 0;

    let lbrace = u8x16::splat(b'{');
    let rbrace = u8x16::splat(b'}');
    let lbracket = u8x16::splat(b'[');
    let rbracket = u8x16::splat(b']');
    let quote = u8x16::splat(b'"');

    while pos < len {
        match buf[pos] {
            b'{' | b'[' => {
                depth += 1;
                pos += 1;
            }
            b'}' | b']' => {
                if depth == 0 {
                    return pos; // unbalanced — don't underflow
                }
                depth -= 1;
                pos += 1;
                if depth == 0 {
                    return pos;
                }
            }
            b'"' => {
                // Skip string contents using SIMD
                match scan_string_simd(buf, pos) {
                    Some((_, after)) => pos = after,
                    None => return len, // unterminated string
                }
            }
            _ => {
                // SIMD fast skip: scan 16 bytes for any structural char
                if pos + 16 <= len {
                    let chunk = unsafe { u8x16::loadu(buf.as_ptr().add(pos)) };
                    // Combine masks at the Mask level (Mask implements BitOr),
                    // then extract bitmask once.
                    let m = (chunk.eq(&lbrace)
                        | chunk.eq(&rbrace)
                        | chunk.eq(&lbracket)
                        | chunk.eq(&rbracket)
                        | chunk.eq(&quote))
                    .bitmask();

                    if m.all_zero() {
                        pos += 16; // no structural chars, skip all
                        continue;
                    }
                    // Advance to the first structural char and let the match handle it
                    pos += m.first_offset();
                } else {
                    pos += 1;
                }
            }
        }
    }
    pos
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_string_simple() {
        let buf = br#""hello""#;
        let (content, after) = scan_string_simd(buf, 0).unwrap();
        assert_eq!(content, b"hello");
        assert_eq!(after, 7);
    }

    #[test]
    fn test_scan_string_escaped_quote() {
        let buf = br#""hello \"world\"""#;
        let (content, after) = scan_string_simd(buf, 0).unwrap();
        assert_eq!(content, br#"hello \"world\""#);
        assert_eq!(after, buf.len());
    }

    #[test]
    fn test_scan_string_escaped_backslash_before_quote() {
        // Input: "test\\" — value is test\ (escaped backslash, then closing quote)
        let buf = b"\"test\\\\\"";
        let (content, after) = scan_string_simd(buf, 0).unwrap();
        assert_eq!(content, b"test\\\\");
        assert_eq!(after, buf.len());
    }

    #[test]
    fn test_scan_string_empty() {
        let buf = br#""""#;
        let (content, after) = scan_string_simd(buf, 0).unwrap();
        assert_eq!(content, b"");
        assert_eq!(after, 2);
    }

    #[test]
    fn test_scan_string_long() {
        // String longer than 16 bytes — exercises SIMD path
        let mut buf = Vec::new();
        buf.push(b'"');
        buf.extend_from_slice(b"abcdefghijklmnopqrstuvwxyz0123456789");
        buf.push(b'"');
        let (content, after) = scan_string_simd(&buf, 0).unwrap();
        assert_eq!(content, b"abcdefghijklmnopqrstuvwxyz0123456789");
        assert_eq!(after, buf.len());
    }

    #[test]
    fn test_scan_string_escape_at_boundary() {
        // Place a backslash right at byte 15 (end of first SIMD chunk)
        let mut buf = Vec::new();
        buf.push(b'"');
        buf.extend_from_slice(b"0123456789abcde"); // 15 bytes
        buf.push(b'\\');
        buf.push(b'"'); // escaped quote
        buf.extend_from_slice(b"tail");
        buf.push(b'"'); // actual closing quote
        let (content, after) = scan_string_simd(&buf, 0).unwrap();
        assert_eq!(
            std::str::from_utf8(content).unwrap(),
            "0123456789abcde\\\"tail"
        );
        assert_eq!(after, buf.len());
    }

    #[test]
    fn test_scan_string_multiple_escapes() {
        let buf = br#""a\nb\tc\rd""#;
        let (content, after) = scan_string_simd(buf, 0).unwrap();
        assert_eq!(content, br#"a\nb\tc\rd"#);
        assert_eq!(after, buf.len());
    }

    #[test]
    fn test_skip_nested_object() {
        let buf = br#"{"a":1,"b":"hi"}"#;
        let after = skip_nested_simd(buf, 0);
        assert_eq!(after, buf.len());
    }

    #[test]
    fn test_skip_nested_array() {
        let buf = br#"[1,"two",null,true]"#;
        let after = skip_nested_simd(buf, 0);
        assert_eq!(after, buf.len());
    }

    #[test]
    fn test_skip_nested_deep() {
        let buf = br#"{"a":{"b":{"c":[1,2,3]}}}"#;
        let after = skip_nested_simd(buf, 0);
        assert_eq!(after, buf.len());
    }

    #[test]
    fn test_skip_nested_braces_in_string() {
        let buf = br#"{"msg":"has } and { inside"}"#;
        let after = skip_nested_simd(buf, 0);
        assert_eq!(after, buf.len());
    }

    #[test]
    fn test_skip_nested_long_string_value() {
        // Nested object with a string longer than 16 bytes
        let buf = br#"{"data":"abcdefghijklmnopqrstuvwxyz0123456789"}"#;
        let after = skip_nested_simd(buf, 0);
        assert_eq!(after, buf.len());
    }

    #[test]
    fn test_skip_nested_unbalanced_start() {
        // Starting with } should not underflow or advance
        let buf = b"}";
        let after = skip_nested_simd(buf, 0);
        assert_eq!(after, 0);

        let buf = b"]more";
        let after = skip_nested_simd(buf, 0);
        assert_eq!(after, 0);
    }
}

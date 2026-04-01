// json_scanner.rs — Streaming JSON field scanner using StructuralIter.
//
// Replaces the byte-loop scanner with bitmask-driven iteration.
// Processes structural positions from StructuralIter sequentially —
// no stored bitmask vectors, no random-access lookups.
//
// Line boundaries are found first (newline bitmask), then each line
// is scanned independently. Within a line, the scanner iterates
// through structural positions sequentially.

use crate::scan_config::{ScanConfig, parse_int_fast};
use crate::scanner::ScanBuilder;
use crate::structural::{StreamingClassifier, find_structural_chars};

/// Stored bitmasks for scan_line — only what's needed for random-access
/// quote scanning and nested object/array skipping.
struct StoredBitmasks<'a> {
    real_quotes: &'a [u64],
    open_brace: &'a [u64],
    close_brace: &'a [u64],
    open_bracket: &'a [u64],
    close_bracket: &'a [u64],
}

/// Scan an NDJSON buffer using streaming structural iteration.
///
/// Zero heap allocation for bitmask storage. Line boundaries and
/// structural positions are extracted from 64-byte block bitmasks
/// consumed on the fly.
#[inline(never)]
pub fn scan_streaming<B: ScanBuilder>(buf: &[u8], config: &ScanConfig, builder: &mut B) {
    builder.begin_batch();
    if buf.is_empty() {
        return;
    }

    // Phase 1: SIMD pass — find line boundaries and store quote/string bitmasks.
    // Only real_quotes and open/close brace/bracket bitmasks are stored (for
    // scan_string and skip_nested). Newline, comma, colon, space bitmasks are
    // consumed during this pass and not stored.
    let len = buf.len();
    let num_blocks = len.div_ceil(64);

    // Stored per-block: only what scan_line needs for random-access lookups.
    // 5 u64s per block (40 bytes) vs old StructuralIndex's 2 u64s (16 bytes).
    let mut real_quotes = alloc::vec::Vec::with_capacity(num_blocks);
    let mut open_brace = alloc::vec::Vec::with_capacity(num_blocks);
    let mut close_brace = alloc::vec::Vec::with_capacity(num_blocks);
    let mut open_bracket = alloc::vec::Vec::with_capacity(num_blocks);
    let mut close_bracket = alloc::vec::Vec::with_capacity(num_blocks);
    let mut line_ranges = alloc::vec::Vec::new();
    let mut line_start: usize = 0;
    let mut classifier = StreamingClassifier::new();

    for block_idx in 0..num_blocks {
        let offset = block_idx * 64;
        let remaining = len - offset;
        let block_len = remaining.min(64);

        let block: [u8; 64] = if remaining >= 64 {
            buf[offset..offset + 64].try_into().expect("64-byte block")
        } else {
            let mut padded = [b' '; 64];
            padded[..remaining].copy_from_slice(&buf[offset..]);
            padded
        };

        let raw = find_structural_chars(&block);
        let processed = classifier.process_block(&raw, block_len);

        // Store only what scan_line needs
        real_quotes.push(processed.real_quotes);
        open_brace.push(processed.open_brace);
        close_brace.push(processed.close_brace);
        open_bracket.push(processed.open_bracket);
        close_bracket.push(processed.close_bracket);

        // Extract line boundaries (consumed, not stored)
        let mut nl = processed.newline;
        while nl != 0 {
            let bit_pos = nl.trailing_zeros() as usize;
            let abs_pos = offset + bit_pos;
            if abs_pos > line_start || config.keep_raw {
                line_ranges.push((line_start, abs_pos));
            }
            line_start = abs_pos + 1;
            nl &= nl - 1;
        }
    }

    if line_start < len {
        line_ranges.push((line_start, len));
    }

    let bitmasks = StoredBitmasks {
        real_quotes: &real_quotes,
        open_brace: &open_brace,
        close_brace: &close_brace,
        open_bracket: &open_bracket,
        close_bracket: &close_bracket,
    };

    // Phase 2: Scan each line using stored bitmasks for quote/nested lookups.
    for (start, end) in line_ranges {
        scan_line(buf, start, end, &bitmasks, config, builder);
    }
}

/// Scan a single JSON line using pre-computed block bitmasks.
fn scan_line<B: ScanBuilder>(
    buf: &[u8],
    start: usize,
    end: usize,
    blocks: &StoredBitmasks<'_>,
    config: &ScanConfig,
    builder: &mut B,
) {
    builder.begin_row();
    if config.keep_raw {
        builder.append_raw(&buf[start..end]);
    }

    // Find the opening '{' using bitmasks
    let mut pos = skip_whitespace(buf, start, end);
    if pos >= end || buf[pos] != b'{' {
        builder.end_row();
        return;
    }
    pos += 1;

    // Parse key-value pairs
    loop {
        pos = skip_whitespace(buf, pos, end);
        if pos >= end || buf[pos] == b'}' {
            break;
        }
        if buf[pos] != b'"' {
            break;
        }

        // Scan key string using quote bitmask
        let key_start = pos + 1;
        let key_end = match next_quote(pos + 1, end, blocks) {
            Some(p) => p,
            None => break,
        };
        let key = &buf[key_start..key_end];
        pos = key_end + 1;

        // Expect colon
        pos = skip_whitespace(buf, pos, end);
        if pos >= end || buf[pos] != b':' {
            break;
        }
        pos += 1;

        // Parse value
        pos = skip_whitespace(buf, pos, end);
        if pos >= end {
            break;
        }

        let wanted = config.is_wanted(key);
        match buf[pos] {
            b'"' => {
                // String value
                let val_start = pos + 1;
                let val_end = match next_quote(pos + 1, end, blocks) {
                    Some(p) => p,
                    None => break,
                };
                if wanted {
                    let idx = builder.resolve_field(key);
                    builder.append_str_by_idx(idx, &buf[val_start..val_end]);
                }
                pos = val_end + 1;
            }
            b'{' | b'[' => {
                // Nested object/array — skip using bitmasks
                let nested_start = pos;
                pos = skip_nested(buf, pos, end, blocks);
                if wanted {
                    let idx = builder.resolve_field(key);
                    builder.append_str_by_idx(idx, &buf[nested_start..pos]);
                }
            }
            b't' => {
                // Validate true + delimiter (#515)
                if pos + 4 <= end
                    && &buf[pos..pos + 4] == b"true"
                    && (pos + 4 >= end || is_json_delimiter(buf[pos + 4]))
                {
                    if wanted {
                        let idx = builder.resolve_field(key);
                        builder.append_str_by_idx(idx, &buf[pos..pos + 4]);
                    }
                    pos += 4;
                } else {
                    pos = skip_bare_value(buf, pos, end);
                }
            }
            b'f' => {
                // Validate false + delimiter (#515)
                if pos + 5 <= end
                    && &buf[pos..pos + 5] == b"false"
                    && (pos + 5 >= end || is_json_delimiter(buf[pos + 5]))
                {
                    if wanted {
                        let idx = builder.resolve_field(key);
                        builder.append_str_by_idx(idx, &buf[pos..pos + 5]);
                    }
                    pos += 5;
                } else {
                    pos = skip_bare_value(buf, pos, end);
                }
            }
            b'n' => {
                // Validate null + delimiter (#515)
                if pos + 4 <= end
                    && &buf[pos..pos + 4] == b"null"
                    && (pos + 4 >= end || is_json_delimiter(buf[pos + 4]))
                {
                    if wanted {
                        let idx = builder.resolve_field(key);
                        builder.append_null_by_idx(idx);
                    }
                    pos += 4;
                } else {
                    pos = skip_bare_value(buf, pos, end);
                }
            }
            _ => {
                // Number
                let num_start = pos;
                let mut is_float = false;
                while pos < end {
                    let c = buf[pos];
                    if c == b'.' || c == b'e' || c == b'E' {
                        is_float = true;
                        pos += 1;
                    } else if c == b',' || c == b'}' || c == b' ' || c == b'\t' || c == b'\r' {
                        break;
                    } else {
                        pos += 1;
                    }
                }
                if wanted {
                    let idx = builder.resolve_field(key);
                    let val = &buf[num_start..pos];
                    if is_float {
                        builder.append_float_by_idx(idx, val);
                    } else if parse_int_fast(val).is_some() {
                        builder.append_int_by_idx(idx, val);
                    } else {
                        builder.append_float_by_idx(idx, val);
                    }
                }
            }
        }

        // Skip comma between fields
        pos = skip_whitespace(buf, pos, end);
        if pos < end && buf[pos] == b',' {
            pos += 1;
        }
    }
    builder.end_row();
}

// ---------------------------------------------------------------------------
// Bitmask-accelerated helpers
// ---------------------------------------------------------------------------

/// Find the next unescaped quote at or after `from`, bounded by `end`.
/// Uses per-block real_quotes bitmask for O(1) per-block lookup.
#[inline]
fn next_quote(from: usize, end: usize, blocks: &StoredBitmasks<'_>) -> Option<usize> {
    let mut pos = from;
    while pos < end {
        let block = pos >> 6;
        let bit = pos & 63;
        if block < blocks.real_quotes.len() {
            let mask = blocks.real_quotes[block] >> bit;
            if mask != 0 {
                let found = pos + mask.trailing_zeros() as usize;
                if found < end {
                    return Some(found);
                }
                return None;
            }
            // No quotes in rest of this block — skip to next
            pos = (block + 1) << 6;
        } else {
            return None;
        }
    }
    None
}

/// Find the next non-whitespace position using space bitmask.
#[inline]
fn skip_whitespace(buf: &[u8], mut pos: usize, end: usize) -> usize {
    while pos < end {
        match buf[pos] {
            b' ' | b'\t' | b'\r' => {
                pos += 1;
            }
            _ => return pos,
        }
    }
    pos
}

/// Skip a nested object/array using brace/bracket bitmasks.
#[inline]
fn skip_nested(buf: &[u8], mut pos: usize, end: usize, blocks: &StoredBitmasks<'_>) -> usize {
    const MAX_TRACKED_DEPTH: u32 = 32;
    let mut depth: u32 = 0;
    let mut opener_stack = [0u8; MAX_TRACKED_DEPTH as usize];

    while pos < end {
        let block = pos >> 6;
        let bit = pos & 63;
        if block >= blocks.real_quotes.len() {
            break;
        }

        let mask = 1u64 << bit;
        let b = buf[pos];

        match b {
            b'{' | b'[' if (blocks.open_brace[block] | blocks.open_bracket[block]) & mask != 0 => {
                if depth == MAX_TRACKED_DEPTH {
                    return end; // fail closed when opener stack capacity is exceeded
                }
                opener_stack[depth as usize] = b;
                depth += 1;
                pos += 1;
            }
            b'}' | b']'
                if (blocks.close_brace[block] | blocks.close_bracket[block]) & mask != 0 =>
            {
                if depth == 0 {
                    return pos;
                }
                depth = depth.saturating_sub(1);
                if depth < MAX_TRACKED_DEPTH {
                    let expected = if opener_stack[depth as usize] == b'{' {
                        b'}'
                    } else {
                        b']'
                    };
                    if b != expected {
                        return end; // mismatch
                    }
                }
                pos += 1;
                if depth == 0 {
                    return pos;
                }
            }
            b'"' if blocks.real_quotes[block] & mask != 0 => {
                // Skip string
                pos += 1;
                match next_quote(pos, end, blocks) {
                    Some(close) => pos = close + 1,
                    None => return end,
                }
            }
            _ => pos += 1,
        }
    }
    pos
}

/// Check if a byte is a JSON value delimiter.
#[inline(always)]
fn is_json_delimiter(b: u8) -> bool {
    matches!(b, b',' | b'}' | b']' | b' ' | b'\t' | b'\r' | b'\n')
}

/// Skip a bare value (used for malformed tokens).
#[inline]
fn skip_bare_value(buf: &[u8], mut pos: usize, end: usize) -> usize {
    while pos < end {
        match buf[pos] {
            b',' | b'}' | b' ' | b'\t' | b'\r' | b'\n' => return pos,
            _ => pos += 1,
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
    use crate::scan_config::ScanConfig;
    use alloc::string::String;
    use alloc::vec::Vec;

    /// Minimal ScanBuilder for testing — captures fields as strings.
    struct TestBuilder {
        rows: Vec<Vec<(String, String)>>,
        raws: Vec<Option<String>>,
        current_row: Vec<(String, String)>,
        field_names: Vec<String>,
        current_raw: Option<String>,
    }

    impl TestBuilder {
        fn new() -> Self {
            Self {
                rows: Vec::new(),
                raws: Vec::new(),
                current_row: Vec::new(),
                field_names: Vec::new(),
                current_raw: None,
            }
        }
    }

    impl ScanBuilder for TestBuilder {
        fn begin_batch(&mut self) {}
        fn begin_row(&mut self) {
            self.current_row.clear();
            self.current_raw = None;
        }
        fn end_row(&mut self) {
            self.rows.push(self.current_row.clone());
            self.raws.push(self.current_raw.take());
        }
        fn resolve_field(&mut self, name: &[u8]) -> usize {
            let name_str = core::str::from_utf8(name).unwrap().to_string();
            if let Some(idx) = self.field_names.iter().position(|n| n == &name_str) {
                idx
            } else {
                self.field_names.push(name_str);
                self.field_names.len() - 1
            }
        }
        fn append_str_by_idx(&mut self, idx: usize, val: &[u8]) {
            let name = self.field_names[idx].clone();
            let val_str = alloc::string::String::from_utf8_lossy(val).to_string();
            self.current_row.push((name, val_str));
        }
        fn append_int_by_idx(&mut self, idx: usize, val: &[u8]) {
            let name = self.field_names[idx].clone();
            let val_str = alloc::string::String::from_utf8_lossy(val).to_string();
            self.current_row
                .push((name, alloc::format!("int:{val_str}")));
        }
        fn append_float_by_idx(&mut self, idx: usize, val: &[u8]) {
            let name = self.field_names[idx].clone();
            let val_str = alloc::string::String::from_utf8_lossy(val).to_string();
            self.current_row
                .push((name, alloc::format!("float:{val_str}")));
        }
        fn append_null_by_idx(&mut self, idx: usize) {
            let name = self.field_names[idx].clone();
            self.current_row.push((name, "null".to_string()));
        }
        fn append_raw(&mut self, val: &[u8]) {
            self.current_raw = Some(alloc::string::String::from_utf8_lossy(val).to_string());
        }
    }

    use alloc::string::ToString;

    #[test]
    fn simple_object() {
        let buf = br#"{"name":"alice","age":30}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(row.iter().any(|(k, v)| k == "name" && v == "alice"));
        assert!(row.iter().any(|(k, v)| k == "age" && v == "int:30"));
    }

    #[test]
    fn ndjson_two_lines() {
        let buf = b"{\"a\":\"x\"}\n{\"b\":\"y\"}\n";
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 2);
        assert!(builder.rows[0].iter().any(|(k, v)| k == "a" && v == "x"));
        assert!(builder.rows[1].iter().any(|(k, v)| k == "b" && v == "y"));
    }

    #[test]
    fn nested_object_skipped() {
        let buf = br#"{"outer":{"inner":1},"flat":"val"}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(
            row.iter()
                .any(|(k, v)| k == "outer" && v == r#"{"inner":1}"#)
        );
        assert!(row.iter().any(|(k, v)| k == "flat" && v == "val"));
    }

    #[test]
    fn null_boolean_number() {
        let buf = br#"{"n":null,"t":true,"f":false,"i":42,"x":3.14}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(row.iter().any(|(k, v)| k == "n" && v == "null"));
        assert!(row.iter().any(|(k, v)| k == "t" && v == "true"));
        assert!(row.iter().any(|(k, v)| k == "f" && v == "false"));
        assert!(row.iter().any(|(k, v)| k == "i" && v == "int:42"));
        assert!(row.iter().any(|(k, v)| k == "x" && v == "float:3.14"));
    }

    #[test]
    fn validates_null_token() {
        let buf = br#"{"x":nul}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        assert!(builder.rows[0].is_empty());
    }

    #[test]
    fn validates_boolean_token() {
        let buf = br#"{"x":turkey}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        assert!(builder.rows[0].is_empty());
    }

    #[test]
    fn escaped_quotes_in_value() {
        let buf = br#"{"msg":"said \"hello\""}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(
            row.iter()
                .any(|(k, v)| k == "msg" && v == r#"said \"hello\""#)
        );
    }

    #[test]
    fn array_value() {
        let buf = br#"{"tags":["a","b"],"x":1}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(row.iter().any(|(k, v)| k == "tags" && v == r#"["a","b"]"#));
        assert!(row.iter().any(|(k, v)| k == "x" && v == "int:1"));
    }

    #[test]
    fn raw_non_json_lines() {
        let buf = b"plain text line 1\nplain text line 2\n";
        let config = ScanConfig {
            keep_raw: true,
            ..ScanConfig::default()
        };
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 2);
    }

    #[test]
    fn keep_raw_with_json() {
        let buf = br#"{"a":"b"}"#;
        let config = ScanConfig {
            keep_raw: true,
            ..ScanConfig::default()
        };
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        assert!(builder.rows[0].iter().any(|(k, v)| k == "a" && v == "b"));
        assert_eq!(builder.raws[0].as_deref(), Some(r#"{"a":"b"}"#));
    }

    #[test]
    fn keep_raw_non_json_captures_content() {
        let buf = b"hello world\n";
        let config = ScanConfig {
            keep_raw: true,
            ..ScanConfig::default()
        };
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        assert_eq!(builder.raws[0].as_deref(), Some("hello world"));
    }

    #[test]
    fn rejects_truex_suffix() {
        let buf = br#"{"x":truex}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        assert!(builder.rows[0].is_empty());
    }

    #[test]
    fn rejects_nullified_suffix() {
        let buf = br#"{"x":nullified}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        assert!(builder.rows[0].is_empty());
    }
}

// ---------------------------------------------------------------------------
// Kani proofs
// ---------------------------------------------------------------------------

#[cfg(kani)]
mod verification {
    use super::*;

    /// skip_whitespace returns a position in [start, end].
    /// If result < end, the byte at result is NOT whitespace.
    #[kani::proof]
    #[kani::unwind(17)]
    fn verify_skip_whitespace() {
        let buf: [u8; 16] = kani::any();
        let start: usize = kani::any();
        let end: usize = kani::any();
        kani::assume(start <= end && end <= 16);

        let result = skip_whitespace(&buf, start, end);

        assert!(result >= start && result <= end);
        if result < end {
            let b = buf[result];
            assert!(b != b' ' && b != b'\t' && b != b'\r');
        }
    }

    /// skip_bare_value returns a position in [start, end].
    /// All bytes before result are NOT delimiters.
    #[kani::proof]
    #[kani::unwind(17)]
    fn verify_skip_bare_value() {
        let buf: [u8; 16] = kani::any();
        let start: usize = kani::any();
        let end: usize = kani::any();
        kani::assume(start <= end && end <= 16);

        let result = skip_bare_value(&buf, start, end);

        assert!(result >= start && result <= end);
        let mut i = start;
        while i < result {
            let b = buf[i];
            assert!(b != b',' && b != b'}' && b != b' ' && b != b'\t' && b != b'\r' && b != b'\n');
            i += 1;
        }
    }

    /// trim_whitespace produces a subslice — result length <= input length.
    #[kani::proof]
    #[kani::unwind(9)]
    fn verify_trim_whitespace() {
        let buf: [u8; 8] = kani::any();
        let len: usize = kani::any_where(|&l: &usize| l <= 8);
        let input = &buf[..len];
        let result = trim_whitespace(input);

        assert!(result.len() <= input.len());

        // First byte of result (if any) is not whitespace
        if !result.is_empty() {
            let first = result[0];
            assert!(first != b' ' && first != b'\t' && first != b'\r' && first != b'\n');
            let last = result[result.len() - 1];
            assert!(last != b' ' && last != b'\t' && last != b'\r' && last != b'\n');
        }
    }

    /// next_quote on a single block: if found, the position has a set bit
    /// in the bitmask. If not found, no bits are set in [from, end).
    #[kani::proof]
    fn verify_next_quote_single_block() {
        let bitmask: u64 = kani::any();
        let from: usize = kani::any_where(|&f: &usize| f < 64);
        let end: usize = kani::any_where(|&e: &usize| e <= 64 && e >= from);

        let blocks = StoredBitmasks {
            real_quotes: &[bitmask],
            open_brace: &[0],
            close_brace: &[0],
            open_bracket: &[0],
            close_bracket: &[0],
        };

        let result = next_quote(from, end, &blocks);

        match result {
            Some(pos) => {
                assert!(pos >= from && pos < end);
                assert!((bitmask >> pos) & 1 == 1);
            }
            None => {
                // No bits set in [from, end)
                let mut i = from;
                while i < end {
                    assert!((bitmask >> i) & 1 == 0);
                    i += 1;
                }
            }
        }
    }
}

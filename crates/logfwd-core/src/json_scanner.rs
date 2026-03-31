// json_scanner.rs — Streaming JSON field scanner using StructuralIter.
//
// Replaces the byte-loop scanner with bitmask-driven iteration.
// Processes structural positions from StructuralIter sequentially —
// no stored bitmask vectors, no random-access lookups.
//
// The scanner is a state machine that expects this pattern:
//   { "key" : value , "key" : value , ... }
// where value is one of: "string", number, true, false, null, {nested}, [array]

use crate::scan_config::{ScanConfig, parse_int_fast};
use crate::scanner::ScanBuilder;
use crate::structural_iter::{StructuralIter, StructuralKind};

/// Scan an NDJSON buffer using streaming structural iteration.
///
/// Zero heap allocation for bitmask storage. All structural positions
/// are consumed on the fly from 64-byte block bitmasks.
#[inline(never)]
pub fn scan_streaming<B: ScanBuilder>(buf: &[u8], config: &ScanConfig, builder: &mut B) {
    if buf.is_empty() {
        return;
    }

    let mut iter = StructuralIter::new(buf);
    builder.begin_batch();

    // Process the buffer as a stream of structural positions.
    // Newlines delimit lines. Between newlines, parse JSON objects.
    while let Some(sp) = iter.advance() {
        match sp.kind {
            StructuralKind::Newline => {
                continue;
            }
            StructuralKind::OpenBrace => {
                // Start of a JSON object — parse it
                scan_json_object(buf, &mut iter, sp.pos, config, builder);
            }
            _ => {
                // Non-object line start — skip to next newline
                continue;
            }
        }
    }

    // Handle final line without trailing newline
    // (scan_json_object already called end_row if it parsed anything)
}

/// Parse a single JSON object from the structural position stream.
/// Returns the byte position after the object (or line end).
fn scan_json_object<B: ScanBuilder>(
    buf: &[u8],
    iter: &mut StructuralIter<'_>,
    open_brace_pos: usize,
    config: &ScanConfig,
    builder: &mut B,
) -> usize {
    builder.begin_row();
    if config.keep_raw {
        // We'll need to find the line end for raw — scan ahead or defer
        // For now, raw is set when we find the newline/end
    }

    loop {
        // Expect: quote (key start) or close brace (end of object)
        let sp = match iter.advance() {
            Some(sp) => sp,
            None => {
                // Buffer ended mid-object
                if config.keep_raw {
                    builder.append_raw(&buf[open_brace_pos..buf.len()]);
                }
                builder.end_row();
                return buf.len();
            }
        };

        match sp.kind {
            StructuralKind::Newline => {
                // Line ended — finalize row
                if config.keep_raw {
                    builder.append_raw(&buf[open_brace_pos..sp.pos]);
                }
                builder.end_row();
                return sp.pos + 1;
            }
            StructuralKind::CloseBrace => {
                // Object ended — find the newline to set raw, then finalize
                // Consume positions until newline or end
                loop {
                    match iter.advance() {
                        Some(nl) if nl.kind == StructuralKind::Newline => {
                            if config.keep_raw {
                                builder.append_raw(&buf[open_brace_pos..nl.pos]);
                            }
                            builder.end_row();
                            return nl.pos + 1;
                        }
                        Some(_) => continue, // skip trailing content
                        None => {
                            if config.keep_raw {
                                builder.append_raw(&buf[open_brace_pos..buf.len()]);
                            }
                            builder.end_row();
                            return buf.len();
                        }
                    }
                }
            }
            StructuralKind::Quote => {
                // Key start — read key, then colon, then value
                let key_start = sp.pos + 1;

                // Next structural must be closing quote of key
                let close_quote = match iter.advance() {
                    Some(sp) if sp.kind == StructuralKind::Quote => sp.pos,
                    Some(sp) if sp.kind == StructuralKind::Newline => {
                        if config.keep_raw {
                            builder.append_raw(&buf[open_brace_pos..sp.pos]);
                        }
                        builder.end_row();
                        return sp.pos + 1;
                    }
                    _ => {
                        builder.end_row();
                        return buf.len();
                    }
                };
                let key = &buf[key_start..close_quote];

                // Expect colon
                match iter.advance() {
                    Some(sp) if sp.kind == StructuralKind::Colon => {}
                    Some(sp) if sp.kind == StructuralKind::Newline => {
                        if config.keep_raw {
                            builder.append_raw(&buf[open_brace_pos..sp.pos]);
                        }
                        builder.end_row();
                        return sp.pos + 1;
                    }
                    _ => {
                        builder.end_row();
                        return buf.len();
                    }
                }

                let wanted = config.is_wanted(key);

                // Now read the value — peek at next structural to determine type
                let value_sp = match iter.advance() {
                    Some(sp) => sp,
                    None => {
                        builder.end_row();
                        return buf.len();
                    }
                };

                match value_sp.kind {
                    StructuralKind::Newline => {
                        if config.keep_raw {
                            builder.append_raw(&buf[open_brace_pos..value_sp.pos]);
                        }
                        builder.end_row();
                        return value_sp.pos + 1;
                    }
                    StructuralKind::Quote => {
                        // String value — next quote is the closing one
                        let val_start = value_sp.pos + 1;
                        let close = match iter.advance() {
                            Some(sp) if sp.kind == StructuralKind::Quote => sp.pos,
                            Some(sp) if sp.kind == StructuralKind::Newline => {
                                if config.keep_raw {
                                    builder.append_raw(&buf[open_brace_pos..sp.pos]);
                                }
                                builder.end_row();
                                return sp.pos + 1;
                            }
                            _ => {
                                builder.end_row();
                                return buf.len();
                            }
                        };
                        if wanted {
                            let idx = builder.resolve_field(key);
                            builder.append_str_by_idx(idx, &buf[val_start..close]);
                        }
                    }
                    StructuralKind::OpenBrace | StructuralKind::OpenBracket => {
                        // Nested object/array — skip by tracking depth
                        let nested_start = value_sp.pos;
                        let mut depth: u32 = 1;
                        let mut nested_end = value_sp.pos + 1;
                        while depth > 0 {
                            match iter.advance() {
                                Some(sp) => {
                                    nested_end = sp.pos + 1;
                                    match sp.kind {
                                        StructuralKind::OpenBrace | StructuralKind::OpenBracket => {
                                            depth += 1;
                                        }
                                        StructuralKind::CloseBrace
                                        | StructuralKind::CloseBracket => {
                                            depth -= 1;
                                        }
                                        StructuralKind::Newline => {
                                            // Malformed — newline inside nested
                                            if config.keep_raw {
                                                builder.append_raw(&buf[open_brace_pos..sp.pos]);
                                            }
                                            builder.end_row();
                                            return sp.pos + 1;
                                        }
                                        _ => {}
                                    }
                                }
                                None => break,
                            }
                        }
                        if wanted {
                            let idx = builder.resolve_field(key);
                            builder.append_str_by_idx(idx, &buf[nested_start..nested_end]);
                        }
                    }
                    StructuralKind::Comma | StructuralKind::CloseBrace => {
                        // The next structural after colon is a comma or brace —
                        // the bare value is between the colon and this delimiter.
                        // We need to find where the colon was... the value bytes
                        // are between (colon_pos+1) and value_sp.pos.
                        // We know colon was at close_quote + skip_ws... approximate
                        // by scanning backwards from value_sp.pos.
                        let val_end = value_sp.pos;
                        let val_start = close_quote + 2; // after ": "
                        let val = trim_ws(&buf[val_start..val_end]);

                        if wanted && !val.is_empty() {
                            dispatch_bare_value(key, val, builder);
                        }

                        // If this was a close brace, the object is ending
                        if value_sp.kind == StructuralKind::CloseBrace {
                            // Find newline to finalize
                            loop {
                                match iter.advance() {
                                    Some(nl) if nl.kind == StructuralKind::Newline => {
                                        if config.keep_raw {
                                            builder.append_raw(&buf[open_brace_pos..nl.pos]);
                                        }
                                        builder.end_row();
                                        return nl.pos + 1;
                                    }
                                    Some(_) => continue,
                                    None => {
                                        if config.keep_raw {
                                            builder.append_raw(&buf[open_brace_pos..buf.len()]);
                                        }
                                        builder.end_row();
                                        return buf.len();
                                    }
                                }
                            }
                        }
                        // Comma — continue to next key
                        continue;
                    }
                    _ => {
                        // Unexpected structural — skip
                    }
                }

                // After value, expect comma or close brace
                // (might already have been consumed if value was bare)
            }
            StructuralKind::Comma => {
                // Between fields — continue
                continue;
            }
            _ => {
                // Unexpected structural position — skip
                continue;
            }
        }
    }
}

/// Dispatch a bare value (number, boolean, null) to the builder.
fn dispatch_bare_value<B: ScanBuilder>(key: &[u8], val: &[u8], builder: &mut B) {
    let idx = builder.resolve_field(key);
    match val.first() {
        Some(b'n') => {
            // Validate null
            if val == b"null" {
                builder.append_null_by_idx(idx);
            }
            // else: malformed — skip (#515)
        }
        Some(b't') => {
            if val == b"true" {
                builder.append_str_by_idx(idx, val);
            }
        }
        Some(b'f') => {
            if val == b"false" {
                builder.append_str_by_idx(idx, val);
            }
        }
        Some(b'-' | b'0'..=b'9') => {
            let is_float = val.iter().any(|&c| c == b'.' || c == b'e' || c == b'E');
            if is_float {
                builder.append_float_by_idx(idx, val);
            } else if parse_int_fast(val).is_some() {
                builder.append_int_by_idx(idx, val);
            } else {
                builder.append_float_by_idx(idx, val);
            }
        }
        _ => {
            // Unknown bare value — skip
        }
    }
}

/// Trim ASCII whitespace from both ends of a byte slice.
#[inline]
fn trim_ws(mut s: &[u8]) -> &[u8] {
    while let Some((&b, rest)) = s.split_first() {
        if matches!(b, b' ' | b'\t' | b'\r' | b'\n') {
            s = rest;
        } else {
            break;
        }
    }
    while let Some((&b, rest)) = s.split_last() {
        if matches!(b, b' ' | b'\t' | b'\r' | b'\n') {
            s = rest;
        } else {
            break;
        }
    }
    s
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
        current_row: Vec<(String, String)>,
        field_names: Vec<String>,
        raw: Option<String>,
    }

    impl TestBuilder {
        fn new() -> Self {
            Self {
                rows: Vec::new(),
                current_row: Vec::new(),
                field_names: Vec::new(),
                raw: None,
            }
        }
    }

    impl ScanBuilder for TestBuilder {
        fn begin_batch(&mut self) {}
        fn begin_row(&mut self) {
            self.current_row.clear();
            self.raw = None;
        }
        fn end_row(&mut self) {
            self.rows.push(self.current_row.clone());
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
            self.raw = Some(alloc::string::String::from_utf8_lossy(val).to_string());
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
        // #515: "nul" should not be accepted as null
        let buf = br#"{"x":nul}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        // "nul" is not "null" — should be skipped, not emitted as null
        assert!(builder.rows[0].is_empty());
    }

    #[test]
    fn validates_boolean_token() {
        // #515: "turkey" should not be accepted as true
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
}

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
use crate::scan_predicate::ExtractedValue;
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

struct DecodeScratch {
    key: alloc::vec::Vec<u8>,
    value: alloc::vec::Vec<u8>,
}

/// Reusable scratch buffers for per-line scanning (decode + predicate).
/// Allocated once in `scan_streaming` and reused across lines.
struct LineScratch {
    decode: DecodeScratch,
    pred: Option<PredicateScratch>,
    deferred: alloc::vec::Vec<DeferredField>,
}

/// Scan an NDJSON buffer using streaming structural iteration.
///
/// Zero heap allocation for bitmask storage. Line boundaries and
/// structural positions are extracted from 64-byte block bitmasks
/// consumed on the fly.
///
/// JSON escape sequences in string values (`\"`, `\\`, `\/`, `\b`, `\f`,
/// `\n`, `\r`, `\t`, `\uXXXX`) are decoded to their UTF-8 representation
/// during extraction. This prevents double-escaping when values are
/// re-serialized downstream (see issue #410).
///
/// # Preconditions
/// - The caller must have already invoked `begin_batch` on the builder before
///   this call (see [`ScanBuilder`] for the initialization contract).
#[inline(never)]
pub fn scan_streaming<B: ScanBuilder>(buf: &[u8], config: &ScanConfig, builder: &mut B) {
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
    // 5 u64s per block (40 bytes) for fields that need random-access lookups.
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

        // Extract line boundaries (consumed, not stored).
        // Strip a trailing \r so that Windows CRLF line endings do not leak
        // into captured line fields or confuse the JSON parser.
        let mut nl = processed.newline;
        while nl != 0 {
            let bit_pos = nl.trailing_zeros() as usize;
            let abs_pos = offset + bit_pos;
            // Strip trailing \r (CRLF → LF normalisation).
            let line_end = if abs_pos > line_start && buf.get(abs_pos - 1).copied() == Some(b'\r') {
                abs_pos - 1
            } else {
                abs_pos
            };
            if line_end > line_start {
                line_ranges.push((line_start, line_end));
            }
            line_start = abs_pos + 1;
            nl &= nl - 1;
        }
    }

    if line_start < len {
        // Strip trailing \r from the last (unterminated) line too.
        let line_end = if len > line_start && buf.get(len - 1).copied() == Some(b'\r') {
            len - 1
        } else {
            len
        };
        if line_end > line_start {
            line_ranges.push((line_start, line_end));
        }
    }

    let bitmasks = StoredBitmasks {
        real_quotes: &real_quotes,
        open_brace: &open_brace,
        close_brace: &close_brace,
        open_bracket: &open_bracket,
        close_bracket: &close_bracket,
    };

    // Scratch buffers allocated once and reused across lines to avoid per-line
    // heap allocations. Predicate scratch is only allocated when needed.
    let mut line_scratch = LineScratch {
        decode: DecodeScratch {
            key: alloc::vec::Vec::new(),
            value: alloc::vec::Vec::new(),
        },
        pred: if config.row_predicate.is_some() {
            Some(PredicateScratch::new())
        } else {
            None
        },
        deferred: alloc::vec::Vec::new(),
    };

    // Phase 2: Scan each line using stored bitmasks for quote/nested lookups.
    for (start, end) in line_ranges {
        scan_line(
            buf,
            start,
            end,
            &bitmasks,
            config,
            builder,
            &mut line_scratch,
        );
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
    line_scratch: &mut LineScratch,
) {
    if config.row_predicate.is_some() {
        scan_line_with_predicate(buf, start, end, blocks, config, builder, line_scratch);
        return;
    }
    let scratch = &mut line_scratch.decode;
    builder.begin_row();
    if config.captures_line() {
        builder.append_line(&buf[start..end]);
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
        let raw_key = &buf[key_start..key_end];
        let key = if memchr::memchr(b'\\', raw_key).is_some() {
            decode_json_escapes(raw_key, &mut scratch.key);
            scratch.key.as_slice()
        } else {
            raw_key
        };
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
                // String value — decode JSON escape sequences (#410)
                let val_start = pos + 1;
                let val_end = match next_quote(pos + 1, end, blocks) {
                    Some(p) => p,
                    None => break,
                };
                if wanted {
                    let idx = builder.resolve_field(key);
                    let raw = &buf[val_start..val_end];
                    if memchr::memchr(b'\\', raw).is_some() {
                        decode_json_escapes(raw, &mut scratch.value);
                        builder.append_decoded_str_by_idx(idx, &scratch.value);
                    } else {
                        builder.append_str_by_idx(idx, raw);
                    }
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
                        builder.append_bool_by_idx(idx, true);
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
                        builder.append_bool_by_idx(idx, false);
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
                    } else if is_json_delimiter(c) {
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
// Predicate-aware scan_line — deferred builder writes
// ---------------------------------------------------------------------------

/// A field value extracted during scanning, deferred until predicate evaluation.
///
/// Stores byte ranges into the input buffer to avoid copying. For decoded
/// strings (with JSON escapes), the decoded bytes are stored in the scratch
/// buffer and a flag is set.
struct DeferredField {
    /// Column index from `resolve_field`.
    idx: usize,
    /// The value to write.
    kind: DeferredValue,
}

/// Value types that can be deferred during predicate-aware scanning.
enum DeferredValue {
    /// Raw string (subslice of input buffer).
    Str { start: usize, end: usize },
    /// Decoded string (stored in scratch.value, copied here because scratch
    /// is reused per field).
    DecodedStr(alloc::vec::Vec<u8>),
    /// Nested object/array (subslice of input buffer).
    Nested { start: usize, end: usize },
    /// Integer (raw ASCII digits in input buffer).
    Int { start: usize, end: usize },
    /// Float (raw ASCII in input buffer).
    Float { start: usize, end: usize },
    /// Boolean.
    Bool(bool),
    /// Null.
    Null,
}

/// Scratch space for predicate field values, reused across lines.
struct PredicateScratch {
    /// Field name → extracted value for predicate evaluation.
    /// Cleared per line. Uses Vec of (name, value) pairs to avoid HashMap
    /// (keeping no_std simple; predicate field count is small).
    fields: alloc::vec::Vec<(alloc::vec::Vec<u8>, PredicateFieldValue)>,
}

/// A predicate field value extracted during scanning.
enum PredicateFieldValue {
    Str(alloc::vec::Vec<u8>),
    Int(i64),
    Float(f64),
    Bool(bool),
    Null,
}

impl PredicateScratch {
    fn new() -> Self {
        Self {
            fields: alloc::vec::Vec::new(),
        }
    }

    fn clear(&mut self) {
        self.fields.clear();
    }

    fn insert(&mut self, key: &[u8], value: PredicateFieldValue) {
        self.fields.push((key.to_vec(), value));
    }

    fn lookup<'a>(&'a self, name: &str) -> ExtractedValue<'a> {
        for (key, val) in &self.fields {
            if key.eq_ignore_ascii_case(name.as_bytes()) {
                return match val {
                    PredicateFieldValue::Str(s) => ExtractedValue::Str(s.as_slice()),
                    PredicateFieldValue::Int(n) => ExtractedValue::Int(*n),
                    PredicateFieldValue::Float(f) => ExtractedValue::Float(*f),
                    PredicateFieldValue::Bool(b) => ExtractedValue::Bool(*b),
                    PredicateFieldValue::Null => ExtractedValue::Null,
                };
            }
        }
        ExtractedValue::Missing
    }
}

/// Scan a single JSON line with predicate evaluation.
///
/// Parses all fields in a single pass, deferring builder writes. Predicate
/// fields are extracted into a scratch buffer for evaluation. If the predicate
/// passes, deferred writes are replayed into the builder. If it fails, the
/// row is skipped entirely (no begin_row/end_row).
fn scan_line_with_predicate<B: ScanBuilder>(
    buf: &[u8],
    start: usize,
    end: usize,
    blocks: &StoredBitmasks<'_>,
    config: &ScanConfig,
    builder: &mut B,
    line_scratch: &mut LineScratch,
) {
    let predicate = match config.row_predicate.as_ref() {
        Some(p) => p,
        None => {
            // Should not be called without a predicate, but handle gracefully.
            builder.begin_row();
            if config.captures_line() {
                builder.append_line(&buf[start..end]);
            }
            builder.end_row();
            return;
        }
    };

    let scratch = &mut line_scratch.decode;
    let pred_scratch = line_scratch.pred.get_or_insert_with(PredicateScratch::new);
    pred_scratch.clear();
    let deferred = &mut line_scratch.deferred;
    deferred.clear();

    // If the predicate references the line-capture field, store the line
    // content in the predicate scratch so IS NOT NULL and string comparisons
    // on the synthetic line column work correctly.
    if let Some(ref line_name) = config.line_field_name
        && predicate.references_field(line_name.as_bytes())
    {
        pred_scratch.insert(
            line_name.as_bytes(),
            PredicateFieldValue::Str(buf[start..end].to_vec()),
        );
    }
    let has_line_capture = config.captures_line();

    // Find the opening '{'
    let mut pos = skip_whitespace(buf, start, end);
    if pos >= end || buf[pos] != b'{' {
        // Empty or non-object line — evaluate predicate (will likely fail
        // since no fields extracted), then emit empty row if it passes.
        if predicate.evaluate(&|name| pred_scratch.lookup(name)) {
            builder.begin_row();
            if has_line_capture {
                builder.append_line(&buf[start..end]);
            }
            builder.end_row();
        }
        return;
    }
    pos += 1;

    // Parse key-value pairs — extract all wanted fields, defer writes
    loop {
        pos = skip_whitespace(buf, pos, end);
        if pos >= end || buf[pos] == b'}' {
            break;
        }
        if buf[pos] != b'"' {
            break;
        }

        // Scan key
        let key_start = pos + 1;
        let key_end = match next_quote(pos + 1, end, blocks) {
            Some(p) => p,
            None => break,
        };
        let raw_key = &buf[key_start..key_end];
        let key = if memchr::memchr(b'\\', raw_key).is_some() {
            decode_json_escapes(raw_key, &mut scratch.key);
            scratch.key.as_slice()
        } else {
            raw_key
        };
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
        let is_pred_field = predicate.references_field(key);

        match buf[pos] {
            b'"' => {
                let val_start = pos + 1;
                let val_end = match next_quote(pos + 1, end, blocks) {
                    Some(p) => p,
                    None => break,
                };
                if wanted {
                    let idx = builder.resolve_field(key);
                    let raw = &buf[val_start..val_end];
                    if memchr::memchr(b'\\', raw).is_some() {
                        decode_json_escapes(raw, &mut scratch.value);
                        // Clone once; move to deferred, clone for predicate only
                        // when both paths need it (avoids the prior double-clone).
                        let decoded = scratch.value.clone();
                        if is_pred_field {
                            pred_scratch.insert(key, PredicateFieldValue::Str(decoded.clone()));
                        }
                        deferred.push(DeferredField {
                            idx,
                            kind: DeferredValue::DecodedStr(decoded),
                        });
                    } else {
                        if is_pred_field {
                            pred_scratch.insert(key, PredicateFieldValue::Str(raw.to_vec()));
                        }
                        deferred.push(DeferredField {
                            idx,
                            kind: DeferredValue::Str {
                                start: val_start,
                                end: val_end,
                            },
                        });
                    }
                } else if is_pred_field {
                    let raw = &buf[val_start..val_end];
                    if memchr::memchr(b'\\', raw).is_some() {
                        decode_json_escapes(raw, &mut scratch.value);
                        pred_scratch.insert(key, PredicateFieldValue::Str(scratch.value.clone()));
                    } else {
                        pred_scratch.insert(key, PredicateFieldValue::Str(raw.to_vec()));
                    }
                }
                pos = val_end + 1;
            }
            b'{' | b'[' => {
                let nested_start = pos;
                pos = skip_nested(buf, pos, end, blocks);
                if wanted {
                    let idx = builder.resolve_field(key);
                    deferred.push(DeferredField {
                        idx,
                        kind: DeferredValue::Nested {
                            start: nested_start,
                            end: pos,
                        },
                    });
                }
                if is_pred_field {
                    // Store the actual nested JSON bytes so predicates like
                    // `payload LIKE '%"a":1%'` see the real slice. This also
                    // makes IS NOT NULL work correctly (field is present).
                    pred_scratch.insert(
                        key,
                        PredicateFieldValue::Str(buf[nested_start..pos].to_vec()),
                    );
                }
            }
            b't' => {
                if pos + 4 <= end
                    && &buf[pos..pos + 4] == b"true"
                    && (pos + 4 >= end || is_json_delimiter(buf[pos + 4]))
                {
                    if is_pred_field {
                        pred_scratch.insert(key, PredicateFieldValue::Bool(true));
                    }
                    if wanted {
                        let idx = builder.resolve_field(key);
                        deferred.push(DeferredField {
                            idx,
                            kind: DeferredValue::Bool(true),
                        });
                    }
                    pos += 4;
                } else {
                    pos = skip_bare_value(buf, pos, end);
                }
            }
            b'f' => {
                if pos + 5 <= end
                    && &buf[pos..pos + 5] == b"false"
                    && (pos + 5 >= end || is_json_delimiter(buf[pos + 5]))
                {
                    if is_pred_field {
                        pred_scratch.insert(key, PredicateFieldValue::Bool(false));
                    }
                    if wanted {
                        let idx = builder.resolve_field(key);
                        deferred.push(DeferredField {
                            idx,
                            kind: DeferredValue::Bool(false),
                        });
                    }
                    pos += 5;
                } else {
                    pos = skip_bare_value(buf, pos, end);
                }
            }
            b'n' => {
                if pos + 4 <= end
                    && &buf[pos..pos + 4] == b"null"
                    && (pos + 4 >= end || is_json_delimiter(buf[pos + 4]))
                {
                    if is_pred_field {
                        pred_scratch.insert(key, PredicateFieldValue::Null);
                    }
                    if wanted {
                        let idx = builder.resolve_field(key);
                        deferred.push(DeferredField {
                            idx,
                            kind: DeferredValue::Null,
                        });
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
                    } else if is_json_delimiter(c) {
                        break;
                    } else {
                        pos += 1;
                    }
                }
                if is_pred_field {
                    let val = &buf[num_start..pos];
                    if is_float {
                        if let Some(f) = crate::scan_config::parse_float_fast(val) {
                            pred_scratch.insert(key, PredicateFieldValue::Float(f));
                        }
                    } else if let Some(n) = parse_int_fast(val) {
                        pred_scratch.insert(key, PredicateFieldValue::Int(n));
                    } else if let Some(f) = crate::scan_config::parse_float_fast(val) {
                        pred_scratch.insert(key, PredicateFieldValue::Float(f));
                    }
                }
                if wanted {
                    let idx = builder.resolve_field(key);
                    if is_float {
                        deferred.push(DeferredField {
                            idx,
                            kind: DeferredValue::Float {
                                start: num_start,
                                end: pos,
                            },
                        });
                    } else if parse_int_fast(&buf[num_start..pos]).is_some() {
                        deferred.push(DeferredField {
                            idx,
                            kind: DeferredValue::Int {
                                start: num_start,
                                end: pos,
                            },
                        });
                    } else {
                        deferred.push(DeferredField {
                            idx,
                            kind: DeferredValue::Float {
                                start: num_start,
                                end: pos,
                            },
                        });
                    }
                }
            }
        }

        // Skip comma
        pos = skip_whitespace(buf, pos, end);
        if pos < end && buf[pos] == b',' {
            pos += 1;
        }
    }

    // Evaluate predicate
    if !predicate.evaluate(&|name| pred_scratch.lookup(name)) {
        return; // Row filtered out — no builder calls.
    }

    // Predicate passed — replay deferred writes.
    builder.begin_row();
    if has_line_capture {
        builder.append_line(&buf[start..end]);
    }
    for field in deferred.iter() {
        match &field.kind {
            DeferredValue::Str { start: s, end: e } => {
                builder.append_str_by_idx(field.idx, &buf[*s..*e]);
            }
            DeferredValue::DecodedStr(bytes) => {
                builder.append_decoded_str_by_idx(field.idx, bytes.as_slice());
            }
            DeferredValue::Nested { start: s, end: e } => {
                builder.append_str_by_idx(field.idx, &buf[*s..*e]);
            }
            DeferredValue::Int { start: s, end: e } => {
                builder.append_int_by_idx(field.idx, &buf[*s..*e]);
            }
            DeferredValue::Float { start: s, end: e } => {
                builder.append_float_by_idx(field.idx, &buf[*s..*e]);
            }
            DeferredValue::Bool(b) => {
                builder.append_bool_by_idx(field.idx, *b);
            }
            DeferredValue::Null => {
                builder.append_null_by_idx(field.idx);
            }
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
    // Overflow stack is allocated only for pathological nesting that exceeds
    // MAX_TRACKED_DEPTH so the common path stays allocation-free.
    let mut overflow_stack: Option<alloc::vec::Vec<u8>> = None;

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
                if depth < MAX_TRACKED_DEPTH {
                    opener_stack[depth as usize] = b;
                } else {
                    overflow_stack
                        .get_or_insert_with(alloc::vec::Vec::new)
                        .push(b);
                }
                depth += 1;
                pos += 1;
            }
            b'}' | b']'
                if (blocks.close_brace[block] | blocks.close_bracket[block]) & mask != 0 =>
            {
                if depth == 0 {
                    return pos;
                }
                let prev_depth = depth;
                depth -= 1;
                let opener = if prev_depth > MAX_TRACKED_DEPTH {
                    match overflow_stack.as_mut().and_then(alloc::vec::Vec::pop) {
                        Some(opener) => opener,
                        None => return end,
                    }
                } else {
                    opener_stack[depth as usize]
                };
                let expected = if opener == b'{' { b'}' } else { b']' };
                if b != expected {
                    return pos; // mismatch — fail-closed to avoid emitting truncated values
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
/// Stops at the first byte where `is_json_delimiter` returns true.
#[inline]
fn skip_bare_value(buf: &[u8], mut pos: usize, end: usize) -> usize {
    while pos < end {
        if is_json_delimiter(buf[pos]) {
            return pos;
        }
        pos += 1;
    }
    pos
}

// ---------------------------------------------------------------------------
// JSON string escape decoding (#410)
// ---------------------------------------------------------------------------

/// Decode JSON escape sequences from `input` into `out`.
///
/// Handles all RFC 8259 §7 escapes: `\"` `\\` `\/` `\b` `\f` `\n` `\r` `\t`
/// and `\uXXXX` (including surrogate pairs for supplementary code points).
///
/// Invalid or truncated escape sequences are passed through unchanged
/// to avoid data loss on malformed input.
fn decode_json_escapes(input: &[u8], out: &mut alloc::vec::Vec<u8>) {
    out.clear();
    // Decoded output is always ≤ input length (escapes expand, never shrink).
    out.reserve(input.len());

    let mut i = 0;
    while i < input.len() {
        if input[i] != b'\\' || i + 1 >= input.len() {
            out.push(input[i]);
            i += 1;
            continue;
        }

        match input[i + 1] {
            b'"' => {
                out.push(b'"');
                i += 2;
            }
            b'\\' => {
                out.push(b'\\');
                i += 2;
            }
            b'/' => {
                out.push(b'/');
                i += 2;
            }
            b'b' => {
                out.push(0x08);
                i += 2;
            }
            b'f' => {
                out.push(0x0C);
                i += 2;
            }
            b'n' => {
                out.push(b'\n');
                i += 2;
            }
            b'r' => {
                out.push(b'\r');
                i += 2;
            }
            b't' => {
                out.push(b'\t');
                i += 2;
            }
            b'u' => {
                i = decode_unicode_escape(input, i, out);
            }
            _ => {
                // Unknown escape — pass through unchanged
                out.push(b'\\');
                i += 1;
            }
        }
    }
}

/// Decode a `\uXXXX` escape (possibly a surrogate pair) starting at `pos`.
/// Appends the decoded UTF-8 bytes to `out` and returns the new position.
fn decode_unicode_escape(input: &[u8], pos: usize, out: &mut alloc::vec::Vec<u8>) -> usize {
    // Need at least 6 bytes: \uXXXX
    if pos + 6 > input.len() {
        out.push(b'\\');
        return pos + 1;
    }
    let cp = match parse_hex4(&input[pos + 2..pos + 6]) {
        Some(v) => v,
        None => {
            out.push(b'\\');
            return pos + 1;
        }
    };

    // High surrogate — expect a following \uXXXX low surrogate
    if (0xD800..=0xDBFF).contains(&cp) {
        if pos + 12 <= input.len()
            && input[pos + 6] == b'\\'
            && input[pos + 7] == b'u'
            && let Some(lo) = parse_hex4(&input[pos + 8..pos + 12])
            && (0xDC00..=0xDFFF).contains(&lo)
        {
            let full = 0x10000 + ((cp as u32 - 0xD800) << 10) + (lo as u32 - 0xDC00);
            if let Some(c) = char::from_u32(full) {
                let mut utf8 = [0u8; 4];
                let s = c.encode_utf8(&mut utf8);
                out.extend_from_slice(s.as_bytes());
                return pos + 12;
            }
        }
        // Unpaired high surrogate — pass through raw
        out.extend_from_slice(&input[pos..pos + 6]);
        return pos + 6;
    }

    // Lone low surrogate — pass through raw
    if (0xDC00..=0xDFFF).contains(&cp) {
        out.extend_from_slice(&input[pos..pos + 6]);
        return pos + 6;
    }

    // BMP code point
    if let Some(c) = char::from_u32(cp as u32) {
        let mut utf8 = [0u8; 4];
        let s = c.encode_utf8(&mut utf8);
        out.extend_from_slice(s.as_bytes());
        pos + 6
    } else {
        // Invalid code point — pass through raw
        out.extend_from_slice(&input[pos..pos + 6]);
        pos + 6
    }
}

/// Parse 4 ASCII hex digits into a `u16`.
#[inline]
fn parse_hex4(bytes: &[u8]) -> Option<u16> {
    if bytes.len() < 4 {
        return None;
    }
    let mut val: u16 = 0;
    let mut j = 0;
    while j < 4 {
        let digit = match bytes[j] {
            b'0'..=b'9' => bytes[j] - b'0',
            b'a'..=b'f' => bytes[j] - b'a' + 10,
            b'A'..=b'F' => bytes[j] - b'A' + 10,
            _ => return None,
        };
        val = (val << 4) | digit as u16;
        j += 1;
    }
    Some(val)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan_config::{FieldSpec, ScanConfig};
    use alloc::string::String;
    use alloc::{vec, vec::Vec};
    use proptest::prelude::*;
    use proptest::test_runner::Config as ProptestConfig;

    /// Maximum padding bytes to prepend when probing SIMD block boundaries
    /// in proptests.  Under Miri each extra byte costs ~10–100x native, and
    /// the invariants under test are memory-agnostic, so we use a narrower
    /// range there. Native proptest still sweeps 0..90 to exercise edges of
    /// multiple 64-byte blocks.
    #[cfg(miri)]
    const MAX_PADDING: usize = 10;
    #[cfg(not(miri))]
    const MAX_PADDING: usize = 90;

    /// Minimal ScanBuilder for testing — captures fields as strings.
    struct TestBuilder {
        rows: Vec<Vec<(String, String)>>,
        lines: Vec<Option<String>>,
        current_row: Vec<(String, String)>,
        field_names: Vec<String>,
        current_line: Option<String>,
    }

    impl TestBuilder {
        fn new() -> Self {
            Self {
                rows: Vec::new(),
                lines: Vec::new(),
                current_row: Vec::new(),
                field_names: Vec::new(),
                current_line: None,
            }
        }
    }

    impl ScanBuilder for TestBuilder {
        fn begin_row(&mut self) {
            self.current_row.clear();
            self.current_line = None;
        }
        fn end_row(&mut self) {
            self.rows.push(self.current_row.clone());
            self.lines.push(self.current_line.take());
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
            let val_str = String::from_utf8_lossy(val).to_string();
            self.current_row.push((name, val_str));
        }
        fn append_int_by_idx(&mut self, idx: usize, val: &[u8]) {
            let name = self.field_names[idx].clone();
            let val_str = String::from_utf8_lossy(val).to_string();
            self.current_row
                .push((name, alloc::format!("int:{val_str}")));
        }
        fn append_float_by_idx(&mut self, idx: usize, val: &[u8]) {
            let name = self.field_names[idx].clone();
            let val_str = String::from_utf8_lossy(val).to_string();
            self.current_row
                .push((name, alloc::format!("float:{val_str}")));
        }
        fn append_bool_by_idx(&mut self, idx: usize, val: bool) {
            let name = self.field_names[idx].clone();
            self.current_row.push((name, alloc::format!("bool:{val}")));
        }
        fn append_null_by_idx(&mut self, idx: usize) {
            let name = self.field_names[idx].clone();
            self.current_row.push((name, "null".to_string()));
        }
        fn append_line(&mut self, val: &[u8]) {
            self.current_line = Some(String::from_utf8_lossy(val).to_string());
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
    fn escaped_key_is_decoded_before_field_filtering() {
        let mut buf = Vec::new();
        buf.extend_from_slice(br#"{"pad":""#);
        buf.extend(core::iter::repeat_n(b'x', 51));
        buf.extend_from_slice(br#"","a"#);
        assert_eq!(buf.len() % 64, 63);
        buf.extend_from_slice(br#"\u002eb":"hit","other":"skip"}"#);
        let config = ScanConfig {
            wanted_fields: vec![FieldSpec {
                name: "a.b".to_string(),
                aliases: vec![],
            }],
            extract_all: false,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: None,
        };
        let mut builder = TestBuilder::new();
        scan_streaming(&buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(row.iter().any(|(k, v)| k == "a.b" && v == "hit"));
        assert!(!row.iter().any(|(k, v)| k == "other" && v == "skip"));
    }

    #[test]
    fn escaped_key_decode_across_simd_boundary() {
        // Pad so the \u002e escape straddles the 64-byte SIMD block boundary.
        // With 58 spaces: {"a starts at offset 58, so \ is at 61 and `e` at 66,
        // meaning the escape spans both block 0 (bytes 0..63) and block 1 (64..).
        let padding = " ".repeat(58);
        let input = alloc::format!("{}{{\"a\\u002eb\":1}}\n", padding);
        let config = ScanConfig {
            wanted_fields: vec![FieldSpec {
                name: "a.b".to_string(),
                aliases: vec![],
            }],
            extract_all: false,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: None,
        };
        let mut builder = TestBuilder::new();
        scan_streaming(input.as_bytes(), &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(
            row.iter().any(|(k, v)| k == "a.b" && v == "int:1"),
            "escaped key \\u002e must decode to '.' across SIMD boundary"
        );
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
        assert!(row.iter().any(|(k, v)| k == "t" && v == "bool:true"));
        assert!(row.iter().any(|(k, v)| k == "f" && v == "bool:false"));
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
        // After #410 fix: scanner decodes \" to " in string values.
        let buf = br#"{"msg":"said \"hello\""}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(
            row.iter()
                .any(|(k, v)| k == "msg" && v == r#"said "hello""#)
        );
    }

    // --- Escape decoding tests (#410) ---

    #[test]
    fn unicode_escape_u0041() {
        // \u0041 is 'A' — must be decoded, not double-escaped.
        let buf = br#"{"a":"\u0041"}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(row.iter().any(|(k, v)| k == "a" && v == "A"));
    }

    #[test]
    fn unicode_escape_e_acute() {
        // \u00e9 is 'é' — must be decoded to UTF-8.
        let buf = br#"{"msg":"caf\u00e9"}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(row.iter().any(|(k, v)| k == "msg" && v == "café"));
    }

    #[test]
    fn unicode_surrogate_pair() {
        // \uD83D\uDE00 is U+1F600 (😀) — surrogate pair decoded to UTF-8.
        let buf = br#"{"e":"\uD83D\uDE00"}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(row.iter().any(|(k, v)| k == "e" && v == "😀"));
    }

    #[test]
    fn escape_newline_tab_cr() {
        let buf = br#"{"a":"line1\nline2\ttab\rret"}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(
            row.iter()
                .any(|(k, v)| k == "a" && v == "line1\nline2\ttab\rret")
        );
    }

    #[test]
    fn escape_backslash() {
        // \\\\ in raw bytes is two JSON-escaped backslashes → two literal backslashes
        let buf = b"{\"a\":\"c:\\\\path\\\\file\"}";
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(row.iter().any(|(k, v)| k == "a" && v == "c:\\path\\file"));
    }

    #[test]
    fn escape_solidus() {
        let buf = br#"{"url":"http:\/\/example.com"}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(
            row.iter()
                .any(|(k, v)| k == "url" && v == "http://example.com")
        );
    }

    #[test]
    fn no_escape_passthrough() {
        // Strings without backslashes pass through unchanged (fast path).
        let buf = br#"{"x":"hello world"}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(row.iter().any(|(k, v)| k == "x" && v == "hello world"));
    }

    #[test]
    fn mixed_escapes_and_plain_text() {
        let buf = br#"{"m":"start\n\tmiddle \u0041 end"}"#;
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(
            row.iter()
                .any(|(k, v)| k == "m" && v == "start\n\tmiddle A end")
        );
    }

    #[test]
    fn decode_json_escapes_unit() {
        let mut out = Vec::new();

        // Simple escapes
        decode_json_escapes(br"hello", &mut out);
        assert_eq!(&out, b"hello");

        decode_json_escapes(br#"say \"hi\""#, &mut out);
        assert_eq!(&out, b"say \"hi\"");

        decode_json_escapes(br"a\\b", &mut out);
        assert_eq!(&out, b"a\\b");

        decode_json_escapes(br"a\/b", &mut out);
        assert_eq!(&out, b"a/b");

        decode_json_escapes(br"a\nb\tc", &mut out);
        assert_eq!(&out, b"a\nb\tc");

        decode_json_escapes(br"\b\f", &mut out);
        assert_eq!(&out, &[0x08, 0x0C]);

        // Unicode escape
        decode_json_escapes(br"\u0041", &mut out);
        assert_eq!(&out, b"A");

        // Multi-byte unicode
        decode_json_escapes(br"\u00e9", &mut out);
        assert_eq!(&out, "é".as_bytes());

        // Surrogate pair
        decode_json_escapes(br"\uD83D\uDE00", &mut out);
        assert_eq!(&out, "😀".as_bytes());

        // Truncated escape at end — pass through
        decode_json_escapes(br"abc\", &mut out);
        assert_eq!(&out, br"abc\");

        // Unknown escape letter — pass through backslash
        decode_json_escapes(br"\x", &mut out);
        assert_eq!(&out, br"\x");
    }

    #[test]
    fn parse_hex4_unit() {
        assert_eq!(parse_hex4(b"0041"), Some(0x0041));
        assert_eq!(parse_hex4(b"00e9"), Some(0x00e9));
        assert_eq!(parse_hex4(b"D83D"), Some(0xD83D));
        assert_eq!(parse_hex4(b"DE00"), Some(0xDE00));
        assert_eq!(parse_hex4(b"FFFF"), Some(0xFFFF));
        assert_eq!(parse_hex4(b"0000"), Some(0x0000));
        assert_eq!(parse_hex4(b"abcf"), Some(0xABCF));
        assert_eq!(parse_hex4(b"ZZZZ"), None);
        assert_eq!(parse_hex4(b"00g0"), None);
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
            line_field_name: Some("message".to_string()),
            ..ScanConfig::default()
        };
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 2);
    }

    #[test]
    fn captures_line_with_json() {
        let buf = br#"{"a":"b"}"#;
        let config = ScanConfig {
            line_field_name: Some("message".to_string()),
            ..ScanConfig::default()
        };
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        assert!(builder.rows[0].iter().any(|(k, v)| k == "a" && v == "b"));
        assert_eq!(builder.lines[0].as_deref(), Some(r#"{"a":"b"}"#));
    }

    #[test]
    fn captures_line_non_json_content() {
        let buf = b"hello world\n";
        let config = ScanConfig {
            line_field_name: Some("message".to_string()),
            ..ScanConfig::default()
        };
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        assert_eq!(builder.lines[0].as_deref(), Some("hello world"));
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

    #[test]
    fn test_deeply_nested_object_graceful_handling() {
        // Create an object with depth 35: 35 '{' followed by 35 '}'
        let mut buf_str = "{\"a\":".to_string();
        for _ in 0..35 {
            buf_str.push_str("{\"x\":");
        }
        buf_str.push('1');
        for _ in 0..35 {
            buf_str.push('}');
        }
        buf_str.push_str(",\"b\":2}");

        let buf = buf_str.as_bytes();
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(row.iter().any(|(k, v)| k == "b" && v == "int:2"));
    }

    #[test]
    fn deeply_nested_mismatch_rejects_trailing_fields() {
        let mut buf = String::from("{\"a\":");
        for _ in 0..33 {
            buf.push('[');
        }
        buf.push_str("{1]}");
        for _ in 0..33 {
            buf.push(']');
        }
        buf.push_str(",\"b\":2}");

        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf.as_bytes(), &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        assert!(
            !row.iter().any(|(k, _)| k == "b"),
            "malformed deeply nested value must not leak trailing sibling fields"
        );
    }

    fn alternating_nested_value(extra_depth: usize) -> String {
        let mut s = String::new();
        let total_depth = 32 + extra_depth;
        for depth in 0..total_depth {
            if depth % 2 == 0 {
                s.push('{');
                s.push_str("\"x\":");
            } else {
                s.push('[');
            }
        }
        s.push('1');
        for depth in (0..total_depth).rev() {
            if depth % 2 == 0 {
                s.push('}');
            } else {
                s.push(']');
            }
        }
        s
    }

    #[test]
    fn test_skip_nested_at_depth_32() {
        let mut buf_str = "{\"a\":".to_string();
        for _ in 0..32 {
            buf_str.push_str("{\"x\":");
        }
        buf_str.push('1');
        for _ in 0..32 {
            buf_str.push('}');
        }
        buf_str.push_str(",\"b\":2}");

        let buf = buf_str.as_bytes();
        let config = ScanConfig::default();
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1);
        let row = &builder.rows[0];
        // If it drops remaining lines, it won't see "b"
        assert!(row.iter().any(|(k, v)| k == "b" && v == "int:2"));
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            failure_persistence: None,
            .. ProptestConfig::default()
        })]
        /// CRLF normalization invariant: scanning a JSON object with CRLF line endings
        /// must yield the same field values as scanning the same object with LF endings,
        /// and neither captured line values nor any field value must contain a bare \r.
        ///
        /// The strategy pads the JSON to probe near 64-byte SIMD block edges.
        #[test]
        fn crlf_and_lf_produce_identical_rows(
            val in "[a-z0-9]{1,20}",
            padding in 0usize..MAX_PADDING,
        ) {
            let spaces = " ".repeat(padding);
            let lf_line = alloc::format!("{{\"k\":\"{val}\"}}\n");
            let crlf_line = alloc::format!("{spaces}{{\"k\":\"{val}\"}}\r\n");
            let lf_with_spaces = alloc::format!("{spaces}{{\"k\":\"{val}\"}}\n");

            let config = ScanConfig::default();

            let mut lf_builder = TestBuilder::new();
            scan_streaming(lf_line.as_bytes(), &config, &mut lf_builder);

            let mut crlf_builder = TestBuilder::new();
            scan_streaming(crlf_line.as_bytes(), &config, &mut crlf_builder);

            let mut lf_spaces_builder = TestBuilder::new();
            scan_streaming(lf_with_spaces.as_bytes(), &config, &mut lf_spaces_builder);

            // Both CRLF and LF variants should produce exactly one row.
            prop_assert_eq!(lf_builder.rows.len(), 1, "LF: should produce one row");
            prop_assert_eq!(crlf_builder.rows.len(), 1, "CRLF: should produce one row");
            prop_assert_eq!(lf_spaces_builder.rows.len(), 1, "LF+spaces: should produce one row");

            // Field values must be identical regardless of line-ending style.
            prop_assert_eq!(
                &lf_builder.rows[0], &crlf_builder.rows[0],
                "CRLF and LF must produce identical field values"
            );

            // Captured lines (if enabled) must not contain a bare \r.
            for raw in crlf_builder.lines.iter().flatten() {
                prop_assert!(
                    !raw.contains('\r'),
                    "raw value must not contain bare \\r after CRLF normalization: {raw:?}"
                );
            }
        }

        #[test]
        fn deep_nesting_preserves_sibling_fields(extra_depth in 1usize..8, nested_first in any::<bool>()) {
            let nested = alternating_nested_value(extra_depth);
            let buf = if nested_first {
                alloc::format!("{{\"a\":{nested},\"b\":2}}")
            } else {
                alloc::format!("{{\"b\":2,\"a\":{nested}}}")
            };

            let config = ScanConfig::default();
            let mut builder = TestBuilder::new();
            scan_streaming(buf.as_bytes(), &config, &mut builder);

            prop_assert_eq!(builder.rows.len(), 1);
            let row = &builder.rows[0];
            prop_assert!(
                row.iter().any(|(k, v)| k == "b" && v == "int:2"),
                "valid deeply nested JSON should preserve sibling fields past MAX_TRACKED_DEPTH"
            );
        }

        /// Proptest: skip_bare_value always stops at the first `]` delimiter.
        ///
        /// Generates numeric payloads immediately followed by `]`, with varying
        /// amounts of prefix padding to probe near 64-byte SIMD block boundaries.
        /// Invariants checked:
        ///  - `skip_bare_value` returns the index of the `]` byte
        ///  - all bytes before the returned index are non-delimiters
        ///  - `scan_streaming` on the enclosing JSON object produces exactly 1 row
        #[test]
        fn skip_bare_value_close_bracket_proptest(
            num in 0u32..1_000_000,
            padding in 0usize..MAX_PADDING,
        ) {
            // Direct: "42]extra" — must stop exactly at position of ']'
            let value_str = alloc::format!("{num}");
            let mut direct_buf: Vec<u8> = Vec::new();
            direct_buf.extend_from_slice(value_str.as_bytes());
            direct_buf.push(b']');
            direct_buf.extend_from_slice(b"extra");
            let pos = skip_bare_value(&direct_buf, 0, direct_buf.len());
            prop_assert_eq!(
                pos, value_str.len(),
                "skip_bare_value must stop exactly at ']' for input {:?}", direct_buf
            );
            prop_assert_eq!(direct_buf[pos], b']', "stopped byte must be ']'");
            for i in 0..pos {
                prop_assert!(!is_json_delimiter(direct_buf[i]), "byte at {i} must not be a delimiter");
            }

            // Integration: {"arr":[<num>]} padded to stress SIMD block edges
            let spaces = " ".repeat(padding);
            let json = alloc::format!("{spaces}{{\"arr\":[{num}]}}\n");
            let config = ScanConfig::default();
            let mut builder = TestBuilder::new();
            scan_streaming(json.as_bytes(), &config, &mut builder);
            prop_assert_eq!(
                builder.rows.len(), 1,
                "padded JSON with array value must produce exactly 1 row"
            );
        }

        /// Escape sequences in string values survive SIMD block boundaries.
        ///
        /// Padding `[0, 90)` bytes of whitespace before the JSON object probes
        /// positions near 64-byte SIMD block edges. Invariants checked:
        ///  - Exactly 1 row is produced
        ///  - The `v` field is present and its decoded value equals the
        ///    original (unescaped) ASCII character
        #[test]
        fn escape_sequences_survive_simd_boundaries(
            padding in 0usize..MAX_PADDING,
        ) {
            // Test the 8 standard RFC 8259 single-char escapes that round-trip
            // to a known single byte.
            let escapes: &[(&str, u8)] = &[
                ("\\\"", b'"'),
                ("\\\\", b'\\'),
                ("\\/", b'/'),
                ("\\b", 0x08),
                ("\\f", 0x0C),
                ("\\n", b'\n'),
                ("\\r", b'\r'),
                ("\\t", b'\t'),
            ];
            for (esc, expected_byte) in escapes {
                let spaces = " ".repeat(padding);
                // Build JSON with the escape inside the string value
                let json = alloc::format!("{spaces}{{\"v\":\"{esc}\"}}\n");
                let config = ScanConfig::default();
                let mut builder = TestBuilder::new();
                scan_streaming(json.as_bytes(), &config, &mut builder);
                prop_assert_eq!(
                    builder.rows.len(), 1,
                    "JSON with escape must produce 1 row"
                );
                let row = &builder.rows[0];
                let val = row.iter().find(|(k, _)| k == "v").map(|(_, v)| v.as_bytes());
                prop_assert!(
                    val.is_some(),
                    "escape sequence {esc:?} must produce field 'v'"
                );
                prop_assert_eq!(
                    val.unwrap(), &[*expected_byte],
                    "escape must decode correctly"
                );
            }
        }

        /// Integer vs float classification is consistent across scan_streaming.
        ///
        /// An integer JSON value must produce a value prefixed with "int:" and
        /// a floating-point JSON value must produce a value prefixed with "float:".
        #[test]
        fn numeric_classification_is_consistent(
            int_val in -1_000_000i64..=1_000_000,
            frac in 1u32..999,
        ) {
            let config = ScanConfig::default();

            // Integer path
            let int_json = alloc::format!("{{\"n\":{int_val}}}\n");
            let mut int_builder = TestBuilder::new();
            scan_streaming(int_json.as_bytes(), &config, &mut int_builder);
            prop_assert_eq!(int_builder.rows.len(), 1, "integer JSON must produce 1 row");
            let int_row = &int_builder.rows[0];
            let int_stored = int_row.iter().find(|(k, _)| k == "n").map(|(_, v)| v.as_str());
            prop_assert!(int_stored.is_some(), "integer JSON must have field 'n'");
            prop_assert!(
                int_stored.unwrap().starts_with("int:"),
                "integer value must be stored with 'int:' prefix, got {:?}", int_stored
            );

            // Float path
            let float_json = alloc::format!("{{\"n\":{int_val}.{frac:03}}}\n");
            let mut float_builder = TestBuilder::new();
            scan_streaming(float_json.as_bytes(), &config, &mut float_builder);
            prop_assert_eq!(float_builder.rows.len(), 1, "float JSON must produce 1 row");
            let float_row = &float_builder.rows[0];
            let float_stored = float_row.iter().find(|(k, _)| k == "n").map(|(_, v)| v.as_str());
            prop_assert!(float_stored.is_some(), "float JSON must have field 'n'");
            prop_assert!(
                float_stored.unwrap().starts_with("float:"),
                "float value must be stored with 'float:' prefix, got {:?}", float_stored
            );
        }

        /// Duplicate keys: scanner emits values in source order (first-writer-wins).
        ///
        /// When a JSON object contains the same key twice, the scanner must
        /// produce exactly 1 row. The first emitted value for "k" must be v1
        /// (first-writer-wins), matching the dedup behavior of production
        /// builders like `StreamingBuilder::check_dup_bits`.
        #[test]
        fn duplicate_keys_produce_consistent_row(
            v1 in "[a-z]{1,10}",
            v2 in "[a-z]{1,10}",
        ) {
            let json = alloc::format!("{{\"k\":\"{v1}\",\"k\":\"{v2}\"}}\n");
            let config = ScanConfig::default();
            let mut builder = TestBuilder::new();
            scan_streaming(json.as_bytes(), &config, &mut builder);
            prop_assert_eq!(builder.rows.len(), 1, "duplicate-key JSON must produce 1 row");
            let row = &builder.rows[0];
            let k_entries: Vec<&str> = row.iter()
                .filter(|(k, _)| k == "k")
                .map(|(_, v)| v.as_str())
                .collect();
            prop_assert!(
                !k_entries.is_empty(),
                "duplicate-key JSON must have at least one 'k' entry"
            );
            prop_assert_eq!(
                k_entries[0], v1.as_str(),
                "first-writer-wins: first emitted value must be v1 {:?}, got {:?}", v1, k_entries[0]
            );
        }
    }

    /// CRLF line endings must not leak \r into extracted field values or captured line fields.
    ///
    /// When the input uses Windows-style CRLF (\r\n) line endings, the scanner
    /// must strip the \r before storing captured line values and before passing
    /// the line to the JSON parser.
    #[test]
    fn crlf_stripped_from_line_capture_and_fields() {
        // Two JSON lines with CRLF line endings.
        let buf =
            b"{\"level\":\"INFO\",\"msg\":\"hello\"}\r\n{\"level\":\"WARN\",\"msg\":\"world\"}\r\n";

        // extract_all=true captures all fields without listing them explicitly.
        let config = ScanConfig {
            wanted_fields: alloc::vec![],
            extract_all: true,
            line_field_name: Some("body".to_string()),
            validate_utf8: false,
            row_predicate: None,
        };

        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 2, "expected 2 rows");

        // Field values must NOT contain \r.
        for (i, row) in builder.rows.iter().enumerate() {
            for (key, val) in row {
                assert!(
                    !val.contains('\r'),
                    "row {i} field {key:?} contains \\r: {val:?}"
                );
            }
        }

        // Captured line values must NOT contain a trailing \r.
        for (i, raw) in builder.lines.iter().enumerate() {
            let raw = raw
                .as_ref()
                .unwrap_or_else(|| panic!("row {i} has no captured line"));
            assert!(
                !raw.ends_with('\r'),
                "row {i} captured line has trailing \\r: {raw:?}"
            );
            assert!(
                !raw.contains('\r'),
                "row {i} captured line contains \\r: {raw:?}"
            );
        }

        // Verify correct field values were extracted.
        assert_eq!(
            builder.rows[0]
                .iter()
                .find(|(k, _)| k == "level")
                .map(|(_, v)| v.as_str()),
            Some("INFO")
        );
        assert_eq!(
            builder.rows[1]
                .iter()
                .find(|(k, _)| k == "level")
                .map(|(_, v)| v.as_str()),
            Some("WARN")
        );
    }

    /// CRLF-only input (line with just \r\n) must be treated as an empty line.
    #[test]
    fn crlf_only_line_is_empty() {
        let buf = b"\r\n{\"x\":\"1\"}\r\n";
        let config = ScanConfig {
            wanted_fields: alloc::vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: None,
        };
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        // The non-empty line is parsed; the empty CRLF-only line is skipped.
        assert_eq!(
            builder.rows.len(),
            1,
            "expected only 1 row (empty line skipped)"
        );
    }

    /// Unterminated trailing CR after a newline must not emit a spurious empty row.
    #[test]
    fn trailing_cr_after_newline_no_spurious_empty_row_with_line_capture() {
        let buf = b"{\"x\":\"1\"}\n\r";
        let config = ScanConfig {
            wanted_fields: alloc::vec![],
            extract_all: true,
            line_field_name: Some("body".to_string()),
            validate_utf8: false,
            row_predicate: None,
        };
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(builder.rows.len(), 1, "expected only the JSON row");
        assert_eq!(builder.lines.len(), 1, "expected one captured line");
        assert_eq!(builder.lines[0].as_deref(), Some("{\"x\":\"1\"}"));
    }

    /// A buffer containing only `\r` is effectively empty after CRLF normalisation.
    #[test]
    fn lone_carriage_return_emits_no_rows_even_with_line_capture() {
        let buf = b"\r";
        let config = ScanConfig {
            wanted_fields: alloc::vec![],
            extract_all: true,
            line_field_name: Some("body".to_string()),
            validate_utf8: false,
            row_predicate: None,
        };
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);
        assert!(builder.rows.is_empty());
        assert!(builder.lines.is_empty());
    }

    /// A bare LF empty line is skipped even when line capture is enabled.
    /// Empty lines never produce rows — there is no content to capture.
    #[test]
    fn empty_lf_line_skipped_even_with_line_capture() {
        let buf = b"\n{\"x\":\"1\"}\n\n";
        let config = ScanConfig {
            wanted_fields: alloc::vec![],
            extract_all: true,
            line_field_name: Some("body".to_string()),
            validate_utf8: false,
            row_predicate: None,
        };
        let mut builder = TestBuilder::new();
        scan_streaming(buf, &config, &mut builder);

        assert_eq!(
            builder.rows.len(),
            1,
            "only the JSON row, empty lines skipped"
        );
        assert_eq!(builder.lines.len(), 1, "one captured line for the JSON row");
        assert_eq!(builder.lines[0].as_deref(), Some("{\"x\":\"1\"}"));
    }

    /// Regression for #2227: skip_bare_value must stop at `]` just like all
    /// other JSON delimiters. Previously `]` was missing from the stop set,
    /// causing bare values inside arrays to consume the closing bracket.
    #[test]
    fn skip_bare_value_stops_at_close_bracket() {
        // Direct call: a number immediately followed by `]` — skip_bare_value
        // must stop at position 2 (the `]`), not consume it.
        let buf = b"12]more";
        let result = skip_bare_value(buf, 0, buf.len());
        assert_eq!(result, 2, "skip_bare_value must stop at ']'");
        assert_eq!(buf[result], b']', "stopped byte must be ']'");

        // Integration: an array value inside a JSON object should parse correctly.
        // The number in `[1,2]` must not swallow the `]`.
        let input = b"{\"arr\":[1,2]}\n";
        let config = ScanConfig {
            wanted_fields: alloc::vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: None,
        };
        let mut builder = TestBuilder::new();
        scan_streaming(input, &config, &mut builder);
        assert_eq!(
            builder.rows.len(),
            1,
            "one JSON line should produce one row"
        );
    }

    // -----------------------------------------------------------------------
    // Predicate pushdown tests
    // -----------------------------------------------------------------------

    #[test]
    fn predicate_filters_non_matching_rows() {
        use crate::scan_predicate::{CmpOp, ScalarValue, ScanPredicate};

        let input = b"{\"level\":\"info\",\"msg\":\"hello\"}\n{\"level\":\"error\",\"msg\":\"bad\"}\n{\"level\":\"debug\",\"msg\":\"trace\"}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: Some(ScanPredicate::Compare {
                field: "level".into(),
                op: CmpOp::Eq,
                value: ScalarValue::Str("error".into()),
            }),
        };
        let mut builder = TestBuilder::new();
        scan_streaming(input, &config, &mut builder);

        // Only the "error" row should pass the predicate.
        assert_eq!(builder.rows.len(), 1, "predicate should filter to 1 row");
        let row = &builder.rows[0];
        let msg = row
            .iter()
            .find(|(k, _)| k == "msg")
            .map(|(_, v)| v.as_str());
        assert_eq!(msg, Some("bad"), "the error row should have msg=bad");
    }

    #[test]
    fn predicate_passes_all_when_all_match() {
        use crate::scan_predicate::{CmpOp, ScalarValue, ScanPredicate};

        let input = b"{\"level\":\"error\",\"msg\":\"a\"}\n{\"level\":\"error\",\"msg\":\"b\"}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: Some(ScanPredicate::Compare {
                field: "level".into(),
                op: CmpOp::Eq,
                value: ScalarValue::Str("error".into()),
            }),
        };
        let mut builder = TestBuilder::new();
        scan_streaming(input, &config, &mut builder);
        assert_eq!(builder.rows.len(), 2, "all rows should pass");
    }

    #[test]
    fn predicate_filters_all_when_none_match() {
        use crate::scan_predicate::{CmpOp, ScalarValue, ScanPredicate};

        let input = b"{\"level\":\"info\"}\n{\"level\":\"debug\"}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: Some(ScanPredicate::Compare {
                field: "level".into(),
                op: CmpOp::Eq,
                value: ScalarValue::Str("error".into()),
            }),
        };
        let mut builder = TestBuilder::new();
        scan_streaming(input, &config, &mut builder);
        assert_eq!(builder.rows.len(), 0, "no rows should pass");
    }

    #[test]
    fn predicate_with_numeric_comparison() {
        use crate::scan_predicate::{CmpOp, ScalarValue, ScanPredicate};

        let input = b"{\"status\":200,\"msg\":\"ok\"}\n{\"status\":503,\"msg\":\"fail\"}\n{\"status\":404,\"msg\":\"miss\"}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: Some(ScanPredicate::Compare {
                field: "status".into(),
                op: CmpOp::Ge,
                value: ScalarValue::Int(500),
            }),
        };
        let mut builder = TestBuilder::new();
        scan_streaming(input, &config, &mut builder);
        assert_eq!(builder.rows.len(), 1, "only status>=500 should pass");
        let msg = builder.rows[0]
            .iter()
            .find(|(k, _)| k == "msg")
            .map(|(_, v)| v.as_str());
        assert_eq!(msg, Some("fail"));
    }

    #[test]
    fn predicate_and_chain() {
        use crate::scan_predicate::{CmpOp, ScalarValue, ScanPredicate};

        let input = b"{\"level\":\"error\",\"status\":503}\n{\"level\":\"error\",\"status\":200}\n{\"level\":\"info\",\"status\":503}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: Some(ScanPredicate::And(vec![
                ScanPredicate::Compare {
                    field: "level".into(),
                    op: CmpOp::Eq,
                    value: ScalarValue::Str("error".into()),
                },
                ScanPredicate::Compare {
                    field: "status".into(),
                    op: CmpOp::Ge,
                    value: ScalarValue::Int(500),
                },
            ])),
        };
        let mut builder = TestBuilder::new();
        scan_streaming(input, &config, &mut builder);
        assert_eq!(
            builder.rows.len(),
            1,
            "only level=error AND status>=500 should pass"
        );
    }

    #[test]
    fn no_predicate_unchanged_behavior() {
        let input = b"{\"a\":\"1\"}\n{\"a\":\"2\"}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: None,
        };
        let mut builder = TestBuilder::new();
        scan_streaming(input, &config, &mut builder);
        assert_eq!(builder.rows.len(), 2, "no predicate = all rows");
    }

    // -----------------------------------------------------------------------
    // Predicate edge case tests (reviewer-requested coverage)
    // -----------------------------------------------------------------------

    #[test]
    fn predicate_with_escape_crossing_64_byte_boundary() {
        use crate::scan_predicate::{CmpOp, ScalarValue, ScanPredicate};

        // Build a line where the "level" value crosses a 64-byte block boundary
        // by padding the key with enough characters. The escape \" in the value
        // forces the decode path.
        let mut line = String::new();
        line.push_str("{\"");
        // Pad key to push the value near the 64-byte boundary
        for _ in 0..50 {
            line.push('x');
        }
        line.push_str("\":\"pad\",\"level\":\"err\\\"or\"}");
        line.push('\n');

        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: Some(ScanPredicate::Compare {
                field: "level".into(),
                op: CmpOp::Eq,
                value: ScalarValue::Str("err\"or".into()),
            }),
        };
        let mut builder = TestBuilder::new();
        scan_streaming(line.as_bytes(), &config, &mut builder);
        assert_eq!(
            builder.rows.len(),
            1,
            "escaped value should match after decode"
        );
    }

    #[test]
    fn predicate_field_order_independence() {
        use crate::scan_predicate::{CmpOp, ScalarValue, ScanPredicate};

        // Same fields in different order — predicate should work regardless.
        let input =
            b"{\"msg\":\"hello\",\"level\":\"error\"}\n{\"level\":\"info\",\"msg\":\"world\"}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: Some(ScanPredicate::Compare {
                field: "level".into(),
                op: CmpOp::Eq,
                value: ScalarValue::Str("error".into()),
            }),
        };
        let mut builder = TestBuilder::new();
        scan_streaming(input, &config, &mut builder);
        assert_eq!(builder.rows.len(), 1);
        let msg = builder.rows[0]
            .iter()
            .find(|(k, _)| k == "msg")
            .map(|(_, v)| v.as_str());
        assert_eq!(msg, Some("hello"));
    }

    #[test]
    fn predicate_duplicate_keys_first_write_wins() {
        use crate::scan_predicate::{CmpOp, ScalarValue, ScanPredicate};

        // Duplicate "level" key — first value should win for both
        // predicate evaluation and output.
        let input = b"{\"level\":\"error\",\"level\":\"info\"}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: Some(ScanPredicate::Compare {
                field: "level".into(),
                op: CmpOp::Eq,
                value: ScalarValue::Str("error".into()),
            }),
        };
        let mut builder = TestBuilder::new();
        scan_streaming(input, &config, &mut builder);
        // First "level" is "error" → predicate passes.
        assert_eq!(builder.rows.len(), 1);
        let level = builder.rows[0]
            .iter()
            .find(|(k, _)| k == "level")
            .map(|(_, v)| v.as_str());
        assert_eq!(level, Some("error"), "first-write-wins for output");
    }

    #[test]
    fn predicate_duplicate_keys_different_types() {
        use crate::scan_predicate::{CmpOp, ScalarValue, ScanPredicate};

        // Same key appears as string then int — scanner creates conflict columns.
        // Predicate evaluates against the first occurrence (string "200").
        let input = b"{\"status\":\"200\",\"status\":503}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: Some(ScanPredicate::Compare {
                field: "status".into(),
                op: CmpOp::Eq,
                value: ScalarValue::Str("200".into()),
            }),
        };
        let mut builder = TestBuilder::new();
        scan_streaming(input, &config, &mut builder);
        // First "status" is string "200" → predicate matches.
        assert_eq!(builder.rows.len(), 1);
    }

    #[test]
    fn predicate_nested_json_is_not_null() {
        use crate::scan_predicate::ScanPredicate;

        // Nested object should be treated as non-null.
        let input = b"{\"payload\":{\"key\":\"val\"},\"level\":\"error\"}\n{\"level\":\"info\"}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: Some(ScanPredicate::IsNull {
                field: "payload".into(),
                negated: true, // IS NOT NULL
            }),
        };
        let mut builder = TestBuilder::new();
        scan_streaming(input, &config, &mut builder);
        // First row has payload (nested object) → IS NOT NULL passes.
        // Second row has no payload → IS NOT NULL fails.
        assert_eq!(builder.rows.len(), 1);
    }

    #[test]
    fn predicate_cross_type_string_vs_int() {
        use crate::scan_predicate::{CmpOp, ScalarValue, ScanPredicate};

        // JSON has status as string "503", predicate compares as int 503.
        let input = b"{\"status\":\"503\",\"msg\":\"fail\"}\n{\"status\":\"200\",\"msg\":\"ok\"}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: Some(ScanPredicate::Compare {
                field: "status".into(),
                op: CmpOp::Ge,
                value: ScalarValue::Int(500),
            }),
        };
        let mut builder = TestBuilder::new();
        scan_streaming(input, &config, &mut builder);
        // String "503" should coerce to int 503 for comparison.
        assert_eq!(builder.rows.len(), 1);
    }

    #[test]
    fn predicate_in_list_filters_correctly() {
        use crate::scan_predicate::{ScalarValue, ScanPredicate};

        let input = b"{\"level\":\"error\"}\n{\"level\":\"fatal\"}\n{\"level\":\"info\"}\n{\"level\":\"warn\"}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: Some(ScanPredicate::InList {
                field: "level".into(),
                values: vec![
                    ScalarValue::Str("error".into()),
                    ScalarValue::Str("fatal".into()),
                ],
                negated: false,
            }),
        };
        let mut builder = TestBuilder::new();
        scan_streaming(input, &config, &mut builder);
        assert_eq!(
            builder.rows.len(),
            2,
            "only error + fatal should pass IN list"
        );
    }

    #[test]
    fn predicate_contains_substring_match() {
        use crate::scan_predicate::ScanPredicate;

        let input = b"{\"msg\":\"connection timeout after 30s\"}\n{\"msg\":\"request completed\"}\n{\"msg\":\"read timeout\"}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: Some(ScanPredicate::Contains {
                field: "msg".into(),
                substring: "timeout".into(),
            }),
        };
        let mut builder = TestBuilder::new();
        scan_streaming(input, &config, &mut builder);
        assert_eq!(builder.rows.len(), 2, "both timeout messages should pass");
    }

    #[test]
    fn predicate_starts_with_prefix_match() {
        use crate::scan_predicate::ScanPredicate;

        let input =
            b"{\"msg\":\"ERROR: disk full\"}\n{\"msg\":\"INFO: ok\"}\n{\"msg\":\"ERROR: oom\"}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: None,
            validate_utf8: false,
            row_predicate: Some(ScanPredicate::StartsWith {
                field: "msg".into(),
                prefix: "ERROR".into(),
            }),
        };
        let mut builder = TestBuilder::new();
        scan_streaming(input, &config, &mut builder);
        assert_eq!(
            builder.rows.len(),
            2,
            "both ERROR-prefixed messages should pass"
        );
    }

    #[test]
    fn predicate_on_line_capture_field() {
        use crate::scan_predicate::ScanPredicate;

        // When line_field_name is set and predicate references it,
        // the line content should be available for predicate evaluation.
        let input = b"{\"level\":\"error\"}\n{\"level\":\"info\"}\n";
        let config = ScanConfig {
            wanted_fields: vec![],
            extract_all: true,
            line_field_name: Some("body".into()),
            validate_utf8: false,
            row_predicate: Some(ScanPredicate::IsNull {
                field: "body".into(),
                negated: true, // IS NOT NULL
            }),
        };
        let mut builder = TestBuilder::new();
        scan_streaming(input, &config, &mut builder);
        // Both rows have line content → IS NOT NULL should pass both.
        assert_eq!(
            builder.rows.len(),
            2,
            "line capture field should be non-null"
        );
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
    #[kani::solver(cadical)]
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

        // Guard vacuity: verify bounds work correctly
        kani::cover!(result == start, "no whitespace at start");
        kani::cover!(result > start, "skipped some whitespace");
        kani::cover!(result == end, "all whitespace");
    }

    /// skip_bare_value returns a position in [start, end].
    /// All bytes before result are NOT delimiters.
    #[kani::proof]
    #[kani::unwind(17)]
    #[kani::solver(cadical)]
    fn verify_skip_bare_value() {
        let buf: [u8; 16] = kani::any();
        let start: usize = kani::any();
        let end: usize = kani::any();
        kani::assume(start <= end && end <= 16);

        let result = skip_bare_value(&buf, start, end);

        assert!(result >= start && result <= end);
        // When we stopped before end, the stop byte MUST be a delimiter.
        if result < end {
            assert!(
                is_json_delimiter(buf[result]),
                "stop byte must be a JSON delimiter"
            );
        }
        let mut i = start;
        while i < result {
            let b = buf[i];
            assert!(
                !is_json_delimiter(b),
                "byte at {i} is a delimiter: {b:#04x}"
            );
            i += 1;
        }

        // Guard vacuity: verify value scanning works
        kani::cover!(result > start, "found non-delimiter bytes");
        kani::cover!(result == start, "delimiter at start");
        kani::cover!(result == end, "no delimiter found");
        // Explicitly cover the previously-missing ] delimiter path
        kani::cover!(result < end && buf[result] == b']', "stopped at ]");
    }

    /// is_json_delimiter covers all JSON value delimiters exhaustively.
    /// Checks every byte value: only the 7 expected delimiters return true.
    #[kani::proof]
    #[kani::solver(cadical)]
    fn verify_is_json_delimiter_exhaustive() {
        let b: u8 = kani::any();
        let result = is_json_delimiter(b);
        let expected = b == b','
            || b == b'}'
            || b == b']'
            || b == b' '
            || b == b'\t'
            || b == b'\r'
            || b == b'\n';
        assert!(result == expected);
    }

    /// skip_nested: result is always in [pos, end], and for a single
    /// balanced pair the returned position is past the closer.
    ///
    /// Keep this harness on a small fixed buffer: the new >32-depth overflow
    /// behavior is covered by unit tests and proptests above, while Kani here
    /// focuses on local crash-freedom and bounds without exploding the search.
    #[kani::proof]
    #[kani::unwind(10)]
    #[kani::solver(cadical)]
    fn verify_skip_nested_bounds() {
        let buf: [u8; 8] = kani::any();
        let pos: usize = kani::any();
        let end: usize = kani::any();
        kani::assume(pos <= end && end <= 8);

        // Build bitmasks for this small buffer (1 block)
        let mut rq = [0u64; 1];
        let mut ob = [0u64; 1];
        let mut cb = [0u64; 1];
        let mut obrk = [0u64; 1];
        let mut cbrk = [0u64; 1];
        let mut i: usize = 0;
        while i < 8 {
            let mask = 1u64 << i;
            match buf[i] {
                b'"' => rq[0] |= mask,
                b'{' => ob[0] |= mask,
                b'}' => cb[0] |= mask,
                b'[' => obrk[0] |= mask,
                b']' => cbrk[0] |= mask,
                _ => {}
            }
            i += 1;
        }

        let blocks = StoredBitmasks {
            real_quotes: &rq,
            open_brace: &ob,
            close_brace: &cb,
            open_bracket: &obrk,
            close_bracket: &cbrk,
        };

        let result = skip_nested(&buf, pos, end, &blocks);
        assert!(result >= pos && result <= end);

        // Guard vacuity: verify nested structure handling
        kani::cover!(result > pos, "skipped nested structure");
        kani::cover!(result == pos, "no structure to skip");
        kani::cover!(result == end, "structure extends to end");
    }

    /// next_quote on a single block: if found, the position has a set bit
    /// in the bitmask. If not found, no bits are set in [from, end).
    #[kani::proof]
    #[kani::unwind(66)] // inner None-branch loop: up to 64 iterations (end≤64) + 2 margin
    #[kani::solver(kissat)]
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
        kani::cover!(result.is_some(), "quote found in range");
        kani::cover!(result.is_none(), "no quote in range");
    }

    /// `parse_hex4` correctness: exhaustive oracle check over all valid 4-hex-digit inputs.
    ///
    /// For every input that consists of exactly 4 ASCII hex characters, the
    /// output must equal the value obtained by parsing each hex nibble manually.
    /// For any byte that is not a hex digit, `parse_hex4` must return `None`.
    #[kani::proof]
    fn verify_parse_hex4_correctness() {
        let bytes: [u8; 4] = kani::any();

        /// True iff `b` is a valid ASCII hex digit.
        fn is_hex(b: u8) -> bool {
            matches!(b, b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F')
        }

        fn hex_val(b: u8) -> u16 {
            match b {
                b'0'..=b'9' => (b - b'0') as u16,
                b'a'..=b'f' => (b - b'a' + 10) as u16,
                b'A'..=b'F' => (b - b'A' + 10) as u16,
                _ => unreachable!(),
            }
        }

        let result = parse_hex4(&bytes);

        if bytes.iter().all(|&b| is_hex(b)) {
            // All four bytes are hex digits → must return Some with the correct value
            let expected = (hex_val(bytes[0]) << 12)
                | (hex_val(bytes[1]) << 8)
                | (hex_val(bytes[2]) << 4)
                | hex_val(bytes[3]);
            assert_eq!(
                result,
                Some(expected),
                "parse_hex4 must return correct value for valid input"
            );
        } else {
            // At least one non-hex byte → must return None
            assert_eq!(
                result, None,
                "parse_hex4 must return None for invalid input"
            );
        }

        kani::cover!(result.is_some(), "valid hex input");
        kani::cover!(result.is_none(), "invalid hex input");
    }

    /// `decode_json_escapes` RFC 8259 correctness for all 8 standard single-char escapes.
    ///
    /// For each recognized escape byte (`"`, `\`, `/`, `b`, `f`, `n`, `r`, `t`),
    /// the function must decode `\X` to exactly the correct output byte.
    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_decode_json_escapes_rfc8259_correctness() {
        let escape_char: u8 = kani::any();
        // Constrain to the 8 standard RFC 8259 single-char escapes (excludes `u`)
        kani::assume(matches!(
            escape_char,
            b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't'
        ));

        let input = [b'\\', escape_char];
        let mut out = alloc::vec::Vec::with_capacity(2);
        decode_json_escapes(&input, &mut out);

        let expected: u8 = match escape_char {
            b'"' => b'"',
            b'\\' => b'\\',
            b'/' => b'/',
            b'b' => 0x08,
            b'f' => 0x0C,
            b'n' => b'\n',
            b'r' => b'\r',
            b't' => b'\t',
            _ => unreachable!(),
        };

        assert_eq!(
            out.len(),
            1,
            "standard escape must decode to exactly 1 byte"
        );
        assert_eq!(
            out[0], expected,
            "standard escape must decode to correct byte"
        );

        kani::cover!(escape_char == b'"', "double-quote escape");
        kani::cover!(escape_char == b'n', "newline escape");
    }

    /// `decode_json_escapes` passes through unknown escapes unchanged.
    ///
    /// For any byte that is not a recognized RFC 8259 escape, `\X` must be
    /// passed through literally as `\X` (two bytes).
    #[kani::proof]
    #[kani::unwind(4)]
    fn verify_decode_json_escapes_unknown_passthrough() {
        let escape_char: u8 = kani::any();
        // Exclude all recognized escapes (standard single-char + u for unicode)
        kani::assume(!matches!(
            escape_char,
            b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' | b'u'
        ));

        let input = [b'\\', escape_char];
        let mut out = alloc::vec::Vec::with_capacity(2);
        decode_json_escapes(&input, &mut out);

        // Unknown escape: pass through the backslash, then continue.
        // The escape_char byte is processed in the next iteration as a literal.
        // So output is [b'\\', escape_char].
        assert_eq!(out.len(), 2, "unknown escape must pass through 2 bytes");
        assert_eq!(out[0], b'\\', "first byte must be backslash");
        assert_eq!(
            out[1], escape_char,
            "second byte must be the original escape char"
        );

        kani::cover!(escape_char == b'x', "non-standard \\x passthrough");
        kani::cover!(escape_char == b' ', "whitespace passthrough");
    }

    /// `decode_json_escapes` never panics on arbitrary short inputs.
    ///
    /// Crash-freedom proof: for any 8-byte input, the function must return
    /// normally (no panic, no OOB access, no infinite loop).
    #[kani::proof]
    #[kani::unwind(16)]
    fn verify_decode_json_escapes_no_panic() {
        let input: [u8; 8] = kani::any();
        let mut out = alloc::vec::Vec::with_capacity(36);
        decode_json_escapes(&input, &mut out);
        // The function must not panic. Output length ≤ input length × 4 + 4
        // (worst case: \uXXXX can expand to 4 UTF-8 bytes from 6 input bytes,
        // plus trailing partial escapes can add a few extra bytes).
        assert!(out.len() <= input.len() * 4 + 4);
    }

    /// `decode_unicode_escape` always advances past its input position.
    ///
    /// Whatever the input, the returned position must be strictly greater
    /// than `pos` (no infinite loop / stuck cursor), and the output must be
    /// non-empty.
    #[kani::proof]
    #[kani::unwind(16)]
    fn verify_decode_unicode_escape_advance() {
        let input: [u8; 14] = kani::any();
        let pos: usize = kani::any_where(|&p: &usize| p < 14);

        let mut out = alloc::vec::Vec::with_capacity(14);
        let new_pos = decode_unicode_escape(&input, pos, &mut out);

        // Must always advance (never return pos itself)
        assert!(new_pos > pos, "decode_unicode_escape must advance past pos");

        // Must always produce at least 1 output byte
        assert!(
            !out.is_empty(),
            "decode_unicode_escape must write at least 1 byte"
        );

        kani::cover!(new_pos == pos + 1, "short/invalid: advance by 1");
        kani::cover!(new_pos == pos + 6, "BMP or lone surrogate: advance by 6");
        kani::cover!(new_pos == pos + 12, "surrogate pair: advance by 12");
    }
}

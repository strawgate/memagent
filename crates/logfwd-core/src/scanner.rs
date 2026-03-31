// scanner.rs — Generic JSON-to-columnar scan loop.
//
// Provides the ScanBuilder trait and the scan_into/scan_line functions.
// Arrow-specific scanner types (SimdScanner, StreamingSimdScanner) live
// in the logfwd-arrow crate.

use crate::scan_config::ScanConfig;
use crate::scan_config::parse_int_fast;
use crate::structural::StructuralIndex;

// ---------------------------------------------------------------------------
// ScanBuilder trait — shared interface for both builders
// ---------------------------------------------------------------------------

/// Trait for building columnar output from scanned JSON fields.
///
/// Implementors receive field-level callbacks as the scanner walks JSON
/// objects. The call sequence per batch is:
///
/// ```text
/// begin_batch()
///   begin_row()
///     resolve_field(key) → idx
///     append_str_by_idx(idx, value)   // or int, float, null
///     ...more fields...
///   end_row()
///   ...more rows...
/// // caller invokes finish on the implementor directly
/// ```
///
/// - `resolve_field`: maps a field name to a column index. Must be stable
///   within a batch (same key → same index). May create new columns.
/// - `append_*_by_idx`: stores a typed value at the current row for the
///   given column index. Byte slices are borrowed from the input buffer.
/// - `append_raw`: stores the entire unparsed line (if `keep_raw` is set).
/// - First-write-wins: if a key appears twice in one row, the first value
///   is kept and subsequent writes are silently ignored.
///
/// Implementations live in `logfwd-arrow` (`StorageBuilder`, `StreamingBuilder`).
pub trait ScanBuilder {
    /// Initialize state for a new batch.
    fn begin_batch(&mut self);
    /// Start a new row.
    fn begin_row(&mut self);
    /// Finish the current row.
    fn end_row(&mut self);
    /// Resolve a field name to a column index.
    fn resolve_field(&mut self, key: &[u8]) -> usize;
    /// Append a string value at the given column index.
    fn append_str_by_idx(&mut self, idx: usize, value: &[u8]);
    /// Append an integer value (as raw ASCII digits) at the given column index.
    fn append_int_by_idx(&mut self, idx: usize, value: &[u8]);
    /// Append a float value (as raw ASCII) at the given column index.
    fn append_float_by_idx(&mut self, idx: usize, value: &[u8]);
    /// Record a null value at the given column index.
    fn append_null_by_idx(&mut self, idx: usize);
    /// Store the raw unparsed line (only called when `keep_raw` is set).
    fn append_raw(&mut self, line: &[u8]);
}

// ---------------------------------------------------------------------------
// Generic scan loop
// ---------------------------------------------------------------------------

/// Scan an NDJSON buffer, extracting fields into a `ScanBuilder`.
///
/// Processes the buffer in two stages:
/// 1. SIMD structural detection (`StructuralIndex`) identifies all structural
///    character positions and newline boundaries in one pass
/// 2. Scalar field extraction walks JSON objects and dispatches to the builder
///
/// # Preconditions
/// - `buf` must be valid UTF-8 (debug-asserted, not checked in release)
/// - Lines are newline-delimited (`\n`)
///
/// # Type parameter
/// - `B`: Any implementation of `ScanBuilder` (e.g., `StorageBuilder`,
///   `StreamingBuilder` from `logfwd-arrow`)
#[inline(never)]
pub fn scan_into<B: ScanBuilder>(buf: &[u8], config: &ScanConfig, builder: &mut B) {
    debug_assert!(
        std::str::from_utf8(buf).is_ok(),
        "Scanner input must be valid UTF-8"
    );
    let (index, line_ranges) = StructuralIndex::new(buf);
    builder.begin_batch();
    for (start, end) in line_ranges {
        scan_line(buf, start, end, &index, config, builder);
    }
}

#[inline]
fn scan_line<B: ScanBuilder>(
    buf: &[u8],
    start: usize,
    end: usize,
    index: &StructuralIndex,
    config: &ScanConfig,
    builder: &mut B,
) {
    builder.begin_row();
    if config.keep_raw {
        builder.append_raw(&buf[start..end]);
    }

    let mut pos = skip_ws(buf, start, end);
    if pos >= end || buf[pos] != b'{' {
        builder.end_row();
        return;
    }
    pos += 1;

    loop {
        pos = skip_ws(buf, pos, end);
        if pos >= end || buf[pos] == b'}' {
            break;
        }
        if buf[pos] != b'"' {
            break;
        }
        let (key, after_key) = match index.scan_string(buf, pos) {
            Some(r) => r,
            None => break,
        };
        pos = after_key;
        pos = skip_ws(buf, pos, end);
        if pos >= end || buf[pos] != b':' {
            break;
        }
        pos += 1;
        pos = skip_ws(buf, pos, end);
        if pos >= end {
            break;
        }

        let wanted = config.is_wanted(key);
        match buf[pos] {
            b'"' => {
                let (val, after) = match index.scan_string(buf, pos) {
                    Some(r) => r,
                    None => break,
                };
                if wanted {
                    let idx = builder.resolve_field(key);
                    builder.append_str_by_idx(idx, val);
                }
                pos = after;
            }
            b'{' | b'[' => {
                let s = pos;
                pos = index.skip_nested(buf, pos).min(end);
                if wanted {
                    let idx = builder.resolve_field(key);
                    builder.append_str_by_idx(idx, &buf[s..pos]);
                }
            }
            b't' | b'f' => {
                let s = pos;
                while pos < end
                    && buf[pos] != b','
                    && buf[pos] != b'}'
                    && buf[pos] != b' '
                    && buf[pos] != b'\t'
                    && buf[pos] != b'\r'
                {
                    pos += 1;
                }
                if wanted {
                    let idx = builder.resolve_field(key);
                    builder.append_str_by_idx(idx, &buf[s..pos]);
                }
            }
            b'n' => {
                // Scan past the null/identifier token to the next delimiter.
                while pos < end
                    && buf[pos] != b','
                    && buf[pos] != b'}'
                    && buf[pos] != b' '
                    && buf[pos] != b'\t'
                    && buf[pos] != b'\r'
                {
                    pos += 1;
                }
                if wanted {
                    let idx = builder.resolve_field(key);
                    builder.append_null_by_idx(idx);
                }
            }
            _ => {
                let s = pos;
                let mut is_float = false;
                while pos < end {
                    let c = buf[pos];
                    if c == b'.' || c == b'e' || c == b'E' {
                        is_float = true;
                    } else if c == b','
                        || c == b'}'
                        || c == b' '
                        || c == b'\t'
                        || c == b'\n'
                        || c == b'\r'
                    {
                        break;
                    }
                    pos += 1;
                }
                if wanted {
                    let idx = builder.resolve_field(key);
                    let val = &buf[s..pos];
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
        pos = skip_ws(buf, pos, end);
        if pos < end && buf[pos] == b',' {
            pos += 1;
        }
    }
    builder.end_row();
}

#[inline(always)]
fn skip_ws(buf: &[u8], mut pos: usize, end: usize) -> usize {
    while pos < end {
        match buf[pos] {
            b' ' | b'\t' | b'\r' | b'\n' => pos += 1,
            _ => break,
        }
    }
    pos
}

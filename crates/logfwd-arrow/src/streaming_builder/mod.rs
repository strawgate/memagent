// streaming_builder -- Zero-copy hot-path builder.
//
// Uses Arrow's StringViewArray to store string values as 16-byte views
// pointing directly into the input buffer. No string copies during scanning
// or during finish_batch/finish_batch_detached. The input buffer (as bytes::Bytes) is reference-counted
// and shared between the builder and the resulting Arrow arrays.
//
// For the hot pipeline path: scan -> query -> output -> discard.

//! Zero-copy Arrow RecordBatch builder for the hot pipeline path.

mod finish;
#[cfg(test)]
mod tests;
#[cfg(kani)]
mod verification;

use std::collections::HashMap;

use arrow::array::StringViewBuilder;
use arrow::error::ArrowError;
use logfwd_lint_attrs::hot_path;

use logfwd_core::scan_config::{parse_float_fast, parse_int_fast};
use logfwd_core::scanner::BuilderState;

use logfwd_types::field_names;

use crate::check_dup_bits;
use crate::columnar::row_protocol::RowLifecycle;

#[cfg(kani)]
type FieldIndexMap = std::collections::BTreeMap<Vec<u8>, usize>;
#[cfg(not(kani))]
type FieldIndexMap = HashMap<Vec<u8>, usize>;

#[cfg(kani)]
type EmittedNameSet = std::collections::BTreeSet<String>;
#[cfg(not(kani))]
type EmittedNameSet = std::collections::HashSet<String>;

#[cfg(kani)]
fn new_field_index() -> FieldIndexMap {
    std::collections::BTreeMap::new()
}

#[cfg(not(kani))]
fn new_field_index() -> FieldIndexMap {
    HashMap::with_capacity(32)
}

fn new_emitted_name_set() -> EmittedNameSet {
    EmittedNameSet::new()
}

pub(crate) fn append_string_view(
    builder: &mut StringViewBuilder,
    original_block: u32,
    decoded_block: Option<u32>,
    original_len: u32,
    offset: u32,
    len: u32,
) -> Result<(), ArrowError> {
    if offset < original_len || (len == 0 && offset == original_len) {
        builder.try_append_view(original_block, offset, len)
    } else if let Some(decoded_block) = decoded_block {
        let decoded_offset = offset.checked_sub(original_len).ok_or_else(|| {
            ArrowError::InvalidArgumentError("decoded string offset underflow".to_string())
        })?;
        builder.try_append_view(decoded_block, decoded_offset, len)
    } else {
        Err(ArrowError::InvalidArgumentError(format!(
            "string view offset {offset} requires decoded buffer, but decoded buffer is absent"
        )))
    }
}

// ---------------------------------------------------------------------------
// Per-field state
// ---------------------------------------------------------------------------

pub(crate) struct FieldColumns {
    pub(crate) name: Vec<u8>,
    /// String values: (row, offset_in_buffer, len). Views into the shared
    /// buffer. Offsets `< buf.len()` reference the original input buffer;
    /// offsets `>= buf.len()` reference the decoded-strings buffer at
    /// `offset - buf.len()`. See `StreamingBuilder::decoded_buf`.
    pub(crate) str_views: Vec<(u32, u32, u32)>,
    /// Int values: (row, parsed_value).
    pub(crate) int_values: Vec<(u32, i64)>,
    /// Float values: (row, parsed_value).
    pub(crate) float_values: Vec<(u32, f64)>,
    /// Bool values: (row, value).
    pub(crate) bool_values: Vec<(u32, bool)>,
    pub(crate) has_str: bool,
    pub(crate) has_int: bool,
    pub(crate) has_float: bool,
    pub(crate) has_bool: bool,
    /// The last row this field was written to, used for dedup when idx >= 64.
    pub(crate) last_row: u32,
}

impl FieldColumns {
    pub(crate) fn new(name: &[u8]) -> Self {
        FieldColumns {
            name: name.to_vec(),
            str_views: Vec::with_capacity(256),
            int_values: Vec::with_capacity(256),
            float_values: Vec::with_capacity(256),
            bool_values: Vec::with_capacity(256),
            has_str: false,
            has_int: false,
            has_float: false,
            has_bool: false,
            last_row: u32::MAX,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.str_views.clear();
        self.int_values.clear();
        self.float_values.clear();
        self.bool_values.clear();
        self.has_str = false;
        self.has_int = false;
        self.has_float = false;
        self.has_bool = false;
        self.last_row = u32::MAX;
    }
}

// ---------------------------------------------------------------------------
// StreamingBuilder
// ---------------------------------------------------------------------------

/// Zero-copy builder for the hot pipeline path.
///
/// String values are stored as (offset, len) views into the input buffer.
/// The buffer is reference-counted via `bytes::Bytes` and shared with the
/// resulting Arrow `StringViewArray` columns -- no string copies at any point.
///
/// Numeric values (int, float) are parsed during scanning and stored directly.
///
/// When `line_field_name` is set, that column is emitted in each batch as a
/// zero-copy `StringViewArray` containing the full unparsed line per row.
///
/// # Usage
/// ```ignore
/// let mut builder = StreamingBuilder::new(None);
/// builder.begin_batch(bytes::Bytes::from(buf));
/// // ... scan fields, call append_*_by_idx ...
/// let batch = builder.finish_batch();
/// ```
pub struct StreamingBuilder {
    pub(crate) fields: Vec<FieldColumns>,
    pub(crate) field_index: FieldIndexMap,
    /// Number of fields active in the current batch. Slots `0..num_active` in
    /// `fields` are in use; slots beyond that are pre-allocated but dormant.
    /// Resetting this to zero on `begin_batch` — together with clearing
    /// `field_index` — bounds `fields.len()` to the high-water mark of unique
    /// fields seen in any *single* batch rather than growing without limit.
    pub(crate) num_active: usize,
    /// Row lifecycle state machine: batch/row phase, row count, dedup bits.
    pub(crate) lifecycle: RowLifecycle,
    /// Reference-counted buffer. Stored here to compute offsets safely
    /// and shared with Arrow StringViewArrays in finish_batch.
    pub(crate) buf: bytes::Bytes,
    /// Secondary buffer for decoded string values (JSON escape sequences).
    /// String views with offsets `>= buf.len()` reference this buffer at
    /// `offset - buf.len()`. Allocated lazily; empty when no escapes are decoded.
    pub(crate) decoded_buf: Vec<u8>,
    /// Optional output column name used for full-line capture.
    pub(crate) line_field_name: Option<String>,
    /// Line views: (offset_in_buf, len) per row, in row order.
    /// Populated only when `line_field_name` is set.
    pub(crate) line_views: Vec<(u32, u32)>,
    /// Constant per-row resource attributes emitted as resource attribute columns.
    pub(crate) resource_attrs: Vec<(String, String)>,
}

impl Default for StreamingBuilder {
    fn default() -> Self {
        Self::new(None)
    }
}

impl StreamingBuilder {
    pub fn new(line_field_name: Option<String>) -> Self {
        StreamingBuilder {
            fields: Vec::with_capacity(32),
            field_index: new_field_index(),
            num_active: 0,
            lifecycle: RowLifecycle::new(),
            buf: bytes::Bytes::new(),
            decoded_buf: Vec::new(),
            line_field_name,
            line_views: Vec::new(),
            resource_attrs: Vec::new(),
        }
    }

    /// Configure constant per-row resource attributes for subsequent batches.
    pub fn set_resource_attributes(&mut self, attrs: &[(String, String)]) {
        self.resource_attrs.clear();
        self.resource_attrs.extend(attrs.iter().cloned());
    }

    pub(crate) fn resource_col_name(key: &str) -> String {
        // Canonical prefix + verbatim key. No mangling.
        format!("{}{key}", field_names::DEFAULT_RESOURCE_PREFIX)
    }

    /// Start a new batch. Takes ownership of the input buffer via Bytes
    /// (zero-copy if converted from Vec via `Bytes::from(vec)`).
    ///
    /// # Panics
    /// Asserts that `buf.len()` fits in a u32, because string-view
    /// offsets are stored as u32. Buffers larger than 4 GiB would produce
    /// silently truncated offsets without this guard.
    pub fn begin_batch(&mut self, buf: bytes::Bytes) {
        debug_assert!(
            matches!(
                self.lifecycle.state(),
                BuilderState::Idle | BuilderState::InBatch
            ),
            "begin_batch called while inside a row (missing end_row)"
        );
        assert!(
            u32::try_from(buf.len()).is_ok(),
            "StreamingBuilder buffer too large for u32 offsets ({} bytes)",
            buf.len()
        );
        self.buf = buf;
        self.decoded_buf.clear();
        self.lifecycle.begin_batch();
        // Only clear the slots that were active in the previous batch.
        // This preserves the inner-Vec capacity of each FieldColumns for
        // hot-path reuse while still bounding memory under key churn.
        for fc in &mut self.fields[..self.num_active] {
            fc.clear();
        }
        // Discard all name→index mappings so resolve_field rebuilds them from
        // scratch.  Combined with resetting num_active, this prevents the
        // HashMap and the fields Vec from growing across batches with churning
        // field names (issue: field_index HashMap grows unboundedly).
        self.field_index.clear();
        self.num_active = 0;
        self.line_views.clear();
    }

    #[hot_path]
    #[inline(always)]
    pub fn begin_row(&mut self) {
        self.lifecycle.begin_row();
    }

    #[hot_path]
    #[inline(always)]
    pub fn end_row(&mut self) {
        self.lifecycle.end_row();
    }

    #[inline]
    pub fn resolve_field(&mut self, key: &[u8]) -> usize {
        debug_assert!(
            self.lifecycle.state() == BuilderState::InBatch
                || self.lifecycle.state() == BuilderState::InRow,
            "resolve_field called outside of an active batch"
        );
        if let Some(&idx) = self.field_index.get(key) {
            return idx;
        }
        let idx = self.num_active;
        if idx < self.fields.len() {
            // Reuse an existing slot — its data was cleared in begin_batch.
            // Only the name needs updating.
            self.fields[idx].name.clear();
            self.fields[idx].name.extend_from_slice(key);
        } else {
            self.fields.push(FieldColumns::new(key));
        }
        self.num_active += 1;
        self.field_index.insert(key.to_vec(), idx);

        idx
    }

    /// Compute the byte offset of `value` within `self.buf`.
    ///
    /// `value` must be a subslice of `self.buf`; i.e. both its pointer and
    /// length lie within `self.buf` bounds. We use `usize` subtraction rather
    /// than `offset_from` to avoid that API's stricter UB preconditions.
    ///
    /// # Panics
    /// Panics if `value` does not lie entirely within `self.buf`, or if the
    /// computed byte offset exceeds `u32::MAX` (buffer larger than 4 GiB).
    /// These checks fire in both debug and release builds to prevent silent
    /// StringView data corruption.
    #[inline(always)]
    fn offset_of(&self, value: &[u8]) -> u32 {
        let base = self.buf.as_ptr() as usize;
        let ptr = value.as_ptr() as usize;
        assert!(
            ptr >= base && ptr + value.len() <= base + self.buf.len(),
            "value must be within buffer bounds"
        );
        let offset = ptr - base;
        u32::try_from(offset).unwrap_or_else(|_| {
            panic!("StreamingBuilder buffer offset exceeds u32::MAX ({offset} bytes)")
        })
    }

    #[inline(always)]
    pub fn append_str_by_idx(&mut self, idx: usize, value: &[u8]) {
        debug_assert_eq!(
            self.lifecycle.state(),
            BuilderState::InRow,
            "append_str_by_idx called outside of a row"
        );
        if check_dup_bits(self.lifecycle.written_bits_mut(), idx) {
            return;
        }
        if idx >= u64::BITS as usize && self.fields[idx].last_row == self.lifecycle.row_count() {
            return;
        }
        // StringViewArray requires valid UTF-8.  JSON is always UTF-8 in
        // production; for fuzz / corrupted input we skip non-UTF-8 bytes so
        // that append_view_unchecked is never called on invalid bytes.
        if std::str::from_utf8(value).is_err() {
            return;
        }
        let Ok(len) = u32::try_from(value.len()) else {
            return;
        };
        if idx >= u64::BITS as usize {
            self.fields[idx].last_row = self.lifecycle.row_count();
        }
        let offset = self.offset_of(value);
        let fc = &mut self.fields[idx];
        fc.has_str = true;
        fc.str_views.push((self.lifecycle.row_count(), offset, len));
    }

    #[inline(always)]
    pub fn append_validated_str_by_idx(&mut self, idx: usize, value: &[u8]) {
        self.append_str_by_idx(idx, value);
    }

    /// Append a decoded string value that is NOT a subslice of the input
    /// buffer. Used for strings whose JSON escape sequences have been decoded
    /// (see issue #410). Appends the bytes to `decoded_buf` and records a
    /// view in the same `str_views` vector as regular strings, with the
    /// offset shifted by `buf.len()` so that `finish_batch` can select the
    /// decoded Arrow StringView block without copying the original input.
    #[inline(always)]
    pub fn append_decoded_str_by_idx(&mut self, idx: usize, value: &[u8]) {
        debug_assert_eq!(
            self.lifecycle.state(),
            BuilderState::InRow,
            "append_decoded_str_by_idx called outside of a row"
        );
        if check_dup_bits(self.lifecycle.written_bits_mut(), idx) {
            return;
        }
        if idx >= u64::BITS as usize && self.fields[idx].last_row == self.lifecycle.row_count() {
            return;
        }
        if std::str::from_utf8(value).is_err() {
            return;
        }
        self.append_decoded_str_inner(idx, value);
    }

    /// Append a pre-validated string (e.g. a prost `String` or `&str`) as a
    /// decoded field value without re-running the UTF-8 validation check.
    ///
    /// **Caller contract**: `value` must be valid UTF-8. Violating this produces
    /// corrupt Arrow arrays (undefined behaviour in downstream consumers).
    #[inline(always)]
    pub fn append_prevalidated_str_by_idx(&mut self, idx: usize, value: &str) {
        debug_assert_eq!(
            self.lifecycle.state(),
            BuilderState::InRow,
            "append_prevalidated_str_by_idx called outside of a row"
        );
        if check_dup_bits(self.lifecycle.written_bits_mut(), idx) {
            return;
        }
        if idx >= u64::BITS as usize && self.fields[idx].last_row == self.lifecycle.row_count() {
            return;
        }
        self.append_decoded_str_inner(idx, value.as_bytes());
    }

    /// Inner append path shared by `append_decoded_str_by_idx` (post-validation)
    /// and `append_prevalidated_str_by_idx` (type-guaranteed UTF-8).
    #[inline(always)]
    fn append_decoded_str_inner(&mut self, idx: usize, value: &[u8]) {
        let Ok(len) = u32::try_from(value.len()) else {
            return;
        };
        // Compute both offsets before mutating decoded_buf so that a bail-out
        // on overflow does not leave unreferenced bytes in decoded_buf.
        let Ok(decoded_offset) = u32::try_from(self.decoded_buf.len()) else {
            // decoded_buf has grown past 4 GiB; drop this field rather than panic.
            return;
        };
        // Offset into the combined buffer: original buf bytes come first,
        // decoded bytes follow at buf.len() + decoded_offset.
        let Some(combined_offset) = u32::try_from(self.buf.len())
            .ok()
            .and_then(|buf_len| buf_len.checked_add(decoded_offset))
        else {
            // Combined offset would overflow u32; drop this field rather than panic.
            return;
        };
        // All validation passed — safe to update dedup guard and extend decoded_buf.
        if idx >= u64::BITS as usize {
            self.fields[idx].last_row = self.lifecycle.row_count();
        }
        self.decoded_buf.extend_from_slice(value);
        let fc = &mut self.fields[idx];
        fc.has_str = true;
        fc.str_views
            .push((self.lifecycle.row_count(), combined_offset, len));
    }

    #[inline(always)]
    pub fn append_validated_decoded_str_by_idx(&mut self, idx: usize, value: &[u8]) {
        self.append_decoded_str_by_idx(idx, value);
    }

    #[inline(always)]
    pub fn append_int_by_idx(&mut self, idx: usize, value: &[u8]) {
        debug_assert_eq!(
            self.lifecycle.state(),
            BuilderState::InRow,
            "append_int_by_idx called outside of a row"
        );
        if check_dup_bits(self.lifecycle.written_bits_mut(), idx) {
            return;
        }
        let fc = &mut self.fields[idx];
        if idx >= 64 {
            if fc.last_row == self.lifecycle.row_count() {
                return;
            }
            fc.last_row = self.lifecycle.row_count();
        }
        if let Some(v) = parse_int_fast(value) {
            fc.has_int = true;
            fc.int_values.push((self.lifecycle.row_count(), v));
        }
    }

    #[inline(always)]
    pub fn append_i64_value_by_idx(&mut self, idx: usize, value: i64) {
        debug_assert_eq!(
            self.lifecycle.state(),
            BuilderState::InRow,
            "append_i64_value_by_idx called outside of a row"
        );
        if check_dup_bits(self.lifecycle.written_bits_mut(), idx) {
            return;
        }
        let fc = &mut self.fields[idx];
        if idx >= 64 {
            if fc.last_row == self.lifecycle.row_count() {
                return;
            }
            fc.last_row = self.lifecycle.row_count();
        }
        fc.has_int = true;
        fc.int_values.push((self.lifecycle.row_count(), value));
    }

    #[inline(always)]
    pub fn append_float_by_idx(&mut self, idx: usize, value: &[u8]) {
        debug_assert_eq!(
            self.lifecycle.state(),
            BuilderState::InRow,
            "append_float_by_idx called outside of a row"
        );
        if check_dup_bits(self.lifecycle.written_bits_mut(), idx) {
            return;
        }
        let fc = &mut self.fields[idx];
        if idx >= 64 {
            if fc.last_row == self.lifecycle.row_count() {
                return;
            }
            fc.last_row = self.lifecycle.row_count();
        }
        if let Some(v) = parse_float_fast(value) {
            fc.has_float = true;
            fc.float_values.push((self.lifecycle.row_count(), v));
        }
    }

    #[inline(always)]
    pub fn append_f64_value_by_idx(&mut self, idx: usize, value: f64) {
        debug_assert_eq!(
            self.lifecycle.state(),
            BuilderState::InRow,
            "append_f64_value_by_idx called outside of a row"
        );
        if check_dup_bits(self.lifecycle.written_bits_mut(), idx) {
            return;
        }
        let fc = &mut self.fields[idx];
        if idx >= 64 {
            if fc.last_row == self.lifecycle.row_count() {
                return;
            }
            fc.last_row = self.lifecycle.row_count();
        }
        fc.has_float = true;
        fc.float_values.push((self.lifecycle.row_count(), value));
    }

    #[inline(always)]
    pub fn append_bool_by_idx(&mut self, idx: usize, value: bool) {
        debug_assert_eq!(
            self.lifecycle.state(),
            BuilderState::InRow,
            "append_bool_by_idx called outside of a row"
        );
        if check_dup_bits(self.lifecycle.written_bits_mut(), idx) {
            return;
        }
        let fc = &mut self.fields[idx];
        if idx >= 64 {
            if fc.last_row == self.lifecycle.row_count() {
                return;
            }
            fc.last_row = self.lifecycle.row_count();
        }
        fc.has_bool = true;
        fc.bool_values.push((self.lifecycle.row_count(), value));
    }

    #[inline(always)]
    pub fn append_null_by_idx(&mut self, idx: usize) {
        debug_assert_eq!(
            self.lifecycle.state(),
            BuilderState::InRow,
            "append_null_by_idx called outside of a row"
        );
        // Nulls are represented by gaps -- no value record needed.
        // But mark as written for duplicate-key detection.
        let _ = check_dup_bits(self.lifecycle.written_bits_mut(), idx);

        let fc = &mut self.fields[idx];
        if idx >= 64 {
            fc.last_row = self.lifecycle.row_count();
        }
    }

    /// Store a zero-copy view of the raw unparsed line.
    ///
    /// Only has effect when the builder was created with line capture enabled.
    /// The line must be a subslice of the buffer passed to `begin_batch`.
    ///
    /// First writer wins: if called multiple times in the same row, only the
    /// first call has effect. This maintains the invariant that `line_views`
    /// has exactly one entry per row when `line_capture` is enabled.
    #[inline(always)]
    pub fn append_line(&mut self, line: &[u8]) {
        debug_assert_eq!(
            self.lifecycle.state(),
            BuilderState::InRow,
            "append_line called outside of a row"
        );
        if self.line_field_name.is_some() && !self.lifecycle.line_written_this_row() {
            let line_view = if std::str::from_utf8(line).is_ok() {
                let Ok(line_len) = u32::try_from(line.len()) else {
                    return;
                };
                Some((self.offset_of(line), line_len))
            } else {
                let lossy = String::from_utf8_lossy(line);
                let Ok(lossy_len) = u32::try_from(lossy.len()) else {
                    return;
                };
                let Ok(decoded_offset) = u32::try_from(self.decoded_buf.len()) else {
                    return;
                };
                let Some(combined_offset) = u32::try_from(self.buf.len())
                    .ok()
                    .and_then(|buf_len| buf_len.checked_add(decoded_offset))
                else {
                    return;
                };
                self.decoded_buf.extend_from_slice(lossy.as_bytes());
                Some((combined_offset, lossy_len))
            };
            if let Some(line_view) = line_view {
                self.line_views.push(line_view);
            }
            self.lifecycle.set_line_written();
        }
    }
}

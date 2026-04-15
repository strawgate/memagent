//! String backing buffer management for columnar construction.
//!
//! The builder's 2-buffer model: "original" (input/wire bytes) + "generated"
//! (`write_str` output / decoded strings). [`StringRef`] offsets below
//! `original_len` point into the original buffer; offsets at or above point
//! into the generated buffer shifted by `original_len`.
//!
//! [`BlockStore`] owns the buffers and provides:
//! - [`BlockStore::read_str_bytes`]: safe byte extraction from the 2-buffer system
//! - [`BlockStore::make_string_view`]: Arrow StringView u128 construction
//! - [`BlockStore::register_blocks`]: buffer registration for `StringViewArray`
//!
//! By centralizing buffer management, `ColumnarBatchBuilder`, `StreamingBuilder`,
//! and future producers can share the same finalization code paths.

use arrow::buffer::Buffer;

use super::accumulator::{MaterializeError, StringRef};

// ---------------------------------------------------------------------------
// Pure helper functions — no heap allocation, Kani-friendly
// ---------------------------------------------------------------------------

/// Read string bytes from a 2-buffer system without UTF-8 validation.
///
/// `original` and `generated` are raw byte slices; `original_len` is the
/// boundary between the two buffers in the StringRef offset space.
///
/// Returns `Ok(slice)` for valid references, `Err` for out-of-bounds or
/// spanning references (start in original, end in generated).
pub(super) fn read_str_bytes_raw<'a>(
    original: &'a [u8],
    generated: &'a [u8],
    original_len: usize,
    sref: StringRef,
) -> Result<&'a [u8], MaterializeError> {
    let start = sref.offset as usize;
    let end =
        start
            .checked_add(sref.len as usize)
            .ok_or(MaterializeError::StringRefOutOfBounds {
                offset: sref.offset,
                len: sref.len,
                buffer_len: original.len() + generated.len(),
            })?;

    if start < original_len {
        if end > original_len {
            return Err(MaterializeError::StringRefSpansBoundary {
                offset: sref.offset,
                len: sref.len,
                original_len,
            });
        }
        original
            .get(start..end)
            .ok_or(MaterializeError::StringRefOutOfBounds {
                offset: sref.offset,
                len: sref.len,
                buffer_len: original.len(),
            })
    } else {
        let gen_start = start - original_len;
        let gen_end = end - original_len;
        generated
            .get(gen_start..gen_end)
            .ok_or(MaterializeError::StringRefOutOfBounds {
                offset: sref.offset,
                len: sref.len,
                buffer_len: generated.len(),
            })
    }
}

/// Construct a u128 StringView for a given StringRef against raw byte slices.
///
/// Strings ≤ 12 bytes are inlined. Longer strings reference a buffer block.
/// `orig_block` and `gen_block` are the Arrow buffer indices.
#[inline(always)]
pub(super) fn make_string_view_raw(
    sref: StringRef,
    original_buf: &[u8],
    generated_buf: &[u8],
    original_len: usize,
    orig_block: u32,
    gen_block: Option<u32>,
) -> Result<u128, MaterializeError> {
    let len = sref.len;
    if len == 0 {
        return Ok(0u128);
    }

    let start = sref.offset as usize;
    let end = start.wrapping_add(len as usize);

    // Detect spanning references before resolving to a single buffer.
    if start < original_len && end > original_len {
        return Err(MaterializeError::StringRefSpansBoundary {
            offset: sref.offset,
            len: sref.len,
            original_len,
        });
    }

    // Resolve buffer, block index, and local offset.
    let (buf, block_idx, local_offset) = if start < original_len {
        (original_buf, orig_block, sref.offset)
    } else {
        let dec_start = (start - original_len) as u32;
        match gen_block {
            Some(gb) => (generated_buf, gb, dec_start),
            None => {
                return Err(MaterializeError::StringRefOutOfBounds {
                    offset: sref.offset,
                    len: sref.len,
                    buffer_len: 0,
                });
            }
        }
    };

    let local_start = local_offset as usize;
    let local_end =
        local_start
            .checked_add(len as usize)
            .ok_or(MaterializeError::StringRefOutOfBounds {
                offset: sref.offset,
                len: sref.len,
                buffer_len: buf.len(),
            })?;
    if local_end > buf.len() {
        return Err(MaterializeError::StringRefOutOfBounds {
            offset: sref.offset,
            len: sref.len,
            buffer_len: buf.len(),
        });
    }

    // Build the u128 view using arithmetic — avoids byte array + copy_from_slice.
    Ok(if len <= 12 {
        // Inline: [len:4][data:12] packed little-endian.
        let mut view_bytes = [0u8; 16];
        view_bytes[0..4].copy_from_slice(&len.to_le_bytes());
        view_bytes[4..4 + len as usize].copy_from_slice(&buf[local_start..local_end]);
        u128::from_le_bytes(view_bytes)
    } else {
        // Buffer ref: [len:4][prefix:4][block_idx:4][offset:4].
        let prefix = u32::from_le_bytes([
            buf[local_start],
            buf[local_start + 1],
            buf[local_start + 2],
            buf[local_start + 3],
        ]);
        (len as u128)
            | ((prefix as u128) << 32)
            | ((block_idx as u128) << 64)
            | ((local_offset as u128) << 96)
    })
}

// ---------------------------------------------------------------------------
// BlockStore — 2-buffer string backing
// ---------------------------------------------------------------------------

/// Manages the string backing buffers for columnar batch construction.
///
/// Two logical blocks:
/// - **Block 0 ("original")**: Input buffer — protobuf wire bytes, scanner
///   input, CSV chunk, etc. Set once at batch start, immutable during writes.
/// - **Block 1 ("generated")**: Append-only buffer for strings produced by
///   `write_str` — JSON-unescaped values, formatted fields, constants, etc.
///
/// The offset encoding is zero-cost: `StringRef.offset < original_len` → block 0,
/// `offset >= original_len` → block 1 at `offset - original_len`.
#[derive(Clone)]
pub struct BlockStore {
    /// Input buffer (e.g., protobuf wire bytes, scanner input).
    /// Stored as Arrow `Buffer` (ref-counted) — O(1) to pass to StringViewArray.
    original: Buffer,
    /// Generated/decoded buffer.
    generated: Buffer,
}

impl BlockStore {
    /// Create a `BlockStore` from an original buffer and a generated buffer.
    ///
    /// Both buffers are moved (O(1) ref-count bump for `Buffer`).
    pub fn new(original: Buffer, generated: Buffer) -> Self {
        Self {
            original,
            generated,
        }
    }

    /// Create an empty `BlockStore` (both buffers empty).
    pub fn empty() -> Self {
        Self {
            original: Buffer::from(Vec::<u8>::new()),
            generated: Buffer::from(Vec::<u8>::new()),
        }
    }

    /// Length of the original (input) buffer.
    #[inline]
    pub fn original_len(&self) -> usize {
        self.original.len()
    }

    /// Length of the generated buffer.
    #[inline]
    pub fn generated_len(&self) -> usize {
        self.generated.len()
    }

    /// Original buffer reference.
    #[inline]
    pub fn original(&self) -> &Buffer {
        &self.original
    }

    /// Generated buffer reference.
    #[inline]
    pub fn generated(&self) -> &Buffer {
        &self.generated
    }

    /// Resolve a `StringRef` to the raw bytes it references.
    ///
    /// Returns `Ok(slice)` for valid references, `Err` for out-of-bounds or
    /// spanning references (start in original, end in generated).
    pub fn read_str_bytes(&self, sref: StringRef) -> Result<&[u8], MaterializeError> {
        read_str_bytes_raw(&self.original, &self.generated, self.original.len(), sref)
    }

    /// Build the Arrow `StringViewArray` u128 view word for a string reference.
    ///
    /// Returns the packed view (inlined for ≤12 bytes, buffer-ref for longer).
    /// `orig_block` and `gen_block` are the Arrow buffer indices from
    /// `register_blocks`.
    pub fn make_string_view(
        &self,
        sref: StringRef,
        orig_block: u32,
        gen_block: Option<u32>,
    ) -> Result<u128, MaterializeError> {
        make_string_view_raw(
            sref,
            &self.original,
            &self.generated,
            self.original.len(),
            orig_block,
            gen_block,
        )
    }

    /// Register the blocks with an Arrow `StringViewBuilder`-style consumer.
    ///
    /// Returns `(buffers_vec, orig_block_idx, gen_block_idx_or_none)`.
    /// The generated block is only registered if non-empty.
    pub fn register_blocks(&self) -> (Vec<Buffer>, u32, Option<u32>) {
        let mut buffers: Vec<Buffer> = Vec::with_capacity(2);
        let orig_block: u32 = 0;
        buffers.push(self.original.clone()); // O(1) Arc bump
        let gen_block = if self.generated.is_empty() {
            None
        } else {
            buffers.push(self.generated.clone()); // O(1) Arc bump
            Some(1u32)
        };
        (buffers, orig_block, gen_block)
    }
}

impl std::fmt::Debug for BlockStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockStore")
            .field("original_len", &self.original.len())
            .field("generated_len", &self.generated.len())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_str_bytes_original_buffer() {
        let original = Buffer::from_vec(b"hello world".to_vec());
        let generated = Buffer::from(Vec::<u8>::new());
        let store = BlockStore::new(original, generated);

        let sref = StringRef { offset: 0, len: 5 };
        assert_eq!(store.read_str_bytes(sref).unwrap(), b"hello");

        let sref = StringRef { offset: 6, len: 5 };
        assert_eq!(store.read_str_bytes(sref).unwrap(), b"world");
    }

    #[test]
    fn read_str_bytes_generated_buffer() {
        let original = Buffer::from_vec(b"orig".to_vec());
        let generated = Buffer::from_vec(b"generated".to_vec());
        let store = BlockStore::new(original, generated);

        // offset = original_len (4) + 0 = 4 → generated[0..3] = "gen"
        let sref = StringRef { offset: 4, len: 3 };
        assert_eq!(store.read_str_bytes(sref).unwrap(), b"gen");

        // offset = 4 + 3 = 7 → generated[3..9] = "erated"
        let sref = StringRef { offset: 7, len: 6 };
        assert_eq!(store.read_str_bytes(sref).unwrap(), b"erated");
    }

    #[test]
    fn read_str_bytes_spanning_rejected() {
        let original = Buffer::from_vec(b"orig".to_vec());
        let generated = Buffer::from_vec(b"gen".to_vec());
        let store = BlockStore::new(original, generated);

        // Starts in original (offset=2), but len=4 extends past original_len=4.
        let sref = StringRef { offset: 2, len: 4 };
        assert!(store.read_str_bytes(sref).is_err());
    }

    #[test]
    fn read_str_bytes_out_of_bounds() {
        let original = Buffer::from_vec(b"orig".to_vec());
        let generated = Buffer::from_vec(b"gen".to_vec());
        let store = BlockStore::new(original, generated);

        // Past generated buffer.
        let sref = StringRef {
            offset: 4 + 3,
            len: 1,
        };
        assert!(store.read_str_bytes(sref).is_err());
    }

    #[test]
    fn read_str_bytes_empty_string() {
        let store = BlockStore::empty();
        let sref = StringRef { offset: 0, len: 0 };
        // Empty string at offset 0 should succeed — no bytes to read.
        // read_str_bytes checks bounds, and 0+0 <= 0 passes.
        assert_eq!(store.read_str_bytes(sref).unwrap(), b"");
    }

    #[test]
    fn make_string_view_inline() {
        let original = Buffer::from_vec(b"hello".to_vec());
        let store = BlockStore::new(original, Buffer::from(Vec::<u8>::new()));

        let sref = StringRef { offset: 0, len: 5 };
        let view = store.make_string_view(sref, 0, None).unwrap();

        // Inline: len in lowest 4 bytes.
        assert_eq!((view & 0xFFFF_FFFF) as u32, 5);
        // Bytes 4..9 should be "hello".
        let bytes = view.to_le_bytes();
        assert_eq!(&bytes[4..9], b"hello");
    }

    #[test]
    fn make_string_view_buffer_ref() {
        let long_str = b"this is a string longer than twelve bytes";
        let original = Buffer::from_vec(long_str.to_vec());
        let store = BlockStore::new(original, Buffer::from(Vec::<u8>::new()));

        let sref = StringRef {
            offset: 0,
            len: long_str.len() as u32,
        };
        let view = store.make_string_view(sref, 0, None).unwrap();

        // Buffer ref: len in bits [0..32], prefix in [32..64], block in [64..96], offset in [96..128].
        let len = (view & 0xFFFF_FFFF) as u32;
        assert_eq!(len, long_str.len() as u32);

        let prefix_val = ((view >> 32) & 0xFFFF_FFFF) as u32;
        let expected_prefix =
            u32::from_le_bytes([long_str[0], long_str[1], long_str[2], long_str[3]]);
        assert_eq!(prefix_val, expected_prefix);

        let block = ((view >> 64) & 0xFFFF_FFFF) as u32;
        assert_eq!(block, 0); // original block

        let offset = ((view >> 96) & 0xFFFF_FFFF) as u32;
        assert_eq!(offset, 0);
    }

    #[test]
    fn make_string_view_generated_block() {
        let original = Buffer::from_vec(b"orig".to_vec());
        let gen_data = b"this is a generated string longer than 12";
        let generated = Buffer::from_vec(gen_data.to_vec());
        let store = BlockStore::new(original, generated);

        // offset = 4 (original_len) → generated buffer, local offset 0
        let sref = StringRef {
            offset: 4,
            len: gen_data.len() as u32,
        };
        let view = store.make_string_view(sref, 0, Some(1)).unwrap();

        let block = ((view >> 64) & 0xFFFF_FFFF) as u32;
        assert_eq!(block, 1); // generated block

        let offset = ((view >> 96) & 0xFFFF_FFFF) as u32;
        assert_eq!(offset, 0); // local offset into generated
    }

    #[test]
    fn make_string_view_empty() {
        let store = BlockStore::empty();
        let sref = StringRef { offset: 0, len: 0 };
        assert_eq!(store.make_string_view(sref, 0, None).unwrap(), 0);
    }

    #[test]
    fn register_blocks_original_only() {
        let original = Buffer::from_vec(b"data".to_vec());
        let store = BlockStore::new(original, Buffer::from(Vec::<u8>::new()));
        let (buffers, orig, gen_block) = store.register_blocks();
        assert_eq!(buffers.len(), 1);
        assert_eq!(orig, 0);
        assert!(gen_block.is_none());
    }

    #[test]
    fn register_blocks_both() {
        let original = Buffer::from_vec(b"orig".to_vec());
        let generated = Buffer::from_vec(b"gen".to_vec());
        let store = BlockStore::new(original, generated);
        let (buffers, orig, gen_block) = store.register_blocks();
        assert_eq!(buffers.len(), 2);
        assert_eq!(orig, 0);
        assert_eq!(gen_block, Some(1));
    }
}

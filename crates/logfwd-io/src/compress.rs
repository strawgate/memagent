//! Compression stage: wraps zstd with a reused context for minimal overhead.
//! The key optimization is reusing the ZSTD_CCtx across chunks — context
//! creation is expensive (~128KB allocation), but reset is nearly free.

use std::io;

/// Compressed chunk with its header, ready for wire transmission.
pub struct CompressedChunk {
    /// The 16-byte header + compressed payload, contiguous in memory.
    pub data: Vec<u8>,
    /// Raw (uncompressed) size.
    pub raw_size: u32,
    /// Number of bytes in the compressed payload (excludes header).
    pub compressed_size: u32,
}

/// Wire format header for a compressed chunk.
/// 16 bytes, fixed layout. The receiver uses this to decompress.
///
/// ```text
/// Offset  Size  Field
/// 0       2     magic (0x4C46 = "LF" for LogFwd)
/// 2       1     version (1)
/// 3       1     flags (bit 0: zstd compressed)
/// 4       4     compressed payload size (bytes)
/// 8       4     raw (uncompressed) size (bytes)
/// 12      4     xxhash32 of compressed payload
/// ```
pub const HEADER_SIZE: usize = 16;
pub const MAGIC: u16 = 0x4C46; // "LF"
pub const VERSION: u8 = 1;
pub const FLAG_ZSTD: u8 = 0x01;

/// Chunk compressor with reusable zstd context.
pub struct ChunkCompressor {
    /// Persistent zstd compressor — reuses the CCtx (~128 KB) across calls.
    compressor: zstd::bulk::Compressor<'static>,
    /// Pre-allocated output buffer. Sized to worst-case expansion.
    out_buf: Vec<u8>,
}

impl ChunkCompressor {
    pub fn new(level: i32) -> io::Result<Self> {
        Ok(ChunkCompressor {
            compressor: zstd::bulk::Compressor::new(level)?,
            // Start with 1MB output buffer, will grow if needed.
            out_buf: Vec::with_capacity(1024 * 1024 + HEADER_SIZE),
        })
    }

    /// Compress a raw chunk into a wire-format message (header + compressed payload).
    /// Reuses internal buffers to avoid allocation.
    pub fn compress(&mut self, raw: &[u8]) -> io::Result<CompressedChunk> {
        let raw_size = raw.len();
        let raw_size_u32 = check_wire_size("raw size", raw_size)?;

        // Worst case: zstd may expand data. zstd_safe::compress_bound gives the max.
        let max_compressed = zstd::zstd_safe::compress_bound(raw_size);
        let total_needed = HEADER_SIZE + max_compressed;
        if self.out_buf.capacity() < total_needed {
            self.out_buf.reserve(total_needed - self.out_buf.capacity());
        }
        self.out_buf.clear();

        // Reserve space for header (we'll fill it after we know the compressed size).
        self.out_buf.resize(HEADER_SIZE, 0);

        // Compress directly into out_buf after the header via a cursor.
        // compress_to_buffer reuses the persistent CCtx and writes starting at the
        // cursor position, so no separate allocation for the compressed payload.
        let mut cursor = io::Cursor::new(&mut self.out_buf);
        cursor.set_position(HEADER_SIZE as u64);
        let compressed_size = self.compressor.compress_to_buffer(raw, &mut cursor)?;

        // Validate that compressed size fits in u32; the wire format only has
        // 4 bytes per size field.
        let compressed_size_u32 = check_wire_size("compressed size", compressed_size)?;

        // Compute checksum over the compressed payload.
        let checksum =
            xxhash_rust::xxh32::xxh32(&self.out_buf[HEADER_SIZE..HEADER_SIZE + compressed_size], 0);

        // Fill in the header.
        self.out_buf[0..2].copy_from_slice(&MAGIC.to_le_bytes());
        self.out_buf[2] = VERSION;
        self.out_buf[3] = FLAG_ZSTD;
        self.out_buf[4..8].copy_from_slice(&compressed_size_u32.to_le_bytes());
        self.out_buf[8..12].copy_from_slice(&raw_size_u32.to_le_bytes());
        self.out_buf[12..16].copy_from_slice(&checksum.to_le_bytes());

        // Swap out_buf to give ownership to the caller without cloning.
        // The replacement buffer is pre-allocated ready for the next call.
        let data = std::mem::replace(
            &mut self.out_buf,
            Vec::with_capacity(total_needed.max(1024 * 1024 + HEADER_SIZE)),
        );

        Ok(CompressedChunk {
            data,
            raw_size: raw_size_u32,
            compressed_size: compressed_size_u32,
        })
    }

    /// Compression ratio of the last operation (raw / compressed).
    /// Returns 0.0 if no compression has been done.
    pub fn last_ratio(chunk: &CompressedChunk) -> f64 {
        if chunk.compressed_size == 0 {
            return 0.0;
        }
        chunk.raw_size as f64 / chunk.compressed_size as f64
    }
}

fn check_wire_size(name: &str, size: usize) -> io::Result<u32> {
    u32::try_from(size).map_err(|_e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} {size} exceeds u32::MAX"),
        )
    })
}

/// Decompress and verify a compressed chunk (for testing / receiver side).
pub fn decompress_chunk(data: &[u8]) -> io::Result<Vec<u8>> {
    if data.len() < HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "chunk too small",
        ));
    }

    let magic = u16::from_le_bytes([data[0], data[1]]);
    if magic != MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
    }

    let version = data[2];
    if version != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported version {version}"),
        ));
    }

    let flags = data[3];
    let compressed_size = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let raw_size = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;
    let expected_checksum = u32::from_le_bytes([data[12], data[13], data[14], data[15]]);

    if data.len() < HEADER_SIZE + compressed_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated payload",
        ));
    }

    let payload = &data[HEADER_SIZE..HEADER_SIZE + compressed_size];

    // Verify checksum.
    let actual_checksum = xxhash_rust::xxh32::xxh32(payload, 0);
    if actual_checksum != expected_checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("checksum mismatch: expected {expected_checksum:#x}, got {actual_checksum:#x}"),
        ));
    }

    if flags & FLAG_ZSTD != 0 {
        zstd::bulk::decompress(payload, raw_size)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    } else {
        Ok(payload.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_roundtrip() {
        let mut compressor = ChunkCompressor::new(1).unwrap();
        let raw = b"2024-01-15T10:30:00Z INFO  service started successfully\n\
                    2024-01-15T10:30:01Z DEBUG processing request id=abc123\n\
                    2024-01-15T10:30:02Z WARN  connection pool running low\n";

        let chunk = compressor.compress(raw).unwrap();
        assert!(chunk.compressed_size > 0);
        assert_eq!(chunk.raw_size, raw.len() as u32);

        let decompressed = decompress_chunk(&chunk.data).unwrap();
        assert_eq!(&decompressed[..], &raw[..]);
    }

    #[test]
    fn test_compression_ratio() {
        let mut compressor = ChunkCompressor::new(1).unwrap();
        // Repetitive log data should compress well.
        let line = "2024-01-15T10:30:00Z INFO  service handled request successfully path=/api/v1/health status=200 duration_ms=3\n";
        let raw: Vec<u8> = line.as_bytes().repeat(1000);

        let chunk = compressor.compress(&raw).unwrap();
        let ratio = ChunkCompressor::last_ratio(&chunk);
        assert!(ratio > 2.0, "expected ratio > 2.0, got {ratio:.2}");
    }

    #[test]
    fn test_bad_magic_rejected() {
        let mut data = vec![0u8; 32];
        data[0] = 0xFF;
        data[1] = 0xFF;
        assert!(decompress_chunk(&data).is_err());
    }

    #[test]
    fn test_truncated_rejected() {
        assert!(decompress_chunk(&[0u8; 4]).is_err());
    }

    /// Verify the validation path used by `compress()` for oversized inputs.
    #[test]
    #[cfg(target_pointer_width = "64")]
    fn test_oversized_raw_input_rejected() {
        let oversized: usize = u32::MAX as usize + 1;
        let result = check_wire_size("raw size", oversized);
        assert!(
            result.is_err(),
            "check_wire_size should fail for values > u32::MAX"
        );

        let err = result.expect_err("oversized raw input must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("raw size"));
        assert!(err.to_string().contains("exceeds u32::MAX"));
    }
}

//! Checkpoint segment file format, writer, reader, and recovery.
//!
//! A checkpoint segment stores Arrow IPC data (zstd-compressed) inside a
//! crash-safe envelope with header and footer. The format is:
//!
//! ```text
//! HEADER (32 bytes) | Arrow IPC Stream (N bytes) | FOOTER (32 bytes)
//! ```
//!
//! The footer contains a checksum (xxh32) over all preceding bytes.
//! Missing footer = incomplete segment (crashed mid-write). Invalid footer
//! = corrupt segment. Both are deleted during recovery.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::ipc::CompressionType;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::record_batch::RecordBatch;

/// Magic bytes identifying a logfwd checkpoint segment.
pub const SEGMENT_MAGIC: [u8; 4] = *b"LCHK";
/// Current format version.
pub const SEGMENT_VERSION: u16 = 1;
/// Fixed header size in bytes.
pub const HEADER_SIZE: usize = 32;
/// Fixed footer size in bytes.
pub const FOOTER_SIZE: usize = 32;
/// Flag: Arrow IPC bodies use zstd compression.
pub const FLAG_ZSTD: u16 = 0x0001;

// ---------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------

/// Segment file header — written first, before any Arrow data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    /// Format version (currently 1).
    pub version: u16,
    /// Bitfield. Bit 0 = zstd body compression in Arrow IPC.
    pub flags: u16,
    /// Monotonically increasing segment sequence number.
    pub segment_id: u64,
    /// xxh64 hash of the upstream SQL transform text. 0 if no transform.
    pub sql_hash: u64,
    /// Wall-clock milliseconds since UNIX epoch when segment was created.
    pub created_epoch_ms: u64,
}

impl SegmentHeader {
    /// Serialize to exactly HEADER_SIZE bytes (little-endian).
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&SEGMENT_MAGIC);
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.flags.to_le_bytes());
        buf[8..16].copy_from_slice(&self.segment_id.to_le_bytes());
        buf[16..24].copy_from_slice(&self.sql_hash.to_le_bytes());
        buf[24..32].copy_from_slice(&self.created_epoch_ms.to_le_bytes());
        buf
    }

    /// Deserialize from exactly HEADER_SIZE bytes.
    pub fn from_bytes(buf: &[u8; HEADER_SIZE]) -> io::Result<Self> {
        if buf[0..4] != SEGMENT_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad segment magic",
            ));
        }
        let version = u16::from_le_bytes([buf[4], buf[5]]);
        if version != SEGMENT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported segment version {version}"),
            ));
        }
        // All slice ranges are within the fixed-size 32-byte array, so
        // try_into to [u8; N] is infallible. Use explicit byte indexing
        // to avoid any unwrap in production code.
        let seg_id = [
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ];
        let hash = [
            buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
        ];
        let ts = [
            buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31],
        ];
        Ok(Self {
            version,
            flags: u16::from_le_bytes([buf[6], buf[7]]),
            segment_id: u64::from_le_bytes(seg_id),
            sql_hash: u64::from_le_bytes(hash),
            created_epoch_ms: u64::from_le_bytes(ts),
        })
    }
}

// ---------------------------------------------------------------------------
// Footer
// ---------------------------------------------------------------------------

/// Segment file footer — written last, after all Arrow IPC data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentFooter {
    /// Total number of rows across all RecordBatch messages.
    pub record_count: u64,
    /// Number of RecordBatch messages in the Arrow IPC stream.
    pub batch_count: u32,
    /// Byte length of the Arrow IPC stream section.
    pub data_size: u64,
    /// xxh32 checksum of all bytes preceding this field.
    pub checksum: u32,
}

impl SegmentFooter {
    /// Serialize to exactly FOOTER_SIZE bytes (little-endian).
    pub fn to_bytes(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0u8; FOOTER_SIZE];
        buf[0..8].copy_from_slice(&self.record_count.to_le_bytes());
        buf[8..12].copy_from_slice(&self.batch_count.to_le_bytes());
        buf[12..20].copy_from_slice(&self.data_size.to_le_bytes());
        buf[20..24].copy_from_slice(&self.checksum.to_le_bytes());
        buf[24..28].copy_from_slice(&SEGMENT_MAGIC);
        buf[28..32].copy_from_slice(&(FOOTER_SIZE as u32).to_le_bytes());
        buf
    }

    /// Deserialize from exactly FOOTER_SIZE bytes read from end of file.
    pub fn from_bytes(buf: &[u8; FOOTER_SIZE]) -> io::Result<Self> {
        if buf[24..28] != SEGMENT_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad footer magic",
            ));
        }
        let footer_size = u32::from_le_bytes([buf[28], buf[29], buf[30], buf[31]]);
        if footer_size as usize != FOOTER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected footer size {footer_size}"),
            ));
        }
        let rc = [
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ];
        let ds = [
            buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19],
        ];
        Ok(Self {
            record_count: u64::from_le_bytes(rc),
            batch_count: u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]),
            data_size: u64::from_le_bytes(ds),
            checksum: u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]),
        })
    }
}

// ---------------------------------------------------------------------------
// Segment file (validated, ready to read)
// ---------------------------------------------------------------------------

/// A validated segment file on disk.
#[derive(Debug)]
pub struct SegmentFile {
    pub path: PathBuf,
    pub header: SegmentHeader,
    pub footer: SegmentFooter,
}

/// Result of attempting to open a segment file.
#[derive(Debug)]
#[non_exhaustive]
pub enum SegmentStatus {
    /// Segment is complete and checksum-verified.
    Valid(SegmentFile),
    /// Footer is missing or incomplete — crashed mid-write.
    Incomplete(PathBuf),
    /// Footer present but checksum does not match.
    Corrupt(PathBuf, String),
    /// Header magic or version is wrong — not a segment file.
    NotASegment(PathBuf),
    /// SQL hash does not match current pipeline config.
    SchemaMismatch {
        path: PathBuf,
        expected_sql_hash: u64,
        found_sql_hash: u64,
    },
}

/// Zero-padded filename for lexicographic = numeric sort.
///
/// u64::MAX is 18_446_744_073_709_551_615 (20 digits), so 20-digit padding
/// ensures lexicographic sort equals numeric sort across the full u64 range.
pub fn segment_filename(segment_id: u64) -> String {
    format!("seg-{segment_id:020}.lchk")
}

impl SegmentFile {
    /// Open and validate a segment file.
    ///
    /// Reads the footer first (last 32 bytes), then the header, then
    /// verifies the xxh32 checksum over the entire file minus the
    /// checksum field itself.
    pub fn open(path: &Path, expected_sql_hash: Option<u64>) -> SegmentStatus {
        let meta = match fs::metadata(path) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return SegmentStatus::NotASegment(path.to_path_buf());
            }
            Err(e) => {
                // Permission denied or other OS error — treat as Corrupt so the
                // segment is not silently dropped from recovery.
                return SegmentStatus::Corrupt(
                    path.to_path_buf(),
                    format!("cannot stat segment file: {e}"),
                );
            }
        };
        let file_size = meta.len();

        if file_size < (HEADER_SIZE + FOOTER_SIZE) as u64 {
            return SegmentStatus::Incomplete(path.to_path_buf());
        }

        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return SegmentStatus::NotASegment(path.to_path_buf());
            }
            Err(e) => {
                return SegmentStatus::Corrupt(
                    path.to_path_buf(),
                    format!("cannot open segment file: {e}"),
                );
            }
        };

        // Read footer (last FOOTER_SIZE bytes).
        let mut footer_buf = [0u8; FOOTER_SIZE];
        match file.seek(SeekFrom::End(-(FOOTER_SIZE as i64))) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return SegmentStatus::Incomplete(path.to_path_buf());
            }
            Err(e) => {
                return SegmentStatus::Corrupt(
                    path.to_path_buf(),
                    format!("cannot seek to footer: {e}"),
                );
            }
        }
        match file.read_exact(&mut footer_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return SegmentStatus::Incomplete(path.to_path_buf());
            }
            Err(e) => {
                return SegmentStatus::Corrupt(
                    path.to_path_buf(),
                    format!("cannot read footer: {e}"),
                );
            }
        }
        let footer = match SegmentFooter::from_bytes(&footer_buf) {
            Ok(f) => f,
            Err(e) => {
                // Footer bytes are present but invalid — corruption, not truncation.
                return SegmentStatus::Corrupt(path.to_path_buf(), format!("invalid footer: {e}"));
            }
        };

        // Read header.
        let mut header_buf = [0u8; HEADER_SIZE];
        if let Err(e) = file.seek(SeekFrom::Start(0)) {
            return SegmentStatus::Corrupt(
                path.to_path_buf(),
                format!("cannot seek to header: {e} (kind: {:?})", e.kind()),
            );
        }
        if let Err(e) = file.read_exact(&mut header_buf) {
            return SegmentStatus::Corrupt(
                path.to_path_buf(),
                format!("cannot read header: {e} (kind: {:?})", e.kind()),
            );
        }
        let header = match SegmentHeader::from_bytes(&header_buf) {
            Ok(h) => h,
            Err(e) => {
                // If the magic bytes are present but the version is wrong, this is a
                // recognizable segment file with an unsupported format — treat it as
                // Corrupt (hard error) rather than silently ignoring it as NotASegment.
                if header_buf[0..4] == SEGMENT_MAGIC {
                    return SegmentStatus::Corrupt(
                        path.to_path_buf(),
                        format!("unrecognized segment header: {e}"),
                    );
                }
                return SegmentStatus::NotASegment(path.to_path_buf());
            }
        };

        // Verify sizes are consistent.
        let expected_size = (HEADER_SIZE as u64)
            .checked_add(footer.data_size)
            .and_then(|n| n.checked_add(FOOTER_SIZE as u64))
            .ok_or_else(|| {
                SegmentStatus::Corrupt(
                    path.to_path_buf(),
                    format!(
                        "footer data_size {} overflows size computation",
                        footer.data_size
                    ),
                )
            });
        let expected_size = match expected_size {
            Ok(n) => n,
            Err(s) => return s,
        };
        if file_size != expected_size {
            return SegmentStatus::Corrupt(
                path.to_path_buf(),
                format!("size mismatch: file={file_size}, expected={expected_size}"),
            );
        }

        // Verify checksum: hash header + IPC data + first 20 bytes of footer.
        let bytes_to_hash = (HEADER_SIZE as u64)
            .checked_add(footer.data_size)
            .and_then(|n| n.checked_add(20))
            .ok_or_else(|| {
                SegmentStatus::Corrupt(
                    path.to_path_buf(),
                    format!(
                        "footer data_size {} overflows checksum computation",
                        footer.data_size
                    ),
                )
            });
        let bytes_to_hash = match bytes_to_hash {
            Ok(n) => n,
            Err(s) => return s,
        };
        let mut hasher = xxhash_rust::xxh32::Xxh32::new(0);
        if let Err(e) = file.seek(SeekFrom::Start(0)) {
            return SegmentStatus::Corrupt(
                path.to_path_buf(),
                format!("seek to start failed: {e} (kind: {:?})", e.kind()),
            );
        }
        let mut remaining = match usize::try_from(bytes_to_hash) {
            Ok(n) => n,
            Err(_) => {
                return SegmentStatus::Corrupt(
                    path.to_path_buf(),
                    format!(
                        "segment too large to checksum on this platform ({bytes_to_hash} bytes)"
                    ),
                );
            }
        };
        let mut read_buf = vec![0u8; 256 * 1024];
        while remaining > 0 {
            let to_read = remaining.min(read_buf.len());
            match file.read_exact(&mut read_buf[..to_read]) {
                Ok(()) => {
                    hasher.update(&read_buf[..to_read]);
                    remaining -= to_read;
                }
                Err(err) => {
                    return SegmentStatus::Corrupt(
                        path.to_path_buf(),
                        format!("read error during checksum: {err} (kind: {:?})", err.kind()),
                    );
                }
            }
        }
        let computed = hasher.digest();
        if computed != footer.checksum {
            return SegmentStatus::Corrupt(
                path.to_path_buf(),
                format!(
                    "checksum mismatch: stored={:#010x}, computed={:#010x}",
                    footer.checksum, computed
                ),
            );
        }

        // Check SQL hash if provided.
        if let Some(expected) = expected_sql_hash {
            if header.sql_hash != expected {
                return SegmentStatus::SchemaMismatch {
                    path: path.to_path_buf(),
                    expected_sql_hash: expected,
                    found_sql_hash: header.sql_hash,
                };
            }
        }

        SegmentStatus::Valid(SegmentFile {
            path: path.to_path_buf(),
            header,
            footer,
        })
    }

    /// Read all RecordBatches from a validated segment.
    ///
    /// Each `append()` wrote an independent IPC stream (schema + batch + EOS),
    /// so we create a new `StreamReader` for each sub-stream using a shared
    /// cursor that tracks the read position across streams.
    /// Maximum data_size we'll allocate for reading (1 GiB).
    /// Defense-in-depth against corrupt footer claiming enormous size.
    const MAX_READ_DATA_SIZE: u64 = 1024 * 1024 * 1024;

    pub fn read_batches(&self) -> io::Result<Vec<RecordBatch>> {
        if self.footer.data_size > Self::MAX_READ_DATA_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "segment data_size {} exceeds maximum {} — likely corrupt",
                    self.footer.data_size,
                    Self::MAX_READ_DATA_SIZE,
                ),
            ));
        }

        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;

        let mut ipc_data = vec![0u8; self.footer.data_size as usize];
        file.read_exact(&mut ipc_data)?;

        // Cap capacity: don't trust the on-disk batch_count blindly (OOM protection).
        let capacity = (self.footer.batch_count as usize).min(65536);
        let mut batches = Vec::with_capacity(capacity);

        // Use a single Cursor over the whole IPC data. Each StreamReader
        // consumes one sub-stream and advances the cursor position.
        let mut cursor = io::Cursor::new(ipc_data);

        while batches.len() < self.footer.batch_count as usize {
            let pos_before = cursor.position();
            if pos_before >= cursor.get_ref().len() as u64 {
                break;
            }

            let reader =
                arrow::ipc::reader::StreamReader::try_new(&mut cursor, None).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("IPC stream at offset {pos_before}: {e}"),
                    )
                })?;

            for batch_result in reader {
                let batch = batch_result
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                batches.push(batch);
            }

            // Guard against infinite loop if reader consumed nothing.
            if cursor.position() == pos_before {
                break;
            }
        }

        if batches.len() != self.footer.batch_count as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "batch count mismatch: footer says {}, read {}",
                    self.footer.batch_count,
                    batches.len()
                ),
            ));
        }

        Ok(batches)
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Writes a single segment file incrementally.
///
/// Lifecycle: `create()` → `append()` × N → `finish()` → `SegmentFile`.
///
/// If dropped without `finish()`, the file has no footer and will be
/// detected as Incomplete on recovery.
pub struct SegmentWriter {
    header: SegmentHeader,
    path: PathBuf,
    /// Buffered writer wrapping the file.
    writer: BufWriter<File>,
    /// Reusable IPC serialization buffer.
    ipc_buf: Vec<u8>,
    /// Running counters for the footer.
    record_count: u64,
    batch_count: u32,
    /// Total Arrow IPC bytes written so far.
    data_size: u64,
    /// Incremental xxh32 hasher (covers header + all IPC data).
    hasher: xxhash_rust::xxh32::Xxh32,
}

impl SegmentWriter {
    /// Create a new segment file and write the header.
    ///
    /// Validates that the header uses a known version and supported flags so
    /// that the resulting file can always be re-opened by `SegmentFile::open`.
    pub fn create(dir: &Path, header: SegmentHeader) -> io::Result<Self> {
        if header.version != SEGMENT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "unsupported segment version {}; only version {} is supported",
                    header.version, SEGMENT_VERSION
                ),
            ));
        }
        // Only the zstd flag is defined; reject anything with unknown flag bits.
        if header.flags & !FLAG_ZSTD != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown segment flags {:#06x}", header.flags),
            ));
        }
        let filename = segment_filename(header.segment_id);
        let path = dir.join(filename);

        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let mut writer = BufWriter::with_capacity(256 * 1024, file);

        let header_bytes = header.to_bytes();
        writer.write_all(&header_bytes)?;

        let mut hasher = xxhash_rust::xxh32::Xxh32::new(0);
        hasher.update(&header_bytes);

        Ok(Self {
            header,
            path,
            writer,
            ipc_buf: Vec::with_capacity(1024 * 1024),
            record_count: 0,
            batch_count: 0,
            data_size: 0,
            hasher,
        })
    }

    /// Append a RecordBatch to the segment.
    ///
    /// Serializes to Arrow IPC Stream format with zstd body compression,
    /// then appends to the file. Not durable until `finish()`.
    pub fn append(&mut self, batch: &RecordBatch) -> io::Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        self.ipc_buf.clear();
        let compression = if self.header.flags & FLAG_ZSTD != 0 {
            Some(CompressionType::ZSTD)
        } else {
            None
        };
        let opts = IpcWriteOptions::default()
            .try_with_compression(compression)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let mut ipc_writer =
            StreamWriter::try_new_with_options(&mut self.ipc_buf, &batch.schema(), opts)
                .map_err(|e| io::Error::other(e.to_string()))?;
        ipc_writer
            .write(batch)
            .map_err(|e| io::Error::other(e.to_string()))?;
        ipc_writer
            .finish()
            .map_err(|e| io::Error::other(e.to_string()))?;

        // Enforce the read-time limit so every written segment can be replayed.
        let new_data_size = self.data_size + self.ipc_buf.len() as u64;
        if new_data_size > SegmentFile::MAX_READ_DATA_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "segment would exceed maximum data size {} bytes after this append",
                    SegmentFile::MAX_READ_DATA_SIZE,
                ),
            ));
        }

        self.writer.write_all(&self.ipc_buf)?;
        self.hasher.update(&self.ipc_buf);

        self.record_count += batch.num_rows() as u64;
        self.batch_count += 1;
        self.data_size = new_data_size;

        Ok(())
    }

    /// Seal the segment: write footer, fsync file, fsync directory.
    ///
    /// After this call the segment is crash-safe.
    pub fn finish(mut self) -> io::Result<SegmentFile> {
        // Hash the footer fields that precede the checksum.
        let mut footer_pre = [0u8; 20];
        footer_pre[0..8].copy_from_slice(&self.record_count.to_le_bytes());
        footer_pre[8..12].copy_from_slice(&self.batch_count.to_le_bytes());
        footer_pre[12..20].copy_from_slice(&self.data_size.to_le_bytes());
        self.hasher.update(&footer_pre);
        let checksum = self.hasher.digest();

        let footer = SegmentFooter {
            record_count: self.record_count,
            batch_count: self.batch_count,
            data_size: self.data_size,
            checksum,
        };

        let footer_bytes = footer.to_bytes();
        self.writer.write_all(&footer_bytes)?;
        self.writer.flush()?;

        // fsync the file.
        let file = self
            .writer
            .into_inner()
            .map_err(|e| io::Error::other(e.to_string()))?;
        file.sync_all()?;

        // fsync the parent directory.
        if let Some(parent) = self.path.parent() {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }

        Ok(SegmentFile {
            path: self.path,
            header: self.header,
            footer,
        })
    }

    /// Current byte size of the segment (header + data written so far).
    pub fn current_size(&self) -> u64 {
        HEADER_SIZE as u64 + self.data_size
    }
}

// ---------------------------------------------------------------------------
// Segment manager (rotation)
// ---------------------------------------------------------------------------

/// Configuration for segment rotation.
#[derive(Debug, Clone)]
pub struct RotationPolicy {
    /// Maximum bytes per segment before rotation.
    pub max_bytes: u64,
    /// Maximum time a segment stays open before rotation.
    pub max_age: Duration,
}

impl Default for RotationPolicy {
    fn default() -> Self {
        Self {
            max_bytes: 256 * 1024 * 1024,
            max_age: Duration::from_secs(5),
        }
    }
}

/// Manages segment lifecycle: create, append, rotate, finish.
pub struct SegmentManager {
    segment_dir: PathBuf,
    policy: RotationPolicy,
    sql_hash: u64,
    next_segment_id: u64,
    current: Option<ActiveSegment>,
}

struct ActiveSegment {
    writer: SegmentWriter,
    opened_at: Instant,
}

impl SegmentManager {
    /// Create a new segment manager.
    pub fn new(
        segment_dir: PathBuf,
        policy: RotationPolicy,
        sql_hash: u64,
        next_segment_id: u64,
    ) -> io::Result<Self> {
        fs::create_dir_all(&segment_dir)?;
        Ok(Self {
            segment_dir,
            policy,
            sql_hash,
            next_segment_id,
            current: None,
        })
    }

    /// Append a RecordBatch. Opens a new segment if needed, rotates if
    /// the current segment exceeds size or age limits.
    ///
    /// Returns the sealed `SegmentFile` if rotation occurred.
    pub fn append(&mut self, batch: &RecordBatch) -> io::Result<Option<SegmentFile>> {
        let mut sealed = None;

        if let Some(ref active) = self.current {
            let size_exceeded = active.writer.current_size() >= self.policy.max_bytes;
            let age_exceeded = active.opened_at.elapsed() >= self.policy.max_age;
            if size_exceeded || age_exceeded {
                sealed = Some(self.rotate()?);
            }
        }

        if self.current.is_none() {
            self.open_new_segment()?;
        }

        // open_new_segment() above ensures current is Some.
        if let Some(ref mut active) = self.current {
            active.writer.append(batch)?;
        }

        Ok(sealed)
    }

    /// Force-rotate: seal the current segment. Called during shutdown.
    pub fn flush(&mut self) -> io::Result<Option<SegmentFile>> {
        if self.current.is_some() {
            Ok(Some(self.rotate()?))
        } else {
            Ok(None)
        }
    }

    /// Current segment ID being written (None if no segment open).
    pub fn current_segment_id(&self) -> Option<u64> {
        self.current.as_ref().map(|a| a.writer.header.segment_id)
    }

    fn open_new_segment(&mut self) -> io::Result<()> {
        let next_segment_id = self
            .next_segment_id
            .checked_add(1)
            .ok_or_else(|| io::Error::other("segment ID overflow: u64::MAX reached"))?;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let header = SegmentHeader {
            version: SEGMENT_VERSION,
            flags: FLAG_ZSTD,
            segment_id: self.next_segment_id,
            sql_hash: self.sql_hash,
            created_epoch_ms: now_ms,
        };

        let writer = SegmentWriter::create(&self.segment_dir, header)?;
        self.current = Some(ActiveSegment {
            writer,
            opened_at: Instant::now(),
        });
        self.next_segment_id = next_segment_id;

        Ok(())
    }

    fn rotate(&mut self) -> io::Result<SegmentFile> {
        let active = self
            .current
            .take()
            .ok_or_else(|| io::Error::other("rotate called with no active segment"))?;
        active.writer.finish()
    }
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

/// Result of recovery: validated segments to replay, and the next ID.
#[derive(Debug)]
pub struct RecoveryPlan {
    /// Validated segments in segment_id order, ready for replay.
    pub segments: Vec<SegmentFile>,
    /// The segment_id to use for the next segment written.
    pub next_segment_id: u64,
}

/// Scan the segment directory, validate each segment, return the replay plan.
///
/// Incomplete segments at the tail are expected (crash during write) and
/// are deleted. Corrupt segments are logged and deleted.
pub fn recover_segments(
    segment_dir: &Path,
    expected_sql_hash: Option<u64>,
) -> io::Result<RecoveryPlan> {
    if !segment_dir.exists() {
        return Ok(RecoveryPlan {
            segments: Vec::new(),
            next_segment_id: 1,
        });
    }

    let mut entries: Vec<PathBuf> = Vec::new();
    // Track the highest segment ID seen across ALL .lchk files (including
    // corrupt/incomplete ones that may fail to delete), so next_segment_id
    // never collides with an existing filename.
    let mut max_seen_id: u64 = 0;

    for entry in fs::read_dir(segment_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("lchk") {
            // Parse segment ID from filename: "seg-0000000042.lchk" → 42
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                if let Some(id_str) = stem.strip_prefix("seg-") {
                    if let Ok(id) = id_str.parse::<u64>() {
                        max_seen_id = max_seen_id.max(id);
                    }
                }
            }
            entries.push(path);
        }
    }
    // Lexicographic sort == numeric sort because segment_filename() zero-pads to 20 digits.
    entries.sort();

    let mut valid: Vec<SegmentFile> = Vec::new();
    let mut to_delete: Vec<PathBuf> = Vec::new();

    for path in &entries {
        match SegmentFile::open(path, expected_sql_hash) {
            SegmentStatus::Valid(seg) => valid.push(seg),
            SegmentStatus::Incomplete(p) => {
                tracing::warn!(path = %p.display(), "deleting incomplete segment (crash recovery)");
                to_delete.push(p);
            }
            SegmentStatus::Corrupt(p, reason) => {
                tracing::error!(path = %p.display(), reason, "corrupt segment detected");
                to_delete.push(p);
            }
            SegmentStatus::NotASegment(p) => {
                tracing::warn!(
                    path = %p.display(),
                    "ignoring non-segment .lchk file in segment directory"
                );
            }
            SegmentStatus::SchemaMismatch {
                path,
                expected_sql_hash,
                found_sql_hash,
            } => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "segment {} has sql_hash {found_sql_hash:#018x} but pipeline \
                         expects {expected_sql_hash:#018x}. Revert the SQL transform \
                         or delete the segment directory: {}",
                        path.display(),
                        segment_dir.display(),
                    ),
                ));
            }
        }
    }

    for p in &to_delete {
        if let Err(e) = fs::remove_file(p) {
            tracing::error!(path = %p.display(), error = %e, "failed to delete segment");
        }
    }

    // Use max of all seen IDs (not just valid), so we never collide with
    // a corrupt file that failed to delete.
    let next_segment_id = if max_seen_id > 0 {
        max_seen_id.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "segment directory {} contains maximum segment id {}; cannot allocate next segment id",
                    segment_dir.display(),
                    max_seen_id,
                ),
            )
        })?
    } else {
        1
    };
    let total_records: u64 = valid.iter().map(|s| s.footer.record_count).sum();

    tracing::info!(
        segments = valid.len(),
        total_records,
        next_segment_id,
        deleted = to_delete.len(),
        "checkpoint recovery complete"
    );

    Ok(RecoveryPlan {
        segments: valid,
        next_segment_id,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("msg", DataType::Utf8, false),
            Field::new("seq", DataType::Int64, false),
        ]));
        let msgs: Vec<String> = (0..n).map(|i| format!("message-{i}")).collect();
        let seqs: Vec<i64> = (0..n as i64).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(msgs)),
                Arc::new(Int64Array::from(seqs)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn header_roundtrip() {
        let header = SegmentHeader {
            version: SEGMENT_VERSION,
            flags: FLAG_ZSTD,
            segment_id: 42,
            sql_hash: 0xDEAD_BEEF_CAFE_BABE,
            created_epoch_ms: 1700000000000,
        };
        let bytes = header.to_bytes();
        let parsed = SegmentHeader::from_bytes(&bytes).unwrap();
        assert_eq!(header, parsed);
    }

    #[test]
    fn footer_roundtrip() {
        let footer = SegmentFooter {
            record_count: 1000,
            batch_count: 5,
            data_size: 123456,
            checksum: 0xABCD1234,
        };
        let bytes = footer.to_bytes();
        let parsed = SegmentFooter::from_bytes(&bytes).unwrap();
        assert_eq!(footer, parsed);
    }

    #[test]
    fn segment_write_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_batch(100);

        let header = SegmentHeader {
            version: SEGMENT_VERSION,
            flags: FLAG_ZSTD,
            segment_id: 1,
            sql_hash: 0,
            created_epoch_ms: 0,
        };
        let mut writer = SegmentWriter::create(dir.path(), header).unwrap();
        writer.append(&batch).unwrap();
        let seg = writer.finish().unwrap();

        assert_eq!(seg.footer.record_count, 100);
        assert_eq!(seg.footer.batch_count, 1);

        let batches = seg.read_batches().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 100);

        let msg_col = batches[0]
            .column_by_name("msg")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(msg_col.value(0), "message-0");
        assert_eq!(msg_col.value(99), "message-99");
    }

    #[test]
    fn segment_multi_batch_roundtrip() {
        let dir = tempfile::tempdir().unwrap();

        let header = SegmentHeader {
            version: SEGMENT_VERSION,
            flags: FLAG_ZSTD,
            segment_id: 1,
            sql_hash: 0,
            created_epoch_ms: 0,
        };
        let mut writer = SegmentWriter::create(dir.path(), header).unwrap();
        writer.append(&make_batch(50)).unwrap();
        writer.append(&make_batch(30)).unwrap();
        writer.append(&make_batch(20)).unwrap();
        let seg = writer.finish().unwrap();

        assert_eq!(seg.footer.record_count, 100);
        assert_eq!(seg.footer.batch_count, 3);

        // Multi-batch reading: each append writes an independent IPC stream,
        // so read_batches reads them sequentially.
        let batches = seg.read_batches().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 100);
    }

    #[test]
    fn incomplete_segment_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg-0000000001.lchk");

        // Write header only, no footer.
        let header = SegmentHeader {
            version: SEGMENT_VERSION,
            flags: FLAG_ZSTD,
            segment_id: 1,
            sql_hash: 0,
            created_epoch_ms: 0,
        };
        fs::write(&path, header.to_bytes()).unwrap();

        assert!(matches!(
            SegmentFile::open(&path, None),
            SegmentStatus::Incomplete(_)
        ));
    }

    #[test]
    fn corrupt_checksum_detected() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_batch(10);

        let header = SegmentHeader {
            version: SEGMENT_VERSION,
            flags: FLAG_ZSTD,
            segment_id: 1,
            sql_hash: 0,
            created_epoch_ms: 0,
        };
        let mut writer = SegmentWriter::create(dir.path(), header).unwrap();
        writer.append(&batch).unwrap();
        let seg = writer.finish().unwrap();

        // Flip a byte in the middle of the file.
        let mut data = fs::read(&seg.path).unwrap();
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        fs::write(&seg.path, &data).unwrap();

        assert!(matches!(
            SegmentFile::open(&seg.path, None),
            SegmentStatus::Corrupt(_, _)
        ));
    }

    #[test]
    fn schema_mismatch_detected() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_batch(10);

        let header = SegmentHeader {
            version: SEGMENT_VERSION,
            flags: FLAG_ZSTD,
            segment_id: 1,
            sql_hash: 0xAAAA,
            created_epoch_ms: 0,
        };
        let mut writer = SegmentWriter::create(dir.path(), header).unwrap();
        writer.append(&batch).unwrap();
        let seg = writer.finish().unwrap();

        assert!(matches!(
            SegmentFile::open(&seg.path, Some(0xBBBB)),
            SegmentStatus::SchemaMismatch { .. }
        ));
    }

    #[test]
    fn recovery_deletes_incomplete() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("segments");
        fs::create_dir_all(&seg_dir).unwrap();

        // Write a valid segment.
        let header = SegmentHeader {
            version: SEGMENT_VERSION,
            flags: FLAG_ZSTD,
            segment_id: 1,
            sql_hash: 0,
            created_epoch_ms: 0,
        };
        let mut writer = SegmentWriter::create(&seg_dir, header).unwrap();
        writer.append(&make_batch(10)).unwrap();
        writer.finish().unwrap();

        // Write an incomplete segment (header only).
        let incomplete_path = seg_dir.join("seg-0000000002.lchk");
        let header2 = SegmentHeader {
            version: SEGMENT_VERSION,
            flags: FLAG_ZSTD,
            segment_id: 2,
            sql_hash: 0,
            created_epoch_ms: 0,
        };
        fs::write(&incomplete_path, header2.to_bytes()).unwrap();

        let plan = recover_segments(&seg_dir, None).unwrap();
        assert_eq!(plan.segments.len(), 1);
        assert_eq!(plan.segments[0].header.segment_id, 1);
        // next_segment_id = max(all seen IDs) + 1, not max(valid) + 1,
        // to avoid collision if the incomplete file failed to delete.
        assert_eq!(plan.next_segment_id, 3);
        assert!(!incomplete_path.exists());
    }

    #[test]
    fn recovery_empty_directory() {
        let dir = tempfile::tempdir().unwrap();
        let plan = recover_segments(dir.path(), None).unwrap();
        assert!(plan.segments.is_empty());
        assert_eq!(plan.next_segment_id, 1);
    }

    #[test]
    fn recovery_nonexistent_directory() {
        let plan = recover_segments(Path::new("/nonexistent/path"), None).unwrap();
        assert!(plan.segments.is_empty());
        assert_eq!(plan.next_segment_id, 1);
    }

    #[test]
    fn segment_manager_rotation_by_size() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("segments");

        let policy = RotationPolicy {
            max_bytes: 500, // tiny — forces rotation
            max_age: Duration::from_secs(3600),
        };
        let mut mgr = SegmentManager::new(seg_dir.clone(), policy, 0, 1).unwrap();

        let batch = make_batch(100);
        let sealed1 = mgr.append(&batch).unwrap();
        // First append opens segment 1, no rotation yet.
        assert!(sealed1.is_none());

        let sealed2 = mgr.append(&batch).unwrap();
        // Second append triggers rotation (segment 1 > 500 bytes).
        assert!(sealed2.is_some());
        assert_eq!(sealed2.unwrap().header.segment_id, 1);

        // Flush the remaining segment.
        let sealed3 = mgr.flush().unwrap();
        assert!(sealed3.is_some());
        assert_eq!(sealed3.unwrap().header.segment_id, 2);

        // Recovery should find both.
        let plan = recover_segments(&seg_dir, None).unwrap();
        assert_eq!(plan.segments.len(), 2);
    }

    #[test]
    fn segment_filename_zero_padded() {
        assert_eq!(segment_filename(1), "seg-00000000000000000001.lchk");
        assert_eq!(segment_filename(999999), "seg-00000000000000999999.lchk");
        // u64::MAX must sort correctly (20 digits)
        assert_eq!(segment_filename(u64::MAX), "seg-18446744073709551615.lchk");
    }

    #[test]
    fn empty_batch_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let empty = make_batch(0);
        let nonempty = make_batch(5);

        let header = SegmentHeader {
            version: SEGMENT_VERSION,
            flags: FLAG_ZSTD,
            segment_id: 1,
            sql_hash: 0,
            created_epoch_ms: 0,
        };
        let mut writer = SegmentWriter::create(dir.path(), header).unwrap();
        writer.append(&empty).unwrap();
        writer.append(&nonempty).unwrap();
        writer.append(&empty).unwrap();
        let seg = writer.finish().unwrap();

        assert_eq!(seg.footer.record_count, 5);
        assert_eq!(seg.footer.batch_count, 1); // only non-empty counted
    }

    #[test]
    fn create_rejects_bad_version() {
        let dir = tempfile::tempdir().unwrap();
        let header = SegmentHeader {
            version: SEGMENT_VERSION + 1,
            flags: FLAG_ZSTD,
            segment_id: 1,
            sql_hash: 0,
            created_epoch_ms: 0,
        };
        let result = SegmentWriter::create(dir.path(), header);
        let err = result.err().expect("expected an error for bad version");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("unsupported segment version"));
    }

    #[test]
    fn create_rejects_unknown_flags() {
        let dir = tempfile::tempdir().unwrap();
        let header = SegmentHeader {
            version: SEGMENT_VERSION,
            flags: 0x00FF, // unknown flag bits
            segment_id: 1,
            sql_hash: 0,
            created_epoch_ms: 0,
        };
        let result = SegmentWriter::create(dir.path(), header);
        let err = result.err().expect("expected an error for unknown flags");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("unknown segment flags"));
    }

    #[test]
    fn append_rejects_oversized_data() {
        let dir = tempfile::tempdir().unwrap();
        // Create a normal segment first to get a valid SegmentWriter.
        let header = SegmentHeader {
            version: SEGMENT_VERSION,
            flags: FLAG_ZSTD,
            segment_id: 1,
            sql_hash: 0,
            created_epoch_ms: 0,
        };
        let mut writer = SegmentWriter::create(dir.path(), header).unwrap();

        // Artificially push data_size to just below the cap so the next
        // append tips it over without allocating a gigabyte of real data.
        writer.data_size = SegmentFile::MAX_READ_DATA_SIZE;

        let err = writer.append(&make_batch(1)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("exceed maximum data size"));
    }

    #[test]
    fn create_without_zstd_flag_does_not_compress() {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_batch(10);

        // flags = 0 means no zstd compression.
        let header_no_zstd = SegmentHeader {
            version: SEGMENT_VERSION,
            flags: 0,
            segment_id: 1,
            sql_hash: 0,
            created_epoch_ms: 0,
        };
        let mut writer = SegmentWriter::create(dir.path(), header_no_zstd).unwrap();
        writer.append(&batch).unwrap();
        let seg_no_zstd = writer.finish().unwrap();

        // Same data with zstd enabled.
        let header_zstd = SegmentHeader {
            version: SEGMENT_VERSION,
            flags: FLAG_ZSTD,
            segment_id: 2,
            sql_hash: 0,
            created_epoch_ms: 0,
        };
        let mut writer_z = SegmentWriter::create(dir.path(), header_zstd).unwrap();
        writer_z.append(&batch).unwrap();
        let seg_zstd = writer_z.finish().unwrap();

        // Both should be readable and contain the same rows.
        let batches_no_zstd = seg_no_zstd.read_batches().unwrap();
        let batches_zstd = seg_zstd.read_batches().unwrap();
        assert_eq!(batches_no_zstd[0].num_rows(), batches_zstd[0].num_rows());

        // Uncompressed IPC stream is typically larger than zstd-compressed.
        assert!(
            seg_no_zstd.footer.data_size >= seg_zstd.footer.data_size,
            "expected uncompressed ({}) >= compressed ({})",
            seg_no_zstd.footer.data_size,
            seg_zstd.footer.data_size,
        );
    }

    #[test]
    fn open_new_segment_overflow_safety() {
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("segments");

        let policy = RotationPolicy::default();
        // Start with next_segment_id at u64::MAX — open_new_segment must fail
        // before creating a file or mutating manager state.
        let mut mgr = SegmentManager::new(seg_dir.clone(), policy, 0, u64::MAX).unwrap();

        let err = mgr.append(&make_batch(1)).unwrap_err();
        assert!(
            err.to_string().contains("segment ID overflow"),
            "unexpected error: {err}"
        );
        assert_eq!(mgr.current_segment_id(), None);
        assert!(
            !seg_dir.exists() || fs::read_dir(&seg_dir).unwrap().next().is_none(),
            "overflow must not leave a partially opened segment on disk"
        );
    }
}

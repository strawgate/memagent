//! CRI (Container Runtime Interface) log format parser.
//!
//! Kubernetes container runtimes (containerd, CRI-O) write logs in this format:
//!   2024-01-15T10:30:00.123456789Z stdout F {"actual":"json","log":"line"}
//!   ^timestamp                     ^stream ^flags ^message
//!
//! - timestamp: RFC3339Nano
//! - stream: "stdout" or "stderr"
//! - flags: "F" (full line) or "P" (partial — line was split at 16KB boundary)
//! - message: the actual log content (rest of the line)
//!
//! Partial lines (flag "P") must be reassembled: concatenate all "P" chunks
//! until an "F" chunk arrives, then emit the combined line.

/// Parsed CRI log line. References point into the original byte slice (zero-copy).
#[derive(Debug)]
pub struct CriLine<'a> {
    /// The RFC3339Nano timestamp bytes.
    pub timestamp: &'a [u8],
    /// "stdout" or "stderr".
    pub stream: &'a [u8],
    /// true if this is a complete line (flag "F"), false if partial ("P").
    pub is_full: bool,
    /// The actual log message content.
    pub message: &'a [u8],
}

/// Parse a single CRI log line. Returns None if the format is invalid.
///
/// This is zero-copy — all returned slices point into the input `line`.
/// The only work is finding the 3 space delimiters.
#[inline]
pub fn parse_cri_line(line: &[u8]) -> Option<CriLine<'_>> {
    // Format: "TIMESTAMP STREAM FLAGS MESSAGE"
    // Find first space (after timestamp).
    let sp1 = memchr::memchr(b' ', line)?;
    if sp1 + 1 >= line.len() {
        return None;
    }

    // Find second space (after stream).
    let sp2 = memchr::memchr(b' ', &line[sp1 + 1..])? + sp1 + 1;
    if sp2 + 1 >= line.len() {
        return None;
    }

    // Find third space (after flags).
    let msg_start = if let Some(sp3) = memchr::memchr(b' ', &line[sp2 + 1..]) {
        sp2 + 1 + sp3 + 1
    } else {
        // No message content (just flags, no trailing space) — empty message.
        line.len()
    };

    let flags = &line[sp2 + 1..if msg_start > sp2 + 1 {
        msg_start - 1
    } else {
        line.len()
    }];
    let is_full = flags == b"F";

    let message = if msg_start < line.len() {
        &line[msg_start..]
    } else {
        &[]
    };

    Some(CriLine {
        timestamp: &line[..sp1],
        stream: &line[sp1 + 1..sp2],
        is_full,
        message,
    })
}

/// CRI partial line reassembler. Buffers "P" (partial) chunks and emits
/// the combined line when an "F" (full) chunk arrives.
pub struct CriReassembler {
    /// Buffer for accumulating partial line chunks.
    partial_buf: Vec<u8>,
    /// Maximum assembled line size. Lines exceeding this are truncated.
    max_line_size: usize,
}

impl CriReassembler {
    pub fn new(max_line_size: usize) -> Self {
        CriReassembler {
            partial_buf: Vec::new(),
            max_line_size,
        }
    }

    /// Feed a parsed CRI line. Returns the complete message if this was
    /// an "F" line (or the final "F" after a series of "P" lines).
    /// Returns None for "P" lines (buffered internally).
    pub fn feed<'a>(&'a mut self, cri: &CriLine<'_>) -> Option<&'a [u8]> {
        if cri.is_full {
            if self.partial_buf.is_empty() {
                // Common fast path: complete line, no partials pending.
                // We can't return a reference to cri.message because it borrows
                // something else. Copy into partial_buf and return that.
                self.partial_buf.clear();
                self.partial_buf.extend_from_slice(cri.message);
            } else {
                // Append the final chunk to the partial buffer.
                let remaining = self.max_line_size.saturating_sub(self.partial_buf.len());
                let to_add = cri.message.len().min(remaining);
                self.partial_buf.extend_from_slice(&cri.message[..to_add]);
            }
            Some(&self.partial_buf)
        } else {
            // Partial line — buffer it.
            let remaining = self.max_line_size.saturating_sub(self.partial_buf.len());
            let to_add = cri.message.len().min(remaining);
            self.partial_buf.extend_from_slice(&cri.message[..to_add]);
            None
        }
    }

    /// Reset the partial buffer (call after consuming the emitted line).
    pub fn reset(&mut self) {
        self.partial_buf.clear();
    }
}

/// Process a chunk of CRI-formatted log data. Parses each CRI line, reassembles
/// partials, and calls `emit` with each complete log message.
///
/// Returns the number of complete lines emitted.
pub fn process_cri_chunk<F>(chunk: &[u8], reassembler: &mut CriReassembler, mut emit: F) -> usize
where
    F: FnMut(&[u8]),
{
    let mut count = 0;
    let mut line_start = 0;

    for pos in memchr::memchr_iter(b'\n', chunk) {
        let line = &chunk[line_start..pos];
        line_start = pos + 1;

        if line.is_empty() {
            continue;
        }

        if let Some(cri) = parse_cri_line(line)
            && let Some(complete_msg) = reassembler.feed(&cri)
        {
            emit(complete_msg);
            count += 1;
            reassembler.reset();
        }
    }

    count
}

/// Process CRI data and write extracted messages directly into an output buffer.
/// Each message is written as: optional JSON prefix + message bytes + newline.
///
/// For the common case (full lines, no partials), the message bytes come straight
/// from the input chunk — zero per-line allocation. Only partial line reassembly
/// copies into the reassembler's internal buffer.
///
/// `json_prefix`: if Some, injected after the opening `{` of each JSON message.
///   Example: `Some(b"\"kubernetes.pod_name\":\"my-pod\",")` turns
///   `{"msg":"hi"}` into `{"kubernetes.pod_name":"my-pod","msg":"hi"}`
///
/// Returns the number of complete lines written.
pub fn process_cri_to_buf(
    chunk: &[u8],
    reassembler: &mut CriReassembler,
    json_prefix: Option<&[u8]>,
    out: &mut Vec<u8>,
) -> usize {
    let mut count = 0;
    let mut line_start = 0;

    for pos in memchr::memchr_iter(b'\n', chunk) {
        let line = &chunk[line_start..pos];
        line_start = pos + 1;

        if line.is_empty() {
            continue;
        }

        if let Some(cri) = parse_cri_line(line)
            && let Some(complete_msg) = reassembler.feed(&cri)
        {
            write_json_line(complete_msg, json_prefix, out);
            count += 1;
            reassembler.reset();
        }
    }

    count
}

/// Write a single message into the output buffer with optional JSON prefix injection.
#[inline]
fn write_json_line(msg: &[u8], json_prefix: Option<&[u8]>, out: &mut Vec<u8>) {
    if let Some(prefix) = json_prefix {
        if msg.first() == Some(&b'{') {
            out.push(b'{');
            out.extend_from_slice(prefix);
            out.extend_from_slice(&msg[1..]);
        } else {
            out.extend_from_slice(msg);
        }
    } else {
        out.extend_from_slice(msg);
    }
    out.push(b'\n');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_full_line() {
        let line =
            b"2024-01-15T10:30:00.123456789Z stdout F {\"level\":\"INFO\",\"msg\":\"hello\"}";
        let cri = parse_cri_line(line).unwrap();
        assert_eq!(cri.timestamp, b"2024-01-15T10:30:00.123456789Z");
        assert_eq!(cri.stream, b"stdout");
        assert!(cri.is_full);
        assert_eq!(cri.message, b"{\"level\":\"INFO\",\"msg\":\"hello\"}");
    }

    #[test]
    fn test_parse_partial_line() {
        let line = b"2024-01-15T10:30:00.123Z stderr P partial content here";
        let cri = parse_cri_line(line).unwrap();
        assert_eq!(cri.stream, b"stderr");
        assert!(!cri.is_full);
        assert_eq!(cri.message, b"partial content here");
    }

    #[test]
    fn test_reassemble_partials() {
        let mut reassembler = CriReassembler::new(1024 * 1024);

        let p1 = parse_cri_line(b"2024-01-15T10:30:00Z stdout P first part").unwrap();
        assert!(reassembler.feed(&p1).is_none());

        let p2 = parse_cri_line(b"2024-01-15T10:30:00Z stdout P second part").unwrap();
        assert!(reassembler.feed(&p2).is_none());

        let f = parse_cri_line(b"2024-01-15T10:30:00Z stdout F final part").unwrap();
        let complete = reassembler.feed(&f).unwrap();
        assert_eq!(complete, b"first partsecond partfinal part");
        reassembler.reset();
    }

    #[test]
    fn test_reassemble_no_partials() {
        let mut reassembler = CriReassembler::new(1024 * 1024);

        let f = parse_cri_line(b"2024-01-15T10:30:00Z stdout F complete line").unwrap();
        let complete = reassembler.feed(&f).unwrap();
        assert_eq!(complete, b"complete line");
        reassembler.reset();
    }

    #[test]
    fn test_process_chunk() {
        let chunk = b"2024-01-15T10:30:00Z stdout F line one\n\
                       2024-01-15T10:30:01Z stdout F line two\n\
                       2024-01-15T10:30:02Z stderr F line three\n";
        let mut reassembler = CriReassembler::new(1024 * 1024);
        let mut lines = Vec::new();
        let count = process_cri_chunk(chunk, &mut reassembler, |msg| {
            lines.push(msg.to_vec());
        });
        assert_eq!(count, 3);
        assert_eq!(lines[0], b"line one");
        assert_eq!(lines[1], b"line two");
        assert_eq!(lines[2], b"line three");
    }

    #[test]
    fn test_max_line_size() {
        let mut reassembler = CriReassembler::new(20);

        let p1 = parse_cri_line(b"2024-01-15T10:30:00Z stdout P 0123456789").unwrap();
        reassembler.feed(&p1);

        let p2 = parse_cri_line(b"2024-01-15T10:30:00Z stdout P abcdefghij").unwrap();
        reassembler.feed(&p2);

        let f = parse_cri_line(b"2024-01-15T10:30:00Z stdout F KLMNOPQRST").unwrap();
        let complete = reassembler.feed(&f).unwrap();
        // Should be truncated to max_line_size=20
        assert_eq!(complete.len(), 20);
        reassembler.reset();
    }
}

//! Byte and slice iteration reference implementations.
//!
//! Simple linear-scan implementations used as comparison targets for
//! verifying the optimized (memchr-accelerated) production implementations.

/// Find the first occurrence of `needle` in `haystack` starting at `from`.
#[inline]
pub fn find_byte(haystack: &[u8], needle: u8, from: usize) -> Option<usize> {
    let mut i = from;
    while i < haystack.len() {
        if haystack[i] == needle {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Find the last occurrence of `needle` in `haystack[..end]`.
#[inline]
pub fn rfind_byte(haystack: &[u8], needle: u8, end: usize) -> Option<usize> {
    if end == 0 || haystack.is_empty() {
        return None;
    }
    let last = end.min(haystack.len());
    let mut i = last;
    while i > 0 {
        i -= 1;
        if haystack[i] == needle {
            return Some(i);
        }
    }
    None
}

/// Skip only ASCII space characters (0x20).
#[inline]
pub fn skip_space(buf: &[u8], mut pos: usize, end: usize) -> usize {
    let limit = end.min(buf.len());
    while pos < limit {
        if buf[pos] == b' ' {
            pos += 1;
        } else {
            return pos;
        }
    }
    pos
}

/// Skip whitespace characters (space, tab, carriage return).
#[inline]
pub fn skip_whitespace(buf: &[u8], mut pos: usize, end: usize) -> usize {
    let limit = end.min(buf.len());
    while pos < limit {
        match buf[pos] {
            b' ' | b'\t' | b'\r' => {
                pos += 1;
            }
            _ => return pos,
        }
    }
    pos
}

/// Skip characters until the predicate returns true.
#[inline]
pub fn skip_until<F>(buf: &[u8], mut pos: usize, end: usize, pred: F) -> usize
where
    F: Fn(u8) -> bool,
{
    let limit = end.min(buf.len());
    while pos < limit {
        if pred(buf[pos]) {
            return pos;
        }
        pos += 1;
    }
    pos
}

/// Iterator over byte positions. Reference implementation of
/// `FindByteIter` from `logfwd-core::byte_search`.
pub struct FindByteIter<'a> {
    haystack: &'a [u8],
    needle: u8,
    pos: usize,
}

impl<'a> FindByteIter<'a> {
    /// Create a new iterator over byte positions.
    pub fn new(haystack: &'a [u8], needle: u8) -> Self {
        Self {
            haystack,
            needle,
            pos: 0,
        }
    }
}

impl Iterator for FindByteIter<'_> {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        match find_byte(self.haystack, self.needle, self.pos) {
            Some(pos) => {
                self.pos = pos + 1;
                Some(pos)
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_byte_basic() {
        assert_eq!(find_byte(b"abc", b'b', 0), Some(1));
        assert_eq!(find_byte(b"abc", b'd', 0), None);
        assert_eq!(find_byte(b"abcbc", b'b', 2), Some(3));
    }

    #[test]
    fn rfind_byte_basic() {
        assert_eq!(rfind_byte(b"abcba", b'b', 5), Some(3));
        assert_eq!(rfind_byte(b"abcba", b'b', 2), Some(1));
        assert_eq!(rfind_byte(b"abc", b'd', 3), None);
        assert_eq!(rfind_byte(b"abc", b'a', 0), None);
    }

    #[test]
    fn skip_whitespace_basic() {
        assert_eq!(skip_whitespace(b"  \thello", 0, 8), 3);
        assert_eq!(skip_whitespace(b"hello", 0, 5), 0);
    }

    #[test]
    fn find_byte_iter_basic() {
        let mut iter = FindByteIter::new(b"a.b.c", b'.');
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.next(), Some(3));
        assert_eq!(iter.next(), None);
    }
}

#[cfg(kani)]
mod verification {
    use super::*;

    #[kani::proof]
    #[kani::unwind(17)]
    fn verify_find_byte() {
        let haystack: [u8; 16] = kani::any();
        let needle: u8 = kani::any();
        let from: usize = kani::any();
        kani::assume(from <= 16);
        let res = find_byte(&haystack, needle, from);
        kani::cover!(res.is_some(), "match found");
        kani::cover!(res.is_none(), "no match");
    }

    #[kani::proof]
    #[kani::unwind(17)]
    fn verify_rfind_byte() {
        let haystack: [u8; 16] = kani::any();
        let needle: u8 = kani::any();
        let end: usize = kani::any();
        kani::assume(end <= 16);
        let res = rfind_byte(&haystack, needle, end);
        kani::cover!(res.is_some(), "match found");
        kani::cover!(res.is_none(), "no match");
    }

    #[kani::proof]
    #[kani::unwind(9)]
    fn verify_skip_whitespace() {
        let buf: [u8; 8] = kani::any();
        let from: usize = kani::any();
        kani::assume(from <= 8);
        let pos = skip_whitespace(&buf, from, 8);
        assert!(pos >= from);
        assert!(pos <= 8);
        kani::cover!(pos == 8, "skip to end");
        kani::cover!(pos < 8 && pos > from, "partial skip");
    }

    #[kani::proof]
    #[kani::unwind(9)]
    fn verify_skip_until() {
        let buf: [u8; 8] = kani::any();
        let from: usize = kani::any();
        kani::assume(from <= 8);
        let target: u8 = kani::any();
        let pos = skip_until(&buf, from, 8, |b| b == target);
        assert!(pos >= from);
        assert!(pos <= 8);
        kani::cover!(pos < 8, "predicate met");
        kani::cover!(pos == 8, "predicate never met");
    }
}

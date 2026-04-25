//! Byte search utilities — Kani-provable alternatives to memchr.
//!
//! These use plain byte loops instead of memchr so Kani can formally
//! verify them. LLVM auto-vectorizes the loops, so runtime performance
//! is comparable to hand-written SIMD.

/// Find the first occurrence of `needle` in `haystack` starting at `from`.
///
/// Returns the index of the first match, or None if not found.
/// Equivalent to `memchr::memchr(needle, &haystack[from..]).map(|p| p + from)`.
///
/// Formally verified by Kani for all 16-byte inputs (see proof below).
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
///
/// Returns the index of the last match, or None if not found.
/// Equivalent to `memchr::memrchr(needle, &haystack[..end])`.
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

/// Iterate over all positions of `needle` in `haystack`.
///
/// Returns an iterator yielding byte offsets. Equivalent to
/// `memchr::memchr_iter(needle, haystack)`.
#[inline]
pub fn find_byte_iter(haystack: &[u8], needle: u8) -> FindByteIter<'_> {
    FindByteIter {
        haystack,
        needle,
        pos: 0,
    }
}

/// Iterator over byte positions. See `find_byte_iter`.
pub struct FindByteIter<'a> {
    haystack: &'a [u8],
    needle: u8,
    pos: usize,
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
    use alloc::{vec, vec::Vec};

    #[test]
    fn find_byte_basic() {
        assert_eq!(find_byte(b"hello world", b' ', 0), Some(5));
        assert_eq!(find_byte(b"hello world", b'o', 0), Some(4));
        assert_eq!(find_byte(b"hello world", b'o', 5), Some(7));
        assert_eq!(find_byte(b"hello", b'x', 0), None);
        assert_eq!(find_byte(b"", b' ', 0), None);
    }

    #[test]
    fn rfind_byte_basic() {
        assert_eq!(rfind_byte(b"hello world", b'o', 11), Some(7));
        assert_eq!(rfind_byte(b"hello world", b'o', 5), Some(4));
        assert_eq!(rfind_byte(b"hello", b'x', 5), None);
        assert_eq!(rfind_byte(b"", b' ', 0), None);
        assert_eq!(rfind_byte(b"abc", b'a', 0), None);
    }

    #[test]
    fn find_byte_iter_basic() {
        let positions: Vec<usize> = find_byte_iter(b"a.b.c", b'.').collect();
        assert_eq!(positions, vec![1, 3]);

        let positions: Vec<usize> = find_byte_iter(b"no dots", b'.').collect();
        assert!(positions.is_empty());
    }

    #[test]
    fn find_byte_iter_newlines() {
        let input = b"line1\nline2\nline3\n";
        let positions: Vec<usize> = find_byte_iter(input, b'\n').collect();
        assert_eq!(positions, vec![5, 11, 17]);
    }
}

#[cfg(kani)]
mod verification {
    use super::*;

    /// Prove find_byte returns the FIRST match and never panics.
    /// Tested on 16-byte inputs — function is a trivial loop, so
    /// correctness does not depend on buffer size.
    #[kani::proof]
    #[kani::unwind(18)]
    fn verify_find_byte_correct() {
        let haystack: [u8; 16] = kani::any();
        let needle: u8 = kani::any();
        let from: usize = kani::any();
        kani::assume(from <= 16);

        let result = find_byte(&haystack, needle, from);

        match result {
            Some(pos) => {
                assert!(pos >= from, "found before start");
                assert!(pos < haystack.len(), "found past end");
                assert!(haystack[pos] == needle, "wrong byte");
                // No earlier match
                let mut k = from;
                while k < pos {
                    assert!(haystack[k] != needle, "earlier match exists");
                    k += 1;
                }
            }
            None => {
                let mut k = from;
                while k < haystack.len() {
                    assert!(haystack[k] != needle, "match exists but returned None");
                    k += 1;
                }
            }
        }

        // Guard vacuity: verify from constraint is meaningful
        kani::cover!(result.is_some(), "needle found");
        kani::cover!(result.is_none(), "needle not found");
        kani::cover!(from > 0, "non-zero start position");
    }

    /// Prove rfind_byte returns the LAST match and never panics.
    /// Tested on 16-byte inputs — same trivial loop, size-independent.
    #[kani::proof]
    #[kani::unwind(18)]
    fn verify_rfind_byte_correct() {
        let haystack: [u8; 16] = kani::any();
        let needle: u8 = kani::any();
        let end: usize = kani::any();
        kani::assume(end <= 16);

        let result = rfind_byte(&haystack, needle, end);

        match result {
            Some(pos) => {
                assert!(pos < end, "found at or past end");
                assert!(haystack[pos] == needle, "wrong byte");
                // No later match before end
                let mut k = pos + 1;
                while k < end {
                    assert!(haystack[k] != needle, "later match exists");
                    k += 1;
                }
            }
            None => {
                let mut k = 0;
                while k < end.min(haystack.len()) {
                    assert!(haystack[k] != needle, "match exists but returned None");
                    k += 1;
                }
            }
        }

        // Guard vacuity: verify end constraint is meaningful
        kani::cover!(result.is_some(), "needle found in reverse");
        kani::cover!(result.is_none(), "needle not found in reverse");
        kani::cover!(end < 16, "non-full end position");
    }

    /// Prove find_byte_iter yields ALL positions of needle, in ascending
    /// order, with no missing or duplicate results.
    /// Tested on 8-byte inputs — the iterator delegates to find_byte
    /// (already proven), so this proves the wrapper is correct.
    #[kani::proof]
    #[kani::unwind(10)]
    fn verify_find_byte_iter_exhaustive() {
        let haystack: [u8; 8] = kani::any();
        let needle: u8 = kani::any();

        // Collect all positions from the iterator
        let mut iter = find_byte_iter(&haystack, needle);
        let mut prev: Option<usize> = None;
        let mut count: usize = 0;

        // Unroll: iterator on 8-byte input can return at most 8 results
        let mut k = 0;
        while k < 9 {
            match iter.next() {
                Some(pos) => {
                    assert!(pos < 8, "position out of bounds");
                    assert!(haystack[pos] == needle, "wrong byte at yielded position");
                    // Strictly ascending
                    if let Some(p) = prev {
                        assert!(pos > p, "not strictly ascending");
                    }
                    prev = Some(pos);
                    count += 1;
                }
                None => break,
            }
            k += 1;
        }

        // Verify completeness: count must equal actual occurrences
        let mut expected = 0;
        let mut i = 0;
        while i < 8 {
            if haystack[i] == needle {
                expected += 1;
            }
            i += 1;
        }
        assert_eq!(count, expected, "iterator missed or duplicated positions");

        // Guard against vacuous proof: verify interesting paths are reachable
        kani::cover!(count > 0, "iterator yields at least one match");
        kani::cover!(count > 1, "iterator yields multiple matches");
        kani::cover!(count == 0, "iterator yields nothing when no matches");
    }
}

//! Reference hex encoding/decoding implementations.

/// Reference hex nibble decoder (branch-based, not LUT).
///
/// Returns the 4-bit value of a hex ASCII character, or `0xFF` for
/// non-hex bytes.
pub fn hex_nibble_oracle(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => 0xFF,
    }
}

/// Reference hex decode: converts pairs of hex ASCII bytes into raw bytes.
///
/// Returns `false` if `hex_bytes.len() != out.len() * 2` or if any byte is
/// not valid hex.
pub fn hex_decode_oracle(hex_bytes: &[u8], out: &mut [u8]) -> bool {
    if hex_bytes.len() != out.len() * 2 {
        return false;
    }
    let mut i = 0;
    while i < out.len() {
        let hi = hex_nibble_oracle(hex_bytes[i * 2]);
        let lo = hex_nibble_oracle(hex_bytes[i * 2 + 1]);
        if hi > 0x0F || lo > 0x0F {
            return false;
        }
        out[i] = (hi << 4) | lo;
        i += 1;
    }
    true
}

/// Reference hex encode: converts each byte to two lowercase hex ASCII bytes.
///
/// Writes `bytes.len() * 2` bytes into `out`. Returns `false` if `out` is
/// too small.
pub fn hex_encode_oracle(bytes: &[u8], out: &mut [u8]) -> bool {
    if out.len() < bytes.len() * 2 {
        return false;
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut i = 0;
    while i < bytes.len() {
        out[i * 2] = HEX[(bytes[i] >> 4) as usize];
        out[i * 2 + 1] = HEX[(bytes[i] & 0x0f) as usize];
        i += 1;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_nibble_valid() {
        assert_eq!(hex_nibble_oracle(b'0'), 0);
        assert_eq!(hex_nibble_oracle(b'9'), 9);
        assert_eq!(hex_nibble_oracle(b'a'), 10);
        assert_eq!(hex_nibble_oracle(b'f'), 15);
        assert_eq!(hex_nibble_oracle(b'A'), 10);
        assert_eq!(hex_nibble_oracle(b'F'), 15);
    }

    #[test]
    fn hex_nibble_invalid() {
        assert_eq!(hex_nibble_oracle(b'g'), 0xFF);
        assert_eq!(hex_nibble_oracle(b' '), 0xFF);
    }

    #[test]
    fn hex_decode_roundtrip() {
        let hex_input = b"48656c6c6f";
        let mut out = [0u8; 5];
        assert!(hex_decode_oracle(hex_input, &mut out));
        assert_eq!(&out, b"Hello");
    }

    #[test]
    fn hex_encode_roundtrip() {
        let input = b"Hello";
        let mut out = [0u8; 10];
        assert!(hex_encode_oracle(input, &mut out));
        assert_eq!(&out, b"48656c6c6f");
    }

    #[test]
    fn hex_decode_length_mismatch() {
        let hex_input = b"48656c";
        let mut out = [0u8; 2];
        assert!(!hex_decode_oracle(hex_input, &mut out));
    }

    #[test]
    fn hex_encode_output_too_small() {
        let input = b"Hi";
        let mut out = [0u8; 3];
        assert!(!hex_encode_oracle(input, &mut out));
    }
}

#[cfg(kani)]
mod verification {
    use super::*;

    #[kani::proof]
    fn verify_hex_nibble_valid_range() {
        let c: u8 = kani::any();
        let result = hex_nibble_oracle(c);
        if c.is_ascii_hexdigit() {
            assert!(result <= 0x0F);
        } else {
            assert_eq!(result, 0xFF);
        }
    }

    #[kani::proof]
    #[kani::unwind(5)]
    fn verify_hex_decode_encode_roundtrip() {
        let input: [u8; 2] = kani::any();
        let mut hex_buf = [0u8; 4];
        assert!(hex_encode_oracle(&input, &mut hex_buf));
        let mut decoded = [0u8; 2];
        assert!(hex_decode_oracle(&hex_buf, &mut decoded));
        assert_eq!(input[0], decoded[0]);
        assert_eq!(input[1], decoded[1]);
    }
}

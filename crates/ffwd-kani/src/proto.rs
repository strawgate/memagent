//! Protobuf and varint verification oracles and stubs.

/// Predict the encoded length of a 64-bit varint (1–10 bytes).
#[cfg_attr(kani, kani::ensures(|result: &usize| *result >= 1 && *result <= 10))]
pub fn varint_len_oracle(mut value: u64) -> usize {
    if value == 0 {
        return 1;
    }
    let mut len = 0;
    while value > 0 {
        len += 1;
        value >>= 7;
    }
    len
}

/// Predict the size of a protobuf tag (`field_number << 3 | wire_type`).
#[cfg_attr(kani, kani::ensures(|result: &usize| *result >= 1 && *result <= 10))]
pub fn tag_size_oracle(field_number: u32) -> usize {
    let tag = (field_number as u64) << 3;
    varint_len_oracle(tag)
}

/// Predict the total encoded size of a protobuf bytes field
/// (tag varint + length varint + data).
///
/// Uses plain addition to match production semantics (overflow wraps in
/// release mode, same as `ffwd-core::otlp::bytes_field_size`).
#[cfg_attr(kani, kani::ensures(|result: &usize|
    // tag + len varints each contribute 1-10 bytes
    *result >= data_len + 2 && *result <= data_len + 20
))]
pub fn bytes_field_total_size_oracle(field_number: u32, data_len: usize) -> usize {
    let tag_size = tag_size_oracle(field_number);
    let len_size = varint_len_oracle(data_len as u64);
    tag_size + len_size + data_len
}

/// Oracle for `decode_varint`: decodes a protobuf varint from `data`.
///
/// Protobuf varint: each byte uses 7 bits for value, MSB as continuation flag.
/// Returns `None` if data is empty/too short or if varint exceeds 10 bytes.
pub fn decode_varint_oracle(data: &[u8]) -> Option<(u64, usize)> {
    if data.is_empty() {
        return None;
    }
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    let mut i = 0;
    loop {
        if i >= data.len() {
            return None;
        }
        let byte = data[i];
        i += 1;
        if shift == 63 && (byte & 0x7F) > 1 {
            return None;
        }
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((value, i));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}

/// Oracle for `decode_tag`: decodes a protobuf tag into `(field_number, wire_type, new_pos)`.
///
/// A protobuf tag is a varint encoding `field_number << 3 | wire_type`.
/// Returns `None` if the varint decode fails.
#[cfg_attr(kani, kani::ensures(|result: &Option<(u32, u8, usize)>| {
    match result {
        Some((_fn, wt, new_pos)) => *wt <= 7 && *new_pos >= 1,
        None => true,
    }
}))]
pub fn decode_tag_oracle(buf: &[u8], pos: usize) -> Option<(u32, u8, usize)> {
    let (tag, new_pos) = decode_varint_oracle(&buf[pos..])?;
    let field_number = (tag >> 3) as u32;
    let wire_type = (tag & 0x07) as u8;
    Some((field_number, wire_type, new_pos))
}

/// Oracle for `skip_field`: skips a protobuf field value based on its wire type.
///
/// Returns `None` if the buffer is too short, the varint decode fails,
/// or the wire type is unsupported.
#[cfg_attr(kani, kani::ensures(|result: &Option<usize>| {
    match result {
        Some(end) => *end >= 1,
        None => true,
    }
}))]
pub fn skip_field_oracle(buf: &[u8], wire_type: u8, pos: usize) -> Option<usize> {
    match wire_type {
        0 => {
            // Varint.
            let (_, new_pos) = decode_varint_oracle(&buf[pos..])?;
            Some(new_pos)
        }
        1 => {
            // 64-bit fixed.
            let end = pos.checked_add(8)?;
            if end > buf.len() {
                return None;
            }
            Some(end)
        }
        2 => {
            // Length-delimited.
            let (len, new_pos) = decode_varint_oracle(&buf[pos..])?;
            let len_usize = usize::try_from(len).ok()?;
            let end = new_pos.checked_add(len_usize)?;
            if end > buf.len() {
                return None;
            }
            Some(end)
        }
        5 => {
            // 32-bit fixed.
            let end = pos.checked_add(4)?;
            if end > buf.len() {
                return None;
            }
            Some(end)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_len_zero() {
        assert_eq!(varint_len_oracle(0), 1);
    }

    #[test]
    fn varint_len_one_byte() {
        assert_eq!(varint_len_oracle(127), 1);
    }

    #[test]
    fn varint_len_two_bytes() {
        assert_eq!(varint_len_oracle(128), 2);
    }

    #[test]
    fn varint_len_max() {
        assert_eq!(varint_len_oracle(u64::MAX), 10);
    }

    #[test]
    fn tag_size_small_field() {
        assert_eq!(tag_size_oracle(1), 1); // (1 << 3) = 8, fits in 1 byte
    }

    #[test]
    fn bytes_field_total_size() {
        // field 1, 5 bytes data: tag(1) + len_varint(1) + data(5) = 7
        assert_eq!(bytes_field_total_size_oracle(1, 5), 7);
    }

    #[test]
    fn decode_varint_single_byte() {
        assert_eq!(decode_varint_oracle(&[0x00]), Some((0, 1)));
        assert_eq!(decode_varint_oracle(&[0x7F]), Some((127, 1)));
    }

    #[test]
    fn decode_varint_multi_byte() {
        // 128 = 0x80 → continuation byte needed
        assert_eq!(decode_varint_oracle(&[0x80, 0x01]), Some((128, 2)));
        // 300 = 0xAC 0x02
        assert_eq!(decode_varint_oracle(&[0xAC, 0x02]), Some((300, 2)));
    }

    #[test]
    fn decode_varint_empty() {
        assert_eq!(decode_varint_oracle(&[]), None);
    }

    #[test]
    fn decode_varint_truncated_continuation() {
        // 0x80 indicates continuation but no byte follows
        assert_eq!(decode_varint_oracle(&[0x80]), None);
    }

    #[test]
    fn decode_varint_overlong() {
        // 11 bytes of continuation (would exceed u64::MAX)
        assert_eq!(
            decode_varint_oracle(&[
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80
            ]),
            None
        );
    }

    #[test]
    fn decode_varint_tenth_byte_overflow_rejected() {
        // 10th byte carries payload 0x02 > 1 → exceeds u64 range
        assert_eq!(
            decode_varint_oracle(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02]),
            None
        );
    }

    #[test]
    fn decode_tag_oracle_single_byte() {
        // Field 1, wire type 2 (length-delimited): (1 << 3) | 2 = 10 = 0x0A
        assert_eq!(decode_tag_oracle(&[0x0A], 0), Some((1, 2, 1)));
    }

    #[test]
    fn decode_tag_oracle_multi_byte() {
        // Field 16, wire type 0: (16 << 3) | 0 = 128 = 0x80 0x01
        assert_eq!(decode_tag_oracle(&[0x80, 0x01], 0), Some((16, 0, 2)));
    }

    #[test]
    fn decode_tag_oracle_truncated() {
        assert_eq!(decode_tag_oracle(&[], 0), None);
    }

    #[test]
    fn skip_field_varint() {
        // Wire type 0, value 300: continuation needed
        assert_eq!(skip_field_oracle(&[0xAC, 0x02], 0, 0), Some(2));
    }

    #[test]
    fn skip_field_fixed64() {
        // Wire type 1, 8 bytes
        let buf = [0u8; 16];
        assert_eq!(skip_field_oracle(&buf, 1, 0), Some(8));
    }

    #[test]
    fn skip_field_length_delimited() {
        // Wire type 2, 3 bytes of data
        // Length varint = 3 (0x03), then 3 bytes
        assert_eq!(skip_field_oracle(&[0x03, 0x41, 0x42, 0x43], 2, 0), Some(4));
    }

    #[test]
    fn skip_field_fixed32() {
        // Wire type 5, 4 bytes
        let buf = [0u8; 16];
        assert_eq!(skip_field_oracle(&buf, 5, 0), Some(4));
    }

    #[test]
    fn skip_field_unsupported_wire_type() {
        assert_eq!(skip_field_oracle(&[0x00], 3, 0), None);
        assert_eq!(skip_field_oracle(&[0x00], 4, 0), None);
        assert_eq!(skip_field_oracle(&[0x00], 6, 0), None);
        assert_eq!(skip_field_oracle(&[0x00], 7, 0), None);
    }

    #[test]
    fn skip_field_truncated() {
        // Fixed64 but buffer too short
        assert_eq!(skip_field_oracle(&[0x00; 5], 1, 0), None);
        // Fixed32 but buffer too short
        assert_eq!(skip_field_oracle(&[0x00; 3], 5, 0), None);
        // Length-delimited truncated
        assert_eq!(skip_field_oracle(&[0x05], 2, 0), None);
    }
}
#[cfg(kani)]
mod verification {
    use super::*;

    #[kani::proof_for_contract(varint_len_oracle)]
    #[kani::unwind(12)]
    fn verify_varint_len_oracle_contract() {
        let val: u64 = kani::any();
        let res = varint_len_oracle(val);
        kani::cover!(res == 1, "single-byte varint reachable");
        kani::cover!(res == 10, "max-byte varint reachable");
    }

    #[kani::proof_for_contract(tag_size_oracle)]
    #[kani::unwind(12)]
    fn verify_tag_size_oracle_contract() {
        let field_number: u32 = kani::any();
        kani::assume(field_number > 0 && field_number <= 0x1FFFFFFF);
        let res = tag_size_oracle(field_number);
        kani::cover!(res == 1, "single-byte tag reachable");
        kani::cover!(res > 1, "multi-byte tag reachable");
    }

    #[kani::proof_for_contract(bytes_field_total_size_oracle)]
    #[kani::unwind(12)]
    fn verify_bytes_field_total_size_oracle_contract() {
        let field_number: u32 = kani::any();
        let data_len: usize = kani::any();
        kani::assume(field_number > 0 && field_number <= 0x1FFFFFFF);
        let res = bytes_field_total_size_oracle(field_number, data_len);
        kani::cover!(data_len == 0, "empty data reachable");
        kani::cover!(data_len > 0, "non-empty data reachable");
    }

    #[kani::proof]
    #[kani::unwind(22)]
    fn verify_decode_varint_oracle_no_panic() {
        let data: [u8; 10] = kani::any();
        let len: usize = kani::any_where(|&l| l <= 10);
        let res = decode_varint_oracle(&data[..len]);
        kani::cover!(res.is_some(), "successful decode reachable");
        kani::cover!(res.is_none(), "error decode reachable");
    }

    #[kani::proof_for_contract(decode_tag_oracle)]
    #[kani::unwind(22)]
    fn verify_decode_tag_oracle_contract() {
        let buf: [u8; 20] = kani::any();
        let pos: usize = kani::any_where(|&p| p < buf.len());
        let res = decode_tag_oracle(&buf, pos);
        kani::cover!(res.is_some(), "successful decode reachable");
        kani::cover!(res.is_none(), "decode error reachable");
    }

    #[kani::proof_for_contract(skip_field_oracle)]
    #[kani::unwind(22)]
    fn verify_skip_field_oracle_contract() {
        let buf: [u8; 20] = kani::any();
        let wire_type: u8 = kani::any();
        let pos: usize = kani::any_where(|&p| p < buf.len());
        let res = skip_field_oracle(&buf, wire_type, pos);
        kani::cover!(res.is_some(), "successful skip reachable");
        kani::cover!(res.is_none(), "skip error reachable");
    }
}

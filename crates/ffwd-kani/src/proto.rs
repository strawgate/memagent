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
pub fn tag_size_oracle(field_number: u32) -> usize {
    let tag = (field_number as u64) << 3;
    varint_len_oracle(tag)
}

/// Predict the total encoded size of a protobuf bytes field
/// (tag varint + length varint + data).
///
/// Uses plain addition to match production semantics (overflow wraps in
/// release mode, same as `ffwd-core::otlp::bytes_field_size`).
pub fn bytes_field_total_size_oracle(field_number: u32, data_len: usize) -> usize {
    let tag_size = tag_size_oracle(field_number);
    let len_size = varint_len_oracle(data_len as u64);
    tag_size + len_size + data_len
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

    #[kani::proof]
    #[kani::unwind(12)]
    fn verify_bytes_field_total_size_oracle_no_panic() {
        let field_number: u32 = kani::any();
        let data_len: usize = kani::any();
        kani::assume(field_number > 0 && field_number <= 0x1FFFFFFF);
        let res = bytes_field_total_size_oracle(field_number, data_len);
        kani::cover!(res > data_len, "total size includes overhead");
    }
}

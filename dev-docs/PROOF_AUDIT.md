# Kani Proof Audit

31 proofs as of 2026-03-30. For each: what it proves, what it
DOESN'T prove, and the gap between proof and real usage.

## chunk_classify.rs (2 proofs)

### verify_prefix_xor
**Proves:** Output matches naive running-XOR oracle for ALL u64 inputs.
**Doesn't prove:** How prefix_xor is composed into ChunkIndex::new().
**Gap:** None for the function itself. The composition with
compute_real_quotes is proven separately. The composition of both
into ChunkIndex::new (multi-block chaining) is NOT proven.

### verify_compute_real_quotes
**Proves:** Matches naive byte-by-byte escape oracle for ALL
(quote_bits, bs_bits, carry) triples. Three properties: submask,
oracle match, carry correctness.
**Doesn't prove:** Multi-block chaining (carry propagation across
multiple calls to compute_real_quotes). That's the ChunkIndex::new
composition, which is tested by proptest but not Kani-proven.
**Gap:** A bug in how ChunkIndex::new calls compute_real_quotes
in a loop would not be caught. The function itself is exhaustively
correct.

## scan_config.rs (2 proofs)

### verify_parse_int_fast_no_panic_8bytes
**Proves:** No panics for any 8-byte input. Also verifies decimal
roundtrip (encode → re-parse gives same value) when parse succeeds.
**Doesn't prove:** Inputs longer than 8 bytes. Real log lines can
have 19-digit integers (i64::MAX).
**Gap:** The vs_oracle proof below covers 20 bytes, so this is
partially redundant. The roundtrip property is valuable and unique
to this proof.

### verify_parse_int_fast_vs_oracle
**Proves:** Full behavioral equivalence with an i128 reference
parser for ALL inputs up to 20 bytes. Covers: correct parsing,
overflow detection, sign handling, non-digit rejection.
**Doesn't prove:** Inputs > 20 bytes (i64 is max 19 digits + sign,
so 20 bytes IS exhaustive for valid integers).
**Gap:** None. This is a complete proof for the function's domain.
**Warning:** Takes ~4 minutes. The i128 oracle adds significant
solver complexity.

## otlp.rs (14 proofs)

### verify_varint_len_matches_encode
**Proves:** varint_len(v) == encode_varint(v).len() for ALL u64.
**Gap:** None. Exhaustive.

### verify_varint_format_and_roundtrip
**Proves:** Valid protobuf format (continuation bits, length 1-10)
AND decode roundtrip for ALL u64.
**Gap:** None. Exhaustive.

### verify_varint_no_panic
**Proves:** No panics for ALL u64.
**Gap:** Redundant with format_and_roundtrip (which also can't
panic). Kept as a simple smoke check.

### verify_encode_tag
**Proves:** Correct field_number + wire_type encoding for all
valid tag combinations (field ≤ 1M, wire_type ≤ 5).
**Gap:** Field numbers > 1M are not tested. Protobuf allows up
to 2^29-1 but in practice fields are small.

### verify_days_from_civil
**Proves:** Epoch is day 0, dates in [1970,2100] are non-negative,
bounded < 50000, monotonic within months.
**Doesn't prove:** Correctness against an independent calendar
oracle (e.g., chrono). The monotonicity check is necessary but
not sufficient — the values could be consistently wrong.
**Gap:** MEDIUM. We verify structural properties but not absolute
correctness. A proptest oracle against chrono would close this.

### verify_bytes_field_size
**Proves:** Size prediction matches actual encode_bytes_field for
field numbers ≤ 1000 and data lengths ≤ 256.
**Gap:** Larger fields/data not tested. Probably fine — the function
is simple arithmetic.

### verify_parse_severity_no_panic
**Proves:** No panics for any 8-byte input.
**Gap:** Doesn't verify correctness — only crash-freedom.

### verify_parse_severity_known_values
**Proves:** All 12 standard level strings (6 upper + 6 lower) map
to correct enum values. Empty/unknown returns Unspecified.
**Gap:** Doesn't test mixed case ("Info", "wArN") or non-ASCII.
The function uses |0x20 which handles these correctly for ASCII
letters, but this isn't proven.

### verify_digit_parsers_no_panic
**Proves:** parse_2digits and parse_4digits never panic for any
8-byte input at any offset ≤ 6.
**Gap:** Doesn't verify the parsed VALUE is correct. Only crash-free.

### verify_parse_2digits_correct
**Proves:** Correct value for all valid 2-digit pairs (0-9 × 0-9).
**Gap:** None for valid inputs. Invalid inputs (non-digits) covered
by the no-panic proof.

### verify_parse_4digits_correct
**Proves:** Correct value for all valid 4-digit quads.
**Gap:** None for valid inputs.

### verify_key_eq_ignore_case_ascii_letters
**Proves:** Matches to_ascii_lowercase for all ASCII letter pairs.
**Gap:** Only tests ASCII letters. The function is USED on JSON keys
which may contain digits, underscores, etc. For non-letter bytes,
|0x20 behaves differently from to_ascii_lowercase (e.g., '@' |0x20
= '`'). This is fine because JSON key comparison with known keys
("timestamp", "level", "message") only involves letters.
**Risk:** If someone adds a key with non-letter chars to the lookup
table, the comparison might fail. LOW risk.

### verify_encode_fixed64
**Proves:** Tag + 8 LE bytes for all inputs with field ≤ 1000.
**Gap:** None for the tested range.

### verify_encode_varint_field
**Proves:** Tag + varint size correct for all inputs with field ≤ 1000.
**Gap:** None for the tested range.

## byte_search.rs (2 proofs)

### verify_find_byte_correct
**Proves:** Returns the FIRST match for all 16-byte inputs, all
needle values, all start positions. Verifies: found position is
correct, no earlier match exists. For None: no match exists.
**Gap:** 16-byte inputs only. Real usage is on multi-KB buffers.
The function is a trivial loop — correctness doesn't depend on
buffer size. But Kani only verifies the logic for ≤ 16 bytes.

### verify_rfind_byte_correct
**Proves:** Returns the LAST match for all 16-byte inputs. Same
completeness as find_byte but in reverse.
**Gap:** Same as find_byte.

## framer.rs (4 proofs)

### verify_newline_framer_no_panic
**Proves:** No panics for any 32-byte input.
**Gap:** Doesn't verify correctness — only crash-freedom. The
ranges_valid, remainder_correct, and content_correct proofs cover
correctness.

### verify_newline_framer_ranges_valid
**Proves:** All returned ranges are valid sub-ranges of input
(start ≤ end ≤ len) for 32-byte inputs.
**Doesn't prove:** That the ranges correspond to ACTUAL lines
(i.e., the bytes between newlines). The content_correct proof
closes this gap.
**Gap:** None — content_correct provides the oracle comparison.

### verify_newline_framer_remainder_correct
**Proves:** No newlines exist in the remainder for 16-byte inputs
(unless output is full).
**Gap:** 16-byte inputs (smaller than ranges_valid's 32). This is
a meaningful property — proves the framer didn't miss any newlines.

### verify_newline_framer_content_correct
**Proves:** Oracle proof for 16-byte inputs. Verifies: (1) no
newlines inside line ranges, (2) each line terminated by \n,
(3) no empty lines emitted, (4) every newline in the processed
region either terminates a line or is part of an empty line
(consecutive \n or leading \n).
**Gap:** 16-byte inputs. The logic is a simple loop so correctness
at 16 bytes implies correctness at larger sizes.

## cri.rs (4 proofs)

### verify_parse_cri_line_no_panic
**Proves:** No panics for any 32-byte input.
**Gap:** Doesn't verify field extraction correctness. Existing
unit tests cover known-good inputs but not all possible inputs.

### verify_reassembler_respects_max_size_pf
**Proves:** P+F sequence output ≤ max_message_size for any
8-byte messages and any max_size 1-32.
**Gap:** Only 8-byte fixed messages. Real P messages can be
up to 16KB. The truncation logic is the same regardless of
message size, so this is likely sufficient.

### verify_reassembler_respects_max_size_f_only
**Proves:** F-only output ≤ max_message_size.
**Gap:** Same fixed-size limitation as above. This proof
caught a real bug (F fast path wasn't enforcing max_line_size).

### verify_reassembler_output_only_on_full
**Proves:** P lines always return None (Pending).
**Gap:** None — this is a complete proof for the property.

## aggregator.rs (3 proofs)

### verify_aggregator_f_only_max_size
**Proves:** F-only output ≤ max_message_size for 16-byte messages.
**Gap:** Fixed 16-byte messages. Same logic as reassembler proofs.

### verify_aggregator_pf_max_size
**Proves:** P+F output ≤ max_message_size for 8-byte messages.
**Gap:** Fixed 8-byte messages.

### verify_aggregator_p_returns_pending
**Proves:** P lines return Pending, not Complete.
**Gap:** None.

## Summary of gaps

### GAPS THAT MATTER:
1. **ChunkIndex multi-block composition** — we prove each function
   but not the loop that chains them. proptest covers this.
2. **days_from_civil absolute correctness** — we prove structural
   properties but not "day N is actually correct." Need chrono oracle.
3. ~~NewlineFramer correctness~~ — CLOSED by content_correct proof.
4. **parse_cri_line field correctness** — no-panic only. Need
   oracle proving fields match expected positions.

### GAPS THAT DON'T MATTER:
1. **Fixed-size inputs** — functions are simple loops. Correctness
   at 16 bytes implies correctness at 16KB. The logic doesn't change.
2. **Bounded field numbers** — encode_tag with field ≤ 1M covers
   all real-world protobuf fields.
3. **Redundant no-panic proofs** — some are subsumed by correctness
   proofs. Kept as quick smoke checks (~0.1s each).

### WHAT TO ADD NEXT:
1. ~~Oracle proof for NewlineFramer~~ — DONE (content_correct)
2. Oracle proof for days_from_civil (compare against known date table)
3. Oracle proof for parse_cri_line (compare against manual space-finding)
4. Proptest state-machine for FormatParser (random chunk sequences)

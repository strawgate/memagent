# Kani Verification Patterns

Reusable proof patterns and templates from the logfwd codebase.

## Pattern 1: No-Panic Proof (Basic)

**When:** Any function handling untrusted input.

**Template:**
```rust
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(N)]  // N = buffer_size + 2
fn verify_<function>_no_panic() {
    let input: [u8; SIZE] = kani::any();
    let _ = my_function(&input);
    // Kani automatically checks: no panic, no overflow, no out-of-bounds
}
```

**Real example (framer.rs:210):**
```rust
#[kani::proof]
#[kani::unwind(34)]
fn verify_newline_framer_no_panic() {
    let input: [u8; 32] = kani::any();
    let _ = NewlineFramer.frame(&input);
}
```

## Pattern 2: Oracle Proof (Correctness)

**When:** Fast/complex implementation exists alongside naive reference.

**Template:**
```rust
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(N)]
fn verify_<function>_correct() {
    let input: [u8; SIZE] = kani::any();

    // Fast implementation under test
    let result = optimized_impl(&input);

    // Naive oracle (slow but obviously correct)
    let mut expected = T::default();
    let mut i = 0;
    while i < input.len() {
        // Simple logic here
        i += 1;
    }

    assert_eq!(result, expected);
}
```

**Real example (structural.rs:350):**
```rust
#[kani::proof]
#[kani::unwind(65)]
#[kani::solver(kissat)]
fn verify_compute_real_quotes() {
    let quote_bits: u64 = kani::any();
    let bs_bits: u64 = kani::any();
    let prev_carry: u64 = kani::any();
    kani::assume(prev_carry <= 1);

    let mut carry = prev_carry;
    let result = compute_real_quotes(quote_bits, bs_bits, &mut carry);

    // Oracle: naive byte-by-byte escape detection
    let mut escaped_naive: u64 = 0;
    let mut prev_was_unescaped_bs = prev_carry == 1;
    let mut pos = 0u32;
    while pos < 64 {
        let is_bs = (bs_bits >> pos) & 1 == 1;
        if prev_was_unescaped_bs {
            escaped_naive |= 1u64 << pos;
            prev_was_unescaped_bs = false;
        } else if is_bs {
            prev_was_unescaped_bs = true;
        }
        pos += 1;
    }
    let expected = quote_bits & !escaped_naive;
    assert_eq!(result, expected);
}
```

## Pattern 3: Bounded Property Proof

**When:** Verifying specific properties (size limits, invariants, range validity).

**Template:**
```rust
#[cfg(kani)]
#[kani::proof]
fn verify_<function>_<property>() {
    // Generate constrained symbolic input
    let param: usize = kani::any();
    kani::assume(param >= MIN && param <= MAX);

    let input: [u8; SIZE] = kani::any();
    let result = my_function(&input, param);

    // Assert property holds
    assert!(result.len() <= param, "property violated");
}
```

**Real example (aggregator.rs:175):**
```rust
#[kani::proof]
fn verify_aggregator_pf_max_size() {
    let max_size: usize = kani::any();
    kani::assume(max_size >= 1 && max_size <= 32);

    let mut agg = CriAggregator::new(max_size);

    let msg1: [u8; 8] = kani::any();
    let _ = agg.feed(&msg1, false);

    let msg2: [u8; 8] = kani::any();
    match agg.feed(&msg2, true) {
        AggregateResult::Complete(out) => {
            assert!(out.len() <= max_size, "P+F output exceeds max");
        }
        AggregateResult::Pending => panic!("F line should produce Complete"),
    }
}
```

## Pattern 4: Range Validity Proof

**When:** Function returns ranges/indices into input buffer.

**Template:**
```rust
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(N)]
fn verify_<function>_ranges_valid() {
    let input: [u8; SIZE] = kani::any();
    let output = my_function(&input);

    // Verify each range
    let mut i = 0;
    while i < output.count() {
        let (start, end) = output.range(i);
        assert!(start <= end, "invalid range: start > end");
        assert!(end <= input.len(), "range exceeds input");
        i += 1;
    }
}
```

**Real example (framer.rs:217):**
```rust
#[kani::proof]
#[kani::unwind(34)]
fn verify_newline_framer_ranges_valid() {
    let input: [u8; 32] = kani::any();
    let output = NewlineFramer.frame(&input);

    let mut i = 0;
    while i < output.count {
        let (start, end) = output.line_ranges[i];
        assert!(start <= end, "start > end");
        assert!(end <= input.len(), "end out of bounds");
        i += 1;
    }
    assert!(
        output.remainder_offset <= input.len(),
        "remainder out of bounds"
    );
}
```

## Pattern 5: State Machine Transitions

**When:** Protocol or state machine with multiple sequential operations.

**Template:**
```rust
#[cfg(kani)]
#[kani::proof]
fn verify_<function>_<sequence>() {
    let mut state_machine = StateMachine::new(MAX);

    // Step 1: symbolic input
    let input1: [u8; SIZE] = kani::any();
    let flag1: bool = kani::any();
    let result1 = state_machine.process(&input1, flag1);

    // Step 2: symbolic input
    let input2: [u8; SIZE] = kani::any();
    let flag2: bool = kani::any();
    let result2 = state_machine.process(&input2, flag2);

    // Assert final invariant
    assert!(state_machine.invariant_holds());
}
```

**Real example (aggregator.rs:200):**
```rust
#[kani::proof]
fn verify_aggregator_ppf_max_size() {
    let max_size: usize = kani::any();
    kani::assume(max_size >= 1 && max_size <= 32);

    let mut agg = CriAggregator::new(max_size);

    // P line
    let msg1: [u8; 8] = kani::any();
    let _ = agg.feed(&msg1, false);

    // P line
    let msg2: [u8; 8] = kani::any();
    let _ = agg.feed(&msg2, false);

    // F line
    let msg3: [u8; 8] = kani::any();
    match agg.feed(&msg3, true) {
        AggregateResult::Complete(out) => {
            assert!(out.len() <= max_size, "P+P+F output exceeds max");
        }
        AggregateResult::Pending => panic!("F line should produce Complete"),
    }
}
```

## Pattern 6: Partition Completeness

**When:** Function partitions input into regions (lines, tokens, fields).

**Template:**
```rust
#[cfg(kani)]
#[kani::proof]
#[kani::unwind(N)]
fn verify_<function>_partition_complete() {
    let input: [u8; SIZE] = kani::any();
    let output = partition_function(&input);

    // Verify bytes INSIDE ranges satisfy expected properties
    let mut i = 0;
    while i < output.count() {
        let (start, end) = output.range(i);
        let mut j = start;
        while j < end {
            assert!(input[j] != DELIMITER, "delimiter found inside range");
            j += 1;
        }
        i += 1;
    }

    // Verify bytes OUTSIDE ranges (gaps + remainder) are delimiters
    // (Implementation depends on specific partitioning logic)
}
```

**Real example (framer.rs:235):**
```rust
#[kani::proof]
#[kani::unwind(34)]
fn verify_newline_framer_remainder_correct() {
    let input: [u8; 16] = kani::any();
    let output = NewlineFramer.frame(&input);

    // If output isn't full, remainder should contain no newlines
    if output.count < MAX_LINES_PER_FRAME {
        let mut j = output.remainder_offset;
        while j < input.len() {
            assert!(input[j] != b'\n', "newline found in remainder");
            j += 1;
        }
    }
}
```

## Pattern 7: With Cover Statements (Vacuity Guard)

**When:** Proof uses `kani::assume()` or has complex branching.

**Template:**
```rust
#[cfg(kani)]
#[kani::proof]
fn verify_<function>_with_covers() {
    let input: T = kani::any();
    kani::assume(input.is_valid() && input.len() < MAX);

    let result = my_function(input);

    // Main assertion
    assert!(result.is_correct());

    // Guard against vacuity
    kani::cover!(result.is_some(), "success path reachable");
    kani::cover!(result.is_none(), "failure path reachable");
    kani::cover!(input.len() == 0, "empty input tested");
    kani::cover!(input.len() > 0, "non-empty input tested");
}
```

**Real example (byte_search.rs with added covers):**
```rust
#[kani::proof]
fn verify_find_byte_with_covers() {
    let haystack: [u8; 16] = kani::any();
    let needle: u8 = kani::any();
    let result = find_byte(&haystack, needle);

    // Oracle check
    let mut expected = None;
    for (i, &byte) in haystack.iter().enumerate() {
        if byte == needle {
            expected = Some(i);
            break;
        }
    }
    assert_eq!(result, expected);

    // Covers
    kani::cover!(result.is_some(), "needle found");
    kani::cover!(result.is_none(), "needle not found");
}
```

## Pattern 8: Bitmask Operations (Full Range)

**When:** Pure bitwise operations without loops.

**Template:**
```rust
#[cfg(kani)]
#[kani::proof]
fn verify_<function>_full_range() {
    // Full symbolic range — no bounds needed
    let input: u64 = kani::any();
    let result = bitmask_operation(input);

    // Oracle: compute expected with simple logic
    let mut expected: u64 = 0;
    // ... bit-by-bit logic ...

    assert_eq!(result, expected);
}
```

**Real example (structural.rs:325):**
```rust
#[kani::proof]
fn verify_prefix_xor() {
    let input: u64 = kani::any();
    let result = prefix_xor(input);

    // Oracle: naive loop
    let mut expected: u64 = 0;
    let mut running = false;
    let mut i = 0u32;
    while i < 64 {
        if (input >> i) & 1 == 1 {
            running = !running;
        }
        if running {
            expected |= 1u64 << i;
        }
        i += 1;
    }
    assert_eq!(result, expected, "prefix_xor mismatch");
}
```

## Pattern 9: Rejection Testing (Negative Cases)

**When:** Function should reject invalid inputs.

**Template:**
```rust
#[cfg(kani)]
#[kani::proof]
fn verify_<function>_rejects_<case>() {
    let input = construct_invalid_input();
    let result = my_function(&input);

    // Assert rejection
    match result {
        Err(_) => {} // Expected
        Ok(_) => panic!("should reject invalid input"),
    }
}
```

**Real example (cri.rs:450):**
```rust
#[kani::proof]
fn verify_parse_cri_line_rejects_invalid_flags() {
    let line = b"2024-01-01T00:00:00.000Z X tag msg";
    let result = parse_cri_line(line);
    assert!(result.is_none(), "should reject invalid flag");
}
```

## Pattern 10: Solver Selection for Slow Proofs

**When:** Proof takes > 10 seconds with default solver.

**Template:**
```rust
#[cfg(kani)]
#[kani::proof]
#[kani::solver(kissat)]  // or z3, bitwuzla for arithmetic
#[kani::unwind(N)]
fn verify_<complex_function>() {
    // Complex proof that benefits from kissat
}
```

**Real examples:**
- `verify_compute_real_quotes` uses `#[kani::solver(kissat)]` (structural.rs:350)
- Escape detection is arithmetic-heavy, kissat provides 47% speedup
- For pure arithmetic, try `z3` or `bitwuzla`

## Common Anti-Patterns

### ❌ Anti-Pattern 1: Over-constrained Input
```rust
// BAD: May eliminate all interesting paths
let x: u32 = kani::any();
kani::assume(x > 100 && x < 50);  // Impossible!
assert!(some_property(x));  // Vacuously true
```

**Fix:** Add cover statements to verify paths are reachable.

### ❌ Anti-Pattern 2: Trusting Internal State
```rust
// BAD: Oracle depends on code under test
let expected = my_struct.internal_count;  // Computed by code under test!
```

**Fix:** Compute expected independently from raw input.

### ❌ Anti-Pattern 3: Monolithic Proofs
```rust
// BAD: Tries to verify entire pipeline at once
fn verify_entire_parser() {
    let input: [u8; 1024] = kani::any();
    let result = parse_full_json(&input);  // Intractable!
}
```

**Fix:** Use function contracts to verify components independently.

### ❌ Anti-Pattern 4: Missing Unwind Bounds
```rust
// BAD: May not terminate
#[kani::proof]
fn verify_loop() {
    let arr: [u8; 100] = kani::any();
    process_array(&arr);  // Loop inside
}
```

**Fix:** Add `#[kani::unwind(102)]` (array size + 2).

### ❌ Anti-Pattern 5: Input Size Too Large
```rust
// BAD: Will timeout
let input: [u8; 1024] = kani::any();  // Too large for complex parsing
```

**Fix:** Start with 8-16 bytes, measure proof time, increase incrementally.

## Proof Checklist

Before submitting a Kani proof:

- [ ] Added `#[cfg(kani)]` to proof module
- [ ] Proof name follows convention: `verify_<function>_<property>`
- [ ] Appropriate `#[kani::unwind(N)]` for loops (N = iterations + 2)
- [ ] Input size is bounded and tractable (8-32 bytes for parsing)
- [ ] Added `kani::cover!()` if proof uses `kani::assume()`
- [ ] Independent oracle (if applicable) doesn't trust code under test
- [ ] Solver attribute added if proof > 10s (`#[kani::solver(kissat)]`)
- [ ] Proof verifies single property (not multiple unrelated things)
- [ ] Comments explain what property is being verified

## References

- Full Kani reference: `docs/references/kani-verification.md`
- Verification guide: `docs/VERIFICATION_GUIDE.md`
- Kani limits: `dev-docs/research/KANI_LIMITS.md`
- Codebase examples: `crates/logfwd-core/src/{framer,aggregator,structural,byte_search}.rs`

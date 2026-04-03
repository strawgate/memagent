# TLA+ Finding 002: Checkpoint must subtract framer buffer length

Date: 2026-04-03
Found by: TLC model checker — liveness counterexample for EventualEmission

## The counterexample

With `MaxLines=3, BatchSize=1`:

```
State 5: TailerRead → reads all 3 lines, framer_buf = <<1,2,3>>, read_offset = 3
State 6: FormBatch → batch = <<1>>, framer_buf = <<2,3>>
State 7: SendBatch → in_flight_offset = read_offset = 3 (BUG!)
State 8: AckBatch → checkpoints[source] = 3, but only line 1 was emitted
State 12: Crash → framer_buf (with lines 2,3) destroyed
State 13: Restart → read_offset = checkpoint = 3 = Len(file_content)
State 14: Stuttering — TailerRead disabled (3 < 3 is false)
```

**Lines 2 and 3 are permanently lost.** The checkpoint jumped to 3 even
though only line 1 was delivered. On restart, the tailer thinks all 3
lines have been processed.

## Root cause

`SendBatch` recorded `in_flight_offset = read_offset`. But `read_offset`
counts ALL lines read from the file, including lines still sitting in
`framer_buf` waiting to be batched. The checkpoint should only advance
to the offset of lines that have actually been dispatched (sent to output).

## The fix

```
in_flight_offset = read_offset - Len(framer_buf)
```

This accounts for the buffered lines that haven't been dispatched yet.
With the fix:

```
State 7: SendBatch → in_flight_offset = 3 - 2 = 1 ✓
State 8: AckBatch → checkpoints[source] = 1 ✓
State 13: Restart → read_offset = 1, TailerRead enabled (1 < 3) ✓
```

Lines 2 and 3 are re-read from the file after restart.

## Verification

After fix:
- Safety: 10,939 states, ALL INVARIANTS PASS
- Liveness: 288 states, ALL PROPERTIES PASS (Progress + EventualEmission)

## Impact on implementation

This confirms our earlier design discussion: `checkpoint = offset -
remainder.len()`. The TLA+ spec models this at the line level
(`read_offset - Len(framer_buf)`); the implementation will use byte
offsets (`read_offset - remainder_bytes`).

The `FramedInput` must report how many bytes are buffered (not yet
dispatched) per source. The checkpoint for a source is:

```
checkpoint = tailer.read_offset[source] - framedinput.remainder[source].len()
```

This is the processed_offset — the position of the last complete line
boundary that has been dispatched downstream.

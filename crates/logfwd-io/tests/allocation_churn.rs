//! Allocation churn tests for the input layer.
//!
//! Uses dhat to detect allocation/deallocation churn — where buffers are
//! allocated and freed repeatedly instead of being reused. A high ratio
//! of total_blocks to max_blocks indicates churn.
//!
//! Separate integration test binary because `#[global_allocator]` is per-binary.
//! Run with `--test-threads=1` since dhat allows only one profiler at a time.

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use logfwd_io::diagnostics::ComponentStats;
use logfwd_io::format::FormatProcessor;
use logfwd_io::framed::FramedInput;
use logfwd_io::input::{InputEvent, InputSource};
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;

/// Mock input source that yields pre-defined chunks.
struct MockSource {
    name: String,
    events: VecDeque<Vec<InputEvent>>,
}

impl MockSource {
    fn repeating(chunk: &[u8], count: usize) -> Self {
        let events: VecDeque<Vec<InputEvent>> = (0..count)
            .map(|_| {
                vec![InputEvent::Data {
                    bytes: chunk.to_vec(),
                }]
            })
            .collect();
        Self {
            name: "mock".to_string(),
            events,
        }
    }
}

impl InputSource for MockSource {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        Ok(self.events.pop_front().unwrap_or_default())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// FramedInput should reuse its internal buffers across polls.
/// After warmup, repeated polls of same-sized data should not cause
/// allocation growth (total_blocks should stay close to max_blocks).
#[test]
fn framed_input_no_buffer_churn() {
    let _prof = dhat::Profiler::builder().testing().build();

    const TOTAL_POLLS: usize = 50;
    const WARMUP_POLLS: usize = 5;

    let chunk = b"{\"msg\":\"hello world\",\"level\":\"info\"}\n".repeat(100);
    let stats = Arc::new(ComponentStats::new());
    let source = MockSource::repeating(&chunk, TOTAL_POLLS);
    let mut framed = FramedInput::new(
        Box::new(source),
        FormatProcessor::Passthrough,
        Arc::clone(&stats),
    );

    // Warmup: first few polls allocate buffers.
    for _ in 0..WARMUP_POLLS {
        let _ = framed.poll().unwrap();
    }

    // Snapshot after warmup.
    let warmup_stats = dhat::HeapStats::get();

    // Process remaining polls.
    loop {
        let events = framed.poll().unwrap();
        if events.is_empty() {
            break;
        }
    }

    let final_stats = dhat::HeapStats::get();
    let new_blocks = final_stats.total_blocks - warmup_stats.total_blocks;
    let peak_blocks = final_stats.max_blocks;

    // After warmup, new allocations per poll should be bounded.
    // Current expected allocations per poll:
    //   1. Vec<InputEvent> result vector
    //   2. InputEvent::Data bytes Vec (escapes to caller — tracked in #608)
    //   3. Occasional Vec growth in format processing
    // Allow up to 5 per poll to catch regressions beyond the known baseline.
    let polls_remaining = TOTAL_POLLS - WARMUP_POLLS;
    let max_acceptable = polls_remaining * 5;
    assert!(
        new_blocks < max_acceptable as u64,
        "allocation churn: {new_blocks} new blocks after warmup over {polls_remaining} polls \
         (peak was {peak_blocks} blocks). Expected <{max_acceptable}. \
         Buffers may not be reused across polls.",
    );
}

/// Verify no memory leaks across many poll cycles.
/// After all data is consumed and results dropped, net allocations should
/// return close to baseline.
#[test]
fn framed_input_no_leak_across_polls() {
    let _prof = dhat::Profiler::builder().testing().build();

    let chunk = b"{\"a\":1}\n{\"b\":2}\n".repeat(50);
    let stats = Arc::new(ComponentStats::new());
    let source = MockSource::repeating(&chunk, 100);
    let mut framed = FramedInput::new(
        Box::new(source),
        FormatProcessor::Passthrough,
        Arc::clone(&stats),
    );

    // Process all data, dropping results.
    loop {
        let events = framed.poll().unwrap();
        if events.is_empty() {
            break;
        }
        drop(events);
    }

    let final_stats = dhat::HeapStats::get();

    // curr_blocks = currently live allocations. Should be small
    // (just the FramedInput's internal buffers, not accumulated data).
    // The FramedInput struct itself has ~3 Vecs (out_buf, spare_buf, remainder).
    assert!(
        final_stats.curr_blocks < 20,
        "possible memory leak: {} blocks still alive after consuming all data \
         ({} bytes). Expected <20 live blocks.",
        final_stats.curr_blocks,
        final_stats.curr_bytes,
    );
}

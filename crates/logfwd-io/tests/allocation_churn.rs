//! Allocation churn tests for the input layer.
//!
//! Uses dhat to detect allocation/deallocation churn — where buffers are
//! allocated and freed repeatedly instead of being reused.
//!
//! Single test function because dhat only allows one active Profiler at a time.
//! Multiple #[test] functions would panic when run in parallel.

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

/// Combined churn + leak test (single function to avoid dhat parallel panic).
///
/// Part 1: After warmup, repeated polls should have bounded allocation count.
/// Part 2: After all data consumed, live allocations should be small.
#[test]
fn framed_input_allocation_behavior() {
    let _prof = dhat::Profiler::builder().testing().build();

    // --- Part 1: Buffer churn check ---
    {
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

        for _ in 0..WARMUP_POLLS {
            let _ = framed.poll().unwrap();
        }

        let warmup_stats = dhat::HeapStats::get();

        loop {
            let events = framed.poll().unwrap();
            if events.is_empty() {
                break;
            }
        }

        let final_stats = dhat::HeapStats::get();
        let new_blocks = final_stats.total_blocks - warmup_stats.total_blocks;
        let peak_blocks = final_stats.max_blocks;

        // Current expected: ~3 allocs/poll (Vec<InputEvent>, data Vec, occasional growth).
        // Allow up to 5 per poll to catch regressions.
        let polls_remaining = TOTAL_POLLS - WARMUP_POLLS;
        let max_acceptable = polls_remaining * 5;
        assert!(
            new_blocks < max_acceptable as u64,
            "allocation churn: {new_blocks} new blocks after warmup over {polls_remaining} polls \
             (peak was {peak_blocks} blocks). Expected <{max_acceptable}.",
        );
    }

    // --- Part 2: Leak check ---
    // Use a fresh FramedInput (the profiler accumulates across both parts).
    {
        let chunk = b"{\"a\":1}\n{\"b\":2}\n".repeat(50);
        let stats = Arc::new(ComponentStats::new());
        let source = MockSource::repeating(&chunk, 100);
        let mut framed = FramedInput::new(
            Box::new(source),
            FormatProcessor::Passthrough,
            Arc::clone(&stats),
        );

        loop {
            let events = framed.poll().unwrap();
            if events.is_empty() {
                break;
            }
            drop(events);
        }
        // Drop framed to release its internal buffers.
        drop(framed);

        let final_stats = dhat::HeapStats::get();

        // After dropping FramedInput, only the ComponentStats Arc and
        // dhat internals should remain. Allow generous headroom.
        assert!(
            final_stats.curr_blocks < 30,
            "possible memory leak: {} blocks still alive after dropping FramedInput \
             ({} bytes). Expected <30.",
            final_stats.curr_blocks,
            final_stats.curr_bytes,
        );
    }
}

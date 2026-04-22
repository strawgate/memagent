//! Allocation churn tests for the input layer.
//!
//! Tests use `#[serial]` because dhat only allows one active Profiler
//! at a time — parallel test execution would panic.

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use serial_test::serial;

use bytes::Bytes;
use logfwd_io::format::FormatDecoder;
use logfwd_io::framed::FramedInput;
use logfwd_io::input::{InputEvent, InputSource};
use logfwd_types::diagnostics::{ComponentHealth, ComponentStats};
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;

struct MockSource {
    name: String,
    events: VecDeque<Vec<InputEvent>>,
}

impl MockSource {
    fn repeating(chunk: &[u8], count: usize) -> Self {
        let events: VecDeque<Vec<InputEvent>> = (0..count)
            .map(|_| {
                vec![InputEvent::Data {
                    bytes: Bytes::from(chunk.to_vec()),
                    source_id: None,
                    accounted_bytes: chunk.len() as u64,
                    cri_metadata: None,
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

    fn health(&self) -> ComponentHealth {
        ComponentHealth::Healthy
    }
}

/// After warmup, repeated polls should have bounded allocation count.
#[test]
#[serial]
fn framed_input_no_buffer_churn() {
    let _prof = dhat::Profiler::builder().testing().build();

    const TOTAL_POLLS: usize = 50;
    const WARMUP_POLLS: usize = 5;

    let chunk = b"{\"msg\":\"hello world\",\"level\":\"info\"}\n".repeat(100);
    let stats = Arc::new(ComponentStats::new());
    let source = MockSource::repeating(&chunk, TOTAL_POLLS);
    let mut framed = FramedInput::new(
        Box::new(source),
        FormatDecoder::passthrough(Arc::clone(&stats)),
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

    let polls_remaining = TOTAL_POLLS - WARMUP_POLLS;
    let max_acceptable = polls_remaining * 5;
    assert!(
        new_blocks < max_acceptable as u64,
        "allocation churn: {new_blocks} new blocks after warmup over {polls_remaining} polls \
         (peak was {peak_blocks} blocks). Expected <{max_acceptable}.",
    );
}

/// After all data consumed and FramedInput dropped, live allocations should be small.
#[test]
#[serial]
fn framed_input_no_leak_across_polls() {
    let _prof = dhat::Profiler::builder().testing().build();

    let chunk = b"{\"a\":1}\n{\"b\":2}\n".repeat(50);
    let stats = Arc::new(ComponentStats::new());
    let source = MockSource::repeating(&chunk, 100);
    let mut framed = FramedInput::new(
        Box::new(source),
        FormatDecoder::passthrough(Arc::clone(&stats)),
        Arc::clone(&stats),
    );

    loop {
        let events = framed.poll().unwrap();
        if events.is_empty() {
            break;
        }
        drop(events);
    }
    drop(framed);
    drop(stats);
    drop(chunk);

    let final_stats = dhat::HeapStats::get();

    assert!(
        final_stats.curr_blocks < 30,
        "possible memory leak: {} blocks still alive after dropping FramedInput \
         ({} bytes). Expected <30.",
        final_stats.curr_blocks,
        final_stats.curr_bytes,
    );
}

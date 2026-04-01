//! Format processing for input data.
//!
//! A `FormatProcessor` transforms framed lines (complete, newline-delimited)
//! into scanner-ready output. This separates format concerns from transport
//! and framing, allowing any transport (file, TCP, UDP) to use any format
//! (JSON, CRI, Raw) via composition.

use crate::diagnostics::ComponentStats;
use logfwd_core::aggregator::{AggregateResult, CriAggregator};
use logfwd_core::cri::parse_cri_line;
use std::sync::Arc;

/// Processes framed input lines according to the configured format.
///
/// - `Passthrough`: lines are already scanner-ready (JSON, Raw)
/// - `Cri`: parse CRI container log format, extract message body
/// - `Auto`: try CRI, fall through to passthrough on parse failure
#[non_exhaustive]
pub enum FormatProcessor {
    Passthrough,
    Cri {
        aggregator: CriAggregator,
        stats: Arc<ComponentStats>,
    },
    Auto {
        aggregator: CriAggregator,
        stats: Arc<ComponentStats>,
    },
}

impl FormatProcessor {
    /// Create a CRI format processor with the given max message size.
    pub fn cri(max_message_size: usize, stats: Arc<ComponentStats>) -> Self {
        Self::Cri {
            aggregator: CriAggregator::new(max_message_size),
            stats,
        }
    }

    /// Create an Auto format processor (tries CRI, falls through to passthrough).
    pub fn auto(max_message_size: usize, stats: Arc<ComponentStats>) -> Self {
        Self::Auto {
            aggregator: CriAggregator::new(max_message_size),
            stats,
        }
    }

    /// Process a chunk of complete, newline-delimited lines into scanner-ready output.
    ///
    /// The input `chunk` must end at a line boundary (after `\n`). The caller
    /// handles remainder splitting — this function only sees complete lines.
    pub fn process_lines(&mut self, chunk: &[u8], out: &mut Vec<u8>) {
        match self {
            Self::Passthrough => {
                out.extend_from_slice(chunk);
            }
            Self::Cri { aggregator, stats } => {
                extract_cri_messages(chunk, out, aggregator, stats, false);
            }
            Self::Auto { aggregator, stats } => {
                extract_cri_messages(chunk, out, aggregator, stats, true);
            }
        }
    }

    /// Reset internal state (e.g. after file rotation or truncation).
    pub fn reset(&mut self) {
        match self {
            Self::Passthrough => {}
            Self::Cri { aggregator, .. } | Self::Auto { aggregator, .. } => {
                aggregator.reset();
            }
        }
    }
}

/// Extract JSON messages from CRI-formatted lines, handling P/F merging.
fn extract_cri_messages(
    input: &[u8],
    out: &mut Vec<u8>,
    aggregator: &mut CriAggregator,
    stats: &ComponentStats,
    passthrough_on_fail: bool,
) {
    let mut pos = 0;
    while pos < input.len() {
        let eol = memchr::memchr(b'\n', &input[pos..]).map_or(input.len(), |o| pos + o);
        let line = &input[pos..eol];
        if let Some(cri) = parse_cri_line(line) {
            match aggregator.feed(cri.message, cri.is_full) {
                AggregateResult::Complete(msg) => {
                    out.extend_from_slice(msg);
                    out.push(b'\n');
                    aggregator.reset();
                }
                AggregateResult::Pending => {}
            }
        } else {
            // Break any pending CRI aggregation at parse/fallback boundaries.
            aggregator.reset();
            if !line.is_empty() && passthrough_on_fail {
                out.extend_from_slice(line);
                out.push(b'\n');
            } else if !line.is_empty() {
                stats.inc_parse_errors(1);
            }
        }
        pos = eol + 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_stats() -> Arc<ComponentStats> {
        Arc::new(ComponentStats::new())
    }

    #[test]
    fn passthrough_copies_verbatim() {
        let mut proc = FormatProcessor::Passthrough;
        let input = b"line1\nline2\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, input);
    }

    #[test]
    fn cri_full_lines_extracted() {
        let stats = make_stats();
        let mut proc = FormatProcessor::cri(2 * 1024 * 1024, stats);
        let input = b"2024-01-15T10:30:00Z stdout F {\"msg\":\"hello\"}\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, b"{\"msg\":\"hello\"}\n");
    }

    #[test]
    fn cri_partial_then_full_merged() {
        let stats = make_stats();
        let mut proc = FormatProcessor::cri(2 * 1024 * 1024, stats);
        let mut out = Vec::new();

        // Partial line
        proc.process_lines(b"2024-01-15T10:30:00Z stdout P hello \n", &mut out);
        assert!(out.is_empty(), "partial should not emit");

        // Full line completes the message
        proc.process_lines(b"2024-01-15T10:30:00Z stdout F world\n", &mut out);
        assert_eq!(out, b"hello world\n");
    }

    #[test]
    fn cri_malformed_lines_count_errors() {
        let stats = make_stats();
        let mut proc = FormatProcessor::cri(2 * 1024 * 1024, Arc::clone(&stats));
        let input = b"not a cri line\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert!(out.is_empty());
        assert_eq!(
            stats
                .parse_errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn auto_passthrough_for_non_cri() {
        let stats = make_stats();
        let mut proc = FormatProcessor::auto(2 * 1024 * 1024, stats);
        let input = b"{\"msg\":\"plain json\"}\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, b"{\"msg\":\"plain json\"}\n");
    }

    #[test]
    fn auto_handles_cri_when_valid() {
        let stats = make_stats();
        let mut proc = FormatProcessor::auto(2 * 1024 * 1024, stats);
        let input = b"2024-01-15T10:30:00Z stdout F {\"msg\":\"cri\"}\n";
        let mut out = Vec::new();
        proc.process_lines(input, &mut out);
        assert_eq!(out, b"{\"msg\":\"cri\"}\n");
    }

    #[test]
    fn auto_malformed_line_resets_pending_state() {
        let stats = make_stats();
        let mut proc = FormatProcessor::auto(2 * 1024 * 1024, stats);
        let mut out = Vec::new();

        proc.process_lines(b"2024-01-15T10:30:00Z stdout P hello \n", &mut out);
        proc.process_lines(b"not a cri line\n", &mut out);
        proc.process_lines(b"2024-01-15T10:30:00Z stdout F world\n", &mut out);

        assert_eq!(out, b"not a cri line\nworld\n");
    }

    #[test]
    fn reset_clears_aggregator_state() {
        let stats = make_stats();
        let mut proc = FormatProcessor::cri(2 * 1024 * 1024, stats);
        let mut out = Vec::new();

        // Feed a partial
        proc.process_lines(b"2024-01-15T10:30:00Z stdout P hello \n", &mut out);
        assert!(out.is_empty());

        // Reset (simulating rotation)
        proc.reset();

        // Next full line should not contain the old partial
        proc.process_lines(b"2024-01-15T10:30:00Z stdout F world\n", &mut out);
        assert_eq!(out, b"world\n");
    }
}

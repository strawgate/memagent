//! Composable framing wrapper for input sources.
//!
//! `FramedInput` wraps any [`InputSource`] and handles newline framing
//! (remainder management across polls) and format processing (CRI, Auto,
//! passthrough). The pipeline receives scanner-ready bytes without knowing
//! about formats or line boundaries.
//!
//! Remainder buffers and format state are tracked per-source so that
//! interleaved data from multiple files (or TCP connections) never
//! cross-contaminates partial lines or CRI P/F aggregation state.

include!("framed_parts/part_1.rs");
include!("framed_parts/part_2.rs");
include!("framed_parts/part_3.rs");
include!("framed_parts/part_4.rs");

#[cfg(test)]
mod tests {
    include!("framed_parts/tests_1.rs");
    include!("framed_parts/tests_2.rs");
    include!("framed_parts/tests_3.rs");
}

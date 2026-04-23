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

include!("framed/state.rs");
include!("framed/input.rs");
include!("framed/processing.rs");
include!("framed/source.rs");

#[cfg(test)]
mod tests {
    include!("framed/tests/basic.rs");
    include!("framed/tests/framing.rs");
    include!("framed/tests/source_state.rs");
}

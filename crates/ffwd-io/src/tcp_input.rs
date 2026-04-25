//! TCP input source. Listens on a TCP socket and produces newline-delimited
//! log lines from connected clients. Multiple concurrent connections supported.
//!
//! Each accepted connection receives a unique `SourceId` derived from a
//! monotonic counter so that `FramedInput`'s per-source remainder tracking
//! can distinguish data from different peers.

#![allow(clippy::indexing_slicing)]

include!("tcp_input/transport.rs");
include!("tcp_input/options.rs");
include!("tcp_input/listener.rs");
include!("tcp_input/input_source.rs");

#[cfg(kani)]
mod verification {
    include!("tcp_input/verification.rs");
}

#[cfg(test)]
mod tests {
    include!("tcp_input/tests/tls.rs");
    include!("tcp_input/tests/basic.rs");
    include!("tcp_input/tests/bounds.rs");
}

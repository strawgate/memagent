//! TCP input source. Listens on a TCP socket and produces newline-delimited
//! log lines from connected clients. Multiple concurrent connections supported.
//!
//! Each accepted connection receives a unique `SourceId` derived from a
//! monotonic counter so that `FramedInput`'s per-source remainder tracking
//! can distinguish data from different peers.

include!("tcp_input_parts/part_1.rs");
include!("tcp_input_parts/part_2.rs");
include!("tcp_input_parts/part_3.rs");
include!("tcp_input_parts/part_4.rs");

#[cfg(test)]
mod tests {
    include!("tcp_input_parts/tests_1.rs");
    include!("tcp_input_parts/tests_2.rs");
    include!("tcp_input_parts/tests_3.rs");
}

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

#[cfg(kani)]
mod verification {
    use super::{
        MAX_BYTES_PER_CLIENT_PER_POLL, MAX_READS_PER_CLIENT_PER_POLL, should_stop_client_read,
    };

    #[kani::proof]
    fn verify_zero_counters_do_not_stop_client_read() {
        assert!(!should_stop_client_read(0, 0));
        kani::cover!(
            !should_stop_client_read(0, 0),
            "non-stopping path is reachable"
        );
    }

    #[kani::proof]
    fn verify_each_limit_independently_stops_client_read() {
        let reads_done = kani::any::<usize>();
        assert!(should_stop_client_read(
            MAX_BYTES_PER_CLIENT_PER_POLL,
            reads_done
        ));
        kani::cover!(
            should_stop_client_read(MAX_BYTES_PER_CLIENT_PER_POLL, 0),
            "byte-cap stop path is reachable"
        );

        let bytes_read = kani::any::<usize>();
        assert!(should_stop_client_read(
            bytes_read,
            MAX_READS_PER_CLIENT_PER_POLL
        ));
        kani::cover!(
            should_stop_client_read(0, MAX_READS_PER_CLIENT_PER_POLL),
            "read-cap stop path is reachable"
        );
    }

    #[kani::proof]
    fn verify_stop_predicate_equivalence() {
        let bytes_read = kani::any::<usize>();
        let reads_done = kani::any::<usize>();
        let expected = bytes_read >= MAX_BYTES_PER_CLIENT_PER_POLL
            || reads_done >= MAX_READS_PER_CLIENT_PER_POLL;
        assert_eq!(should_stop_client_read(bytes_read, reads_done), expected);
        kani::cover!(
            should_stop_client_read(MAX_BYTES_PER_CLIENT_PER_POLL, 0),
            "stop branch reachable"
        );
        kani::cover!(!should_stop_client_read(0, 0), "continue branch reachable");
    }
}

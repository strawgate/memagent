#![cfg(kani)]

use super::{
    MAX_BYTES_PER_CLIENT_PER_POLL, MAX_READS_PER_CLIENT_PER_POLL, should_stop_client_read,
};

#[kani::proof]
fn verify_zero_counters_do_not_stop_client_read() {
    assert!(!should_stop_client_read(0, 0));
    kani::cover!(
        !should_stop_client_read(0, 0),
        "zero counters allow another read"
    );
}

#[kani::proof]
fn verify_each_limit_independently_stops_client_read() {
    let reads_done = kani::any::<usize>();
    assert!(should_stop_client_read(
        MAX_BYTES_PER_CLIENT_PER_POLL,
        reads_done,
    ));

    let bytes_read = kani::any::<usize>();
    assert!(should_stop_client_read(
        bytes_read,
        MAX_READS_PER_CLIENT_PER_POLL,
    ));

    kani::cover!(
        should_stop_client_read(MAX_BYTES_PER_CLIENT_PER_POLL, 0),
        "byte cap alone stops reads"
    );
    kani::cover!(
        should_stop_client_read(0, MAX_READS_PER_CLIENT_PER_POLL),
        "read cap alone stops reads"
    );
}

#[kani::proof]
fn verify_stop_predicate_equivalence() {
    let bytes_read = kani::any::<usize>();
    let reads_done = kani::any::<usize>();

    let expected =
        bytes_read >= MAX_BYTES_PER_CLIENT_PER_POLL || reads_done >= MAX_READS_PER_CLIENT_PER_POLL;
    assert_eq!(should_stop_client_read(bytes_read, reads_done), expected);

    kani::cover!(
        should_stop_client_read(MAX_BYTES_PER_CLIENT_PER_POLL - 1, MAX_READS_PER_CLIENT_PER_POLL),
        "read cap can stop reads before byte cap"
    );
    kani::cover!(
        should_stop_client_read(MAX_BYTES_PER_CLIENT_PER_POLL, MAX_READS_PER_CLIENT_PER_POLL - 1),
        "byte cap can stop reads before read cap"
    );
}

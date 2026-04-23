//! Pure policy helpers for shared-buffer input handling.
//!
//! This isolates the buffered shutdown/flush decision surface from the async
//! I/O shell so it can be covered with Kani and proptest.

/// Continue shutdown repolling when the shared-buffer path might still emit
/// scanner-ready payload on a later poll.
#[must_use]
pub(super) const fn should_repoll_buffered_shutdown(
    is_finished: bool,
    had_source_payload: bool,
    emitted_any_events: bool,
    emitted_payload: bool,
) -> bool {
    if is_finished {
        return false;
    }
    if !emitted_any_events {
        return had_source_payload;
    }
    if had_source_payload && !emitted_payload {
        return true;
    }
    emitted_payload
}

/// Preserve the old "large first chunk flushes immediately" behavior on the
/// shared-buffer path.
#[must_use]
pub(super) const fn should_flush_large_single_shared_buffer_chunk(
    buffer_was_empty_at_poll: bool,
    event_count: usize,
    first_event_starts_at_zero: bool,
    first_event_len: usize,
) -> bool {
    const MIN_DIRECT_FLUSH_BYTES: usize = 64 * 1024;
    buffer_was_empty_at_poll
        && event_count == 1
        && first_event_starts_at_zero
        && first_event_len >= MIN_DIRECT_FLUSH_BYTES
}

#[cfg(test)]
mod tests {
    use super::{should_flush_large_single_shared_buffer_chunk, should_repoll_buffered_shutdown};
    use proptest::prelude::*;

    #[test]
    fn shutdown_repoll_stops_once_source_is_finished() {
        assert!(!should_repoll_buffered_shutdown(true, true, true, true));
        assert!(!should_repoll_buffered_shutdown(true, false, false, false));
    }

    #[test]
    fn shutdown_repoll_continues_after_source_payload_even_with_empty_output() {
        assert!(should_repoll_buffered_shutdown(false, true, false, false));
    }

    #[test]
    fn large_single_chunk_flush_requires_empty_start_and_threshold() {
        assert!(should_flush_large_single_shared_buffer_chunk(
            true,
            1,
            true,
            64 * 1024
        ));
        assert!(!should_flush_large_single_shared_buffer_chunk(
            false,
            1,
            true,
            64 * 1024
        ));
        assert!(!should_flush_large_single_shared_buffer_chunk(
            true,
            2,
            true,
            64 * 1024
        ));
        assert!(!should_flush_large_single_shared_buffer_chunk(
            true,
            1,
            false,
            64 * 1024
        ));
        assert!(!should_flush_large_single_shared_buffer_chunk(
            true,
            1,
            true,
            (64 * 1024) - 1
        ));
    }

    proptest! {
        #[test]
        fn shutdown_repoll_matches_reference_formula(
            is_finished in any::<bool>(),
            had_source_payload in any::<bool>(),
            emitted_any_events in any::<bool>(),
            emitted_payload in any::<bool>(),
        ) {
            let expected = if is_finished {
                false
            } else if !emitted_any_events {
                had_source_payload
            } else if had_source_payload && !emitted_payload {
                true
            } else {
                emitted_payload
            };

            prop_assert_eq!(
                should_repoll_buffered_shutdown(
                    is_finished,
                    had_source_payload,
                    emitted_any_events,
                    emitted_payload,
                ),
                expected
            );
        }

        #[test]
        fn large_single_chunk_flush_matches_reference_formula(
            buffer_was_empty_at_poll in any::<bool>(),
            event_count in 0usize..4,
            first_event_starts_at_zero in any::<bool>(),
            first_event_len in 0usize..(128 * 1024),
        ) {
            let expected = buffer_was_empty_at_poll
                && event_count == 1
                && first_event_starts_at_zero
                && first_event_len >= 64 * 1024;

            prop_assert_eq!(
                should_flush_large_single_shared_buffer_chunk(
                    buffer_was_empty_at_poll,
                    event_count,
                    first_event_starts_at_zero,
                    first_event_len,
                ),
                expected
            );
        }
    }
}

#[cfg(kani)]
mod verification {
    use super::{should_flush_large_single_shared_buffer_chunk, should_repoll_buffered_shutdown};

    #[kani::proof]
    fn verify_buffered_shutdown_repoll_matches_reference_formula() {
        let is_finished = kani::any::<bool>();
        let had_source_payload = kani::any::<bool>();
        let emitted_any_events = kani::any::<bool>();
        let emitted_payload = kani::any::<bool>();

        let expected = if is_finished {
            false
        } else if !emitted_any_events {
            had_source_payload
        } else if had_source_payload && !emitted_payload {
            true
        } else {
            emitted_payload
        };

        assert_eq!(
            should_repoll_buffered_shutdown(
                is_finished,
                had_source_payload,
                emitted_any_events,
                emitted_payload,
            ),
            expected
        );

        kani::cover!(
            should_repoll_buffered_shutdown(false, true, false, false),
            "empty buffered output after source payload is reachable"
        );
        kani::cover!(
            !should_repoll_buffered_shutdown(true, true, true, true),
            "finished buffered shutdown stops repolling"
        );
    }

    #[kani::proof]
    fn verify_large_single_chunk_flush_matches_reference_formula() {
        let buffer_was_empty_at_poll = kani::any::<bool>();
        let event_count = kani::any_where(|count: &usize| *count <= 2);
        let first_event_starts_at_zero = kani::any::<bool>();
        let first_event_len = kani::any_where(|len: &usize| *len <= 128 * 1024);

        let expected = buffer_was_empty_at_poll
            && event_count == 1
            && first_event_starts_at_zero
            && first_event_len >= 64 * 1024;

        assert_eq!(
            should_flush_large_single_shared_buffer_chunk(
                buffer_was_empty_at_poll,
                event_count,
                first_event_starts_at_zero,
                first_event_len,
            ),
            expected
        );

        kani::cover!(
            should_flush_large_single_shared_buffer_chunk(true, 1, true, 64 * 1024),
            "direct flush threshold path reachable"
        );
        kani::cover!(
            !should_flush_large_single_shared_buffer_chunk(true, 1, true, 1024),
            "below-threshold shared buffer stays buffered"
        );
    }
}

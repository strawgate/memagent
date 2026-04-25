    use super::*;

    // -----------------------------------------------------------------------
    // Pure dispatch_step tests (no async required)
    // -----------------------------------------------------------------------

    #[test]
    fn rejection_reason_bound_keeps_short_messages() {
        let reason = "bad request".to_string();
        assert_eq!(bound_rejection_reason(reason.clone()), reason);
    }

    #[test]
    fn rejection_reason_bound_truncates_long_messages() {
        let reason = "x".repeat(MAX_REJECTION_REASON_BYTES + 32);
        let bounded = bound_rejection_reason(reason);
        assert!(bounded.ends_with("..."));
        assert!(bounded.len() <= MAX_REJECTION_REASON_BYTES);
    }

    #[test]
    fn dispatch_empty_pool_spawns() {
        assert_eq!(dispatch_step(&[], 4), DispatchOutcome::SpawnNew);
    }

    #[test]
    fn dispatch_sends_to_first_available() {
        let states = [
            ChannelState::Full,
            ChannelState::HasSpace,
            ChannelState::HasSpace,
        ];
        assert_eq!(dispatch_step(&states, 4), DispatchOutcome::SentToIndex(1));
    }

    #[test]
    fn dispatch_skips_closed_workers() {
        let states = [
            ChannelState::Closed,
            ChannelState::Closed,
            ChannelState::HasSpace,
        ];
        assert_eq!(dispatch_step(&states, 4), DispatchOutcome::SentToIndex(2));
    }

    #[test]
    fn dispatch_all_closed_spawns_new() {
        let states = [ChannelState::Closed, ChannelState::Closed];
        // active = 0, max = 4 → spawn
        assert_eq!(dispatch_step(&states, 4), DispatchOutcome::SpawnNew);
    }

    #[test]
    fn dispatch_all_full_at_max_waits() {
        let states = [ChannelState::Full, ChannelState::Full];
        assert_eq!(dispatch_step(&states, 2), DispatchOutcome::WaitOnFront);
    }

    #[test]
    fn dispatch_all_full_under_max_spawns() {
        let states = [ChannelState::Full, ChannelState::Full];
        assert_eq!(dispatch_step(&states, 4), DispatchOutcome::SpawnNew);
    }

    #[test]
    fn dispatch_single_full_at_limit_waits() {
        assert_eq!(
            dispatch_step(&[ChannelState::Full], 1),
            DispatchOutcome::WaitOnFront
        );
    }

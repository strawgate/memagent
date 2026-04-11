use std::time::Duration;

use logfwd_io::checkpoint::CheckpointStore;

use super::internal_faults;

#[must_use]
const fn should_retry_flush(attempt: u32, max_attempts: u32) -> bool {
    attempt < max_attempts.saturating_sub(1)
}

#[must_use]
const fn is_retryable_flush_error(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::WriteZero
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::NotConnected
            | std::io::ErrorKind::BrokenPipe
    )
}

/// Flush checkpoint store with bounded retry (3 attempts, 100ms between).
///
/// The final shutdown flush is the last chance to persist checkpoint progress.
/// A single failure here loses all checkpoint advancement from the run.
/// Retry with brief sleeps to handle transient I/O errors (disk busy, NFS glitch).
///
/// Uses `tokio::time::sleep` for the retry delay so the async task yields
/// between attempts (Turmoil-compatible). Note: `store.flush()` itself is
/// synchronous I/O; only the inter-retry sleep is non-blocking.
pub(super) async fn flush_checkpoint_with_retry(store: &mut dyn CheckpointStore) {
    const MAX_ATTEMPTS: u32 = 3;
    const RETRY_DELAY: Duration = Duration::from_millis(100);

    for attempt in 0..MAX_ATTEMPTS {
        #[cfg(feature = "turmoil")]
        crate::turmoil_barriers::trigger(
            crate::turmoil_barriers::RuntimeBarrierEvent::BeforeCheckpointFlushAttempt { attempt },
        )
        .await;

        if internal_faults::checkpoint_flush_should_fail() {
            #[cfg(feature = "turmoil")]
            crate::turmoil_barriers::trigger(
                crate::turmoil_barriers::RuntimeBarrierEvent::CheckpointFlush { success: false },
            )
            .await;
            if should_retry_flush(attempt, MAX_ATTEMPTS) {
                tracing::warn!(
                    attempt,
                    "pipeline: checkpoint flush failpoint fired, retrying"
                );
                tokio::time::sleep(RETRY_DELAY).await;
            } else {
                tracing::error!(
                    attempts = MAX_ATTEMPTS,
                    "pipeline: checkpoint flush failpoint fired on final attempt"
                );
            }
            continue;
        }

        match store.flush() {
            Ok(()) => {
                #[cfg(feature = "turmoil")]
                crate::turmoil_barriers::trigger(
                    crate::turmoil_barriers::RuntimeBarrierEvent::CheckpointFlush { success: true },
                )
                .await;
                if attempt > 0 {
                    tracing::info!(attempt, "pipeline: checkpoint flush succeeded after retry");
                }
                return;
            }
            Err(e) => {
                #[cfg(feature = "turmoil")]
                crate::turmoil_barriers::trigger(
                    crate::turmoil_barriers::RuntimeBarrierEvent::CheckpointFlush {
                        success: false,
                    },
                )
                .await;
                let retryable = is_retryable_flush_error(e.kind());
                if retryable && should_retry_flush(attempt, MAX_ATTEMPTS) {
                    tracing::warn!(
                        attempt,
                        error = %e,
                        "pipeline: checkpoint flush failed, retrying"
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                } else {
                    tracing::error!(
                        attempts = MAX_ATTEMPTS,
                        retryable,
                        error = %e,
                        "pipeline: checkpoint flush failed after all retries — \
                         checkpoint progress from this run may be lost"
                    );
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use logfwd_io::checkpoint::{CheckpointStore, SourceCheckpoint};
    use proptest::prelude::*;

    use super::{flush_checkpoint_with_retry, is_retryable_flush_error, should_retry_flush};

    trait RetryFaultHook {
        fn fail_flush_attempt(&self, attempt: u32) -> bool;
    }

    struct NeverFailHook;

    impl RetryFaultHook for NeverFailHook {
        fn fail_flush_attempt(&self, _attempt: u32) -> bool {
            false
        }
    }

    struct FailFirstAttemptHook;

    impl RetryFaultHook for FailFirstAttemptHook {
        fn fail_flush_attempt(&self, attempt: u32) -> bool {
            attempt == 0
        }
    }

    async fn flush_checkpoint_with_retry_hooked(
        store: &mut dyn CheckpointStore,
        hook: &dyn RetryFaultHook,
    ) {
        const MAX_ATTEMPTS: u32 = 3;
        for attempt in 0..MAX_ATTEMPTS {
            if hook.fail_flush_attempt(attempt) {
                continue;
            }
            if store.flush().is_ok() {
                return;
            }
        }
    }

    struct SequenceCheckpointStore {
        outcomes: Vec<io::Result<()>>,
        calls: usize,
    }

    impl SequenceCheckpointStore {
        fn new(outcomes: Vec<io::Result<()>>) -> Self {
            Self { outcomes, calls: 0 }
        }
    }

    impl CheckpointStore for SequenceCheckpointStore {
        fn update(&mut self, _checkpoint: SourceCheckpoint) {}

        fn flush(&mut self) -> io::Result<()> {
            let idx = self.calls;
            self.calls += 1;
            match self.outcomes.get(idx) {
                Some(Ok(())) => Ok(()),
                Some(Err(err)) => Err(io::Error::new(err.kind(), err.to_string())),
                None => Err(io::Error::other("unexpected flush call")),
            }
        }

        fn load(&self, _source_id: u64) -> Option<SourceCheckpoint> {
            None
        }

        fn load_all(&self) -> Vec<SourceCheckpoint> {
            Vec::new()
        }
    }

    struct AlwaysFailCheckpointStore {
        calls: Arc<AtomicU32>,
    }

    impl AlwaysFailCheckpointStore {
        fn new() -> (Self, Arc<AtomicU32>) {
            let calls = Arc::new(AtomicU32::new(0));
            (
                Self {
                    calls: Arc::clone(&calls),
                },
                calls,
            )
        }
    }

    impl CheckpointStore for AlwaysFailCheckpointStore {
        fn update(&mut self, _checkpoint: SourceCheckpoint) {}

        fn flush(&mut self) -> io::Result<()> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Err(io::Error::new(io::ErrorKind::TimedOut, "flush failed"))
        }

        fn load(&self, _source_id: u64) -> Option<SourceCheckpoint> {
            None
        }

        fn load_all(&self) -> Vec<SourceCheckpoint> {
            Vec::new()
        }
    }

    #[test]
    fn retry_decision_matches_attempt_window() {
        assert!(should_retry_flush(0, 3));
        assert!(should_retry_flush(1, 3));
        assert!(!should_retry_flush(2, 3));
    }

    #[test]
    fn retry_decision_is_false_for_zero_max_attempts() {
        assert!(!should_retry_flush(0, 0));
    }

    #[test]
    fn retryable_error_kind_classification_matches_policy() {
        assert!(is_retryable_flush_error(io::ErrorKind::Interrupted));
        assert!(is_retryable_flush_error(io::ErrorKind::TimedOut));
        assert!(is_retryable_flush_error(io::ErrorKind::WriteZero));
        assert!(!is_retryable_flush_error(io::ErrorKind::InvalidInput));
        assert!(!is_retryable_flush_error(io::ErrorKind::PermissionDenied));
    }

    proptest! {
        #[test]
        fn retry_decision_equivalent_to_next_attempt_check(
            attempt in 0u32..32,
            max_attempts in 0u32..32,
        ) {
            let expected = max_attempts > 0 && attempt.saturating_add(1) < max_attempts;
            prop_assert_eq!(should_retry_flush(attempt, max_attempts), expected);
        }

        #[test]
        fn retryable_error_kind_only_for_transient_kinds(kind in any::<io::ErrorKind>()) {
            let expected = matches!(
                kind,
                io::ErrorKind::Interrupted
                    | io::ErrorKind::WouldBlock
                    | io::ErrorKind::TimedOut
                    | io::ErrorKind::WriteZero
                    | io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::ConnectionRefused
                    | io::ErrorKind::NotConnected
                    | io::ErrorKind::BrokenPipe
            );
            prop_assert_eq!(is_retryable_flush_error(kind), expected);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn flush_stops_after_first_success() {
        let mut store = SequenceCheckpointStore::new(vec![
            Err(io::Error::new(io::ErrorKind::TimedOut, "first failure")),
            Ok(()),
            Err(io::Error::other("must not be observed")),
        ]);

        flush_checkpoint_with_retry(&mut store).await;
        assert_eq!(store.calls, 2, "flush should stop after first success");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn flush_retries_up_to_max_attempts() {
        let (mut store, calls) = AlwaysFailCheckpointStore::new();

        flush_checkpoint_with_retry(&mut store).await;
        assert_eq!(
            calls.load(Ordering::Relaxed),
            3,
            "flush should execute exactly three attempts"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn trait_hook_prototype_can_inject_first_attempt_failure() {
        let mut store = SequenceCheckpointStore::new(vec![Ok(()), Ok(())]);
        let hook = FailFirstAttemptHook;

        flush_checkpoint_with_retry_hooked(&mut store, &hook).await;
        assert_eq!(store.calls, 1, "first real store.flush call should succeed");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn trait_hook_prototype_noop_matches_normal_path() {
        let mut store = SequenceCheckpointStore::new(vec![Ok(())]);
        let hook = NeverFailHook;

        flush_checkpoint_with_retry_hooked(&mut store, &hook).await;
        assert_eq!(store.calls, 1, "noop hook must preserve baseline behavior");
    }

    #[cfg(feature = "internal-failpoints")]
    #[tokio::test(flavor = "current_thread")]
    #[serial_test::serial]
    async fn failpoint_checkpoint_flush_retries_then_succeeds() {
        let scenario = fail::FailScenario::setup();
        fail::cfg(
            "runtime::pipeline::checkpoint_flush::before_flush",
            "1*return->off",
        )
        .expect("configure failpoint");

        let mut store = SequenceCheckpointStore::new(vec![Ok(()), Ok(())]);
        flush_checkpoint_with_retry(&mut store).await;
        assert_eq!(store.calls, 1, "one injected failure + one real success");

        fail::remove("runtime::pipeline::checkpoint_flush::before_flush");
        scenario.teardown();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn flush_does_not_retry_non_retryable_failures() {
        let mut store = SequenceCheckpointStore::new(vec![
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "permission denied",
            )),
            Ok(()),
        ]);

        flush_checkpoint_with_retry(&mut store).await;
        assert_eq!(
            store.calls, 1,
            "flush should stop immediately for non-retryable failures"
        );
    }
}

#[cfg(kani)]
mod verification {
    use super::{is_retryable_flush_error, should_retry_flush};

    #[kani::proof]
    fn verify_zero_or_one_attempt_never_retries() {
        let attempt = kani::any::<u32>();
        assert!(!should_retry_flush(attempt, 0));
        assert!(!should_retry_flush(attempt, 1));
        kani::cover!(
            !should_retry_flush(0, 0),
            "zero-attempt no-retry path reachable"
        );
        kani::cover!(
            !should_retry_flush(0, 1),
            "one-attempt no-retry path reachable"
        );
    }

    #[kani::proof]
    fn verify_retry_window_equivalence() {
        let attempt = kani::any::<u32>();
        let max_attempts = kani::any::<u32>();
        let expected = max_attempts > 0 && attempt.saturating_add(1) < max_attempts;
        assert_eq!(should_retry_flush(attempt, max_attempts), expected);
        kani::cover!(should_retry_flush(0, 2), "retry path reachable");
        kani::cover!(!should_retry_flush(1, 2), "terminal-attempt path reachable");
    }

    #[kani::proof]
    fn verify_retryable_flush_error_classification() {
        let kind = kani::any::<std::io::ErrorKind>();
        let expected = matches!(
            kind,
            std::io::ErrorKind::Interrupted
                | std::io::ErrorKind::WouldBlock
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::WriteZero
                | std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::NotConnected
                | std::io::ErrorKind::BrokenPipe
        );
        assert_eq!(is_retryable_flush_error(kind), expected);
    }
}

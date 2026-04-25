#[cfg_attr(not(kani), allow(unused_imports))]
use std::io::ErrorKind;
use std::time::Duration;

use ffwd_io::checkpoint::CheckpointStore;

use super::internal_faults;

#[must_use]
const fn should_retry_flush(attempt: u32, max_attempts: u32) -> bool {
    attempt < max_attempts.saturating_sub(1)
}

/// Returns `true` if the given I/O error kind is transient and worth retrying.
#[must_use]
#[cfg_attr(not(kani), allow(dead_code))]
const fn is_retryable_flush_error(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::Interrupted
            | ErrorKind::WouldBlock
            | ErrorKind::TimedOut
            | ErrorKind::WriteZero
            | ErrorKind::UnexpectedEof
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::ConnectionRefused
            | ErrorKind::NotConnected
            | ErrorKind::BrokenPipe
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
                if should_retry_flush(attempt, MAX_ATTEMPTS) {
                    tracing::warn!(
                        attempt,
                        error = %e,
                        "pipeline: checkpoint flush failed, retrying"
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                } else {
                    tracing::error!(
                        attempts = MAX_ATTEMPTS,
                        error = %e,
                        "pipeline: checkpoint flush failed after all retries — \
                         checkpoint progress from this run may be lost"
                    );
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

    use ffwd_io::checkpoint::{CheckpointStore, SourceCheckpoint};
    use proptest::prelude::*;

    use super::{flush_checkpoint_with_retry, should_retry_flush};

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
            Err(io::Error::other("flush failed"))
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

    proptest! {
        #[test]
        fn retry_decision_equivalent_to_next_attempt_check(
            attempt in 0u32..32,
            max_attempts in 0u32..32,
        ) {
            let expected = max_attempts > 0 && attempt.saturating_add(1) < max_attempts;
            prop_assert_eq!(should_retry_flush(attempt, max_attempts), expected);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn flush_stops_after_first_success() {
        let mut store = SequenceCheckpointStore::new(vec![
            Err(io::Error::other("first failure")),
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
}

#[cfg(kani)]
mod verification {
    use super::{is_retryable_flush_error, should_retry_flush};
    use std::io::ErrorKind;

    fn any_error_kind() -> ErrorKind {
        match kani::any::<u8>() {
            0 => ErrorKind::Interrupted,
            1 => ErrorKind::WouldBlock,
            2 => ErrorKind::TimedOut,
            3 => ErrorKind::WriteZero,
            4 => ErrorKind::UnexpectedEof,
            5 => ErrorKind::ConnectionReset,
            6 => ErrorKind::ConnectionAborted,
            7 => ErrorKind::ConnectionRefused,
            8 => ErrorKind::NotConnected,
            9 => ErrorKind::BrokenPipe,
            10 => ErrorKind::PermissionDenied,
            11 => ErrorKind::NotFound,
            12 => ErrorKind::AlreadyExists,
            13 => ErrorKind::InvalidInput,
            14 => ErrorKind::InvalidData,
            _ => ErrorKind::Other,
        }
    }

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
        let kind = any_error_kind();
        let expected = matches!(
            kind,
            ErrorKind::Interrupted
                | ErrorKind::WouldBlock
                | ErrorKind::TimedOut
                | ErrorKind::WriteZero
                | ErrorKind::UnexpectedEof
                | ErrorKind::ConnectionReset
                | ErrorKind::ConnectionAborted
                | ErrorKind::ConnectionRefused
                | ErrorKind::NotConnected
                | ErrorKind::BrokenPipe
        );
        assert_eq!(is_retryable_flush_error(kind), expected);
        kani::cover!(
            matches!(kind, ErrorKind::TimedOut),
            "retryable flush error covered"
        );
        kani::cover!(
            matches!(kind, ErrorKind::PermissionDenied),
            "non-retryable flush error covered"
        );
    }
}

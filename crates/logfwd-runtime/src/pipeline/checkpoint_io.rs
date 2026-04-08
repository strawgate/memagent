use std::time::Duration;

use logfwd_io::checkpoint::CheckpointStore;

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
        match store.flush() {
            Ok(()) => {
                if attempt > 0 {
                    tracing::info!(attempt, "pipeline: checkpoint flush succeeded after retry");
                }
                return;
            }
            Err(e) => {
                if attempt + 1 < MAX_ATTEMPTS {
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

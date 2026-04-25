use std::thread::JoinHandle;
use tokio::sync::oneshot;

/// Owns the diagnostics HTTP background thread and its shutdown trigger.
///
/// Dropping the handle signals shutdown via the oneshot channel.
/// Call `shutdown_and_join()` when teardown needs to wait for the worker
/// thread to exit before proceeding.
pub(crate) struct BackgroundHttpTask {
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl BackgroundHttpTask {
    /// Wrap a spawned diagnostics HTTP worker and its shutdown channel.
    pub(crate) fn new(shutdown_tx: oneshot::Sender<()>, handle: JoinHandle<()>) -> Self {
        Self {
            shutdown_tx: Some(shutdown_tx),
            handle: Some(handle),
        }
    }

    /// Signal shutdown and block until the HTTP worker exits.
    ///
    /// This is idempotent: repeated calls after the first are no-ops.
    pub(crate) fn shutdown_and_join(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take()
            && let Err(_panic) = handle.join()
        {
            tracing::error!("diagnostics HTTP worker panicked during shutdown");
        }
    }
}

impl Drop for BackgroundHttpTask {
    fn drop(&mut self) {
        // Signal shutdown only. Dropping the JoinHandle detaches the thread;
        // to wait for exit, call `shutdown_and_join()` instead.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

use std::sync::Arc;
use std::thread::JoinHandle;

/// Owns the diagnostics HTTP background thread and its shutdown trigger.
///
/// Dropping the handle only signals shutdown. Call `shutdown_and_join()` when
/// teardown needs to wait for the worker thread to exit before proceeding.
pub(crate) struct BackgroundHttpTask {
    shutdown: Option<ShutdownHandle>,
    handle: Option<JoinHandle<()>>,
}

enum ShutdownHandle {
    TinyHttp(Arc<tiny_http::Server>),
}

impl BackgroundHttpTask {
    /// Wrap a spawned diagnostics HTTP worker and its server handle.
    pub(crate) fn new(server: Arc<tiny_http::Server>, handle: JoinHandle<()>) -> Self {
        Self {
            shutdown: Some(ShutdownHandle::TinyHttp(server)),
            handle: Some(handle),
        }
    }

    fn signal_shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            match shutdown {
                ShutdownHandle::TinyHttp(server) => server.unblock(),
            }
        }
    }

    /// Signal shutdown and block until the HTTP worker exits.
    ///
    /// This is idempotent: repeated calls after the first are no-ops.
    pub(crate) fn shutdown_and_join(&mut self) {
        self.signal_shutdown();
        if let Some(handle) = self.handle.take()
            && let Err(_panic) = handle.join()
        {
            tracing::error!("diagnostics HTTP worker panicked during shutdown");
        }
    }
}

impl Drop for BackgroundHttpTask {
    fn drop(&mut self) {
        self.signal_shutdown();
    }
}

use std::thread::JoinHandle;

use tokio::sync::oneshot;

/// Owns an axum HTTP server and its background worker thread.
///
/// Drop signals shutdown and joins the thread to prevent leaked listeners
/// and leaked thread ownership.
pub(crate) struct BackgroundHttpTask {
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl BackgroundHttpTask {
    pub(crate) fn new_axum(shutdown: oneshot::Sender<()>, handle: JoinHandle<()>) -> Self {
        Self {
            shutdown: Some(shutdown),
            handle: Some(handle),
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.handle.as_ref().is_some_and(JoinHandle::is_finished)
    }
}

impl Drop for BackgroundHttpTask {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

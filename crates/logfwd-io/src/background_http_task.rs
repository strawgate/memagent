use std::sync::Arc;
use std::thread::JoinHandle;

/// Owns a tiny_http server and its background worker thread.
///
/// Drop unblocks the server and joins the thread to prevent leaked listeners
/// and leaked thread ownership.
pub(crate) struct BackgroundHttpTask {
    server: Arc<tiny_http::Server>,
    handle: Option<JoinHandle<()>>,
}

impl BackgroundHttpTask {
    pub(crate) fn new(server: Arc<tiny_http::Server>, handle: JoinHandle<()>) -> Self {
        Self {
            server,
            handle: Some(handle),
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.handle.as_ref().is_some_and(JoinHandle::is_finished)
    }
}

impl Drop for BackgroundHttpTask {
    fn drop(&mut self) {
        self.server.unblock();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

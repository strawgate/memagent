use std::sync::Arc;
use std::thread::JoinHandle;

pub(crate) struct BackgroundHttpTask {
    shutdown: Option<ShutdownHandle>,
    handle: Option<JoinHandle<()>>,
}

enum ShutdownHandle {
    TinyHttp(Arc<tiny_http::Server>),
}

impl BackgroundHttpTask {
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

    pub(crate) fn shutdown_and_join(&mut self) {
        self.signal_shutdown();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for BackgroundHttpTask {
    fn drop(&mut self) {
        self.signal_shutdown();
    }
}

//! Controlled TCP server for turmoil network simulation tests.
//!
//! The server accepts NDJSON over TCP and counts received lines and connections.
//! It runs inside a `sim.host()` closure and is designed to be crashed/bounced
//! by the turmoil framework.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::AsyncReadExt;

/// Handle for inspecting server state from outside the turmoil host.
#[derive(Clone)]
pub struct TcpServerHandle {
    pub received_lines: Arc<AtomicU64>,
    pub connection_count: Arc<AtomicU64>,
}

impl TcpServerHandle {
    pub fn new() -> Self {
        Self {
            received_lines: Arc::new(AtomicU64::new(0)),
            connection_count: Arc::new(AtomicU64::new(0)),
        }
    }
}

/// Run a TCP server that accepts NDJSON lines.
///
/// Call this inside a `sim.host()` closure. The server loops forever,
/// accepting connections and counting newline-delimited lines.
pub async fn run_tcp_server(
    port: u16,
    handle: TcpServerHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = turmoil::net::TcpListener::bind(("0.0.0.0", port)).await?;
    loop {
        let (mut stream, _addr) = listener.accept().await?;
        handle.connection_count.fetch_add(1, Ordering::Relaxed);
        let line_counter = handle.received_lines.clone();
        // Spawn a task per connection so we can accept multiple concurrently.
        tokio::spawn(async move {
            let mut buf = vec![0u8; 8192];
            loop {
                match stream.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        let lines = buf[..n].iter().filter(|&&b| b == b'\n').count();
                        line_counter.fetch_add(lines as u64, Ordering::Relaxed);
                    }
                    Err(_) => break,
                }
            }
        });
    }
}

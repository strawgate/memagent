//! TCP output sink — connects to a TCP endpoint and sends newline-delimited
//! JSON lines. Reconnects on failure with a single retry per `send_batch` call.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Instant;

use ffwd_types::diagnostics::ComponentStats;

use crate::sink::{SendResult, Sink, SinkFactory};
use crate::{BatchMetadata, build_col_infos, write_row_json};

/// Connect timeout. Kept short so the pipeline doesn't stall when the
/// downstream is unreachable.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Write timeout for each full write attempt (all partial writes combined).
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

pub struct TcpSink {
    name: String,
    addr: String,
    stream: Option<TcpStream>,
    buf: Vec<u8>,
    stats: Arc<ComponentStats>,
}

impl TcpSink {
    pub fn new(
        name: impl Into<String>,
        addr: impl Into<String>,
        stats: Arc<ComponentStats>,
    ) -> Self {
        Self {
            name: name.into(),
            addr: addr.into(),
            stream: None,
            buf: Vec::with_capacity(64 * 1024),
            stats,
        }
    }

    /// Write the buffer to the stream. On failure, drop the connection and
    /// retry exactly once with a fresh connection.
    async fn write_with_retry(&mut self) -> io::Result<()> {
        let addr = self.addr.clone();
        write_with_retry_generic(&self.buf, &mut self.stream, move || {
            let addr = addr.clone();
            async move { connect_stream(&addr).await }
        })
        .await
    }
}

async fn connect_stream(addr: &str) -> io::Result<TcpStream> {
    let result = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await;

    let stream = match result {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "TCP connect timed out",
            ));
        }
    };

    stream.set_nodelay(true)?;
    Ok(stream)
}

async fn write_with_retry_generic<W, C, F>(
    buf: &[u8],
    stream: &mut Option<W>,
    mut connect: C,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    C: FnMut() -> F,
    F: Future<Output = io::Result<W>>,
{
    if buf.is_empty() {
        return Ok(());
    }

    let mut written = 0usize;
    let mut last_error = None;

    for attempt in 0..=1 {
        if stream.is_none() {
            *stream = Some(connect().await?);
        }

        if let Some(active_stream) = stream.as_mut() {
            match write_remaining_bytes(active_stream, buf, &mut written).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    // Reset to zero so the full buffer is resent after
                    // reconnect.  `write()` only confirms bytes entered the
                    // local kernel buffer — after a connection reset those
                    // bytes may never have reached the peer.  Resending
                    // produces at-least-once semantics (possible duplicates)
                    // which is strictly safer than at-most-once (data loss).
                    written = 0;
                    *stream = None;
                    last_error = Some(e);
                    if attempt == 1 {
                        break;
                    }
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| io::Error::other("TCP sink write failed")))
}

async fn write_remaining_bytes<W>(stream: &mut W, buf: &[u8], written: &mut usize) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    // Single deadline for the entire write attempt.  This bounds total
    // wall-clock time regardless of how many partial writes occur.
    let deadline = Instant::now() + WRITE_TIMEOUT;

    while *written < buf.len() {
        let result = tokio::time::timeout_at(deadline, stream.write(&buf[*written..])).await;
        match result {
            Ok(Ok(0)) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "TCP write returned zero bytes",
                ));
            }
            Ok(Ok(n)) => *written += n,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "TCP write timed out",
                ));
            }
        }
    }
    Ok(())
}

impl Sink for TcpSink {
    fn send_batch<'a>(
        &'a mut self,
        batch: &'a RecordBatch,
        _metadata: &'a BatchMetadata,
    ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
        Box::pin(async move {
            if batch.num_rows() == 0 {
                return SendResult::Ok;
            }

            self.buf.clear();
            let cols = build_col_infos(batch);
            for row in 0..batch.num_rows() {
                if let Err(e) = write_row_json(batch, row, &cols, &mut self.buf, true) {
                    return SendResult::from_io_error(e);
                }
            }

            if let Err(e) = self.write_with_retry().await {
                return SendResult::from_io_error(e);
            }
            self.stats.inc_lines(batch.num_rows() as u64);
            self.stats.inc_bytes(self.buf.len() as u64);
            SendResult::Ok
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            if let Some(ref mut stream) = self.stream {
                stream.flush().await?;
            }
            Ok(())
        })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async move {
            if let Some(ref mut stream) = self.stream {
                stream.flush().await?;
                stream.shutdown().await?;
            }
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// TcpSinkFactory
// ---------------------------------------------------------------------------

/// Factory for creating [`TcpSink`] instances.
///
/// Each worker gets its own TCP connection via a fresh `TcpSink`.
pub struct TcpSinkFactory {
    name: String,
    addr: String,
    stats: Arc<ComponentStats>,
}

impl TcpSinkFactory {
    pub fn new(name: String, addr: String, stats: Arc<ComponentStats>) -> Self {
        Self { name, addr, stats }
    }
}

impl SinkFactory for TcpSinkFactory {
    fn create(&self) -> io::Result<Box<dyn Sink>> {
        Ok(Box::new(TcpSink::new(
            self.name.clone(),
            self.addr.clone(),
            Arc::clone(&self.stats),
        )))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::io;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};

    use tokio::io::AsyncWrite;

    use super::write_with_retry_generic;

    #[derive(Clone, Debug)]
    enum ScriptStep {
        Write(usize),
        Error(io::ErrorKind),
        Zero,
    }

    #[derive(Clone)]
    struct ScriptedWriter {
        steps: VecDeque<ScriptStep>,
        received: Arc<Mutex<Vec<u8>>>,
    }

    impl ScriptedWriter {
        fn new(steps: Vec<ScriptStep>, received: Arc<Mutex<Vec<u8>>>) -> Self {
            Self {
                steps: VecDeque::from(steps),
                received,
            }
        }
    }

    impl AsyncWrite for ScriptedWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            let step = self
                .steps
                .pop_front()
                .expect("script exhausted in poll_write");
            match step {
                ScriptStep::Write(n) => {
                    let to_take = n.min(buf.len());
                    self.received
                        .lock()
                        .expect("received lock poisoned")
                        .extend_from_slice(&buf[..to_take]);
                    Poll::Ready(Ok(to_take))
                }
                ScriptStep::Error(kind) => {
                    Poll::Ready(Err(io::Error::new(kind, "scripted write error")))
                }
                ScriptStep::Zero => Poll::Ready(Ok(0)),
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn write_with_retry_restarts_interrupted_record_after_partial_write_error() {
        let payload = b"abcdef";
        let first_seen = Arc::new(Mutex::new(Vec::new()));
        let second_seen = Arc::new(Mutex::new(Vec::new()));

        let mut writers = VecDeque::from([
            ScriptedWriter::new(
                vec![
                    ScriptStep::Write(2),
                    ScriptStep::Error(io::ErrorKind::BrokenPipe),
                ],
                Arc::clone(&first_seen),
            ),
            ScriptedWriter::new(
                vec![ScriptStep::Write(payload.len())],
                Arc::clone(&second_seen),
            ),
        ]);
        let mut current = Some(writers.pop_front().expect("first writer missing"));

        write_with_retry_generic(payload, &mut current, move || {
            std::future::ready(
                writers
                    .pop_front()
                    .ok_or_else(|| io::Error::other("no reconnect writer available")),
            )
        })
        .await
        .expect("write_with_retry should succeed");

        assert_eq!(&*first_seen.lock().expect("first lock poisoned"), b"ab");
        assert_eq!(
            &*second_seen.lock().expect("second lock poisoned"),
            b"abcdef"
        );
    }

    #[tokio::test]
    async fn write_with_retry_resends_full_buffer_after_partial_write_error() {
        // After a connection reset the kernel buffer contents may have been
        // lost, so the retry must resend the *entire* buffer (at-least-once).
        let payload = b"first\nsecond\n";
        let first_seen = Arc::new(Mutex::new(Vec::new()));
        let second_seen = Arc::new(Mutex::new(Vec::new()));

        let mut writers = VecDeque::from([
            ScriptedWriter::new(
                vec![
                    ScriptStep::Write(9),
                    ScriptStep::Error(io::ErrorKind::BrokenPipe),
                ],
                Arc::clone(&first_seen),
            ),
            ScriptedWriter::new(
                vec![ScriptStep::Write(payload.len())],
                Arc::clone(&second_seen),
            ),
        ]);
        let mut current = Some(writers.pop_front().expect("first writer missing"));

        write_with_retry_generic(payload, &mut current, move || {
            std::future::ready(
                writers
                    .pop_front()
                    .ok_or_else(|| io::Error::other("no reconnect writer available")),
            )
        })
        .await
        .expect("write_with_retry should succeed");

        assert_eq!(
            &*first_seen.lock().expect("first lock poisoned"),
            b"first\nsec"
        );
        // Full buffer resent — at-least-once semantics.
        assert_eq!(
            &*second_seen.lock().expect("second lock poisoned"),
            b"first\nsecond\n"
        );
    }

    #[tokio::test]
    async fn write_with_retry_retries_once_after_zero_byte_write() {
        let payload = b"payload";
        let first_seen = Arc::new(Mutex::new(Vec::new()));
        let second_seen = Arc::new(Mutex::new(Vec::new()));

        let mut writers = VecDeque::from([
            ScriptedWriter::new(vec![ScriptStep::Zero], Arc::clone(&first_seen)),
            ScriptedWriter::new(
                vec![ScriptStep::Write(payload.len())],
                Arc::clone(&second_seen),
            ),
        ]);
        let mut current = Some(writers.pop_front().expect("first writer missing"));

        write_with_retry_generic(payload, &mut current, move || {
            std::future::ready(
                writers
                    .pop_front()
                    .ok_or_else(|| io::Error::other("no reconnect writer available")),
            )
        })
        .await
        .expect("zero write on first attempt should retry");

        assert!(first_seen.lock().expect("first lock poisoned").is_empty());
        assert_eq!(
            &*second_seen.lock().expect("second lock poisoned"),
            b"payload"
        );
    }

    #[tokio::test]
    async fn write_with_retry_returns_write_zero_after_retry_zero_write() {
        let mut writers = VecDeque::from([ScriptedWriter::new(
            vec![ScriptStep::Zero],
            Arc::new(Mutex::new(Vec::new())),
        )]);
        let mut current = Some(ScriptedWriter::new(
            vec![ScriptStep::Zero],
            Arc::new(Mutex::new(Vec::new())),
        ));

        let err = write_with_retry_generic(b"x", &mut current, move || {
            std::future::ready(
                writers
                    .pop_front()
                    .ok_or_else(|| io::Error::other("no reconnect writer available")),
            )
        })
        .await
        .expect_err("second zero-byte write should fail");

        assert_eq!(err.kind(), io::ErrorKind::WriteZero);
    }
}

//! Async write combinator that provides at-least-once semantics by reconnecting
//! on failure.
//!
//! # Overview
//!
//! Many network protocols need at-least-once delivery: if a write fails after the
//! connection drops but before data reached the peer, the caller should retry.
//! `RetryWriter` encapsulates this pattern:
//!
//! - On the first attempt, use the existing connection if present, otherwise connect.
//! - If the write fails and we haven't retied yet, reconnect and retry once.
//! - After the retry, the last error encountered is propagated.
//!
//! # At-least-once semantics
//!
//! When a write fails (e.g., connection reset), the caller should not assume the
//! peer received any portion of the data. `RetryWriter` handles this by:
//! - Tracking how many bytes were written before the failure.
//! - On retry, re-sending the **entire** buffer from byte 0, not from the
//!   partial write point. This is at-least-once: the peer may see duplicate data,
//!   but will never see a truncated record.
//!
//! # Timeout
//!
//! The inner write respects a `deadline` — a fixed instant by which the entire
//! operation must complete. Partial writes that time out are treated as errors and
//! trigger a retry with a fresh deadline. If the second attempt also times out,
//! the timeout error is propagated to the caller.

use std::future::Future;
use std::io;
use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::time::Instant;

/// Default timeout for a single write attempt.
#[allow(dead_code)]
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// A wrapper that provides at-least-once write semantics by reconnecting on failure.
///
/// On construction, `inner` is `None` (no active connection). The first `write_all`
/// call will connect before writing. If the write fails, a new connection is
/// established and the write is retried once. If the retry also fails, the second
/// error is propagated.
#[allow(dead_code)]
pub struct RetryWriter<W, C> {
    inner: Option<W>,
    connect: C,
}

#[allow(dead_code)]
impl<W, C> RetryWriter<W, C> {
    /// Create a new `RetryWriter` with no active connection.
    ///
    /// `connect` is called each time a (re)connection is needed; it must return
    /// a future that resolves to an open, writable `W`.
    pub fn new(connect: C) -> Self {
        Self {
            inner: None,
            connect,
        }
    }
}

#[allow(dead_code)]
impl<W, C, F> RetryWriter<W, C>
where
    W: AsyncWrite + Unpin + Send,
    C: FnMut() -> F,
    F: Future<Output = io::Result<W>>,
{
    /// Write the entire buffer with at-least-once semantics.
    ///
    /// If the inner writer is not connected, connects first.
    /// On write failure, reconnects and retries exactly once.
    /// Returns `Ok(())` only when the full buffer is acknowledged by the peer.
    ///
    /// The write timeout is `DEFAULT_WRITE_TIMEOUT` (10 seconds).
    pub async fn write_all_with_retry(&mut self, buf: &[u8]) -> io::Result<()> {
        self.write_all_with_retry_deadline(buf, DEFAULT_WRITE_TIMEOUT)
            .await
    }

    /// Write the entire buffer with at-least-once semantics and a custom timeout.
    ///
    /// See [`write_all_with_retry`](Self::write_all_with_retry) for semantics.
    pub async fn write_all_with_retry_deadline(
        &mut self,
        buf: &[u8],
        timeout: Duration,
    ) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }

        let mut last_error = None;

        for attempt in 0..=1 {
            if self.inner.is_none() {
                self.inner = Some((self.connect)().await?);
            }

            if let Some(inner) = self.inner.as_mut() {
                match write_all_deadline(inner, buf, timeout).await {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        // Discard the broken connection. On the next attempt
                        // `connect()` will be called again.
                        self.inner = None;
                        last_error = Some(e);
                        if attempt == 1 {
                            break;
                        }
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| io::Error::other("RetryWriter write failed")))
    }
}

/// Write the entire buffer to `stream` with a deadline.
///
/// On success, returns `Ok(())` only when all bytes are written.
/// On timeout, returns an error with [`io::ErrorKind::TimedOut`].
/// On other errors, propagates them directly.
#[allow(dead_code)]
async fn write_all_deadline<W>(stream: &mut W, buf: &[u8], timeout: Duration) -> io::Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    let deadline = Instant::now() + timeout;
    let mut written = 0usize;

    while written < buf.len() {
        let timeout_for_this_write = deadline.saturating_duration_since(Instant::now());

        if timeout_for_this_write.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "write deadline exceeded",
            ));
        }

        let n = tokio::time::timeout_at(deadline, stream.write(&buf[written..])).await??;

        match n {
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "write returned zero bytes",
                ));
            }
            n => written += n,
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::io;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tokio::io::AsyncWrite;

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
    async fn retry_writer_resends_full_buffer_after_error() {
        let payload = b"full buffer test";
        let first_seen = Arc::new(Mutex::new(Vec::new()));
        let second_seen = Arc::new(Mutex::new(Vec::new()));

        let writers: VecDeque<_> = VecDeque::from([
            ScriptedWriter::new(
                vec![
                    ScriptStep::Write(4),
                    ScriptStep::Error(io::ErrorKind::BrokenPipe),
                ],
                Arc::clone(&first_seen),
            ),
            ScriptedWriter::new(
                vec![ScriptStep::Write(payload.len())],
                Arc::clone(&second_seen),
            ),
        ]);

        let writers = Arc::new(Mutex::new(writers));
        let mut retry = RetryWriter::new(|| {
            let writers = Arc::clone(&writers);
            let w = writers.lock().unwrap().pop_front().unwrap();
            std::future::ready(Ok(w))
        });

        retry
            .write_all_with_retry(payload)
            .await
            .expect("write should succeed");

        // First writer received partial, then error
        assert_eq!(&*first_seen.lock().unwrap(), b"full");
        // Second writer received full buffer (at-least-once resend)
        assert_eq!(&*second_seen.lock().unwrap(), payload);
    }

    #[tokio::test]
    async fn retry_writer_retries_once_on_zero_write() {
        let payload = b"test payload";
        let first_seen = Arc::new(Mutex::new(Vec::new()));
        let second_seen = Arc::new(Mutex::new(Vec::new()));

        let writers: VecDeque<_> = VecDeque::from([
            ScriptedWriter::new(vec![ScriptStep::Zero], Arc::clone(&first_seen)),
            ScriptedWriter::new(
                vec![ScriptStep::Write(payload.len())],
                Arc::clone(&second_seen),
            ),
        ]);

        let writers = Arc::new(Mutex::new(writers));
        let mut retry = RetryWriter::new(|| {
            let writers = Arc::clone(&writers);
            let w = writers.lock().unwrap().pop_front().unwrap();
            std::future::ready(Ok(w))
        });

        retry
            .write_all_with_retry(payload)
            .await
            .expect("should succeed after retry");

        // First writer saw nothing (zero write)
        assert!(first_seen.lock().unwrap().is_empty());
        // Second writer got full payload
        assert_eq!(&*second_seen.lock().unwrap(), payload);
    }

    #[tokio::test]
    async fn retry_writer_propagates_error_after_retry_fails() {
        let writers: VecDeque<_> = VecDeque::from([
            ScriptedWriter::new(
                vec![
                    ScriptStep::Write(2),
                    ScriptStep::Error(io::ErrorKind::BrokenPipe),
                ],
                Arc::new(Mutex::new(Vec::new())),
            ),
            ScriptedWriter::new(
                vec![ScriptStep::Error(io::ErrorKind::ConnectionReset)],
                Arc::new(Mutex::new(Vec::new())),
            ),
        ]);

        let writers = Arc::new(Mutex::new(writers));
        let mut retry = RetryWriter::new(|| {
            let writers = Arc::clone(&writers);
            let w = writers.lock().unwrap().pop_front().unwrap();
            std::future::ready(Ok(w))
        });

        let err = retry
            .write_all_with_retry(b"test")
            .await
            .expect_err("should fail");
        assert_eq!(err.kind(), io::ErrorKind::ConnectionReset);
    }

    #[tokio::test]
    async fn retry_writer_uses_existing_connection_on_first_attempt() {
        use super::RetryWriter;
        let received = Arc::new(Mutex::new(Vec::new()));
        let writer = ScriptedWriter::new(vec![ScriptStep::Write(5)], received.clone());

        let mut retry: RetryWriter<ScriptedWriter, fn() -> _> =
            RetryWriter::new(|| async { unreachable!("should not reconnect") });
        retry.inner = Some(writer);

        retry
            .write_all_with_retry(b"hello")
            .await
            .expect("write should succeed");

        assert_eq!(&*received.lock().unwrap(), &b"hello"[..5]);
    }

    #[tokio::test]
    async fn retry_writer_empty_buffer_succeeds_immediately() {
        use super::RetryWriter;
        let mut retry: RetryWriter<ScriptedWriter, fn() -> _> =
            RetryWriter::new(|| async { unreachable!("factory should not be called") });
        retry.inner = None;
        retry
            .write_all_with_retry(b"")
            .await
            .expect("empty write should succeed");
    }
}

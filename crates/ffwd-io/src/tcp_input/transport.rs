use std::io::{self, Read};
use std::net::{TcpListener, TcpStream};
use std::sync::Once;
use std::time::{Duration, Instant};

use bytes::Bytes;
use ffwd_types::diagnostics::ComponentHealth;
use ffwd_types::pipeline::SourceId;
use rustls::RootCertStore;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use socket2::SockRef;

use crate::input::{SourceEvent, InputSource};
use crate::polling_input_health::{PollingInputHealthEvent, reduce_polling_input_health};

/// Extract the `io::ErrorKind` from a `rustls_pki_types::pem::Error`,
/// preserving the underlying I/O kind when available.
fn pem_error_kind(e: &rustls::pki_types::pem::Error) -> io::ErrorKind {
    match e {
        rustls::pki_types::pem::Error::Io(io_err) => io_err.kind(),
        _ => io::ErrorKind::InvalidData,
    }
}

/// Per-poll read buffer size (64 KiB). Shared across all connections within a
/// single `poll` call; data is copied into per-client buffers immediately, so
/// one moderate buffer is sufficient.
const READ_BUF_SIZE: usize = 64 * 1024;

/// Default disconnect timeout for idle clients (no data received).
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Maximum frame/line payload length accepted from TCP senders.
///
/// Applies to both RFC 6587 octet-counted frames and newline-delimited lines.
/// Oversized records are discarded (not forwarded) to prevent memory abuse.
const MAX_LINE_LENGTH: usize = 1024 * 1024; // 1 MiB

/// Maximum total bytes buffered across all client `client_data` vecs within a
/// single `poll` call.  When this budget is exhausted we stop reading from
/// further clients in that poll, deferring them to the next call.  This
/// propagates TCP backpressure to senders and prevents OOM when many clients
/// flood data faster than the pipeline can drain it (fix for #576).
const MAX_TOTAL_BUFFERED_BYTES: usize = 256 * 1024 * 1024; // 256 MiB

/// Hard per-client byte bound for one `poll()` call.
///
/// Prevents one hot connection from monopolizing a full poll cycle.
const MAX_BYTES_PER_CLIENT_PER_POLL: usize = 512 * 1024;

/// Hard per-client read syscall bound for one `poll()` call.
///
/// Complements the byte cap so highly fragmented payloads are also bounded.
const MAX_READS_PER_CLIENT_PER_POLL: usize = 64;

static TLS_PROVIDER_INIT: Once = Once::new();

#[inline]
const fn should_stop_client_read(bytes_read: usize, reads_done: usize) -> bool {
    bytes_read >= MAX_BYTES_PER_CLIENT_PER_POLL || reads_done >= MAX_READS_PER_CLIENT_PER_POLL
}

/// Derive a `SourceId` for a TCP connection from a monotonic counter.
///
/// The counter is hashed to avoid trivially predictable identifiers and to
/// spread keys evenly in hash maps.
fn source_id_for_connection(connection_seq: u64) -> SourceId {
    // Use a domain-separated hash so TCP source ids never collide with
    // file-based source ids (which hash device+inode+fingerprint).
    let mut h = xxhash_rust::xxh64::Xxh64::new(0);
    h.update(b"tcp:");
    h.update(&connection_seq.to_le_bytes());
    SourceId(h.digest())
}

/// A connected TCP client with an associated last-data timestamp.
struct Client {
    transport: ClientTransport,
    source_id: SourceId,
    last_data: Instant,
    last_read: Instant,
    /// Unparsed bytes for this connection.
    pending: Vec<u8>,
    /// Set after the first successfully parsed octet-counted frame.
    octet_counting_mode: bool,
    /// Remaining bytes to drop from an oversized octet-counted frame.
    discard_octet_bytes: usize,
    /// After consuming an octet-counted payload, optionally consume exactly one
    /// delimiter newline if it appears as the immediate next byte.
    should_consume_octet_delimiter_newline: bool,
    /// Whether we are dropping newline-delimited bytes until a newline appears.
    discard_until_newline: bool,
    /// Raw bytes read from the socket that have not yet been charged to an
    /// outgoing `Data` event.  Incremented on every successful `read()` call
    /// and drained (reset to 0) when a `Data` event is emitted for this client
    /// — including on the EOF flush path.  This ensures bytes that land in
    /// `pending` without producing a complete record in the same poll are
    /// correctly counted when the record is eventually flushed.
    unaccounted_bytes: u64,
}

enum ClientTransport {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ServerConnection, TcpStream>>),
}

impl ClientTransport {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buf),
            Self::Tls(stream) => stream.read(buf),
        }
    }
}

#[derive(Clone)]
enum ListenerMode {
    Plain,
    Tls(std::sync::Arc<ServerConfig>),
}

fn parse_octet_prefix(buf: &[u8]) -> Option<(usize, usize)> {
    if buf.is_empty() || !buf[0].is_ascii_digit() {
        return None;
    }
    let mut i = 0usize;
    let mut len = 0usize;
    while i < buf.len() && buf[i].is_ascii_digit() {
        len = len.checked_mul(10)?.checked_add((buf[i] - b'0') as usize)?;
        i += 1;
    }
    if i == 0 || i >= buf.len() || buf[i] != b' ' {
        return None;
    }
    Some((len, i + 1))
}

fn advance_pending(client: &mut Client, consumed: usize) {
    if consumed == 0 {
        return;
    }
    let remaining = client.pending.len().saturating_sub(consumed);
    client.pending.copy_within(consumed.., 0);
    client.pending.truncate(remaining);
}

#[inline]
fn has_incomplete_octet_frame_tail(buf: &[u8]) -> bool {
    if buf.is_empty() || !buf[0].is_ascii_digit() {
        return false;
    }
    let mut i = 0usize;
    let mut len = 0usize;
    while i < buf.len() && buf[i].is_ascii_digit() {
        len = match len
            .checked_mul(10)
            .and_then(|v| v.checked_add((buf[i] - b'0') as usize))
        {
            Some(v) => v,
            None => return true,
        };
        i += 1;
    }
    if i >= buf.len() {
        // Digits with no delimiter can still be a truncated octet prefix.
        return true;
    }
    if buf[i] != b' ' {
        return false;
    }
    let prefix_len = i + 1;
    match prefix_len.checked_add(len) {
        Some(needed) => buf.len() < needed,
        None => true,
    }
}

fn extract_complete_records(client: &mut Client, out: &mut Vec<u8>) {
    let mut consumed = 0usize;

    loop {
        let pending = &client.pending[consumed..];
        if pending.is_empty() {
            break;
        }

        if client.discard_octet_bytes > 0 {
            let drop_n = client.discard_octet_bytes.min(pending.len());
            consumed += drop_n;
            client.discard_octet_bytes -= drop_n;
            if client.discard_octet_bytes > 0 {
                break;
            }
            continue;
        }

        if client.should_consume_octet_delimiter_newline {
            if pending[0] == b'\n' {
                consumed += 1;
            }
            client.should_consume_octet_delimiter_newline = false;
            continue;
        }

        if client.discard_until_newline {
            if let Some(pos) = memchr::memchr(b'\n', pending) {
                consumed += pos + 1;
                client.discard_until_newline = false;
                continue;
            }
            if pending.len() > MAX_LINE_LENGTH {
                consumed += pending.len() - MAX_LINE_LENGTH;
            }
            break;
        }

        if let Some((len, prefix_len)) = parse_octet_prefix(pending)
            && let Some(needed) = prefix_len.checked_add(len)
        {
            // Only commit to octet-counting when a complete, plausibly bounded
            // frame is available. This avoids legacy lines like "200 OK\n"
            // stalling behind an ambiguous "<digits><space>" prefix.
            let octet_frame_ready = pending.len() >= needed;
            let octet_boundary_is_plausible = octet_frame_ready
                && (pending.len() == needed
                    || matches!(pending.get(needed), Some(b'\n'))
                    || parse_octet_prefix(&pending[needed..]).is_some());

            if octet_frame_ready && octet_boundary_is_plausible {
                client.octet_counting_mode = true;
                let should_consume_octet_delimiter_newline =
                    pending.len() == needed || matches!(pending.get(needed), Some(b'\n'));
                if len > MAX_LINE_LENGTH {
                    client.discard_octet_bytes = len;
                    consumed += prefix_len;
                    client.should_consume_octet_delimiter_newline =
                        should_consume_octet_delimiter_newline;
                    continue;
                }
                out.extend_from_slice(&pending[prefix_len..needed]);
                out.push(b'\n');
                consumed += needed;
                client.should_consume_octet_delimiter_newline =
                    should_consume_octet_delimiter_newline;
                continue;
            }
        }

        if let Some(pos) = memchr::memchr(b'\n', pending) {
            if pos > MAX_LINE_LENGTH {
                client.discard_until_newline = true;
                continue;
            }
            out.extend_from_slice(&pending[..=pos]);
            consumed += pos + 1;
            continue;
        }

        if pending.len() > MAX_LINE_LENGTH {
            client.discard_until_newline = true;
            continue;
        }
        break;
    }

    advance_pending(client, consumed);
}

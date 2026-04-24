impl InputSource for TcpInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        let mut under_pressure = false;

        // Accept new connections up to the limit.
        loop {
            if let Some(max_clients) = self.max_clients
                && self.clients.len() >= max_clients
            {
                    // Drain (and drop) any pending connections beyond the limit so
                    // the kernel accept queue does not fill up and stall.
                    match self.listener.accept() {
                        Ok((_stream, _addr)) => {
                            self.connections_accepted += 1;
                            self.stats.tcp_accepted.store(
                                self.connections_accepted,
                                std::sync::atomic::Ordering::Relaxed,
                            );
                            if self
                                .last_max_clients_warning
                                .is_none_or(|t| t.elapsed() > Duration::from_secs(60))
                            {
                                tracing::warn!(
                                    "TCP input '{}' reached max_clients limit ({}), dropping incoming connections",
                                    self.name,
                                    max_clients
                                );
                                self.last_max_clients_warning = Some(Instant::now());
                            }
                            under_pressure = true;
                            continue; // dropped immediately
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                        Err(e) => {
                            if let Some(raw) = e.raw_os_error()
                                && (raw == libc::EMFILE || raw == libc::ENFILE)
                            {
                                under_pressure = true;
                            }
                            break; // transient accept error, not fatal
                        }
                    }
            }
            match self.listener.accept() {
                Ok((stream, _addr)) => {
                    stream.set_nonblocking(true)?;

                    // Enable TCP keepalive so we detect dead peers promptly.
                    // Use SockRef to borrow the fd — no clone, no leak.
                    let sock_ref = SockRef::from(&stream);
                    let _ = sock_ref.set_keepalive(true);
                    let keepalive = socket2::TcpKeepalive::new()
                        .with_time(Duration::from_secs(60))
                        .with_interval(Duration::from_secs(10));
                    let _ = sock_ref.set_tcp_keepalive(&keepalive);

                    let transport = match &self.listener_mode {
                        ListenerMode::Plain => ClientTransport::Plain(stream),
                        ListenerMode::Tls(cfg) => {
                            match ServerConnection::new(std::sync::Arc::clone(cfg)) {
                                Ok(conn) => {
                                    ClientTransport::Tls(Box::new(StreamOwned::new(conn, stream)))
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "tcp tls handshake initialization failed, dropping connection: {e}"
                                    );
                                    continue;
                                }
                            }
                        }
                    };
                    let sid = source_id_for_connection(self.next_connection_seq);
                    self.next_connection_seq += 1;
                    self.connections_accepted += 1;
                    self.stats.tcp_accepted.store(
                        self.connections_accepted,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    let now = Instant::now();
                    self.clients.push(Client {
                        transport,
                        source_id: sid,
                        last_data: now,
                        last_read: now,
                        pending: Vec::new(),
                        octet_counting_mode: false,
                        discard_octet_bytes: 0,
                        should_consume_octet_delimiter_newline: false,
                        discard_until_newline: false,
                        unaccounted_bytes: 0,
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::ConnectionAborted => {
                    // Peer reset before we accepted — harmless, keep going.
                }
                Err(e) => {
                    if let Some(raw) = e.raw_os_error()
                        && (raw == libc::EMFILE || raw == libc::ENFILE)
                    {
                        under_pressure = true;
                        break;
                    }
                    return Err(e);
                }
            }
        }

        // Read from all clients, collecting per-connection buffers.
        let now = Instant::now();
        // Track which connections are dead using a bitmap for O(1) lookup.
        let mut alive = vec![true; self.clients.len()];
        // Per-client data buffers — only allocated when data arrives.
        let mut client_data: Vec<Option<Vec<u8>>> = vec![None; self.clients.len()];

        // Running total of bytes stored in client_data during this poll.
        // When this reaches MAX_TOTAL_BUFFERED_BYTES we stop reading from
        // further clients (backpressure — fix for #576).
        let mut total_buffered: usize = 0;

        for (i, client) in self.clients.iter_mut().enumerate() {
            // If the global per-poll budget is exhausted, stop reading more
            // clients this poll.  They will be read on the next poll call,
            // which propagates TCP flow-control back to the senders.
            if total_buffered >= MAX_TOTAL_BUFFERED_BYTES {
                break;
            }

            let mut got_data = false;
            let mut bytes_read_for_client = 0usize;
            let mut reads_for_client = 0usize;
            loop {
                if should_stop_client_read(bytes_read_for_client, reads_for_client) {
                    break;
                }
                let remaining_bytes =
                    MAX_BYTES_PER_CLIENT_PER_POLL.saturating_sub(bytes_read_for_client);
                if remaining_bytes == 0 {
                    break;
                }
                let read_cap = remaining_bytes.min(self.buf.len());
                match client.transport.read(&mut self.buf[..read_cap]) {
                    Ok(0) => {
                        // Clean EOF.
                        alive[i] = false;
                        break;
                    }
                    Ok(n) => {
                        let chunk = &self.buf[..n];
                        client.last_data = now;
                        client.last_read = now;
                        got_data = true;
                        bytes_read_for_client = bytes_read_for_client.saturating_add(n);
                        reads_for_client = reads_for_client.saturating_add(1);

                        // Snapshot totals before this read so we can compute the
                        // net increase in buffered memory after parsing.
                        let out = client_data[i].get_or_insert_with(Vec::new);
                        let out_before = out.len();
                        let pending_before = client.pending.len(); // before extend

                        client.pending.extend_from_slice(chunk);
                        client.unaccounted_bytes =
                            client.unaccounted_bytes.saturating_add(n as u64);
                        extract_complete_records(client, out);

                        // Net new bytes held in memory for this client after this
                        // read.  We account for both the parsed output (out) and the
                        // unparsed remainder (client.pending) so that a client with a
                        // long in-flight partial record counts against the global
                        // budget.  Without this, a client can accumulate up to
                        // MAX_LINE_LENGTH unparsed bytes without tripping the
                        // backpressure threshold, undermining OOM protection (fix for
                        // accounting gap in #576).
                        //
                        // extract_complete_records may also discard oversized data, so
                        // this delta can be negative; saturating_sub keeps accounting
                        // monotonic and bounded.
                        let total_after = out.len() + client.pending.len();
                        let total_before = out_before + pending_before;
                        total_buffered += total_after.saturating_sub(total_before);

                        // We must store bytes we have already read from the
                        // socket (discarding them would be data loss), so the
                        // budget check necessarily happens after the increment.
                        // The maximum overage is one READ_BUF_SIZE chunk
                        // (64 KiB), which is negligible relative to 256 MiB.
                        if total_buffered >= MAX_TOTAL_BUFFERED_BYTES {
                            under_pressure = true;
                            break;
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) if e.kind() == io::ErrorKind::ConnectionReset => {
                        // Peer sent RST — treat as a close.
                        alive[i] = false;
                        break;
                    }
                    Err(_) => {
                        // Any other read error — drop this connection.
                        alive[i] = false;
                        break;
                    }
                }
            }
            // Check idle AFTER reading — data may have arrived since last poll.
            if !got_data && alive[i] && now.duration_since(client.last_data) > self.idle_timeout {
                alive[i] = false;
            } else if !got_data && alive[i] && !client.pending.is_empty() {
                let has_timed_out = self
                    .read_timeout
                    .is_some_and(|rt| now.duration_since(client.last_read) > rt);
                if has_timed_out {
                    alive[i] = false;
                }
            }
        }

        // Build events before removing dead connections — we need client_data
        // indices to match the current clients vec.
        let mut events = Vec::new();

        // Step 1: Data events.
        events.extend(client_data.into_iter().enumerate().filter_map(|(i, data)| {
            data.filter(|bytes| !bytes.is_empty()).map(|bytes| {
                // Drain the per-client raw-byte counter into accounted_bytes.
                // Bytes that accumulated in client.pending across previous polls
                // without producing a complete record are included here because
                // unaccounted_bytes is persistent on the Client struct.
                let accounted_bytes = std::mem::take(&mut self.clients[i].unaccounted_bytes);
                InputEvent::Data {
                    bytes: Bytes::from(bytes),
                    source_id: Some(self.clients[i].source_id),
                    accounted_bytes,
                    cri_metadata: None,
                }
            })
        }));

        // Step 2: EndOfFile events for every connection that is dying.
        //
        // A dying connection's SourceId may have an associated partial-line
        // remainder in FramedInput.  Emitting EndOfFile (after any Data for
        // the same source) signals FramedInput to flush that remainder so the
        // last unterminated record is not silently dropped — fixes #804/#580.
        //
        // Additionally, any bytes still held in client.pending were never
        // passed to FramedInput as a Data event (extract_complete_records only
        // emits complete newline-terminated records or octet-counted frames).
        // On a clean close those bytes are a valid partial line that would
        // otherwise be silently dropped.  Flush them now — with a synthetic
        // newline so FramedInput treats them as a complete record rather than
        // parking them in its own remainder — before the EndOfFile so that the
        // subsequent EndOfFile can still flush any *prior* remainder already
        // held by FramedInput.  Skip the flush if the client was mid-discard
        // (oversized frame/line): those bytes are intentionally garbage.
        for (i, &is_alive) in alive.iter().enumerate() {
            if !is_alive {
                let client = &mut self.clients[i];
                let has_pending = !client.pending.is_empty();
                let mid_discard = client.discard_octet_bytes > 0 || client.discard_until_newline;
                let incomplete_octet_tail =
                    client.octet_counting_mode && has_incomplete_octet_frame_tail(&client.pending);
                if has_pending && !mid_discard && !incomplete_octet_tail {
                    let mut tail = std::mem::take(&mut client.pending);
                    tail.push(b'\n');
                    // Drain any raw bytes accumulated but not yet charged to a
                    // Data event (e.g. bytes that never completed a record
                    // before EOF).  Resetting to 0 prevents double-counting if
                    // a Data event was already emitted earlier in this same poll
                    // for this client.
                    let accounted_bytes = std::mem::replace(&mut client.unaccounted_bytes, 0);
                    events.push(InputEvent::Data {
                        bytes: Bytes::from(tail),
                        source_id: Some(client.source_id),
                        accounted_bytes,
                        cri_metadata: None,
                    });
                }
                events.push(InputEvent::EndOfFile {
                    source_id: Some(self.clients[i].source_id),
                });
            }
        }

        // Remove dead connections, preserving order of remaining ones.
        // `retain` is cleaner than manual swap_remove with index tracking.
        let mut idx = 0;
        self.clients.retain(|_| {
            let keep = alive[idx];
            idx += 1;
            keep
        });

        self.health = reduce_polling_input_health(
            self.health,
            if under_pressure {
                PollingInputHealthEvent::BackpressureObserved
            } else {
                PollingInputHealthEvent::PollHealthy
            },
        );

        self.stats
            .tcp_active
            .store(self.clients.len(), std::sync::atomic::Ordering::Relaxed);

        Ok(events)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        self.health
    }
}

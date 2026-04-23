#[derive(Debug, Clone)]
struct RecordFields {
    attributes: Vec<Vec<u8>>,
    sequence: Option<GeneratorGeneratedFieldState>,
    event_created_unix_nano_field: Option<String>,
}

#[derive(Debug, Clone)]
struct GeneratorGeneratedFieldState {
    field: String,
    start: u64,
}

impl GeneratorInput {
    pub fn new(name: impl Into<String>, config: GeneratorConfig) -> Self {
        let name = name.into();
        // The generator treats a zero batch size as the smallest useful batch.
        // User-facing config validation should reject zero before construction.
        let batch_size = config.batch_size.max(1);
        let initial_rate_credit_events = if config.events_per_sec > 0 {
            batch_size as f64
        } else {
            0.0
        };
        let mut config = config;
        config.batch_size = batch_size;
        let mut attributes: Vec<(&String, &GeneratorAttributeValue)> =
            config.attributes.iter().collect();
        attributes.sort_by(|a, b| a.0.cmp(b.0));
        let record_fields = RecordFields {
            attributes: attributes
                .into_iter()
                .map(|(key, value)| encode_static_field(key, value))
                .collect(),
            sequence: config
                .sequence
                .as_ref()
                .map(|seq| GeneratorGeneratedFieldState {
                    field: seq.field.clone(),
                    start: seq.start,
                }),
            event_created_unix_nano_field: config.event_created_unix_nano_field.clone(),
        };
        // Pre-escape the message_template so we don't re-escape on every event.
        let message_template_escaped = config.message_template.as_deref().map(|tmpl| {
            let mut escaped = Vec::with_capacity(tmpl.len() + 4);
            write_json_escaped_string_contents(&mut escaped, tmpl);
            escaped
        });
        Self {
            name,
            buf: Vec::with_capacity(batch_size * 512),
            config,
            counter: 0,
            done: false,
            record_fields,
            last_refill: std::time::Instant::now(),
            // Seed one batch worth of credits so the first poll() can emit
            // immediately in rate-limited mode.
            rate_credit_events: initial_rate_credit_events,
            message_template_escaped,
        }
    }

    /// Return the total number of events generated so far.
    pub fn events_generated(&self) -> u64 {
        self.counter
    }

    fn generate_batch(&mut self, n: usize) {
        self.buf.clear();
        let batch_created_unix_nano = self
            .record_fields
            .event_created_unix_nano_field
            .as_ref()
            .map(|_| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            });
        for batch_offset in 0_u128..n as u128 {
            if self.config.total_events > 0 && self.counter >= self.config.total_events {
                self.done = true;
                break;
            }
            let event_created_unix_nano = batch_created_unix_nano.map(|base| base + batch_offset);
            let len_before = self.buf.len();
            self.write_event(event_created_unix_nano);
            if self.done {
                self.buf.truncate(len_before);
                break;
            }
            self.buf.push(b'\n');
            self.counter += 1;
        }
    }

    fn write_event(&mut self, event_created_unix_nano: Option<u128>) {
        match self.config.profile {
            GeneratorProfile::Logs => self.write_logs_event(),
            GeneratorProfile::Record => self.write_record_event(event_created_unix_nano),
        }
    }

    fn write_logs_event(&mut self) {
        let Some(fields) = compute_log_fields(
            self.counter,
            &self.config.timestamp,
            self.config.complexity,
            self.message_template_escaped.as_deref(),
        ) else {
            self.done = true;
            return;
        };
        write_log_fields_json(&mut self.buf, &fields);
    }

    fn write_record_event(&mut self, event_created_unix_nano: Option<u128>) {
        self.buf.push(b'{');
        let mut first = true;
        for encoded_field in &self.record_fields.attributes {
            if !first {
                self.buf.push(b',');
            }
            first = false;
            self.buf.extend_from_slice(encoded_field);
        }
        if let Some(sequence) = &self.record_fields.sequence {
            let Some(value) = sequence.start.checked_add(self.counter) else {
                self.done = true;
                return;
            };
            write_json_u64_field(&mut self.buf, &sequence.field, value, &mut first);
        }
        if let (Some(field), Some(event_created_unix_nano)) = (
            &self.record_fields.event_created_unix_nano_field,
            event_created_unix_nano,
        ) {
            write_json_u128_field(&mut self.buf, field, event_created_unix_nano, &mut first);
        }
        self.buf.push(b'}');
    }
}

impl InputSource for GeneratorInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        if self.done {
            return Ok(vec![]);
        }

        if self.config.total_events > 0 && self.counter >= self.config.total_events {
            self.done = true;
            return Ok(vec![]);
        }

        let mut events_to_emit = self.config.batch_size as u64;
        if self.config.events_per_sec > 0 {
            let batch_size = self.config.batch_size as u64;
            let now = std::time::Instant::now();
            let elapsed_sec = now
                .checked_duration_since(self.last_refill)
                .unwrap_or_default()
                .as_secs_f64();
            self.last_refill = now;
            self.rate_credit_events += elapsed_sec * self.config.events_per_sec as f64;

            // Bound carried credits to one poll burst window so scheduler stalls
            // do not turn into an arbitrarily large catch-up burst.
            let burst_cap = self.config.events_per_sec.max(batch_size);
            self.rate_credit_events = self.rate_credit_events.min(burst_cap as f64);
            let available = self.rate_credit_events.floor() as u64;
            let remaining_total = if self.config.total_events > 0 {
                self.config.total_events.saturating_sub(self.counter)
            } else {
                u64::MAX
            };
            let full_batches_available = available / batch_size;
            if full_batches_available == 0 {
                // Preserve full-batch cadence, but allow a final partial batch
                // when the finite total_events tail is smaller than batch_size.
                if self.config.total_events > 0
                    && remaining_total < batch_size
                    && available >= remaining_total
                {
                    events_to_emit = remaining_total;
                } else {
                    return Ok(vec![]);
                }
            } else {
                // Preserve the legacy "full batches only" behavior in rate-limited
                // mode while still allowing multiple batches per poll at high EPS.
                let max_full_batches = burst_cap / batch_size;
                events_to_emit = full_batches_available.min(max_full_batches) * batch_size;
            }
        }

        if self.config.total_events > 0 {
            let remaining = self.config.total_events.saturating_sub(self.counter);
            events_to_emit = events_to_emit.min(remaining);
        }

        if events_to_emit == 0 {
            return Ok(vec![]);
        }

        if self.config.events_per_sec > 0 {
            self.rate_credit_events -= events_to_emit as f64;
        }

        let expected_batches = events_to_emit.div_ceil(self.config.batch_size as u64) as usize;
        let mut out_events = Vec::with_capacity(expected_batches);
        let out_capacity = self.buf.capacity().max(self.config.batch_size * 512);
        let mut remaining = events_to_emit;
        while remaining > 0 {
            let chunk = remaining.min(self.config.batch_size as u64) as usize;
            self.generate_batch(chunk);
            if self.buf.is_empty() {
                break;
            }
            // Transfer batch ownership without copying, while keeping a
            // full-capacity hot buffer for the next generator write.
            let mut out = Vec::with_capacity(out_capacity);
            std::mem::swap(&mut self.buf, &mut out);
            let accounted_bytes = out.len() as u64;
            out_events.push(InputEvent::Data {
                bytes: Bytes::from(out),
                source_id: None,
                accounted_bytes,
                cri_metadata: None,
            });
            remaining = remaining.saturating_sub(chunk as u64);
            if self.done {
                break;
            }
        }
        Ok(out_events)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn health(&self) -> ComponentHealth {
        // Generator input has no independent bind/startup/shutdown lifecycle.
        // It is either idle or emitting synthetic events under pipeline control.
        ComponentHealth::Healthy
    }
}

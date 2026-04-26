/// Input source that generates synthetic JSON log lines.
pub struct GeneratorInput {
    name: String,
    config: GeneratorConfig,
    counter: u64,
    buf: Vec<u8>,
    done: bool,
    last_refill: std::time::Instant,
    rate_credit_events: f64,
    record_fields: RecordFields,
    /// Pre-escaped `message_template` bytes (the inner JSON string content,
    /// without surrounding quotes). `None` means use the default message.
    message_template_escaped: Option<Vec<u8>>,
}

const LEVELS: [&str; 4] = ["INFO", "DEBUG", "WARN", "ERROR"];
const PATHS: [&str; 5] = [
    "/api/v1/users",
    "/api/v1/orders",
    "/api/v2/products",
    "/health",
    "/api/v1/auth",
];
const METHODS: [&str; 4] = ["GET", "POST", "PUT", "DELETE"];
const SERVICES: [&str; 3] = ["myapp", "gateway", "auth-svc"];

// ---------------------------------------------------------------------------
// Shared event field computation
// ---------------------------------------------------------------------------

/// Computed field values for a single `logs` profile event.
///
/// Pure function of counter + config — shared between JSON and Arrow generators.
pub(crate) struct LogEventFields<'a> {
    pub timestamp: TimestampParts,
    pub level: &'static str,
    pub message_template: Option<&'a [u8]>,
    pub method: &'static str,
    pub path: &'static str,
    pub id: u64,
    pub duration_ms: u64,
    pub request_id: u64,
    pub service: &'static str,
    pub status: u32,
    pub complexity: ComplexityFields,
}

/// Pre-decomposed timestamp for both JSON formatting and Arrow string building.
pub(crate) struct TimestampParts {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub min: u32,
    pub sec: u32,
    pub ms: u32,
}

impl TimestampParts {
    /// Format as `YYYY-MM-DDTHH:MM:SS.mmmZ` into a stack buffer.
    pub fn write_iso8601(&self, out: &mut [u8; 24]) {
        // "2024-01-15T00:00:00.000Z"
        let y = self.year as u32;
        out[0] = b'0' + (y / 1000 % 10) as u8;
        out[1] = b'0' + (y / 100 % 10) as u8;
        out[2] = b'0' + (y / 10 % 10) as u8;
        out[3] = b'0' + (y % 10) as u8;
        out[4] = b'-';
        out[5] = b'0' + (self.month / 10) as u8;
        out[6] = b'0' + (self.month % 10) as u8;
        out[7] = b'-';
        out[8] = b'0' + (self.day / 10) as u8;
        out[9] = b'0' + (self.day % 10) as u8;
        out[10] = b'T';
        out[11] = b'0' + (self.hour / 10) as u8;
        out[12] = b'0' + (self.hour % 10) as u8;
        out[13] = b':';
        out[14] = b'0' + (self.min / 10) as u8;
        out[15] = b'0' + (self.min % 10) as u8;
        out[16] = b':';
        out[17] = b'0' + (self.sec / 10) as u8;
        out[18] = b'0' + (self.sec % 10) as u8;
        out[19] = b'.';
        out[20] = b'0' + (self.ms / 100) as u8;
        out[21] = b'0' + (self.ms / 10 % 10) as u8;
        out[22] = b'0' + (self.ms % 10) as u8;
        out[23] = b'Z';
    }

    /// Append `YYYY-MM-DDTHH:MM:SS.mmmZ` to `buf` (avoids a separate stack buffer).
    pub fn write_iso8601_into(&self, buf: &mut Vec<u8>) {
        let mut tmp = [0u8; 24];
        self.write_iso8601(&mut tmp);
        buf.extend_from_slice(&tmp);
    }
}

/// Extra fields present only in `Complex` events.
pub(crate) enum ComplexityFields {
    /// `Simple` profile — no extra fields.
    Simple,
    /// `Complex` profile with varying extra fields per event.
    Complex {
        bytes_in: u64,
        bytes_out: u64,
        variant: ComplexVariant,
    },
}

/// Which structural variant a complex event takes.
pub(crate) enum ComplexVariant {
    /// Includes `headers` map and `tags` array.
    WithHeadersAndTags,
    /// Includes `upstream` array with nested objects.
    WithUpstream { upstream_ms: u64 },
    /// Only `bytes_in`/`bytes_out` extras.
    Basic,
}

/// Compute the field values for one `logs` event from the counter and config.
///
/// Returns `None` if the counter overflows timestamp arithmetic (signals done).
pub(crate) fn compute_log_fields<'a>(
    counter: u64,
    timestamp_config: &GeneratorTimestamp,
    complexity: GeneratorComplexity,
    message_template_escaped: Option<&'a [u8]>,
) -> Option<LogEventFields<'a>> {
    let seq = counter;
    let level = LEVELS[(seq % LEVELS.len() as u64) as usize];
    let path = PATHS[(seq % PATHS.len() as u64) as usize];
    let method = METHODS[(seq % METHODS.len() as u64) as usize];
    let service = SERVICES[(seq % SERVICES.len() as u64) as usize];
    let id = 10000 + seq.wrapping_mul(7) % 90000;
    let dur = 1 + seq.wrapping_mul(13) % 500;
    let rid = seq.wrapping_mul(0x517c_c1b7_2722_0a95);
    let status = match seq % 20 {
        0 => 500,
        1 | 2 => 404,
        3 => 429,
        _ => 200,
    };

    let counter_i64 = i64::try_from(counter).ok()?;
    let event_ms = counter_i64
        .checked_mul(timestamp_config.step_ms)
        .and_then(|offset| timestamp_config.start_epoch_ms.checked_add(offset))?;
    let (year, month, day, hour, min, sec, ms) = convert_epoch_ms_to_parts(event_ms);

    let complexity_fields = match complexity {
        GeneratorComplexity::Simple => ComplexityFields::Simple,
        GeneratorComplexity::Complex => {
            let bytes_in = 128 + seq.wrapping_mul(17) % 8192;
            let bytes_out = 64 + seq.wrapping_mul(31) % 4096;
            let variant = if seq.is_multiple_of(5) {
                ComplexVariant::WithHeadersAndTags
            } else if seq.is_multiple_of(7) {
                ComplexVariant::WithUpstream {
                    upstream_ms: 1 + seq.wrapping_mul(19) % 200,
                }
            } else {
                ComplexVariant::Basic
            };
            ComplexityFields::Complex {
                bytes_in,
                bytes_out,
                variant,
            }
        }
    };

    Some(LogEventFields {
        timestamp: TimestampParts {
            year,
            month,
            day,
            hour,
            min,
            sec,
            ms,
        },
        level,
        message_template: message_template_escaped,
        method,
        path,
        id,
        duration_ms: dur,
        request_id: rid,
        service,
        status,
        complexity: complexity_fields,
    })
}

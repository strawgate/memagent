//! Synthetic data generator input source.
//!
//! Produces JSON log lines at a configurable rate. Used for benchmarking
//! and testing pipelines without external data sources.

use std::io;
use std::io::Write;

use crate::input::{InputEvent, InputSource};

/// Configuration for the generator input.
pub struct GeneratorConfig {
    /// Target events per second. 0 = unlimited (as fast as possible).
    pub events_per_sec: u64,
    /// Number of events per batch (per poll() call).
    pub batch_size: usize,
    /// Total events to generate. 0 = infinite.
    pub total_events: u64,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            events_per_sec: 0,
            batch_size: 1000,
            total_events: 0,
        }
    }
}

/// Input source that generates synthetic JSON log lines.
pub struct GeneratorInput {
    name: String,
    config: GeneratorConfig,
    counter: u64,
    buf: Vec<u8>,
    done: bool,
    last_batch: std::time::Instant,
}

const LEVELS: [&str; 4] = ["INFO", "DEBUG", "WARN", "ERROR"];
const PATHS: [&str; 5] = [
    "/api/v1/users",
    "/api/v1/orders",
    "/api/v2/products",
    "/health",
    "/api/v1/auth",
];

impl GeneratorInput {
    pub fn new(name: impl Into<String>, config: GeneratorConfig) -> Self {
        Self {
            name: name.into(),
            buf: Vec::with_capacity(config.batch_size * 256),
            config,
            counter: 0,
            done: false,
            last_batch: std::time::Instant::now(),
        }
    }

    fn generate_batch(&mut self) {
        self.buf.clear();
        let n = self.config.batch_size;
        for _ in 0..n {
            if self.config.total_events > 0 && self.counter >= self.config.total_events {
                self.done = true;
                break;
            }
            let i = self.counter as usize;
            let level = LEVELS[i % 4];
            let path = PATHS[i % 5];
            let id = 10000 + (i * 7) % 90000;
            let dur = 1 + (i * 13) % 500;
            let rid = (self.counter).wrapping_mul(0x517cc1b727220a95);

            let _ = write!(
                self.buf,
                r#"{{"timestamp":"2024-01-15T10:30:00.{:03}Z","level":"{level}","message":"request handled GET {path}/{id}","duration_ms":{dur},"request_id":"{rid:016x}","service":"myapp"}}"#,
                i % 1000,
            );
            self.buf.push(b'\n');
            self.counter += 1;
        }
    }
}

impl InputSource for GeneratorInput {
    fn poll(&mut self) -> io::Result<Vec<InputEvent>> {
        if self.done {
            return Ok(vec![]);
        }

        // Rate limiting: if events_per_sec > 0, sleep to hit target rate.
        if self.config.events_per_sec > 0 {
            let target_interval = std::time::Duration::from_secs_f64(
                self.config.batch_size as f64 / self.config.events_per_sec as f64,
            );
            let elapsed = self.last_batch.elapsed();
            if elapsed < target_interval {
                if let Some(wait) = target_interval.checked_sub(elapsed) {
                    std::thread::sleep(wait);
                }
            }
        }

        self.last_batch = std::time::Instant::now();
        self.generate_batch();

        if self.buf.is_empty() {
            return Ok(vec![]);
        }

        Ok(vec![InputEvent::Data {
            bytes: std::mem::take(&mut self.buf),
        }])
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_json_lines() {
        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 10,
                total_events: 10,
                ..Default::default()
            },
        );

        let events = input.poll().unwrap();
        assert_eq!(events.len(), 1);

        if let InputEvent::Data { bytes } = &events[0] {
            let text = String::from_utf8_lossy(bytes);
            let lines: Vec<&str> = text.trim().split('\n').collect();
            assert_eq!(lines.len(), 10);
            assert!(lines[0].contains("\"level\":\"INFO\""));
            assert!(lines[0].contains("\"service\":\"myapp\""));
        } else {
            panic!("expected Data event");
        }

        // Should be done after total_events.
        let events = input.poll().unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn unlimited_keeps_going() {
        let mut input = GeneratorInput::new(
            "test",
            GeneratorConfig {
                batch_size: 100,
                total_events: 0, // infinite
                ..Default::default()
            },
        );

        for _ in 0..5 {
            let events = input.poll().unwrap();
            assert_eq!(events.len(), 1);
        }
    }
}

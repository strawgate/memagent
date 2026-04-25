use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Meter};

use super::{ActiveBatch, ComponentHealth, ComponentStats};
use crate::diagnostics::render::now_nanos;

/// Pipeline-wide diagnostics counters and timing state.
///
/// This type dual-writes metrics into local atomics for status/readiness
/// snapshots and OpenTelemetry counters for OTLP export.
pub struct PipelineMetrics {
    pub name: String,
    /// (name, type, stats)
    pub inputs: Vec<(String, String, Arc<ComponentStats>)>,
    pub transform_sql: String,
    pub transform_in: Arc<ComponentStats>,
    pub transform_out: Arc<ComponentStats>,
    pub transform_errors: AtomicU64,
    /// (name, type, stats)
    pub outputs: Vec<(String, String, Arc<ComponentStats>)>,
    pub backpressure_stalls: AtomicU64,
    // Batch-level metrics (atomics for local, OTel for push)
    pub batches_total: AtomicU64,
    pub batch_rows_total: AtomicU64,
    pub flush_by_size: AtomicU64,
    pub flush_by_timeout: AtomicU64,
    /// Number of immediate repolls performed instead of sleeping between polls.
    pub cadence_fast_repolls: AtomicU64,
    /// Number of baseline poll-interval sleeps taken on idle input polls.
    pub cadence_idle_sleeps: AtomicU64,
    /// Batches that were dropped due to scan, transform, or output errors.
    pub dropped_batches_total: AtomicU64,
    /// Batches that failed the scan stage specifically.
    pub scan_errors_total: AtomicU64,
    /// Parse failures seen during scan/input decode.
    pub parse_errors_total: AtomicU64,
    // Per-stage cumulative timing (nanoseconds)
    pub scan_nanos_total: AtomicU64,
    pub transform_nanos_total: AtomicU64,
    pub output_nanos_total: AtomicU64,
    /// Cumulative nanoseconds spent waiting in the pool queue before a worker
    /// picks up the batch.
    pub queue_wait_nanos_total: AtomicU64,
    /// Cumulative nanoseconds of pure `send_batch` wall time (per-attempt).
    pub send_nanos_total: AtomicU64,
    /// Cumulative nanoseconds of total batch latency (submission to ack).
    pub batch_latency_nanos_total: AtomicU64,
    /// Unix timestamp (nanoseconds) of the last successfully processed batch.
    /// Zero means no batch has been processed yet.
    pub last_batch_time_ns: AtomicU64,
    /// Number of batches currently submitted to workers but not yet acked.
    pub inflight_batches: AtomicU64,
    pub channel_depth: AtomicU64,
    pub channel_capacity: AtomicU64,
    pub active_batches: std::sync::Mutex<HashMap<u64, ActiveBatch>>,
    pub next_batch_id: AtomicU64,
    // OTel counters (for OTLP push)
    meter: Meter,
    otel_attrs: Vec<KeyValue>,
    otel_transform_errors: Counter<u64>,
    otel_batches: Counter<u64>,
    otel_batch_rows: Counter<u64>,
    otel_flush_by_size: Counter<u64>,
    otel_flush_by_timeout: Counter<u64>,
    otel_cadence_fast_repolls: Counter<u64>,
    otel_cadence_idle_sleeps: Counter<u64>,
    otel_dropped_batches: Counter<u64>,
    otel_scan_errors: Counter<u64>,
    otel_parse_errors: Counter<u64>,
    otel_scan_nanos: Counter<u64>,
    otel_transform_nanos: Counter<u64>,
    otel_output_nanos: Counter<u64>,
    otel_queue_wait_nanos: Counter<u64>,
    otel_send_nanos: Counter<u64>,
    otel_batch_latency_nanos: Counter<u64>,
    otel_backpressure_stalls: Counter<u64>,
}

impl PipelineMetrics {
    pub fn new(name: impl Into<String>, transform_sql: impl Into<String>, meter: &Meter) -> Self {
        let name = name.into();
        let attrs = vec![KeyValue::new("pipeline", name.clone())];
        Self {
            transform_sql: transform_sql.into(),
            transform_in: Arc::new(ComponentStats::with_meter(
                meter,
                "logfwd_transform_in",
                attrs.clone(),
                ComponentHealth::Healthy,
            )),
            transform_out: Arc::new(ComponentStats::with_meter(
                meter,
                "logfwd_transform_out",
                attrs.clone(),
                ComponentHealth::Healthy,
            )),
            transform_errors: AtomicU64::new(0),
            inputs: Vec::new(),
            outputs: Vec::new(),
            backpressure_stalls: AtomicU64::new(0),
            batches_total: AtomicU64::new(0),
            batch_rows_total: AtomicU64::new(0),
            flush_by_size: AtomicU64::new(0),
            flush_by_timeout: AtomicU64::new(0),
            cadence_fast_repolls: AtomicU64::new(0),
            cadence_idle_sleeps: AtomicU64::new(0),
            dropped_batches_total: AtomicU64::new(0),
            scan_errors_total: AtomicU64::new(0),
            parse_errors_total: AtomicU64::new(0),
            scan_nanos_total: AtomicU64::new(0),
            transform_nanos_total: AtomicU64::new(0),
            output_nanos_total: AtomicU64::new(0),
            queue_wait_nanos_total: AtomicU64::new(0),
            send_nanos_total: AtomicU64::new(0),
            batch_latency_nanos_total: AtomicU64::new(0),
            last_batch_time_ns: AtomicU64::new(0),
            inflight_batches: AtomicU64::new(0),
            channel_depth: AtomicU64::new(0),
            channel_capacity: AtomicU64::new(16),
            active_batches: std::sync::Mutex::new(HashMap::new()),
            next_batch_id: AtomicU64::new(0),
            otel_transform_errors: meter.u64_counter("logfwd_transform_errors").build(),
            otel_batches: meter.u64_counter("logfwd_batches").build(),
            otel_batch_rows: meter.u64_counter("logfwd_batch_rows").build(),
            otel_flush_by_size: meter.u64_counter("logfwd_flush_by_size").build(),
            otel_flush_by_timeout: meter.u64_counter("logfwd_flush_by_timeout").build(),
            otel_cadence_fast_repolls: meter
                .u64_counter("logfwd_input_cadence_fast_repolls")
                .build(),
            otel_cadence_idle_sleeps: meter
                .u64_counter("logfwd_input_cadence_idle_sleeps")
                .build(),
            otel_dropped_batches: meter.u64_counter("logfwd_dropped_batches").build(),
            otel_scan_errors: meter.u64_counter("logfwd_scan_errors").build(),
            otel_parse_errors: meter.u64_counter("logfwd_parse_errors").build(),
            otel_scan_nanos: meter.u64_counter("logfwd_stage_scan_nanos").build(),
            otel_transform_nanos: meter.u64_counter("logfwd_stage_transform_nanos").build(),
            otel_output_nanos: meter.u64_counter("logfwd_stage_output_nanos").build(),
            otel_queue_wait_nanos: meter.u64_counter("logfwd_stage_queue_wait_nanos").build(),
            otel_send_nanos: meter.u64_counter("logfwd_stage_send_nanos").build(),
            otel_batch_latency_nanos: meter.u64_counter("logfwd_batch_latency_nanos").build(),
            otel_backpressure_stalls: meter.u64_counter("logfwd_backpressure_stalls").build(),
            meter: meter.clone(),
            otel_attrs: attrs,
            name,
        }
    }

    pub fn add_input(
        &mut self,
        name: impl Into<String>,
        typ: impl Into<String>,
    ) -> Arc<ComponentStats> {
        let name = name.into();
        let typ = typ.into();
        let attrs = vec![
            KeyValue::new("pipeline", self.name.clone()),
            KeyValue::new("input", name.clone()),
        ];
        let stats = Arc::new(ComponentStats::with_meter(
            &self.meter,
            "logfwd_input",
            attrs,
            ComponentHealth::Starting,
        ));
        self.inputs.push((name, typ, Arc::clone(&stats)));
        stats
    }

    pub fn add_output(
        &mut self,
        name: impl Into<String>,
        typ: impl Into<String>,
    ) -> Arc<ComponentStats> {
        let name = name.into();
        let typ = typ.into();
        let attrs = vec![
            KeyValue::new("pipeline", self.name.clone()),
            KeyValue::new("output", name.clone()),
        ];
        let stats = Arc::new(ComponentStats::with_meter(
            &self.meter,
            "logfwd_output",
            attrs,
            ComponentHealth::Healthy,
        ));
        self.outputs.push((name, typ, Arc::clone(&stats)));
        stats
    }

    /// Increment error counter on one output.
    /// Record successful output delivery for all output sinks.
    pub fn inc_output_success(&self, lines: u64) {
        for (_, _, stats) in &self.outputs {
            stats.inc_lines(lines);
        }
    }

    pub fn output_error(&self, output_name: &str) {
        let mut matched = false;
        for (name, _, stats) in &self.outputs {
            if name == output_name {
                stats.inc_errors();
                matched = true;
            }
        }
        if matched {
            return;
        }

        let is_fanout_runtime_name = output_name == "fanout" || output_name.starts_with("fanout(");
        let is_pipeline_rollup_name = output_name == self.name;
        if self.outputs.len() > 1 && (is_fanout_runtime_name || is_pipeline_rollup_name) {
            for (_, _, stats) in &self.outputs {
                stats.inc_errors();
            }
        }
    }

    // -- Helper methods for dual-write (called from pipeline hot loop) --------

    pub fn inc_transform_error(&self) {
        self.transform_errors.fetch_add(1, Ordering::Relaxed);
        self.otel_transform_errors.add(1, &self.otel_attrs);
    }

    pub fn inc_flush_by_size(&self) {
        self.flush_by_size.fetch_add(1, Ordering::Relaxed);
        self.otel_flush_by_size.add(1, &self.otel_attrs);
    }

    pub fn inc_flush_by_timeout(&self) {
        self.flush_by_timeout.fetch_add(1, Ordering::Relaxed);
        self.otel_flush_by_timeout.add(1, &self.otel_attrs);
    }

    /// Record one immediate adaptive repoll (sleep bypass).
    pub fn inc_cadence_fast_repoll(&self) {
        self.cadence_fast_repolls.fetch_add(1, Ordering::Relaxed);
        self.otel_cadence_fast_repolls.add(1, &self.otel_attrs);
    }

    /// Record one idle poll that slept for the baseline poll interval.
    pub fn inc_cadence_idle_sleep(&self) {
        self.cadence_idle_sleeps.fetch_add(1, Ordering::Relaxed);
        self.otel_cadence_idle_sleeps.add(1, &self.otel_attrs);
    }

    pub fn record_batch(&self, rows: u64, scan_ns: u64, transform_ns: u64, output_ns: u64) {
        self.batches_total.fetch_add(1, Ordering::Relaxed);
        self.batch_rows_total.fetch_add(rows, Ordering::Relaxed);
        self.scan_nanos_total.fetch_add(scan_ns, Ordering::Relaxed);
        self.transform_nanos_total
            .fetch_add(transform_ns, Ordering::Relaxed);
        self.output_nanos_total
            .fetch_add(output_ns, Ordering::Relaxed);
        self.last_batch_time_ns
            .store(now_nanos(), Ordering::Relaxed);

        self.otel_batches.add(1, &self.otel_attrs);
        self.otel_batch_rows.add(rows, &self.otel_attrs);
        self.otel_scan_nanos.add(scan_ns, &self.otel_attrs);
        self.otel_transform_nanos
            .add(transform_ns, &self.otel_attrs);
        self.otel_output_nanos.add(output_ns, &self.otel_attrs);
    }

    /// Record cumulative queue-wait time (time a batch spent waiting in the
    /// pool queue before a worker picked it up).
    pub fn record_queue_wait(&self, nanos: u64) {
        self.queue_wait_nanos_total
            .fetch_add(nanos, Ordering::Relaxed);
        self.otel_queue_wait_nanos.add(nanos, &self.otel_attrs);
    }

    /// Record cumulative pure `send_batch` wall time (per-attempt).
    pub fn record_send_latency(&self, nanos: u64) {
        self.send_nanos_total.fetch_add(nanos, Ordering::Relaxed);
        self.otel_send_nanos.add(nanos, &self.otel_attrs);
    }

    /// Record total batch latency (submission to ack).
    pub fn record_batch_latency(&self, nanos: u64) {
        self.batch_latency_nanos_total
            .fetch_add(nanos, Ordering::Relaxed);
        self.otel_batch_latency_nanos.add(nanos, &self.otel_attrs);
    }

    pub fn inc_backpressure_stall(&self) {
        self.backpressure_stalls.fetch_add(1, Ordering::Relaxed);
        self.otel_backpressure_stalls.add(1, &self.otel_attrs);
    }

    /// Increment the dropped-batches counter. Call whenever a batch is
    /// discarded due to a scan, transform, or output error.
    pub fn inc_dropped_batch(&self) {
        self.dropped_batches_total.fetch_add(1, Ordering::Relaxed);
        self.otel_dropped_batches.add(1, &self.otel_attrs);
    }

    /// Increment the scan-errors counter. Call when `scanner.scan()` fails.
    pub fn inc_scan_error(&self) {
        self.scan_errors_total.fetch_add(1, Ordering::Relaxed);
        self.otel_scan_errors.add(1, &self.otel_attrs);
    }

    /// Increment the parse-errors counter.
    pub fn inc_parse_error(&self) {
        self.parse_errors_total.fetch_add(1, Ordering::Relaxed);
        self.otel_parse_errors.add(1, &self.otel_attrs);
    }

    pub fn set_channel_capacity(&self, capacity: u64) {
        self.channel_capacity.store(capacity, Ordering::Relaxed);
    }

    pub fn inc_channel_depth(&self) {
        self.channel_depth.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_channel_depth(&self) {
        let _ = self
            .channel_depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_sub(1));
    }

    pub fn alloc_batch_id(&self) -> u64 {
        self.next_batch_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn begin_active_batch(&self, id: u64, start_unix_ns: u64, scan_ns: u64, transform_ns: u64) {
        if let Ok(mut m) = self.active_batches.lock() {
            m.insert(
                id,
                ActiveBatch {
                    start_unix_ns,
                    scan_ns,
                    transform_ns,
                    stage: "queued",
                    stage_start_unix_ns: start_unix_ns,
                    worker_id: None,
                    output_start_unix_ns: 0,
                },
            );
        }
    }

    /// Called by the worker pool when a worker picks up a batch for output.
    pub fn assign_worker_to_active_batch(&self, batch_id: u64, worker_id: usize, now_unix_ns: u64) {
        if let Ok(mut m) = self.active_batches.lock()
            && let Some(b) = m.get_mut(&batch_id)
        {
            b.stage = "output";
            b.worker_id = Some(worker_id as u64);
            b.output_start_unix_ns = now_unix_ns;
            b.stage_start_unix_ns = now_unix_ns;
        }
    }

    pub fn advance_active_batch(
        &self,
        id: u64,
        next_stage: &'static str,
        elapsed_ns: u64,
        now_unix_ns: u64,
    ) {
        if let Ok(mut m) = self.active_batches.lock()
            && let Some(b) = m.get_mut(&id)
        {
            match b.stage {
                "scan" => b.scan_ns = elapsed_ns,
                "transform" => b.transform_ns = elapsed_ns,
                _ => {}
            }
            b.stage = next_stage;
            b.stage_start_unix_ns = now_unix_ns;
        }
    }

    pub fn finish_active_batch(&self, id: u64) {
        if let Ok(mut m) = self.active_batches.lock() {
            m.remove(&id);
        }
    }
}

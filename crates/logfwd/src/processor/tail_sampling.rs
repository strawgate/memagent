use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, LargeStringArray, StringArray, StringViewArray, UInt32Array};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use logfwd_output::BatchMetadata;
use logfwd_transform::SqlTransform;
use smallvec::SmallVec;

use super::{Processor, ProcessorError};

/// Grouped partition result: a list of (group_key, sub-batch) pairs plus an
/// optional passthrough batch for rows with a null group value.
type PartitionResult = (Vec<(String, RecordBatch)>, Option<RecordBatch>);

const DEFAULT_MAX_BUFFERED_TRACES: usize = 50_000;

#[derive(Debug)]
struct TraceState {
    batches: Vec<RecordBatch>,
    last_seen_ns: u64,
    first_seen_ord: u64,
}

/// Tail-sampling core processor:
/// - buffer rows by trace/group id
/// - flush timed out groups on heartbeat or data batches
/// - run decision SQL over flushed data
pub struct TailSamplingProcessor {
    group_by_field: String,
    timeout_ns: u64,
    max_buffered_traces: usize,
    decision: SqlTransform,
    traces: HashMap<String, TraceState>,
    next_ord: u64,
}

impl std::fmt::Debug for TailSamplingProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TailSamplingProcessor")
            .field("group_by_field", &self.group_by_field)
            .field("timeout_ns", &self.timeout_ns)
            .field("max_buffered_traces", &self.max_buffered_traces)
            .field("traces_len", &self.traces.len())
            .finish_non_exhaustive()
    }
}

impl TailSamplingProcessor {
    /// Create a new `TailSamplingProcessor`.
    ///
    /// # Parameters
    ///
    /// - `query`: A DataFusion SQL query applied to each timed-out trace group as
    ///   a sampling decision. The query runs against a virtual table named `logs`
    ///   whose schema matches the buffered record batches. Rows returned by the
    ///   query are emitted downstream; an empty result means the trace is dropped.
    ///   Example: `"SELECT * FROM logs WHERE severity = 'ERROR'"`.
    ///
    /// - `group_by_field`: Name of the column used to group rows into trace
    ///   buckets. Must be present in every incoming batch and must have one of
    ///   the supported string types: `Utf8`, `Utf8View`, or `LargeUtf8`. Rows
    ///   where this column is null bypass buffering and pass through immediately.
    ///
    /// - `trace_timeout`: Inactivity duration after which a trace group is
    ///   considered complete. The timeout is evaluated against
    ///   `BatchMetadata::observed_time_ns`: a group times out when
    ///   `observed_time_ns - last_seen_ns >= timeout_ns`. Durations longer than
    ///   `u64::MAX` nanoseconds are clamped to `u64::MAX`.
    ///
    /// # Errors
    ///
    /// Returns `ProcessorError::Permanent` if `query` fails to parse or compile
    /// as a valid DataFusion SQL expression.
    ///
    /// Uses a default in-memory trace-group cap of `50_000` keys. To tune this,
    /// call [`TailSamplingProcessor::try_new_with_limit`].
    pub fn try_new(
        query: impl Into<String>,
        group_by_field: impl Into<String>,
        trace_timeout: Duration,
    ) -> Result<Self, ProcessorError> {
        Self::try_new_with_limit(
            query,
            group_by_field,
            trace_timeout,
            DEFAULT_MAX_BUFFERED_TRACES,
        )
    }

    /// Create a new `TailSamplingProcessor` with an explicit trace-group cap.
    ///
    /// `max_buffered_traces` bounds the number of distinct in-flight groups kept
    /// in memory at once. When the cap is reached and a new group arrives,
    /// `process()` returns `ProcessorError::Transient` so callers can retry
    /// rather than letting memory grow without bound.
    pub fn try_new_with_limit(
        query: impl Into<String>,
        group_by_field: impl Into<String>,
        trace_timeout: Duration,
        max_buffered_traces: usize,
    ) -> Result<Self, ProcessorError> {
        if max_buffered_traces == 0 {
            return Err(ProcessorError::Permanent(
                "tail-sampling max_buffered_traces must be > 0".into(),
            ));
        }

        let query = query.into();
        let decision = SqlTransform::new(&query).map_err(|e| {
            ProcessorError::Permanent(format!("invalid tail-sampling SQL query '{query}': {e}"))
        })?;
        let timeout_ns = u64::try_from(trace_timeout.as_nanos()).unwrap_or(u64::MAX);
        Ok(Self {
            group_by_field: group_by_field.into(),
            timeout_ns,
            max_buffered_traces,
            decision,
            traces: HashMap::new(),
            next_ord: 0,
        })
    }

    fn partition_by_group(&self, batch: RecordBatch) -> Result<PartitionResult, ProcessorError> {
        if batch.num_rows() > u32::MAX as usize {
            return Err(ProcessorError::Permanent(format!(
                "tail-sampling cannot partition batches larger than {} rows",
                u32::MAX
            )));
        }

        let col_idx = batch.schema().index_of(&self.group_by_field).map_err(|_| {
            ProcessorError::Permanent(format!(
                "tail-sampling group-by column '{}' not found",
                self.group_by_field
            ))
        })?;

        let group_col =
            GroupColumn::try_from_array(&self.group_by_field, batch.column(col_idx).as_ref())?;
        let mut grouped_rows: HashMap<String, (usize, Vec<u32>)> = HashMap::new();
        let mut null_rows = Vec::new();

        for row in 0..batch.num_rows() {
            let row_u32 = u32::try_from(row).map_err(|_| {
                ProcessorError::Permanent(format!(
                    "tail-sampling row index {row} exceeds u32 range"
                ))
            })?;
            match group_col.value(row) {
                Some(group) => {
                    if let Some((_, rows)) = grouped_rows.get_mut(group) {
                        rows.push(row_u32);
                    } else {
                        grouped_rows.insert(group.to_string(), (row, vec![row_u32]));
                    }
                }
                None => null_rows.push(row_u32),
            }
        }

        let mut grouped_ordered: Vec<(usize, String, Vec<u32>)> = grouped_rows
            .into_iter()
            .map(|(group, (first_seen_row, rows))| (first_seen_row, group, rows))
            .collect();
        grouped_ordered.sort_by_key(|(first_seen_row, _, _)| *first_seen_row);

        let mut grouped = Vec::with_capacity(grouped_ordered.len());
        for (_, group, rows) in grouped_ordered {
            // Intentionally materialize per-group sub-batches at partition time.
            // This keeps timeout drains/flushes simple and avoids holding
            // row-index indirection across mutable trace windows.
            grouped.push((group, take_rows(&batch, rows)?));
        }

        let passthrough = if null_rows.is_empty() {
            None
        } else {
            Some(take_rows(&batch, null_rows)?)
        };

        Ok((grouped, passthrough))
    }

    fn append_group_batch(&mut self, group: String, batch: RecordBatch, now_ns: u64) {
        match self.traces.entry(group) {
            Entry::Occupied(mut occ) => {
                let st = occ.get_mut();
                st.last_seen_ns = now_ns;
                st.batches.push(batch);
            }
            Entry::Vacant(vac) => {
                let ord = self.next_ord;
                self.next_ord = self.next_ord.saturating_add(1);
                vac.insert(TraceState {
                    batches: vec![batch],
                    last_seen_ns: now_ns,
                    first_seen_ord: ord,
                });
            }
        }
    }

    /// Collect the keys of timed-out trace groups (sorted by arrival order) WITHOUT
    /// removing them from `self.traces`. Callers must remove entries explicitly
    /// after a successful decision so that transient errors leave state intact and
    /// can be retried on the next heartbeat/batch.
    fn timed_out_keys(&self, now_ns: u64) -> Vec<(String, u64)> {
        let mut timed_out: Vec<(String, u64)> = self
            .traces
            .iter()
            .filter_map(|(k, st)| {
                if now_ns.saturating_sub(st.last_seen_ns) >= self.timeout_ns {
                    Some((k.clone(), st.first_seen_ord))
                } else {
                    None
                }
            })
            .collect();

        timed_out.sort_by_key(|(_, ord)| *ord);
        timed_out
    }

    fn run_decision_for_state(
        &mut self,
        state: &TraceState,
    ) -> Result<Option<RecordBatch>, ProcessorError> {
        if state.batches.is_empty() {
            return Ok(None);
        }
        let merged_schema = if state.batches.len() == 1 {
            state.batches[0].schema()
        } else {
            let merged =
                Schema::try_merge(state.batches.iter().map(|b| b.schema().as_ref().clone()))
                    .map_err(|e| {
                        ProcessorError::Transient(format!(
                            "failed to merge buffered trace schemas: {e}"
                        ))
                    })?;
            Arc::new(merged)
        };
        let combined = concat_batches(&merged_schema, &state.batches).map_err(|e| {
            ProcessorError::Transient(format!("failed to concat buffered trace batches: {e}"))
        })?;
        let out = self.decision.execute_blocking(combined).map_err(|e| {
            ProcessorError::Transient(format!("tail-sampling decision query failed: {e}"))
        })?;
        if out.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(out))
        }
    }

    fn drain_timed_out_into(
        &mut self,
        now_ns: u64,
        out: &mut SmallVec<[RecordBatch; 1]>,
    ) -> Result<(), ProcessorError> {
        self.drain_group_keys_into(self.timed_out_keys(now_ns), out)
    }

    fn drain_group_keys_into(
        &mut self,
        keys: Vec<(String, u64)>,
        out: &mut SmallVec<[RecordBatch; 1]>,
    ) -> Result<(), ProcessorError> {
        let mut decided: Vec<(String, TraceState, Option<RecordBatch>)> = Vec::new();

        for (key, _) in keys {
            let Some(state) = self.traces.remove(&key) else {
                continue;
            };

            match self.run_decision_for_state(&state) {
                Ok(batch) => decided.push((key, state, batch)),
                Err(e) => {
                    // Keep timeout drain transactional: on transient decision
                    // failure, restore all previously removed groups so retry
                    // can reevaluate the exact same buffered state.
                    self.traces.insert(key, state);
                    for (restore_key, restore_state, _) in decided {
                        self.traces.insert(restore_key, restore_state);
                    }
                    return Err(e);
                }
            }
        }

        for (_, _, maybe_batch) in decided {
            if let Some(batch) = maybe_batch {
                out.push(batch);
            }
        }

        Ok(())
    }
}

impl Processor for TailSamplingProcessor {
    fn process(
        &mut self,
        batch: RecordBatch,
        meta: &BatchMetadata,
    ) -> Result<SmallVec<[RecordBatch; 1]>, ProcessorError> {
        let mut out = SmallVec::new();
        let now_ns = meta.observed_time_ns;

        if batch.num_rows() > 0 {
            let (grouped, passthrough) = self.partition_by_group(batch)?;

            let timed_out_keys = self.timed_out_keys(now_ns);
            let timed_out_set: HashSet<&str> =
                timed_out_keys.iter().map(|(key, _)| key.as_str()).collect();
            let existing_after_drain = self.traces.len().saturating_sub(timed_out_keys.len());

            let new_group_count = grouped
                .iter()
                .filter(|(group, _)| {
                    !self.traces.contains_key(group) || timed_out_set.contains(group.as_str())
                })
                .count();
            if existing_after_drain.saturating_add(new_group_count) > self.max_buffered_traces {
                return Err(ProcessorError::Transient(format!(
                    "tail-sampling buffered trace limit ({}) reached; retry after timeout drain or raise the limit",
                    self.max_buffered_traces
                )));
            }

            self.drain_group_keys_into(timed_out_keys, &mut out)?;

            for (group, sub_batch) in grouped {
                self.append_group_batch(group, sub_batch, now_ns);
            }
            if let Some(pass) = passthrough {
                out.push(pass);
            }
            // Maintain immediate-timeout behavior for `Duration::ZERO`.
            if self.timeout_ns == 0 {
                self.drain_timed_out_into(now_ns, &mut out)?;
            }
        } else {
            self.drain_timed_out_into(now_ns, &mut out)?;
        }

        Ok(out)
    }

    fn flush(&mut self) -> SmallVec<[RecordBatch; 1]> {
        let mut keys: Vec<(String, u64)> = self
            .traces
            .iter()
            .map(|(k, v)| (k.clone(), v.first_seen_ord))
            .collect();
        keys.sort_by_key(|(_, ord)| *ord);

        let mut out = SmallVec::new();
        for (key, _) in keys {
            let Some(state) = self.traces.remove(&key) else {
                continue;
            };
            match self.run_decision_for_state(&state) {
                Ok(Some(batch)) => out.push(batch),
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(
                        group = %key,
                        error = %e,
                        "tail-sampling decision query failed during flush; buffered group dropped"
                    );
                }
            }
        }
        out
    }

    fn name(&self) -> &'static str {
        "tail_sampling"
    }

    fn is_stateful(&self) -> bool {
        true
    }
}

fn take_rows(batch: &RecordBatch, rows: Vec<u32>) -> Result<RecordBatch, ProcessorError> {
    let indices = UInt32Array::from(rows);
    let mut cols = Vec::with_capacity(batch.num_columns());
    for col in batch.columns() {
        let taken = take(col.as_ref(), &indices, None).map_err(|e| {
            ProcessorError::Transient(format!("failed to partition batch rows: {e}"))
        })?;
        cols.push(taken);
    }
    RecordBatch::try_new(batch.schema(), cols)
        .map_err(|e| ProcessorError::Transient(format!("failed to build partitioned batch: {e}")))
}

enum GroupColumn<'a> {
    Utf8(&'a StringArray),
    Utf8View(&'a StringViewArray),
    LargeUtf8(&'a LargeStringArray),
}

impl<'a> GroupColumn<'a> {
    fn try_from_array(field_name: &str, col: &'a dyn Array) -> Result<Self, ProcessorError> {
        match col.data_type() {
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    ProcessorError::Permanent(format!(
                        "failed to read tail-sampling group-by column '{field_name}' as Utf8"
                    ))
                })?;
                Ok(Self::Utf8(arr))
            }
            DataType::Utf8View => {
                let arr = col
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| {
                        ProcessorError::Permanent(format!(
                            "failed to read tail-sampling group-by column '{field_name}' as Utf8View"
                        ))
                    })?;
                Ok(Self::Utf8View(arr))
            }
            DataType::LargeUtf8 => {
                let arr = col
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| {
                        ProcessorError::Permanent(format!(
                            "failed to read tail-sampling group-by column '{field_name}' as LargeUtf8"
                        ))
                    })?;
                Ok(Self::LargeUtf8(arr))
            }
            other => Err(ProcessorError::Permanent(format!(
                "tail-sampling group-by column '{field_name}' has unsupported type {other:?}; expected Utf8, Utf8View, or LargeUtf8"
            ))),
        }
    }

    fn value(&self, row: usize) -> Option<&str> {
        match self {
            Self::Utf8(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
            Self::Utf8View(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
            Self::LargeUtf8(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn batch(trace_id: Vec<Option<&str>>, val: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("val", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(trace_id)),
                Arc::new(Int64Array::from(val)),
            ],
        )
        .expect("test batch")
    }

    fn batch_with_utf8_val(trace_id: Vec<Option<&str>>, val: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("val", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(trace_id)),
                Arc::new(StringArray::from(val)),
            ],
        )
        .expect("test batch")
    }

    fn meta(ns: u64) -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::new(vec![]),
            observed_time_ns: ns,
        }
    }

    #[test]
    fn timeout_emits_decided_trace() {
        let mut p = TailSamplingProcessor::try_new(
            "SELECT * FROM logs WHERE val >= 10",
            "trace_id",
            Duration::from_nanos(10),
        )
        .expect("processor");

        let out = p
            .process(batch(vec![Some("t1"), Some("t1")], vec![1, 20]), &meta(100))
            .expect("process");
        assert!(out.is_empty());

        let out = p
            .process(
                RecordBatch::new_empty(Arc::new(Schema::empty())),
                &meta(111),
            )
            .expect("heartbeat");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 1);
    }

    #[test]
    fn null_group_values_passthrough_immediately() {
        let mut p = TailSamplingProcessor::try_new(
            "SELECT * FROM logs",
            "trace_id",
            Duration::from_secs(30),
        )
        .expect("processor");

        let out = p
            .process(batch(vec![None, Some("t1")], vec![5, 6]), &meta(10))
            .expect("process");

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 1);
    }

    #[test]
    fn flush_emits_buffered_data() {
        let mut p = TailSamplingProcessor::try_new(
            "SELECT * FROM logs WHERE val > 0",
            "trace_id",
            Duration::from_secs(60),
        )
        .expect("processor");

        p.process(batch(vec![Some("a"), Some("b")], vec![1, 2]), &meta(10))
            .expect("process");

        let flushed = p.flush();
        assert_eq!(flushed.len(), 2);
        assert_eq!(flushed[0].num_rows() + flushed[1].num_rows(), 2);
    }

    #[test]
    fn timed_out_group_drains_before_appending_new_rows() {
        let mut p = TailSamplingProcessor::try_new(
            "SELECT * FROM logs",
            "trace_id",
            Duration::from_nanos(10),
        )
        .expect("processor");

        let out = p
            .process(batch(vec![Some("t1")], vec![1]), &meta(100))
            .expect("initial process");
        assert!(out.is_empty());

        let out = p
            .process(batch(vec![Some("t1")], vec![99]), &meta(111))
            .expect("timed-out drain + append new row");
        assert_eq!(out.len(), 1, "old timed-out window should be emitted first");
        let val_col = out[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        assert_eq!(val_col.value(0), 1);

        let out = p
            .process(
                RecordBatch::new_empty(Arc::new(Schema::empty())),
                &meta(122),
            )
            .expect("heartbeat");
        assert_eq!(
            out.len(),
            1,
            "new row should remain buffered as a new window"
        );
        let val_col = out[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        assert_eq!(val_col.value(0), 99);
    }

    #[test]
    fn flush_preserves_first_seen_group_order() {
        let mut p = TailSamplingProcessor::try_new(
            "SELECT * FROM logs",
            "trace_id",
            Duration::from_secs(60),
        )
        .expect("processor");

        p.process(
            batch(
                vec![Some("b"), Some("a"), Some("b"), Some("c")],
                vec![1, 2, 3, 4],
            ),
            &meta(10),
        )
        .expect("process");

        let flushed = p.flush();
        assert_eq!(flushed.len(), 3);
        let first_ids: Vec<&str> = flushed
            .iter()
            .map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("trace id column")
                    .value(0)
            })
            .collect();
        assert_eq!(first_ids, vec!["b", "a", "c"]);
    }

    #[test]
    fn configured_trace_limit_bounds_buffer_growth() {
        let mut p = TailSamplingProcessor::try_new_with_limit(
            "SELECT * FROM logs",
            "trace_id",
            Duration::from_secs(60),
            1,
        )
        .expect("processor");

        let out = p
            .process(batch(vec![Some("a")], vec![1]), &meta(10))
            .expect("first trace");
        assert!(out.is_empty());

        let err = p
            .process(batch(vec![Some("b")], vec![2]), &meta(11))
            .expect_err("second distinct trace should hit configured cap");
        match err {
            ProcessorError::Transient(msg) => {
                assert!(msg.contains("buffered trace limit"));
            }
            other => panic!("expected transient limit error, got: {other:?}"),
        }
    }

    #[test]
    fn transient_limit_error_does_not_drop_timed_out_groups() {
        let mut p = TailSamplingProcessor::try_new_with_limit(
            "SELECT * FROM logs",
            "trace_id",
            Duration::from_nanos(10),
            1,
        )
        .expect("processor");

        p.process(batch(vec![Some("old")], vec![1]), &meta(100))
            .expect("seed old group");

        let err = p
            .process(batch(vec![Some("a"), Some("b")], vec![2, 3]), &meta(111))
            .expect_err("new groups should exceed configured cap");
        assert!(
            matches!(err, ProcessorError::Transient(_)),
            "expected transient limit error"
        );

        // The timed-out "old" group must still be buffered after the transient
        // failure, so a heartbeat can drain it.
        let drained = p
            .process(
                RecordBatch::new_empty(Arc::new(Schema::empty())),
                &meta(112),
            )
            .expect("heartbeat");
        assert_eq!(drained.len(), 1);
        let val_col = drained[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        assert_eq!(val_col.value(0), 1);
    }

    #[test]
    fn timed_out_drain_is_transactional_on_decision_error() {
        let mut p = TailSamplingProcessor::try_new(
            "SELECT * FROM logs",
            "trace_id",
            Duration::from_nanos(10),
        )
        .expect("processor");

        p.process(batch(vec![Some("a"), Some("b")], vec![1, 2]), &meta(100))
            .expect("seed groups");
        p.process(
            batch_with_utf8_val(vec![Some("b")], vec![Some("schema-drift")]),
            &meta(101),
        )
        .expect("append drifted schema to b");

        let err = p
            .process(
                RecordBatch::new_empty(Arc::new(Schema::empty())),
                &meta(200),
            )
            .expect_err("mixed-schema timed-out group should error transiently");
        assert!(
            matches!(err, ProcessorError::Transient(_)),
            "expected transient error"
        );

        // No drained group should be lost when one timed-out decision fails.
        assert_eq!(p.traces.len(), 2);
        let a = p.traces.get("a").expect("group a still buffered");
        let b = p.traces.get("b").expect("group b still buffered");
        assert_eq!(a.batches.len(), 1);
        assert_eq!(b.batches.len(), 2);
    }
}

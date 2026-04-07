use std::collections::{HashMap, hash_map::Entry};
use std::time::Duration;

use arrow::array::{Array, LargeStringArray, StringArray, StringViewArray, UInt32Array};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use logfwd_output::BatchMetadata;
use logfwd_transform::SqlTransform;
use smallvec::SmallVec;

use super::{Processor, ProcessorError};

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
    decision: SqlTransform,
    traces: HashMap<String, TraceState>,
    next_ord: u64,
}

impl std::fmt::Debug for TailSamplingProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TailSamplingProcessor")
            .field("group_by_field", &self.group_by_field)
            .field("timeout_ns", &self.timeout_ns)
            .field("traces_len", &self.traces.len())
            .finish()
    }
}

impl TailSamplingProcessor {
    pub fn try_new(
        query: impl Into<String>,
        group_by_field: impl Into<String>,
        trace_timeout: Duration,
    ) -> Result<Self, ProcessorError> {
        let query = query.into();
        let decision = SqlTransform::new(&query).map_err(|e| {
            ProcessorError::Permanent(format!("invalid tail-sampling SQL query '{query}': {e}"))
        })?;
        let timeout_ns = u64::try_from(trace_timeout.as_nanos()).unwrap_or(u64::MAX);
        Ok(Self {
            group_by_field: group_by_field.into(),
            timeout_ns,
            decision,
            traces: HashMap::new(),
            next_ord: 0,
        })
    }

    fn partition_by_group(
        &self,
        batch: RecordBatch,
    ) -> Result<(Vec<(String, RecordBatch)>, Option<RecordBatch>), ProcessorError> {
        let col_idx = batch.schema().index_of(&self.group_by_field).map_err(|_| {
            ProcessorError::Permanent(format!(
                "tail-sampling group-by column '{}' not found",
                self.group_by_field
            ))
        })?;

        let group_col = batch.column(col_idx).as_ref();
        let mut grouped_rows: HashMap<String, Vec<u32>> = HashMap::new();
        let mut null_rows = Vec::new();

        for row in 0..batch.num_rows() {
            match group_value(group_col, row)? {
                Some(group) => grouped_rows.entry(group).or_default().push(row as u32),
                None => null_rows.push(row as u32),
            }
        }

        let mut grouped = Vec::with_capacity(grouped_rows.len());
        for (group, rows) in grouped_rows {
            grouped.push((group, take_rows(&batch, &rows)?));
        }

        let passthrough = if null_rows.is_empty() {
            None
        } else {
            Some(take_rows(&batch, &null_rows)?)
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

    fn drain_timed_out(&mut self, now_ns: u64) -> Vec<TraceState> {
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

        let mut drained = Vec::with_capacity(timed_out.len());
        for (k, _) in timed_out {
            if let Some(state) = self.traces.remove(&k) {
                drained.push(state);
            }
        }
        drained
    }

    fn run_decision_for_state(
        &mut self,
        state: TraceState,
    ) -> Result<Option<RecordBatch>, ProcessorError> {
        if state.batches.is_empty() {
            return Ok(None);
        }
        let schema = state.batches[0].schema();
        let combined = concat_batches(&schema, &state.batches).map_err(|e| {
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
            for (group, sub_batch) in grouped {
                self.append_group_batch(group, sub_batch, now_ns);
            }
            if let Some(pass) = passthrough {
                out.push(pass);
            }
        }

        for state in self.drain_timed_out(now_ns) {
            if let Some(decided) = self.run_decision_for_state(state)? {
                out.push(decided);
            }
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
            if let Some(state) = self.traces.remove(&key)
                && let Ok(Some(batch)) = self.run_decision_for_state(state)
            {
                out.push(batch);
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

fn take_rows(batch: &RecordBatch, rows: &[u32]) -> Result<RecordBatch, ProcessorError> {
    let indices = UInt32Array::from(rows.to_vec());
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

fn group_value(col: &dyn Array, row: usize) -> Result<Option<String>, ProcessorError> {
    if col.is_null(row) {
        return Ok(None);
    }

    match col.data_type() {
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                ProcessorError::Permanent("failed to read Utf8 group_by column".into())
            })?;
            Ok(Some(arr.value(row).to_string()))
        }
        DataType::Utf8View => {
            let arr = col
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    ProcessorError::Permanent("failed to read Utf8View group_by column".into())
                })?;
            Ok(Some(arr.value(row).to_string()))
        }
        DataType::LargeUtf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    ProcessorError::Permanent("failed to read LargeUtf8 group_by column".into())
                })?;
            Ok(Some(arr.value(row).to_string()))
        }
        other => Err(ProcessorError::Permanent(format!(
            "tail-sampling group-by column '{}' has unsupported type: {other:?}",
            col.data_type()
        ))),
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
}

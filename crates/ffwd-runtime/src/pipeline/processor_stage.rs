use arrow::record_batch::RecordBatch;
use ffwd_output::BatchMetadata;

use crate::processor::Processor;

/// Outcome of running a batch through the post-transform processor stage.
pub(super) enum ProcessorStageResult {
    /// Batch is ready for output submission.
    Forward {
        batch: RecordBatch,
        output_rows: u64,
    },
    /// Processor reported a transient error; keep checkpoints queued.
    Hold { reason: String },
    /// Processor reported a permanent (non-retryable) error; drop batch and advance checkpoints.
    PermanentError { reason: String },
    /// Processor emitted no rows; acknowledge without treating it as a drop.
    ZeroRow { reason: String },
    /// Processor reported a fatal error; shut down the pipeline and keep checkpoints queued.
    Fatal { reason: String },
    /// Processor outputs could not be merged into one batch.
    Reject { reason: String },
}

/// Run the processor chain and normalize outputs into one submission batch.
///
/// This seam keeps processor semantics (retry/drop/shutdown/reject) out of the async
/// submission shell so future processor work can evolve in a pure reducer.
pub(super) fn run_processor_stage(
    processors: &mut [Box<dyn Processor>],
    batch: RecordBatch,
    metadata: &BatchMetadata,
) -> ProcessorStageResult {
    let results = if processors.is_empty() {
        smallvec::smallvec![batch]
    } else {
        match crate::processor::run_chain(processors, batch, metadata) {
            Ok(batches) => batches,
            Err(crate::processor::ProcessorError::Transient(e)) => {
                return ProcessorStageResult::Hold { reason: e };
            }
            Err(crate::processor::ProcessorError::Permanent(e)) => {
                return ProcessorStageResult::PermanentError { reason: e };
            }
            Err(crate::processor::ProcessorError::Fatal(e)) => {
                return ProcessorStageResult::Fatal { reason: e };
            }
        }
    };

    let output_rows: u64 = results.iter().map(|b| b.num_rows() as u64).sum();
    if output_rows == 0 {
        return ProcessorStageResult::ZeroRow {
            reason: "processor chain emitted zero rows".to_string(),
        };
    }

    let batch = if results.len() == 1 {
        results.into_iter().next().expect("checked len == 1")
    } else {
        match arrow::compute::concat_batches(&results[0].schema(), results.iter()) {
            Ok(batch) => batch,
            Err(e) => {
                return ProcessorStageResult::Reject {
                    reason: e.to_string(),
                };
            }
        }
    };

    ProcessorStageResult::Forward { batch, output_rows }
}

#[cfg(test)]
mod tests {
    use super::{ProcessorStageResult, run_processor_stage};
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use ffwd_output::BatchMetadata;
    use smallvec::SmallVec;
    use std::sync::Arc;

    use crate::processor::{Processor, ProcessorError};

    fn test_meta() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::from([]),
            observed_time_ns: 0,
        }
    }

    fn int_batch(name: &str, values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
        let array = Int32Array::from(values.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("valid int32 batch")
    }

    fn string_batch(name: &str, values: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, false)]));
        let array = StringArray::from(values.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("valid utf8 batch")
    }

    #[derive(Debug)]
    struct SplitProcessor;

    impl Processor for SplitProcessor {
        fn process(
            &mut self,
            batch: RecordBatch,
            _meta: &BatchMetadata,
        ) -> Result<SmallVec<[RecordBatch; 1]>, ProcessorError> {
            let row_count = batch.num_rows();
            let string_values = vec!["left"; row_count.max(2)];
            Ok(smallvec::smallvec![
                int_batch("a", &[1, 2]),
                string_batch("a", &string_values).slice(0, row_count),
            ])
        }

        fn flush(&mut self) -> SmallVec<[RecordBatch; 1]> {
            SmallVec::new()
        }

        fn name(&self) -> &'static str {
            "split"
        }

        fn is_stateful(&self) -> bool {
            false
        }
    }

    #[derive(Debug)]
    struct CompatibleSplitProcessor;

    impl Processor for CompatibleSplitProcessor {
        fn process(
            &mut self,
            _batch: RecordBatch,
            _meta: &BatchMetadata,
        ) -> Result<SmallVec<[RecordBatch; 1]>, ProcessorError> {
            Ok(smallvec::smallvec![
                int_batch("x", &[1]),
                int_batch("x", &[2]),
            ])
        }

        fn flush(&mut self) -> SmallVec<[RecordBatch; 1]> {
            SmallVec::new()
        }

        fn name(&self) -> &'static str {
            "compatible-split"
        }

        fn is_stateful(&self) -> bool {
            false
        }
    }

    #[derive(Debug)]
    struct FailingProcessor {
        error: ProcessorError,
    }

    impl Processor for FailingProcessor {
        fn process(
            &mut self,
            _batch: RecordBatch,
            _meta: &BatchMetadata,
        ) -> Result<SmallVec<[RecordBatch; 1]>, ProcessorError> {
            Err(match &self.error {
                ProcessorError::Transient(e) => ProcessorError::Transient(e.clone()),
                ProcessorError::Permanent(e) => ProcessorError::Permanent(e.clone()),
                ProcessorError::Fatal(e) => ProcessorError::Fatal(e.clone()),
            })
        }

        fn flush(&mut self) -> SmallVec<[RecordBatch; 1]> {
            SmallVec::new()
        }

        fn name(&self) -> &'static str {
            "failing"
        }

        fn is_stateful(&self) -> bool {
            false
        }
    }

    #[test]
    fn empty_processor_chain_forwards_batch() {
        let batch = int_batch("x", &[1, 2, 3]);
        let out = run_processor_stage(&mut [], batch.clone(), &test_meta());
        match out {
            ProcessorStageResult::Forward {
                batch: forwarded,
                output_rows,
            } => {
                assert_eq!(forwarded.num_rows(), batch.num_rows());
                assert_eq!(output_rows, batch.num_rows() as u64);
            }
            _ => panic!("expected forward result"),
        }
    }

    #[test]
    fn transient_processor_error_maps_to_hold() {
        let mut processors: Vec<Box<dyn Processor>> = vec![Box::new(FailingProcessor {
            error: ProcessorError::Transient("retry me".to_string()),
        })];
        let out = run_processor_stage(&mut processors, int_batch("x", &[1]), &test_meta());
        match out {
            ProcessorStageResult::Hold { reason } => {
                assert!(reason.contains("retry me"));
            }
            _ => panic!("expected hold result"),
        }
    }

    #[test]
    fn permanent_processor_error_maps_to_permanent_error() {
        let mut processors: Vec<Box<dyn Processor>> = vec![Box::new(FailingProcessor {
            error: ProcessorError::Permanent("bad batch".to_string()),
        })];
        let out = run_processor_stage(&mut processors, int_batch("x", &[1]), &test_meta());
        match out {
            ProcessorStageResult::PermanentError { reason } => {
                assert!(reason.contains("bad batch"));
            }
            _ => panic!("expected permanent-error result"),
        }
    }

    #[test]
    fn empty_processor_output_maps_to_zero_row() {
        #[derive(Debug)]
        struct EmptyProcessor;

        impl Processor for EmptyProcessor {
            fn process(
                &mut self,
                _batch: RecordBatch,
                _meta: &BatchMetadata,
            ) -> Result<SmallVec<[RecordBatch; 1]>, ProcessorError> {
                Ok(SmallVec::new())
            }

            fn flush(&mut self) -> SmallVec<[RecordBatch; 1]> {
                SmallVec::new()
            }

            fn name(&self) -> &'static str {
                "empty"
            }

            fn is_stateful(&self) -> bool {
                false
            }
        }

        let mut processors: Vec<Box<dyn Processor>> = vec![Box::new(EmptyProcessor)];
        let out = run_processor_stage(&mut processors, int_batch("x", &[1]), &test_meta());
        match out {
            ProcessorStageResult::ZeroRow { reason } => {
                assert!(reason.contains("zero rows"));
            }
            _ => panic!("expected zero-row result"),
        }
    }

    #[test]
    fn fatal_processor_error_maps_to_fatal() {
        let mut processors: Vec<Box<dyn Processor>> = vec![Box::new(FailingProcessor {
            error: ProcessorError::Fatal("shutdown".to_string()),
        })];
        let out = run_processor_stage(&mut processors, int_batch("x", &[1]), &test_meta());
        match out {
            ProcessorStageResult::Fatal { reason } => {
                assert!(reason.contains("shutdown"));
            }
            _ => panic!("expected fatal result"),
        }
    }

    #[test]
    fn incompatible_processor_outputs_map_to_reject() {
        let mut processors: Vec<Box<dyn Processor>> = vec![Box::new(SplitProcessor)];
        let out = run_processor_stage(&mut processors, int_batch("x", &[1]), &test_meta());
        match out {
            ProcessorStageResult::Reject { reason } => {
                assert!(
                    !reason.is_empty(),
                    "reject path should include a diagnostic reason"
                );
            }
            _ => panic!("expected reject result"),
        }
    }

    #[test]
    fn compatible_multi_batch_outputs_forward_with_summed_rows() {
        let mut processors: Vec<Box<dyn Processor>> = vec![Box::new(CompatibleSplitProcessor)];
        let out = run_processor_stage(&mut processors, int_batch("x", &[1, 2]), &test_meta());
        match out {
            ProcessorStageResult::Forward { batch, output_rows } => {
                assert_eq!(output_rows, 2);
                assert_eq!(batch.num_rows(), 2);
            }
            _ => panic!("expected forward result"),
        }
    }
}

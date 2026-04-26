#![allow(clippy::indexing_slicing)]

use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};
use tokio::sync::oneshot;

/// Worker-owned blocking processor for synchronous CPU or decode work.
pub(crate) trait BlockingWorker: Send + 'static {
    /// Submitted job type.
    type Job: Send + 'static;
    /// Successful job output.
    type Output: Send + 'static;
    /// Recoverable worker error.
    type Error: Send + 'static;

    /// Process one job using state owned by this worker thread.
    fn process(&mut self, job: Self::Job) -> Result<Self::Output, Self::Error>;
}

/// Error returned when a job cannot be submitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BlockingStageSubmitError {
    /// The stage has reached its configured active-plus-queued job limit.
    Full,
    /// The stage has closed.
    Closed,
    /// A worker panicked and the stage is no longer accepting work.
    WorkerFailed,
}

/// Error returned while waiting for a submitted job.
#[derive(Debug)]
pub(crate) enum BlockingStageReceiveError<E> {
    /// Worker returned a recoverable processing error.
    Worker(E),
    /// Worker panicked while processing this job.
    WorkerPanicked,
    /// Stage closed before this job produced a result.
    Closed,
}

/// Result handle for a submitted blocking-stage job.
pub(crate) struct BlockingStageResult<O, E> {
    rx: oneshot::Receiver<Result<O, BlockingStageReceiveError<E>>>,
}

impl<O, E> BlockingStageResult<O, E> {
    /// Wait for the submitted job result.
    pub(crate) async fn recv(self) -> Result<O, BlockingStageReceiveError<E>> {
        self.rx
            .await
            .unwrap_or(Err(BlockingStageReceiveError::Closed))
    }
}

struct OutstandingJob {
    outstanding: Arc<AtomicUsize>,
}

impl Drop for OutstandingJob {
    fn drop(&mut self) {
        self.outstanding.fetch_sub(1, Ordering::Release);
    }
}

struct StageJob<W: BlockingWorker> {
    job: W::Job,
    response_tx: oneshot::Sender<Result<W::Output, BlockingStageReceiveError<W::Error>>>,
    _outstanding: OutstandingJob,
}

/// Fixed-worker blocking stage with bounded active-plus-queued jobs.
pub(crate) struct BoundedBlockingStage<W: BlockingWorker> {
    submit_txs: Vec<Sender<StageJob<W>>>,
    worker_handles: Vec<JoinHandle<()>>,
    next_worker: AtomicUsize,
    max_outstanding: usize,
    outstanding: Arc<AtomicUsize>,
    is_worker_failed: Arc<AtomicBool>,
    is_shutting_down: Arc<AtomicBool>,
}

impl<W: BlockingWorker> BoundedBlockingStage<W> {
    /// Create a stage with fixed worker count and bounded outstanding jobs.
    pub(crate) fn new<F>(
        worker_count: usize,
        max_outstanding: usize,
        mut build_worker: F,
    ) -> io::Result<Self>
    where
        F: FnMut(usize) -> W,
    {
        let worker_count = worker_count.max(1);
        let max_outstanding = max_outstanding.max(1);
        let outstanding = Arc::new(AtomicUsize::new(0));
        let is_worker_failed = Arc::new(AtomicBool::new(false));
        let is_shutting_down = Arc::new(AtomicBool::new(false));

        let mut submit_txs = Vec::with_capacity(worker_count);
        let mut worker_handles = Vec::with_capacity(worker_count);
        for worker_id in 0..worker_count {
            let (worker_tx, worker_rx) = bounded::<StageJob<W>>(max_outstanding);
            submit_txs.push(worker_tx);
            let worker_failed = Arc::clone(&is_worker_failed);
            let shutting_down = Arc::clone(&is_shutting_down);
            let mut worker = build_worker(worker_id);
            let handle = thread::Builder::new()
                .name(format!("blocking-stage-{worker_id}"))
                .spawn(move || worker_loop(&mut worker, worker_rx, worker_failed, shutting_down))?;
            worker_handles.push(handle);
        }

        Ok(Self {
            submit_txs,
            worker_handles,
            next_worker: AtomicUsize::new(0),
            max_outstanding,
            outstanding,
            is_worker_failed,
            is_shutting_down,
        })
    }

    /// Try to submit a job without waiting for capacity.
    pub(crate) fn try_submit(
        &self,
        job: W::Job,
    ) -> Result<BlockingStageResult<W::Output, W::Error>, BlockingStageSubmitError> {
        if self.is_worker_failed.load(Ordering::Acquire) {
            return Err(BlockingStageSubmitError::WorkerFailed);
        }
        if self.is_shutting_down.load(Ordering::Acquire) {
            return Err(BlockingStageSubmitError::Closed);
        }
        if self.submit_txs.is_empty() {
            return Err(BlockingStageSubmitError::Closed);
        }

        self.reserve_outstanding()?;
        let outstanding = OutstandingJob {
            outstanding: Arc::clone(&self.outstanding),
        };
        let (response_tx, rx) = oneshot::channel();
        let message = StageJob {
            job,
            response_tx,
            _outstanding: outstanding,
        };

        match self.try_send_round_robin(message) {
            Ok(()) => Ok(BlockingStageResult { rx }),
            Err(err) => Err(err),
        }
    }

    fn try_send_round_robin(
        &self,
        mut message: StageJob<W>,
    ) -> Result<(), BlockingStageSubmitError> {
        let worker_count = self.submit_txs.len();
        let start = self.next_worker.fetch_add(1, Ordering::Relaxed);
        let mut saw_disconnected = false;

        for offset in 0..worker_count {
            let index = start.wrapping_add(offset) % worker_count;
            match self.submit_txs[index].try_send(message) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(returned)) => {
                    message = returned;
                }
                Err(TrySendError::Disconnected(returned)) => {
                    saw_disconnected = true;
                    message = returned;
                }
            }
        }

        drop(message);
        if self.is_worker_failed.load(Ordering::Acquire) {
            Err(BlockingStageSubmitError::WorkerFailed)
        } else if saw_disconnected {
            Err(BlockingStageSubmitError::Closed)
        } else {
            Err(BlockingStageSubmitError::Full)
        }
    }

    fn reserve_outstanding(&self) -> Result<(), BlockingStageSubmitError> {
        let mut current = self.outstanding.load(Ordering::Acquire);
        loop {
            if current >= self.max_outstanding {
                return Err(BlockingStageSubmitError::Full);
            }
            match self.outstanding.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(next) => current = next,
            }
        }
    }
}

impl<W: BlockingWorker> Drop for BoundedBlockingStage<W> {
    fn drop(&mut self) {
        self.is_shutting_down.store(true, Ordering::Release);
        self.submit_txs.clear();
        for handle in self.worker_handles.drain(..) {
            let _ = handle.join();
        }
    }
}

fn worker_loop<W: BlockingWorker>(
    worker: &mut W,
    submit_rx: Receiver<StageJob<W>>,
    is_worker_failed: Arc<AtomicBool>,
    is_shutting_down: Arc<AtomicBool>,
) {
    while let Ok(StageJob {
        job,
        response_tx,
        _outstanding,
    }) = submit_rx.recv()
    {
        if is_worker_failed.load(Ordering::Acquire) || is_shutting_down.load(Ordering::Acquire) {
            let _ = response_tx.send(Err(BlockingStageReceiveError::Closed));
            close_pending_jobs(&submit_rx);
            return;
        }

        let outcome = catch_unwind(AssertUnwindSafe(|| worker.process(job)));
        match outcome {
            Ok(Ok(output)) => {
                let _ = response_tx.send(Ok(output));
            }
            Ok(Err(err)) => {
                let _ = response_tx.send(Err(BlockingStageReceiveError::Worker(err)));
            }
            Err(_) => {
                is_worker_failed.store(true, Ordering::Release);
                let _ = response_tx.send(Err(BlockingStageReceiveError::WorkerPanicked));
                close_pending_jobs(&submit_rx);
                return;
            }
        }
    }
}

fn close_pending_jobs<W: BlockingWorker>(submit_rx: &Receiver<StageJob<W>>) {
    while let Ok(StageJob { response_tx, .. }) = submit_rx.try_recv() {
        let _ = response_tx.send(Err(BlockingStageReceiveError::Closed));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::thread::ThreadId;
    use std::time::{Duration, Instant};

    struct CountingWorker {
        processed: usize,
        total_seen: Arc<AtomicUsize>,
    }

    impl BlockingWorker for CountingWorker {
        type Job = usize;
        type Output = usize;
        type Error = &'static str;

        fn process(&mut self, job: Self::Job) -> Result<Self::Output, Self::Error> {
            if job == usize::MAX {
                panic!("forced panic");
            }
            if job == usize::MAX - 1 {
                return Err("forced worker error");
            }
            if job == usize::MAX - 2 {
                thread::sleep(Duration::from_millis(100));
                return Ok(job);
            }
            self.processed += 1;
            self.total_seen.fetch_add(1, Ordering::Relaxed);
            Ok(job + self.processed)
        }
    }

    struct WorkerIdWorker {
        worker_id: usize,
    }

    impl BlockingWorker for WorkerIdWorker {
        type Job = ();
        type Output = usize;
        type Error = ();

        fn process(&mut self, (): Self::Job) -> Result<Self::Output, Self::Error> {
            Ok(self.worker_id)
        }
    }

    struct ThreadIdentityWorker;

    impl BlockingWorker for ThreadIdentityWorker {
        type Job = ();
        type Output = (ThreadId, Option<String>);
        type Error = ();

        fn process(&mut self, (): Self::Job) -> Result<Self::Output, Self::Error> {
            let current = thread::current();
            Ok((current.id(), current.name().map(str::to_owned)))
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should build")
            .block_on(future)
    }

    #[test]
    fn reports_full_when_outstanding_limit_is_reached() {
        let total_seen = Arc::new(AtomicUsize::new(0));
        let stage = BoundedBlockingStage::new(1, 1, |_| CountingWorker {
            processed: 0,
            total_seen: Arc::clone(&total_seen),
        })
        .expect("stage should build");

        let first = stage
            .try_submit(usize::MAX - 2)
            .expect("first submission should enqueue");
        assert!(matches!(
            stage.try_submit(2),
            Err(BlockingStageSubmitError::Full)
        ));
        let _ = block_on(first.recv()).expect("first result should complete");
    }

    #[test]
    fn reuses_worker_owned_state() {
        let total_seen = Arc::new(AtomicUsize::new(0));
        let stage = BoundedBlockingStage::new(1, 4, |_| CountingWorker {
            processed: 0,
            total_seen: Arc::clone(&total_seen),
        })
        .expect("stage should build");

        let first = stage.try_submit(10).expect("first submit");
        let second = stage.try_submit(10).expect("second submit");
        let first_out = block_on(first.recv()).expect("first output");
        let second_out = block_on(second.recv()).expect("second output");

        assert_eq!(first_out, 11);
        assert_eq!(second_out, 12);
        assert_eq!(total_seen.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn reports_worker_errors_per_job() {
        let total_seen = Arc::new(AtomicUsize::new(0));
        let stage = BoundedBlockingStage::new(1, 4, |_| CountingWorker {
            processed: 0,
            total_seen: Arc::clone(&total_seen),
        })
        .expect("stage should build");

        let result = block_on(
            stage
                .try_submit(usize::MAX - 1)
                .expect("worker error job should submit")
                .recv(),
        );
        assert!(matches!(
            result,
            Err(BlockingStageReceiveError::Worker("forced worker error"))
        ));
    }

    #[test]
    fn marks_worker_panic_as_stage_failure() {
        let total_seen = Arc::new(AtomicUsize::new(0));
        let stage = BoundedBlockingStage::new(1, 4, |_| CountingWorker {
            processed: 0,
            total_seen: Arc::clone(&total_seen),
        })
        .expect("stage should build");

        let panic_job = stage.try_submit(usize::MAX).expect("panic job submit");
        let panic_result = block_on(panic_job.recv());
        assert!(matches!(
            panic_result,
            Err(BlockingStageReceiveError::WorkerPanicked)
        ));

        assert!(matches!(
            stage.try_submit(1),
            Err(BlockingStageSubmitError::WorkerFailed)
        ));
    }

    #[test]
    fn worker_panic_closes_queued_jobs() {
        let total_seen = Arc::new(AtomicUsize::new(0));
        let stage = BoundedBlockingStage::new(1, 2, |_| CountingWorker {
            processed: 0,
            total_seen: Arc::clone(&total_seen),
        })
        .expect("stage should build");

        let panic_job = stage.try_submit(usize::MAX).expect("panic job submit");
        let queued = stage.try_submit(1).expect("queued job submit");

        assert!(matches!(
            block_on(panic_job.recv()),
            Err(BlockingStageReceiveError::WorkerPanicked)
        ));
        assert!(matches!(
            block_on(queued.recv()),
            Err(BlockingStageReceiveError::Closed)
        ));
    }

    #[test]
    fn drop_closes_queued_jobs_without_processing_them() {
        let total_seen = Arc::new(AtomicUsize::new(0));
        let stage = BoundedBlockingStage::new(1, 2, |_| CountingWorker {
            processed: 0,
            total_seen: Arc::clone(&total_seen),
        })
        .expect("stage should build");

        let first = stage
            .try_submit(usize::MAX - 2)
            .expect("sleeping job submit");
        let queued = stage.try_submit(1).expect("queued job submit");
        drop(stage);

        match block_on(first.recv()) {
            Ok(_) | Err(BlockingStageReceiveError::Closed) => {}
            Err(other) => panic!("unexpected first job result during shutdown: {other:?}"),
        }
        assert!(matches!(
            block_on(queued.recv()),
            Err(BlockingStageReceiveError::Closed)
        ));
        assert_eq!(
            total_seen.load(Ordering::Relaxed),
            0,
            "queued normal job should not be processed during shutdown"
        );
    }

    #[test]
    fn concurrent_submissions_complete_without_deadlock() {
        let total_seen = Arc::new(AtomicUsize::new(0));
        let stage = Arc::new(
            BoundedBlockingStage::new(4, 32, |_| CountingWorker {
                processed: 0,
                total_seen: Arc::clone(&total_seen),
            })
            .expect("stage should build"),
        );

        let mut handles = Vec::new();
        for i in 0..32usize {
            let stage = Arc::clone(&stage);
            handles.push(thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(10);
                loop {
                    assert!(
                        Instant::now() < deadline,
                        "timed out waiting for submit slot"
                    );
                    match stage.try_submit(i) {
                        Ok(result) => return block_on(result.recv()).expect("result"),
                        Err(BlockingStageSubmitError::Full) => thread::yield_now(),
                        Err(other) => panic!("unexpected submit error: {other:?}"),
                    }
                }
            }));
        }

        let mut joined = Vec::with_capacity(handles.len());
        for handle in handles {
            joined.push(handle.join().expect("thread should join"));
        }
        assert_eq!(joined.len(), 32);
        assert_eq!(total_seen.load(Ordering::Relaxed), 32);
    }

    #[test]
    fn submissions_are_dispatched_across_workers() {
        let stage = BoundedBlockingStage::new(2, 4, |worker_id| WorkerIdWorker { worker_id })
            .expect("stage should build");

        let first = stage.try_submit(()).expect("first submit");
        let second = stage.try_submit(()).expect("second submit");
        let first_worker = block_on(first.recv()).expect("first result");
        let second_worker = block_on(second.recv()).expect("second result");

        assert_ne!(
            first_worker, second_worker,
            "round-robin dispatch should use both workers"
        );
    }

    #[test]
    fn dropping_result_receiver_does_not_leak_outstanding_capacity() {
        let total_seen = Arc::new(AtomicUsize::new(0));
        let stage = BoundedBlockingStage::new(1, 1, |_| CountingWorker {
            processed: 0,
            total_seen: Arc::clone(&total_seen),
        })
        .expect("stage should build");

        let deadline = Instant::now() + Duration::from_secs(10);
        for _ in 0..64usize {
            loop {
                assert!(
                    Instant::now() < deadline,
                    "timed out submitting dropped-receiver jobs"
                );
                match stage.try_submit(1) {
                    Ok(result) => {
                        drop(result);
                        break;
                    }
                    Err(BlockingStageSubmitError::Full) => thread::yield_now(),
                    Err(other) => panic!("unexpected submit error while probing leak: {other:?}"),
                }
            }
        }

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(
                Instant::now() < deadline,
                "timed out submitting final verification job"
            );
            match stage.try_submit(1) {
                Ok(result) => {
                    let output = block_on(result.recv()).expect("final output should complete");
                    assert!(output >= 2, "worker should continue processing jobs");
                    break;
                }
                Err(BlockingStageSubmitError::Full) => thread::yield_now(),
                Err(other) => panic!("unexpected submit error after dropped receivers: {other:?}"),
            }
        }

        assert_eq!(
            total_seen.load(Ordering::Relaxed),
            65,
            "all dropped-receiver jobs should still be processed without leaking permits"
        );
    }

    #[test]
    fn jobs_run_on_dedicated_worker_threads_not_submitter_threads() {
        let stage =
            BoundedBlockingStage::new(1, 1, |_| ThreadIdentityWorker).expect("stage should build");
        let submitter = thread::current();
        let submitter_id = submitter.id();

        let (worker_id, worker_name) =
            block_on(stage.try_submit(()).expect("submit").recv()).expect("result should succeed");

        assert_ne!(
            worker_id, submitter_id,
            "CPU job must execute on dedicated blocking-stage worker thread"
        );
        assert!(
            worker_name
                .as_deref()
                .is_some_and(|name| name.starts_with("blocking-stage-")),
            "worker thread should use blocking-stage thread namespace"
        );
    }
}

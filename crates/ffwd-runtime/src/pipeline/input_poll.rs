#[cfg(feature = "turmoil")]
use std::sync::Arc;
#[cfg(feature = "turmoil")]
use std::time::Duration;

#[cfg(feature = "turmoil")]
use ffwd_diagnostics::diagnostics::PipelineMetrics;
#[cfg(feature = "turmoil")]
use ffwd_io::input::SourceEvent;
#[cfg(feature = "turmoil")]
use ffwd_io::poll_cadence::AdaptivePollController;
#[cfg(feature = "turmoil")]
use tokio_util::sync::CancellationToken;

#[cfg(feature = "turmoil")]
use super::health::{
    HealthTransitionEvent, reduce_component_health, reduce_component_health_after_poll_failure,
};
#[cfg(feature = "turmoil")]
use super::submit::{scan_and_transform_for_send, transform_direct_batch_for_send};
#[cfg(feature = "turmoil")]
use super::{IngestState, ProcessedBatch, SourcePipeline};

#[cfg(feature = "turmoil")]
const SHUTDOWN_DRAIN_PROGRESS_LOG_INTERVAL_ROUNDS: usize = 64;
#[cfg(feature = "turmoil")]
const MAX_SHUTDOWN_POLL_ROUNDS: usize = 4096;

#[cfg(feature = "turmoil")]
fn should_repoll_shutdown(
    events: &[SourceEvent],
    is_finished: bool,
    had_source_payload: bool,
) -> bool {
    if is_finished {
        return false;
    }
    if events
        .iter()
        .any(|event| matches!(event, SourceEvent::EndOfFile { source_id: None }))
    {
        return false;
    }
    if events.is_empty() {
        return had_source_payload;
    }
    if had_source_payload
        && !events
            .iter()
            .any(|event| matches!(event, SourceEvent::Data { .. } | SourceEvent::Batch { .. }))
    {
        return true;
    }
    events.iter().any(|event| {
        let payload_source_id = match event {
            SourceEvent::Data { source_id, .. } | SourceEvent::Batch { source_id, .. } => {
                *source_id
            }
            SourceEvent::Rotated { .. }
            | SourceEvent::Truncated { .. }
            | SourceEvent::EndOfFile { .. } => {
                return false;
            }
        };
        !events.iter().any(|event| {
            matches!(
                event,
                SourceEvent::EndOfFile { source_id }
                    if *source_id == payload_source_id
            )
        })
    })
}

#[inline]
#[cfg(any(feature = "turmoil", test, kani))]
const fn should_flush_buffer(
    buffered_len: usize,
    batch_target_bytes: usize,
    timeout_elapsed: bool,
) -> bool {
    let safe_target = if batch_target_bytes == 0 {
        1
    } else {
        batch_target_bytes
    };
    buffered_len >= safe_target || (buffered_len > 0 && timeout_elapsed)
}

#[cfg(feature = "turmoil")]
async fn send_channel_msg(
    tx: &tokio::sync::mpsc::Sender<ProcessedBatch>,
    msg: ProcessedBatch,
) -> Result<(), tokio::sync::mpsc::error::SendError<ProcessedBatch>> {
    tx.send(msg).await
}

#[cfg(feature = "turmoil")]
#[allow(clippy::too_many_arguments)]
async fn process_input_events(
    input: &mut IngestState,
    transform: &mut SourcePipeline,
    tx: &tokio::sync::mpsc::Sender<ProcessedBatch>,
    metrics: &PipelineMetrics,
    input_index: usize,
    events: Vec<SourceEvent>,
    buffered_since: &mut Option<tokio::time::Instant>,
) -> bool {
    for event in events {
        match event {
            SourceEvent::Data { bytes, .. } => {
                input.buf.extend_from_slice(&bytes);
            }
            SourceEvent::Batch { batch, .. } => {
                if !input.buf.is_empty() {
                    if let Some(msg) =
                        scan_and_transform_for_send(input, transform, metrics, input_index).await
                    {
                        if send_channel_msg(tx, msg).await.is_err() {
                            return false;
                        }
                        metrics.inc_channel_depth();
                    }
                    *buffered_since = None;
                }

                if let Some(msg) =
                    transform_direct_batch_for_send(input, transform, metrics, input_index, batch)
                        .await
                {
                    if send_channel_msg(tx, msg).await.is_err() {
                        return false;
                    }
                    metrics.inc_channel_depth();
                }
            }
            SourceEvent::Rotated { .. } => {
                input.stats.inc_rotations();
            }
            SourceEvent::Truncated { .. } => {
                // Treat truncation as a rotation-equivalent rewind signal
                // for existing dashboards that chart a single restart counter.
                input.stats.inc_rotations();
            }
            SourceEvent::EndOfFile { .. } => {}
        }
    }
    if buffered_since.is_none() && !input.buf.is_empty() {
        *buffered_since = Some(tokio::time::Instant::now());
    }
    true
}

/// Async input loop for simulation testing.
///
/// Polls source, accumulates bytes, scans + SQL transforms, and sends
/// `ProcessedBatch` — same output type as production CPU workers. Uses
/// `tokio::time::sleep` so Turmoil's simulated time advances deterministically.
#[cfg(feature = "turmoil")]
#[allow(clippy::too_many_arguments)]
pub(super) async fn async_input_poll_loop(
    mut input: IngestState,
    mut transform: SourcePipeline,
    tx: tokio::sync::mpsc::Sender<ProcessedBatch>,
    metrics: Arc<PipelineMetrics>,
    shutdown: CancellationToken,
    batch_target_bytes: usize,
    batch_timeout: Duration,
    poll_interval: Duration,
    input_index: usize,
) {
    let mut buffered_since: Option<tokio::time::Instant> = None;
    let mut consecutive_poll_failures: u32 = 0;
    let mut adaptive_poll =
        AdaptivePollController::new(input.source.get_cadence().adaptive_fast_polls_max);
    'poll_loop: loop {
        if shutdown.is_cancelled() {
            input.stats.set_health(reduce_component_health(
                input.stats.health(),
                HealthTransitionEvent::ShutdownRequested,
            ));
            let mut shutdown_poll_rounds = 0usize;
            loop {
                match input.source.poll_shutdown() {
                    Ok(events) => {
                        let cadence = input.source.get_cadence();
                        let should_repoll = should_repoll_shutdown(
                            &events,
                            input.source.is_finished(),
                            cadence.signal.had_data,
                        );
                        if !process_input_events(
                            &mut input,
                            &mut transform,
                            &tx,
                            &metrics,
                            input_index,
                            events,
                            &mut buffered_since,
                        )
                        .await
                        {
                            break 'poll_loop;
                        }
                        if !should_repoll {
                            break;
                        }
                        shutdown_poll_rounds = shutdown_poll_rounds.saturating_add(1);
                        if shutdown_poll_rounds
                            .is_multiple_of(SHUTDOWN_DRAIN_PROGRESS_LOG_INTERVAL_ROUNDS)
                        {
                            tracing::warn!(
                                input = input.source.name(),
                                rounds = shutdown_poll_rounds,
                                "input.shutdown_drain_still_active"
                            );
                        }
                        if shutdown_poll_rounds >= MAX_SHUTDOWN_POLL_ROUNDS {
                            tracing::error!(
                                input = input.source.name(),
                                rounds = shutdown_poll_rounds,
                                "input.shutdown_drain_aborted_hard_limit"
                            );
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            input = input.source.name(),
                            error = %e,
                            "input.shutdown_poll_error"
                        );
                        break;
                    }
                }
            }
            break;
        }

        let events = match input.source.poll() {
            Ok(e) => e,
            Err(e) => {
                adaptive_poll.reset_fast_polls();
                input.stats.inc_errors();
                consecutive_poll_failures = consecutive_poll_failures.saturating_add(1);
                input
                    .stats
                    .set_health(reduce_component_health_after_poll_failure(
                        input.stats.health(),
                        consecutive_poll_failures,
                    ));
                tracing::warn!(input = input.source.name(), error = %e, "input.poll_error");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        consecutive_poll_failures = 0;

        input.stats.set_health(reduce_component_health(
            input.stats.health(),
            HealthTransitionEvent::Observed(input.source.health()),
        ));
        let cadence = input.source.get_cadence();
        adaptive_poll.observe_signal(cadence.signal);

        if events.is_empty() {
            if adaptive_poll.should_fast_poll() {
                metrics.inc_cadence_fast_repoll();
            } else {
                metrics.inc_cadence_idle_sleep();
                tokio::time::sleep(poll_interval).await;
            }
        } else if !process_input_events(
            &mut input,
            &mut transform,
            &tx,
            &metrics,
            input_index,
            events,
            &mut buffered_since,
        )
        .await
        {
            break 'poll_loop;
        }

        if input.source.is_finished() {
            if !input.buf.is_empty()
                && let Some(msg) =
                    scan_and_transform_for_send(&mut input, &mut transform, &metrics, input_index)
                        .await
            {
                if tx.send(msg).await.is_err() {
                    break;
                }
                metrics.inc_channel_depth();
            }
            break;
        }

        let timeout_elapsed = buffered_since.is_some_and(|t| t.elapsed() >= batch_timeout);
        let should_send = should_flush_buffer(input.buf.len(), batch_target_bytes, timeout_elapsed);
        if should_send {
            if let Some(msg) =
                scan_and_transform_for_send(&mut input, &mut transform, &metrics, input_index).await
            {
                if tx.send(msg).await.is_err() {
                    break;
                }
                metrics.inc_channel_depth();
            }
            buffered_since = None;
        }
    }

    // Drain remaining buffered data.
    if !input.buf.is_empty()
        && let Some(msg) =
            scan_and_transform_for_send(&mut input, &mut transform, &metrics, input_index).await
    {
        if let Err(e) = tx.send(msg).await {
            tracing::warn!(
                input = input.source.name(),
                error = %e,
                "input.channel_closed_on_shutdown_drain"
            );
        } else {
            metrics.inc_channel_depth();
        }
    }
    input.stats.set_health(reduce_component_health(
        input.stats.health(),
        HealthTransitionEvent::ShutdownCompleted,
    ));
}

#[cfg(test)]
mod tests {
    use super::should_flush_buffer;
    use proptest::prelude::*;

    #[test]
    fn never_flushes_empty_buffer() {
        assert!(!should_flush_buffer(0, 1024, false));
        assert!(!should_flush_buffer(0, 1024, true));
    }

    #[test]
    fn flushes_when_target_reached_even_without_timeout() {
        assert!(should_flush_buffer(1024, 1024, false));
        assert!(should_flush_buffer(2048, 1024, false));
    }

    #[test]
    fn flushes_non_empty_buffer_on_timeout() {
        assert!(should_flush_buffer(1, 1024, true));
        assert!(should_flush_buffer(1023, 1024, true));
    }

    proptest! {
        #[test]
        fn flush_decision_matches_policy(
            buffered_len in 0usize..2048,
            batch_target_bytes in 1usize..2048,
            timeout_elapsed in any::<bool>()
        ) {
            let expected = buffered_len >= batch_target_bytes || (buffered_len > 0 && timeout_elapsed);
            prop_assert_eq!(
                should_flush_buffer(buffered_len, batch_target_bytes, timeout_elapsed),
                expected
            );
        }
    }
}

#[cfg(kani)]
mod verification {
    use super::should_flush_buffer;

    #[kani::proof]
    fn verify_empty_buffer_never_flushes() {
        let batch_target_bytes = kani::any::<usize>().max(1);
        let timeout_elapsed = kani::any::<bool>();
        let should_flush = should_flush_buffer(0, batch_target_bytes, timeout_elapsed);
        assert!(!should_flush);
        kani::cover!(
            !should_flush_buffer(0, batch_target_bytes, timeout_elapsed),
            "empty-buffer non-flush path reachable"
        );
    }

    #[kani::proof]
    fn verify_timeout_only_flushes_when_buffered() {
        let buffered_len = kani::any::<usize>();
        let batch_target_bytes = kani::any::<usize>().max(1);
        if buffered_len < batch_target_bytes {
            assert_eq!(
                should_flush_buffer(buffered_len, batch_target_bytes, false),
                false
            );
            assert_eq!(
                should_flush_buffer(buffered_len, batch_target_bytes, true),
                buffered_len > 0
            );
        }
        kani::cover!(
            should_flush_buffer(1, 2, true),
            "timeout flush path reachable"
        );
        kani::cover!(
            !should_flush_buffer(0, 2, true),
            "empty timeout no-flush path reachable"
        );
    }

    #[kani::proof]
    fn verify_flush_predicate_equivalence() {
        let buffered_len = kani::any::<usize>();
        let batch_target_bytes = kani::any::<usize>().max(1);
        let timeout_elapsed = kani::any::<bool>();
        let expected = buffered_len >= batch_target_bytes || (buffered_len > 0 && timeout_elapsed);
        assert_eq!(
            should_flush_buffer(buffered_len, batch_target_bytes, timeout_elapsed),
            expected
        );
        kani::cover!(
            should_flush_buffer(2, 2, false),
            "batch-threshold flush path reachable"
        );
        kani::cover!(
            !should_flush_buffer(0, 2, false),
            "below-threshold non-flush path reachable"
        );
    }
}

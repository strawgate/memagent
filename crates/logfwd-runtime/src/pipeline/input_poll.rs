#[cfg(feature = "turmoil")]
use std::sync::Arc;
#[cfg(feature = "turmoil")]
use std::time::Duration;

#[cfg(feature = "turmoil")]
use arrow::record_batch::RecordBatch;
#[cfg(feature = "turmoil")]
use logfwd_diagnostics::diagnostics::PipelineMetrics;
#[cfg(feature = "turmoil")]
use logfwd_io::input::InputEvent;
#[cfg(feature = "turmoil")]
use tokio_util::sync::CancellationToken;

#[cfg(feature = "turmoil")]
use super::health::{HealthTransitionEvent, reduce_component_health};
#[cfg(feature = "turmoil")]
use super::submit::{scan_and_transform_for_send, transform_direct_batch_for_send};
#[cfg(feature = "turmoil")]
use super::{ChannelMsg, InputState, InputTransform};

/// Async input loop for simulation testing.
///
/// Polls source, accumulates bytes, scans + SQL transforms, and sends
/// `ChannelMsg` — same output type as production CPU workers. Uses
/// `tokio::time::sleep` so Turmoil's simulated time advances deterministically.
#[cfg(feature = "turmoil")]
#[allow(clippy::too_many_arguments)]
pub(super) async fn async_input_poll_loop(
    mut input: InputState,
    mut transform: InputTransform,
    tx: tokio::sync::mpsc::Sender<ChannelMsg>,
    metrics: Arc<PipelineMetrics>,
    shutdown: CancellationToken,
    batch_target_bytes: usize,
    batch_timeout: Duration,
    poll_interval: Duration,
    input_index: usize,
) {
    let mut buffered_since: Option<tokio::time::Instant> = None;
    'poll_loop: loop {
        if shutdown.is_cancelled() {
            input.stats.set_health(reduce_component_health(
                input.stats.health(),
                HealthTransitionEvent::ShutdownRequested,
            ));
            break;
        }

        let events = match input.source.poll() {
            Ok(e) => e,
            Err(e) => {
                input.stats.inc_errors();
                input.stats.set_health(reduce_component_health(
                    input.stats.health(),
                    HealthTransitionEvent::PollFailed,
                ));
                tracing::warn!(input = input.source.name(), error = %e, "input.poll_error");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        input.stats.set_health(reduce_component_health(
            input.stats.health(),
            HealthTransitionEvent::Observed(input.source.health()),
        ));

        if events.is_empty() {
            tokio::time::sleep(poll_interval).await;
        } else {
            for event in events {
                match event {
                    InputEvent::Data { bytes, .. } => {
                        input.buf.extend_from_slice(&bytes);
                    }
                    InputEvent::Batch { batch, .. } => {
                        if !input.buf.is_empty() {
                            if let Some(msg) = scan_and_transform_for_send(
                                &mut input,
                                &mut transform,
                                &metrics,
                                input_index,
                            )
                            .await
                            {
                                if tx.send(msg).await.is_err() {
                                    break 'poll_loop;
                                }
                            }
                            buffered_since = None;
                        }

                        if let Some(msg) = transform_direct_batch_for_send(
                            &mut input,
                            &mut transform,
                            &metrics,
                            input_index,
                            batch,
                        )
                        .await
                        {
                            if tx.send(msg).await.is_err() {
                                break 'poll_loop;
                            }
                        }
                    }
                    InputEvent::Rotated { .. } => {
                        input.stats.inc_rotations();
                    }
                    InputEvent::Truncated { .. } => {
                        // Treat truncation as a rotation-equivalent rewind signal
                        // for existing dashboards that chart a single restart counter.
                        input.stats.inc_rotations();
                    }
                    InputEvent::EndOfFile { .. } => {}
                }
            }
            if buffered_since.is_none() && !input.buf.is_empty() {
                buffered_since = Some(tokio::time::Instant::now());
            }
        }

        let timeout_elapsed = buffered_since.is_some_and(|t| t.elapsed() >= batch_timeout);
        let should_send =
            input.buf.len() >= batch_target_bytes || (!input.buf.is_empty() && timeout_elapsed);
        if should_send {
            if let Some(msg) =
                scan_and_transform_for_send(&mut input, &mut transform, &metrics, input_index).await
            {
                if tx.send(msg).await.is_err() {
                    break;
                }
            }
            buffered_since = None;
        }
    }

    // Drain remaining buffered data.
    if !input.buf.is_empty() {
        if let Some(msg) =
            scan_and_transform_for_send(&mut input, &mut transform, &metrics, input_index).await
        {
            if let Err(e) = tx.send(msg).await {
                tracing::warn!(
                    input = input.source.name(),
                    error = %e,
                    "input.channel_closed_on_shutdown_drain"
                );
            }
        }
    }
    input.stats.set_health(reduce_component_health(
        input.stats.health(),
        HealthTransitionEvent::ShutdownCompleted,
    ));
}

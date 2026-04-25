//! Pure diagnostics policy helpers.
//!
//! This module keeps readiness and health roll-up logic out of the HTTP shell
//! so the diagnostics server stays a thin transport/view layer.

use std::sync::Arc;

use ffwd_types::diagnostics::ComponentHealth;

use super::PipelineMetrics;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HealthReasonTag {
    AllHealthy,
    DegradedButOperational,
    Starting,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadinessImpactTag {
    Ready,
    NonBlocking,
    Gating,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadyReasonTag {
    NoPipelines,
    Health(HealthReasonTag),
}

/// Snapshot of readiness state derived from the current pipeline health view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ReadinessSnapshot {
    pub ready: bool,
    pub reason: &'static str,
    pub component_health: ComponentHealth,
}

/// Roll component health up across all registered pipelines and component
/// roles.
pub(super) fn aggregate_component_health(pipelines: &[Arc<PipelineMetrics>]) -> ComponentHealth {
    pipelines
        .iter()
        .flat_map(|pm| {
            pm.inputs
                .iter()
                .map(|(_, _, stats)| stats.health())
                .chain([pm.transform_in.health(), pm.transform_out.health()])
                .chain(pm.outputs.iter().map(|(_, _, stats)| stats.health()))
        })
        .fold(ComponentHealth::Healthy, ComponentHealth::combine)
}

/// Human-readable machine-stable label for a [`HealthReasonTag`].
///
/// Single source of truth for the tag-to-label mapping, used by both
/// [`health_reason`] and [`ready_reason_label`].
const fn health_reason_tag_label(tag: HealthReasonTag) -> &'static str {
    match tag {
        HealthReasonTag::AllHealthy => "all_components_healthy",
        HealthReasonTag::DegradedButOperational => "components_degraded_but_operational",
        HealthReasonTag::Starting => "components_starting",
        HealthReasonTag::Stopping => "components_stopping",
        HealthReasonTag::Stopped => "components_stopped",
        HealthReasonTag::Failed => "components_failed",
    }
}

/// Human-readable machine-stable reason for an aggregated component state.
pub(super) const fn health_reason(health: ComponentHealth) -> &'static str {
    health_reason_tag_label(health_reason_tag(health))
}

/// Whether the given health state blocks readiness.
pub(super) const fn readiness_impact(health: ComponentHealth) -> &'static str {
    match readiness_impact_tag(health) {
        ReadinessImpactTag::Ready => "ready",
        ReadinessImpactTag::NonBlocking => "non_blocking",
        ReadinessImpactTag::Gating => "gating",
    }
}

/// Return readiness and reason from one shared aggregate snapshot.
///
/// The returned [`ReadinessSnapshot`] includes the aggregated
/// [`ComponentHealth`] so that callers can reuse it without a second
/// traversal (avoiding a TOCTOU window if health changes between calls).
pub(super) fn readiness_snapshot(pipelines: &[Arc<PipelineMetrics>]) -> ReadinessSnapshot {
    let has_pipelines = !pipelines.is_empty();
    let aggregate = aggregate_component_health(pipelines);
    let (ready, reason_tag) = readiness_snapshot_from_state(has_pipelines, aggregate);
    ReadinessSnapshot {
        ready,
        reason: ready_reason_label(reason_tag),
        component_health: aggregate,
    }
}

const fn health_reason_tag(health: ComponentHealth) -> HealthReasonTag {
    match health {
        ComponentHealth::Healthy => HealthReasonTag::AllHealthy,
        ComponentHealth::Degraded => HealthReasonTag::DegradedButOperational,
        ComponentHealth::Starting => HealthReasonTag::Starting,
        ComponentHealth::Stopping => HealthReasonTag::Stopping,
        ComponentHealth::Stopped => HealthReasonTag::Stopped,
        ComponentHealth::Failed => HealthReasonTag::Failed,
    }
}

const fn readiness_impact_tag(health: ComponentHealth) -> ReadinessImpactTag {
    match health {
        ComponentHealth::Healthy => ReadinessImpactTag::Ready,
        ComponentHealth::Degraded => ReadinessImpactTag::NonBlocking,
        ComponentHealth::Starting
        | ComponentHealth::Stopping
        | ComponentHealth::Stopped
        | ComponentHealth::Failed => ReadinessImpactTag::Gating,
    }
}

const fn ready_reason_tag(has_pipelines: bool, aggregate: ComponentHealth) -> ReadyReasonTag {
    if has_pipelines {
        ReadyReasonTag::Health(health_reason_tag(aggregate))
    } else {
        ReadyReasonTag::NoPipelines
    }
}

const fn ready_reason_label(reason: ReadyReasonTag) -> &'static str {
    match reason {
        ReadyReasonTag::NoPipelines => "no_pipelines_registered",
        ReadyReasonTag::Health(tag) => health_reason_tag_label(tag),
    }
}

const fn readiness_snapshot_from_state(
    has_pipelines: bool,
    aggregate: ComponentHealth,
) -> (bool, ReadyReasonTag) {
    (
        has_pipelines
            && matches!(
                readiness_impact_tag(aggregate),
                ReadinessImpactTag::Ready | ReadinessImpactTag::NonBlocking
            ),
        ready_reason_tag(has_pipelines, aggregate),
    )
}

/// Return the health view exposed for the transform section in `/admin/v1/status`.
pub(super) fn transform_health(pipeline: &PipelineMetrics) -> ComponentHealth {
    pipeline
        .transform_in
        .health()
        .combine(pipeline.transform_out.health())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn pipeline_with_io() -> PipelineMetrics {
        let meter = opentelemetry::global::meter("policy-test");
        let mut pm = PipelineMetrics::new("pipe", "SELECT * FROM logs", &meter);
        let input = pm.add_input("input", "otlp");
        pm.add_output("output", "otlp");
        input.set_health(ComponentHealth::Healthy);
        pm
    }

    #[test]
    fn ready_requires_pipelines_and_ready_components() {
        assert!(!readiness_snapshot(&[]).ready);

        let pm = pipeline_with_io();
        assert!(readiness_snapshot(&[Arc::new(pm)]).ready);
    }

    #[test]
    fn ready_fails_when_any_component_is_starting() {
        let pm = pipeline_with_io();
        pm.inputs[0].2.set_health(ComponentHealth::Starting);

        assert!(!readiness_snapshot(&[Arc::new(pm)]).ready);
    }

    #[test]
    fn aggregate_component_health_uses_worst_component() {
        let pm = pipeline_with_io();
        pm.inputs[0].2.set_health(ComponentHealth::Healthy);
        pm.outputs[0].2.set_health(ComponentHealth::Degraded);

        assert_eq!(
            aggregate_component_health(&[Arc::new(pm)]),
            ComponentHealth::Degraded
        );
    }

    #[test]
    fn transform_health_combines_input_and_output_views() {
        let pm = pipeline_with_io();
        pm.transform_in.set_health(ComponentHealth::Healthy);
        pm.transform_out.set_health(ComponentHealth::Stopping);

        assert_eq!(transform_health(&pm), ComponentHealth::Stopping);
    }

    #[test]
    fn ready_reason_tracks_empty_and_degraded_states() {
        assert_eq!(readiness_snapshot(&[]).reason, "no_pipelines_registered");

        let pm = pipeline_with_io();
        pm.outputs[0].2.set_health(ComponentHealth::Degraded);
        assert_eq!(
            readiness_snapshot(&[Arc::new(pm)]).reason,
            "components_degraded_but_operational"
        );
    }

    #[test]
    fn readiness_snapshot_is_consistent_for_empty_and_degraded_states() {
        let empty = readiness_snapshot(&[]);
        assert!(!empty.ready);
        assert_eq!(empty.reason, "no_pipelines_registered");

        let pm = pipeline_with_io();
        pm.outputs[0].2.set_health(ComponentHealth::Degraded);
        let degraded = readiness_snapshot(&[Arc::new(pm)]);
        assert!(degraded.ready);
        assert_eq!(degraded.reason, "components_degraded_but_operational");
    }

    fn arb_health() -> impl Strategy<Value = ComponentHealth> {
        prop_oneof![
            Just(ComponentHealth::Starting),
            Just(ComponentHealth::Healthy),
            Just(ComponentHealth::Degraded),
            Just(ComponentHealth::Stopping),
            Just(ComponentHealth::Stopped),
            Just(ComponentHealth::Failed),
        ]
    }

    proptest! {
        #[test]
        fn readiness_impact_only_marks_ready_states_as_non_gating(
            health in arb_health()
        ) {
            let impact = readiness_impact(health);
            if health.is_ready() {
                prop_assert!(matches!(impact, "ready" | "non_blocking"));
            } else {
                prop_assert_eq!(impact, "gating");
            }
        }

        #[test]
        fn readiness_snapshot_ready_matches_component_readiness_when_pipelines_exist(
            health in arb_health()
        ) {
            let (ready, _reason) = readiness_snapshot_from_state(true, health);
            prop_assert_eq!(ready, health.is_ready());
        }
    }
}

#[cfg(kani)]
mod verification {
    use super::{
        HealthReasonTag, ReadinessImpactTag, ReadyReasonTag, health_reason_tag,
        readiness_impact_tag, readiness_snapshot_from_state,
    };
    use ffwd_types::diagnostics::ComponentHealth;

    #[kani::proof]
    fn verify_readiness_impact_matches_component_readiness() {
        let health = ComponentHealth::from_repr(kani::any());
        let impact = readiness_impact_tag(health);

        if health.is_ready() {
            assert!(matches!(
                impact,
                ReadinessImpactTag::Ready | ReadinessImpactTag::NonBlocking
            ));
        } else {
            assert_eq!(impact, ReadinessImpactTag::Gating);
        }

        kani::cover!(health == ComponentHealth::Healthy, "healthy_is_ready");
        kani::cover!(
            health == ComponentHealth::Degraded,
            "degraded_is_non_blocking"
        );
        kani::cover!(health == ComponentHealth::Starting, "starting_is_gating");
    }

    #[kani::proof]
    fn verify_health_reason_labels_every_variant() {
        let health = ComponentHealth::from_repr(kani::any());
        let reason = health_reason_tag(health);

        match health {
            ComponentHealth::Healthy => assert_eq!(reason, HealthReasonTag::AllHealthy),
            ComponentHealth::Degraded => {
                assert_eq!(reason, HealthReasonTag::DegradedButOperational)
            }
            ComponentHealth::Starting => assert_eq!(reason, HealthReasonTag::Starting),
            ComponentHealth::Stopping => assert_eq!(reason, HealthReasonTag::Stopping),
            ComponentHealth::Stopped => assert_eq!(reason, HealthReasonTag::Stopped),
            ComponentHealth::Failed => assert_eq!(reason, HealthReasonTag::Failed),
        }

        kani::cover!(health == ComponentHealth::Failed, "failed_reason_labelled");
        kani::cover!(
            health == ComponentHealth::Stopped,
            "stopped_reason_labelled"
        );
    }

    #[kani::proof]
    fn verify_readiness_snapshot_requires_pipelines() {
        let aggregate = ComponentHealth::from_repr(kani::any());
        let (ready, reason) = readiness_snapshot_from_state(false, aggregate);
        assert!(!ready);
        assert_eq!(reason, ReadyReasonTag::NoPipelines);

        kani::cover!(
            aggregate == ComponentHealth::Healthy,
            "healthy_without_pipelines_not_ready"
        );
    }

    #[kani::proof]
    fn verify_readiness_snapshot_reason_matches_health_when_pipelines_exist() {
        let aggregate = ComponentHealth::from_repr(kani::any());
        let (_ready, reason) = readiness_snapshot_from_state(true, aggregate);
        assert_eq!(reason, ReadyReasonTag::Health(health_reason_tag(aggregate)));

        kani::cover!(
            aggregate == ComponentHealth::Degraded,
            "degraded_reason_with_pipelines"
        );
        kani::cover!(
            aggregate == ComponentHealth::Failed,
            "failed_reason_with_pipelines"
        );
    }
}

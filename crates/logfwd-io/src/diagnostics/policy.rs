//! Pure diagnostics policy helpers.
//!
//! This module keeps readiness and health roll-up logic out of the HTTP shell
//! so the diagnostics server stays a thin transport/view layer.

use std::sync::Arc;

use logfwd_types::diagnostics::ComponentHealth;

use super::PipelineMetrics;

/// Return `true` when the diagnostics server should report ready.
pub(super) fn is_ready(pipelines: &[Arc<PipelineMetrics>]) -> bool {
    !pipelines.is_empty() && pipelines.iter().all(|pm| pipeline_is_ready(pm))
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

/// Human-readable machine-stable reason for an aggregated component state.
pub(super) const fn health_reason(health: ComponentHealth) -> &'static str {
    match health {
        ComponentHealth::Healthy => "all_components_healthy",
        ComponentHealth::Degraded => "components_degraded_but_operational",
        ComponentHealth::Starting => "components_starting",
        ComponentHealth::Stopping => "components_stopping",
        ComponentHealth::Stopped => "components_stopped",
        ComponentHealth::Failed => "components_failed",
    }
}

/// Whether the given health state blocks readiness.
pub(super) const fn readiness_impact(health: ComponentHealth) -> &'static str {
    match health {
        ComponentHealth::Healthy => "ready",
        ComponentHealth::Degraded => "non_blocking",
        ComponentHealth::Starting
        | ComponentHealth::Stopping
        | ComponentHealth::Stopped
        | ComponentHealth::Failed => "gating",
    }
}

/// Machine-stable reason for the top-level readiness snapshot.
pub(super) fn ready_reason(pipelines: &[Arc<PipelineMetrics>]) -> &'static str {
    if pipelines.is_empty() {
        return "no_pipelines_registered";
    }
    health_reason(aggregate_component_health(pipelines))
}

/// Return the health view exposed for the transform section in `/admin/v1/status`.
pub(super) fn transform_health(pipeline: &PipelineMetrics) -> ComponentHealth {
    pipeline
        .transform_in
        .health()
        .combine(pipeline.transform_out.health())
}

fn pipeline_is_ready(pipeline: &PipelineMetrics) -> bool {
    pipeline.transform_in.health().is_ready()
        && pipeline.transform_out.health().is_ready()
        && pipeline
            .inputs
            .iter()
            .all(|(_, _, stats)| stats.health().is_ready())
        && pipeline
            .outputs
            .iter()
            .all(|(_, _, stats)| stats.health().is_ready())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(!is_ready(&[]));

        let pm = pipeline_with_io();
        assert!(is_ready(&[Arc::new(pm)]));
    }

    #[test]
    fn ready_fails_when_any_component_is_starting() {
        let pm = pipeline_with_io();
        pm.inputs[0].2.set_health(ComponentHealth::Starting);

        assert!(!is_ready(&[Arc::new(pm)]));
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
        assert_eq!(ready_reason(&[]), "no_pipelines_registered");

        let pm = pipeline_with_io();
        pm.outputs[0].2.set_health(ComponentHealth::Degraded);
        assert_eq!(
            ready_reason(&[Arc::new(pm)]),
            "components_degraded_but_operational"
        );
    }
}

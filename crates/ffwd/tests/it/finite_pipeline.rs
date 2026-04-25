use ffwd_config::Config;
use ffwd_runtime::pipeline::Pipeline;
use opentelemetry::metrics::MeterProvider;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn finite_pipeline_shuts_down_automatically() {
    let yaml = r#"
pipelines:
  test:
    inputs:
      - type: generator
        generator:
          total_events: 5
          batch_size: 2
    outputs:
      - type: "null"
"#;
    let config = Config::load_str(yaml).expect("parse config");

    let meter = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .build()
        .meter("test");
    let mut pipeline =
        Pipeline::from_config_with_data_dir("test", &config.pipelines["test"], &meter, None, None)
            .unwrap();

    let shutdown = CancellationToken::new();

    // We expect the pipeline to return Ok(()) naturally, without us cancelling the token.
    // Wrap in timeout to fail if it hangs.
    let result = tokio::time::timeout(Duration::from_secs(30), pipeline.run_async(&shutdown)).await;

    assert!(
        result.is_ok(),
        "Pipeline did not shut down automatically within timeout"
    );
    let result = result.unwrap();
    assert!(result.is_ok(), "Pipeline failed with error: {:?}", result);
}

Vendored OpenTelemetry protobuf definitions used by OTLP projection codegen.

Source: https://github.com/open-telemetry/opentelemetry-proto/tree/v1.8.0

Only the logs projection subset is vendored:

- `opentelemetry/proto/collector/logs/v1/logs_service.proto`
- `opentelemetry/proto/logs/v1/logs.proto`
- `opentelemetry/proto/resource/v1/resource.proto`
- `opentelemetry/proto/common/v1/common.proto`

These files are licensed by the OpenTelemetry Authors under the Apache License,
Version 2.0, as stated in each vendored `.proto` file header.

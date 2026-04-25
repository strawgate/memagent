# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-03-30

### Added

- Initial release of the `ffwd` log forwarder.
- File tail reader with inotify-based change detection.
- UDP and TCP syslog/OTLP source support.
- CRI (Container Runtime Interface) log format parser.
- Apache Arrow RecordBatch pipeline with zero-copy string views.
- SQL transform layer powered by Apache DataFusion.
- Output sinks: OTLP, Elasticsearch, Loki, Parquet, JSON lines, stdout.
- zstd-compressed Arrow IPC disk queue for backpressure.
- OpenTelemetry metrics endpoint for diagnostics.
- YAML-based configuration (`ffwd-config`).

[Unreleased]: https://github.com/strawgate/fastforward/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/strawgate/fastforward/releases/tag/v0.1.0

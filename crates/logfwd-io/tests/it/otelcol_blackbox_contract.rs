//! Black-box OTLP contract tests against the official OpenTelemetry Collector.
//!
//! These tests treat `otelcol-contrib` as an external oracle: our sink sends
//! OTLP/protobuf over HTTP to a real Collector receiver, and we compare the
//! Collector's own exported OTLP JSON against an independently constructed
//! expected semantic row.

use std::env;
use std::fs::{self, File};
use std::io::{self, Read as _};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use flate2::read::GzDecoder;
use logfwd_io::input::{InputEvent, InputSource};
use logfwd_io::otlp_receiver::OtlpReceiverInput;
use logfwd_output::{BatchMetadata, Compression, OtlpProtocol, OtlpSink, SendResult, Sink};
use logfwd_types::diagnostics::ComponentStats;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use serial_test::serial;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use crate::otlp_contract_support::{
    emitted_single_row_from_otlp_json, expected_single_row_from_request,
};

const OTELCOL_VERSION: &str = "0.148.0";
const OTELCOL_BINARY_NAME: &str = "otelcol-contrib";

struct OtelcolProcess {
    child: Child,
    temp_dir: TempDir,
    output_path: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

impl Drop for OtelcolProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl OtelcolProcess {
    fn diagnostics(&self) -> String {
        let stdout = fs::read_to_string(&self.stdout_path).unwrap_or_default();
        let stderr = fs::read_to_string(&self.stderr_path).unwrap_or_default();
        format!(
            "stdout:\n{}\n\nstderr:\n{}",
            stdout.trim_end(),
            stderr.trim_end()
        )
    }
}

fn allocate_local_listener() -> io::Result<TcpListener> {
    TcpListener::bind("127.0.0.1:0")
}

fn platform() -> (&'static str, &'static str) {
    let os = match env::consts::OS {
        "macos" => "darwin",
        other => other,
    };
    let arch = match env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" | "arm64" => "arm64",
        other => other,
    };
    (os, arch)
}

fn otelcol_download_url() -> String {
    let (os, arch) = platform();
    format!(
        "https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v{OTELCOL_VERSION}/otelcol-contrib_{OTELCOL_VERSION}_{os}_{arch}.tar.gz"
    )
}

fn otelcol_archive_name() -> String {
    let (os, arch) = platform();
    format!("otelcol-contrib_{OTELCOL_VERSION}_{os}_{arch}.tar.gz")
}

fn otelcol_checksums_url() -> String {
    format!(
        "https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v{OTELCOL_VERSION}/opentelemetry-collector-releases_otelcol-contrib_checksums.txt"
    )
}

fn cached_otelcol_binary_path() -> PathBuf {
    env::temp_dir()
        .join("memagent-otelcol-cache")
        .join(OTELCOL_VERSION)
        .join(OTELCOL_BINARY_NAME)
}

fn cached_otelcol_archive_path() -> PathBuf {
    env::temp_dir()
        .join("memagent-otelcol-cache")
        .join(OTELCOL_VERSION)
        .join(otelcol_archive_name())
}

fn fetch_expected_otelcol_archive_sha256() -> io::Result<String> {
    let response = ureq::get(&otelcol_checksums_url())
        .call()
        .map_err(|err| io::Error::other(format!("failed to download otelcol checksums: {err}")))?;
    let mut checksums = String::new();
    response
        .into_body()
        .into_reader()
        .read_to_string(&mut checksums)?;

    let archive_name = otelcol_archive_name();
    for line in checksums.lines() {
        let mut parts = line.split_whitespace();
        let Some(sha) = parts.next() else {
            continue;
        };
        let Some(name) = parts.next() else {
            continue;
        };
        if name == archive_name {
            return Ok(sha.to_string());
        }
    }

    Err(io::Error::other(format!(
        "checksum manifest did not contain {archive_name}"
    )))
}

fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 16 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn verify_archive_checksum(path: &Path, expected_sha256: &str) -> io::Result<()> {
    let actual = sha256_file(path)?;
    if actual == expected_sha256 {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "checksum mismatch for {}: expected {expected_sha256}, got {actual}",
            path.display()
        )))
    }
}

fn ensure_otelcol_binary() -> io::Result<PathBuf> {
    if let Some(path) = env::var_os("OTELCOL_BIN") {
        return Ok(PathBuf::from(path));
    }

    let expected_sha256 = fetch_expected_otelcol_archive_sha256()?;
    let archive = cached_otelcol_archive_path();
    let binary = cached_otelcol_binary_path();

    let parent = archive
        .parent()
        .ok_or_else(|| io::Error::other("otelcol cache path has no parent"))?;
    fs::create_dir_all(parent)?;

    if !archive.exists() {
        let url = otelcol_download_url();
        eprintln!("downloading official otelcol oracle from {url}");
        let response = ureq::get(&url)
            .call()
            .map_err(|err| io::Error::other(format!("failed to download otelcol: {err}")))?;
        let tmp_archive = archive.with_extension("tmp");
        let mut reader = response.into_body().into_reader();
        let mut file = File::create(&tmp_archive)?;
        io::copy(&mut reader, &mut file)?;
        verify_archive_checksum(&tmp_archive, &expected_sha256)?;
        fs::rename(&tmp_archive, &archive)?;
    }

    verify_archive_checksum(&archive, &expected_sha256)?;

    let file = File::open(&archive)?;
    let gzip = GzDecoder::new(file);
    let mut archive = tar::Archive::new(gzip);
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;
        if path
            .file_name()
            .is_some_and(|name| name == OTELCOL_BINARY_NAME)
        {
            let mut file = File::create(&binary)?;
            io::copy(&mut entry, &mut file)?;

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&binary, fs::Permissions::from_mode(0o755))?;
            }

            return Ok(binary);
        }
    }

    Err(io::Error::other(
        "downloaded otelcol archive did not contain otelcol-contrib",
    ))
}

fn wait_for_port(addr: &str, timeout: Duration) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(addr).is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!("timed out waiting for otelcol to listen on {addr}"),
    ))
}

fn wait_for_export_json(path: &Path, timeout: Duration) -> io::Result<serde_json::Value> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        match fs::read_to_string(path) {
            Ok(contents) if !contents.trim().is_empty() => {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&contents) {
                    return Ok(json);
                }
            }
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "timed out waiting for otelcol file exporter output at {}",
            path.display()
        ),
    ))
}

fn yaml_single_quoted(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn start_otelcol() -> io::Result<(OtelcolProcess, String)> {
    let binary = ensure_otelcol_binary()?;
    let mut last_error: Option<io::Error> = None;

    for _attempt in 0..3 {
        let temp_dir = tempfile::tempdir()?;
        let listener = allocate_local_listener()?;
        let receiver_addr = listener.local_addr()?.to_string();
        let output_path = temp_dir.path().join("collector-output.json");
        let stdout_path = temp_dir.path().join("collector-stdout.log");
        let stderr_path = temp_dir.path().join("collector-stderr.log");
        let config_path = temp_dir.path().join("otelcol.yaml");

        let output_path_yaml = yaml_single_quoted(&output_path.to_string_lossy());
        let config = format!(
            r#"receivers:
  otlp:
    protocols:
      http:
        endpoint: {receiver_addr}

exporters:
  file:
    path: {output_path_yaml}

service:
  telemetry:
    logs:
      level: debug
    metrics:
      level: none
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [file]
"#,
            output_path_yaml = output_path_yaml,
        );
        fs::write(&config_path, config)?;

        let stdout = File::create(&stdout_path)?;
        let stderr = File::create(&stderr_path)?;
        drop(listener);

        let child = Command::new(&binary)
            .arg("--config")
            .arg(&config_path)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()?;

        let process = OtelcolProcess {
            child,
            temp_dir,
            output_path,
            stdout_path,
            stderr_path,
        };

        match wait_for_port(&receiver_addr, Duration::from_secs(30)) {
            Ok(()) => return Ok((process, format!("http://{receiver_addr}/v1/logs"))),
            Err(err) => {
                let diagnostics = process.diagnostics();
                let bind_race = diagnostics.contains("address already in use");
                last_error = Some(io::Error::other(format!(
                    "{err}; collector diagnostics:\n{}",
                    diagnostics
                )));
                drop(process);
                if bind_race {
                    continue;
                }
                break;
            }
        }
    }

    Err(last_error.unwrap_or_else(|| io::Error::other("failed to start otelcol")))
}

fn start_otelcol_filelog_to_receiver(
    log_path: &Path,
    receiver_base_url: &str,
) -> io::Result<OtelcolProcess> {
    let binary = ensure_otelcol_binary()?;
    let temp_dir = tempfile::tempdir()?;
    let output_path = temp_dir.path().join("collector-output.json");
    let stdout_path = temp_dir.path().join("collector-stdout.log");
    let stderr_path = temp_dir.path().join("collector-stderr.log");
    let config_path = temp_dir.path().join("otelcol.yaml");

    let log_path_yaml = yaml_single_quoted(&log_path.to_string_lossy());
    let output_path_yaml = yaml_single_quoted(&output_path.to_string_lossy());
    let receiver_base_url_yaml = yaml_single_quoted(receiver_base_url);
    let config = format!(
        r#"receivers:
  filelog:
    include:
      - {log_path_yaml}
    start_at: beginning
    operators:
      - type: json_parser
        timestamp:
          parse_from: attributes.timestamp
          layout: '%Y-%m-%dT%H:%M:%SZ'
        severity:
          parse_from: attributes.level

exporters:
  file:
    path: {output_path_yaml}
  otlphttp:
    endpoint: {receiver_base_url_yaml}
    compression: none
    tls:
      insecure: true

service:
  telemetry:
    logs:
      level: debug
    metrics:
      level: none
  pipelines:
    logs:
      receivers: [filelog]
      exporters: [file, otlphttp]
"#,
        log_path_yaml = log_path_yaml,
        output_path_yaml = output_path_yaml,
        receiver_base_url_yaml = receiver_base_url_yaml,
    );
    fs::write(&config_path, config)?;

    let stdout = File::create(&stdout_path)?;
    let stderr = File::create(&stderr_path)?;
    let child = Command::new(binary)
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()?;

    Ok(OtelcolProcess {
        child,
        temp_dir,
        output_path,
        stdout_path,
        stderr_path,
    })
}

fn build_batch_and_expected_request() -> (RecordBatch, BatchMetadata, ExportLogsServiceRequest) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Utf8, true),
        Field::new("level", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("request_count", DataType::Int64, true),
        Field::new("latency_ratio", DataType::Float64, true),
        Field::new("sampled", DataType::Boolean, true),
        Field::new("host", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["2024-01-15T10:30:00Z"])),
            Arc::new(StringArray::from(vec!["ERROR"])),
            Arc::new(StringArray::from(vec!["disk full"])),
            Arc::new(StringArray::from(vec!["0102030405060708090a0b0c0d0e0f10"])),
            Arc::new(StringArray::from(vec!["0102030405060708"])),
            Arc::new(Int64Array::from(vec![7i64])),
            Arc::new(Float64Array::from(vec![3.5f64])),
            Arc::new(BooleanArray::from(vec![Some(true)])),
            Arc::new(StringArray::from(vec!["web-01"])),
        ],
    )
    .expect("valid oracle batch");

    let metadata = BatchMetadata {
        resource_attrs: Arc::new(vec![(
            "service.name".to_string(),
            "checkout-api".to_string(),
        )]),
        observed_time_ns: 1_705_314_601_000_000_000,
    };

    let expected = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("checkout-api".into())),
                    }),
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_705_314_600_000_000_000,
                    severity_text: "ERROR".into(),
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("disk full".into())),
                    }),
                    attributes: vec![
                        KeyValue {
                            key: "request_count".into(),
                            value: Some(AnyValue {
                                value: Some(Value::IntValue(7)),
                            }),
                        },
                        KeyValue {
                            key: "latency_ratio".into(),
                            value: Some(AnyValue {
                                value: Some(Value::DoubleValue(3.5)),
                            }),
                        },
                        KeyValue {
                            key: "sampled".into(),
                            value: Some(AnyValue {
                                value: Some(Value::BoolValue(true)),
                            }),
                        },
                        KeyValue {
                            key: "host".into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("web-01".into())),
                            }),
                        },
                    ],
                    trace_id: vec![
                        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
                        0x0d, 0x0e, 0x0f, 0x10,
                    ],
                    span_id: vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    (batch, metadata, expected)
}

fn poll_single_row(input: &mut dyn InputSource, timeout: Duration) -> serde_json::Value {
    let deadline = Instant::now() + timeout;
    let mut buf = Vec::new();

    while Instant::now() < deadline {
        for event in input.poll().expect("poll receiver") {
            if let InputEvent::Data { bytes, .. } = event {
                buf.extend_from_slice(&bytes);
            }
        }

        if !buf.is_empty() {
            let line = String::from_utf8(buf)
                .expect("receiver emits utf8 json")
                .lines()
                .next()
                .expect("one decoded row")
                .to_string();
            return serde_json::from_str(&line)
                .unwrap_or_else(|err| panic!("valid json row: {err}; line={line}"));
        }

        std::thread::sleep(Duration::from_millis(20));
    }

    panic!("timed out waiting for OTLP receiver data");
}

#[test]
#[ignore = "downloads and runs official otelcol-contrib as an external oracle"]
#[serial]
fn otlp_sink_matches_official_otelcol_oracle() {
    let (collector, endpoint) = start_otelcol().expect("start official otelcol oracle");
    let (batch, metadata, expected_request) = build_batch_and_expected_request();

    let mut sink = OtlpSink::new(
        "oracle-otlp".into(),
        endpoint,
        OtlpProtocol::Http,
        Compression::None,
        vec![],
        reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("build reqwest client"),
        Arc::new(ComponentStats::new()),
    )
    .expect("create OTLP sink");

    let runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
    let result = runtime.block_on(async { sink.send_batch(&batch, &metadata).await });
    match result {
        SendResult::Ok => {}
        other => panic!(
            "collector oracle rejected batch: {other:?}\n{}",
            collector.diagnostics()
        ),
    }

    let exported = wait_for_export_json(&collector.output_path, Duration::from_secs(15))
        .unwrap_or_else(|err| {
            panic!(
                "collector oracle did not write export JSON: {err}\n{}",
                collector.diagnostics()
            )
        });

    let exported_row = emitted_single_row_from_otlp_json(&exported);
    let expected_row = expected_single_row_from_request(&expected_request);
    assert_eq!(
        exported_row, expected_row,
        "official collector observed different OTLP semantics than expected"
    );

    assert!(
        collector.temp_dir.path().join("otelcol.yaml").exists(),
        "collector temp dir should stay alive for diagnostics during the test"
    );
}

#[test]
#[ignore = "downloads and runs official otelcol-contrib as an external oracle"]
#[serial]
fn otlp_receiver_matches_official_otelcol_sender_oracle() {
    let log_dir = tempfile::tempdir().expect("create sender temp dir");
    let log_path = log_dir.path().join("app.log");
    let log_line = serde_json::json!({
        "timestamp": "2024-01-15T10:30:00Z",
        "level": "ERROR",
        "message": "disk full",
        "request_count": 7,
        "latency_ratio": 3.5,
        "sampled": true,
        "service.name": "checkout-api"
    });
    fs::write(&log_path, format!("{log_line}\n")).expect("write filelog input");

    let mut receiver =
        OtlpReceiverInput::new("otelcol-sender-oracle", "127.0.0.1:0").expect("start receiver");
    let receiver_base_url = format!("http://{}", receiver.local_addr());

    let collector = start_otelcol_filelog_to_receiver(&log_path, &receiver_base_url)
        .expect("start official otelcol sender oracle");

    let exported = wait_for_export_json(&collector.output_path, Duration::from_secs(15))
        .unwrap_or_else(|err| {
            panic!(
                "collector sender oracle did not write export JSON: {err}\n{}",
                collector.diagnostics()
            )
        });
    let oracle_row = emitted_single_row_from_otlp_json(&exported);
    let receiver_row = poll_single_row(&mut receiver, Duration::from_secs(15));

    assert_eq!(
        receiver_row, oracle_row,
        "our OTLP receiver must match the official collector's emitted semantics"
    );

    assert!(
        collector.temp_dir.path().join("otelcol.yaml").exists(),
        "collector temp dir should stay alive for diagnostics during the test"
    );
}

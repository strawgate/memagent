//! Integration tests for the S3 input's parallel fetch/delivery pipeline.
//!
//! Uses mockito to serve fake S3 responses (ListObjectsV2, HEAD, GET with Range)
//! so tests are fast, deterministic, and don't require MinIO or real AWS.

#![cfg(feature = "s3")]

use std::time::Duration;

use logfwd_io::input::{InputEvent, InputSource};
use logfwd_io::s3_input::{S3Input, S3InputSettings};

/// Helper: create an `S3Input` pointed at `endpoint` using list-mode discovery.
fn make_s3_input(endpoint: &str, bucket: &str, prefix: &str) -> S3Input {
    let settings = S3InputSettings {
        bucket: bucket.to_string(),
        region: "us-east-1".to_string(),
        endpoint: Some(endpoint.to_string()),
        prefix: Some(prefix.to_string()),
        sqs_queue_url: None,
        start_after: None,
        access_key_id: "test-key".to_string(),
        secret_access_key: "test-secret".to_string(),
        session_token: None,
        part_size_bytes: 256, // Tiny: force many parts for testing
        max_concurrent_fetches: 2,
        max_concurrent_objects: 1,
        visibility_timeout_secs: 300,
        compression_override: None,
        poll_interval_ms: 100,
        should_expose_source_paths: false,
    };
    S3Input::new("s3-test", settings).expect("S3Input::new")
}

/// Helper: poll until we have at least `expected_bytes` of data or timeout.
fn poll_until_bytes(
    input: &mut S3Input,
    expected_bytes: usize,
    timeout: Duration,
) -> (Vec<u8>, bool) {
    let deadline = std::time::Instant::now() + timeout;
    let mut backoff = Duration::from_millis(5);
    let max_backoff = Duration::from_millis(100);
    let mut all_bytes = Vec::new();
    let mut got_eof = false;

    while std::time::Instant::now() < deadline {
        if let Ok(events) = input.poll() {
            for event in events {
                match event {
                    InputEvent::Data { bytes, .. } => {
                        all_bytes.extend_from_slice(&bytes);
                    }
                    InputEvent::EndOfFile { .. } => {
                        got_eof = true;
                    }
                    _ => {}
                }
            }
        }
        if all_bytes.len() >= expected_bytes && got_eof {
            return (all_bytes, got_eof);
        }
        std::thread::sleep(backoff);
        backoff = (backoff * 2).min(max_backoff);
    }
    (all_bytes, got_eof)
}

/// Build a minimal `ListObjectsV2` XML response with one object.
fn list_xml(key: &str, size: u64) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name>
  <Prefix></Prefix>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>{key}</Key>
    <Size>{size}</Size>
  </Contents>
</ListBucketResult>"#
    )
}

/// Build an empty `ListObjectsV2` response (no objects).
fn list_xml_empty() -> String {
    r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name>
  <Prefix></Prefix>
  <IsTruncated>false</IsTruncated>
</ListBucketResult>"#
        .to_string()
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// Parallel fetch delivers all data for an object that spans many small parts.
///
/// This is the test that would have caught the semaphore priority-inversion
/// deadlock: with part_size=256 and a 2 KiB object, we get 8 parts — enough
/// to exceed max_concurrent_fetches=2 and exercise the spawn-on-drain path.
#[test]
fn parallel_fetch_delivers_all_data_for_multi_part_object() {
    let data = "A".repeat(2048); // 2 KiB = 8 parts at 256 B part_size
    let data_len = data.len();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut server = mockito::Server::new_async().await;
        let url = server.url();

        // 1. ListObjectsV2 → one object
        server
            .mock("GET", "/test-bucket")
            .match_query(mockito::Matcher::AllOf(vec![mockito::Matcher::UrlEncoded(
                "list-type".into(),
                "2".into(),
            )]))
            .with_status(200)
            .with_body(list_xml("test-prefix/data.json", data_len as u64))
            .create_async()
            .await;

        // After first list, return empty for subsequent polls
        server
            .mock("GET", "/test-bucket")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("list-type".into(), "2".into()),
                mockito::Matcher::UrlEncoded("start-after".into(), "test-prefix/data.json".into()),
            ]))
            .with_status(200)
            .with_body(list_xml_empty())
            .expect_at_most(100)
            .create_async()
            .await;

        // 2. HEAD → content-length, no content-encoding (uncompressed)
        server
            .mock("HEAD", "/test-bucket/test-prefix/data.json")
            .with_status(200)
            .with_header("content-length", &data_len.to_string())
            .create_async()
            .await;

        // 3. Range GETs → return correct byte slices with 206
        for i in 0..(data_len / 256) {
            let start = i * 256;
            let end = start + 255;
            let range_header = format!("bytes={start}-{end}");
            server
                .mock("GET", "/test-bucket/test-prefix/data.json")
                .match_header("range", range_header.as_str())
                .with_status(206)
                .with_body(&data[start..=end])
                .create_async()
                .await;
        }

        let mut input = make_s3_input(&url, "test-bucket", "test-prefix/");
        let (received, got_eof) = poll_until_bytes(&mut input, data_len, Duration::from_secs(10));

        assert!(
            got_eof,
            "expected EOF event, did not receive one within timeout"
        );
        assert_eq!(
            received.len(),
            data_len,
            "expected {data_len} bytes, got {}",
            received.len()
        );
        assert_eq!(
            received,
            data.as_bytes(),
            "received data does not match original"
        );
    });
}

/// Data arrives in correct order even when concurrent fetches complete
/// out of order. The delivery loop must drain parts sequentially.
#[test]
fn parallel_fetch_preserves_byte_order() {
    // Each part has a unique byte pattern so order corruption is detectable.
    let mut data = Vec::with_capacity(1024);
    for i in 0u8..4 {
        data.extend(std::iter::repeat_n(i, 256));
    }
    let data_len = data.len(); // 1024

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut server = mockito::Server::new_async().await;
        let url = server.url();

        server
            .mock("GET", "/test-bucket")
            .match_query(mockito::Matcher::AllOf(vec![mockito::Matcher::UrlEncoded(
                "list-type".into(),
                "2".into(),
            )]))
            .with_status(200)
            .with_body(list_xml("pfx/ordered.bin", data_len as u64))
            .create_async()
            .await;

        server
            .mock("GET", "/test-bucket")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("list-type".into(), "2".into()),
                mockito::Matcher::UrlEncoded("start-after".into(), "pfx/ordered.bin".into()),
            ]))
            .with_status(200)
            .with_body(list_xml_empty())
            .expect_at_most(100)
            .create_async()
            .await;

        server
            .mock("HEAD", "/test-bucket/pfx/ordered.bin")
            .with_status(200)
            .with_header("content-length", &data_len.to_string())
            .create_async()
            .await;

        for i in 0..4usize {
            let start = i * 256;
            let end = start + 255;
            let range_header = format!("bytes={start}-{end}");
            server
                .mock("GET", "/test-bucket/pfx/ordered.bin")
                .match_header("range", range_header.as_str())
                .with_status(206)
                .with_body(&data[start..=end])
                .create_async()
                .await;
        }

        let mut input = make_s3_input(&url, "test-bucket", "pfx/");
        let (received, got_eof) = poll_until_bytes(&mut input, data_len, Duration::from_secs(10));

        assert!(got_eof);
        assert_eq!(received.len(), data_len);
        // Verify each 256-byte block has the correct fill byte.
        for (i, chunk) in received.chunks(256).enumerate() {
            let expected_byte = i as u8;
            assert!(
                chunk.iter().all(|&b| b == expected_byte),
                "part {i}: expected all bytes to be {expected_byte:#04x}, got mixed content"
            );
        }
    });
}

/// Empty-bytes EOF markers must not produce `InputEvent::Data` with 0 bytes
/// (which would panic in CheckpointTracker::apply_read).
#[test]
fn eof_markers_do_not_emit_zero_byte_data_events() {
    let data = "hello\n";
    let data_len = data.len();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut server = mockito::Server::new_async().await;
        let url = server.url();

        server
            .mock("GET", "/test-bucket")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("list-type".into(), "2".into()),
            ]))
            .with_status(200)
            .with_body(list_xml("pfx/tiny.txt", data_len as u64))
            .create_async()
            .await;

        server
            .mock("GET", "/test-bucket")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("list-type".into(), "2".into()),
                mockito::Matcher::UrlEncoded("start-after".into(), "pfx/tiny.txt".into()),
            ]))
            .with_status(200)
            .with_body(list_xml_empty())
            .expect_at_most(100)
            .create_async()
            .await;

        server
            .mock("HEAD", "/test-bucket/pfx/tiny.txt")
            .with_status(200)
            .with_header("content-length", &data_len.to_string())
            .create_async()
            .await;

        // Object is smaller than part_size (256), so single part covers it.
        let range_header = format!("bytes=0-{}", data_len - 1);
        server
            .mock("GET", "/test-bucket/pfx/tiny.txt")
            .match_header("range", range_header.as_str())
            .with_status(206)
            .with_body(data)
            .create_async()
            .await;

        let mut input = make_s3_input(&url, "test-bucket", "pfx/");

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut data_events = Vec::new();
        let mut eof_count = 0;

        while std::time::Instant::now() < deadline {
            if let Ok(events) = input.poll() {
                for event in events {
                    match event {
                        InputEvent::Data { bytes, .. } => {
                            assert!(
                                !bytes.is_empty(),
                                "InputEvent::Data emitted with 0 bytes — would panic in CheckpointTracker"
                            );
                            data_events.push(bytes);
                        }
                        InputEvent::EndOfFile { .. } => {
                            eof_count += 1;
                        }
                        _ => {}
                    }
                }
            }
            if eof_count > 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        assert!(eof_count > 0, "expected at least one EndOfFile event");
        let total: usize = data_events.iter().map(Vec::len).sum();
        assert_eq!(total, data_len);
    });
}

/// Verify the accounted_bytes field: only the first chunk carries the full
/// object size, all subsequent chunks carry 0.
#[test]
fn accounted_bytes_set_only_on_first_chunk() {
    let data = "X".repeat(768); // 3 parts at 256 B
    let data_len = data.len();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut server = mockito::Server::new_async().await;
        let url = server.url();

        server
            .mock("GET", "/test-bucket")
            .match_query(mockito::Matcher::AllOf(vec![mockito::Matcher::UrlEncoded(
                "list-type".into(),
                "2".into(),
            )]))
            .with_status(200)
            .with_body(list_xml("pfx/acc.bin", data_len as u64))
            .create_async()
            .await;

        server
            .mock("GET", "/test-bucket")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("list-type".into(), "2".into()),
                mockito::Matcher::UrlEncoded("start-after".into(), "pfx/acc.bin".into()),
            ]))
            .with_status(200)
            .with_body(list_xml_empty())
            .expect_at_most(100)
            .create_async()
            .await;

        server
            .mock("HEAD", "/test-bucket/pfx/acc.bin")
            .with_status(200)
            .with_header("content-length", &data_len.to_string())
            .create_async()
            .await;

        for i in 0..3usize {
            let start = i * 256;
            let end = start + 255;
            let range_header = format!("bytes={start}-{end}");
            server
                .mock("GET", "/test-bucket/pfx/acc.bin")
                .match_header("range", range_header.as_str())
                .with_status(206)
                .with_body(&data[start..=end])
                .create_async()
                .await;
        }

        let mut input = make_s3_input(&url, "test-bucket", "pfx/");

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut accounted_values: Vec<u64> = Vec::new();
        let mut got_eof = false;

        while std::time::Instant::now() < deadline {
            if let Ok(events) = input.poll() {
                for event in events {
                    match event {
                        InputEvent::Data {
                            accounted_bytes, ..
                        } => {
                            accounted_values.push(accounted_bytes);
                        }
                        InputEvent::EndOfFile { .. } => {
                            got_eof = true;
                        }
                        _ => {}
                    }
                }
            }
            if got_eof {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        assert!(got_eof, "expected EOF");
        assert!(
            !accounted_values.is_empty(),
            "expected at least one Data event"
        );
        assert_eq!(
            accounted_values[0], data_len as u64,
            "first chunk should carry full object size"
        );
        for (i, &v) in accounted_values.iter().enumerate().skip(1) {
            assert_eq!(v, 0, "chunk {i} should have accounted_bytes=0, got {v}");
        }
    });
}

/// An empty object (0 bytes) should produce an EOF event without panicking.
#[test]
fn empty_object_produces_eof_without_panic() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut server = mockito::Server::new_async().await;
        let url = server.url();

        server
            .mock("GET", "/test-bucket")
            .match_query(mockito::Matcher::AllOf(vec![mockito::Matcher::UrlEncoded(
                "list-type".into(),
                "2".into(),
            )]))
            .with_status(200)
            .with_body(list_xml("pfx/empty.txt", 0))
            .create_async()
            .await;

        server
            .mock("GET", "/test-bucket")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("list-type".into(), "2".into()),
                mockito::Matcher::UrlEncoded("start-after".into(), "pfx/empty.txt".into()),
            ]))
            .with_status(200)
            .with_body(list_xml_empty())
            .expect_at_most(100)
            .create_async()
            .await;

        server
            .mock("HEAD", "/test-bucket/pfx/empty.txt")
            .with_status(200)
            .with_header("content-length", "0")
            .create_async()
            .await;

        // size==0 falls through to single-GET stream path
        server
            .mock("GET", "/test-bucket/pfx/empty.txt")
            .with_status(200)
            .with_body("")
            .create_async()
            .await;

        let mut input = make_s3_input(&url, "test-bucket", "pfx/");

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut got_eof = false;
        let mut data_bytes = 0usize;

        while std::time::Instant::now() < deadline {
            if let Ok(events) = input.poll() {
                for event in events {
                    match event {
                        InputEvent::Data { bytes, .. } => {
                            data_bytes += bytes.len();
                        }
                        InputEvent::EndOfFile { .. } => {
                            got_eof = true;
                        }
                        _ => {}
                    }
                }
            }
            if got_eof {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        assert!(got_eof, "expected EOF for empty object");
        assert_eq!(data_bytes, 0, "empty object should produce no data bytes");
    });
}

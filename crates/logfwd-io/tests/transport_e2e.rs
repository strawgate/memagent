//! End-to-end integration tests for TCP, UDP, and OTLP transports.
//!
//! Each test spins up a real receiver, sends data over the network, and
//! verifies the data arrives through the `InputSource::poll()` interface.

use std::io::Write;
use std::net::{TcpStream, UdpSocket};
use std::thread;
use std::time::Duration;

use logfwd_io::{
    input::{InputEvent, InputSource},
    otlp_receiver::OtlpReceiverInput,
    tcp_input::TcpInput,
    udp_input::UdpInput,
};

/// Small sleep to let the OS deliver data / accept connections.
fn settle() {
    thread::sleep(Duration::from_millis(80));
}

/// Poll up to `max_polls` times (with a short sleep between attempts),
/// collecting all received bytes. Returns the concatenated data.
fn poll_until_bytes(input: &mut dyn InputSource, max_polls: usize) -> Vec<u8> {
    let mut all = Vec::new();
    for _ in 0..max_polls {
        settle();
        for event in input.poll().unwrap() {
            if let InputEvent::Data { bytes } = event {
                all.extend_from_slice(&bytes);
            }
        }
    }
    all
}

// ---------------------------------------------------------------------------
// TCP tests
// ---------------------------------------------------------------------------

#[test]
fn tcp_single_line() {
    let mut input = TcpInput::new("test", "127.0.0.1:0").unwrap();
    let addr = input.local_addr().unwrap();

    let mut client = TcpStream::connect(addr).unwrap();
    client.write_all(b"{\"msg\":\"hello\"}\n").unwrap();
    client.flush().unwrap();

    let data = poll_until_bytes(&mut input, 3);
    let text = String::from_utf8_lossy(&data);
    assert!(
        text.contains("{\"msg\":\"hello\"}"),
        "expected JSON line, got: {text}"
    );
}

#[test]
fn tcp_multiple_lines() {
    let mut input = TcpInput::new("test", "127.0.0.1:0").unwrap();
    let addr = input.local_addr().unwrap();

    let mut client = TcpStream::connect(addr).unwrap();
    for i in 0..100 {
        write!(client, "{{\"seq\":{i}}}\n").unwrap();
    }
    client.flush().unwrap();

    let data = poll_until_bytes(&mut input, 5);
    let text = String::from_utf8_lossy(&data);

    // Verify all 100 lines arrived.
    for i in 0..100 {
        assert!(
            text.contains(&format!("\"seq\":{i}")),
            "missing seq {i} in: {text}"
        );
    }
}

#[test]
fn tcp_partial_line_across_reads() {
    let mut input = TcpInput::new("test", "127.0.0.1:0").unwrap();
    let addr = input.local_addr().unwrap();

    let mut client = TcpStream::connect(addr).unwrap();

    // Send first half of a line.
    client.write_all(b"{\"partial\":\"fir").unwrap();
    client.flush().unwrap();
    settle();

    // Send the rest.
    client.write_all(b"st\"}\n").unwrap();
    client.flush().unwrap();

    let data = poll_until_bytes(&mut input, 5);
    let text = String::from_utf8_lossy(&data);
    assert!(
        text.contains("{\"partial\":\"first\"}"),
        "expected reassembled line, got: {text}"
    );
}

#[test]
fn tcp_multiple_clients() {
    let mut input = TcpInput::new("test", "127.0.0.1:0").unwrap();
    let addr = input.local_addr().unwrap();

    let handles: Vec<_> = (0..3)
        .map(|id| {
            thread::spawn(move || {
                let mut client = TcpStream::connect(addr).unwrap();
                for i in 0..10 {
                    write!(client, "{{\"client\":{id},\"seq\":{i}}}\n").unwrap();
                }
                client.flush().unwrap();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let data = poll_until_bytes(&mut input, 5);
    let text = String::from_utf8_lossy(&data);

    // Each client should have sent 10 lines.
    for id in 0..3 {
        for i in 0..10 {
            assert!(
                text.contains(&format!("\"client\":{id},\"seq\":{i}")),
                "missing client={id} seq={i}"
            );
        }
    }
}

#[test]
fn tcp_client_disconnect_mid_stream() {
    let mut input = TcpInput::new("test", "127.0.0.1:0").unwrap();
    let addr = input.local_addr().unwrap();

    {
        let mut client = TcpStream::connect(addr).unwrap();
        client.write_all(b"{\"before\":true}\n").unwrap();
        client.flush().unwrap();
        // Client drops here — connection closed.
    }

    let data = poll_until_bytes(&mut input, 5);
    let text = String::from_utf8_lossy(&data);
    assert!(
        text.contains("\"before\":true"),
        "expected data before disconnect, got: {text}"
    );

    // Subsequent polls should not panic.
    let events = input.poll().unwrap();
    // No more data expected.
    assert!(
        events.is_empty()
            || events
                .iter()
                .all(|e| matches!(e, InputEvent::Data { bytes } if bytes.is_empty()))
    );
}

#[test]
fn tcp_large_message() {
    let mut input = TcpInput::new("test", "127.0.0.1:0").unwrap();
    let addr = input.local_addr().unwrap();

    // Build a 64 KB JSON line.
    let payload = "x".repeat(64 * 1024 - 20); // leave room for JSON wrapper + newline
    let line = format!("{{\"big\":\"{payload}\"}}\n");

    let mut client = TcpStream::connect(addr).unwrap();
    client.write_all(line.as_bytes()).unwrap();
    client.flush().unwrap();

    let data = poll_until_bytes(&mut input, 10);
    let text = String::from_utf8_lossy(&data);
    assert!(
        text.contains(&payload),
        "64KB payload not fully received (got {} bytes)",
        data.len()
    );
}

#[test]
fn tcp_rapid_connect_disconnect() {
    let mut input = TcpInput::new("test", "127.0.0.1:0").unwrap();
    let addr = input.local_addr().unwrap();

    // Rapidly connect and disconnect 50 times.
    for _ in 0..50 {
        let _ = TcpStream::connect(addr).unwrap();
        // Immediately dropped — connection closed.
    }

    // Give the system time to process all connections.
    settle();

    // Poll several times to accept and then clean up all connections.
    for _ in 0..10 {
        let _ = input.poll().unwrap();
        thread::sleep(Duration::from_millis(20));
    }

    // All connections should be cleaned up (EOF detected).
    assert_eq!(
        input.client_count(),
        0,
        "expected 0 tracked clients after rapid connect/disconnect"
    );
}

// ---------------------------------------------------------------------------
// UDP tests
// ---------------------------------------------------------------------------

#[test]
fn udp_single_datagram() {
    let mut input = UdpInput::new("test", "127.0.0.1:0").unwrap();
    let addr = input.local_addr().unwrap();

    let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    sender.send_to(b"{\"msg\":\"hello\"}\n", addr).unwrap();

    let data = poll_until_bytes(&mut input, 3);
    let text = String::from_utf8_lossy(&data);
    assert!(
        text.contains("{\"msg\":\"hello\"}"),
        "expected datagram content, got: {text}"
    );
}

#[test]
fn udp_multiple_datagrams() {
    let mut input = UdpInput::new("test", "127.0.0.1:0").unwrap();
    let addr = input.local_addr().unwrap();

    let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    for i in 0..100 {
        let msg = format!("{{\"seq\":{i}}}\n");
        sender.send_to(msg.as_bytes(), addr).unwrap();
    }

    let data = poll_until_bytes(&mut input, 5);
    let text = String::from_utf8_lossy(&data);

    // UDP can drop packets, but on localhost most should arrive.
    let mut received = 0;
    for i in 0..100 {
        if text.contains(&format!("\"seq\":{i}")) {
            received += 1;
        }
    }
    assert!(
        received >= 90,
        "expected at least 90/100 datagrams, got {received}"
    );
}

#[test]
fn udp_max_size_datagram() {
    let mut input = UdpInput::new("test", "127.0.0.1:0").unwrap();
    let addr = input.local_addr().unwrap();

    // 65507 is the max UDP payload: 65535 - 20 (IP) - 8 (UDP).
    let mut payload = vec![b'A'; 65507];
    // Put a newline at the end so it's a valid line.
    *payload.last_mut().unwrap() = b'\n';

    // Use socket2 to create a sender with a large enough send buffer.
    let sock2 = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )
    .unwrap();
    sock2
        .bind(
            &"127.0.0.1:0"
                .parse::<std::net::SocketAddr>()
                .unwrap()
                .into(),
        )
        .unwrap();
    let _ = sock2.set_send_buffer_size(256 * 1024); // ensure kernel buffer is large enough
    let sender: UdpSocket = sock2.into();
    sender.send_to(&payload, addr).unwrap();

    let data = poll_until_bytes(&mut input, 3);
    assert_eq!(
        data.len(),
        65507,
        "expected 65507-byte datagram, got {} bytes",
        data.len()
    );
}

#[test]
fn udp_no_trailing_newline() {
    let mut input = UdpInput::new("test", "127.0.0.1:0").unwrap();
    let addr = input.local_addr().unwrap();

    let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    // Send without trailing newline.
    sender.send_to(b"no newline here", addr).unwrap();

    let data = poll_until_bytes(&mut input, 3);
    assert!(
        data.ends_with(b"\n"),
        "expected trailing newline to be appended"
    );
    let text = String::from_utf8_lossy(&data);
    assert!(
        text.contains("no newline here"),
        "expected payload content, got: {text}"
    );
}

// ---------------------------------------------------------------------------
// OTLP tests
// ---------------------------------------------------------------------------

#[test]
fn otlp_protobuf_roundtrip() {
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest,
        common::v1::{AnyValue, KeyValue, any_value::Value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    };
    use prost::Message;

    let mut input = OtlpReceiverInput::new("test", "127.0.0.1:0").unwrap();
    let addr = input.local_addr();
    let url = format!("http://{addr}/v1/logs");

    // Build a protobuf request with one log record.
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    severity_text: "INFO".into(),
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("test message".into())),
                    }),
                    attributes: vec![KeyValue {
                        key: "env".into(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("prod".into())),
                        }),
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let body = request.encode_to_vec();

    // POST the protobuf to the OTLP endpoint.
    let resp = ureq::post(&url)
        .header("Content-Type", "application/x-protobuf")
        .send(&body)
        .expect("OTLP POST should succeed");
    assert_eq!(resp.status(), 200);

    // Poll for the decoded JSON lines.
    let data = poll_until_bytes(&mut input, 5);
    let text = String::from_utf8_lossy(&data);

    assert!(
        text.contains("\"level\":\"INFO\""),
        "expected level field, got: {text}"
    );
    assert!(
        text.contains("\"message\":\"test message\""),
        "expected message field, got: {text}"
    );
    assert!(
        text.contains("\"env\":\"prod\""),
        "expected env attribute, got: {text}"
    );
}

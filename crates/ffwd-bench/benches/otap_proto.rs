//! OTAP protobuf boundary benchmarks.
//!
//! Compares the current generated/prost OTAP message boundary against the
//! previous handwritten protobuf helpers that lived in `ffwd-output`.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ffwd_core::otlp::{
    decode_tag, decode_varint, encode_bytes_field, encode_tag, encode_varint, encode_varint_field,
    skip_field,
};
use ffwd_output::{
    ArrowPayloadType, BatchStatus, DecodedPayload, StatusCode, decode_batch_arrow_records,
    decode_batch_arrow_records_generated_fast, decode_batch_status,
    decode_batch_status_generated_fast, encode_batch_arrow_records,
    encode_batch_arrow_records_generated_fast,
};

fn manual_encode_arrow_payload(
    buf: &mut Vec<u8>,
    schema_id: &str,
    payload_type: ArrowPayloadType,
    record: &[u8],
) {
    if !schema_id.is_empty() {
        encode_bytes_field(buf, 1, schema_id.as_bytes());
    }
    let type_val = payload_type as u64;
    if type_val != 0 {
        encode_varint_field(buf, 2, type_val);
    }
    if !record.is_empty() {
        encode_bytes_field(buf, 3, record);
    }
}

fn manual_encode_batch_arrow_records(
    buf: &mut Vec<u8>,
    batch_id: i64,
    payloads: &[(String, ArrowPayloadType, Vec<u8>)],
    headers: &[u8],
) {
    if batch_id != 0 {
        encode_varint_field(buf, 1, batch_id as u64);
    }
    for (schema_id, payload_type, record) in payloads {
        let mut sub = Vec::with_capacity(record.len() + 64);
        manual_encode_arrow_payload(&mut sub, schema_id, *payload_type, record);
        encode_tag(buf, 2, 2);
        encode_varint(buf, sub.len() as u64);
        buf.extend_from_slice(&sub);
    }
    if !headers.is_empty() {
        encode_bytes_field(buf, 3, headers);
    }
}

fn manual_decode_batch_status(data: &[u8]) -> std::io::Result<BatchStatus> {
    if data.is_empty() {
        return Ok(BatchStatus {
            batch_id: 0,
            status_code: StatusCode::Ok,
            status_message: String::new(),
        });
    }

    let mut batch_id: i64 = 0;
    let mut status_code = StatusCode::Ok;
    let mut status_message = String::new();
    let mut pos = 0;

    while pos < data.len() {
        let (field_number, wire_type, new_pos) = decode_tag(data, pos)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        pos = new_pos;
        match (field_number, wire_type) {
            (1, 0) => {
                let (val, new_pos) = decode_varint(data, pos)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                batch_id = val as i64;
                pos = new_pos;
            }
            (2, 0) => {
                let (val, new_pos) = decode_varint(data, pos)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                status_code = match val {
                    0 => StatusCode::Ok,
                    1 => StatusCode::Unavailable,
                    2 => StatusCode::InvalidArgument,
                    _ => StatusCode::InvalidArgument,
                };
                pos = new_pos;
            }
            (3, 2) => {
                let (len, new_pos) = decode_varint(data, pos)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                let end = new_pos + len as usize;
                if end > data.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "truncated status_message",
                    ));
                }
                status_message = String::from_utf8_lossy(&data[new_pos..end]).into_owned();
                pos = end;
            }
            _ => {
                pos = skip_field(data, wire_type, pos)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            }
        }
    }

    Ok(BatchStatus {
        batch_id,
        status_code,
        status_message,
    })
}

fn manual_decode_arrow_payload(data: &[u8]) -> std::io::Result<DecodedPayload> {
    let mut schema_id = String::new();
    let mut payload_type: u32 = 0;
    let mut record = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        let (field_number, wire_type, new_pos) = decode_tag(data, pos)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        pos = new_pos;
        match (field_number, wire_type) {
            (1, 2) => {
                let (len, new_pos) = decode_varint(data, pos)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                let end = new_pos + len as usize;
                if end > data.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "truncated schema_id",
                    ));
                }
                schema_id = String::from_utf8_lossy(&data[new_pos..end]).into_owned();
                pos = end;
            }
            (2, 0) => {
                let (val, new_pos) = decode_varint(data, pos)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                payload_type = val as u32;
                pos = new_pos;
            }
            (3, 2) => {
                let (len, new_pos) = decode_varint(data, pos)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                let end = new_pos + len as usize;
                if end > data.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "truncated record",
                    ));
                }
                record = data[new_pos..end].to_vec();
                pos = end;
            }
            _ => {
                pos = skip_field(data, wire_type, pos)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            }
        }
    }

    Ok((schema_id, payload_type, record))
}

fn manual_decode_batch_arrow_records(
    data: &[u8],
) -> std::io::Result<(i64, Vec<DecodedPayload>, Vec<u8>)> {
    let mut batch_id: i64 = 0;
    let mut payloads = Vec::new();
    let mut headers = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        let (field_number, wire_type, new_pos) = decode_tag(data, pos)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        pos = new_pos;
        match (field_number, wire_type) {
            (1, 0) => {
                let (val, new_pos) = decode_varint(data, pos)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                batch_id = val as i64;
                pos = new_pos;
            }
            (2, 2) => {
                let (len, new_pos) = decode_varint(data, pos)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                let end = new_pos + len as usize;
                if end > data.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "truncated ArrowPayload",
                    ));
                }
                payloads.push(manual_decode_arrow_payload(&data[new_pos..end])?);
                pos = end;
            }
            (3, 2) => {
                let (len, new_pos) = decode_varint(data, pos)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                let end = new_pos + len as usize;
                if end > data.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "truncated headers",
                    ));
                }
                headers = data[new_pos..end].to_vec();
                pos = end;
            }
            _ => {
                pos = skip_field(data, wire_type, pos)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            }
        }
    }

    Ok((batch_id, payloads, headers))
}

fn make_payloads(bytes_per_payload: usize) -> Vec<(String, ArrowPayloadType, Vec<u8>)> {
    let mk = |name: &str, payload_type: ArrowPayloadType, byte: u8| {
        (
            name.to_string(),
            payload_type,
            vec![byte; bytes_per_payload],
        )
    };
    vec![
        mk("logs", ArrowPayloadType::Logs, 0x11),
        mk("log_attrs", ArrowPayloadType::LogAttrs, 0x22),
        mk("resource_attrs", ArrowPayloadType::ResourceAttrs, 0x33),
        mk("scope_attrs", ArrowPayloadType::ScopeAttrs, 0x44),
    ]
}

fn bench_otap_proto(c: &mut Criterion) {
    let mut group = c.benchmark_group("otap_proto");
    group.sample_size(20);

    for (label, payload_size) in [("small", 512usize), ("large", 64 * 1024usize)] {
        let payloads = make_payloads(payload_size);
        let headers = b"authorization: bearer bench-token";
        let total_bytes: usize =
            payloads.iter().map(|(_, _, r)| r.len()).sum::<usize>() + headers.len();

        group.throughput(Throughput::Bytes(total_bytes as u64));

        group.bench_function(BenchmarkId::new("encode_manual", label), |b| {
            let mut buf = Vec::with_capacity(total_bytes + 512);
            b.iter(|| {
                buf.clear();
                manual_encode_batch_arrow_records(&mut buf, 42, &payloads, headers);
                std::hint::black_box(&buf);
            });
        });

        group.bench_function(BenchmarkId::new("encode_generated", label), |b| {
            let mut buf = Vec::with_capacity(total_bytes + 512);
            b.iter(|| {
                buf.clear();
                encode_batch_arrow_records(&mut buf, 42, &payloads, headers);
                std::hint::black_box(&buf);
            });
        });

        group.bench_function(BenchmarkId::new("encode_generated_fast", label), |b| {
            let mut buf = Vec::with_capacity(total_bytes + 512);
            b.iter(|| {
                buf.clear();
                encode_batch_arrow_records_generated_fast(&mut buf, 42, &payloads, headers);
                std::hint::black_box(&buf);
            });
        });

        let mut encoded = Vec::with_capacity(total_bytes + 512);
        encode_batch_arrow_records(&mut encoded, 42, &payloads, headers);

        group.bench_function(BenchmarkId::new("decode_manual", label), |b| {
            b.iter(|| {
                let decoded = manual_decode_batch_arrow_records(&encoded).expect("manual decode");
                std::hint::black_box(decoded);
            });
        });

        group.bench_function(BenchmarkId::new("decode_generated", label), |b| {
            b.iter(|| {
                let decoded = decode_batch_arrow_records(&encoded).expect("generated decode");
                std::hint::black_box(decoded);
            });
        });

        group.bench_function(BenchmarkId::new("decode_generated_fast", label), |b| {
            b.iter(|| {
                let decoded = decode_batch_arrow_records_generated_fast(&encoded)
                    .expect("generated fast decode");
                std::hint::black_box(decoded);
            });
        });
    }

    let mut status_buf = Vec::new();
    encode_bytes_field(&mut status_buf, 3, b"bench status");
    encode_varint_field(&mut status_buf, 1, 42);
    encode_varint_field(&mut status_buf, 2, StatusCode::Unavailable as u64);

    group.bench_function("status_decode_manual", |b| {
        b.iter(|| {
            let decoded = manual_decode_batch_status(&status_buf).expect("manual status decode");
            std::hint::black_box(decoded);
        });
    });

    group.bench_function("status_decode_generated", |b| {
        b.iter(|| {
            let decoded = decode_batch_status(&status_buf).expect("generated status decode");
            std::hint::black_box(decoded);
        });
    });

    group.bench_function("status_decode_generated_fast", |b| {
        b.iter(|| {
            let decoded = decode_batch_status_generated_fast(&status_buf)
                .expect("generated fast status decode");
            std::hint::black_box(decoded);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_otap_proto);
criterion_main!(benches);

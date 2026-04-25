    use super::*;
    use ffwd_core::otlp;

    fn sample_field_for_wire(number: u32, wire: WireKind) -> Vec<u8> {
        let mut out = Vec::new();
        push_varint(
            &mut out,
            (u64::from(number) << 3) | u64::from(wire_id(wire)),
        );
        match wire {
            WireKind::Varint => push_varint(&mut out, 1),
            WireKind::Fixed64 => out.extend_from_slice(&1u64.to_le_bytes()),
            WireKind::Len => {
                push_varint(&mut out, 1);
                out.push(0);
            }
            WireKind::StartGroup => {
                push_varint(
                    &mut out,
                    (u64::from(number) << 3) | u64::from(wire_id(WireKind::EndGroup)),
                );
            }
            WireKind::EndGroup => {}
            WireKind::Fixed32 => out.extend_from_slice(&1u32.to_le_bytes()),
        }
        out
    }

    fn push_varint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push(((value & 0x7f) as u8) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
    }

    fn push_len_field(out: &mut Vec<u8>, number: u32, bytes: &[u8]) {
        push_varint(
            out,
            (u64::from(number) << 3) | u64::from(wire_id(WireKind::Len)),
        );
        push_varint(out, bytes.len() as u64);
        out.extend_from_slice(bytes);
    }

    fn push_fixed64_field(out: &mut Vec<u8>, number: u32, value: u64) {
        push_varint(
            out,
            (u64::from(number) << 3) | u64::from(wire_id(WireKind::Fixed64)),
        );
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn push_fixed32_field(out: &mut Vec<u8>, number: u32, value: u32) {
        push_varint(
            out,
            (u64::from(number) << 3) | u64::from(wire_id(WireKind::Fixed32)),
        );
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn push_varint_field(out: &mut Vec<u8>, number: u32, value: u64) {
        push_varint(
            out,
            (u64::from(number) << 3) | u64::from(wire_id(WireKind::Varint)),
        );
        push_varint(out, value);
    }

    fn wire_id(wire: WireKind) -> u8 {
        match wire {
            WireKind::Varint => 0,
            WireKind::Fixed64 => 1,
            WireKind::Len => 2,
            WireKind::StartGroup => 3,
            WireKind::EndGroup => 4,
            WireKind::Fixed32 => 5,
        }
    }

    fn all_messages() -> &'static [MessageKind] {
        &[
            MessageKind::ExportLogsServiceRequest,
            MessageKind::ResourceLogs,
            MessageKind::Resource,
            MessageKind::ScopeLogs,
            MessageKind::InstrumentationScope,
            MessageKind::LogRecord,
            MessageKind::KeyValue,
            MessageKind::AnyValue,
            MessageKind::ArrayValue,
            MessageKind::KeyValueList,
        ]
    }

    fn wrong_wires(expected: WireKind) -> [WireKind; 5] {
        match expected {
            WireKind::Varint => [
                WireKind::Fixed64,
                WireKind::Len,
                WireKind::StartGroup,
                WireKind::EndGroup,
                WireKind::Fixed32,
            ],
            WireKind::Fixed64 => [
                WireKind::Varint,
                WireKind::Len,
                WireKind::StartGroup,
                WireKind::EndGroup,
                WireKind::Fixed32,
            ],
            WireKind::Len => [
                WireKind::Varint,
                WireKind::Fixed64,
                WireKind::StartGroup,
                WireKind::EndGroup,
                WireKind::Fixed32,
            ],
            WireKind::StartGroup => [
                WireKind::Varint,
                WireKind::Fixed64,
                WireKind::Len,
                WireKind::EndGroup,
                WireKind::Fixed32,
            ],
            WireKind::EndGroup => [
                WireKind::Varint,
                WireKind::Fixed64,
                WireKind::Len,
                WireKind::StartGroup,
                WireKind::Fixed32,
            ],
            WireKind::Fixed32 => [
                WireKind::Varint,
                WireKind::Fixed64,
                WireKind::Len,
                WireKind::StartGroup,
                WireKind::EndGroup,
            ],
        }
    }

    fn complex_anyvalue_field_numbers() -> &'static [u32] {
        &[5, 6]
    }

    #[test]
    fn generated_anyvalue_table_covers_all_current_oneof_fields() {
        let fields = fields_for(MessageKind::AnyValue);
        assert_eq!(fields.len(), 7);
        assert!(
            fields
                .iter()
                .any(|f| f.name == "array_value" && f.action == ProjectionAction::Project)
        );
        assert!(
            fields
                .iter()
                .any(|f| f.name == "kvlist_value" && f.action == ProjectionAction::Project)
        );
        assert!(
            fields
                .iter()
                .any(|f| f.name == "bytes_value" && f.action == ProjectionAction::Project)
        );
    }

    #[test]
    fn generated_wire_any_field_kind_matches_variant_policy() {
        assert_eq!(
            wire_any_field_kind(&WireAny::String(b"s")),
            FieldKind::Utf8View
        );
        assert_eq!(wire_any_field_kind(&WireAny::Bool(true)), FieldKind::Bool);
        assert_eq!(wire_any_field_kind(&WireAny::Int(42)), FieldKind::Int64);
        assert_eq!(
            wire_any_field_kind(&WireAny::Double(42.5)),
            FieldKind::Float64
        );
        assert_eq!(
            wire_any_field_kind(&WireAny::Bytes(b"b")),
            FieldKind::Utf8View
        );
        assert_eq!(
            wire_any_field_kind(&WireAny::ArrayRaw(&[])),
            FieldKind::Utf8View
        );
        assert_eq!(
            wire_any_field_kind(&WireAny::KvListRaw(&[])),
            FieldKind::Utf8View
        );
    }

    #[test]
    fn generated_wire_any_json_handles_scalar_and_complex_variants() {
        let mut scratch = WireScratch::default();
        let cases: &[(WireAny<'_>, &[u8])] = &[
            (WireAny::String(br#"a"b"#), br#""a\"b""#),
            (WireAny::Bool(true), b"true"),
            (WireAny::Int(-42), b"-42"),
            (WireAny::Double(12.5), b"12.5"),
            (WireAny::Bytes(&[0xde, 0xad]), br#""dead""#),
            (WireAny::ArrayRaw(&[]), b"[]"),
            (WireAny::KvListRaw(&[]), b"[]"),
        ];

        for &(value, expected) in cases {
            let mut out = Vec::new();
            write_wire_any_json(value, &mut out, &mut scratch)
                .expect("generated JSON writer should handle projected variant");
            assert_eq!(out, expected);
        }
    }

    #[test]
    fn generated_complex_json_writers_handle_nested_messages() {
        let mut scratch = WireScratch::default();
        let any_string = [10, 1, b'x'];
        let array = [10, 3, 10, 1, b'x'];
        let kv = [10, 1, b'k', 18, 3, 10, 1, b'v'];
        let kvlist = [10, 8, 10, 1, b'k', 18, 3, 10, 1, b'v'];

        let mut out = Vec::new();
        write_array_value_json(&array, &mut out, &mut scratch, 0)
            .expect("generated array JSON writer should decode repeated AnyValue");
        assert_eq!(out, br#"["x"]"#);

        out.clear();
        write_key_value_json(&kv, &mut out, &mut scratch, 0)
            .expect("generated key value JSON writer should decode KeyValue");
        assert_eq!(out, br#"{"k":"k","v":"v"}"#);

        out.clear();
        write_kvlist_value_json(&kvlist, &mut out, &mut scratch, 0)
            .expect("generated kvlist JSON writer should decode repeated KeyValue");
        assert_eq!(out, br#"[{"k":"k","v":"v"}]"#);

        out.clear();
        write_wire_any_json(WireAny::ArrayRaw(&array), &mut out, &mut scratch)
            .expect("generated AnyValue JSON writer should dispatch arrays");
        assert_eq!(out, br#"["x"]"#);

        out.clear();
        write_wire_any_json(WireAny::String(&any_string[2..]), &mut out, &mut scratch)
            .expect("generated AnyValue JSON writer should keep scalar dispatch");
        assert_eq!(out, br#""x""#);
    }

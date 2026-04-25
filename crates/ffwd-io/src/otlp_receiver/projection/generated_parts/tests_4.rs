    #[test]
    fn generated_security_malformed_wire_corpus_is_rejected() {
        let malformed_cases: &[&[u8]] = &[
            &[0x0a, 0x02, 0x01],
            &[
                0x08, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02,
            ],
            &[0x53, 0x64],
            &[0x0b, 0x13],
            &[0x0c],
        ];
        for bytes in malformed_cases {
            let err = classify_projection_support(bytes)
                .expect_err("malformed protobuf bytes must be rejected");
            assert!(matches!(err, ProjectionError::Invalid(_)));
        }
    }

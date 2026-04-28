fn main() {
    #[allow(clippy::expect_used)]
    {
        ffwd_proto_build::compile_with_vendored_protoc(&["proto/otap.proto"], &["proto"])
            .expect("compile OTAP protobuf schema");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    ffwd_proto_build::compile_with_vendored_protoc(&["proto/otap.proto"], &["proto"])?;
    Ok(())
}

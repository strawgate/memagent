//! Shared protobuf build orchestration for generated protocol boundary crates.
//!
//! This crate intentionally shares only build-time mechanics. Protocol-specific
//! semantics and performance-sensitive encoders stay in their owning crates.

use std::path::Path;

/// Compile one or more proto files with vendored `protoc`.
pub fn compile_with_vendored_protoc(
    protos: &[impl AsRef<Path>],
    includes: &[impl AsRef<Path>],
) -> std::io::Result<()> {
    #[allow(clippy::expect_used)]
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc path");
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    config.compile_protos(protos, includes)
}

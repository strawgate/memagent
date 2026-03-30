# Installation

## From source (recommended)

```bash
git clone https://github.com/strawgate/memagent.git
cd memagent
cargo build --release
# Binary at ./target/release/logfwd
```

## Prerequisites

- Rust stable toolchain (1.86+)
- `just` task runner: `cargo install just`

## Verify installation

```bash
./target/release/logfwd --version
./target/release/logfwd --help
```

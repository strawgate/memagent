with open('crates/logfwd-io/src/macos_log_input.rs', 'r') as f:
    content = f.read()

target = "    use std::sync::atomic::AtomicU64;"
replacement = "    // use std::sync::atomic::AtomicU64;"

content = content.replace(target, replacement)

with open('crates/logfwd-io/src/macos_log_input.rs', 'w') as f:
    f.write(content)

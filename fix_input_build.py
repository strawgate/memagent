import re

with open('crates/logfwd-runtime/src/pipeline/input_build.rs', 'r') as f:
    content = f.read()

content = content.replace(
"""        include_process_names: cfg.and_then(|c| c.include_process_names.clone()),
        exclude_process_names: cfg.and_then(|c| c.exclude_process_names.clone()),
        include_event_types: cfg.and_then(|c| c.include_event_types.clone()),
        exclude_event_types: cfg.and_then(|c| c.exclude_event_types.clone()),
        ring_buffer_size_kb: cfg.and_then(|c| c.ring_buffer_size_kb),""",
""
)

with open('crates/logfwd-runtime/src/pipeline/input_build.rs', 'w') as f:
    f.write(content)

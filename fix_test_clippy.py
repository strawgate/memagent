import re

with open('crates/logfwd-io/tests/it/framed_state_machine.rs', 'r') as f:
    content = f.read()

content = re.sub(
    r'if let InputEvent::Data \{ bytes: out_bytes, \.\. \} = event \{\s*if !out_bytes\.is_empty\(\) \{',
    r'if let InputEvent::Data { bytes: out_bytes, .. } = event && !out_bytes.is_empty() {',
    content
)

with open('crates/logfwd-io/tests/it/framed_state_machine.rs', 'w') as f:
    f.write(content)

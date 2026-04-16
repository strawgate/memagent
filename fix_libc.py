import re

with open('crates/logfwd-io/src/journald_input.rs', 'r') as f:
    content = f.read()

replacement = """        #[cfg(unix)]
        if let Some(pid) = self.pid {
            unsafe {
                libc::kill(pid as i32, libc::SIGKILL);
            }
        }
        self.pid = None;"""

content = re.sub(
    r'        if let Some\(pid\) = self\.pid \{\n            unsafe \{\n                libc::kill\(pid as i32, libc::SIGKILL\);\n            \}\n        \}\n        self\.pid = None;',
    replacement,
    content
)

with open('crates/logfwd-io/src/journald_input.rs', 'w') as f:
    f.write(content)

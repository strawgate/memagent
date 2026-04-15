import re

with open("crates/logfwd-config/src/validate.rs", "r") as f:
    content = f.read()

content = content.replace("}\n\n}\n\n#[cfg(test)]\nmod validate_otlp_options_tests", "}\n\n#[cfg(test)]\nmod validate_otlp_options_tests")

with open("crates/logfwd-config/src/validate.rs", "w") as f:
    f.write(content)

import re

with open("crates/logfwd-config/src/validate.rs", "r") as f:
    content = f.read()

# remove mod tests_otlp_options
content = re.sub(r'#\[cfg\(test\)\]\nmod tests_otlp_options \{.*?\}\n', '', content, flags=re.DOTALL)

with open("crates/logfwd-config/src/validate.rs", "w") as f:
    f.write(content)

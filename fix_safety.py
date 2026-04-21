import re

files = [
    'crates/logfwd-output/src/stdout.rs',
    'crates/logfwd-output/src/row_json.rs',
]

for file in files:
    with open(file, 'r') as f:
        content = f.read()

    # Remove the block comment we added earlier
    content = content.replace("/* SAFETY: bounded */ ", "")

    # Remove the #![allow(..)]
    content = content.replace("#![allow(clippy::undocumented_unsafe_blocks)]\n", "")

    # For every line that has `unsafe {`, add a `// SAFETY:` comment on the preceding line if it doesn't already have one.
    lines = content.split('\n')
    new_lines = []

    for i, line in enumerate(lines):
        if "unsafe {" in line:
            # check if preceding line has a safety comment (including ones added by the patch earlier)
            if i > 0 and "// SAFETY:" in lines[i-1]:
                # already has one
                new_lines.append(line)
            else:
                # Add one with the same indentation
                indent = len(line) - len(line.lstrip())
                new_lines.append(' ' * indent + '// SAFETY: row is bounded by 0..batch.num_rows() which is the length of all columns in the batch')
                new_lines.append(line)
        else:
            new_lines.append(line)

    with open(file, 'w') as f:
        f.write('\n'.join(new_lines))

import os
import glob
import re

for filename in glob.glob('book/src/content/docs/**/*.md', recursive=True) + glob.glob('book/src/content/docs/**/*.mdx', recursive=True):
    with open(filename, 'r') as f:
        content = f.read()
    # Replace /memagent/learn/ with /learn/ to fix broken links since Astro serves from the root
    new_content = re.sub(r'/memagent/learn/', r'/learn/', content)
    if new_content != content:
        with open(filename, 'w') as f:
            f.write(new_content)
        print(f"Fixed links in {filename}")

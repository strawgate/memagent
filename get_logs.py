import urllib.request
import json
import zipfile
import io
import os

url = "https://api.github.com/repos/strawgate/memagent/actions/runs/24439395673/jobs"
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as response:
    data = json.loads(response.read().decode())
    for job in data['jobs']:
        if job['conclusion'] == 'failure':
            print(f"Failed job: {job['name']}")
            print(f"Log URL: {job['url']}")

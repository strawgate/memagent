import urllib.request
import json
import zipfile
import io
import os

url = "https://api.github.com/repos/strawgate/memagent/actions/runs/24439395673/artifacts"
req = urllib.request.Request(url)
req.add_header("Authorization", f"Bearer {os.environ.get('GITHUB_TOKEN', '')}")
with urllib.request.urlopen(req) as response:
    data = json.loads(response.read().decode())
    for artifact in data['artifacts']:
        if artifact['name'] == 'kind-cri-smoke-results':
            print(f"URL: {artifact['archive_download_url']}")
            break

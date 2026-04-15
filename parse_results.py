import zipfile

with zipfile.ZipFile("kind-cri-smoke-results.zip", 'r') as zip_ref:
    zip_ref.extractall("kind_results")

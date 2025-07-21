# github_uploader.py

import base64
import requests
import os
from datetime import datetime

def upload_to_github(file_path, repo, path_in_repo, commit_message, branch="main"):
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise ValueError("Missing GITHUB_TOKEN environment variable")

    owner, repo_name = repo.split("/")
    api_url = f"https://api.github.com/repos/{owner}/{repo_name}/contents/{path_in_repo}"

    # Read file content and encode
    with open(file_path, "rb") as f:
        content = f.read()
    encoded = base64.b64encode(content).decode("utf-8")

    # Prepare headers and check if file exists
    headers = {"Authorization": f"token {token}"}
    r = requests.get(api_url, headers=headers)
    sha = r.json().get("sha") if r.status_code == 200 else None

    # Construct upload payload
    data = {
        "message": commit_message,
        "content": encoded,
        "branch": branch
    }
    if sha:
        data["sha"] = sha  # Required for overwrites

    # Upload via PUT request
    r = requests.put(api_url, headers=headers, json=data)
    if r.status_code in [200, 201]:
        print(f"Uploaded {file_path} to GitHub at {path_in_repo}")
        return r.json().get("commit", {}).get("sha")
    else:
        print(f"GitHub upload failed for {file_path}: {r.status_code} - {r.text}")
        return None

def create_github_tag(repo, tag_name, tag_message, commit_sha, branch="main"):
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise ValueError("Missing GITHUB_TOKEN environment variable")

    owner, repo_name = repo.split("/")
    api_url = f"https://api.github.com/repos/{owner}/{repo_name}/git/tags"

    headers = {"Authorization": f"token {token}"}

    tag_data = {
        "tag": tag_name,
        "message": tag_message,
        "object": commit_sha,
        "type": "commit",
        "tagger": {
            "name": "Market State Bot",
            "email": "bot@nirvana.com",
            "date": datetime.utcnow().isoformat() + "Z"
        }
    }

    tag_response = requests.post(api_url, headers=headers, json=tag_data)
    if tag_response.status_code not in [201]:
        print(f"Failed to create tag: {tag_response.status_code} - {tag_response.text}")
        return

    # Link tag to the refs
    ref_url = f"https://api.github.com/repos/{owner}/{repo_name}/git/refs"
    ref_data = {
        "ref": f"refs/tags/{tag_name}",
        "sha": tag_response.json()["sha"]
    }
    ref_response = requests.post(ref_url, headers=headers, json=ref_data)
    if ref_response.status_code not in [201]:
        print(f"Failed to create tag ref: {ref_response.status_code} - {ref_response.text}")
    else:
        print(f"🏷Created tag {tag_name} pointing to commit {commit_sha}")

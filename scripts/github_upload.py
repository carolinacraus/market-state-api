# scripts/github_upload.py
from __future__ import annotations

import base64
import json
import os
from typing import Tuple, Optional

import requests
from scripts.logger import get_logger

logger = get_logger("github_upload")


def _gh_headers() -> dict:
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN is required to upload to GitHub")
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _get_contents(repo: str, path_in_repo: str, ref: Optional[str] = None) -> dict | None:
    url = f"https://api.github.com/repos/{repo}/contents/{path_in_repo}"
    params = {"ref": ref} if ref else None
    r = requests.get(url, headers=_gh_headers(), params=params, timeout=30)
    if r.status_code == 200:
        return r.json()
    if r.status_code == 404:
        return None
    r.raise_for_status()


def upload_to_github(
    file_path: str,
    repo: str,
    path_in_repo: str,
    commit_message: str,
    branch: str | None = None,
) -> Tuple[bool, str]:
    """Create or update a single file in a GitHub repo. Returns (ok, sha_or_error)."""
    try:
        with open(file_path, "rb") as f:
            content_b64 = base64.b64encode(f.read()).decode("utf-8")
    except Exception as e:
        logger.error(f"Failed to read file {file_path}: {e}")
        return False, str(e)

    payload = {"message": commit_message, "content": content_b64}
    if branch:
        payload["branch"] = branch

    existing = _get_contents(repo, path_in_repo, ref=branch)
    if existing and "sha" in existing:
        payload["sha"] = existing["sha"]

    url = f"https://api.github.com/repos/{repo}/contents/{path_in_repo}"
    r = requests.put(url, headers=_gh_headers(), data=json.dumps(payload), timeout=60)
    if r.status_code in (200, 201):
        sha = (r.json().get("content") or {}).get("sha", "")
        logger.info(f"Uploaded {path_in_repo} to {repo} (sha={sha[:7]})")
        return True, sha or ""
    else:
        logger.error(f"GitHub upload failed: {r.status_code} {r.text}")
        return False, f"{r.status_code} {r.text}"


def upload_if_changed(
    file_path: str,
    repo: str,
    path_in_repo: str,
    commit_message: str,
    branch: str | None = None,
) -> Tuple[bool, str, bool]:
    """
    Upload only if remote content differs. Returns (ok, sha_or_err, changed).
    """
    # If remote missing, upload.
    existing = _get_contents(repo, path_in_repo, ref=branch)
    if not existing:
        ok, sha = upload_to_github(file_path, repo, path_in_repo, commit_message, branch)
        return ok, sha, True

    # Compare decoded contents to avoid unnecessary commits
    try:
        remote_b64 = existing.get("content", "")
        remote_bytes = base64.b64decode(remote_b64.encode("utf-8"))
        with open(file_path, "rb") as f:
            local_bytes = f.read()
        if local_bytes == remote_bytes:
            logger.info(f"No change detected for {path_in_repo}; skipping upload.")
            return True, existing.get("sha", ""), False
    except Exception as e:
        logger.warning(f"Content compare failed for {path_in_repo}; uploading anyway. {e}")

    ok, sha = upload_to_github(file_path, repo, path_in_repo, commit_message, branch)
    return ok, sha, True

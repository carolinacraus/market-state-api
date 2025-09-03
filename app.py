# app.py
from __future__ import annotations

import os
from datetime import datetime
from flask import Flask, jsonify, request

from market_pipeline.config import PipelineConfig
from market_pipeline.pipeline import DataPipeline
from scripts.logger import get_logger
from scripts.github_upload import upload_to_github  # provided below

app = Flask(__name__)
logger = get_logger("flask_api")

# Simple header-based auth
API_KEY = os.getenv("API_KEY")  # set in Railway env


def _require_key():
    key = request.headers.get("X-API-Key")
    if not API_KEY or key != API_KEY:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    return None


def _run_and_upload(which: str):
    cfg = PipelineConfig.from_env()
    pipe = DataPipeline(cfg=cfg, logger=logger)

    if which == "historical":
        pipe.run_historical()
    elif which == "daily":
        pipe.run_daily()
    elif which == "auto":
        pipe.run_once()
    else:
        return {"ok": False, "error": f"Unknown mode {which}"}

    # Upload the three CSVs to GitHub
    repo = os.getenv("GITHUB_REPO", cfg.repo)
    commit_prefix = os.getenv("COMMIT_PREFIX", "Pipeline")
    stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")

    uploads = []
    for local_path, remote_path in [
        (cfg.market_path,    f"data/{os.path.basename(cfg.market_path)}"),
        (cfg.indicator_path, f"data/{os.path.basename(cfg.indicator_path)}"),
        (cfg.breadth_path,   f"data/{os.path.basename(cfg.breadth_path)}"),
    ]:
        if os.path.exists(local_path):
            msg = f"{commit_prefix}: {which} run @ {stamp}"
            ok, sha_or_err = upload_to_github(
                file_path=local_path,
                repo=repo,
                path_in_repo=remote_path,
                commit_message=msg,
            )
            uploads.append({"file": remote_path, "ok": ok, "ref": sha_or_err})

    return {"ok": True, "mode": which, "uploads": uploads}


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "service": "market-state-api"})


@app.route("/run-historical", methods=["POST"])
def run_historical():
    if (resp := _require_key()) is not None:
        return resp
    out = _run_and_upload("historical")
    code = 200 if out.get("ok") else 500
    return jsonify(out), code


@app.route("/run-daily", methods=["POST"])
def run_daily():
    if (resp := _require_key()) is not None:
        return resp
    out = _run_and_upload("daily")
    code = 200 if out.get("ok") else 500
    return jsonify(out), code


@app.route("/run-pipeline", methods=["POST"])
def run_pipeline():
    if (resp := _require_key()) is not None:
        return resp
    out = _run_and_upload("auto")
    code = 200 if out.get("ok") else 500
    return jsonify(out), code


if __name__ == "__main__":
    # For local dev only. Railway uses its own server.
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))

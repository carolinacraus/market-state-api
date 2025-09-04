# app.py
from __future__ import annotations

import os
from datetime import datetime, timedelta

from flask import Flask, jsonify, request, send_file
import pandas as pd

from market_pipeline.config import PipelineConfig
from market_pipeline.pipeline import DataPipeline  # your orchestrator
from market_pipeline.errors import PipelineError, BadRequestError, UnauthorizedError

from scripts.logger import get_logger

app = Flask(__name__)
logger = get_logger("flask_app_noauth")

@app.before_request
def inject_request_id():
    g.request_id = request.headers.get("X-Request-Id") or str(uuid.uuid4())

def _json_error(status: int, code: str, message: str, *, details: dict | None = None):
    payload = {
        "ok": False,
        "error": {
            "code": code,
            "message": message,
            "details": details or {},
        },
        "request_id": g.get("request_id"),
        "path": request.path,
        "method": request.method,
    }
    # Log a single concise line for Railway logs
    logger.error(f"{code} {status} :: {message} :: req_id={payload['request_id']} details={payload['details']}")
    return jsonify(payload), status

# Known pipeline errors → clean JSON
@app.errorhandler(PipelineError)
def handle_pipeline_error(err: PipelineError):
    return _json_error(err.http_status, err.code, err.message, details=err.details)

# 400s from Flask / your code
@app.errorhandler(400)
def handle_400(e):
    return _json_error(400, "BAD_REQUEST", "Invalid request.", details={"hint": str(e)})

@app.errorhandler(401)
def handle_401(e):
    return _json_error(401, "UNAUTHORIZED", "Authentication required.", details={"hint": str(e)})

@app.errorhandler(404)
def handle_404(e):
    return _json_error(404, "NOT_FOUND", "Endpoint not found.", details={"hint": request.path})

# Catch-all for uncaught exceptions
@app.errorhandler(Exception)
def handle_uncaught(e):
    # You can add Sentry here later if desired
    return _json_error(500, "UNCAUGHT_EXCEPTION", "Unexpected server error.", details={"type": type(e).__name__})
# === helpers ===
def _cfg() -> PipelineConfig:
    cfg = PipelineConfig.from_env()
    cfg.ensure_dirs()
    return cfg

def _pipe(logger=logger) -> DataPipeline:
    return DataPipeline(cfg=_cfg(), logger=logger)

@app.route("/", methods=["GET"])
def index():
    logger.info("Health check hit.")
    return "Market State API is running (no-auth mode)."

# ----------------- pipeline endpoints -----------------
@app.route("/run-daily-pipeline", methods=["POST"])
def run_daily_pipeline():
    try:
        cfg = PipelineConfig.from_env()
        cfg.ensure_dirs()
        DataPipeline(cfg=cfg, logger=logger).run_daily()
        return jsonify({"status": "✅ Daily pipeline complete"}), 200
    except Exception as e:
        logger.error(f"Daily pipeline failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
@app.route("/run-historical", methods=["POST"])
def run_historical():
    """Force full historical run (no auth)."""
    try:
        _pipe().run_historical()
        return jsonify({"ok": True, "mode": "historical"}), 200
    except Exception as e:
        logger.error(f"Historical failed: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/run-daily", methods=["POST"])
def run_daily():
    """Daily run only."""
    try:
        pipe = _pipe()
        # If market file missing, mirror old behavior: do historical first.
        if not os.path.exists(pipe.cfg.market_path):
            logger.info("MarketStates_Data.csv not found → running historical first.")
            pipe.run_historical()
        else:
            pipe.run_daily()
        return jsonify({"ok": True, "mode": "daily"}), 200
    except Exception as e:
        logger.error(f"Daily failed: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/run-pipeline", methods=["POST"])
def run_pipeline():
    """Auto: historical if missing, otherwise daily."""
    try:
        _pipe().run_once()
        return jsonify({"ok": True, "mode": "auto"}), 200
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

# ----------------- downloads (use config paths, not data/) -----------------

@app.route("/download/market", methods=["GET"])
def download_market():
    cfg = _cfg()
    if not os.path.exists(cfg.market_path):
        return jsonify({"ok": False, "error": "MarketStates_Data.csv not found"}), 404
    return send_file(cfg.market_path, as_attachment=True)

@app.route("/download/indicators", methods=["GET"])
def download_indicators():
    cfg = _cfg()
    if not os.path.exists(cfg.indicator_path):
        return jsonify({"ok": False, "error": "MarketData_with_Indicators.csv not found"}), 404
    return send_file(cfg.indicator_path, as_attachment=True)

@app.route("/download/breadth", methods=["GET"])
def download_breadth():
    cfg = _cfg()
    if not os.path.exists(cfg.breadth_path):
        return jsonify({"ok": False, "error": "market_breadth.csv not found"}), 404
    return send_file(cfg.breadth_path, as_attachment=True)

# ----------------- optional: date-window run, like your old fetch endpoint -----------------

@app.route("/fetch-market-data", methods=["POST"])
def fetch_market_data():
    """
    Optional: run just the market-data historical fetch for a custom window.
    Uses the configured fetcher; writes to cfg.market_path.
    """
    try:
        from scripts.DataRetrieval_FMP import FmpMarketDataFetcher
        from pandas_market_calendars import get_calendar
        from dotenv import load_dotenv

        load_dotenv()
        cfg = _cfg()
        start_date = request.json.get("start_date", "2005-01-01")
        end_date = request.json.get("end_date") or datetime.today().strftime("%Y-%m-%d")
        api_key = os.getenv("FMP_API_KEY")
        if not api_key:
            return jsonify({"ok": False, "error": "Missing FMP_API_KEY"}), 500

        fetcher = FmpMarketDataFetcher(api_key, cfg.ticker_map, logger)
        df = fetcher.fetch_all(start_date, end_date)
        if df.empty:
            return jsonify({"ok": False, "error": "No data returned by FMP"}), 500

        valid = fetcher.get_valid_trading_days(start_date, end_date)
        df = df[df["Date"].isin(valid)].sort_values("Date")
        df.to_csv(cfg.market_path, index=False)
        logger.info(f"Saved market data {start_date}→{end_date} to {cfg.market_path}")
        return jsonify({"ok": True, "rows": int(len(df))}), 200
    except Exception as e:
        logger.error(f"fetch-market-data failed: {e}", exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

if __name__ == "__main__":
    # Dev server only; Railway will use gunicorn
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

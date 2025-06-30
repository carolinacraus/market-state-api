from flask import Flask, request, jsonify, send_file
import subprocess
import os
import sys
from scripts.logger import get_logger
from datetime import datetime

app = Flask(__name__)
logger = get_logger("flask_app")

@app.route("/")
def index():
    logger.info("Health check hit.")
    return "✅ Market State AI Microservice is running!"

@app.route("/fetch-market-data", methods=["POST"])
def fetch_market_data():
    try:
        start_date = request.json.get("start_date", "2005-01-01")
        end_date = request.json.get("end_date") or datetime.today().strftime("%Y-%m-%d")
        subprocess.run([sys.executable, "scripts/DataRetrieval_FMP.py", "--start", start_date, "--end", end_date], check=True)
        logger.info(f"Fetched market data from {start_date} to {end_date}")
        return jsonify({"status": f"Market data fetched from {start_date} to {end_date}"}), 200
    except subprocess.CalledProcessError as e:
        logger.error(f"Error fetching market data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/fetch-market-breadth", methods=["POST"])
def fetch_market_breadth():
    try:
        subprocess.run([sys.executable, "scripts/MarketBreadth_SQL.py"], check=True)
        logger.info("Fetched market breadth from SQL and merged it")
        return jsonify({"status": "Market breadth data fetched and merged into MarketStates_Data.csv"}), 200
    except subprocess.CalledProcessError as e:
        logger.error(f"Error fetching market breadth: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/run-indicators", methods=["POST"])
def run_indicators():
    try:
        subprocess.run([sys.executable, "scripts/calculators.py"], check=True)
        logger.info("Indicators calculated")
        return jsonify({"status": "Indicators calculated and saved"}), 200
    except subprocess.CalledProcessError as e:
        logger.error(f"Error calculating indicators: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/run-classification", methods=["POST"])
def run_classification():
    try:
        subprocess.run([sys.executable, "scripts/classify_markets.py"], check=True)
        logger.info("Market states classified")
        return jsonify({"status": "Market states classified and saved"}), 200
    except subprocess.CalledProcessError as e:
        logger.error(f"Error classifying market states: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/run-daily-pipeline", methods=["POST"])
def run_daily_pipeline():
    try:
        start_date = request.json.get("start_date", None)
        end_date = request.json.get("end_date") or datetime.today().strftime("%Y-%m-%d")
        cmd = [sys.executable, "scripts/update_daily_pipeline.py"]
        if start_date:
            cmd.append(start_date)
        if end_date:
            cmd.append(end_date)
        subprocess.run(cmd, check=True)
        logger.info(f"Daily pipeline run from {start_date or 'last update'} to {end_date}")
        return jsonify({"status": f"Full daily pipeline executed from {start_date or 'last update'} to {end_date}"}), 200
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running daily pipeline: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/download/<filename>", methods=["GET"])
def download_file(filename):
    try:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        file_path = os.path.join(base_dir, filename)

        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return jsonify({"error": f"{filename} does not exist"}), 404

        return send_file(file_path, as_attachment=True)
    except Exception as e:
        logger.error(f"Error sending file: {e}")
        return jsonify({"error": str(e)}), 500
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

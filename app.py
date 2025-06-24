# app.py
from flask import Flask, request, jsonify
import subprocess
import os

app = Flask(__name__)

@app.route("/")
def index():
    return "Market State AI Microservice is running!"

@app.route("/fetch-market-data", methods=["POST"])
def fetch_market_data():
    try:
        start_date = request.json.get("start_date", "2008-11-03")
        end_date = request.json.get("end_date", "2025-06-03")
        subprocess.run(["python3", "scripts/DataRetrieval_FMP.py", "--start", start_date, "--end", end_date], check=True)
        return jsonify({"status": f"Market data fetched from {start_date} to {end_date}"}), 200
    except subprocess.CalledProcessError as e:
        return jsonify({"error": str(e)}), 500

@app.route("/fetch-market-breadth", methods=["POST"])
def fetch_market_breadth():
    try:
        subprocess.run(["python3", "scripts/MarketBreadth_SQL.py"], check=True)
        return jsonify({"status": "Market breadth data fetched from SQL"}), 200
    except subprocess.CalledProcessError as e:
        return jsonify({"error": str(e)}), 500

@app.route("/merge-market-breadth", methods=["POST"])
def merge_market_breadth():
    try:
        subprocess.run(["python3", "scripts/merge_market_breadth.py"], check=True)
        return jsonify({"status": "Market breadth merged into MarketStates_Data.csv"}), 200
    except subprocess.CalledProcessError as e:
        return jsonify({"error": str(e)}), 500

@app.route("/run-indicators", methods=["POST"])
def run_indicators():
    try:
        subprocess.run(["python3", "scripts/calculators.py"], check=True)
        return jsonify({"status": "Indicators calculated and saved"}), 200
    except subprocess.CalledProcessError as e:
        return jsonify({"error": str(e)}), 500

@app.route("/run-classification", methods=["POST"])
def run_classification():
    try:
        subprocess.run(["python3", "scripts/classify_markets.py"], check=True)
        return jsonify({"status": "Market states classified and saved"}), 200
    except subprocess.CalledProcessError as e:
        return jsonify({"error": str(e)}), 500

@app.route("/run-daily-pipeline", methods=["POST"])
def run_daily_pipeline():
    try:
        start_date = request.json.get("start_date", None)
        end_date = request.json.get("end_date", None)
        cmd = ["python3", "scripts/update_daily_pipeline.py"]
        if start_date:
            cmd.append(start_date)
        if end_date:
            cmd.append(end_date)
        subprocess.run(cmd, check=True)
        return jsonify({"status": f"Full daily pipeline executed from {start_date or 'last update'} to {end_date or 'today'}"}), 200
    except subprocess.CalledProcessError as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

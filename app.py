from flask import Flask, request, jsonify, send_file
import subprocess
import os
import sys
from scripts.logger import get_logger
from datetime import datetime
import pyodbc
from scripts.data_retrieval  import daily_data_retrieval
from scripts.sql_upload import upload_market_states_system_a
from scripts.sql_upload import upload_market_states_system_b
app = Flask(__name__)
logger = get_logger("flask_app")

@app.route("/")
def index():
    logger.info("Health check hit.")
    return "Market State AI Microservice is running!"

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
        daily_data_retrieval()
        logger.info("Daily pipeline executed using in-memory daily_data_retrieval()")
        return jsonify({"status": "Daily pipeline completed successfully"}), 200
    except Exception as e:
        logger.error(f"Daily pipeline failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/update-local-files", methods=["POST"])
def update_local_files():
    try:
        daily_data_retrieval()
        logger.info("Local files updated via /update-local-files API route.")
        return jsonify({"status": "Local file update successful"}), 200
    except Exception as e:
        logger.error(f"Local file update failed: {e}", exc_info=True)
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

@app.route("/test-sql-connection", methods=["GET"])
def test_sql_connection():
    try:
        # Load from .env
        driver = os.getenv("SQL_DRIVER_MS")
        server = os.getenv("SQL_SERVER_MS")
        db = os.getenv("SQL_DATABASE_MS")
        user = os.getenv("SQL_UID_MS")
        pwd = os.getenv("SQL_PWD_MS")

        # Validate
        if not all([driver, server, db, user, pwd]):
            missing = [k for k in ["SQL_DRIVER_MS", "SQL_SERVER_MS", "SQL_DATABASE_MS", "SQL_UID_MS", "SQL_PWD_MS"] if not os.getenv(k)]
            return jsonify({"error": f"Missing env vars: {', '.join(missing)}"}), 500

        # Connect
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={db};"
            f"UID={user};"
            f"PWD={pwd};"
        )
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        cursor.execute("SELECT GETDATE()")
        result = cursor.fetchone()

        return jsonify({
            "status": "✅ SQL connection successful",
            "datetime": str(result[0]),
            "server": server,
            "database": db
        }), 200

    except Exception as e:
        logger.error(f"SQL connection failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/run-classify-upload-system-a", methods=["POST"])
def run_classify_upload_system_a():
    try:
        result = subprocess.run(
            [sys.executable, "scripts/scoring_Euclidean.py"],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info("System A classification completed.")
        logger.info(result.stdout)

        upload_market_states_system_a()
        logger.info("System A upload to SQL completed.")
        return jsonify({"status": "System A classification + upload complete"}), 200

    except subprocess.CalledProcessError as e:
        logger.error(f"System A script failed: {e.stderr}")
        return jsonify({"error": e.stderr}), 500

    except Exception as e:
        logger.error(f"System A pipeline failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/run-classify-upload-system-b", methods=["POST"])
def run_classify_upload_system_b():
    try:
        result = subprocess.run(
            [sys.executable, "scripts/scoring_Original.py"],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info("System B classification completed.")
        logger.info(result.stdout)

        upload_market_states_system_b()
        logger.info("System B upload to SQL completed.")
        return jsonify({"status": "System B classification + upload complete"}), 200

    except subprocess.CalledProcessError as e:
        logger.error(f"System B script failed: {e.stderr}")
        return jsonify({"error": e.stderr}), 500

    except Exception as e:
        logger.error(f"System B pipeline failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/upload-market-states-system-a", methods=["POST"])
def run_upload_system_a():
    try:
        upload_market_states_system_a()
        logger.info("System A market states uploaded to SQL from API call.")
        return jsonify({"status": "System A upload successful"}), 200
    except Exception as e:
        logger.error(f"System A SQL upload failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/upload-market-states-system-b", methods=["POST"])
def run_upload_system_b():
    try:
        upload_market_states_system_b()
        logger.info("System B market states uploaded to SQL from API call.")
        return jsonify({"status": "System B upload successful"}), 200
    except Exception as e:
        logger.error(f"System B SQL upload failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
# === Dedicated Download Routes ===

@app.route("/download/market-data", methods=["GET"])
def download_market_data():
    return _send_data_file("MarketStates_Data.csv")

@app.route("/download/indicators", methods=["GET"])
def download_indicators():
    return _send_data_file("MarketData_with_Indicators.csv")

@app.route("/download/states", methods=["GET"])
def download_states():
    return _send_data_file("MarketData_with_States.csv")

@app.route("/download/diagnostics", methods=["GET"])
def download_diagnostics():
    return _send_data_file("MarketStates_Diagnostics.txt")

@app.route("/download/states-txt", methods=["GET"])
def download_states_txt():
    return _send_data_file("MarketStates.txt")

@app.route("/download/logs/pipeline-crash", methods=["GET"])
def download_pipeline_crash_log():
    crash_path = os.path.join(os.path.dirname(__file__), "pipeline_crash_log.txt")
    if not os.path.exists(crash_path):
        return jsonify({"error": "No crash log found."}), 404
    return send_file(crash_path, as_attachment=True)

def _send_data_file(filename):
    try:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        file_path = os.path.join(base_dir, filename)

        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return jsonify({"error": f"{filename} does not exist"}), 404

        return send_file(file_path, as_attachment=True)
    except Exception as e:
        logger.error(f"Error sending file {filename}: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)

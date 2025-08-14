from flask import Flask, request, jsonify, send_file
import subprocess
import os
import sys
from scripts.logger import get_logger
from datetime import datetime, timedelta
import pandas as pd
from scripts.data_retrieval  import daily_data_retrieval
from scripts.plot_chart import generate_state_charts_pdf
from scripts.sql_upload import upload_market_state_to_api

app = Flask(__name__)
logger = get_logger("flask_app")



@app.route("/")
def index():
    logger.info("Health check hit.")
    return "Market State AI Microservice is running!"

#commenttest
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
        base_dir = os.path.dirname(os.path.abspath(__file__))
        market_path = os.path.join(base_dir, "data", "MarketStates_Data.csv")
        indicator_path = os.path.join(base_dir, "data", "MarketData_with_Indicators.csv")

        if not os.path.exists(market_path):
            return jsonify({"error": "MarketStates_Data.csv not found."}), 404

        # Determine date range from MarketStates_Data.csv
        df_existing = pd.read_csv(market_path, parse_dates=["Date"])
        last_date = df_existing["Date"].max()
        start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = datetime.today().strftime("%Y-%m-%d")

        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            msg = f" Dataset is already up to date. Last available date: {last_date.strftime('%Y-%m-%d')}."
            logger.info(msg)
            return jsonify({"status": msg}), 200

        logger.info(f" Running daily_data_retrieval for {start_date} to {end_date}")
        daily_data_retrieval()  # Handles everything internally

        msg = f"Ppeline complete. MarketStates_Data.csv and MarketData_with_Indicators.csv updated for {start_date} to {end_date} and pushed to GitHub."
        logger.info(msg)
        return jsonify({"status": msg}), 200

    except Exception as e:
        logger.error(f" Pipeline failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/upload-market-state-sql", methods=["POST"])
def upload_market_state_sql():
    try:
        system_name = request.json.get("system_name")
        if not system_name:
            return jsonify({"error": "Missing 'system_name'"}), 400

        result = upload_market_state_to_api(system_name)
        status_code = 200 if "status" in result else 500
        return jsonify(result), status_code

    except Exception as e:
        logger.error(f"Upload route failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/run-classify-system", methods=["POST"])
def run_classify_system():
    try:
        system_name = request.json.get("system_name")
        if not system_name:
            return jsonify({"error": "Missing 'system_name' in request body"}), 400

        os.environ["SYSTEM_NAME"] = system_name

        if system_name.lower() == "euclidean":
            script_to_run = "scripts/scoring_Euclidean.py"
        elif system_name.lower() == "original":
            script_to_run = "scripts/scoring_Original.py"

        else:
            return jsonify({"error": f"Unsupported system name: {system_name}"}), 400

        logger.info(f"Running classification script: {script_to_run}")
        result = subprocess.run(
            [sys.executable, script_to_run],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(result.stdout)

        return jsonify({
            "status": f"{system_name} classification complete",
            "stdout": result.stdout
        }), 200

    except subprocess.CalledProcessError as e:
        stderr = e.stderr or "No stderr output"
        logger.error(f"Classification subprocess failed:\n{stderr}")
        return jsonify({"error": stderr}), 500

    except Exception as e:
        logger.error(f"Classification route failed: {e}", exc_info=True)
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

@app.route("/generate-market-charts", methods=["POST"])
def generate_market_charts():
    try:
        pdf_path = generate_state_charts_pdf()
        return send_file(pdf_path, as_attachment=True)
    except Exception as e:
        logger.error(f"Chart PDF generation failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/download/states-chart", methods=["GET"])
def download_states_chart():
    try:
        chart_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "data", "SP500_Market_States_System_A.png"))

        if not os.path.exists(chart_path):
            logger.error(f"Chart file not found: {chart_path}")
            return jsonify({"error": "Market states chart not found"}), 404

        return send_file(chart_path, mimetype='image/png', as_attachment=True)
    except Exception as e:
        logger.error(f"Error sending chart image: {e}")
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



# # === Dedicated Download Routes ===

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

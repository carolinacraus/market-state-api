import os
import pandas as pd
import requests
from datetime import datetime
from scripts.logger import get_logger

API_BASE = "http://38.67.1.241:46221/v1"
API_KEY = "djmPfVBCricS/fG8CznCsKGYBtJmUk80urPZC2Yhca7/WHBS55rdOKf1vBZ5S6KvtJUANn+Tshs0L13h7J6axw=="

logger = get_logger("upload_api")


def upload_market_state_to_api(system_name: str) -> dict:
    system_name = system_name.lower()
    logger.info(f"🔄 Starting upload to API for system: {system_name}")

    # === System mappings ===
    list_id_map = {
        "euclidean": 1,
        "original": 2
    }
    file_map = {
        "euclidean": "MarketStates_Diagnostics_System_Euclidean.txt",
        "original": "MarketStates_Diagnostics_System_Original.txt"
    }

    if system_name not in list_id_map:
        logger.error(f"❌ Invalid system name: {system_name}")
        return {"error": f"Invalid system name '{system_name}'."}

    list_id = list_id_map[system_name]

    # === Resolve file path (../data/<file>) ===
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.abspath(os.path.join(base_dir, "..", "data"))
    filepath = os.path.join(data_dir, file_map[system_name])
    logger.info(f"📄 Resolved file path: {filepath}")

    if not os.path.exists(filepath):
        logger.error(f"❌ File does not exist: {filepath}")
        return {"error": f"Market state file not found: {filepath}"}

    # === Step 1: Fetch latest uploaded date from API ===
    direction_url = f"{API_BASE}/MarketStates/{list_id}/Direction"
    headers = {
        "accept": "application/json",
        "X-API-Key": API_KEY
    }

    try:
        response = requests.get(direction_url, headers=headers)
        response.raise_for_status()
        existing_data = response.json()
    except Exception as e:
        logger.exception("❌ Failed to fetch existing data from API")
        return {"error": f"Failed to fetch existing data: {str(e)}"}

    latest_date = pd.to_datetime("1900-01-01")
    if existing_data:
        latest_date = max(pd.to_datetime(entry["date"]).normalize() for entry in existing_data)
        logger.info(f"📅 Last date in API: {latest_date.strftime('%Y-%m-%d')}")

    # === Step 2: Read local .txt file ===
    try:
        df = pd.read_csv(filepath, sep=",")
    except Exception as e:
        logger.exception("❌ Failed to read local file")
        return {"error": f"Failed to read local file: {e}"}

    if "date" not in df.columns or "direction" not in df.columns:
        logger.error("❌ Required columns missing in file: 'date' or 'direction'")
        return {"error": "Required columns 'date' or 'direction' not found in file."}

    df["date"] = pd.to_datetime(df["date"]).dt.normalize()

    # === Step 3: Filter new entries ===
    new_entries = df[df["date"] > latest_date].copy()
    if new_entries.empty:
        msg = f"✅ No new entries to upload. Latest date: {latest_date.date()}"
        logger.info(msg)
        return {"status": msg}

    logger.info(f"🆕 Found {len(new_entries)} new entries to upload")

    # === Step 4: Format payload with DB-compatible datetime ===
    payload = [
        {
            "date": row["date"].strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",  # e.g. 2025-07-23T00:00:00.000Z
            "direction": int(row["direction"])
        }
        for _, row in new_entries.iterrows()
    ]

    # === Step 5: Upload to API ===
    try:
        post_response = requests.post(
            f"{API_BASE}/MarketStates/{list_id}/Direction",
            json=payload,
            headers={
                "Content-Type": "application/json",
                "X-API-Key": API_KEY
            }
        )
        post_response.raise_for_status()
    except Exception as e:
        logger.exception("❌ Upload to API failed")
        return {
            "error": f"❌ Upload failed: {str(e)}",
            "payload_sample": payload[:3]
        }

    logger.info(f"✅ Successfully uploaded {len(payload)} entries")
    return {
        "status": f"✅ Uploaded {len(payload)} new entries.",
        "from_date": new_entries['date'].min().strftime("%Y-%m-%d"),
        "to_date": new_entries['date'].max().strftime("%Y-%m-%d")
    }

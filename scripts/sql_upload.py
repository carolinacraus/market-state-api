import os
import pandas as pd
import requests
from datetime import datetime

API_BASE = "http://38.67.1.241:46221/v1"
API_KEY = "djmPfVBCricS/fG8CznCsKGYBtJmUk80urPZC2Yhca7/WHBS55rdOKf1vBZ5S6KvtJUANn+Tshs0L13h7J6axw=="


def upload_market_state_to_api(system_name: str) -> dict:
    system_name = system_name.lower()

    # === System mappings ===
    list_id_map = {
        "euclidean": 1,
        "original": 2
    }
    file_map = {
        "euclidean": "data/MarketStates_Diagnostics_System_Euclidean.txt",
        "original": "data/MarketStates_Diagnostics_System_Original.txt"
    }

    if system_name not in list_id_map:
        return {"error": f"Invalid system name '{system_name}'."}

    list_id = list_id_map[system_name]
    filepath = file_map[system_name]

    if not os.path.exists(filepath):
        return {"error": f"Market state file not found: {filepath}"}

    # === Step 1: Get latest uploaded date from API ===
    direction_url = f"{API_BASE}/MarketStates/{list_id}/Direction"
    headers = {
        "accept": "application/json",
        "X-API-Key": API_KEY
    }

    response = requests.get(direction_url, headers=headers)
    if response.status_code != 200:
        return {"error": f"Failed to fetch existing data: {response.text}"}

    existing_data = response.json()
    latest_date = pd.to_datetime("1900-01-01")
    if existing_data:
        latest_date = max(pd.to_datetime(entry["date"]).normalize() for entry in existing_data)

    # === Step 2: Load local .txt file ===
    try:
        df = pd.read_csv(filepath, sep=", ", engine="python")
    except Exception as e:
        return {"error": f"Failed to read local diagnostics file: {e}"}

    if "date" not in df.columns or "direction" not in df.columns:
        return {"error": "Required columns 'date' or 'direction' not found in diagnostics file."}

    df["date"] = pd.to_datetime(df["date"]).dt.normalize()

    # === Step 3: Filter for new entries ===
    new_entries = df[df["date"] > latest_date].copy()
    if new_entries.empty:
        return {"status": f"No new entries to upload. Latest date: {latest_date.date()}"}

    # === Step 4: Format payload ===
    payload = [
        {
            "date": row["date"].isoformat(),
            "direction": int(row["direction"])
        }
        for _, row in new_entries.iterrows()
    ]

    # === Step 5: Upload to API ===
    post_response = requests.post(
        f"{API_BASE}/MarketStates/{list_id}/Direction",
        json=payload,
        headers={
            "Content-Type": "application/json",
            "X-API-Key": API_KEY
        }
    )

    if post_response.status_code == 200:
        return {
            "status": f"✅ Uploaded {len(payload)} new entries.",
            "from_date": new_entries['date'].min().strftime("%Y-%m-%d"),
            "to_date": new_entries['date'].max().strftime("%Y-%m-%d")
        }
    else:
        return {
            "error": f"❌ Upload failed: {post_response.text}",
            "payload_sample": payload[:3]
        }

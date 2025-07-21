# scripts/upload_to_api.py

import os
import pandas as pd
import requests
from datetime import datetime

API_BASE = "http://38.67.1.241:46221/v1"
API_KEY = "djmPfVBCricS/fG8CznCsKGYBtJmUk80urPZC2Yhca7/WHBS55rdOKf1vBZ5S6KvtJUANn+Tshs0L13h7J6axw=="


def upload_market_state_to_api(system_name: str) -> dict:
    system_name = system_name.lower()

    # Map system names to API IDs and file paths
    list_id_map = {
        "euclidean": 1,
        "original": 2
    }
    file_map = {
        "euclidean": "data/MarketStates_System_Euclidean.txt",
        "original": "data/MarketStates_System_Original.txt"
    }

    if system_name not in list_id_map:
        return {"error": f"Invalid system name '{system_name}'."}

    list_id = list_id_map[system_name]
    filepath = file_map[system_name]

    if not os.path.exists(filepath):
        return {"error": f"Market state file not found: {filepath}"}

    # Step 1: Fetch last uploaded date from API
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
        latest_date = max(pd.to_datetime(entry["date"]) for entry in existing_data)

    # Step 2: Load and filter local data
    df = pd.read_csv(filepath, sep="\t")
    df["date"] = pd.to_datetime(df["date"])

    if "direction" not in df.columns:
        if "state_code" in df.columns:
            df["direction"] = df["state_code"]
        else:
            return {"error": "Missing 'direction' or 'state_code' column in local file."}

    new_entries = df[df["date"] > latest_date]
    if new_entries.empty:
        return {"status": "No new entries to upload."}

    # Step 3: Format payload and upload
    payload = [
        {"date": row["date"].isoformat(), "direction": int(row["direction"])}
        for _, row in new_entries.iterrows()
    ]

    post_response = requests.post(
        f"{API_BASE}/MarketStates/{list_id}/Direction",
        json=payload,
        headers={
            "Content-Type": "application/json",
            "X-API-Key": API_KEY
        }
    )

    if post_response.status_code == 200:
        return {"status": f"Uploaded {len(payload)} new entries."}
    else:
        return {"error": f"Upload failed: {post_response.text}"}

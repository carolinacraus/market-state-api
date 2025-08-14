import requests
import pandas as pd
from datetime import datetime
import os

API_BASE = "http://38.67.1.241:46221/v1"
API_KEY = "djmPfVBCricS/fG8CznCsKGYBtJmUk80urPZC2Yhca7/WHBS55rdOKf1vBZ5S6KvtJUANn+Tshs0L13h7J6axw=="
# === Determine the data folder and output file path ===
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # parent of scripts
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

OUTPUT_FILE = os.path.join(DATA_DIR, "market_breadth.csv")

TICKERS = {
    "$NYAD.N": "NYAD",
    "$NYMO.N": "NYMO"
}

HEADERS = {"accept": "application/json", "X-API-Key": API_KEY}

def fetch_data(ticker, start_date, num_recs=None):
    """Fetch data from API for a given ticker"""
    url = f"{API_BASE}/Tickers/{ticker}/price_history"
    params = {
        "start": start_date,
        "numRecs": num_recs,
        "timeframe": "D"  # daily
    }
    resp = requests.get(url, params=params, headers=HEADERS)
    print(f"Fetching {ticker}: HTTP {resp.status_code}")

    if resp.status_code != 200 or not resp.text.strip():
        print(f"⚠️ Empty or invalid response for {ticker} starting {start_date}")
        return pd.DataFrame()

    try:
        data = resp.json()
    except Exception:
        print(f"⚠️ JSON decode failed for {ticker}. Response preview:")
        print(resp.text[:500])
        return pd.DataFrame()

    df = pd.DataFrame(data)
    if df.empty:
        return df

    df.rename(columns={
        "open": f"Open_{TICKERS[ticker]}",
        "high": f"High_{TICKERS[ticker]}",
        "low": f"Low_{TICKERS[ticker]}",
        "close": f"Close_{TICKERS[ticker]}",
        "volume": f"Volume_{TICKERS[ticker]}",
        "date": "Date"
    }, inplace=True)

    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%-m/%-d/%Y")
    return df[["Date",
               f"Open_{TICKERS[ticker]}", f"High_{TICKERS[ticker]}",
               f"Low_{TICKERS[ticker]}", f"Close_{TICKERS[ticker]}",
               f"Volume_{TICKERS[ticker]}"]]


def gather_historical():
    """Gather historical data from 2005-01-01 to 2025-08-01"""
    start_date = "2005-01-01"
    all_data = None

    for ticker in TICKERS:
        df = fetch_data(ticker, start_date)
        if all_data is None:
            all_data = df
        else:
            all_data = pd.merge(all_data, df, on="Date", how="outer")

    all_data.sort_values("Date", inplace=True)
    print(all_data)
    all_data.to_csv(OUTPUT_FILE, index=False)
    print(f"Historical data saved to {OUTPUT_FILE}")


def daily_update_function():
    """Update CSV with new data from the last date available"""
    if not os.path.exists(OUTPUT_FILE):
        print("No historical CSV found. Gathering historical data first...")
        gather_historical()
        return

    existing = pd.read_csv(OUTPUT_FILE)
    existing["Date_dt"] = pd.to_datetime(existing["Date"])
    last_date = existing["Date_dt"].max()
    start_date = (last_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Fetching new data starting from {start_date}...")

    new_data = None
    for ticker in TICKERS:
        df = fetch_data(ticker, start_date)
        if df.empty:
            continue
        if new_data is None:
            new_data = df
        else:
            new_data = pd.merge(new_data, df, on="Date", how="outer")

    if new_data is None or new_data.empty:
        print("No new data available.")
        return

    updated = pd.concat([existing.drop(columns="Date_dt"), new_data], ignore_index=True)
    updated.sort_values("Date", inplace=True)
    updated.to_csv(OUTPUT_FILE, index=False)
    print(f"CSV updated: {OUTPUT_FILE}")


if __name__ == "__main__":
    # Example: Run historical first, then daily updates
    if not os.path.exists(OUTPUT_FILE):
        gather_historical()
    else:
        daily_update_function()

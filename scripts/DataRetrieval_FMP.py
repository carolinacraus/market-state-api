# scripts/DataRetrieval_FMP.py

import requests
import pandas as pd
import os
import pandas_market_calendars as mcal
from datetime import datetime
import argparse

API_KEY = os.getenv("FMP_API_KEY")

TICKER_MAP = {
    "^GSPC": "SP500",
    "^TNX": "Yield",
    "DX-Y.NYB": "DXY",
    "CL=F": "Oil",
    "HG=F": "Copper",
    "GC=F": "Gold",
    "^VIX": "VIX",
    "RSP": "RSP"
}

def get_valid_trading_days(start_date, end_date):
    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(start_date=start_date, end_date=end_date)
    return pd.to_datetime(schedule.index)

def fetch_ticker_data(ticker, start_date, end_date):
    print(f"Fetching: {ticker}")
    symbol = ticker.replace("^", "%5E").replace("=", "%3D")
    url = (
        f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"
        f"?from={start_date}&to={end_date}&apikey={API_KEY}"
    )

    response = requests.get(url)
    if response.status_code != 200:
        print(f"❌ Failed to fetch {ticker}: {response.text}")
        return None

    data = response.json()
    if "historical" not in data:
        print(f"⚠️ No historical data found for {ticker}")
        return None

    df = pd.DataFrame(data["historical"])
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['date'].dt.weekday < 5]
    df.sort_values("date", inplace=True)

    short_name = TICKER_MAP[ticker]
    df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
    df.columns = ['Date', f'Open_{short_name}', f'High_{short_name}', f'Low_{short_name}', f'Close_{short_name}', f'Volume_{short_name}']

    return df

def fetch_all_tickers(tickers, start_date, end_date):
    all_data = None
    for ticker in tickers:
        df = fetch_ticker_data(ticker, start_date, end_date)
        if df is not None:
            all_data = df if all_data is None else pd.merge(all_data, df, on="Date", how="outer")
    return all_data

def save_market_data(start_date, end_date):
    tickers = list(TICKER_MAP.keys())
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    filepath = os.path.join(data_dir, "MarketStates_Data.csv")

    df = fetch_all_tickers(tickers, start_date, end_date)
    if df is not None:
        valid_days = get_valid_trading_days(start_date, end_date)
        df = df[df["Date"].isin(valid_days)]
        df.sort_values("Date", inplace=True)
        df.to_csv(filepath, index=False)
        print(f"✅ Saved MarketStates_Data.csv to {filepath}")
    else:
        print("⚠️ No data retrieved from FMP API.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build initial historical market dataset")
    parser.add_argument("--start", required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end", required=True, help="End date in YYYY-MM-DD format")
    args = parser.parse_args()

    save_market_data(args.start, args.end)

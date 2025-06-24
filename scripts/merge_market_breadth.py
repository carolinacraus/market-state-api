# scripts/merge_market_breadth.py

import pandas as pd
import os

def reformat_market_breadth():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")

    # Load market breadth data
    breadth_path = os.path.join(data_dir, "market_breadth_data_6-18.csv")
    df = pd.read_csv(breadth_path, parse_dates=["Date"])

    # Split and pivot by symbol
    df["Symbol"] = df["Symbol"].str.replace("$", "", regex=False)
    nyad = df[df["Symbol"] == "NYAD.N"].drop(columns="Symbol").copy()
    nymo = df[df["Symbol"] == "NYMO.N"].drop(columns="Symbol").copy()

    nyad = nyad.rename(columns={
        "Open": "Open_NYAD", "High": "High_NYAD", "Low": "Low_NYAD",
        "Close": "Close_NYAD", "Volume": "Volume_NYAD"
    })

    nymo = nymo.rename(columns={
        "Open": "Open_NYMO", "High": "High_NYMO", "Low": "Low_NYMO",
        "Close": "Close_NYMO", "Volume": "Volume_NYMO"
    })

    # Merge NYMO and NYAD
    merged = pd.merge(nymo, nyad, on="Date", how="outer")
    merged.sort_values("Date", inplace=True)
    merged_path = os.path.join(data_dir, "MarketData_NYAD_NYMO.csv")
    merged.to_csv(merged_path, index=False)

    # Merge with MarketStates_Data.csv
    market_path = os.path.join(data_dir, "MarketStates_Data.csv")
    market_df = pd.read_csv(market_path, parse_dates=["Date"])
    final_df = pd.merge(market_df, merged, on="Date", how="outer")
    final_df.to_csv(market_path, index=False)
    print(f"✅ Merged final dataset saved to {market_path}")

if __name__ == "__main__":
    reformat_market_breadth()
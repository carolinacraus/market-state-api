# update_daily_pipeline.py
# Full end-to-end market state pipeline for Railway deployment

import os
import pandas as pd
from datetime import datetime, timedelta
from scripts.DataRetrieval_FMP import fetch_all_tickers, get_valid_trading_days, TICKER_MAP
from scripts.MarketBreadth_SQL import fetch_breadth_data
from scripts.MergeBreadth_SQL import reformat_market_breadth
from scripts.calculate_indicators import calculate_all_indicators
from scripts.classify_markets import classify_market_state
from scripts.logger import get_logger

def update_pipeline(start_date=None, end_date=None):
    logger = get_logger("daily_update")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")

    market_path = os.path.join(data_dir, "MarketStates_Data.csv")
    indicator_path = os.path.join(data_dir, "MarketData_with_Indicators.csv")
    state_output_path = os.path.join(data_dir, "MarketData_with_States.csv")

    if not os.path.exists(market_path):
        logger.error("MarketStates_Data.csv not found. Run initial build first.")
        return

    df_existing = pd.read_csv(market_path, parse_dates=["Date"])
    df_existing.sort_values("Date", inplace=True)

    last_date = df_existing["Date"].max()
    start_date = start_date or (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = end_date or datetime.today().strftime("%Y-%m-%d")
    logger.info(f"Checking for new market data from {start_date} to {end_date}")

    df_new = fetch_all_tickers(list(TICKER_MAP.keys()), start_date, end_date)
    if df_new is None or df_new.empty:
        logger.info("No new FMP data available.")
        return

    valid_days = get_valid_trading_days(start_date, end_date)
    df_new = df_new[df_new["Date"].isin(valid_days)]
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    df_combined.drop_duplicates(subset=["Date"], keep="last", inplace=True)
    df_combined.sort_values("Date", inplace=True)
    df_combined.to_csv(market_path, index=False)
    logger.info("Appended new rows to MarketStates_Data.csv")

    fetch_breadth_data()
    reformat_market_breadth()

    df_merged = pd.read_csv(market_path, parse_dates=["Date"])
    df_indicators_existing = pd.read_csv(indicator_path, parse_dates=["Date"]) if os.path.exists(indicator_path) else pd.DataFrame()
    known_dates = set(df_indicators_existing["Date"]) if not df_indicators_existing.empty else set()

    df_new_rows = df_merged[~df_merged["Date"].isin(known_dates)]
    if df_new_rows.empty:
        logger.info("No new dates to compute indicators for.")
        return

    new_dates = df_new_rows["Date"].dt.strftime("%Y-%m-%d").tolist()
    logger.info(f"New dates to append: {', '.join(new_dates)}")

    tmp_input = os.path.join(data_dir, "temp_new_data.csv")
    tmp_output = os.path.join(data_dir, "temp_new_indicators.csv")
    df_new_rows.to_csv(tmp_input, index=False)
    calculate_all_indicators(tmp_input, tmp_output)

    df_new_indicators = pd.read_csv(tmp_output, parse_dates=["Date"])
    final_df = pd.concat([df_indicators_existing, df_new_indicators], ignore_index=True)
    final_df.drop_duplicates(subset=["Date"], keep="last", inplace=True)
    final_df.sort_values("Date", inplace=True)
    final_df.to_csv(indicator_path, index=False)
    logger.info(f"Appended {len(new_dates)} new row(s) to MarketData_with_Indicators.csv")

    os.remove(tmp_input)
    os.remove(tmp_output)
    logger.info("Temporary files cleaned up")

    # Classify final output
    df_classified = final_df.copy()
    df_classified[['MarketState', 'Score', 'Diagnostics']] = df_classified.apply(classify_market_state, axis=1)
    df_classified.to_csv(state_output_path, index=False)
    logger.info("✅ Market states classified and saved.")

if __name__ == "__main__":
    import sys
    start = sys.argv[1] if len(sys.argv) > 1 else None
    end = sys.argv[2] if len(sys.argv) > 2 else None
    update_pipeline(start, end)

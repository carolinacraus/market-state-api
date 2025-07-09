# scripts/historical_run.py

import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

from DataRetrieval_FMP import fetch_all_tickers, get_valid_trading_days, TICKER_MAP
from MarketBreadth_SQL import gather_market_breadth_data, reformat_breadth_data, merge_with_market_data
from calculate_indicators import calculate_all_indicators
from classify_markets import classify_market_states, append_to_txt_logs
from sql_upload import upload_market_states
from logger import get_logger

def run_historical_pipeline():
    logger = get_logger("historical_run")
    load_dotenv()

    # base_dir is the root of the project (one level above 'scripts/')
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")

    market_path = os.path.join(data_dir, "MarketStates_Data.csv")
    indicator_path = os.path.join(data_dir, "MarketData_with_Indicators.csv")
    state_output_path = os.path.join(data_dir, "MarketData_with_States.csv")
    states_txt = os.path.join(data_dir, "MarketStates.txt")
    diag_txt = os.path.join(data_dir, "MarketStates_Diagnostics.txt")

    start_date = "2005-01-01"
    end_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"Historical run from {start_date} to {end_date}")

    # Step 1: FMP Market Data
    try:
        df_market = fetch_all_tickers(list(TICKER_MAP.keys()), start_date, end_date)
        valid_days = get_valid_trading_days(start_date, end_date)
        df_market = df_market[df_market["Date"].isin(valid_days)]
        df_market.sort_values("Date", inplace=True)
        df_market.to_csv(market_path, index=False)
        logger.info(f"Saved {len(df_market)} rows to MarketStates_Data.csv")
    except Exception as e:
        logger.error(f"[FMP] Failed to retrieve historical market data: {e}")
        return

    # Step 2: Breadth Merge
    try:
        gather_market_breadth_data()
        reformat_breadth_data()
        merge_with_market_data()
        logger.info("Market breadth merged successfully")
    except Exception as e:
        logger.error(f"[Breadth] Merge failed: {e}")
        return

    # Step 3: Indicator Calculation
    try:
        calculate_all_indicators(market_path, indicator_path)
        logger.info("Technical indicators calculated")
    except Exception as e:
        logger.error(f"[Indicators] Failed: {e}")
        return

    # Step 4: Classification
    try:
        df = pd.read_csv(indicator_path, parse_dates=["Date"])
        df_classified = classify_market_states(df)
        df_classified.to_csv(state_output_path, index=False)
        append_to_txt_logs(df_classified, data_dir, logger)
        logger.info("Market states classified and written to all outputs")
    except Exception as e:
        logger.error(f"[Classification] Failed: {e}")
        return

    # Step 5: SQL Upload
    try:
        logger.info("Uploading historical market states to SQL...")
        upload_market_states(
            txt_file_path=states_txt,
            list_name="Market States 2005-Present Euclidean Scoring",
            list_description="Market States List 7-9 Euclidean Scoring"
        )

        logger.info("Market states uploaded to SQL successfully")
    except Exception as e:
        logger.error(f"[SQL Upload] Failed: {e}")
        return

    logger.info("Historical pipeline completed successfully")

if __name__ == "__main__":
    run_historical_pipeline()

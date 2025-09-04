import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

from DataRetrieval_FMP import fetch_all_tickers, get_valid_trading_days, TICKER_MAP
from JulyBuild.MarketBreadth_SQL import gather_market_breadth_data, reformat_breadth_data, merge_with_market_data
from calculate_indicators import calculate_all_indicators
from scoring_Euclidean import classify_market_states_system_a, append_to_txt_logs_system_a
from scoring_Original import classify_market_states_system_b, append_to_txt_logs_system_b
from sql_upload import upload_market_states_system_a, upload_market_states_system_b
from logger import get_logger

def run_historical_pipeline():
    logger = get_logger("historical_run")
    load_dotenv()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.abspath(os.path.join(base_dir, "..", "data"))

    market_path = os.path.join(data_dir, "MarketStates_Data.csv")
    indicator_path = os.path.join(data_dir, "MarketData_with_Indicators.csv")

    state_output_a = os.path.join(data_dir, "MarketData_with_States_System_A.csv")
    state_output_b = os.path.join(data_dir, "MarketData_with_States_System_B.csv")

    start_date = "2005-01-01"
    end_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"Historical run from {start_date} to {end_date}")

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

    try:
        gather_market_breadth_data()
        reformat_breadth_data()
        merge_with_market_data()
        logger.info("Market breadth merged successfully")
    except Exception as e:
        logger.error(f"[Breadth] Merge failed: {e}")
        return

    try:
        calculate_all_indicators(market_path, indicator_path)
        logger.info("Technical indicators calculated")
    except Exception as e:
        logger.error(f"[Indicators] Failed: {e}")
        return

    try:
        df = pd.read_csv(indicator_path, parse_dates=["Date"])

        df_classified_a = classify_market_states_system_a(df)
        df_classified_a.to_csv(state_output_a, index=False)
        append_to_txt_logs_system_a(df_classified_a, data_dir, logger)
        logger.info("System A: Market states classified and saved")

        df_classified_b = classify_market_states_system_b(df)
        df_classified_b.to_csv(state_output_b, index=False)
        append_to_txt_logs_system_b(df_classified_b, data_dir, logger)
        logger.info("System B: Market states classified and saved")

    except Exception as e:
        logger.error(f"[Classification] Failed: {e}")
        return

    try:
        logger.info("Uploading System A market states to SQL...")
        upload_market_states_system_a()
        logger.info("System A market states uploaded")

        logger.info("Uploading System B market states to SQL...")
        upload_market_states_system_b()
        logger.info("System B market states uploaded")

    except Exception as e:
        logger.error(f"[SQL Upload] Failed: {e}")
        return

    logger.info("Historical pipeline completed successfully")

if __name__ == "__main__":
    run_historical_pipeline()

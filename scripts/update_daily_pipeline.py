# Full end-to-end market state pipeline for Railway deployment

import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import sys

from DataRetrieval_FMP import fetch_all_tickers, get_valid_trading_days, TICKER_MAP
from MarketBreadth_SQL import gather_market_breadth_data, reformat_breadth_data, merge_with_market_data
from calculate_indicators import calculate_all_indicators
from classify_markets import classify_market_states
from logger import get_logger
from sql_upload import upload_market_states
from classify_markets import classify_market_states, append_to_txt_logs


with open("pipeline_crash_log.txt", "a") as f:
    f.write(f"\n=== Pipeline Triggered ===\n")
    f.write(f"Args: {sys.argv}\n")


def update_pipeline(start_date=None, end_date=None):
    logger = get_logger("daily_update")
    load_dotenv()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")

    market_path = os.path.join(data_dir, "MarketStates_Data.csv")
    indicator_path = os.path.join(data_dir, "MarketData_with_Indicators.csv")
    state_output_path = os.path.join(data_dir, "MarketData_with_States.csv")

    logger.info(f"Starting pipeline with start_date={start_date}, end_date={end_date}")

    # Step 1: Load and update market data
    try:
        if not os.path.exists(market_path):
            logger.error("MarketStates_Data.csv not found. Run initial build first.")
            return

        df_existing = pd.read_csv(market_path, parse_dates=["Date"])
        df_existing.sort_values("Date", inplace=True)

        last_date = df_existing["Date"].max()
        start_date = start_date or (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = end_date or datetime.today().strftime("%Y-%m-%d")
        logger.info(f"Fetching FMP market data from {start_date} to {end_date}")

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
        logger.info(f"Appended {len(df_new)} new row(s) to MarketStates_Data.csv")

    except Exception as e:
        logger.error(f"[Step 1 - FMP fetch] failed: {e}")
        return

    # Step 2: Market breadth SQL and merge
    try:
        logger.info("Gathering market breadth data from SQL")
        gather_market_breadth_data()
        reformat_breadth_data()
        merge_with_market_data()
        logger.info("Market breadth merged successfully")
    except Exception as e:
        logger.error(f"[Step 2 - Breadth merge] failed: {e}")
        return

    # Step 3: Calculate indicators
    try:
        df_merged = pd.read_csv(market_path, parse_dates=["Date"])

        if os.path.exists(indicator_path):
            df_indicators_existing = pd.read_csv(indicator_path, parse_dates=["Date"])
            known_dates = set(df_indicators_existing["Date"])
        else:
            df_indicators_existing = pd.DataFrame()
            known_dates = set()

        df_new_rows = df_merged[~df_merged["Date"].isin(known_dates)]
        if df_new_rows.empty:
            logger.info("No new dates to compute indicators for.")
            return

        new_dates = df_new_rows["Date"].dt.strftime("%Y-%m-%d").tolist()
        logger.info(f"Computing indicators for {len(new_dates)} new dates: {', '.join(new_dates)}")

        tmp_input = os.path.join(data_dir, "temp_new_data.csv")
        tmp_output = os.path.join(data_dir, "temp_new_indicators.csv")

        df_new_rows.to_csv(tmp_input, index=False)
        calculate_all_indicators(tmp_input, tmp_output)

        df_new_indicators = pd.read_csv(tmp_output, parse_dates=["Date"])
        final_df = pd.concat([df_indicators_existing, df_new_indicators], ignore_index=True)
        final_df.drop_duplicates(subset=["Date"], keep="last", inplace=True)
        final_df.sort_values("Date", inplace=True)
        final_df.to_csv(indicator_path, index=False)
        logger.info(f"Appended indicators for {len(new_dates)} date(s) to MarketData_with_Indicators.csv")

        os.remove(tmp_input)
        os.remove(tmp_output)
        logger.info("Temporary files cleaned up")

    except Exception as e:
        logger.error(f"[Step 3 - Indicators] failed: {e}")
        return

    # Step 4: Classify final market states
    try:
        df_classified = final_df.copy()
        df_classified[['MarketState', 'Score', 'Diagnostics']] = df_classified.apply(classify_market_states, axis=1)
        df_classified.to_csv(state_output_path, index=False)
        logger.info(f"Market states classified and saved to {state_output_path}")

    except Exception as e:
        logger.error(f"[Step 4 - Classification] failed: {e}")
        return

    # Step 5: Upload to SQL
    # try:
    #     logger.info("Uploading market states to SQL...")
    #     upload_market_states_to_sql(
    #         csv_path=state_output_path,
    #         list_name="Market States 2024 Euclidean",
    #         list_description="Historical Market States List 2024 Euclidean Scoring System 6-19-2025"
    #     )
    #     logger.info("Market states uploaded to SQL successfully")
    # except Exception as e:
    #     logger.error(f"[Step 5 - SQL Upload] failed: {e}")


    logger.info("Full pipeline completed successfully")

if __name__ == "__main__":
    start = sys.argv[1] if len(sys.argv) > 1 else None
    end = sys.argv[2] if len(sys.argv) > 2 else None
    try:
        update_pipeline(start, end)
    except Exception as e:
        import traceback
        with open("pipeline_crash_log.txt", "a") as f:
            f.write("Exception caught:\n")
            traceback.print_exc(file=f)
        raise


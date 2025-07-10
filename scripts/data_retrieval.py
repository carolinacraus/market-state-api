import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

from scripts.DataRetrieval_FMP import fetch_all_tickers, get_valid_trading_days, TICKER_MAP
from scripts.MarketBreadth_SQL import gather_market_breadth_data, reformat_breadth_data, merge_with_market_data
from scripts.calculate_indicators import calculate_all_indicators
from scripts.logger import get_logger
from scripts.google_drive_uploader import upload_to_drive

load_dotenv()
logger = get_logger("data_retrieval")

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(base_dir, "data")
drive_folder_id = os.getenv("DRIVE_FOLDER_ID")



def historical_data_retrieval():
    logger.info("Running historical data retrieval...")

    market_path = os.path.join(data_dir, "MarketStates_Data.csv")
    indicator_path = os.path.join(data_dir, "MarketData_with_Indicators.csv")

    start_date = "2005-01-01"
    end_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    try:
        df_market = fetch_all_tickers(list(TICKER_MAP.keys()), start_date, end_date)
        valid_days = get_valid_trading_days(start_date, end_date)
        df_market = df_market[df_market["Date"].isin(valid_days)]
        df_market.sort_values("Date", inplace=True)
        df_market.to_csv(market_path, index=False)
        logger.info(f"Saved {len(df_market)} rows to MarketStates_Data.csv")

        gather_market_breadth_data()
        reformat_breadth_data()
        merge_with_market_data()
        logger.info("Market breadth merged successfully")

        calculate_all_indicators(market_path, indicator_path)
        logger.info("Technical indicators calculated")

        # Upload files to Google Drive (initial upload)
        upload_to_drive(market_path, drive_folder_id)
        upload_to_drive(indicator_path, drive_folder_id)

    except Exception as e:
        logger.error(f"[Historical] Data retrieval failed: {e}")


def daily_data_retrieval():
    logger.info("Running daily data retrieval...")

    market_path = os.path.join(data_dir, "MarketStates_Data.csv")
    indicator_path = os.path.join(data_dir, "MarketData_with_Indicators.csv")

    if not os.path.exists(market_path):
        logger.error("MarketStates_Data.csv not found. Run historical_data_retrieval first.")
        return

    try:
        df_existing = pd.read_csv(market_path, parse_dates=["Date"])
        df_existing.sort_values("Date", inplace=True)

        last_date = df_existing["Date"].max()
        start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = datetime.today().strftime("%Y-%m-%d")

        df_new = fetch_all_tickers(list(TICKER_MAP.keys()), start_date, end_date)
        valid_days = get_valid_trading_days(start_date, end_date)
        df_new = df_new[df_new["Date"].isin(valid_days)]

        if df_new.empty:
            logger.info("No new market data to append.")
            return

        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        df_combined.drop_duplicates(subset=["Date"], keep="last", inplace=True)
        df_combined.sort_values("Date", inplace=True)
        df_combined.to_csv(market_path, index=False)
        logger.info(f"Appended {len(df_new)} new row(s) to MarketStates_Data.csv")

        if os.path.exists(indicator_path):
            df_existing_ind = pd.read_csv(indicator_path, parse_dates=["Date"])
            known_dates = set(df_existing_ind["Date"])
        else:
            df_existing_ind = pd.DataFrame()
            known_dates = set()

        df_new_rows = df_combined[~df_combined["Date"].isin(known_dates)]
        if df_new_rows.empty:
            logger.info("No new dates to compute indicators for.")
            return

        tmp_input = os.path.join(data_dir, "temp_new_data.csv")
        tmp_output = os.path.join(data_dir, "temp_new_indicators.csv")
        df_new_rows.to_csv(tmp_input, index=False)
        calculate_all_indicators(tmp_input, tmp_output)

        df_new_indicators = pd.read_csv(tmp_output, parse_dates=["Date"])
        final_df = pd.concat([df_existing_ind, df_new_indicators], ignore_index=True)
        final_df.drop_duplicates(subset=["Date"], keep="last", inplace=True)
        final_df.sort_values("Date", inplace=True)
        final_df.to_csv(indicator_path, index=False)
        logger.info(f"Appended indicators for {len(df_new_indicators)} new date(s)")

        os.remove(tmp_input)
        os.remove(tmp_output)

        # Upload updated files to Google Drive
        upload_to_drive(market_path, drive_folder_id)
        upload_to_drive(indicator_path, drive_folder_id)

    except Exception as e:
        logger.error(f"[Daily] Data retrieval failed: {e}")


if __name__ == "__main__":
    historical_data_retrieval()
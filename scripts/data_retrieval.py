import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

from scripts.DataRetrieval_FMP import fetch_all_tickers, get_valid_trading_days, TICKER_MAP
from scripts.market_breadth import gather_historical as gather_market_breadth_data, daily_update_function as update_market_breadth
from scripts.calculate_indicators import calculate_all_indicators
from scripts.logger import get_logger
#from scripts.github_upload import upload_to_github, create_github_tag

load_dotenv()
logger = get_logger("data_retrieval")

# === Paths ===
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(base_dir, "data")

market_path = os.path.join(data_dir, "MarketStates_Data.csv")
indicator_path = os.path.join(data_dir, "MarketData_with_Indicators.csv")
breadth_path = os.path.join(data_dir, "market_breadth.csv")


def merge_with_market_data():
    """Merge market breadth CSV into MarketStates_Data.csv safely"""
    if not os.path.exists(market_path) or not os.path.exists(breadth_path):
        logger.error("❌ Cannot merge: Missing market or breadth CSV.")
        return

    df_market = pd.read_csv(market_path, parse_dates=["Date"])
    df_breadth = pd.read_csv(breadth_path, parse_dates=["Date"])

    # Drop any breadth columns already in df_market to avoid _x/_y
    breadth_cols = [c for c in df_breadth.columns if c != "Date"]
    df_market.drop(columns=[c for c in df_market.columns if c in breadth_cols], inplace=True)

    # Merge cleanly
    df_merged = pd.merge(df_market, df_breadth, on="Date", how="left")

    # Sort and save
    df_merged.sort_values("Date", inplace=True)
    df_merged.to_csv(market_path, index=False)
    logger.info(f"✅ Market breadth merged into MarketStates_Data.csv ({len(df_merged)} rows). Clean columns: {len(df_merged.columns)}")


def merge_indicators_into_market():
    """Merge indicator CSV into MarketStates_Data.csv safely"""
    if not os.path.exists(market_path) or not os.path.exists(indicator_path):
        logger.warning("⚠️ Cannot merge indicators: Missing MarketStates_Data.csv or indicator CSV.")
        return

    df_market = pd.read_csv(market_path, parse_dates=["Date"])
    df_ind = pd.read_csv(indicator_path, parse_dates=["Date"])

    # Drop duplicate indicator columns to avoid _x/_y
    indicator_cols = [c for c in df_ind.columns if c != "Date"]
    df_market.drop(columns=[c for c in df_market.columns if c in indicator_cols], inplace=True)

    # Merge
    df_merged = pd.merge(df_market, df_ind, on="Date", how="left")
    df_merged.sort_values("Date", inplace=True)
    df_merged.to_csv(market_path, index=False)
    logger.info(f"✅ Indicators merged into MarketStates_Data.csv ({len(df_merged)} rows). Columns: {len(df_merged.columns)}")


def historical_data_retrieval():
    logger.info("🚀 Running historical data retrieval...")

    start_date = "2005-01-01"
    end_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    try:
        # === 1. Fetch main market data ===
        df_market = fetch_all_tickers(list(TICKER_MAP.keys()), start_date, end_date)
        valid_days = get_valid_trading_days(start_date, end_date)
        df_market = df_market[df_market["Date"].isin(valid_days)]
        df_market.sort_values("Date", inplace=True)
        df_market.to_csv(market_path, index=False)
        logger.info(f"Saved {len(df_market)} rows to MarketStates_Data.csv")

        # === 2. Fetch market breadth data and merge ===
        gather_market_breadth_data()
        merge_with_market_data()

        # === 3. Calculate technical indicators ===
        calculate_all_indicators(market_path, indicator_path)
        logger.info("✅ Technical indicators calculated and saved")

        # === 4. Merge indicators into MarketStates_Data.csv ===
        merge_indicators_into_market()

        # === 5. Upload initial data to GitHub ===
        upload_to_github(market_path, "carolinacraus/market-state-api", "data/MarketStates_Data.csv", "Initial historical market data upload")
        upload_to_github(indicator_path, "carolinacraus/market-state-api", "data/MarketData_with_Indicators.csv", "Initial historical indicators upload")

    except Exception as e:
        logger.error(f"[Historical] Data retrieval failed: {e}", exc_info=True)


def daily_data_retrieval():
    logger.info("⏳ Starting daily data retrieval...")

    if not os.path.exists(market_path):
        logger.error("MarketStates_Data.csv not found. Run historical_data_retrieval first.")
        return

    try:
        # === 1. Load existing market data ===
        df_existing = pd.read_csv(market_path, parse_dates=["Date"])
        df_existing.sort_values("Date", inplace=True)

        last_date = df_existing["Date"].max()
        start_date_dt = last_date + timedelta(days=1)
        end_date_dt = datetime.today()

        # ✅ Skip if no new trading days
        if start_date_dt > end_date_dt:
            logger.info(f"⏸ No new trading days to fetch. Last date: {last_date.date()}, today: {end_date_dt.date()}")
            return

        start_date = start_date_dt.strftime("%Y-%m-%d")
        end_date = end_date_dt.strftime("%Y-%m-%d")
        logger.info(f"Fetching market data from {start_date} to {end_date}")

        # === 2. Fetch new market data ===
        df_new = fetch_all_tickers(list(TICKER_MAP.keys()), start_date, end_date)
        valid_days = get_valid_trading_days(start_date, end_date)
        df_new = df_new[df_new["Date"].isin(valid_days)]

        # === 3. Merge with existing market data ===
        if df_new.empty:
            logger.info("✅ No new market data to append.")
        else:
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined.drop_duplicates(subset=["Date"], keep="last", inplace=True)
            df_combined.sort_values("Date", inplace=True)
            df_combined.to_csv(market_path, index=False)
            logger.info(f"✅ Appended {len(df_new)} new row(s) to MarketStates_Data.csv")

        # === 4. Update market breadth and merge ===
        update_market_breadth()
        merge_with_market_data()

        # === 5. Recalculate all indicators for the combined file ===
        calculate_all_indicators(market_path, indicator_path)
        merge_indicators_into_market()

        # === 6. Upload to GitHub ===
        # upload_to_github(market_path, "carolinacraus/market-state-api", "data/MarketStates_Data.csv", "📈 Daily update: market data + indicators")
        # upload_to_github(indicator_path, "carolinacraus/market-state-api", "data/MarketData_with_Indicators.csv", "📊 Daily update: indicators")
        #
        # # === 7. Tag commit ===
        # commit_sha = upload_to_github(
        #     market_path,
        #     "carolinacraus/market-state-api",
        #     "data/MarketStates_Data.csv",
        #     f"📈 Daily update: market data + indicators ({start_date} to {end_date})",
        #     branch="main"
        # )
        # if commit_sha:
        #     tag = f"update-{start_date}-to-{end_date}"
        #     message = f"Market update for {start_date} to {end_date}"
        #     create_github_tag(
        #         repo="carolinacraus/market-state-api",
        #         tag_name=tag,
        #         tag_message=message,
        #         commit_sha=commit_sha,
        #         branch="main"
        #     )

    except Exception as e:
        logger.error(f"[Daily] Data retrieval failed: {e}", exc_info=True)


if __name__ == "__main__":
    historical_data_retrieval()
    daily_data_retrieval()

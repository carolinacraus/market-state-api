import os
import argparse
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
import requests
from pandas_market_calendars import get_calendar

from market_pipeline.config import PipelineConfig
from scripts.logger import get_logger


class FmpMarketDataFetcher:
    def __init__(self, api_key: str, ticker_map: dict, logger):
        self.api_key = api_key
        self.ticker_map = ticker_map
        self.logger = logger
        self.calendar = get_calendar("NYSE")

    def get_valid_trading_days(self, start_date: str, end_date: str) -> pd.DatetimeIndex:
        schedule = self.calendar.schedule(start_date=start_date, end_date=end_date)
        return pd.to_datetime(schedule.index)

    def _build_url(self, ticker: str, start_date: str, end_date: str) -> str:
        symbol = requests.utils.quote(ticker, safe="")
        return (
            f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"
            f"?from={start_date}&to={end_date}&apikey={self.api_key}"
        )

    def fetch_ticker(self, ticker: str, start_date: str, end_date: str) -> pd.DataFrame | None:
        self.logger.info(f"Fetching data for {ticker}")
        try:
            resp = requests.get(self._build_url(ticker, start_date, end_date))
            resp.raise_for_status()
        except requests.RequestException as e:
            self.logger.error(f"Request failed for {ticker}: {e}")
            return None

        payload = resp.json()
        hist = payload.get("historical")
        if not hist:
            self.logger.warning(f"No historical data for {ticker}")
            return None

        df = pd.DataFrame(hist)
        df["Date"] = pd.to_datetime(df["date"])
        df = df[df["Date"].dt.weekday < 5].sort_values("Date")

        short = self.ticker_map.get(ticker, ticker)
        cols = ["open", "high", "low", "close", "volume"]
        df = df[["Date"] + cols]
        df.columns = ["Date"] + [f"{c.title()}_{short}" for c in cols]
        return df

    def fetch_all(self, start_date: str, end_date: str) -> pd.DataFrame:
        merged = None
        for ticker in self.ticker_map:
            df = self.fetch_ticker(ticker, start_date, end_date)
            if df is not None:
                merged = df if merged is None else merged.merge(df, on="Date", how="outer")
        return merged or pd.DataFrame()


def main():
    load_dotenv()
    config = PipelineConfig()
    logger = get_logger("fmp_data")
    api_key = os.getenv("FMP_API_KEY")

    if not api_key:
        logger.error("Missing FMP_API_KEY in environment")
        return

    parser = argparse.ArgumentParser(description="Fetch historical market data from FMP")
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end",   required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    fetcher = FmpMarketDataFetcher(api_key, config.ticker_map, logger)
    df = fetcher.fetch_all(args.start, args.end)
    if df.empty:
        logger.warning("No data retrieved; exiting.")
        return

    valid_days = fetcher.get_valid_trading_days(args.start, args.end)
    df = df[df["Date"].isin(valid_days)].sort_values("Date")

    os.makedirs(config.data_dir, exist_ok=True)
    out_path = os.path.join(config.data_dir, config.market_file)
    df.to_csv(out_path, index=False)
    logger.info(f"✅ Saved market data to {out_path}")


if __name__ == "__main__":
    main()

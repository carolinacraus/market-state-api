# market_pipeline/fetcher.py

from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd
from dotenv import load_dotenv

from market_pipeline.config import PipelineConfig
from scripts.DataRetrieval_FMP import FmpMarketDataFetcher
from scripts.logger import get_logger


class DataFetcher:
    """
    Orchestrates historical and daily market data2 fetches
    using FmpMarketDataFetcher under the hood.

    Responsibilities:
      - Determine date windows
      - Call upstream fetcher
      - Filter to valid trading days
      - Persist CSV to configured location
      - Log progress/results
    """

    def __init__(self, cfg: PipelineConfig | None = None, logger=None):
        load_dotenv()

        self.cfg = cfg or PipelineConfig.from_env()
        self.logger = logger or get_logger(__name__)
        self.cfg.ensure_dirs()

        api_key = os.getenv("FMP_API_KEY")
        if not api_key:
            self.logger.error("Missing FMP_API_KEY in environment")
            raise RuntimeError("FMP_API_KEY is required")

        if not self.cfg.ticker_map:
            # Fail fast if the ticker map wasn't loaded; keeps behavior explicit.
            self.logger.error("ticker_map is empty. Ensure config/ticker_map.yaml exists and is valid.")
            raise RuntimeError("ticker_map is required")

        self._fmp = FmpMarketDataFetcher(
            api_key=api_key,
            ticker_map=self.cfg.ticker_map,
            logger=self.logger
        )

    # -------- Public API --------

    def fetch_historical(self) -> bool:
        """Fetch full history and write to market CSV. Returns True if wrote data."""
        start = self.cfg.historical_start
        end = self.cfg.historical_end
        self.logger.info(f"Fetching historical market data: {start} → {end}")

        df = self._fmp.fetch_all(start, end)
        if df.empty:
            self.logger.warning("No data retrieved for historical fetch.")
            return False

        df = self._restrict_to_valid_days(df, start, end)
        if df.empty:
            self.logger.warning("No valid trading days in historical fetch window.")
            return False

        self._write_market_csv(df)
        self.logger.info(f"Historical saved: {len(df)} rows → {self.cfg.market_path}")
        return True

    def fetch_daily(self) -> bool:
        """Fetch new rows since last date; append; save. Returns True if new rows appended."""
        if not os.path.exists(self.cfg.market_path):
            self.logger.error(f"{self.cfg.market_path!r} not found; run fetch_historical() first.")
            return False

        df_existing = pd.read_csv(self.cfg.market_path, parse_dates=["Date"]).sort_values("Date")
        last_date = df_existing["Date"].max()
        next_date = last_date + timedelta(days=1)
        today = pd.to_datetime(date.today())

        if next_date > today:
            self.logger.info(f"No new trading days (last={last_date.date()}, today={today.date()})")
            return False

        start = next_date.strftime("%Y-%m-%d")
        end = today.strftime("%Y-%m-%d")
        self.logger.info(f"Fetching daily market data: {start} → {end}")

        df_new = self._fmp.fetch_all(start, end)
        if df_new.empty:
            self.logger.info("No new rows returned by FMP API.")
            return False

        df_new = self._restrict_to_valid_days(df_new, start, end)
        if df_new.empty:
            self.logger.info("New rows were outside valid trading days.")
            return False

        df_out = (
            pd.concat([df_existing, df_new], ignore_index=True)
              .drop_duplicates(subset=["Date"], keep="last")
              .sort_values("Date")
        )
        if len(df_out) == len(df_existing):
            self.logger.info("No net new rows after de-duplication.")
            return False

        df_out.to_csv(self.cfg.market_path, index=False)
        self.logger.info(f"Daily append complete: +{len(df_out) - len(df_existing)} rows → {self.cfg.market_path}")
        return True

    # -------- Internals --------

    def _restrict_to_valid_days(self, df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
        valid_days = self._fmp.get_valid_trading_days(start, end)
        return df[df["Date"].isin(valid_days)].sort_values("Date")

    def _write_market_csv(self, df: pd.DataFrame) -> None:
        df.to_csv(self.cfg.market_path, index=False)

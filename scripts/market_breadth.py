# scripts/market_breadth.py
from __future__ import annotations

import os
from typing import Dict, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

from market_pipeline.config import PipelineConfig
from scripts.logger import get_logger


DEFAULT_TICKERS: Dict[str, str] = {
    "$NYAD.N": "NYAD",
    "$NYMO.N": "NYMO",
}

TIMEFRAME = "D"  # daily bars from API


class BreadthClient:
    """Thin HTTP client for your breadth API."""
    def __init__(self, api_base: str, api_key: str, logger=None, timeout: float = 20.0):
        self.api_base = api_base.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.logger = logger or get_logger(__name__)

    def fetch(self, raw_symbol: str, start_date: str, num_recs: Optional[int] = None) -> pd.DataFrame:
        url = f"{self.api_base}/Tickers/{raw_symbol}/price_history"
        params = {"start": start_date, "timeframe": TIMEFRAME}
        if num_recs is not None:
            params["numRecs"] = num_recs

        headers = {
            "accept": "application/json",
            "X-API-Key": self.api_key,
        }

        try:
            resp = requests.get(url, params=params, headers=headers, timeout=self.timeout)
            resp.raise_for_status()
        except requests.RequestException as e:
            self.logger.error(f"HTTP error for {raw_symbol}: {e}")
            return pd.DataFrame()

        try:
            data = resp.json()
        except ValueError:
            self.logger.error(f"JSON decode failed for {raw_symbol}. Response head: {resp.text[:400]}")
            return pd.DataFrame()

        if not data:
            self.logger.warning(f"No data for {raw_symbol} from {start_date}")
            return pd.DataFrame()

        return pd.DataFrame(data)


def _shape_breadth_df(df: pd.DataFrame, short: str) -> pd.DataFrame:
    if df.empty:
        return df
    # Expect columns: date, open, high, low, close, volume
    rename = {
        "date": "Date",
        "open": f"Open_{short}",
        "high": f"High_{short}",
        "low": f"Low_{short}",
        "close": f"Close_{short}",
        "volume": f"Volume_{short}",
    }
    df = df.rename(columns=rename)
    if "Date" not in df:
        return pd.DataFrame()

    df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()  # keep as ISO yyyy-mm-dd on save
    keep_cols = ["Date", f"Open_{short}", f"High_{short}", f"Low_{short}", f"Close_{short}", f"Volume_{short}"]
    return df[keep_cols].sort_values("Date")


def gather_historical(
    start_date: str = "2005-01-01",
    *,
    cfg: PipelineConfig | None = None,
    tickers: Dict[str, str] | None = None,
    client: BreadthClient | None = None,
    logger=None,
) -> None:
    """
    Pull full breadth history and write cfg.breadth_path.
    """
    load_dotenv()
    cfg = cfg or PipelineConfig.from_env()
    logger = logger or get_logger("breadth_historical")
    cfg.ensure_dirs()

    api_base = os.getenv("BREADTH_API_BASE", "http://38.67.1.241:46221/v1")
    api_key = os.getenv("BREADTH_API_KEY") or os.getenv("API_KEY")  # fallback to legacy
    if not api_key:
        logger.error("Missing BREADTH_API_KEY (or API_KEY) in environment.")
        raise RuntimeError("Breadth API key required")

    client = client or BreadthClient(api_base, api_key, logger=logger)
    tickers = tickers or DEFAULT_TICKERS

    merged: pd.DataFrame | None = None
    for raw, short in tickers.items():
        df_raw = client.fetch(raw, start_date=start_date)
        df = _shape_breadth_df(df_raw, short)
        if df.empty:
            logger.warning(f"Empty breadth for {raw}")
            continue
        merged = df if merged is None else merged.merge(df, on="Date", how="outer")

    if merged is None:
        logger.warning("No breadth series retrieved.")
        return

    merged.sort_values("Date", inplace=True)
    merged.to_csv(cfg.breadth_path, index=False)
    logger.info(f"✅ Historical breadth saved → {cfg.breadth_path} ({len(merged)} rows)")


def daily_update_function(
    *,
    cfg: PipelineConfig | None = None,
    tickers: Dict[str, str] | None = None,
    client: BreadthClient | None = None,
    logger=None,
) -> None:
    """
    Append new breadth rows after last Date in cfg.breadth_path.
    """
    load_dotenv()
    cfg = cfg or PipelineConfig.from_env()
    logger = logger or get_logger("breadth_daily")
    cfg.ensure_dirs()

    if not os.path.exists(cfg.breadth_path):
        logger.info("No breadth CSV found; running historical first.")
        gather_historical(cfg=cfg, tickers=tickers, client=client, logger=logger)
        return

    existing = pd.read_csv(cfg.breadth_path, parse_dates=["Date"]).sort_values("Date")
    if existing.empty:
        logger.info("Existing breadth file is empty; running historical.")
        gather_historical(cfg=cfg, tickers=tickers, client=client, logger=logger)
        return

    last_date = existing["Date"].max()
    start_date = (last_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"Fetching breadth from {start_date}…")

    api_base = os.getenv("BREADTH_API_BASE", "http://38.67.1.241:46221/v1")
    api_key = os.getenv("BREADTH_API_KEY") or os.getenv("API_KEY")
    if not api_key:
        logger.error("Missing BREADTH_API_KEY (or API_KEY) in environment.")
        raise RuntimeError("Breadth API key required")

    client = client or BreadthClient(api_base, api_key, logger=logger)
    tickers = tickers or DEFAULT_TICKERS

    new_merged: pd.DataFrame | None = None
    for raw, short in tickers.items():
        df_raw = client.fetch(raw, start_date=start_date)
        df = _shape_breadth_df(df_raw, short)
        if df.empty:
            continue
        new_merged = df if new_merged is None else new_merged.merge(df, on="Date", how="outer")

    if new_merged is None or new_merged.empty:
        logger.info("✅ No new breadth rows.")
        return

    updated = (
        pd.concat([existing, new_merged], ignore_index=True)
        .drop_duplicates(subset=["Date"], keep="last")
        .sort_values("Date")
    )
    updated.to_csv(cfg.breadth_path, index=False)
    logger.info(f"✅ Breadth CSV updated → {cfg.breadth_path} (+{len(new_merged)} rows)")


if __name__ == "__main__":
    # Dev convenience: run historical once, then daily updates
    cfg = PipelineConfig.from_env()
    if not os.path.exists(cfg.breadth_path):
        gather_historical(cfg=cfg)
    else:
        daily_update_function(cfg=cfg)

# scripts/calculate_indicators.py
from __future__ import annotations

import os
from typing import Iterable, Optional

import numpy as np
import pandas as pd

from scripts.logger import get_logger


# Public entry point -----------------------------------------------------------

def calculate_all_indicators(
    input_path: str,
    output_path: str,
    *,
    logger=None,
    rsi_window: int = 14,
    roc_window: int = 10,
    slope_window: int = 20,
    sma_window: int = 3,
    bbw_window: int = 20,
) -> None:
    """
    Load CSV at input_path, compute indicators, and write CSV to output_path.

    Parameters
    ----------
    input_path : str
        Path to the base market CSV (with Close_*, Open_*, High_*, Low_* columns).
    output_path : str
        Path to write the enriched CSV with indicators.
    logger : logging.Logger, optional
        Logger to use. If None, a module logger will be created.
    rsi_window, roc_window, slope_window, sma_window, bbw_window : int
        Windows for respective indicators.
    """
    log = logger or get_logger("indicators")

    df = _load_csv(input_path, log)
    if df.empty:
        log.warning("No data to process. Aborting indicator pipeline.")
        return

    log.info("Calculating indicators…")

    try:
        close_cols = _find_cols(df, prefix="Close_")

        df = _calc_5d_pct(df, close_cols)
        df = _calc_roc(df, close_cols, window=roc_window)
        df = _calc_rsi(df, close_cols, window=rsi_window)
        df = _calc_regression_slope(df, close_cols, window=slope_window)
        df = _calc_sma(df, close_cols, window=sma_window)
        df = _calc_intermarket_5d_slope(df, close_cols)

        # SP500-dependent metrics
        if "Close_SP500" in df.columns:
            df = _calc_bbw(df, price_col="Close_SP500", window=bbw_window)
            # Kept for back-compat with your Mod3 logic:
            if "5d_Slope_SP500" in df.columns:
                df["Normalized_ATR"] = df["5d_Slope_SP500"] / df["Close_SP500"]
            else:
                log.warning("Missing 5d_Slope_SP500; skipping Normalized_ATR.")
        else:
            log.warning("Close_SP500 not found. Skipping BBW and Normalized_ATR.")

        # RSP/SPY
        if "Close_RSP" in df.columns and "Close_SPY" in df.columns:
            df["RSP/SPY_Ratio"] = df["Close_RSP"] / df["Close_SPY"]
        else:
            log.info("RSP/SPY ratio skipped (requires Close_RSP and Close_SPY).")

        # Save
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        df.to_csv(output_path, index=False)
        log.info(f"✅ Indicators saved: {output_path} ({len(df)} rows)")

    except Exception as e:
        log.error(f"Failed during indicator calculation or save: {e}", exc_info=True)


# Internals -------------------------------------------------------------------

def _load_csv(path: str, logger) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, parse_dates=["Date"])
        df.sort_values("Date", inplace=True)
        df.reset_index(drop=True, inplace=True)
        logger.info(f"Loaded {path} ({len(df)} rows).")
        return df
    except Exception as e:
        logger.error(f"Failed to load CSV {path}: {e}", exc_info=True)
        return pd.DataFrame()


def _find_cols(df: pd.DataFrame, *, prefix: str) -> list[str]:
    return [c for c in df.columns if c.startswith(prefix)]


def _base_name(col: str, prefix: str = "Close_") -> str:
    return col[len(prefix):]


def _calc_5d_pct(df: pd.DataFrame, close_cols: Iterable[str]) -> pd.DataFrame:
    for col in close_cols:
        base = _base_name(col)
        df[f"5d_pct_{base}"] = df[col].pct_change(periods=5) * 100.0
    return df


def _calc_roc(df: pd.DataFrame, close_cols: Iterable[str], *, window: int) -> pd.DataFrame:
    for col in close_cols:
        base = _base_name(col)
        df[f"{window}d_ROC_{base}"] = df[col].pct_change(window) * 100.0
    return df


def _calc_rsi(df: pd.DataFrame, close_cols: Iterable[str], *, window: int) -> pd.DataFrame:
    for col in close_cols:
        base = _base_name(col)
        delta = df[col].diff()
        gain = delta.clip(lower=0.0)
        loss = (-delta).clip(lower=0.0)

        # Wilder's smoothing (simple rolling mean as a practical approximation)
        avg_gain = gain.rolling(window=window, min_periods=window).mean()
        avg_loss = loss.rolling(window=window, min_periods=window).mean()
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))
        df[f"RSI_{window}_{base}"] = rsi
    return df


def _calc_regression_slope(df: pd.DataFrame, close_cols: Iterable[str], *, window: int) -> pd.DataFrame:
    """
    OLS slope over a rolling window (units: price per day).
    """
    x = np.arange(window)
    for col in close_cols:
        base = _base_name(col)
        slopes = np.full(len(df), np.nan)
        # vectorized-ish loop (keeps code simple & correct for moderate sizes)
        for i in range(window, len(df)):
            y = df[col].to_numpy()[i - window:i]
            slope, _ = np.polyfit(x, y, 1)
            slopes[i] = slope
        df[f"{window}d_slope_{base}"] = slopes
    return df


def _calc_sma(df: pd.DataFrame, close_cols: Iterable[str], *, window: int) -> pd.DataFrame:
    for col in close_cols:
        base = _base_name(col)
        df[f"SMA_{window}_{base}"] = df[col].rolling(window=window, min_periods=window).mean()
    return df


def _calc_intermarket_5d_slope(df: pd.DataFrame, close_cols: Iterable[str]) -> pd.DataFrame:
    """
    5-day OLS slope, used by your downstream Mod3 'Normalized_ATR' proxy.
    """
    x = np.arange(5)
    for col in close_cols:
        base = _base_name(col)
        slopes = np.full(len(df), np.nan)
        for i in range(5, len(df)):
            y = df[col].to_numpy()[i - 5:i]
            slope, _ = np.polyfit(x, y, 1)
            slopes[i] = slope
        df[f"5d_Slope_{base}"] = slopes
    return df


def _calc_bbw(df: pd.DataFrame, *, price_col: str, window: int) -> pd.DataFrame:
    """
    Bollinger Band Width (BBW) = (Upper - Lower) / SMA
    Upper/Lower = SMA ± 2*STD over 'window'.
    """
    sma = df[price_col].rolling(window=window, min_periods=window).mean()
    std = df[price_col].rolling(window=window, min_periods=window).std()
    upper = sma + 2 * std
    lower = sma - 2 * std
    with np.errstate(invalid="ignore", divide="ignore"):
        df["BBW"] = (upper - lower) / sma
    return df


# Optional CLI for local runs --------------------------------------------------

if __name__ == "__main__":
    from market_pipeline.config import PipelineConfig

    cfg = PipelineConfig.from_env()
    calculate_all_indicators(cfg.market_path, cfg.indicator_path)

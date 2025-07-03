import pandas as pd
import numpy as np
import os
from logger import get_logger

# Initialize logger
logger = get_logger("indicators")

def load_data(file_path):
    try:
        df = pd.read_csv(file_path, parse_dates=['Date'])
        df.sort_values('Date', inplace=True)
        df.reset_index(drop=True, inplace=True)
        logger.info(f"Loaded data from {file_path} with {len(df)} rows.")
        return df
    except Exception as e:
        logger.error(f"Failed to load CSV: {e}")
        return pd.DataFrame()

def calculate_5d_pct(df):
    for col in df.columns:
        if col.startswith("Close_"):
            base = col.replace("Close_", "")
            df[f"5d_pct_{base}"] = df[col].pct_change(periods=5) * 100
    return df

def calculate_roc(df, window=10):
    for col in df.columns:
        if col.startswith("Close_"):
            base = col.replace("Close_", "")
            df[f"{window}d_ROC_{base}"] = df[col].pct_change(periods=window) * 100
    return df

def calculate_rsi(df, window=14):
    for col in df.columns:
        if col.startswith("Close_"):
            base = col.replace("Close_", "")
            delta = df[col].diff()
            gain = delta.where(delta > 0, 0.0)
            loss = -delta.where(delta < 0, 0.0)
            avg_gain = gain.rolling(window=window, min_periods=window).mean()
            avg_loss = loss.rolling(window=window, min_periods=window).mean()
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            df[f"RSI_{window}_{base}"] = rsi
    return df

def calculate_regression_slope(df, window=20):
    for col in df.columns:
        if col.startswith("Close_"):
            base = col.replace("Close_", "")
            slopes = []
            for i in range(len(df)):
                if i < window:
                    slopes.append(np.nan)
                else:
                    y = df[col].iloc[i-window:i]
                    x = np.arange(window)
                    slope, _ = np.polyfit(x, y, 1)
                    slopes.append(slope)
            df[f"{window}d_slope_{base}"] = slopes
    return df

def calculate_sma(df, window=3):
    for col in df.columns:
        if col.startswith("Close_"):
            base = col.replace("Close_", "")
            df[f"SMA_{window}_{base}"] = df[col].rolling(window=window).mean()
    return df

def calculate_intermarket_scores(df):
    for col in df.columns:
        if col.startswith("Close_"):
            base = col.replace("Close_", "")
            slopes = []
            for i in range(len(df)):
                if i < 5:
                    slopes.append(np.nan)
                else:
                    y = df[col].iloc[i-5:i]
                    x = np.arange(5)
                    slope, _ = np.polyfit(x, y, 1)
                    slopes.append(slope)
            df[f"5d_Slope_{base}"] = slopes
    return df

def calculate_bbw(df, window=20):
    target = 'Close_SP500'
    if target not in df.columns:
        logger.warning(f"{target} not found. Skipping BBW.")
        return df

    rolling_mean = df[target].rolling(window)
    sma = rolling_mean.mean()
    std = rolling_mean.std()
    upper_band = sma + 2 * std
    lower_band = sma - 2 * std
    bbw = (upper_band - lower_band) / sma
    df['BBW'] = bbw
    return df

def calculate_rsp_spy_ratio(df):
    if 'Close_RSP' in df.columns and 'Close_SPY' in df.columns:
        df['RSP/SPY_Ratio'] = df['Close_RSP'] / df['Close_SPY']
    else:
        logger.warning("Missing Close_RSP or Close_SPY columns. Skipping RSP/SPY ratio.")
    return df

def calculate_normalized_atr(df):
    if "5d_Slope_SP500" in df.columns and "Close_SP500" in df.columns:
        df["Normalized_ATR"] = df["5d_Slope_SP500"] / df["Close_SP500"]
    else:
        logger.warning("Missing 5d_Slope_SP500 or Close_SP500 for Normalized_ATR calculation.")
    return df

def calculate_all_indicators(input_path, output_path):
    df = load_data(input_path)
    if df.empty:
        logger.warning("No data to process. Aborting.")
        return

    logger.info("Calculating indicators...")
    df = calculate_5d_pct(df)
    df = calculate_roc(df)
    df = calculate_rsi(df)
    df = calculate_regression_slope(df)
    df = calculate_sma(df)
    df = calculate_intermarket_scores(df)
    df = calculate_normalized_atr(df)  # ✅ NEW LINE
    df = calculate_bbw(df)
    df = calculate_rsp_spy_ratio(df)

    try:
        df.to_csv(output_path, index=False)
        logger.info(f"Indicators saved to: {output_path}")
    except Exception as e:
        logger.error(f"Failed to save output CSV: {e}")

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")
    input_path = os.path.join(data_dir, "MarketStates_Data.csv")
    output_path = os.path.join(data_dir, "MarketData_with_Indicators.csv")
    calculate_all_indicators(input_path, output_path)

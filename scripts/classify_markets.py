import pandas as pd
import numpy as np
import os
from logger import get_logger

# Initialize logger
logger = get_logger("market_state_classifier")

# Define paths
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
data_path = os.path.join(base_dir, 'data', 'MarketStates_Data.csv')
output_csv = os.path.join(base_dir, 'data', 'MarketData_with_States.csv')
states_txt = os.path.join(base_dir, 'data', 'MarketStates.txt')
diag_txt = os.path.join(base_dir, 'data', 'MarketStates_Diagnostics.txt')

# Load data
try:
    df = pd.read_csv(data_path)
    logger.info(f"Loaded data from {data_path} with {len(df)} rows.")
except Exception as e:
    logger.error(f"Failed to load {data_path}: {e}")
    raise

# Market state classification logic
def classify_market_state(row):
    scores = {
        "Bullish Momentum": 0,
        "Steady Climb": 0,
        "Trend Pullback": 0,
        "Bearish Collapse": 0,
        "Stagnant Drift": 0,
        "Volatile Chop": 0,
        "Volatile Drop": 0,
    }

    # Indicator mapping
    sp500 = row.get("5d_pct_SP500", np.nan)
    yield_ = row.get("5d_pct_Yield", np.nan)
    dxy = row.get("5d_pct_DXY", np.nan)
    oil = row.get("5d_pct_Oil", np.nan)
    copper = row.get("5d_pct_Copper", np.nan)
    gold = row.get("5d_pct_Gold", np.nan)
    vix = row.get("Close_VIX", np.nan)
    ma = row.get("20d_slope_SP500", np.nan)
    rsi = row.get("RSI_14_SP500", np.nan)
    atr = row.get("5d_Slope_SP500", np.nan)
    bbw = row.get("BBW", np.nan)
    ad = row.get("Close_NYAD", np.nan)
    nymo = row.get("Close_NYMO", np.nan)
    rsp_spy = row.get("RSP/SPY_Ratio", 1)

    # (Scoring logic remains unchanged)
    # ...

    # Evaluate scores
    top_state = max(scores, key=scores.get)
    score = scores[top_state]

    # Diagnostics string
    sp500_str = f"{sp500:+.2f}%" if pd.notna(sp500) else "N/A"
    rsp_str = f"{(rsp_spy - 1):+.2f}%" if pd.notna(rsp_spy) else "N/A"
    vix_str = f"{vix:.2f}" if pd.notna(vix) else "N/A"
    rsi_str = f"{rsi:.0f}" if pd.notna(rsi) else "N/A"
    ad_str = f"{ad:.0f}" if pd.notna(ad) else "N/A"
    nymo_str = f"{nymo:.0f}" if pd.notna(nymo) else "N/A"

    diag = (
        f"S&P 5-day {sp500_str}, RSP Divergence {rsp_str}, VIX {vix_str}, "
        f"RSI ~{rsi_str}, Net Advances {ad_str}, McClellan {nymo_str}, Score: {score}/39"
    )
    return pd.Series([top_state, score, diag])

# Apply and export
try:
    logger.info("Applying market state classification...")
    df[['MarketState', 'Score', 'Diagnostics']] = df.apply(classify_market_state, axis=1)
    df.to_csv(output_csv, index=False)
    logger.info(f"Saved classified dataset to {output_csv}")

    with open(states_txt, 'w') as f1, open(diag_txt, 'w') as f2:
        for _, row in df.iterrows():
            date_str = pd.to_datetime(row['Date']).strftime('%Y-%m-%d')
            f1.write(f"{date_str}, {row['MarketState']}\n")
            f2.write(f"{date_str}, {row['MarketState']}, {row['Diagnostics']}\n")
    logger.info(f"Saved states to {states_txt} and diagnostics to {diag_txt}")
except Exception as e:
    logger.error(f"Error during classification or file writing: {e}")
    raise

import os
import pandas as pd
import numpy as np
import logging
import sys

# ========== Logger Setup ==========
def get_logger(name="market_state_system_a"):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(base_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, f"{name}.log")

    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.FileHandler(log_path)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

    return logger

logger = get_logger()

# ========== Market State Profiles ==========
state_profiles = {
    "Steady Climb": [2, 1, 2],
    "Trend Pullback": [-1, 1, 0],
    "Orderly Decline": [-2, -1, 1],
    "Sharp Decline": [-3, -2, -2],
    "Volatile Chop": [0, 0, -2],
}

# ========== Scoring Logic ==========
def compute_scores_system_a(row):
    trend_score = 0
    sp500 = row.get("5d_pct_SP500", np.nan)
    ma20 = row.get("20d_slope_SP500", np.nan)

    if sp500 > 2.0: trend_score += 2
    elif 0.5 <= sp500 <= 2.0: trend_score += 1
    elif -0.5 <= sp500 < 0.5: trend_score += 0
    elif -2.0 <= sp500 < -0.5: trend_score += -1
    elif sp500 < -2.0: trend_score += -2

    if ma20 > 0.5: trend_score += 2
    elif 0.2 <= ma20 <= 0.5: trend_score += 1
    elif -0.2 <= ma20 < 0.2: trend_score += 0
    elif -0.5 <= ma20 < -0.2: trend_score += -1
    elif ma20 < -0.5: trend_score += -2

    rsi = row.get("RSI_14_SP500", np.nan)
    if rsi > 65: momentum_score = 2
    elif 50 <= rsi <= 65: momentum_score = 1
    elif 40 <= rsi < 50: momentum_score = 0
    else: momentum_score = -2

    vix = row.get("Close_VIX", np.nan)
    atr = row.get("Normalized_ATR", np.nan)
    bbw = row.get("BBW", np.nan)

    vix_score = 1 if vix < 16 else 0 if 16 <= vix <= 20 else -1 if 20 < vix <= 25 else -2
    atr_score = 1 if atr < 0.01 else 0 if 0.01 <= atr <= 0.015 else -1
    bbw_score = 1 if bbw < 3.0 else 0 if 3.0 <= bbw <= 5.0 else -1

    volatility_score = vix_score + atr_score + bbw_score

    return pd.Series([trend_score, momentum_score, volatility_score])

# ========== Classification Function ==========
def classify_market_states_system_a(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Scoring and classifying market states (System A)...")
    df = df.copy()
    df[['TrendScore_A', 'MomentumScore_A', 'VolatilityScore_A']] = df.apply(compute_scores_system_a, axis=1)

    def classify_row(row):
        vector = np.array([row['TrendScore_A'], row['MomentumScore_A'], row['VolatilityScore_A']])
        distances = {state: np.linalg.norm(vector - np.array(profile)) for state, profile in state_profiles.items()}
        best_state = min(distances, key=distances.get)
        dist = distances[best_state]
        diag = (
            f"5d%: {row['5d_pct_SP500']:+.2f}%, MA20: {row['20d_slope_SP500']:+.2f}, "
            f"RSI: {row['RSI_14_SP500']:.1f}, VIX: {row['Close_VIX']:.2f}, "
            f"ATR: {row['Normalized_ATR']:.4f}, BBW: {row['BBW']:.2f}, "
            f"Score: {[row['TrendScore_A'], row['MomentumScore_A'], row['VolatilityScore_A']]}, Dist: {dist:.2f}"
        )
        return pd.Series([best_state, dist, diag])

    df[['MarketState_A', 'EuclideanDist_A', 'Diagnostics_A']] = df.apply(classify_row, axis=1)
    return df

# ========== Write to .txt Logs ==========
def append_to_txt_logs_system_a(df: pd.DataFrame, data_dir: str, logger=None):
    if logger:
        logger.info("Appending System A logs to txt files...")

    states_txt = os.path.join(data_dir, "MarketStates_System_A.txt")
    diag_txt = os.path.join(data_dir, "MarketStates_Diagnostics_System_A.txt")

    existing_dates = set()
    if os.path.exists(states_txt):
        with open(states_txt, 'r') as f:
            existing_dates = {line.split(",")[0].strip() for line in f.readlines()}

    new_rows = 0
    with open(states_txt, 'a') as f1, open(diag_txt, 'a') as f2:
        for _, row in df.iterrows():
            if pd.isna(row['Date']):
                continue
            date_str = pd.to_datetime(row['Date']).strftime('%Y-%m-%d')
            if date_str in existing_dates:
                continue
            f1.write(f"{date_str}, {row['MarketState_A']}\n")
            f2.write(f"{date_str}, {row['MarketState_A']}, {row['Diagnostics_A']}\n")
            new_rows += 1

    if logger:
        logger.info(f"Appended {new_rows} new rows to System A txt logs.")

# ========== Optional Standalone Entry ==========
if __name__ == "__main__":
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        input_path = os.path.join(base_dir, "data", "MarketData_with_Indicators.csv")
        output_path = os.path.join(base_dir, "data", "MarketData_with_States_System_A.csv")

        df = pd.read_csv(input_path, parse_dates=["Date"])
        df_classified = classify_market_states_system_a(df)
        df_classified.to_csv(output_path, index=False)

        append_to_txt_logs_system_a(df_classified, os.path.join(base_dir, "data"), logger)

    except Exception as e:
        logger.error(f"Failed to classify and save markets (System A): {e}", exc_info=True)
        sys.exit(1)

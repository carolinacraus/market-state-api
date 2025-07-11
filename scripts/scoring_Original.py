import os
import pandas as pd
import numpy as np
import logging
import sys

# ========== Logger Setup ==========
def get_logger(name="market_state_system_b"):
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

# ========== Scoring Logic for System B ==========
def score_row_system_b(row, last_sustained_state):
    sp500 = row.get("5d_pct_SP500", np.nan)
    rsi = row.get("RSI_14_SP500", np.nan)
    vix = row.get("Close_VIX", np.nan)
    atr = row.get("Normalized_ATR", np.nan) * 100  # convert to %
    bbw = row.get("BBW", np.nan)

    scores = {}

    # Steady Climb
    score = 0
    score += 4 if sp500 > 1.5 else 2 if 0.5 < sp500 <= 1.5 else 0
    score += 2 if 50 <= rsi <= 70 else 0
    score += 2 if vix < 16 else 0
    score += 2 if atr < 1.2 else 0
    score += 2 if bbw < 4.0 else 0
    scores["Steady Climb"] = score

    # Trend Pullback (only if last was Steady Climb)
    if last_sustained_state == "Steady Climb":
        score = 0
        score += 4 if -2.0 <= sp500 <= -0.2 else 2 if -0.2 < sp500 <= 0.5 else 0
        score += 2 if 45 <= rsi <= 60 else 0
        score += 2 if vix <= 20 else 0
        score += 2 if atr < 1.6 else 0
        score += 2 if bbw < 5.5 else 0
        scores["Trend Pullback"] = score

    # Orderly Decline
    score = 0
    score += 4 if -3.5 <= sp500 <= -0.5 else 2 if -5.0 <= sp500 < -3.5 else 0
    score += 2 if 35 <= rsi <= 50 else 0
    score += 2 if 15 <= vix <= 22 else 0
    score += 2 if atr > 1.0 else 0
    score += 2 if bbw >= 4.0 else 0
    scores["Orderly Decline"] = score

    # Sharp Decline
    score = 0
    score += 4 if sp500 < -3.5 else 2 if -3.5 <= sp500 <= -2.0 else 0
    score += 2 if rsi < 40 else 0
    score += 2 if vix > 22 else 0
    score += 2 if atr > 1.5 else 0
    score += 2 if bbw > 5.0 else 0
    scores["Sharp Decline"] = score

    # Volatile Chop
    score = 0
    score += 4 if -1.0 <= sp500 <= 1.0 else 0
    score += 2 if 45 <= rsi <= 55 else 0
    score += 2 if 16 <= vix <= 24 else 0
    score += 2 if 1.0 <= atr <= 1.7 else 0
    score += 2 if 4.0 <= bbw <= 6.0 else 0
    scores["Volatile Chop"] = score

    return scores

# ========== Classification Function ==========
def classify_market_states_system_b(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Scoring and classifying market states (System B)...")
    df = df.copy()
    results = []
    last_sustained_state = None

    for _, row in df.iterrows():
        scores = score_row_system_b(row, last_sustained_state)
        if not scores:
            results.append((None, 0, "No state qualified"))
            continue

        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        best_state, best_score = sorted_scores[0]
        prev_state = last_sustained_state or "None"

        # Enforce 2-point gap rule
        if last_sustained_state:
            current_score = scores.get(last_sustained_state, 0)
            if best_score - current_score < 2:
                best_state = last_sustained_state
                best_score = current_score

        last_sustained_state = best_state

        diag = (
            f"SP500: {row['5d_pct_SP500']:+.2f}%, RSI: {row['RSI_14_SP500']:.1f}, VIX: {row['Close_VIX']:.2f}, "
            f"ATR: {row['Normalized_ATR']:.4f}, BBW: {row['BBW']:.2f}, PrevState: {prev_state}, Score: {best_score}"
        )
        results.append((best_state, best_score, diag))

    df[['MarketState_B', 'Score_B', 'Diagnostics_B']] = pd.DataFrame(results, index=df.index)
    return df

# ========== Write to .txt Logs ==========
def append_to_txt_logs_system_b(df: pd.DataFrame, data_dir: str, logger=None):
    if logger:
        logger.info("Appending System B logs to txt files...")

    states_txt = os.path.join(data_dir, "MarketStates_System_B.txt")
    diag_txt = os.path.join(data_dir, "MarketStates_Diagnostics_System_B.txt")

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
            f1.write(f"{date_str}, {row['MarketState_B']}\n")
            f2.write(f"{date_str}, {row['MarketState_B']}, {row['Diagnostics_B']}\n")
            new_rows += 1

    if logger:
        logger.info(f"Appended {new_rows} new rows to System B txt logs.")

# ========== Optional Standalone Entry ==========
if __name__ == "__main__":
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        input_path = os.path.join(base_dir, "data", "MarketData_with_Indicators.csv")
        output_path = os.path.join(base_dir, "data", "MarketData_with_States_System_B.csv")

        df = pd.read_csv(input_path, parse_dates=["Date"])
        df_classified = classify_market_states_system_b(df)
        df_classified.to_csv(output_path, index=False)

        append_to_txt_logs_system_b(df_classified, os.path.join(base_dir, "data"), logger)

    except Exception as e:
        logger.error(f"Failed to classify and save markets (System B): {e}", exc_info=True)
        sys.exit(1)

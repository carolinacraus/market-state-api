import os
import pandas as pd
import numpy as np
import logging
import sys
from logger import get_logger
from github_upload import upload_to_github  # ✅ New import

# ========== Configurable System Name ==========
system_name = "Euclidean"

# ========== Logger Setup ==========
def get_logger(name=f"market_state_system_{system_name.lower()}"):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(base_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, f"{name}.log")

    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.FileHandler(log_path, encoding='utf-8')
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

    # Trend
    trend_score += 2 if sp500 > 2.0 else 1 if 0.5 <= sp500 <= 2.0 else 0 if -0.5 <= sp500 < 0.5 else -1 if -2.0 <= sp500 < -0.5 else -2
    trend_score += 2 if ma20 > 0.5 else 1 if 0.2 <= ma20 <= 0.5 else 0 if -0.2 <= ma20 < 0.2 else -1 if -0.5 <= ma20 < -0.2 else -2

    # Momentum
    rsi = row.get("RSI_14_SP500", np.nan)
    momentum_score = 2 if rsi > 65 else 1 if 50 <= rsi <= 65 else 0 if 40 <= rsi < 50 else -2

    # Volatility
    vix = row.get("Close_VIX", np.nan)
    atr = row.get("Normalized_ATR", np.nan)
    bbw = row.get("BBW", np.nan)
    vix_score = 1 if vix < 16 else 0 if 16 <= vix <= 20 else -1 if 20 < vix <= 25 else -2
    atr_score = 1 if atr < 0.01 else 0 if 0.01 <= atr <= 0.015 else -1
    bbw_score = 1 if bbw < 3.0 else 0 if 3.0 <= bbw <= 5.0 else -1
    volatility_score = vix_score + atr_score + bbw_score

    return pd.Series([trend_score, momentum_score, volatility_score])

# ========== Classification ==========
def classify_market_states_system_a(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Scoring and classifying market states (System {system_name})...")
    df = df.copy()
    df[[f"TrendScore_{system_name}", f"MomentumScore_{system_name}", f"VolatilityScore_{system_name}"]] = df.apply(compute_scores_system_a, axis=1)

    def classify_row(row):
        vector = np.array([row[f"TrendScore_{system_name}"], row[f"MomentumScore_{system_name}"], row[f"VolatilityScore_{system_name}"]])
        distances = {state: np.linalg.norm(vector - np.array(profile)) for state, profile in state_profiles.items()}
        best_state = min(distances, key=distances.get)
        dist = distances[best_state]
        diag = (
            f"5d%: {row['5d_pct_SP500']:+.2f}%, MA20: {row['20d_slope_SP500']:+.2f}, "
            f"RSI: {row['RSI_14_SP500']:.1f}, VIX: {row['Close_VIX']:.2f}, "
            f"ATR: {row['Normalized_ATR']:.4f}, BBW: {row['BBW']:.2f}, "
            f"Score: {[row[f'TrendScore_{system_name}'], row[f'MomentumScore_{system_name}'], row[f'VolatilityScore_{system_name}']]}, Dist: {dist:.2f}"
        )
        return pd.Series([best_state, dist, diag])

    df[[f"MarketState_{system_name}", f"EuclideanDist_{system_name}", f"Diagnostics_{system_name}"]] = df.apply(classify_row, axis=1)
    return df

# ========== Append to Diagnostics Log ==========
def append_diagnostics_txt_log(df: pd.DataFrame, data_dir: str, logger=None):
    diag_txt = os.path.join(data_dir, f"MarketStates_Diagnostics_System_{system_name}.txt")
    state_to_id = {
        "Steady Climb": 1,
        "Trend Pullback": 2,
        "Orderly Decline": 3,
        "Sharp Decline": 4,
        "Volatile Chop": 5
    }

    existing_dates = set()
    if os.path.exists(diag_txt):
        with open(diag_txt, "r", encoding="utf-8") as f:
            existing_dates = {line.split(",")[0].strip() for line in f.readlines()[1:]}

    new_rows = []
    for _, row in df.iterrows():
        date_str = pd.to_datetime(row["Date"]).strftime("%Y-%m-%d")
        state = row.get(f"MarketState_{system_name}")
        direction = state_to_id.get(state)
        if pd.isna(row["Date"]) or direction is None or date_str in existing_dates:
            continue

        new_rows.append({
            "date": date_str,
            "state": state,
            "direction": direction,
            "5d%": f"{row.get('5d_pct_SP500', np.nan):+.2f}",
            "MA20": f"{row.get('20d_slope_SP500', np.nan):+.2f}",
            "RSI": f"{row.get('RSI_14_SP500', np.nan):.1f}",
            "VIX": f"{row.get('Close_VIX', np.nan):.2f}",
            "ATR": f"{row.get('Normalized_ATR', np.nan):.4f}",
            "BBW": f"{row.get('BBW', np.nan):.2f}",
            "Score": f"[{row.get(f'TrendScore_{system_name}')}, {row.get(f'MomentumScore_{system_name}')}, {row.get(f'VolatilityScore_{system_name}')}]",
            "Dist": f"{row.get(f'EuclideanDist_{system_name}', np.nan):.2f}"
        })

    if not new_rows:
        if logger: logger.info("No new diagnostics rows to append.")
        return

    df_new = pd.DataFrame(new_rows)
    write_header = not os.path.exists(diag_txt)
    with open(diag_txt, "a", encoding="utf-8") as f:
        if write_header:
            f.write(", ".join(df_new.columns) + "\n")
        for _, row in df_new.iterrows():
            f.write(", ".join(str(row[col]) for col in df_new.columns) + "\n")

    if logger:
        logger.info(f"✅ Appended {len(df_new)} new diagnostics rows to {diag_txt}")

# ========== Main ==========
if __name__ == "__main__":
    try:
        logger.info(f">>> Starting System {system_name} classification")

        base_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.abspath(os.path.join(base_dir, "..", "data"))
        os.makedirs(data_dir, exist_ok=True)

        input_path = os.path.join(data_dir, "MarketData_with_Indicators.csv")
        output_path = os.path.join(data_dir, f"MarketData_with_States_System_{system_name}.csv")

        df = pd.read_csv(input_path, parse_dates=["Date"])
        df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()

        if os.path.exists(output_path):
            df_existing = pd.read_csv(output_path, parse_dates=["Date"])
            df_existing["Date"] = pd.to_datetime(df_existing["Date"]).dt.normalize()
            existing_dates = set(df_existing["Date"])
            df_new = df[~df["Date"].isin(existing_dates)]
            logger.info(f"Found {len(df_new)} new rows to classify.")
        else:
            df_existing = pd.DataFrame()
            df_new = df
            logger.info("No existing state file found. Classifying full dataset.")

        if df_new.empty:
            logger.info(f"Market states for System {system_name} already up to date.")
            sys.exit(0)

        df_classified_new = classify_market_states_system_a(df_new)

        df_combined = pd.concat([df_existing, df_classified_new], ignore_index=True)
        df_combined.drop_duplicates(subset=["Date"], keep="last", inplace=True)
        df_combined.sort_values("Date", inplace=True)
        df_combined.to_csv(output_path, index=False)

        append_diagnostics_txt_log(df_classified_new, data_dir, logger)

        start_date = df_classified_new["Date"].min().strftime("%Y-%m-%d")
        end_date = df_classified_new["Date"].max().strftime("%Y-%m-%d")
        commit_msg = f"📊 Euclidean classification for {start_date} to {end_date}"

        commit_sha = upload_to_github(
            file_path=output_path,
            repo="carolinacraus/market-state-api",
            path_in_repo=f"data/MarketData_with_States_System_{system_name}.csv",
            commit_message=commit_msg,
            branch="main"
        )

        diag_path = os.path.join(data_dir, f"MarketStates_Diagnostics_System_{system_name}.txt")
        upload_to_github(
            file_path=diag_path,
            repo="carolinacraus/market-state-api",
            path_in_repo=f"data/MarketStates_Diagnostics_System_{system_name}.txt",
            commit_message=f"🧪 Diagnostics update ({start_date} to {end_date}) [{system_name}]",
            branch="main"
        )

        if commit_sha:
            tag = f"tag-{system_name.lower()}-{start_date}-to-{end_date}"
            from github_upload import create_github_tag
            create_github_tag(
                repo="carolinacraus/market-state-api",
                tag_name=tag,
                tag_message=commit_msg,
                commit_sha=commit_sha,
                branch="main"
            )

        logger.info(f">>> ✅ Finished classification and update for system {system_name}")

    except Exception as e:
        logger.error(f"❌ Failed to classify and upload markets (System {system_name}): {e}", exc_info=True)
        sys.exit(1)

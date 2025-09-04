import os
import pandas as pd
import numpy as np
import logging
import sys
from JulyBuild.github_upload import upload_to_github

system_name = "Euclidean"

# Logger Setup
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

# Market State Profiles
state_profiles = {
    "Steady Climb": [2, 1, 2],
    "Trend Pullback": [-1, 1, 0],
    "Orderly Decline": [-2, -1, 1],
    "Sharp Decline": [-3, -2, -2],
    "Volatile Chop": [0, 0, -2],
}

# Scoring Logic
def compute_scores_system_a(row):
    trend_score = 0
    sp500 = row.get("5d_pct_SP500", np.nan)
    ma20 = row.get("20d_slope_SP500", np.nan)

    trend_score += 2 if sp500 > 2.0 else 1 if 0.5 <= sp500 <= 2.0 else 0 if -0.5 <= sp500 < 0.5 else -1 if -2.0 <= sp500 < -0.5 else -2
    trend_score += 2 if ma20 > 0.5 else 1 if 0.2 <= ma20 <= 0.5 else 0 if -0.2 <= ma20 < 0.2 else -1 if -0.5 <= ma20 < -0.2 else -2

    rsi = row.get("RSI_14_SP500", np.nan)
    momentum_score = 2 if rsi > 65 else 1 if 50 <= rsi <= 65 else 0 if 40 <= rsi < 50 else -2

    vix = row.get("Close_VIX", np.nan)
    atr = row.get("Normalized_ATR", np.nan)
    bbw = row.get("BBW", np.nan)
    vix_score = 1 if vix < 16 else 0 if 16 <= vix <= 20 else -1 if 20 < vix <= 25 else -2
    atr_score = 1 if atr < 0.01 else 0 if 0.01 <= atr <= 0.015 else -1
    bbw_score = 1 if bbw < 3.0 else 0 if 3.0 <= bbw <= 5.0 else -1
    volatility_score = vix_score + atr_score + bbw_score

    return pd.Series([trend_score, momentum_score, volatility_score])

# Classification Function
def classify_market_states_system_a(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Scoring and classifying market states (System {system_name})...")
    df = df.copy()
    df[[f"TrendScore_{system_name}", f"MomentumScore_{system_name}", f"VolatilityScore_{system_name}"]] = df.apply(compute_scores_system_a, axis=1)

    def classify_row(row):
        vector = np.array([row[f"TrendScore_{system_name}"], row[f"MomentumScore_{system_name}"], row[f"VolatilityScore_{system_name}"]])
        distance_dict = {state: np.linalg.norm(vector - np.array(profile)) for state, profile in state_profiles.items()}
        sorted_states = sorted(distance_dict.items(), key=lambda x: x[1])

        best_state, best_dist = sorted_states[0]
        alt_state, alt_dist = sorted_states[1]
        distance_delta = abs(best_dist - alt_dist)

        is_borderline = distance_delta < 0.5

        return pd.Series([
            best_state,
            best_dist,
            f"5d%: {row['5d_pct_SP500']:+.2f}%, MA20: {row['20d_slope_SP500']:+.2f}, RSI: {row['RSI_14_SP500']:.1f}, VIX: {row['Close_VIX']:.2f}, ATR: {row['Normalized_ATR']:.4f}, BBW: {row['BBW']:.2f}, Score: {[row[f'TrendScore_{system_name}'], row[f'MomentumScore_{system_name}'], row[f'VolatilityScore_{system_name}']]}, Dist: {best_dist:.2f}",
            alt_state,
            alt_dist,
            distance_delta,
            is_borderline
        ])

    df[[
        f"MarketState_{system_name}",
        f"EuclideanDist_{system_name}",
        f"Diagnostics_{system_name}",
        "AltState",
        "AltStateDist",
        "DistanceDelta",
        "IsBorderline"
    ]] = df.apply(classify_row, axis=1)

    return df

# Export Borderline Cases
def export_borderline_cases(df: pd.DataFrame, data_dir: str):
    borderline_output = os.path.join(data_dir, f"Borderline_States_{system_name}.csv")
    df[df["IsBorderline"]].to_csv(borderline_output, index=False)
    logger.info(f"🧠 Exported borderline cases to {borderline_output}")

# Main Script
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
        else:
            df_existing = pd.DataFrame()
            df_new = df

        if df_new.empty:
            logger.info(f"Market states for System {system_name} already up to date.")
            sys.exit(0)

        df_classified_new = classify_market_states_system_a(df_new)
        export_borderline_cases(df_classified_new, data_dir)
        upload_to_github(
            file_path=os.path.join(data_dir, f"Borderline_States_{system_name}.csv"),
            repo="carolinacraus/market-state-api",
            path_in_repo=f"../JulyBuild/data2/Borderline_States_{system_name}.csv",
            commit_message="🧠 Export borderline states for GPT comparison",
            branch="main"
        )
        df_combined = pd.concat([df_existing, df_classified_new], ignore_index=True)
        df_combined.drop_duplicates(subset=["Date"], keep="last", inplace=True)
        df_combined.sort_values("Date", inplace=True)
        df_combined.to_csv(output_path, index=False)

        logger.info(f">>> ✅ Finished classification and update for system {system_name}")

    except Exception as e:
        logger.error(f"❌ Failed to classify and upload markets (System {system_name}): {e}", exc_info=True)
        sys.exit(1)
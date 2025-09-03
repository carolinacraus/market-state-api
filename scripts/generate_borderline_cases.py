import pandas as pd
import numpy as np
import os

# === Config ===
system_name = "Euclidean"
threshold = 0.5  # distance delta threshold to flag borderline cases
data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data2"))
input_path = os.path.join(data_dir, f"MarketData_with_States_System_{system_name}.csv")
output_path = os.path.join(data_dir, f"Borderline_States_{system_name}.csv")

# === Market State Profiles ===
state_profiles = {
    "Steady Climb": [2, 1, 2],
    "Trend Pullback": [-1, 1, 0],
    "Orderly Decline": [-2, -1, 1],
    "Sharp Decline": [-3, -2, -2],
    "Volatile Chop": [0, 0, -2],
}

# === Load the data2 ===
df = pd.read_csv(input_path, parse_dates=["Date"])
df = df.dropna(subset=["Date"])
df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()

# === Ensure score columns exist ===
required_cols = [
    f"TrendScore_{system_name}",
    f"MomentumScore_{system_name}",
    f"VolatilityScore_{system_name}",
]

for col in required_cols:
    if col not in df.columns:
        raise ValueError(f"Missing score column: {col}")

# === Process and classify borderline cases ===
def process_row(row):
    vector = np.array([
        row[f"TrendScore_{system_name}"],
        row[f"MomentumScore_{system_name}"],
        row[f"VolatilityScore_{system_name}"]
    ])
    distances = {state: np.linalg.norm(vector - np.array(profile)) for state, profile in state_profiles.items()}
    sorted_states = sorted(distances.items(), key=lambda x: x[1])

    best_state, best_dist = sorted_states[0]
    alt_state, alt_dist = sorted_states[1]
    distance_delta = abs(best_dist - alt_dist)
    is_borderline = distance_delta < threshold

    return pd.Series({
        "TopState": best_state,
        "TopDist": best_dist,
        "AltState": alt_state,
        "AltDist": alt_dist,
        "DistanceDelta": distance_delta,
        "IsBorderline": is_borderline
    })

df_scores = df.apply(process_row, axis=1)
df_combined = pd.concat([df, df_scores], axis=1)

# === Filter and Save ===
df_borderline = df_combined[df_combined["IsBorderline"] == True]
# Create yearly folder if needed
yearly_dir = os.path.join(data_dir, "borderline_by_year")
os.makedirs(yearly_dir, exist_ok=True)

df_borderline["Year"] = df_borderline["Date"].dt.year

for year, group in df_borderline.groupby("Year"):
    year_path = os.path.join(yearly_dir, f"Borderline_{system_name}_{year}.csv")
    group.to_csv(year_path, index=False)
    print(f"📁 Saved {len(group)} borderline rows for {year} → {year_path}")

print(f"✅ Saved {len(df_borderline)} borderline rows to {output_path}")

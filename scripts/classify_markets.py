import pandas as pd
import numpy as np
import os

# Load data
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
data_path = os.path.join(base_dir, 'data', 'MarketStates_Data.csv')
output_csv = os.path.join(base_dir, 'data', 'MarketData_with_States.csv')
states_txt = os.path.join(base_dir, 'data', 'MarketStates.txt')
diag_txt = os.path.join(base_dir, 'data', 'MarketStates_Diagnostics.txt')

df = pd.read_csv(data_path)

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

    # Bullish Momentum
    if sp500 > 3: scores["Bullish Momentum"] += 4
    if -0.2 <= yield_ <= -0.1: scores["Bullish Momentum"] += 2
    if dxy <= -1: scores["Bullish Momentum"] += 2
    if oil >= 3 and copper >= 3: scores["Bullish Momentum"] += 2
    if gold < 20: scores["Bullish Momentum"] += 2
    if 15 <= vix <= 25: scores["Bullish Momentum"] += 2
    if rsp_spy > 2: scores["Bullish Momentum"] += 2
    if 0.003 <= ma <= 0.01: scores["Bullish Momentum"] += 2
    if 60 <= rsi <= 75: scores["Bullish Momentum"] += 2
    if 1 <= atr <= 2: scores["Bullish Momentum"] += 2
    if 2 <= bbw <= 3: scores["Bullish Momentum"] += 2
    if ad >= 2: scores["Bullish Momentum"] += 2
    if 60 <= nymo <= 100: scores["Bullish Momentum"] += 2

    # Steady Climb
    if 0.5 <= sp500 <= 3: scores["Steady Climb"] += 4
    if -0.2 <= yield_ <= 0.2: scores["Steady Climb"] += 2
    if -1 <= dxy <= 1: scores["Steady Climb"] += 2
    if -3 <= oil <= 3 and 1 <= copper <= 3: scores["Steady Climb"] += 2
    if -30 <= gold <= 30: scores["Steady Climb"] += 2
    if 12 <= vix <= 20: scores["Steady Climb"] += 2
    if 1.5 <= rsp_spy <= 2.5: scores["Steady Climb"] += 2
    if 0.003 <= ma <= 0.01: scores["Steady Climb"] += 2
    if 50 <= rsi <= 65: scores["Steady Climb"] += 2
    if 0.5 <= atr <= 1.5: scores["Steady Climb"] += 2
    if 1.5 <= bbw <= 2.5: scores["Steady Climb"] += 2
    if 1.5 <= ad <= 2.0: scores["Steady Climb"] += 2
    if 30 <= nymo <= 60: scores["Steady Climb"] += 2

    # Trend Pullback
    if -5 <= sp500 <= -1: scores["Trend Pullback"] += 4
    if 4 <= yield_ <= 4.5: scores["Trend Pullback"] += 2
    if -1 <= dxy <= 1: scores["Trend Pullback"] += 2
    if -5 <= oil <= -2 and -5 <= copper <= -2: scores["Trend Pullback"] += 2
    if 20 <= gold <= 40: scores["Trend Pullback"] += 2
    if 18 <= vix <= 25: scores["Trend Pullback"] += 2
    if rsp_spy < 2: scores["Trend Pullback"] += 2
    if ma < 0.002: scores["Trend Pullback"] += 2
    if 40 <= rsi <= 60: scores["Trend Pullback"] += 2
    if 1.5 <= atr <= 2.5: scores["Trend Pullback"] += 2
    if 2 <= bbw <= 3: scores["Trend Pullback"] += 2
    if 1 <= ad <= 1.5: scores["Trend Pullback"] += 2
    if -40 <= nymo <= 20: scores["Trend Pullback"] += 2

    # Bearish Collapse
    if sp500 <= -3: scores["Bearish Collapse"] += 4
    if abs(yield_) >= 0.3 or row['Close_Yield'] >= 4.5 or row['Close_Yield'] <= 3: scores["Bearish Collapse"] += 2
    if dxy >= 1: scores["Bearish Collapse"] += 2
    if oil <= -5 and copper <= -5: scores["Bearish Collapse"] += 2
    if gold >= 50: scores["Bearish Collapse"] += 2
    if vix > 30: scores["Bearish Collapse"] += 2
    if rsp_spy < 0.5: scores["Bearish Collapse"] += 2
    if ma < -0.01: scores["Bearish Collapse"] += 2
    if rsi < 40: scores["Bearish Collapse"] += 2
    if atr > 3: scores["Bearish Collapse"] += 2
    if bbw > 4: scores["Bearish Collapse"] += 2
    if ad < 0.8: scores["Bearish Collapse"] += 2
    if nymo < -100: scores["Bearish Collapse"] += 2

    # Stagnant Drift
    if -1 <= sp500 <= 1: scores["Stagnant Drift"] += 4
    if abs(yield_) < 0.1: scores["Stagnant Drift"] += 2
    if abs(dxy) <= 0.5: scores["Stagnant Drift"] += 2
    if abs(oil) <= 2 and abs(copper) <= 2: scores["Stagnant Drift"] += 2
    if abs(gold) <= 20: scores["Stagnant Drift"] += 2
    if 15 <= vix <= 20: scores["Stagnant Drift"] += 2
    if rsp_spy < 1.5: scores["Stagnant Drift"] += 2
    if abs(ma) < 0.005: scores["Stagnant Drift"] += 2
    if 45 <= rsi <= 55: scores["Stagnant Drift"] += 2
    if atr < 1: scores["Stagnant Drift"] += 2
    if bbw < 1.5: scores["Stagnant Drift"] += 2
    if 1.0 <= ad <= 1.2: scores["Stagnant Drift"] += 2
    if -10 <= nymo <= 10: scores["Stagnant Drift"] += 2

    # Volatile Chop
    if -2 <= sp500 <= 2: scores["Volatile Chop"] += 4
    if 0.1 <= abs(yield_) <= 0.3: scores["Volatile Chop"] += 2
    if -1 <= dxy <= 1: scores["Volatile Chop"] += 2
    if -5 <= oil <= 5 and -5 <= copper <= 5: scores["Volatile Chop"] += 2
    if -50 <= gold <= 50: scores["Volatile Chop"] += 2
    if 20 <= vix <= 30: scores["Volatile Chop"] += 2
    if rsp_spy < 2: scores["Volatile Chop"] += 2
    if abs(ma) <= 0.005: scores["Volatile Chop"] += 2
    if 40 <= rsi <= 60: scores["Volatile Chop"] += 2
    if 2 <= atr <= 3: scores["Volatile Chop"] += 2
    if 3 <= bbw <= 4: scores["Volatile Chop"] += 2
    if 0.8 <= ad <= 1.3: scores["Volatile Chop"] += 2
    if -20 <= nymo <= 20: scores["Volatile Chop"] += 2

    # Volatile Drop
    if -4 <= sp500 <= -2: scores["Volatile Drop"] += 4
    if -0.2 <= yield_ <= 0.2: scores["Volatile Drop"] += 2
    if -1 <= dxy <= 1: scores["Volatile Drop"] += 2
    if -5 <= oil <= -2 and -5 <= copper <= -2: scores["Volatile Drop"] += 2
    if 10 <= gold <= 40: scores["Volatile Drop"] += 2
    if 20 <= vix <= 28: scores["Volatile Drop"] += 2
    if rsp_spy < 0.5: scores["Volatile Drop"] += 2
    if -0.01 <= ma <= -0.005: scores["Volatile Drop"] += 2
    if 35 <= rsi <= 50: scores["Volatile Drop"] += 2
    if 2 <= atr <= 3: scores["Volatile Drop"] += 2
    if 3 <= bbw <= 4: scores["Volatile Drop"] += 2
    if ad < 1.0: scores["Volatile Drop"] += 2
    if -100 <= nymo <= -40: scores["Volatile Drop"] += 2

    top_state = max(scores, key=scores.get)
    score = scores[top_state]

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


# Apply logic and save
df[['MarketState', 'Score', 'Diagnostics']] = df.apply(classify_market_state, axis=1)
df.to_csv(output_csv, index=False)

with open(states_txt, 'w') as f1, open(diag_txt, 'w') as f2:
    for _, row in df.iterrows():
        date_str = pd.to_datetime(row['Date']).strftime('%Y-%m-%d')
        f1.write(f"{date_str}, {row['MarketState']}\n")
        f2.write(f"{date_str}, {row['MarketState']}, {row['Diagnostics']}\n")
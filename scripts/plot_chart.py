import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from PIL import Image

# === Color map ===
colors = {
    "Bullish Momentum": "green",
    "Steady Climb": "lime",
    "Trend Pullback": "yellow",
    "Bearish Collapse": "red",
    "Stagnant Drift": "gray",
    "Volatile Chop": "orange",
    "Volatile Drop": "darkred",
    "Orderly Decline": "blue",
    "Sharp Decline": "firebrick"
}

def plot_market_state_chart(txt_file_path, sp500_path, system_name, year, output_path):
    df = pd.read_csv(txt_file_path, sep=",", header=None, names=["Date", "MarketState"])
    df["Date"] = pd.to_datetime(df["Date"])
    df["MarketState"] = df["MarketState"].str.strip()
    df = df[df["Date"].dt.year == year]

    sp500_df = pd.read_csv(sp500_path)
    sp500_df["Date"] = pd.to_datetime(sp500_df["Date"])
    sp500_year = sp500_df[sp500_df["Date"].dt.year == year]

    merged = pd.merge(sp500_year, df, on="Date", how="left")

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(merged["Date"], merged["Close_SP500"], color="black", label="S&P 500")

    current_state = None
    start_date = None
    used_states = set()

    for i in range(len(merged)):
        state = merged.loc[i, "MarketState"]
        if state != current_state:
            if current_state is not None:
                ax.axvspan(start_date, merged.loc[i - 1, "Date"],
                           color=colors.get(current_state, "gray"), alpha=0.3,
                           label=current_state if current_state not in used_states else "")
                used_states.add(current_state)
            current_state = state
            start_date = merged.loc[i, "Date"]

    if current_state is not None:
        ax.axvspan(start_date, merged["Date"].iloc[-1],
                   color=colors.get(current_state, "gray"), alpha=0.3,
                   label=current_state if current_state not in used_states else "")

    ax.set_title(f"S&P 500 in {year} with Market States - {system_name}")
    ax.set_ylabel("S&P 500 Close")
    ax.set_xlabel("Date")
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    plt.xticks(rotation=45)
    plt.tight_layout()

    plt.savefig(output_path)
    plt.close()
    print(f"Chart saved to: {output_path}")

def analyze_market_state_agreement(year=None):
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
    df_a = pd.read_csv(os.path.join(data_dir, "MarketStates_System_A.txt"), names=["Date", "State_A"])
    df_b = pd.read_csv(os.path.join(data_dir, "MarketStates_System_B.txt"), names=["Date", "State_B"])
    df_a["Date"] = pd.to_datetime(df_a["Date"])
    df_b["Date"] = pd.to_datetime(df_b["Date"])
    df = pd.merge(df_a, df_b, on="Date")
    if year:
        df = df[df["Date"].dt.year == year]

    df["Agreement"] = df["State_A"] == df["State_B"]
    agreement_pct = df["Agreement"].mean() * 100

    fig, ax = plt.subplots(figsize=(12, 2))
    ax.scatter(df["Date"], [1]*len(df), c=df["Agreement"].map({True: "green", False: "red"}), s=10)
    ax.set_yticks([])
    ax.set_title(f"System A vs B Agreement Timeline {f'- {year}' if year else ''} — {agreement_pct:.1f}% Match")
    plt.tight_layout()

    tag = f"_{year}" if year else ""
    img_path = os.path.join(data_dir, f"system_agreement_analysis{tag}.png")
    plt.savefig(img_path)
    plt.close()
    return img_path

def generate_state_charts_pdf():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.abspath(os.path.join(base_dir, "..", "data"))
    sp500_file = os.path.join(data_dir, "MarketStates_Data.csv")
    txt_a = os.path.join(data_dir, "MarketStates_System_A.txt")
    txt_b = os.path.join(data_dir, "MarketStates_System_B.txt")

    image_list = []
    for year in range(2005, 2026):
        chart_a = os.path.join(data_dir, f"SP500_Market_States_System_A_{year}.png")
        chart_b = os.path.join(data_dir, f"SP500_Market_States_System_B_{year}.png")
        agree_chart = analyze_market_state_agreement(year)

        plot_market_state_chart(txt_a, sp500_file, "System A", year, chart_a)
        plot_market_state_chart(txt_b, sp500_file, "System B", year, chart_b)

        image_list.extend([
            Image.open(chart_a).convert("RGB"),
            Image.open(chart_b).convert("RGB"),
            Image.open(agree_chart).convert("RGB")
        ])

    final_agreement = analyze_market_state_agreement(None)
    image_list.append(Image.open(final_agreement).convert("RGB"))

    pdf_path = os.path.join(data_dir, "SP500_Market_States_Combined.pdf")
    if image_list:
        image_list[0].save(pdf_path, save_all=True, append_images=image_list[1:])
        print(f"PDF generated: {pdf_path}")
    return pdf_path

if __name__ == "__main__":
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.abspath(os.path.join(base_dir, "..", "data"))

        txt_file = sys.argv[1] if len(sys.argv) > 1 else os.path.join(data_dir, "MarketStates_System_A.txt")
        sp500_file = os.path.join(data_dir, "MarketStates_Data.csv")
        output_file = os.path.join(data_dir, "SP500_Market_States_System_A.png")
        system_name = "System A"

        # Default preview chart if run directly
        plot_market_state_chart(txt_file, sp500_file, system_name, 2024, output_file)

    except Exception as e:
        print(f"Error generating chart: {e}", file=sys.stderr)
        sys.exit(1)

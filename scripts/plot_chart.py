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

def plot_market_state_chart(txt_file_path, sp500_path, system_name, output_path):
    df = pd.read_csv(txt_file_path, sep=",", header=None, names=["Date", "MarketState"])
    df["Date"] = pd.to_datetime(df["Date"])
    df["MarketState"] = df["MarketState"].str.strip()
    df = df[df["Date"].dt.year == 2024]

    sp500_df = pd.read_csv(sp500_path)
    sp500_df["Date"] = pd.to_datetime(sp500_df["Date"])
    sp500_2024 = sp500_df[sp500_df["Date"].dt.year == 2024]

    merged = pd.merge(sp500_2024, df, on="Date", how="left")

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

    ax.set_title(f"S&P 500 in 2024 with Market States - {system_name}")
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

def generate_state_charts_pdf():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.abspath(os.path.join(base_dir, "..", "data"))

    # Define inputs
    sp500_file = os.path.join(data_dir, "MarketStates_Data.csv")

    txt_a = os.path.join(data_dir, "MarketStates_System_A.txt")
    chart_a = os.path.join(data_dir, "SP500_Market_States_System_A.png")
    plot_market_state_chart(txt_a, sp500_file, "System A", chart_a)

    txt_b = os.path.join(data_dir, "MarketStates_System_B.txt")
    chart_b = os.path.join(data_dir, "SP500_Market_States_System_B.png")
    plot_market_state_chart(txt_b, sp500_file, "System B", chart_b)

    # Combine into PDF
    image_list = [Image.open(chart_a).convert("RGB"), Image.open(chart_b).convert("RGB")]
    pdf_path = os.path.join(data_dir, "SP500_Market_States_Combined.pdf")
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

        plot_market_state_chart(txt_file, sp500_file, system_name, output_file)

    except Exception as e:
        print(f"Error generating chart: {e}", file=sys.stderr)
        sys.exit(1)

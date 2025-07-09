# 📊 Market State Classification API

A fully automated Flask-based microservice for classifying financial market states from technical indicators and breadth data — deployed via **Railway**, orchestrated by **n8n**, and optionally backed up to **Google Drive**.

---

## 🚀 Project Overview

This project:
- Fetches historical market and breadth data
- Calculates technical indicators
- Classifies each date into a defined market state
- Appends new results to rolling `.txt` logs
- Uploads results to a SQL Server (if configured)
- Exposes an API to trigger all pipeline steps or download results
- Can run daily in production using Railway + n8n

---

## 📁 Folder Structure

```plaintext
market-state-api/
├── app.py                    # Flask app with API routes
├── .env                      # Environment variables (not committed)
├── requirements.txt          # Python dependencies
├── service_account.json      # Google Drive service credentials (not committed)

├── data/                     # Stores all CSV and TXT output files
│   ├── MarketStates_Data.csv
│   ├── MarketData_with_Indicators.csv
│   ├── MarketData_with_States.csv
│   ├── MarketStates.txt                      # Plain date + state list
│   └── MarketStates_Diagnostics.txt          # Date, state, and reason

├── scripts/
│   ├── DataRetrieval_FMP.py       # Fetches historical price data from FMP API
│   ├── MarketBreadth_SQL.py       # Pulls $NYAD and $NYMO from SQL Server
│   ├── merge_market_breadth.py    # Merges NYAD/NYMO into main dataset
│   ├── calculate_indicators.py    # Computes RSI, ROC, slope, BBW, etc.
│   ├── classify_markets.py        # Scores and labels each row into market states
│   ├── update_daily_pipeline.py   # End-to-end automation script
│   ├── sql_upload.py                   # Uploads final MarketStates to MS SQL Server
│   ├── google_drive_uploader.py   # (Optional) Uploads output files to Google Drive
│   └── logger.py                  # Custom logging utility
```

---

## 🧠 Market States

The model classifies market conditions into:

1. Steady Climb  
2. Trend Pullback  
3. Orderly Decline
4. Sharp Decline
5. Volatile Chop  

| Market State             | Description |
|--------------------------|-------------|
| 🟦 **Steady Climb**      | • The market is trending upward with low volatility and consistent price structure.<br>• Pullbacks are shallow and orderly, offering clean entries on trend-following setups.<br>• Breakouts tend to hold, and moving averages typically act as dynamic support.<br>• Ideal for riding momentum with tight risk control. |
| 🟧 **Trend Pullback**    | • A controlled retracement within an active uptrend.<br>• Prices dip temporarily but momentum remains intact.<br>• Often presents high-probability bounce setups near key support levels, such as rising moving averages or prior breakout zones.<br>• Best for fade trades or positioning ahead of a trend resumption. |
| 🟨 **Orderly Decline**   | • The market is grinding lower with measured selling pressure.<br>• There’s no panic, but the path of least resistance is down.<br>• Short setups can work on breakdowns or failed rallies, though bounces may occur without changing the bias.<br>• Good for tactical shorts or put spreads in a weakening tape. |
| 🟥 **Sharp Decline**     | • A high-velocity selloff marked by heavy volume, wide ranges, and spiking volatility.<br>• Gaps, breakdowns, and failed supports dominate.<br>• Best suited for aggressive short setups or quick scalp opportunities — but also risky due to intraday reversals.<br>• Often coincides with news events or systemic risk. |
| 🟪 **Volatile Chop**     | • No trend, just noise.<br>• The market whipsaws in both directions with frequent fakeouts and little follow-through.<br>• Breakouts and breakdowns regularly fail.<br>• Most trend strategies underperform here.<br>• Best suited for range trading, mean reversion, or staying out entirely until clarity returns. |


Each classification is based on S&P500 movement, breadth indicators, VIX, RSI, and more.

---

## 🖥️ API Endpoints

All endpoints are hosted by your Railway app: 

| Endpoint                | Method | Description                               |
| ----------------------- | ------ |-------------------------------------------|
| `/`                     | GET    | Health check                              |
| `/fetch-market-data`    | POST   | Fetch data from FMP (JSON body w/ dates)  |
| `/fetch-market-breadth` | POST   | Pull NYAD/NYMO from SQL                   |
| `/run-indicators`       | POST   | Calculate technical indicators            |
| `/run-classification`   | POST   | Label rows with market state              |
| `/run-daily-pipeline`   | POST   | Run full end-to-end workflow (JSON w/ dates) |
| `/upload-market-states` | POST   | Upload classified states to SQL Server    |
| `/download/<filename>`  | GET    | Download any file by name                 |
| `/download/market-data` | GET    | MarketStates_Data.csv                     |
| `/download/indicators`  | GET    | MarketData_with_Indicators.csv            |
| `/download/states`      | GET    | MarketData_with_States.csv                |
| `/download/states-txt`  | GET    | MarketStates.txt                          |
| `/download/diagnostics` | GET    | MarketStates_Diagnostics.txt              |

---

## ⚙️ Setup (Local or Railway)

### 1. Clone the Repo

```bash
git clone https://github.com/your-username/market-state-api.git
cd market-state-api
```

### 2. Install Requirements

```bash
pip install -r requirements.txt
```
### 3. Environment Variables

Create a ```.env``` file at the root with:
```bash
FMP_API_KEY=your_fmp_api_key
DRIVE_FOLDER_ID=your_google_drive_folder_id
SQL_SERVER=your_server
SQL_DB=your_db
SQL_USER=your_user
SQL_PWD=your_password

```

## 🔁 Deployment with Railway + Automation in n8n 




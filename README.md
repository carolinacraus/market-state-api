# 📊 Market State Classification API

A fully automated Flask-based microservice for classifying financial market states from technical indicators and breadth data — deployed via **Railway**, orchestrated by **n8n**, and optionally backed up to **Google Drive**.

---

## 🚀 Project Overview

This project:
- Fetches historical market and breadth data
- Calculates technical indicators
- Classifies each date into a defined market state
- Exposes an API to trigger all pipeline steps or download results
- Can run daily in production using Railway + n8n

---

## 📁 Folder Structure

market-state-api/
│
├── app.py # Flask app with API routes
├── .env # Environment variables (not committed)
├── requirements.txt # Python dependencies
├── service_account.json # Google Drive service credentials (not committed)
│
├── data/ # Stores all CSV and TXT output files
│ ├── MarketStates_Data.csv
│ ├── MarketData_with_Indicators.csv
│ ├── MarketData_with_States.csv
│ ├── MarketStates.txt
│ └── MarketStates_Diagnostics.txt
│
├── scripts/
│ ├── DataRetrieval_FMP.py # Fetches historical price data from FMP API
│ ├── MarketBreadth_SQL.py # Pulls $NYAD and $NYMO from SQL Server
│ ├── merge_market_breadth.py # Merges NYAD/NYMO into main dataset
│ ├── calculate_indicators.py # Computes RSI, ROC, slope, BBW, etc.
│ ├── classify_markets.py # Scores and labels each row into market states
│ ├── update_daily_pipeline.py # End-to-end automation script
│ ├── google_drive_uploader.py # (Optional) Uploads output files to Google Drive
│ └── logger.py # Custom logging utility


---

## 🧠 Market States

The model classifies market conditions into:

- Bullish Momentum  
- Steady Climb  
- Trend Pullback  
- Bearish Collapse  
- Volatile Drop  
- Volatile Chop  
- Stagnant Drift  

Each classification is based on S&P500 movement, breadth indicators, VIX, RSI, and more.

---

## 🖥️ API Endpoints

All endpoints are hosted by your Railway app:

| Endpoint                     | Method | Description                                  |
|-----------------------------|--------|----------------------------------------------|
| `/`                         | GET    | Health check                                 |
| `/fetch-market-data`        | POST   | Fetch data from FMP (JSON body w/ dates)     |
| `/fetch-market-breadth`     | POST   | Pull NYAD/NYMO from SQL                      |
| `/run-indicators`           | POST   | Calculate technical indicators               |
| `/run-classification`       | POST   | Label rows with market state                 |
| `/run-daily-pipeline`       | POST   | Run full end-to-end workflow (JSON w/ dates) |
| `/download/<filename>`      | GET    | Download any file by name                    |
| `/download/market-data`     | GET    | MarketStates_Data.csv                        |
| `/download/indicators`      | GET    | MarketData_with_Indicators.csv               |
| `/download/states`          | GET    | MarketData_with_States.csv                   |
| `/download/states-txt`      | GET    | MarketStates.txt                             |
| `/download/diagnostics`     | GET    | MarketStates_Diagnostics.txt                 |

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




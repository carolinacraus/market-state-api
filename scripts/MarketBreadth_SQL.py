import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv
from logger import get_logger

# Load .env
load_dotenv()
logger = get_logger("market_breadth")

# Get shared paths
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(base_dir, "data")
os.makedirs(data_dir, exist_ok=True)

# Read SQL credentials
driver = os.getenv("SQL_DRIVER")
server = os.getenv("SQL_SERVER")
database = os.getenv("SQL_DATABASE")
username = os.getenv("SQL_UID")
password = os.getenv("SQL_PWD")


def gather_market_breadth_data():
    """Connect to SQL and pull NYAD + NYMO data into CSV (starting from 2005-01-01)."""
    conn_str = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )

    try:
        conn = pyodbc.connect(conn_str)
        logger.info("Connected to SQL Server.")
    except Exception as e:
        logger.error(f"Failed to connect to SQL Server: {e}")
        raise

    query = """
    SELECT 
        Symbol,
        CAST([Date] AS DATE) AS [Date],
        [Open], 
        [High], 
        [Low], 
        [Close], 
        [Volume]
    FROM CustomSymbols
    WHERE Symbol IN ('$NYAD.N', '$NYMO.N')
      AND [Date] >= '2005-01-01'
    ORDER BY [Date];
    """

    try:
        df = pd.read_sql(query, conn)
        logger.info(f"Retrieved {len(df)} rows from SQL Server (from 2005-01-01).")

        path = os.path.join(data_dir, "market_breadth_data.csv")
        df.to_csv(path, index=False)
        logger.info(f"Saved market_breadth_data.csv to {path}")
    except Exception as e:
        logger.error(f"Failed to query and save data: {e}")
        raise
    finally:
        try:
            conn.close()
            logger.info("Closed SQL connection.")
        except:
            logger.warning("Could not close SQL connection properly.")

def reformat_breadth_data():
    """Reformat NYAD + NYMO columns and save merged output."""
    input_path = os.path.join(data_dir, "market_breadth_data.csv")
    output_path = os.path.join(data_dir, "MarketData_NYAD_NYMO.csv")

    try:
        df = pd.read_csv(input_path, parse_dates=["Date"])
        df["Symbol"] = df["Symbol"].str.replace("$", "", regex=False)

        nyad = df[df["Symbol"] == "NYAD.N"].drop(columns="Symbol").copy()
        nymo = df[df["Symbol"] == "NYMO.N"].drop(columns="Symbol").copy()

        nyad = nyad.rename(columns={
            "Open": "Open_NYAD", "High": "High_NYAD", "Low": "Low_NYAD",
            "Close": "Close_NYAD", "Volume": "Volume_NYAD"
        })

        nymo = nymo.rename(columns={
            "Open": "Open_NYMO", "High": "High_NYMO", "Low": "Low_NYMO",
            "Close": "Close_NYMO", "Volume": "Volume_NYMO"
        })

        merged = pd.merge(nymo, nyad, on="Date", how="outer")
        merged.sort_values("Date", inplace=True)
        merged.to_csv(output_path, index=False)
        logger.info(f"Merged NYAD/NYMO saved to {output_path}")
    except Exception as e:
        logger.error(f"Failed to reformat breadth data: {e}")
        raise


def merge_with_market_data():
    """Merge reformatted breadth data into MarketStates_Data.csv, starting from 2005-01-01."""
    market_path = os.path.join(data_dir, "MarketStates_Data.csv")
    breadth_path = os.path.join(data_dir, "MarketData_NYAD_NYMO.csv")

    if not os.path.exists(market_path):
        logger.warning("MarketStates_Data.csv not found. Skipping merge.")
        return

    try:
        market_df = pd.read_csv(market_path, parse_dates=["Date"])
        breadth_df = pd.read_csv(breadth_path, parse_dates=["Date"])

        # Filter breadth data to 2005-01-01 and later
        breadth_df = breadth_df[breadth_df["Date"] >= pd.Timestamp("2005-01-01")]

        final_df = pd.merge(market_df, breadth_df, on="Date", how="outer")
        final_df.sort_values("Date", inplace=True)
        final_df.to_csv(market_path, index=False)
        logger.info(f"Merged breadth data (from 2005) into MarketStates_Data.csv")
    except Exception as e:
        logger.error(f"Failed to merge with market data: {e}")
        raise


if __name__ == "__main__":
    gather_market_breadth_data()
    reformat_breadth_data()
    merge_with_market_data()

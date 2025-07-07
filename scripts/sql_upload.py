import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv

load_dotenv()  # Make sure this is called early


def upload_market_states_to_sql(csv_path, list_name, list_description):
    # Database Connection String
    conn_str = (
        f"DRIVER={{{os.getenv('DB_DRIVER')}}};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={os.getenv('DB_NAME')};"
        f"UID={os.getenv('DB_USER')};"
        f"PWD={os.getenv('DB_PASSWORD')};"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Check for or insert MarketStates list
    cursor.execute("SELECT Id FROM dbo.MarketStates WHERE Name = ?", list_name)
    row = cursor.fetchone()
    if row:
        market_state_id = row[0]
    else:
        cursor.execute(
            "INSERT INTO dbo.MarketStates (Name, Description) VALUES (?, ?)",
            list_name, list_description
        )
        conn.commit()
        cursor.execute("SELECT SCOPE_IDENTITY();")
        market_state_id = cursor.fetchone()[0]

    # Market state name ➝ direction ID mapping
    cursor.execute("SELECT ID, Category FROM dbo.MarketStateCategories")
    mapping = {row.Category.strip(): row.ID for row in cursor.fetchall()}

    # Read classified CSV and clean
    df = pd.read_csv(csv_path, parse_dates=["Date"])
    df = df[["Date", "MarketState"]].copy()
    df["MarketState"] = df["MarketState"].astype(str).str.strip()

    # Insert each row into dbo.MarketStateDirection
    for _, row in df.iterrows():
        direction_id = mapping.get(row["MarketState"])
        if pd.notnull(row["Date"]) and direction_id is not None:
            cursor.execute(
                "INSERT INTO dbo.MarketStateDirection (MarketStateId, Date, Direction) VALUES (?, ?, ?)",
                market_state_id, row["Date"], direction_id
            )

    conn.commit()
    cursor.close()
    conn.close()

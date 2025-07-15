import os
import pandas as pd
import numpy as np
import logging
import sys
import pymssql
from dotenv import load_dotenv

# ========== Configurable System ==========
system_name = os.getenv("SYSTEM_NAME", "A")
list_id = int(os.getenv("LIST_ID", 1))
list_name = os.getenv("LIST_NAME", "Market States 2005-Present Original Scoring")
list_description = os.getenv("LIST_DESCRIPTION", "Market States List 7-9 Original Scoring")

# ========== Logger Setup ==========
def get_logger(name=f"upload_sql_system_{system_name.lower()}"):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(base_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, f"{name}.log")

    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.FileHandler(log_path)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)

    return logger

logger = get_logger()

# ========== SQL Connection ==========
def get_sql_connection():
    load_dotenv()
    server   = os.getenv("SQL_SERVER_MS")
    user     = os.getenv("SQL_UID_MS")
    password = os.getenv("SQL_PWD_MS")
    database = os.getenv("SQL_DATABASE_MS")
    return pymssql.connect(server=server, user=user, password=password, database=database)

# ========== Core Upload Function ==========
def upload_market_states():
    txt_file_relpath = f"data/MarketStates_System_{system_name}.txt"

    # Build absolute path
    script_dir   = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    txt_file_path = os.path.join(project_root, txt_file_relpath)

    if not os.path.isfile(txt_file_path):
        print(f"ERROR: file not found → {txt_file_path}")
        return

    try:
        conn = get_sql_connection()
        cursor = conn.cursor()

        # Ensure MarketStates entry exists
        cursor.execute("SELECT Id FROM dbo.MarketStates WHERE Id = %s", (list_id,))
        if not cursor.fetchone():
            cursor.execute("SET IDENTITY_INSERT dbo.MarketStates ON;")
            cursor.execute(
                "INSERT INTO dbo.MarketStates (Id, Name, Description) VALUES (%s, %s, %s)",
                (list_id, list_name, list_description)
            )
            cursor.execute("SET IDENTITY_INSERT dbo.MarketStates OFF;")
            conn.commit()
            print(f"Inserted new MarketStates entry: {list_id}")
        else:
            print(f"MarketStates entry {list_id} already exists")

        # Load mapping
        market_state_mapping = {}
        cursor.execute("SELECT ID, Category FROM dbo.MarketStateCategories")
        for cat_id, cat_name in cursor.fetchall():
            market_state_mapping[cat_name.strip()] = cat_id

        # Read txt file
        df = pd.read_csv(txt_file_path, names=["Date", "MarketState"])
        df["Date"] = pd.to_datetime(df["Date"].str.strip(), errors="coerce")
        df["MarketState"] = df["MarketState"].astype(str).str.strip()

        cursor.execute(
            "SELECT [Date] FROM dbo.MarketStateDirection WHERE MarketStateId = %s",
            (list_id,)
        )
        existing_dates = {r[0].date() for r in cursor.fetchall()}

        new_rows = 0
        for idx, row in df.iterrows():
            dt = row["Date"]
            state = row["MarketState"]
            direction_id = market_state_mapping.get(state)

            if pd.isna(dt) or direction_id is None:
                print(f"Skipping row {idx}: invalid → {row.tolist()}")
                continue

            if dt.date() in existing_dates:
                print(f"Date {dt.date()} already exists, skipping")
                continue

            cursor.execute(
                """
                INSERT INTO dbo.MarketStateDirection
                    (MarketStateId, [Date], Direction)
                VALUES
                    (%s, %s, %s)
                """,
                (list_id, dt.strftime("%Y-%m-%d"), direction_id)
            )
            new_rows += 1

        conn.commit()
        print(f"Inserted {new_rows} new row(s) into dbo.MarketStateDirection.")

    except Exception as e:
        print(f"ERROR: {e}")

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

# ========== Entry ==========
if __name__ == "__main__":
    upload_market_states()

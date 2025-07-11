import os
import pandas as pd
import numpy as np
import logging
import sys
import pymssql
from dotenv import load_dotenv

# ========== Logger Setup ==========
def get_logger(name="market_state_system"):
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

# ========== SQL Upload Utilities ==========
def get_sql_connection():
    load_dotenv()
    server = os.getenv("SQL_SERVER_MS")
    user = os.getenv("SQL_UID_MS")
    password = os.getenv("SQL_PWD_MS")
    database = os.getenv("SQL_DATABASE_MS")
    return pymssql.connect(server=server, user=user, password=password, database=database)

def upload_market_states(txt_file_path, list_id, list_name, list_description):
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT Id FROM dbo.MarketStates WHERE Id = %s", (list_id,))
        row = cursor.fetchone()

        if not row:
            cursor.execute(
                "INSERT INTO dbo.MarketStates (Id, Name, Description) VALUES (%s, %s, %s)",
                (list_id, list_name, list_description)
            )
            conn.commit()
            print(f"Inserted new MarketStates entry. Using MarketStateId: {list_id}")
        else:
            print(f"Found existing MarketStates entry. Using MarketStateId: {list_id}")

        market_state_mapping = {}
        cursor.execute("SELECT ID, Category FROM dbo.MarketStateCategories")
        for row in cursor.fetchall():
            market_state_mapping[row[1].strip()] = row[0]

        df_split = pd.read_csv(txt_file_path, names=["Date", "MarketState"])
        df_split["Date"] = pd.to_datetime(df_split["Date"].str.strip(), errors="coerce")
        df_split["MarketState"] = df_split["MarketState"].astype(str).str.strip()

        cursor.execute("SELECT Date FROM dbo.MarketStateDirection WHERE MarketStateId = %s", (list_id,))
        existing_dates = {row[0].date() for row in cursor.fetchall()}

        new_rows = 0
        for index, row in df_split.iterrows():
            date = row["Date"]
            state = row["MarketState"]
            direction_id = market_state_mapping.get(state)

            if pd.notnull(date) and direction_id and date.date() not in existing_dates:
                cursor.execute(
                    "INSERT INTO dbo.MarketStateDirection (MarketStateId, Date, Direction) VALUES (%s, %s, %s)",
                    (list_id, date.strftime('%Y-%m-%d'), direction_id)
                )
                new_rows += 1
            else:
                print(f"Skipping row {index}: invalid or duplicate → {date}, {state}")

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

# ========== Upload Functions for Each System ==========
def upload_market_states_system_a():
    upload_market_states(
        txt_file_path="data/MarketStates_System_A.txt",
        list_id=1,
        list_name="Market States 2005-Present Original Scoring",
        list_description="Market States List 7-9 Original Scoring"
    )

def upload_market_states_system_b():
    upload_market_states(
        txt_file_path="data/MarketStates_System_B.txt",
        list_id=2,
        list_name="Market States 2005-Present Original Scoring",
        list_description="Market States List 7-9 Original Scoring"
    )

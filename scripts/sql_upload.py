import os
import pymssql
import pandas as pd
from dotenv import load_dotenv

def upload_market_states(txt_file_path="data/MarketStates.txt", list_name="Market States 2024", list_description="Historical Market States List 2024 6-19-2025"):
    load_dotenv()

    # Load environment variables
    server = os.getenv("SQL_SERVER_MS")
    user = os.getenv("SQL_UID_MS")
    password = os.getenv("SQL_PWD_MS")
    database = os.getenv("SQL_DATABASE_MS")

    try:
        # Connect to SQL Server
        conn = pymssql.connect(server=server, user=user, password=password, database=database)
        cursor = conn.cursor()

        # Step 1: Get or create MarketStates ID
        cursor.execute("SELECT Id FROM dbo.MarketStates WHERE Name = %s", (list_name,))
        row = cursor.fetchone()

        if row:
            market_state_id = row[0]
            print(f"Found existing MarketStates entry. Using MarketStateId: {market_state_id}")
        else:
            cursor.execute(
                "INSERT INTO dbo.MarketStates (Name, Description) VALUES (%s, %s)",
                (list_name, list_description)
            )
            conn.commit()
            cursor.execute("SELECT @@IDENTITY")
            market_state_id = cursor.fetchone()[0]
            print(f"🆕 Inserted new MarketStates entry. Using MarketStateId: {market_state_id}")

        # Step 2: Create mapping of MarketState → Direction ID
        market_state_mapping = {}
        cursor.execute("SELECT ID, Category FROM dbo.MarketStateCategories")
        for row in cursor.fetchall():
            market_state_mapping[row[1].strip()] = row[0]

        # Step 3: Read txt file
        df_raw = pd.read_csv(txt_file_path, header=None)
        df_split = df_raw[0].str.split(",", n=1, expand=True)
        df_split.columns = ["Date", "MarketState"]
        df_split["Date"] = pd.to_datetime(df_split["Date"].str.strip(), errors="coerce")
        df_split["MarketState"] = df_split["MarketState"].astype(str).str.strip()

        # Step 4: Check existing dates to skip duplicates
        cursor.execute("SELECT Date FROM dbo.MarketStateDirection WHERE MarketStateId = %s", (market_state_id,))
        existing_dates = {row[0].date() for row in cursor.fetchall()}

        # Step 5: Insert new rows
        new_rows = 0
        for index, row in df_split.iterrows():
            date = row["Date"]
            state = row["MarketState"]
            direction_id = market_state_mapping.get(state)

            if pd.notnull(date) and direction_id and date.date() not in existing_dates:
                cursor.execute(
                    "INSERT INTO dbo.MarketStateDirection (MarketStateId, Date, Direction) VALUES (%s, %s, %s)",
                    (market_state_id, date.strftime('%Y-%m-%d'), direction_id)
                )
                new_rows += 1
            else:
                print(f"⚠Skipping row {index}: invalid or duplicate → {date}, {state}")

        # Finalize
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

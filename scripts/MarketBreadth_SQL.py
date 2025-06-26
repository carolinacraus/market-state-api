import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Read from .env
driver = os.getenv("SQL_DRIVER")
server = os.getenv("SQL_SERVER")
database = os.getenv("SQL_DATABASE")
username = os.getenv("SQL_UID")
password = os.getenv("SQL_PWD")

# Construct connection string
conn_str = (
    f"DRIVER={driver};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
)

# Connect to SQL Server
try:
    conn = pyodbc.connect(conn_str)
    print("✅ Connected to SQL Server.")
except Exception as e:
    print(f"❌ Failed to connect: {e}")
    exit()

# SQL query for NYAD and NYMO
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
ORDER BY [Date];
"""

# Execute query and load to DataFrame
try:
    df = pd.read_sql(query, conn)
    print(f"✅ Retrieved {len(df)} rows.")
except Exception as e:
    print(f"❌ SQL query failed: {e}")
    conn.close()
    exit()

# Save output to data folder
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(base_dir, "data")
os.makedirs(data_dir, exist_ok=True)
output_path = os.path.join(data_dir, "market_breadth_data.csv")

df.to_csv(output_path, index=False)
print(f"✅ Saved market_breadth_data.csv to {output_path}")

# Close the connection
conn.close()

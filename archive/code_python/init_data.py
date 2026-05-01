import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load data with local file

df = pd.read_csv("data/btcusd_bitstamp_1min_latest.csv")

# Remane columns

name_col = {
    "timestamp" : "ts",
    "open" : "open_price",
    "high" : "high_price",
    "low" : "low_price",
    "close" : "close_price"
}

df = df.rename(columns=name_col)

# ADD ohter columns 

df["symbol"] = "btc"
df["source"] = "bitstamp"

print(df.columns)

# Check datatype 
print(df.info())

cols_price = ["open_price", "high_price", "low_price", "close_price"]
df[cols_price] = df[cols_price].astype(float)

df["ts"] = pd.to_datetime(df["ts"], unit="s", utc=True)

# Verification of convert data type
print(df.info())

# Verification of date range 
print(df["ts"].min())
print(df["ts"].max())

# Remove data ()
remove_mask = df["ts"] >= "2026-04-13 01:12:00+00:00"
print(df[remove_mask])  # show rax deleted

# raw delete
df = df[~remove_mask]

# Load in postgreSQL

load_dotenv()

engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# Test connection
try:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("Database connection successful!")
except Exception as e:
    print(f"Connection failed: {e}")
    exit(1)

# Insert data
df.to_sql("btcusd_1min", engine, if_exists="replace", index=False)
print("Data inserted successfully!")
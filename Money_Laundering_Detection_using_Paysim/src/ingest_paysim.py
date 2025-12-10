import os
import sys

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

# ----------------------------------------------------
# 1. Load environment variables from .env
# ----------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")

if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)
else:
    print(f"ERROR: .env file not found at {ENV_PATH}")
    sys.exit(1)

DB_PATH = os.getenv("DB_PATH")

if not DB_PATH:
    print("ERROR: DB_PATH is not set in .env")
    sys.exit(1)

# Create db directory if it doesn't exist
db_full_path = os.path.join(BASE_DIR, DB_PATH)
db_dir = os.path.dirname(db_full_path)
os.makedirs(db_dir, exist_ok=True)

# ----------------------------------------------------
# 2. Create SQLite engine
# ----------------------------------------------------
db_url = f"sqlite:///{db_full_path}"
print(f"Using SQLite database at: {db_full_path}")

try:
    engine = create_engine(db_url)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✅ Successfully connected to SQLite database.")
except Exception as e:
    print("❌ Failed to connect to SQLite.")
    print(e)
    sys.exit(1)

# ----------------------------------------------------
# 3. Locate the PaySim CSV file in data/raw
# ----------------------------------------------------
raw_dir = os.path.join(BASE_DIR, "data", "raw")

if not os.path.isdir(raw_dir):
    print(f"ERROR: Raw data folder not found at {raw_dir}")
    sys.exit(1)

csv_files = [f for f in os.listdir(raw_dir) if f.lower().endswith(".csv")]

if not csv_files:
    print(f"ERROR: No CSV files found in {raw_dir}")
    sys.exit(1)

if len(csv_files) > 1:
    print("WARNING: Multiple CSV files found in data/raw. Using the first one:")
    for f in csv_files:
        print(" -", f)

csv_name = csv_files[0]
csv_path = os.path.join(raw_dir, csv_name)
print(f"Reading PaySim data from: {csv_path}")

# ----------------------------------------------------
# 4. Read CSV into pandas
# ----------------------------------------------------
try:
    df = pd.read_csv(csv_path)
except Exception as e:
    print("❌ Failed to read CSV file.")
    print(e)
    sys.exit(1)

print("✅ CSV loaded.")
print("DataFrame shape:", df.shape)
print("Columns:", df.columns.tolist())

# ----------------------------------------------------
# 5. Validate and rename columns
# ----------------------------------------------------
rename_map = {
    "step": "step",
    "type": "type",
    "amount": "amount",
    "nameOrig": "name_orig",
    "oldbalanceOrg": "old_balance_orig",
    "newbalanceOrig": "new_balance_orig",
    "nameDest": "name_dest",
    "oldbalanceDest": "old_balance_dest",
    "newbalanceDest": "new_balance_dest",
    "isFraud": "is_fraud",
    "isFlaggedFraud": "is_flagged_fraud",
}

missing_cols = [c for c in rename_map.keys() if c not in df.columns]
if missing_cols:
    print("ERROR: The CSV is missing expected columns:", missing_cols)
    sys.exit(1)

df = df.rename(columns=rename_map)

# Add synthetic primary key
df.insert(0, "transaction_id", range(1, len(df) + 1))

# ----------------------------------------------------
# 6. Write to SQLite: table 'raw_transactions'
# ----------------------------------------------------
table_name = "raw_transactions"

print(f"Writing DataFrame to SQLite table '{table_name}' (this may take a while)...")

try:
    df.to_sql(table_name, engine, if_exists="replace", index=False, chunksize=100000)
    print(f"✅ Successfully loaded data into table '{table_name}'.")
except Exception as e:
    print("❌ Failed to write DataFrame to SQLite.")
    print(e)
    sys.exit(1)

print("🎉 Ingestion completed successfully.")


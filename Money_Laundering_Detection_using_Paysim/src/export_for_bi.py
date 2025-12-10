import os
import sys

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

# ----------------------------------------------------
# 1. Load environment variables
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

db_full_path = os.path.join(BASE_DIR, DB_PATH)
db_url = f"sqlite:///{db_full_path}"
print(f"Using SQLite database at: {db_full_path}")

# ----------------------------------------------------
# 2. Connect to SQLite
# ----------------------------------------------------
try:
    engine = create_engine(db_url)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✅ Connected to SQLite.")
except Exception as e:
    print("❌ Failed to connect to SQLite.")
    print(e)
    sys.exit(1)

# ----------------------------------------------------
# 3. Export helper
# ----------------------------------------------------
output_dir = os.path.join(BASE_DIR, "data", "bi")
os.makedirs(output_dir, exist_ok=True)

def export_table(table_name: str):
    csv_path = os.path.join(output_dir, f"{table_name}.csv")
    print(f"\nExporting {table_name} -> {csv_path}")

    try:
        with engine.connect() as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    except Exception as e:
        print(f"❌ Failed to read table '{table_name}'.")
        print(e)
        return

    print(f"{table_name} shape: {df.shape}")
    try:
        df.to_csv(csv_path, index=False)
        print(f"✅ Exported {table_name} to {csv_path}")
    except Exception as e:
        print(f"❌ Failed to write CSV for '{table_name}'.")
        print(e)

# ----------------------------------------------------
# 4. Export the BI tables
# ----------------------------------------------------
tables = [
    "suspicious_transactions",
    "suspicious_customers",
    "suspicious_by_day",
    "suspicious_by_type",
]

for t in tables:
    export_table(t)

print("\n🎉 BI export completed.")

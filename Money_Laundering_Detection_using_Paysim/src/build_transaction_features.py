import os
import sys

from dotenv import load_dotenv
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
    print("✅ Successfully connected to SQLite database.")
except Exception as e:
    print("❌ Failed to connect to SQLite.")
    print(e)
    sys.exit(1)

# ----------------------------------------------------
# 3. Create transaction_features table from clean_transactions
# ----------------------------------------------------
# We engineer a few simple features:
# - is_high_value: transaction_amount > 200000
# - is_night_txn: hour_of_day < 8 OR hour_of_day > 20
# - src_balance_change, dst_balance_change (already in clean)
#
# You can extend this later with more complex patterns.

create_sql = """
DROP TABLE IF EXISTS transaction_features;

CREATE TABLE transaction_features AS
SELECT
    transaction_id,
    event_time,
    step,
    transaction_type,
    transaction_amount,
    hour_of_day,
    day_of_week,
    src_account_id,
    old_balance_orig,
    new_balance_orig,
    src_balance_change,
    dst_account_id,
    old_balance_dest,
    new_balance_dest,
    dst_balance_change,
    CASE WHEN transaction_amount > 200000 THEN 1 ELSE 0 END AS is_high_value,
    CASE WHEN hour_of_day < 8 OR hour_of_day > 20 THEN 1 ELSE 0 END AS is_night_txn,
    is_fraud,
    is_flagged_fraud
FROM clean_transactions;
"""

print("Creating table 'transaction_features' in SQLite using SQL...")

try:
    with engine.begin() as conn:
        for statement in create_sql.strip().split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt + ";"))
    print("✅ Successfully created table 'transaction_features'.")
except Exception as e:
    print("❌ Failed to create 'transaction_features' table.")
    print(e)
    sys.exit(1)

print("🎉 Feature table transaction_features created successfully.")

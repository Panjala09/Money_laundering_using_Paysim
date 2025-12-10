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
# 3. Create clean_transactions using pure SQL
# ----------------------------------------------------
# We will:
# - derive event_time from step (step is hours from a base date)
# - compute hour_of_day and day_of_week from event_time
# - compute src_balance_change and dst_balance_change
# - rename columns for clarity
#
# In SQLite:
#   event_time = datetime(strftime('%s','2018-01-01 00:00:00') + step*3600, 'unixepoch')

create_sql = """
DROP TABLE IF EXISTS clean_transactions;

CREATE TABLE clean_transactions AS
SELECT
    transaction_id,
    datetime(strftime('%s','2018-01-01 00:00:00') + step * 3600, 'unixepoch') AS event_time,
    step,
    CAST(strftime('%H', datetime(strftime('%s','2018-01-01 00:00:00') + step * 3600, 'unixepoch')) AS INTEGER) AS hour_of_day,
    ((CAST(strftime('%w', datetime(strftime('%s','2018-01-01 00:00:00') + step * 3600, 'unixepoch')) AS INTEGER) + 6) % 7) AS day_of_week,
    type AS transaction_type,
    amount AS transaction_amount,
    name_orig AS src_account_id,
    old_balance_orig,
    new_balance_orig,
    (new_balance_orig - old_balance_orig) AS src_balance_change,
    name_dest AS dst_account_id,
    old_balance_dest,
    new_balance_dest,
    (new_balance_dest - old_balance_dest) AS dst_balance_change,
    is_fraud,
    is_flagged_fraud
FROM raw_transactions;
"""

print("Creating table 'clean_transactions' in SQLite using SQL...")

try:
    with engine.begin() as conn:
        for statement in create_sql.strip().split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt + ";"))
    print("✅ Successfully created table 'clean_transactions'.")
except Exception as e:
    print("❌ Failed to create 'clean_transactions' table.")
    print(e)
    sys.exit(1)

print("🎉 Transformation to clean_transactions (SQL-based) completed successfully.")


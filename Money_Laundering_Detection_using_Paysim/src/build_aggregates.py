import os
import sys

from dotenv import load_dotenv
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
    print("✅ Successfully connected to SQLite database.")
except Exception as e:
    print("❌ Failed to connect to SQLite.")
    print(e)
    sys.exit(1)

# ----------------------------------------------------
# 3. Build aggregate tables from suspicious_transactions
# ----------------------------------------------------
sql_script = """
-- 1) Suspicious customers: aggregate by src_account_id
DROP TABLE IF EXISTS suspicious_customers;

CREATE TABLE suspicious_customers AS
SELECT
    src_account_id,
    COUNT(*) AS suspicious_txn_count,
    SUM(transaction_amount) AS suspicious_total_amount,
    MAX(fraud_score) AS max_fraud_score,
    MAX(event_time) AS last_suspicious_time
FROM suspicious_transactions
GROUP BY src_account_id;

-- 2) Suspicious by day
DROP TABLE IF EXISTS suspicious_by_day;

CREATE TABLE suspicious_by_day AS
SELECT
    DATE(event_time) AS event_date,
    COUNT(*) AS suspicious_txn_count,
    SUM(transaction_amount) AS suspicious_total_amount,
    AVG(fraud_score) AS avg_fraud_score
FROM suspicious_transactions
GROUP BY DATE(event_time);

-- 3) Suspicious by transaction type
DROP TABLE IF EXISTS suspicious_by_type;

CREATE TABLE suspicious_by_type AS
SELECT
    transaction_type,
    COUNT(*) AS suspicious_txn_count,
    SUM(transaction_amount) AS suspicious_total_amount,
    AVG(fraud_score) AS avg_fraud_score
FROM suspicious_transactions
GROUP BY transaction_type;
"""

print("Creating aggregate tables (suspicious_customers, suspicious_by_day, suspicious_by_type)...")

try:
    with engine.begin() as conn:
        for statement in sql_script.strip().split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt + ";"))
    print("✅ Aggregate tables created successfully.")
except Exception as e:
    print("❌ Failed to create aggregate tables.")
    print(e)
    sys.exit(1)

print("🎉 Aggregation script finished successfully.")

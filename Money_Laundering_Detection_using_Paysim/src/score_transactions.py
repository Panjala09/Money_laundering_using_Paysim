import os
import sys

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
import joblib

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
# 3. Load trained model & feature columns
# ----------------------------------------------------
models_dir = os.path.join(BASE_DIR, "models")
model_path = os.path.join(models_dir, "rf_aml_model.pkl")

if not os.path.exists(model_path):
    print(f"ERROR: Model file not found at {model_path}")
    sys.exit(1)

print(f"Loading model from: {model_path}")
model_bundle = joblib.load(model_path)
model = model_bundle["model"]
feature_cols = model_bundle["feature_cols"]

print("✅ Model loaded.")
print("Feature columns used by model:")
print(feature_cols)

# ----------------------------------------------------
# 4. Prepare suspicious_transactions table (drop if exists)
# ----------------------------------------------------
print("Dropping existing 'suspicious_transactions' table if it exists...")

try:
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS suspicious_transactions;"))
    print("✅ Old suspicious_transactions (if any) dropped.")
except Exception as e:
    print("❌ Failed to drop suspicious_transactions table.")
    print(e)
    sys.exit(1)

# ----------------------------------------------------
# 5. Score transaction_features in chunks
# ----------------------------------------------------
chunk_size = 100000  # number of rows per batch
threshold = 0.8      # classify as suspicious if fraud_score >= threshold

print("Counting rows in transaction_features...")

with engine.connect() as conn:
    total_rows = conn.execute(text("SELECT COUNT(*) FROM transaction_features;")).scalar()

print(f"Total rows in transaction_features: {total_rows}")

offset = 0
processed = 0
suspicious_total = 0

while offset < total_rows:
    print(f"\nProcessing chunk starting at offset {offset}...")

    query = f"""
    SELECT *
    FROM transaction_features
    LIMIT {chunk_size} OFFSET {offset};
    """

    try:
        with engine.connect() as conn:
            df_chunk = pd.read_sql(query, conn)
    except Exception as e:
        print("❌ Failed to read chunk from transaction_features.")
        print(e)
        sys.exit(1)

    if df_chunk.empty:
        print("No more rows to process.")
        break

    # Make a copy for feature engineering
    df_feat = df_chunk.copy()

    # One-hot encode transaction_type (same logic as in training)
    df_feat = pd.get_dummies(df_feat, columns=["transaction_type"], drop_first=True)

    # Ensure all feature_cols exist; if missing, add as 0
    for col in feature_cols:
        if col not in df_feat.columns:
            df_feat[col] = 0

    # Keep only the feature columns in the right order
    X = df_feat[feature_cols]

    # Predict probabilities
    try:
        proba = model.predict_proba(X)[:, 1]
    except Exception as e:
        print("❌ Failed during model.predict_proba.")
        print(e)
        sys.exit(1)

    df_chunk["fraud_score"] = proba

    # Filter suspicious rows
    df_susp = df_chunk[df_chunk["fraud_score"] >= threshold]

    n_susp = len(df_susp)
    suspicious_total += n_susp
    processed += len(df_chunk)

    print(f"Chunk processed: {len(df_chunk)} rows, suspicious: {n_susp}, total processed: {processed}")

    # Append suspicious rows to suspicious_transactions table
    if n_susp > 0:
        try:
            df_susp.to_sql(
                "suspicious_transactions",
                engine,
                if_exists="append",
                index=False,
            )
        except Exception as e:
            print("❌ Failed to write suspicious rows to SQLite.")
            print(e)
            sys.exit(1)

    offset += chunk_size

print("\n🎉 Scoring completed.")
print(f"Total processed rows: {processed}")
print(f"Total suspicious rows (fraud_score >= {threshold}): {suspicious_total}")

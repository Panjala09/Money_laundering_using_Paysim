import os
import sys

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
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
# 3. Load a balanced SAMPLE from transaction_features
# ----------------------------------------------------
# Strategy:
#  - load ALL fraud rows (is_fraud = 1)
#  - load an equal number of non-fraud rows (is_fraud = 0, random sample)
#  => small, balanced training set that fits in memory

print("Loading fraud rows...")
fraud_query = """
SELECT *
FROM transaction_features
WHERE is_fraud = 1;
"""

try:
    with engine.connect() as conn:
        df_fraud = pd.read_sql(fraud_query, conn)
except Exception as e:
    print("❌ Failed to read fraud rows.")
    print(e)
    sys.exit(1)

n_fraud = len(df_fraud)
print(f"✅ Loaded fraud rows: {n_fraud}")

if n_fraud == 0:
    print("ERROR: No fraud rows found in transaction_features. Cannot train model.")
    sys.exit(1)

print("Loading non-fraud sample...")
nonfraud_query = f"""
SELECT *
FROM transaction_features
WHERE is_fraud = 0
ORDER BY RANDOM()
LIMIT {n_fraud};
"""

try:
    with engine.connect() as conn:
        df_nonfraud = pd.read_sql(nonfraud_query, conn)
except Exception as e:
    print("❌ Failed to read non-fraud sample.")
    print(e)
    sys.exit(1)

print(f"✅ Loaded non-fraud sample rows: {len(df_nonfraud)}")

df = pd.concat([df_fraud, df_nonfraud], ignore_index=True)
print("Combined sample shape:", df.shape)

# ----------------------------------------------------
# 4. Feature selection
# ----------------------------------------------------
feature_cols = [
    "transaction_amount",
    "hour_of_day",
    "day_of_week",
    "is_high_value",
    "is_night_txn",
    "src_balance_change",
    "dst_balance_change",
]

# simple one-hot encode for transaction_type
df = pd.get_dummies(df, columns=["transaction_type"], drop_first=True)

# update feature_cols to include the new one-hot columns
feature_cols_extended = feature_cols + [
    c for c in df.columns if c.startswith("transaction_type_")
]

for c in feature_cols_extended:
    if c not in df.columns:
        print(f"ERROR: Expected feature column missing: {c}")
        sys.exit(1)

X = df[feature_cols_extended]
y = df["is_fraud"].astype(int)

print("Feature matrix shape:", X.shape)
print("Target distribution:")
print(y.value_counts())

# ----------------------------------------------------
# 5. Train / test split
# ----------------------------------------------------
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42, stratify=y
)

print("Train shape:", X_train.shape)
print("Test shape:", X_test.shape)

# ----------------------------------------------------
# 6. Train model
# ----------------------------------------------------
model = RandomForestClassifier(
    n_estimators=200,
    max_depth=None,
    n_jobs=-1,
    random_state=42,
    class_weight="balanced_subsample",
)

print("Training RandomForest model...")
model.fit(X_train, y_train)
print("✅ Model training completed.")

# ----------------------------------------------------
# 7. Evaluate
# ----------------------------------------------------
y_pred = model.predict(X_test)
y_proba = model.predict_proba(X_test)[:, 1]

print("\nClassification report:")
print(classification_report(y_test, y_pred, digits=4))

try:
    auc = roc_auc_score(y_test, y_proba)
    print(f"ROC AUC: {auc:.4f}")
except Exception as e:
    print("Could not compute ROC AUC:", e)

# ----------------------------------------------------
# 8. Save model artifact
# ----------------------------------------------------
models_dir = os.path.join(BASE_DIR, "models")
os.makedirs(models_dir, exist_ok=True)

model_path = os.path.join(models_dir, "rf_aml_model.pkl")
joblib.dump(
    {
        "model": model,
        "feature_cols": feature_cols_extended,
    },
    model_path,
)

print(f"✅ Saved model to: {model_path}")
print("🎉 Model training script finished successfully.")

Money Laundering Detection using PaySim

End-to-End Data Engineering & ML Pipeline | Spark • DBT • ML • Power BI

This project implements a full data engineering and machine learning pipeline to detect potential money laundering using the PaySim synthetic financial transactions dataset. It covers ingestion, cleaning, feature engineering, ML scoring, Spark transformations, DBT modeling, and a Power BI dashboard.

📂 Project Structure
Money_Laundering_Detection_using_Paysim/
│
├── .env
│
├── data/
│   ├── raw/
│   │   └── PS_20174392719_1491204439457_log.csv
│   │
│   ├── bi/
│   │   ├── suspicious_transactions.csv
│   │   ├── suspicious_customers.csv
│   │   ├── suspicious_by_day.csv
│   │   └── suspicious_by_type.csv
│   │
│   └── spark/
│       └── clean_transactions/
│           ├── part-00000-xxxx.snappy.parquet
│           ├── part-00001-xxxx.snappy.parquet
│           ├── ...
│           └── _SUCCESS
│
├── db/
│   └── aml_paysim.db
│
├── models/
│   └── rf_aml_model.pkl
│
├── src/
│   ├── ingest_paysim.py
│   ├── transform_to_clean.py
│   ├── build_features.py
│   ├── train_model.py
│   ├── score_transactions.py
│   ├── build_aggregates.py
│   ├── export_for_bi.py
│   └── spark_clean_paysim.py
│
├── aml_dbt/
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/
│       │   └── stg_transaction_features.sql
│       │
│       └── marts/
│           ├── mart_suspicious_customers.sql
│           ├── mart_suspicious_by_day.sql
│           └── mart_suspicious_by_type.sql
│
└── Money_Laundering_Detection.pbix

🏗️ Pipeline Overview
1️⃣ Ingestion

Loads raw PaySim CSV into the project.

2️⃣ Cleaning (Spark)

Transforms raw data into clean parquet files.

3️⃣ Feature Engineering

Builds AML-focused behavior features.

4️⃣ Machine Learning

Trains a Random Forest model to classify suspicious transactions.

5️⃣ Scoring

Applies the model to generate risk scores.

6️⃣ DBT Modeling

Creates staging and mart tables for analytics.

7️⃣ Power BI Dashboard

Visualizes suspicious patterns, high-risk customers, and trends.

🧱 Technologies Used

Python

PySpark

Scikit-learn

SQLite

DBT

Power BI

Parquet / CSV

📊 Power BI Dashboard

The Power BI report provides:

Suspicious transactions

Risk patterns by day & transaction type

High-risk customers

Aggregated AML insights

🎯 Highlights

End-to-end data engineering pipeline

Spark-based scalable transformations

ML model for AML detection

DBT semantic layer

BI-ready analytical datasets

Full project lifecycle from raw data → ML → dashboard

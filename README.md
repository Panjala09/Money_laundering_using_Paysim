Money Laundering Detection using PaySim

End-to-End Data Engineering & ML Pipeline | Spark • DBT • ML • Power BI

This project implements a complete data engineering and machine learning pipeline to detect potential money laundering activities using the PaySim synthetic financial transaction dataset. It showcases modern data engineering practices including ingestion, cleaning, feature engineering, ML model training, DBT modeling, Spark transformations, and a Power BI dashboard for business insights.

🏗️ Pipeline Overview
1.	Data Ingestion

•	Loads raw PaySim dataset into the pipeline

•	Environment variables managed using .env

2.	Data Cleaning (Spark)

•	Converts raw transaction logs into optimized parquet format

•	Handles schema standardization and data quality checks

3.	Feature Engineering

•	Creates AML-focused behavioral features

•	Identifies abnormal patterns such as high-value transfers and rapid repeated transactions

4.	Machine Learning Model

•	Trains a Random Forest classifier to detect suspicious transactions

•	Saves trained model for downstream scoring

5.	Transaction Scoring

•	Scores all transactions to assign risk levels

•	Outputs suspicious activity for analytics

6.	DBT Transformation Layer

•	Builds staging and mart models

•	Produces analytical datasets for reporting

7.	Power BI Dashboard

•	Interactive AML dashboard that includes:

•	Suspicious transactions

•	High-risk customers

•	Daily & category-wise risk trends

•	Aggregated AML insights

🧱 Tech Stack

•	Languages & Tools

•	Python

•	PySpark

•	Scikit-learn

•	DBT

•	Power BI

•	SQLite

•	Parquet / CSV

Concepts Covered

•	ETL / ELT Pipelines

•	Feature Engineering

•	ML Model Lifecycle

•	Data Warehousing (Marts/Staging)

•	Distributed Processing

•	BI Reporting

🎯 Key Highlights

•	Complete end-to-end data engineering project

•	Spark-based scalable processing

•	ML-driven suspicious activity detection

•	DBT modeling for clean analytical layers

•	BI dashboard enabling AML insights

•	Follows modular, production-style pipeline design



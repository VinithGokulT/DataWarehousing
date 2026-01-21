📘 Data Warehousing Project – Bronze, Silver, Gold Layers
📂 Project Overview
This project demonstrates a modern data warehousing pipeline using the medallion architecture:
•	Bronze Layer → Raw ingestion of source data into staging tables.
•	Silver Layer → Cleaned and transformed data with business rules applied.
•	Gold Layer → Curated, modeled data ready for analytics and reporting.
The pipeline is implemented entirely in SQL Server using BULK INSERT, TRUNCATE, and transformation queries.
🥉 Bronze Layer – Raw Data Ingestion
The Bronze layer stores raw data exactly as received from source systems (CRM, ERP, etc.).
Steps:
1.	Truncate existing tables to ensure fresh loads.
2.	Bulk insert CSV files into staging tables.
3.	Repeat for all source tables (customers, products, sales, ERP data).
🥈 Silver Layer – Data Cleaning & Transformation
The Silver layer applies cleaning, deduplication, and normalization.
Typical SQL Operations:
•	Remove duplicates:
•	Standardize data types (e.g., dates, numeric formats).
•	Apply business rules (e.g., filter invalid records, join CRM + ERP).
•	Insert into Silver tables:
🥇 Gold Layer – Business Modeling
The Gold layer contains curated, analytics-ready tables for BI dashboards and reporting.
Examples:
•	Star schema modeling (fact + dimension tables).
•	Fact table creation:
•	Dimension tables:
•	dim_customer
•	dim_product
•	dim_location
⚙️ Execution Flow
1.	Run Bronze load procedure (bronze.load_bronze) to ingest raw CSVs.
2.	Execute Silver transformation scripts to clean and normalize.
3.	Populate Gold schema with fact/dimension tables for analytics.
📊 Tools & Technologies
•	SQL Server (T-SQL)
•	CSV files as source data
•	SSMS for execution and monitoring
•	BI tools (Power BI, Tableau) can connect to Gold layer for reporting
🚀 Outcomes
•	Bronze → Raw, unmodified data for traceability.
•	Silver → Clean, reliable data for business use.
•	Gold → Optimized schema for analytics and decision-making.


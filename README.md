📌 Loan Ledger ETL Pipeline : 

Overview

The Loan Ledger ETL Pipeline is an end-to-end data engineering project built using PySpark to process, validate, transform, and load loan-related financial data.

This pipeline is designed to simulate a real-world production data system, supporting both local and cloud (AWS S3) environments using a configuration-driven architecture.

Features

```
✅ Config-driven environment setup (Local / AWS)
✅ Scalable data ingestion from file system / S3
✅ Data cleaning and validation
✅ Business rule transformations
✅ Data quality checks
✅ Curated data storage
✅ Modular load layer (S3 / RDS ready)
✅ Centralized logging
✅ Test-ready structure
```

Project Architecture :

```
Extract  →  Transform  →  Quality Check  →  Load
   ↓           ↓             ↓              ↓
 Local/S3   Cleaning     Validation      S3 / RDS

```

📂 Folder Structure

```

src/
├── main.py
├── jobs/
│   └── etl_job.py
├── config/
│   └── app_config.yaml
├── extract/
│   └── s3_reader.py
├── transform/
│   ├── data_cleaning.py
│   └── business_rules.py
├── quality/
│   └── data_quality.py
├── load/
│   ├── s3_writer.py
│   └── rds_writer.py
├── utils/
│   ├── spark_session.py
│   ├── logger.py
│   └── common.py
tests/
deployment/
requirements.txt
README.md

```

⚙️ Technology Stack

Language: Python
Processing Engine: PySpark
Storage: Local / AWS S3
Database: MySQL (RDS – optional)
Configuration: YAML
Logging: Python Logging
Version Control: Git & GitHub


🔧 Configuration

All environment-specific configurations are maintained in:
config/app_config.yaml

Example:

environment: local

paths:
  local:
    input: /mnt/d/data/loan.csv
    output: /mnt/d/output/

  aws:
    input: s3://bucket/raw/
    output: s3://bucket/curated/

Change the environment to switch between local and AWS.


▶️ How to Run the Pipeline

```

1️⃣ Create Virtual Environment
python3 -m venv venv
source venv/bin/activate
2️⃣ Install Dependencies
pip install -r requirements.txt
3️⃣ Run the ETL Job
spark-submit main.py

```


🧪 Testing
Unit tests are available under the tests/ directory.
pytest tests/


📊 Logging
Logs are generated for every run in:
logs/


Example format:
2026-02-14 11:23:35 | INFO | ETL-Job | Job started


🗄️ Data Flow

1. Extract data from Local / S3
2. Clean and standardize records
3. Apply business rules
4. Perform data quality checks
5. Store curated output
6. (Optional) Load into RDS


🛠️ Current Status

```
-------------------------------------------------
Module	                     Status             
Local Processing	            ✅                 
AWS S3 Support	               ✅                 
Data Quality	               ✅
Logging	                     ✅
RDS Integration	            ⏳ In Progress
Incremental Load	            ⏳ Planned
Airflow Orchestration   	    ⏳ Planned
-------------------------------------------------
```

📈 Future Enhancements

🔹 Incremental load using watermarking
🔹 Schema versioning
🔹 Idempotent writes
🔹 Audit & reconciliation framework
🔹 Airflow DAG orchestration
🔹 Retry & failure handling
🔹 Performance optimization


💼 Use Case

This project simulates how financial institutions process loan ledger data for:

Reporting
Compliance
Analytics
Risk assessment


👨‍💻 Author

Suraj Tupkar
Data Engineer 
Python | SQL | PySpark | AWS | ETL Pipelines






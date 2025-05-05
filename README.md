
# 🚀 Data Platform with Airflow, Spark, and PostgreSQL

## 🧭 Objective

Build a modern **Data Platform** to **collect**, **process**, and **store** data from various sources.  
This project simulates how data engineers in financial or economic domains manage data workflows.

🔗 **System integrates:**

- 🗓️ **Apache Airflow** – for ETL orchestration and scheduling  
- ⚡ **Apache Spark** – for scalable and distributed data processing  
- 🗄️ **PostgreSQL** – for storing cleaned data and supporting analytics/BI tools (e.g., Power BI)

---

## 🏗️ System Architecture

```plaintext
📂 Data Sources (CSV / JSON / API / Streaming)
           │
           ▼
📋 Apache Airflow (DAG Scheduler)
           │
           ▼
⚙️ Apache Spark (ETL Processing)
           │
           ▼
🧮 PostgreSQL (Data Warehouse)
```

---

## 📊 Datasets

📥 Download from: [Kaggle - Transactions Fraud Datasets](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets)

📁 The archive contains:
- 3 CSV files  
- 2 JSON files  

### 💳 `transactions_data.csv`
- 💰 Transaction records: amount, timestamp, merchant  
- 🔍 Useful for spending pattern analysis & fraud detection

### 🧾 `cards_data.csv`
- 💳 Credit/debit card metadata  
- 🔗 Links with users via `card_id`  
- 📊 Helps profile customer financial behavior

### 🏪 `mcc_codes.json`
- 🏷️ Business classification codes  
- 🧠 Enables categorizing transactions by industry

### 🚨 `train_fraud_labels.json`
- ⚖️ Fraud / Legitimate labels  
- 🧠 Suitable for training supervised ML models

### 👤 `users_data.csv`
- 🧬 Demographic & account details  
- 🎯 Enables segmentation and personalization

📂 After downloading and extracting, place the files in the `data/` folder.  
📦 `load/` and `staging/` folders will be **auto-generated** after running the ETL DAG.

🖼️ Example:

![Dataset](screenshots/datasetPlacing.png)

---

## 🛠️ System Setup

⚠️ **NOTE for WSL Users:**  
Do **NOT** place this project in the `/mnt/` folder.  
WSL **mount volumes** do not allow writing files, which will break Airflow and Spark execution.

---

### 🐳 1. Start Docker Containers

```bash
docker-compose up -d
```

This spins up:
- 🗄️ PostgreSQL Database  
- 📋 Airflow Scheduler  
- 🌐 Airflow Webserver  
- ⚙️ Spark Master + Worker  

---

### 🌐 2. Access Airflow UI

📍 Open your browser and go to:
```bash
localhost:8082
```

You’ll see the Apache Airflow interface:

![AirflowUI](screenshots/airflowUI.png)

---

📂 In this project directory:
- `dags/` – contains the DAG for ETL  
- `main_pipeline.py` – orchestrates task flow  
- `spark-jobs/` – holds Spark transformation scripts  

✅ After successful execution:
- `data/staging/` – intermediate cleaned data  
- `data/load/` – final transformed data ready for analytics


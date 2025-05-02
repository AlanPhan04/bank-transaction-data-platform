# 🚀 Data Platform with Airflow, Spark, and PostgreSQL

## 🗂️ Objective

Build a modern **Data Platform** to collect, process, and store data from various sources. This project simulates how data engineers in financial or economic domains manage data workflows.

The system integrates:

- **Apache Airflow** – for ETL orchestration and scheduling.
- **Apache Spark** – for scalable and distributed data processing.
- **PostgreSQL** – for storing processed and cleaned data to support analytics and BI tools (e.g., Power BI).

---

## 🛠️ System Architecture

```plaintext
Data Sources (CSV/JSON/API/Streaming)
           │
           ▼
     Apache Airflow (DAG Scheduler)
           │
           ▼
     Apache Spark (ETL Processing)
           │
           ▼
     PostgreSQL (Data Warehouse)

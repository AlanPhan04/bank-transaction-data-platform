# Bank Transaction Data Platform

## 📚 Project Overview

This project builds a simple Data Platform that ingests, transforms, and analyzes anonymous bank transaction data.  
It demonstrates an end-to-end ETL pipeline, data warehouse modeling, and business intelligence dashboard focused on fraud detection.

**Key Features:**
- Automated ETL workflow using Apache Airflow
- Raw and clean data storage (PostgreSQL + Parquet)
- Data warehouse star schema modeling
- BI Dashboard visualization (Apache Superset or Metabase)
- Dockerized environment for easy setup

---

## 🏗️ Architecture

```plaintext
CSV Dataset
    ↓
Apache Airflow (ETL)
    ↓
PostgreSQL (Raw Data Layer)
    ↓
Transform & Clean
    ↓
PostgreSQL (DW Schema) / Parquet (Processed Data)
    ↓
BI Dashboard (Fraud Analysis)

# Data Platform with Airflow, Spark, and PostgreSQL

## 🗂️ Mục tiêu

Xây dựng một nền tảng dữ liệu (Data Platform) hiện đại để thu thập, xử lý và lưu trữ dữ liệu từ nhiều nguồn khác nhau. Hệ thống sử dụng:

- **Apache Airflow**: Điều phối các pipeline ETL.
- **Apache Spark**: Xử lý dữ liệu lớn theo kiểu batch hoặc streaming.
- **PostgreSQL**: Lưu trữ kết quả dữ liệu đã xử lý để phân tích và truy vấn.

---

## 🛠️ Kiến trúc hệ thống

```plaintext
Nguồn dữ liệu (CSV/API/Streaming)
           │
           ▼
     Apache Airflow (scheduler)
           │
           ▼
     Apache Spark (ETL/ELT)
           │
           ▼
     PostgreSQL (Data Warehouse)

import psycopg2 
import json
from pathlib import Path

def run(tablename):
    print("Ingesting from PostgreSQL...")
    # Đọc config từ file .env hoặc config.yaml
    conn = psycopg2.connect(
        host="localhost",
        port = 5432,
        dbname="source_db",
        user="admin",
        password="admin"
    ) 
    print("Connect successfully")
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {tablename}")  # Ví dụ
    rows = cur.fetchall()
    
    output_path = Path("data/raw/postgres_data.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(rows, f)

    cur.close()
    conn.close()
    print(f"Data saved to {output_path}")
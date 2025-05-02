from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default args setup
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG("bank_analytics", default_args=default_args, schedule_interval=timedelta(1)) as dag:
    
    # Define the transform task using BashOperator
    transform = BashOperator(
        task_id="transform",
        bash_command="""
        spark-submit --master local[*] /opt/airflow/spark-jobs/transform.py
        """
    )

    # Task dependencies (if any, otherwise it's just the transform task)
    transform

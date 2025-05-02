from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 2),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG("bank_pipeline", default_args=default_args, schedule_interval=timedelta(1)) as dag:
    extract = BashOperator(
        task_id="extract",
        bash_command="""
        spark-submit --master local[*] /opt/airflow/spark-jobs/extract.py
        """
    )

    transform_and_load = BashOperator(
        task_id="transform_and_load",
        bash_command="""
        spark-submit --master local[*] /opt/airflow/spark-jobs/transform_and_load.py
        """
    )

    extract >> transform_and_load
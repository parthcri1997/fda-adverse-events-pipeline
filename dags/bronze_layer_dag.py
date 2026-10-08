from ingestion import run_bronze_ingestion
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta

with DAG(dag_id = 'bronze_layer_data_ingestion',
         start_date = datetime(2026,07,29),
         schedule_interval = '@daily',
         catchup = False,
         default_args = {
             'retries':1,
             'retry_delay': timedelta(minutes=5)
         }


) as dag:
    bronze_layer_ingestion_task = PythonOperator(task_id='bronze_layer_ingestion',
                   python_callable = run_bronze_ingestion)


    bronze_layer_ingestion_task

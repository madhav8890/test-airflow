from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

def list_s3_buckets():
    hook = S3Hook(aws_conn_id='')  # Replace 'aws_conn' with your Airflow connection ID
    buckets = hook.get_conn().list_buckets()
    for bucket in buckets['Buckets']:
        print(f"Bucket Name: {bucket['Name']}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 28),
    'retries': 1,
}

with DAG(
    dag_id='list_s3_buckets_dag-1',
    default_args=default_args,
    schedule_interval=None,  # Run on demand
    catchup=False,
) as dag:
    
    list_buckets_task = PythonOperator(
        task_id='list_s3_buckets',
        python_callable=list_s3_buckets,
    )


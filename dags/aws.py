from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
from airflow.hooks.base_hook import BaseHook

def get_aws_credentials():
    aws_connection = BaseHook.get_connection('aws_default')
    aws_access_key = aws_connection.login
    aws_secret_key = aws_connection.password
    return aws_access_key, aws_secret_key

def list_s3_buckets():
    aws_access_key, aws_secret_key = get_aws_credentials()
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    response = s3_client.list_buckets()
    for bucket in response['Buckets']:
        print(f"Bucket Name: {bucket['Name']}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 28),
    'retries': 1,
}

with DAG(
    dag_id='list_s3_buckets_with_aws_default',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    list_buckets_task = PythonOperator(
        task_id='list_s3_buckets_task',
        python_callable=list_s3_buckets,
    )


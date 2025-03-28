from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3

def list_s3_buckets():
    # Create a Boto3 client for S3
    s3_client = boto3.client('s3')
    
    # List all buckets
    response = s3_client.list_buckets()
    
    # Print the bucket names
    for bucket in response.get('Buckets', []):
        print(f"Bucket Name: {bucket['Name']}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 28),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='list_s3_buckets_with_boto3',
    default_args=default_args,
    schedule_interval=None,  # Run on demand
    catchup=False,
) as dag:
    
    # Task to list S3 buckets
    list_buckets_task = PythonOperator(
        task_id='list_s3_buckets_task',
        python_callable=list_s3_buckets,
    )


from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def check_s3_buckets():
    try:
        s3_hook = S3Hook(aws_conn_id='aws_default')  # Use Airflow Connection
        buckets = s3_hook.get_conn().list_buckets()
        print("✅ AWS S3 Connection Successful! Available Buckets:")
        for bucket in buckets['Buckets']:
            print(f"- {bucket['Name']}")
    except Exception as e:
        print(f"❌ AWS S3 Connection Failed: {str(e)}")
        raise

with DAG(
    "test_aws_connectivity",
    schedule_interval=None,
    start_date=datetime(2025, 3, 31),
    catchup=False,
) as dag:

    test_connection = PythonOperator(
        task_id="check_s3_buckets",
        python_callable=check_s3_buckets,
    )

    test_connection

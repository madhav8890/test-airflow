from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_aws_s3_connection():
    """Test AWS S3 connection using Airflow"""
    try:
        aws_s3_hook = S3Hook(aws_conn_id="aws_default")
        buckets = aws_s3_hook.list_buckets()
        print(f"✅ AWS S3 Connection Successful! Buckets: {buckets}")
    except Exception as e:
        print(f"❌ AWS S3 Connection Failed: {e}")
        raise

with DAG(
    dag_id="test_minio_aws_connectivity",
    schedule_interval=None,
    start_date=datetime(2024, 3, 31),
    catchup=False,
    tags=["aws", "minio", "test"]
) as dag:

    test_aws_s3 = PythonOperator(
        task_id="test_aws_s3_connectivity",
        python_callable=test_aws_s3_connection,
    )

    test_aws_s3  # MinIO check runs before AWS check

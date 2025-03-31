from datetime import datetime
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook

def test_aws_s3_connection():
    # Using the 'aws_default' connection ID configured in Airflow UI
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    try:
        # List S3 Buckets
        buckets = s3_hook.get_conn().list_buckets()
        if buckets['Buckets']:
            print("S3 Buckets:")
            for bucket in buckets['Buckets']:
                print(f"- {bucket['Name']}")
        else:
            print("No S3 Buckets found.")
    except Exception as e:
        print(f"Error: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 31),
    'retries': 1,
}

with DAG(
    'test_aws_s3_connection',
    default_args=default_args,
    description='A simple AWS S3 connection test',
    schedule_interval=None,  # Manual run
    catchup=False,
) as dag:

    # Task to test S3 connection
    test_s3_connection_task = PythonOperator(
        task_id='test_aws_s3_connection',
        python_callable=test_aws_s3_connection
    )

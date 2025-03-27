import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def check_aws_connection():
    try:
        # Create a boto3 client for EC2
        ec2 = boto3.client("ec2", region_name='ap-southeast-1')

        # Test the connection by listing available regions
        regions = ec2.describe_regions()
        print("AWS Connection Successful. Available Regions:")
        for region in regions["Regions"]:
            print(region["RegionName"])

    except Exception as e:
        print(f"AWS Connection Failed: {str(e)}")
        raise

with DAG(
    "test_aws_connection",
    schedule_interval=None,
    start_date=datetime(2024, 3, 27),
    catchup=False,
) as dag:

    test_connection = PythonOperator(
        task_id="test_aws_connectivity",
        python_callable=check_aws_connection,
    )

    test_connection


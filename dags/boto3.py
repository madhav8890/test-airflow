from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3

# AWS Connection ID (configured in Airflow Connections)
AWS_CONN_ID = "my_aws"

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "list_ec2_instances-1",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

def list_ec2_instances():
    """Lists all EC2 instances in the AWS account."""
    session = boto3.Session()
    ec2_client = session.client("ec2")

    response = ec2_client.describe_instances()

    instances = []
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            instance_id = instance["InstanceId"]
            state = instance["State"]["Name"]
            instances.append({"InstanceId": instance_id, "State": state})

    print("EC2 Instances:", instances)

# Task: List EC2 instances
list_instances_task = PythonOperator(
    task_id="list_ec2_instances",
    python_callable=list_ec2_instances,
    dag=dag,
)

list_instances_task

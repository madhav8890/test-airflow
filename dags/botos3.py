from airflow import DAG
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import boto3

def list_ec2_instances():
    aws_hook = AwsBaseHook(aws_conn_id='my_aws', client_type='ec2')
    client = aws_hook.get_client_type('ec2')
    
    instances = client.describe_instances()
    
    for reservation in instances['Reservations']:
        for instance in reservation['Instances']:
            print(f"Instance ID: {instance['InstanceId']}, State: {instance['State']['Name']}")

# Define the DAG
dag = DAG(
    'list_ec2_instances',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# Python Operator to List Instances
list_instances_task = PythonOperator(
    task_id='list_instances',
    python_callable=list_ec2_instances,
    dag=dag,
)

list_instances_task

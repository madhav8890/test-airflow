import json
from airflow import DAG
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='list_ec2_instances',
    default_args=default_args,
    description='List all EC2 instances',
    schedule_interval=None,  # Change to a cron expression if needed
    start_date=datetime(2025, 3, 28),
    catchup=False,
) as dag:

    def list_instances(**kwargs):
        # Create an EC2 hook using the connection ID
        ec2_hook = EC2Hook(aws_conn_id='my_aws')
        
        # Get all instance IDs
        instance_ids = ec2_hook.get_instance_ids()
        
        # Print out the instance IDs and their states
        print("EC2 Instance IDs and their states:")
        for instance_id in instance_ids:
            instance_info = ec2_hook.get_instance(instance_id)
            print(f"Instance ID: {instance_info.id}, State: {instance_info.state['Name']}")

    # Define a PythonOperator to execute the function
    list_ec2_instances_task = PythonOperator(
        task_id='list_ec2_instances_task',
        python_callable=list_instances,
        provide_context=True,
    )

list_ec2_instances_task

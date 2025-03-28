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
        # Initialize EC2Hook with client_type API
        ec2_hook = EC2Hook(aws_conn_id='my_aws', api_type="client_type")
        
        # Get all instance IDs using the client API
        client = ec2_hook.get_conn()
        response = client.describe_instances()
        
        # Print out instance details
        print("EC2 Instance Details:")
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                print(f"Instance ID: {instance['InstanceId']}, State: {instance['State']['Name']}")

    # Define a PythonOperator to execute the function
    list_ec2_instances_task = PythonOperator(
        task_id='list_ec2_instances_task',
        python_callable=list_instances,
        provide_context=True,
    )

list_ec2_instances_task

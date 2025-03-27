from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
from kubernetes.client import models as k8s

pod_override = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                image="apache/airflow:2.7.2",  # Use your Airflow image
                command=["/bin/bash", "-c"],
                args=["pip install boto3 requests && exec airflow tasks run"],
            )
        ]
    )
)

def list_ec2_instances(**kwargs):
    """Fetch EC2 instances and store them in XCom."""
    ec2 = boto3.client('ec2', region_name='ap-southeast-1')  # Uses default AWS authentication
    instances = ec2.describe_instances()

    instance_info = []
    for reservation in instances['Reservations']:
        for instance in reservation['Instances']:
            instance_info.append({
                'Instance ID': instance['InstanceId'],
                'State': instance['State']['Name'],
                'Type': instance['InstanceType']
            })

    # Push instance data to XCom
    ti = kwargs['ti']
    ti.xcom_push(key='ec2_instances', value=instance_info)

    print("EC2 Instances Found:", instance_info)
    return instance_info  # Also returns the value for logs

def print_ec2_instances(**kwargs):
    """Retrieve EC2 instances from XCom and print them."""
    ti = kwargs['ti']
    instances = ti.xcom_pull(task_ids='list_ec2_instances', key='ec2_instances')
    
    print("Instances Retrieved from XCom:", instances)

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'test_aws_connectivity_xcom',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    catchup=False
)

# Task 1: Fetch EC2 instances and store in XCom
fetch_instances_task = PythonOperator(
    task_id='list_ec2_instances',
    python_callable=list_ec2_instances,
    provide_context=True,
    dag=dag,
    executor_config={"pod_override": pod_override}
)

# Task 2: Retrieve and print instances from XCom
print_instances_task = PythonOperator(
    task_id='print_ec2_instances',
    python_callable=print_ec2_instances,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
fetch_instances_task >> print_instances_task


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'share_file_between_tasks',
    default_args=default_args,
    description='Example DAG showing file sharing between tasks using PVC',
    schedule_interval=None,
    start_date=datetime(2025, 3, 26),
    catchup=False,
    tags=['example'],
)

# Shared volume configuration
volume_mount = k8s.V1VolumeMount(
    name='shared-data',
    mount_path='/shared_data',
    sub_path=None,
    read_only=False
)

volume = k8s.V1Volume(
    name='shared-data',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name='airflow-shared-pvc'  # This PVC should exist in your k8s cluster
    )
)

def write_to_file(**context):
    """Task 1: Write data to a file"""
    shared_file_path = '/shared_data/example.txt'
    data = "Hello from Task 1! Timestamp: " + str(datetime.now())
    
    with open(shared_file_path, 'w') as f:
        f.write(data)
    
    print(f"Data written to {shared_file_path}")

def read_from_file(**context):
    """Task 2: Read data from the file"""
    shared_file_path = '/shared_data/example.txt'
    
    with open(shared_file_path, 'r') as f:
        data = f.read()
    
    print(f"Data read from {shared_file_path}: {data}")

# Task 1: Write to file
write_task = PythonOperator(
    task_id='write_to_file',
    python_callable=write_to_file,
    dag=dag,
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        volume_mounts=[volume_mount]
                    )
                ],
                volumes=[volume]
            )
        )
    }
)

# Task 2: Read from file
read_task = PythonOperator(
    task_id='read_from_file',
    python_callable=read_from_file,
    dag=dag,
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        volume_mounts=[volume_mount]
                    )
                ],
                volumes=[volume]
            )
        )
    }
)

# Set task dependencies
write_task >> read_task

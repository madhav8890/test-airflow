from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s

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
    description='Share file between tasks using PVC',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example'],
)

def write_to_file(**context):
    shared_file_path = '/shared_data/example.txt'
    data = f"Hello from Task 1! Timestamp: {datetime.now()}"
    
    print(f"Writing to path: {shared_file_path}")
    with open(shared_file_path, 'w') as f:
        f.write(data)
    print(f"Successfully wrote data: {data}")

def read_from_file(**context):
    shared_file_path = '/shared_data/example.txt'
    
    print(f"Reading from path: {shared_file_path}")
    with open(shared_file_path, 'r') as f:
        data = f.read()
    print(f"Successfully read data: {data}")

# Common pod override configuration
pod_override = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                volume_mounts=[
                    k8s.V1VolumeMount(
                        name="shared-data",
                        mount_path="/shared_data"
                    )
                ]
            )
        ],
        volumes=[
            k8s.V1Volume(
                name="shared-data",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name="airflow-shared-pvc"
                )
            )
        ]
    )
)

write_task = PythonOperator(
    task_id='write_to_file',
    python_callable=write_to_file,
    dag=dag,
    executor_config={"pod_override": pod_override}
)

read_task = PythonOperator(
    task_id='read_from_file',
    python_callable=read_from_file,
    dag=dag,
    executor_config={"pod_override": pod_override}
)

# Ensure sequential execution
write_task >> read_task


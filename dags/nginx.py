from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# Define DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 24),
    "catchup": False
}

# Define the DAG
dag = DAG(
    "launch_nginx_pod",
    default_args=default_args,
    schedule_interval=None
)

# KubernetesPodOperator to launch an NGINX pod
nginx_pod = KubernetesPodOperator(
    task_id="nginx",
    name="nginx-pod",
    namespace="airflow",  # Change this to match your Airflow namespace
    image="nginx:latest",
    cmds=["nginx", "-g", "daemon off;"],  # Keeps the container running
    get_logs=True,
    dag=dag
)

# Task execution
nginx_pod

